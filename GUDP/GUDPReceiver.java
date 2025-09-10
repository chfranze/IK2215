import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.io.IOException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.net.InetSocketAddress;
import java.net.InetAddress;

/*
 * GUDPReceiver monitors incoming packets and updates receiverQueue and senderQueue
 */
public class GUDPReceiver extends Thread {
    private final DatagramSocket sock;
    private final ConcurrentLinkedQueue<GUDPEndPoint> receiverQueue;
    private final GUDPSender sender;
    private final ConcurrentLinkedQueue<GUDPEndPoint> senderQueue;
    private volatile boolean runFlag = true;
    private boolean debug = true;

    public GUDPReceiver(DatagramSocket sock,
            ConcurrentLinkedQueue<GUDPEndPoint> receiverQueue,
            GUDPSender sender,
            ConcurrentLinkedQueue<GUDPEndPoint> senderQueue) {
        this.sock = sock;
        this.receiverQueue = receiverQueue;
        this.sender = sender;
        this.senderQueue = senderQueue;
    }

    public void stopGUDPReceiver() {
        this.runFlag = false;
        this.interrupt();
    }

    @Override
    public void run() {
        while (runFlag) {
            try {
                byte[] buf = new byte[GUDPPacket.MAX_DATAGRAM_LEN];
                DatagramPacket udpPacket = new DatagramPacket(buf, buf.length);
                sock.receive(udpPacket);

                GUDPPacket packet = GUDPPacket.unpack(udpPacket);
                InetSocketAddress remoteSock = packet.getSocketAddress();
                InetAddress remoteAddr = remoteSock.getAddress();
                int remotePort = remoteSock.getPort();

                if (debug)
                    System.out.println("GUDPReceiver: Got type=" + packet.getType() +
                            " seq=" + packet.getSeqno() + " from " + remoteSock);

                GUDPEndPoint ep = findReceiverEndPoint(remoteAddr, remotePort);

                switch (packet.getType()) {
                    case GUDPPacket.TYPE_ACK:
                        handleACK(remoteAddr, remotePort, packet.getSeqno());
                        break;

                    case GUDPPacket.TYPE_BSN:
                        if (ep == null) {
                            ep = new GUDPEndPoint(remoteAddr, remotePort);
                            receiverQueue.add(ep);
                        }
                        // next DATA expected is BSN + 1
                        ep.setExpectedseqnum(packet.getSeqno() + 1);
                        // keep BSN in buffer (optional)
                        ep.add(packet);
                        sendACK(ep.getRemoteEndPoint(), ep.getExpectedseqnum());
                        if (debug)
                            System.out.println("GUDPReceiver: BSN -> expected=" + ep.getExpectedseqnum());
                        break;

                    case GUDPPacket.TYPE_DATA: {
                        if (ep == null) {
                            ep = new GUDPEndPoint(remoteAddr, remotePort);
                            receiverQueue.add(ep);
                            // If no BSN seen yet, set expected to this seqno
                            ep.setExpectedseqnum(packet.getSeqno());
                            if (debug)
                                System.out.println(
                                        "Receiver: new endpoint created, syncing expected=" + ep.getExpectedseqnum());
                        }

                        int expected = ep.getExpectedseqnum();
                        if (packet.getSeqno() == expected) {
                            ep.add(packet);
                            ep.setExpectedseqnum(expected + 1);
                            if (debug)
                                System.out.println("Receiver: accepted DATA seq=" + packet.getSeqno());
                        } else {
                            if (debug)
                                System.out.println("Receiver: out-of-order DATA seq=" + packet.getSeqno()
                                        + " expected=" + expected + " (ignored)");
                        }
                        sendACK(ep.getRemoteEndPoint(), ep.getExpectedseqnum());
                        break;
                    }

                    case GUDPPacket.TYPE_FIN:
                        if (ep == null) {
                            ep = new GUDPEndPoint(remoteAddr, remotePort);
                            receiverQueue.add(ep);
                        }
                        if (packet.getSeqno() == ep.getExpectedseqnum()) {
                            ep.setExpectedseqnum(ep.getExpectedseqnum() + 1);
                            ep.setFinished(true);
                            if (debug)
                                System.out.println("GUDPReceiver: FIN accepted seq=" + packet.getSeqno());
                        }
                        sendACK(ep.getRemoteEndPoint(), ep.getExpectedseqnum());
                        break;

                    default:
                        if (debug)
                            System.out.println("GUDPReceiver: unknown type=" + packet.getType());
                }

            } catch (IOException e) {
                if (runFlag && debug)
                    System.err.println("GUDPReceiver IOException: " + e.getMessage());
            } catch (Exception ex) {
                if (debug)
                    ex.printStackTrace();
            }
        }

        receiverQueue.clear();
        senderQueue.clear();
        if (debug)
            System.out.println("GUDPReceiver: stopped");
    }

    private void handleACK(InetAddress addr, int port, int ackSeq) {
        if (debug)
            System.out.println("GUDPReceiver: processing ACK=" + ackSeq + " from " + addr + ":" + port);
        for (GUDPEndPoint senderEp : senderQueue) {
            InetSocketAddress re = senderEp.getRemoteEndPoint();
            if (re.getAddress().equals(addr) && re.getPort() == port) {
                // cumulative ACK: remove <= ackSeq-1, set base to ackSeq
                senderEp.removeAllACK(ackSeq - 1);
                senderEp.setBase(ackSeq);
                senderEp.setState(GUDPEndPoint.endPointState.RCV);
                if (debug)
                    System.out.println("GUDPReceiver: updated sender base=" + senderEp.getBase());
                // trigger sender FSM to process RCV state immediately
                sender.FSMSender(senderEp);
                break;
            }
        }
    }

    private GUDPEndPoint findReceiverEndPoint(InetAddress addr, int port) {
        for (GUDPEndPoint ep : receiverQueue) {
            InetSocketAddress re = ep.getRemoteEndPoint();
            if (re.getAddress().equals(addr) && re.getPort() == port)
                return ep;
        }
        return null;
    }

    private void sendACK(InetSocketAddress dest, int ackSeq) throws IOException {
        byte[] empty = new byte[0];
        DatagramPacket control = new DatagramPacket(empty, 0, dest);
        GUDPPacket ack = GUDPPacket.encapsulate(control); // use encapsulate to build header
        ack.setType(GUDPPacket.TYPE_ACK);
        ack.setSeqno(ackSeq);
        DatagramPacket out = ack.pack();
        out.setAddress(dest.getAddress());
        out.setPort(dest.getPort());
        sock.send(out);
        if (debug)
            System.out.println("GUDPReceiver: sent ACK=" + ackSeq + " to " + dest);
    }
}
