import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.io.IOException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.net.InetSocketAddress;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Iterator;

// Corrected GUDPReceiver
public class GUDPReceiver extends Thread {
    private final DatagramSocket sock;
    private final ConcurrentLinkedQueue<GUDPEndPoint> receiverQueue;
    private final GUDPSender s;
    private final ConcurrentLinkedQueue<GUDPEndPoint> senderQueue;
    private boolean runFlag = true;
    private boolean debug = true;

    public GUDPReceiver(DatagramSocket sock, ConcurrentLinkedQueue<GUDPEndPoint> receiverQueue,
            GUDPSender s, ConcurrentLinkedQueue<GUDPEndPoint> senderQueue) {
        this.sock = sock;
        this.receiverQueue = receiverQueue;
        this.s = s;
        this.senderQueue = senderQueue;
    }

    public void stopGUDPReceiver() {
        this.runFlag = false;
    }

    @Override
    public void run() {
        while (runFlag) {
            byte[] buf = new byte[GUDPPacket.MAX_DATAGRAM_LEN];
            DatagramPacket udpPacket = new DatagramPacket(buf, buf.length);
            try {
                sock.receive(udpPacket);
                GUDPPacket packet = GUDPPacket.unpack(udpPacket);
                InetSocketAddress remoteSock = packet.getSocketAddress();
                InetAddress remoteAddr = remoteSock.getAddress();
                int remotePort = remoteSock.getPort();

                if (debug)
                    System.out.println("GUDPReceiver FSM: Received packet type=" + packet.getType() +
                            " seq=" + packet.getSeqno() + " from " + remoteSock);

                GUDPEndPoint ep = findEndpoint(remoteAddr, remotePort);

                switch (packet.getType()) {
                    case GUDPPacket.TYPE_ACK: {
                        int ackSeq = packet.getSeqno();
                        if (debug)
                            System.out.println("GUDPReceiver FSM: Processing ACK seq=" + ackSeq);

                        for (GUDPEndPoint senderEp : senderQueue) {
                            InetSocketAddress re = senderEp.getRemoteEndPoint();
                            if (re.getAddress().equals(remoteAddr) && re.getPort() == remotePort) {
                                senderEp.removeAllACK(ackSeq - 1);
                                senderEp.setBase(ackSeq);
                                senderEp.setState(GUDPEndPoint.endPointState.RCV);
                                if (debug)
                                    System.out.println(
                                            "GUDPReceiver FSM: Updated sender endpoint Base=" + senderEp.getBase());
                                s.FSMSender(senderEp);
                                break;
                            }
                        }
                        break;
                    }

                    case GUDPPacket.TYPE_BSN: {
                        int bsnSeq = packet.getSeqno();
                        if (ep == null) {
                            ep = new GUDPEndPoint(remoteAddr, remotePort);
                            ep.setExpectedseqnum(bsnSeq + 1); // first DATA expected
                            receiverQueue.add(ep);
                            ep.add(packet);
                            sendACK(ep.getRemoteEndPoint(), bsnSeq);
                            if (debug) {
                                System.out.println("GUDPReceiver FSM: New BSN endpoint added, expected seq="
                                        + ep.getExpectedseqnum() + " ACK=" + bsnSeq);
                            }
                        } else if (ep.getFinished()) {
                            ep.clear();
                            ep.setExpectedseqnum(bsnSeq + 1);
                            ep.add(packet);
                            sendACK(ep.getRemoteEndPoint(), bsnSeq);
                            if (debug) {
                                System.out.println("GUDPReceiver FSM: BSN received, reset finished endpoint");
                            }
                        } else {
                            sendACK(ep.getRemoteEndPoint(), bsnSeq);
                            if (debug) {
                                System.out
                                        .println("GUDPReceiver FSM: BSN received, endpoint exists, sent ACK=" + bsnSeq);
                            }
                        }
                        break;
                    }

                    case GUDPPacket.TYPE_DATA: {
                        if (ep == null) {
                            ep = new GUDPEndPoint(remoteAddr, remotePort);
                            receiverQueue.add(ep);
                            if (debug)
                                System.out.println("GUDPReceiver FSM: DATA received for new endpoint");
                        }

                        int expected = ep.getExpectedseqnum();
                        if (packet.getSeqno() == expected) {
                            ep.add(packet);
                            ep.setExpectedseqnum(expected + 1);
                            if (debug)
                                System.out.println("GUDPReceiver FSM: DATA accepted seq=" + packet.getSeqno());
                        } else if (packet.getSeqno() > expected) {
                            if (debug)
                                System.out.println("GUDPReceiver FSM: DATA out-of-order seq=" + packet.getSeqno() +
                                        ", expected=" + expected + ", discarded");
                        }

                        sendACK(ep.getRemoteEndPoint(), ep.getExpectedseqnum());
                        if (debug)
                            System.out.println("GUDPReceiver FSM: ACK sent seq=" + ep.getExpectedseqnum());
                        break;
                    }

                    case GUDPPacket.TYPE_FIN: {
                        if (ep == null) {
                            ep = new GUDPEndPoint(remoteAddr, remotePort);
                            receiverQueue.add(ep);
                            if (debug)
                                System.out.println("GUDPReceiver FSM: FIN received for new endpoint");
                        }

                        int expected = ep.getExpectedseqnum();
                        if (packet.getSeqno() == expected) {
                            ep.setExpectedseqnum(expected + 1);
                            ep.setFinished(true);
                            if (debug)
                                System.out.println("GUDPReceiver FSM: FIN accepted seq=" + packet.getSeqno());
                        }

                        sendACK(ep.getRemoteEndPoint(), ep.getExpectedseqnum());
                        if (debug)
                            System.out.println("GUDPReceiver FSM: ACK sent for FIN seq=" + ep.getExpectedseqnum());
                        break;
                    }

                    default:
                        if (debug)
                            System.out.println("GUDPReceiver FSM: Unknown packet type from " + remoteSock);
                        break;
                }
            } catch (IOException e) {
                if (debug)
                    System.err.println("GUDPReceiver FSM IOException: " + e.getMessage());
            }

            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }

        synchronized (receiverQueue) {
            receiverQueue.clear();
        }
        synchronized (senderQueue) {
            senderQueue.clear();
        }
        System.out.println("GUDPReceiver FSM: Receiver ended");
    }

    private GUDPEndPoint findEndpoint(InetAddress addr, int port) {
        for (GUDPEndPoint ep : receiverQueue) {
            InetSocketAddress re = ep.getRemoteEndPoint();
            if (re.getAddress().equals(addr) && re.getPort() == port)
                return ep;
        }
        return null;
    }

    private void sendACK(InetSocketAddress dest, int ackSeq) throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(GUDPPacket.HEADER_SIZE);
        buf.order(ByteOrder.BIG_ENDIAN);
        GUDPPacket ack = new GUDPPacket(buf);
        ack.setType(GUDPPacket.TYPE_ACK);
        ack.setVersion(GUDPPacket.GUDP_VERSION);
        ack.setPayloadLength(0);
        ack.setSocketAddress(dest);
        ack.setSeqno(ackSeq);
        DatagramPacket dp = ack.pack();
        dp.setAddress(dest.getAddress());
        dp.setPort(dest.getPort());
        sock.send(dp);
        if (debug)
            System.out.println("SEND ACK " + ackSeq + " to " + dest);
    }
}
