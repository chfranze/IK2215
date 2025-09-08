import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.io.IOException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.Iterator;

public class GUDPSender extends Thread {
    private final DatagramSocket sock;
    private final ConcurrentLinkedQueue<GUDPEndPoint> senderQueue;
    private boolean runFlag = true;
    private boolean debug = true;

    public GUDPSender(DatagramSocket sock, ConcurrentLinkedQueue<GUDPEndPoint> senderQueue) {
        this.sock = sock;
        this.senderQueue = senderQueue;
    }

    public void stopGUDPSender() {
        this.runFlag = false;
    }

    @Override
    public void run() {
        while (this.runFlag) {
            Iterator<GUDPEndPoint> iterator = senderQueue.iterator();
            while (iterator.hasNext()) {
                GUDPEndPoint ep = iterator.next();
                FSMSender(ep);
                if (ep.getFinished() && ep.isEmptyQueue()) {
                    iterator.remove();
                    if (debug) {
                        System.out.println("GUDPSender: removed endpoint to " + ep.getRemoteEndPoint());
                    }
                }
            }

            try {
                Thread.sleep(10); // small sleep to avoid busy loop
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        }

        // cleanup
        Iterator<GUDPEndPoint> iterator = senderQueue.iterator();
        while (iterator.hasNext()) {
            GUDPEndPoint ep = iterator.next();
            iterator.remove();
            if (debug) {
                System.out.println("GUDPSender: removed endpoint to " + ep.getRemoteEndPoint());
            }
        }

        System.out.println("GUDPSender ended");
    }

    public void FSMSender(GUDPEndPoint ep) {
        try {
            switch (ep.getState()) {
                case INIT:
                    ep.setState(GUDPEndPoint.endPointState.WAIT);
                    break;

                case WAIT:
                    if (!ep.isEmptyQueue()) {
                        ep.setState(GUDPEndPoint.endPointState.SEND);
                    }
                    break;

                case SEND:
                    // Send all packets in window
                    int base = ep.getBase();
                    int next = ep.getNextseqnum();
                    for (int seq = base; seq < next && seq < base + ep.getWindowSize(); seq++) {
                        GUDPPacket pkt = ep.getPacket(seq);
                        if (pkt == null)
                            continue;

                        DatagramPacket udpPkt = pkt.pack();
                        InetSocketAddress dest = pkt.getSocketAddress();
                        udpPkt.setAddress(dest.getAddress()); // <--- crucial fix
                        udpPkt.setPort(dest.getPort()); // <--- crucial fix

                        sock.send(udpPkt);

                        if (debug)
                            System.out.println("SEND seq=" + pkt.getSeqno() + " to " + dest);

                        if (seq == base)
                            ep.startTimer(); // first in window
                    }
                    ep.setState(GUDPEndPoint.endPointState.WAIT);
                    break;

                case RCV:
                    // Timer management
                    if (ep.getBase() == ep.getNextseqnum()) {
                        try {
                            ep.stopTimer(); // may throw NPE
                        } catch (NullPointerException ignored) {
                        }
                        ep.setRetry(0);
                    } else {
                        try {
                            ep.stopTimer();
                            ep.startTimer();
                        } catch (NullPointerException ignored) {
                            // first-time timer will be started later by startTimer()
                        }
                    }
                    ep.setState(GUDPEndPoint.endPointState.WAIT);
                    break;

                case TIMEOUT:
                    if (ep.getRetry() >= ep.getMaxRetry()) {
                        System.out.println("GUDPSender: max retries reached for " + ep.getRemoteEndPoint());
                        stopGUDPSender();
                        return;
                    }

                    for (int seq = ep.getBase(); seq < ep.getNextseqnum(); seq++) {
                        GUDPPacket pkt = ep.getPacket(seq);
                        if (pkt != null) {
                            DatagramPacket udpPkt = pkt.pack();
                            InetSocketAddress dest = pkt.getSocketAddress();
                            udpPkt.setAddress(dest.getAddress());
                            udpPkt.setPort(dest.getPort());
                            sock.send(udpPkt);

                            if (debug)
                                System.out.println("RESEND seq=" + seq + " to " + dest);
                        }
                    }

                    ep.setRetry(ep.getRetry() + 1);
                    ep.startTimer();
                    ep.setState(GUDPEndPoint.endPointState.WAIT);
                    break;
            }
        } catch (IOException e) {
            System.err.println("GUDPSender: IOException in FSMSender: " + e.getMessage());
        }
    }
}
