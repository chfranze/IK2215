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
        while (runFlag) {
            boolean anyWorkDone = false;

            Iterator<GUDPEndPoint> iterator = senderQueue.iterator();
            while (iterator.hasNext()) {
                GUDPEndPoint ep = iterator.next();

                int oldBase = ep.getBase();
                int oldNext = ep.getNextseqnum();

                if (debug) {
                    System.out.println("FSMSender: Endpoint " + ep.getRemoteEndPoint() +
                            " Base=" + oldBase + " Next=" + oldNext +
                            " Window=" + ep.getWindowSize() + " QueueSize=" + ep.queueSize() +
                            " Retry=" + ep.getRetry());
                }

                FSMSender(ep);

                if (ep.getBase() != oldBase) {
                    anyWorkDone = true; // Base moved, progress made
                    if (debug)
                        System.out.println("ACK progress: Base moved from " + oldBase + " to " + ep.getBase());
                }

                if (ep.getFinished() && ep.isEmptyQueue()) {
                    iterator.remove();
                    if (debug)
                        System.out.println("GUDPSender: removed endpoint to " + ep.getRemoteEndPoint());
                }
            }

            try {
                // If no progress, sleep longer to avoid repeated sends
                Thread.sleep(anyWorkDone ? 10 : 50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        senderQueue.clear();
        System.out.println("GUDPSender ended");
    }

    public void FSMSender(GUDPEndPoint ep) {
        try {
            switch (ep.getState()) {
                case INIT -> ep.setState(GUDPEndPoint.endPointState.WAIT);

                case WAIT -> {
                    if (!ep.isEmptyQueue())
                        ep.setState(GUDPEndPoint.endPointState.SEND);
                }

                case SEND -> {
                    int base = ep.getBase();
                    int next = ep.getNextseqnum();

                    for (int seq = base; seq < next && seq < base + ep.getWindowSize(); seq++) {
                        GUDPPacket pkt = ep.getPacket(seq);
                        if (pkt == null) {
                            if (debug)
                                System.out.println("SEND: No packet for seq=" + seq);
                            continue;
                        }

                        DatagramPacket udpPkt = pkt.pack();
                        InetSocketAddress dest = pkt.getSocketAddress();
                        udpPkt.setAddress(dest.getAddress());
                        udpPkt.setPort(dest.getPort());
                        sock.send(udpPkt);

                        if (debug)
                            System.out.println("SEND seq=" + pkt.getSeqno() + " to " + dest +
                                    " Base=" + ep.getBase() + " Next=" + ep.getNextseqnum());

                        if (seq == base)
                            ep.startTimer(); // first in window
                    }
                    ep.setState(GUDPEndPoint.endPointState.WAIT);
                }

                case RCV -> {
                    if (debug)
                        System.out.println("RCV State: Base=" + ep.getBase() + " Next=" + ep.getNextseqnum());

                    try {
                        if (ep.getBase() == ep.getNextseqnum()) {
                            ep.stopTimer();
                            ep.setRetry(0);
                        } else {
                            ep.stopTimer();
                            ep.startTimer();
                        }
                    } catch (NullPointerException ignored) {
                    }
                    ep.setState(GUDPEndPoint.endPointState.WAIT);
                }

                case TIMEOUT -> {
                    if (debug)
                        System.out.println("TIMEOUT: Base=" + ep.getBase() + " Next=" + ep.getNextseqnum() +
                                " Retry=" + ep.getRetry());

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
                }
            }
        } catch (IOException e) {
            System.err.println("GUDPSender: IOException in FSMSender: " + e.getMessage());
        }
    }
}
