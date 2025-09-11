import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.io.IOException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.Iterator;

/*
 * GUDPSender monitors send queues and sends packets whenever there are packets in the queues
 */
public class GUDPSender extends Thread {
    private final DatagramSocket sock;
    private final ConcurrentLinkedQueue<GUDPEndPoint> senderQueue;
    private boolean runFlag = true;
    private boolean debug = true; // set to print message for testing purpose

    private enum drop { // use for test dropping different type of packets
        NOTHING,
        FIRST_BSN,
        FIRST_DATA,
        FIRST_ACK,
        FIRST_FIN,
        RANDOM,
        ALL,
    }

    private drop senderDrop;

    public GUDPSender(DatagramSocket sock, ConcurrentLinkedQueue<GUDPEndPoint> senderQueue) {
        this.sock = sock;
        this.senderQueue = senderQueue;
        this.senderDrop = drop.NOTHING;
    }

    /*
     * Method to terminate the GUDPSender thread
     */
    public void stopGUDPSender() {
        this.runFlag = false;
    }

    @Override
    public void run() {
        while (runFlag) {
            // iterate snapshot-style; removal done via senderQueue.remove(ep)
            Iterator<GUDPEndPoint> iterator = senderQueue.iterator();
            while (iterator.hasNext()) {
                GUDPEndPoint ep = iterator.next();
                try {
                    FSMSender(ep);
                } catch (Exception e) {
                    // protect sender thread from unexpected exceptions
                    System.err.println("GUDPSender: exception in FSMSender: " + e.getMessage());
                    if (debug)
                        e.printStackTrace();
                }

                // if (ep.getFinished() && ep.isEmptyQueue()) {
                // boolean removed = senderQueue.remove(ep);
                // senderQueue.remove(ep)
                // if (removed && debug)
                // System.out.println("GUDPSender: removed endpoint " + ep.getRemoteEndPoint());
                // }
                if (ep.getFinished()) {
                    senderQueue.remove(ep);
                    System.out.println("GUDPSender: removed endpoint " + ep.getRemoteEndPoint());
                }
            }

            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        // After runFlag is false
        GUDPEndPoint ep;
        while ((ep = senderQueue.poll()) != null) { // removes head safely
            ep.stopTimer(); // stop timers
            if (debug)
                System.out.println("GUDPSender: removed endpoint " + ep.getRemoteEndPoint());
        }

        senderQueue.clear();

        System.out.println("GUDPSender ended");
    }

    public void FSMSender(GUDPEndPoint endPoint) {
        switch (endPoint.getState()) {
            case INIT: {
                endPoint.setState(GUDPEndPoint.endPointState.WAIT);
                break;
            }

            case WAIT: {
                // if there is work to do (new packets) or it's finished (send FINs), go to SEND
                if (!endPoint.isEmptyQueue() || (endPoint.isEmptyQueue() && endPoint.getFinished())) {
                    endPoint.setState(GUDPEndPoint.endPointState.SEND);
                } else if (endPoint.getNextseqnum() >= endPoint.getBase() + endPoint.getWindowSize()
                        || endPoint.getNextseqnum() > endPoint.getLast()) {
                    // window full or nothing to send: sleep a bit
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
                break;
            }

            case SEND: {
                /*
                 * Correct send policy:
                 * - send all packets in the send window [base, min(last, base+window-1)]
                 * This ensures BSN (at seq=base) is transmitted even when nextseqnum==base+1.
                 * - After attempting to send up to sendLimit, advance nextseqnum to sendLimit+1
                 * if it wasn't already that large.
                 */
                int base = endPoint.getBase();
                int last = endPoint.getLast();
                int windowLimit = base + endPoint.getWindowSize() - 1;
                int sendLimit = Math.min(last, windowLimit);

                for (int seq = base; seq <= sendLimit; seq++) {
                    GUDPPacket pkt = endPoint.getPacket(seq);
                    if (pkt == null) {
                        if (debug)
                            System.out.println("SEND: No packet for seq=" + seq + " (skipped)");
                        continue;
                    }

                    try {
                        DatagramPacket udpPkt = pkt.pack();
                        InetSocketAddress dest = pkt.getSocketAddress();
                        udpPkt.setAddress(dest.getAddress());
                        udpPkt.setPort(dest.getPort());
                        sock.send(udpPkt);

                        if (debug)
                            System.out.println("SEND seq=" + pkt.getSeqno() + " to " + dest +
                                    " Base=" + endPoint.getBase() + " Next=" + endPoint.getNextseqnum());

                        // start timer for the oldest unacked packet
                        if (seq == base) {
                            try {
                                endPoint.startTimer();
                            } catch (Exception ignored) {
                            }
                        }
                    } catch (IOException e) {
                        System.err.println("SEND IOException: " + e.getMessage());
                    }
                }

                // Advance nextseqnum to reflect that we've tried to send through sendLimit
                int newNext = Math.max(endPoint.getNextseqnum(), sendLimit + 1);
                endPoint.setNextseqnum(newNext);

                endPoint.setState(GUDPEndPoint.endPointState.WAIT);
                break;
            }

            case RCV: {
                if (debug)
                    System.out.println("RCV State: Base=" + endPoint.getBase() + " Next=" + endPoint.getNextseqnum());

                try {
                    if (endPoint.getBase() == endPoint.getNextseqnum() || endPoint.getBase() > endPoint.getLast()) {
                        try {
                            endPoint.stopTimer();
                        } catch (Exception ignored) {
                        }
                        endPoint.setRetry(0);
                    } else {
                        // restart timer for oldest unacked
                        try {
                            endPoint.stopTimer();
                        } catch (Exception ignored) {
                        }
                        try {
                            endPoint.startTimer();
                        } catch (Exception ignored) {
                        }
                    }
                } catch (NullPointerException ignored) {
                }
                endPoint.setState(GUDPEndPoint.endPointState.WAIT);
                break;
            }

            case TIMEOUT: {
                if (debug)
                    System.out.println("TIMEOUT: Base=" + endPoint.getBase() + " Next=" + endPoint.getNextseqnum() +
                            " Retry=" + endPoint.getRetry());

                if (endPoint.getRetry() >= endPoint.getMaxRetry()) {
                    System.out.println("GUDPSender: max retries reached for " + endPoint.getRemoteEndPoint());
                    stopGUDPSender();
                    return;
                }

                // resend all unacked packets [base, nextseqnum)
                for (int seq = endPoint.getBase(); seq < endPoint.getNextseqnum(); seq++) {
                    GUDPPacket pkt = endPoint.getPacket(seq);
                    if (pkt != null) {
                        try {
                            DatagramPacket udpPkt = pkt.pack();
                            InetSocketAddress dest = pkt.getSocketAddress();
                            udpPkt.setAddress(dest.getAddress());
                            udpPkt.setPort(dest.getPort());
                            sock.send(udpPkt);

                            if (debug)
                                System.out.println("RESEND seq=" + seq + " to " + dest);
                        } catch (IOException e) {
                            System.err.println("GUDPSender: IOException in TIMEOUT resend: " + e.getMessage());
                        }
                    }
                }

                endPoint.setRetry(endPoint.getRetry() + 1);
                try {
                    endPoint.startTimer();
                } catch (Exception ignored) {
                }
                endPoint.setState(GUDPEndPoint.endPointState.WAIT);
                break;
            }

            default:
                // unknown state -> back to WAIT
                endPoint.setState(GUDPEndPoint.endPointState.WAIT);
        }
    }
}
