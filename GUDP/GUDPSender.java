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
        /*
         * While the GUDPSender thread is active
         * iterate through each end point in the senderQueue
         * if there is a packet in the queue, send it according to the GBN sender
         * algorithm.
         * if the end point finishes sending, remove it from the senderQueue
         */
        while (runFlag) {
            /* GUDPSender thread is active */
            Iterator<GUDPEndPoint> iterator = senderQueue.iterator();
            while (iterator.hasNext()) {
                GUDPEndPoint ep = iterator.next();

                FSMSender(ep);

                if (ep.getFinished() && ep.isEmptyQueue()) {
                    senderQueue.remove(ep);
                    if (debug) {
                        System.out.println("GUDPSender: removed endpoint " + ep.getRemoteEndPoint());
                    }
                }
            }

            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        /* GUDPSender thread is inactive */
        /*
         * After the GUDPSender thread become inactive (after stopGUDPSender() was
         * called)
         * iterate through the senderQueue to remove all end points
         * This is to ensure that finish() will end and the application can terminate
         * gracefully
         */
        // cleanup after stop
        GUDPEndPoint ep;
        while ((ep = senderQueue.poll()) != null) {
            ep.stopTimer();
            if (debug) {
                System.out.println("GUDPSender: removed endpoint " + ep.getRemoteEndPoint());
            }
        }

        senderQueue.clear();
        System.out.println("GUDPSender ended");
    }

    public void FSMSender(GUDPEndPoint endPoint) {
        /*
         * FSM for GBN Sender
         * INIT: Do nothing. Progress to WAIT.
         * WAIT: If the queue is not empty or it is empty and finished, progress to
         * SEND.
         * Otherwise, if send window is full or no more packet to send, then
         * put the thread to sleep for 100 ms.
         * SEND: Send packets according to GBN algorithm while the window is not full.
         * Move to WAIT after sending packets
         * RCV: The actual packet reception (for the ACK packet) is done by the
         * GUDPReceiver.
         * Here you deal with the timer according to the GBN algorithm and then move to
         * WAIT
         * TIMEOUT: If maximum retransmission, terminate the GUDPSender thread.
         * Otherwise, resend packets and restart the timer according to GBN algorithm
         * Move to WAIT
         */
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
                    // window full or nothing to send: sleep 100 ms
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
                break;
            }

            case SEND: {
                int base = endPoint.getBase();
                int last = endPoint.getLast();
                int next = endPoint.getNextseqnum();
                int windowSize = endPoint.getWindowSize();

                // while send window not full AND nextseqnum <= last
                while ((next < base + windowSize) && (next <= last)) {
                    GUDPPacket pkt = endPoint.getPacket(next);
                    if (pkt != null) {
                        try {
                            DatagramPacket udpPkt = pkt.pack();
                            InetSocketAddress dest = pkt.getSocketAddress();
                            udpPkt.setAddress(dest.getAddress());
                            udpPkt.setPort(dest.getPort());
                            sock.send(udpPkt);

                            if (debug) {
                                System.out.println("GUDPSender: SEND seq=" + pkt.getSeqno() + " to " + dest +
                                        " Base=" + base + " Next=" + next);
                            }

                            if ((base == next) && (base <= last)) {
                                endPoint.startTimer();
                            }
                        } catch (IOException e) {
                            System.err.println("GUDPSender: SEND IOException: " + e.getMessage());
                        }
                    }

                    next++;
                    endPoint.setNextseqnum(next);
                }

                endPoint.setState(GUDPEndPoint.endPointState.WAIT);
                break;
            }

            case RCV: {
                int base = endPoint.getBase();
                int next = endPoint.getNextseqnum();
                int last = endPoint.getLast();

                if (debug) {
                    System.out.println("GUDPSender: RCV State: Base=" + base + " Next=" + next + " Last=" + last);
                }

                if (base == next || base > last) {
                    endPoint.stopTimer();
                    endPoint.setRetry(0);
                } else if (base < next) {
                    endPoint.stopTimer();
                    endPoint.startTimer();
                }

                endPoint.setState(GUDPEndPoint.endPointState.WAIT);
                break;
            }

            case TIMEOUT: {
                int base = endPoint.getBase();
                int next = endPoint.getNextseqnum();

                if (debug) {
                    System.out.println("TIMEOUT: Base=" + base + " Next=" + next +
                            " Retry=" + endPoint.getRetry());
                }

                if (endPoint.getRetry() >= endPoint.getMaxRetry()) {
                    System.out.println("GUDPSender: max retries reached for " + endPoint.getRemoteEndPoint());
                    stopGUDPSender();
                    break;
                }

                endPoint.startTimer();

                // resend all packets in window [base, next)
                for (int seq = base; seq < next; seq++) {
                    GUDPPacket pkt = endPoint.getPacket(seq);
                    if (pkt != null) {
                        try {
                            DatagramPacket udpPkt = pkt.pack();
                            InetSocketAddress dest = pkt.getSocketAddress();
                            udpPkt.setAddress(dest.getAddress());
                            udpPkt.setPort(dest.getPort());
                            sock.send(udpPkt);

                            if (debug) {
                                System.out.println("GUDPSender: RESEND seq=" + seq + " to " + dest);
                            }
                        } catch (IOException e) {
                            System.err.println("GUDPSender: TIMEOUT IOException: " + e.getMessage());
                        }
                    }
                }

                endPoint.setRetry(endPoint.getRetry() + 1);

                endPoint.setState(GUDPEndPoint.endPointState.WAIT);
                break;
            }
        }
    }
}
