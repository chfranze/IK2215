import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.io.IOException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.Random;
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
            boolean anyWorkDone = false;

            Iterator<GUDPEndPoint> iterator = senderQueue.iterator();
            while (iterator.hasNext()) {
                GUDPEndPoint ep = iterator.next();

                int oldBase = ep.getBase();
                int oldNext = ep.getNextseqnum();

                // if (debug) {
                //     System.out.println("FSMSender: Endpoint " + ep.getRemoteEndPoint() +
                //             " Base=" + oldBase + " Next=" + oldNext +
                //             " Window=" + ep.getWindowSize() + " QueueSize=" + ep.queueSize() +
                //             " Retry=" + ep.getRetry());
                // }

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
        /* GUDPSender thread is inactive */

        /*
         * After the GUDPSender thread become inactive (after stopGUDPSender() was
         * called)
         * iterate through the senderQueue to remove all end points
         * This is to ensure that finish() will end and the application can terminate
         * gracefully
         */

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
                // change endpoint.state to WAIT
                endPoint.setState(GUDPEndPoint.endPointState.WAIT);
                break;
            }

            case WAIT: {
                /*
                 * if (endpoint send queue is not empty) OR (if send queue is empty AND
                 * finished), change endpoint.state to SEND
                 * if endpoint send window is full OR nextseqnum > last
                 * put the thread to sleep for 100 ms
                 */
                if (!endPoint.isEmptyQueue() || (endPoint.isEmptyQueue() && endPoint.getFinished())) {
                    endPoint.setState(GUDPEndPoint.endPointState.SEND);
                } else if (endPoint.getNextseqnum() >= endPoint.getBase() + endPoint.getWindowSize()
                        || endPoint.getNextseqnum() > endPoint.getLast()) {
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
                 * /
                 * while send window is not full AND nextseqnum <= last
                 * send a packet
                 * if base = nextseqnum AND base <= last, start timer
                 * nextseqnum++
                 * change endpoint.state to WAIT
                 */
                int base = endPoint.getBase();
                int next = endPoint.getNextseqnum();

                for (int seq = base; seq < next && seq < base + endPoint.getWindowSize(); seq++) {
                    GUDPPacket pkt = endPoint.getPacket(seq);
                    if (pkt == null) {
                        if (debug)
                            System.out.println("SEND: No packet for seq=" + seq);
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

                        if (seq == base)
                            endPoint.startTimer(); // first in window
                    } catch (IOException e) {
                        System.err.println("GUDPSender: IOException in SEND: " + e.getMessage());
                    }
                }
                endPoint.setState(GUDPEndPoint.endPointState.WAIT);
                break;
            }

            case RCV: {
                /*
                 * if base = nextseqnum OR base > last, stop timer; retry=0
                 * else if base < nextseqnum, restart timer
                 * change endpoint.state to WAIT
                 */
                if (debug)
                    System.out.println("RCV State: Base=" + endPoint.getBase() + " Next=" + endPoint.getNextseqnum());

                try {
                    if (endPoint.getBase() == endPoint.getNextseqnum()) {
                        endPoint.stopTimer();
                        endPoint.setRetry(0);
                    } else {
                        endPoint.stopTimer();
                        endPoint.startTimer();
                    }
                } catch (NullPointerException ignored) {
                }
                endPoint.setState(GUDPEndPoint.endPointState.WAIT);
                break;
            }

            case TIMEOUT: {
                /*
                 * if retry >= max_retry, stop GUDPSender
                 * start timer
                 * resend all packets that have not been ACKed
                 * retry++; change endpoint.state to WAIT
                 */
                if (debug)
                    System.out.println("TIMEOUT: Base=" + endPoint.getBase() + " Next=" + endPoint.getNextseqnum() +
                            " Retry=" + endPoint.getRetry());

                if (endPoint.getRetry() >= endPoint.getMaxRetry()) {
                    System.out.println("GUDPSender: max retries reached for " + endPoint.getRemoteEndPoint());
                    stopGUDPSender();
                    return;
                }

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
                endPoint.startTimer();
                endPoint.setState(GUDPEndPoint.endPointState.WAIT);
                break;
            }
        }
    }
}
