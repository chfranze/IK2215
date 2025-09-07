import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.io.IOException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.Random;
import java.util.Iterator;

/*
 * GUDPSender monitors send queues and sends packets whenever there are packets in the queues
 */
@SuppressWarnings("unused")
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
        while (this.runFlag) {
            // Use iterator to safely remove elements while iterating
            Iterator<GUDPEndPoint> iterator = senderQueue.iterator();
            while (iterator.hasNext()) {
                GUDPEndPoint ep = iterator.next();
                FSMSender(ep);
                if (ep.getFinished() && ep.isEmptyQueue()) {
                    iterator.remove();
                    if (debug) {
                        System.out.println("GUDPSender: removed endpoint to " + ep.getRemoteEndPoint().toString());
                    }
                }
            }
        }

        /*
         * After the GUDPSender thread becomes inactive (after stopGUDPSender() was
         * called)
         * iterate through the senderQueue to remove all end points
         * This is to ensure that finish() will end and the application can terminate
         * gracefully
         */
        Iterator<GUDPEndPoint> iterator = senderQueue.iterator();
        while (iterator.hasNext()) {
            GUDPEndPoint ep = iterator.next();
            iterator.remove();
            if (debug) {
                System.out.println("GUDPSender: removed endpoint to " + ep.getRemoteEndPoint().toString());
            }
        }

        System.out.println("GUDPSender ended");
    }

    @SuppressWarnings("incomplete-switch")
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

        GUDPPacket gudpPacket;
        DatagramPacket udppacket;
        try {
            switch (endPoint.getState()) {
                case INIT:
                    /* EndPoint initalizes with INIT state */
                    // base = Random()
                    // nextsequennum = base
                    // I think the above things are done in Socket class when BSN packet is created
                    endPoint.setState(GUDPEndPoint.endPointState.WAIT);
                    break;
                case WAIT:
                    /*
                     * If the queue is not empty or it is empty and finished, progress to SEND.
                     * Otherwise, if send window is full or no more packet to send, then
                     * put the thread to sleep for 100 ms.
                     */
                    if (!endPoint.isEmptyQueue() || (endPoint.isEmptyQueue() && endPoint.getFinished())) {
                        endPoint.setState(GUDPEndPoint.endPointState.SEND);
                    } else if (endPoint.getWindowSize() >= GUDPEndPoint.MAX_WINDOW_SIZE || endPoint.getNextseqnum() > endPoint.getLast()) {
                        try { Thread.sleep(100); } catch (InterruptedException ignored) {}
                    }
                    break;
                case SEND:
                    /*
                     * while send window is not full AND nextseqnum <= last
                     * send a packet
                     * if base = nextseqnum AND base <= last, start timer
                     * nextseqnum++
                     * change endpoint.state to WAIT
                     */
                    while ((endPoint.getWindowSize() < GUDPEndPoint.MAX_WINDOW_SIZE) && (endPoint.getNextseqnum() <= endPoint.getLast())) {
                        gudpPacket = endPoint.getPacket(endPoint.getNextseqnum());
                        if (gudpPacket != null) {
                            udppacket = gudpPacket.pack();
                            sock.send(udppacket); // send a packet with nextseqnum

                            if (endPoint.getBase() == endPoint.getNextseqnum() && endPoint.getBase() <= endPoint.getLast()) {
                                endPoint.startTimer(); // start timer for first packet in window
                            }
                        }
                        endPoint.setNextseqnum(endPoint.getNextseqnum() + 1); // advance sequence number
                        // Keep state as WAIT (if state is set elsewhere, this may be redundant)
                        endPoint.setState(GUDPEndPoint.endPointState.WAIT);
                    }
                    break;
                case RCV:
                    if (endPoint.getBase() == endPoint.getNextseqnum() || endPoint.getBase() > endPoint.getLast()) {
                        endPoint.stopTimer();
                        endPoint.setRetry(0);
                    } else if (endPoint.getBase() < endPoint.getNextseqnum()) {
                        // Restart timer
                        endPoint.stopTimer();   
                        endPoint.startTimer();
                    }
                    endPoint.setState(GUDPEndPoint.endPointState.WAIT);
                    break;
                case TIMEOUT:
                    if(endPoint.getRetry() >= endPoint.getMaxRetry()){
                        System.out.println("GUDPSender: reached maximum retry, terminating GUDPSender thread");
                        stopGUDPSender();
                        return;
                    }
                    endPoint.startTimer();
                    // resend all packets in the window
                    for (int seq = endPoint.getBase(); seq < endPoint.getNextseqnum(); seq++) {
                        gudpPacket = endPoint.getPacket(seq);
                        if (gudpPacket != null) {
                            udppacket = gudpPacket.pack();
                            sock.send(udppacket); 
                        }
                    }
                    endPoint.setRetry(endPoint.getRetry() + 1);
                    endPoint.setState(GUDPEndPoint.endPointState.WAIT);
                    break;
            }
        } catch (IOException e) {
            System.err.println("GUDPSender: IOException in FSMSender: " + e.getMessage());
        }
    }

}