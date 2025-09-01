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
    private boolean debug = true;  // set to print message for testing purpose
    private enum drop {  // use for test dropping different type of packets
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
         * if there is a packet in the queue, send it according to the GBN sender algorithm. 
         * if the end point finishes sending, remove it from the senderQueue
	 */
	while (this.runFlag) {
            /* GUDPSender thread is active */
        }

            /* GUDPSender thread is inactive */
        /* 
	 * After the GUDPSender thread become inactive (after stopGUDPSender() was called)
         * iterate through the senderQueue to remove all end points
         * This is to ensure that finish() will end and the application can terminate gracefully
	 */

	System.out.println("GUDPSender ended");
    } /* public void run() */

    @SuppressWarnings("incomplete-switch")
    public void FSMSender(GUDPEndPoint endPoint) {
        /* 
	 * FSM for GBN Sender
	 * INIT: Do nothing. Progress to WAIT.
	 * WAIT: If the queue is not empty or it is empty and finished, progress to SEND. 
	 *     Otherwise, if send window is full or no more packet to send, then 
	 *     put the thread to sleep for 100 ms.
	 * SEND: Send packets according to GBN algorithm while the window is not full. 
	 *     Move to WAIT after sending packets
	 * RCV:  The actual packet reception (for the ACK packet) is done by the GUDPReceiver.
	 *     Here you deal with the timer according to the GBN algorithm and then move to WAIT
	 * TIMEOUT: If maximum retransmission, terminate the GUDPSender thread.
	 *     Otherwise, resend packets and restart the timer according to GBN algorithm
	 *     Move to WAIT
	 */

        DatagramPacket udppacket;
	switch (endPoint.getState()) {
        case INIT:
            endPoint.setState(GUDPEndPoint.endPointState.WAIT);
        case WAIT:
        case SEND:
        case RCV:
        case TIMEOUT:
	}

    } /* public void FSMSender(GUDPEndPoint endPoint) */

} /* public class GUDPSender */

