import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.io.IOException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.net.InetSocketAddress;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Iterator;

/*
 * GUDPReceiver is a non-static nested class (inner class)
 * GUDPReceiver receives incoming packets from remote hosts and put them in the receiverQueue.
 */
@SuppressWarnings("unused")
public class GUDPReceiver extends Thread {
    private final DatagramSocket sock;
    private final ConcurrentLinkedQueue<GUDPEndPoint> receiverQueue;
    private final GUDPSender s;
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
    private drop receiverDrop;

    public GUDPReceiver(DatagramSocket sock, ConcurrentLinkedQueue<GUDPEndPoint> receiverQueue, GUDPSender s, ConcurrentLinkedQueue<GUDPEndPoint> senderQueue) {
            this.sock = sock;
            this.receiverQueue = receiverQueue;
	    this.s = s;
            this.senderQueue = senderQueue;
	    this.senderDrop = drop.NOTHING;
	    this.receiverDrop = drop.NOTHING;
    }

    public void stopGUDPReceiver() {
        this.runFlag = false;
    }

    @Override
    public void run() {
        while (this.runFlag) {
            /* 
	     * This is a loop that continously runs until the GUDPReceiver thread is terminated.
	     *
	     * Receive incoming packets and process each packet according to the type of packets
	     * ACK: Receive ACK as a part of GBN sender logic. Remove all ACKed packets from senderQueue.
	     *      Progress to the RCV state for this end point and call FSMSender.
	     * BSN: Create a new end point if it does not exist. Then, add BSN to its receive queue and send ACK.
	     *      If the existing end point was already finished. Reset the end point and add the BSN and send ACK.
	     *      Otherwise, just send ACK with the expected sequence number.
	     * DATA: If DATA packet with expected sequence number, add it to receive queue and send ACK.
	     *       Otherwise, just send ACK with the expected sequence number.
	     *       (If DATA packet for non-existing remoteEndPoint arrives, do nothing)
	     * FIN: Same as DATA. But also set "finished" to true to indicate file reception is completed.
	     *
	     * The teacher's code also put the GUDPReceiver thread to sleep for 50 ms at the end of the loop.
	     * This is to reduce the chance of inconsistency due to GUDPSender and GUDPReceiver accessing 
	     * the senderQueue using iterator 
	     *
	     * IMPORTANT:
	     * The receiver may drop a new BSN that is sent to the already finished end point.
	     * To simplify the process, before dropping the new BSN, the teacher code's will reset the end point 
	     * if the new BSN is outside the prevous expectedseqnum's range (i.e., expected seqnum +/- window size).
	     * It also set the new expectedseqnum to the new BSN before dropping the new BSN.
	     */
            byte[] buf = new byte[GUDPPacket.MAX_DATAGRAM_LEN];
            DatagramPacket udppacket = new DatagramPacket(buf, buf.length);
            try {
                this.sock.receive(udppacket);
	    } catch (IOException e) {
                System.err.println("GUDPReceiver IOException: probably because of socket closed");
		break;
            }

            try {
                GUDPPacket gudppacket = GUDPPacket.unpack(udppacket);
                InetSocketAddress sock = gudppacket.getSocketAddress();
                GUDPEndPoint endPoint;

                switch (gudppacket.getType()) {
	            case GUDPPacket.TYPE_ACK:
                    case GUDPPacket.TYPE_BSN:
                    case GUDPPacket.TYPE_DATA:
                    case GUDPPacket.TYPE_FIN:
	        } /* switch */

	    } catch (IOException e) {
                System.err.println("IOException in GUDPReceiver: GUDPPacket.unpack");
                e.printStackTrace();
            }

	    /* The teacher's code also put the GUDPReceiver thread to sleep for 50 ms at the end of the loop.
	     * This is to reduce the chance of inconsistency due to GUDPSender and GUDPReceiver accessing 
	     * the senderQueue using iterator 
	     */

	} /* while (this.runFlag) */

        /* 
	 * After the GUDPReceiver thread become inactive (after stopGUDPReceiver() was called)
         * In this case, after the GUDPReceiver thread become inactive (after stopGUDPReceiver() was called)
         * iterate through the receiverQueue to remove all end points
         * This is to ensure that finish() will end and the application can terminate gracefully
         */

	System.out.println("GUDPReceiver ended");
    } /* public void run() */

} /* public class GUDPReceiver */

