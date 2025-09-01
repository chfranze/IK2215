import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.io.IOException;
import java.util.concurrent.ConcurrentLinkedQueue;

public class GUDPSocket implements GUDPSocketAPI {
    DatagramSocket datagramSocket; // a socket for sending and receiving datagram packets
    ConcurrentLinkedQueue<GUDPEndPoint> senderQueue; // the send queues, one element per destination (remoteEndPoint)
    ConcurrentLinkedQueue<GUDPEndPoint> receiverQueue; // the receive queues, one element per destination (remoteEndPoint)
    GUDPSender s; // Thread sending packets from send queues to destinations
    GUDPReceiver r; // Thread receiving packets and putting them into corresponding receive queues
    private boolean debug = true;
    private boolean startSender = false; // set to true after starting GUDPSender
    private boolean startReceiver = false; // set to true after starting GUDPReceiver

    public GUDPSocket(DatagramSocket socket) {
        datagramSocket = socket;
        /*
         * - initialize senderList and receiverList
         * - initialize s and r (NOTE: threads are not started yet)
         */
        this.senderQueue = new ConcurrentLinkedQueue<GUDPEndPoint>();
        this.receiverQueue = new ConcurrentLinkedQueue<GUDPEndPoint>();
        this.s = new GUDPSender(socket, senderQueue);
        this.s.setName("GUDPSender");
        this.r = new GUDPReceiver(socket, receiverQueue, s, senderQueue);
        this.r.setName("GUDPReceiver");
    }

    public void send(DatagramPacket packet) throws IOException {
        /*
         * three lines below let send() function sends packets out on the socket
         * directly.
         * You can use it to test the communication of the skeleton code.
         * However, you must comment out these three line and write your own code
         * to let GUDPSender thread send packets instead of send()
         */
        GUDPPacket gudppacket = GUDPPacket.encapsulate(packet);
        DatagramPacket udppacket = gudppacket.pack();
        datagramSocket.send(udppacket);

        /*
         * if the packet is a new transmission of non-existing end point,
         * create and add a new endPoint to the senderQueue,
         * also create and add GUDP BSN packet with a random sequence number
         */

        /*
         * At this point, senderQueue has an endPoint for this packet.
         * Create and add the packet as GUDP DATA packet to the endPoint
         */

        /*
         * start the GUDPSender thread if it is not running (i.e, startSender is false),
         * also start the GUDPReceiver thread if it is not running (startReceiver is
         * false)
         * since we need it for handling the ACK
         * after that set startSender and startReceiver to true
         */
    }

    public void receive(DatagramPacket packet) throws IOException {
        /*
         * five lines below let receive() function receives packet directly from the
         * socket
         * You can use it to test the communication of the skeleton code.
         * However, you must comment out these five lines and write your own code
         * to let GUDPReceiver thread receives packets instead of receive()
         */
        byte[] buf = new byte[GUDPPacket.MAX_DATAGRAM_LEN];
        DatagramPacket udppacket = new DatagramPacket(buf, buf.length);
        datagramSocket.receive(udppacket);
        GUDPPacket gudppacket = GUDPPacket.unpack(udppacket);
        gudppacket.decapsulate(packet);

        /*
         * start the GUDPReceiver thread if it is not running (i.e, startReceiver is
         * false)
         * after that set startReceiver to true
         */

        /*
         * iterate through the receiverQueue to fetch a packet from an exsiting end
         * point
         * If no packet in the receiverQueue, put the thread to sleep (e.g., for 50ms)
         * to wait for a packet to arrive before iterating through the receiverQueue
         * again
         */
    }

    public void finish() throws IOException {
        /*
         * Called after all data are sent
         * Create a FIN packet for every remote end point
         *
         * Then, wait for the GUDPSender thread to finish sending all packets
         * You can put main thread to sleep for 1 seconds and periodically check
         * whether the GUDPSender thread has sent all packets
         */
    }

    public void close() throws IOException {
        /*
         * called after finish() is completed
         * terminate GUDP gracefully by stopping sender and receiver threads and close
         * the socket
         */
    }

}
