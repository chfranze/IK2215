import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;

//@SuppressWarnings("unused")
public class GUDPSocket implements GUDPSocketAPI {
    DatagramSocket datagramSocket; // a socket for sending and receiving datagram packets
    ConcurrentLinkedQueue<GUDPEndPoint> senderQueue; // the send queues, one element per destination (remoteEndPoint)
    ConcurrentLinkedQueue<GUDPEndPoint> receiverQueue; // the receive queues, one element per destination
                                                       // (remoteEndPoint)
    GUDPSender s; // Thread sending packets from send queues to destinations
    GUDPReceiver r; // Thread receiving packets and putting them into corresponding receive queues
    private boolean debug = true;
    private boolean startSender = false; // set to true after starting GUDPSender
    private boolean startReceiver = false; // set to true after starting GUDPReceiver

    // Sleep time for receive() when no packet is in the receiverQueue
    private final int SLEEP_TIME = 50;

    private final int FIN_TIMEOUT = 20000; // Timeout for finish() when waiting for all FINs to be ACKed

    // Sleep time for finish() when waiting for GUDPSender to finish sending all
    private final int FIN_WAIT_SLEEP = 1000;

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
        // GUDPPacket gudppacket = GUDPPacket.encapsulate(packet);
        // DatagramPacket udppacket = gudppacket.pack();
        // datagramSocket.send(udppacket);

        /*
         * if the packet is a new transmission of non-existing end point,
         * create and add a new endPoint to the senderQueue,
         * also create and add GUDP BSN packet with a random sequence number
         */

        InetAddress addr = packet.getAddress();
        int port = packet.getPort();
        GUDPEndPoint endPoint = null;

        for (GUDPEndPoint ep : senderQueue) {
            if (ep.getRemoteEndPoint().getAddress().equals(addr) && ep.getRemoteEndPoint().getPort() == port) {
                endPoint = ep;
                break;
            }
        }

        if (endPoint == null) {
            endPoint = new GUDPEndPoint(addr, port);
            senderQueue.add(endPoint);

            // Create BSN packet the same way as in GUDPPacket.encapsulate()
            ByteBuffer buffer = ByteBuffer.allocate(GUDPPacket.HEADER_SIZE);
            buffer.order(ByteOrder.BIG_ENDIAN);
            GUDPPacket bsnPacket = new GUDPPacket(buffer);
            bsnPacket.setType(GUDPPacket.TYPE_BSN);
            bsnPacket.setVersion(GUDPPacket.GUDP_VERSION);
            bsnPacket.setPayloadLength(0);
            bsnPacket.setSocketAddress(new InetSocketAddress(addr, port));

            // Sequence number should be random and not zero or one
            Random rand = new Random();
            int bsn = 2 + rand.nextInt(Short.MAX_VALUE - 2);
            bsnPacket.setSeqno(bsn);

            // Add the BSN packet to the endpoint's queue
            endPoint.add(bsnPacket);
        }

        /*
         * At this point, senderQueue has an endPoint for this packet.
         * Create and add the packet as GUDP DATA packet to the endPoint,
         */

        // Here create DATA packet that uses the encapsulate() function
        GUDPPacket dataPacket = GUDPPacket.encapsulate(packet);
        endPoint.add(dataPacket);

        /*
         * start the GUDPSender thread if it is not running (i.e, startSender is false),
         * also start the GUDPReceiver thread if it is not running (startReceiver is
         * false)
         * since we need it for handling the ACK
         * after that set startSender and startReceiver to true
         */
        if (startSender == false) {
            s.start();
            startSender = true;
        }
        if (startReceiver == false) {
            r.start();
            startReceiver = true;
        }
    }

    public void receive(DatagramPacket packet) throws IOException {
        /*
         * five lines below let receive() function receives packet directly from the
         * socket
         * You can use it to test the communication of the skeleton code.
         * However, you must comment out these five lines and write your own code
         * to let GUDPReceiver thread receives packets instead of receive()
         */
        // byte[] buf = new byte[GUDPPacket.MAX_DATAGRAM_LEN];
        // DatagramPacket udppacket = new DatagramPacket(buf, buf.length);
        // datagramSocket.receive(udppacket);
        // GUDPPacket gudppacket = GUDPPacket.unpack(udppacket);
        // gudppacket.decapsulate(packet);

        /*
         * start the GUDPReceiver thread if it is not running (i.e, startReceiver is
         * false)
         * after that set startReceiver to true
         */

        if (startReceiver == false) {
            r.start();
            startReceiver = true;
        }

        /*
         * iterate through the receiverQueue to fetch a packet from an exsiting end
         * point
         * If no packet in the receiverQueue, put the thread to sleep (e.g., for 50ms)
         * to wait for a packet to arrive before iterating through the receiverQueue
         * again
         */

        while (true) {
            boolean foundPacket = false;

            for (GUDPEndPoint ep : receiverQueue) {
                if (ep == null)
                    continue;

                GUDPPacket gpacket = ep.remove(); // try to get one
                if (gpacket != null) {
                    gpacket.decapsulate(packet);
                    foundPacket = true;
                    break;
                }
            }

            if (foundPacket) {
                break; // exit once a packet is found
            }

            try {
                Thread.sleep(SLEEP_TIME); // wait before retry
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    public void finish() throws IOException {
        /*
         * Called after all data are sent
         * Create a FIN packet for every remote end point
         */

        for (GUDPEndPoint ep : senderQueue) {
            // Get the address and port of the remote end point
            InetAddress addr = ep.getRemoteEndPoint().getAddress();
            int port = ep.getRemoteEndPoint().getPort();

            // Create FIN packet the same way as in GUDPPacket.encapsulate()
            ByteBuffer buffer = ByteBuffer.allocate(GUDPPacket.HEADER_SIZE);
            buffer.order(ByteOrder.BIG_ENDIAN);
            GUDPPacket finPacket = new GUDPPacket(buffer);
            finPacket.setType(GUDPPacket.TYPE_FIN);
            finPacket.setVersion(GUDPPacket.GUDP_VERSION);
            finPacket.setPayloadLength(0);
            finPacket.setSocketAddress(new InetSocketAddress(addr, port));

            // Add the FIN packet to the endpoint's queue
            ep.add(finPacket);
        }

        /*
         * Then, wait for the GUDPSender thread to finish sending all packets
         * You can put main thread to sleep for 1 seconds and periodically check
         * whether the GUDPSender thread has sent all packets
         */

        long start = System.currentTimeMillis();
        boolean allDone;

        do {
            try {
                Thread.sleep(1000); // initial 1-second sleep
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Finish interrupted", e);
            }

            allDone = true;

            for (GUDPEndPoint ep : senderQueue) {
                if (!ep.isEmptyQueue()) {
                    allDone = false;
                    break;
                }
            }

            if (System.currentTimeMillis() - start > FIN_TIMEOUT) {
                throw new IOException("FIN waiting for ACK timeout");
            }

        } while (!allDone);

        // Cleanup
        for (GUDPEndPoint ep : senderQueue) {
            ep.setFinished(true);
            ep.clear();
        }
        senderQueue.clear();
    }

    public void close() throws IOException {
        /*
         * called after finish() is completed
         * terminate GUDP gracefully by stopping sender and receiver threads and close
         * the socket
         */

        s.stopGUDPSender();
        startSender = false;

        r.stopGUDPReceiver();
        startReceiver = false;

        datagramSocket.close();
    }

}
