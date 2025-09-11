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

        InetSocketAddress remoteEndPoint = (InetSocketAddress) packet.getSocketAddress();
        GUDPEndPoint endPoint = findSenderEndPoint(remoteEndPoint);

        /*
         * if the packet is a new transmission of non-existing end point,
         * create and add a new endPoint to the senderQueue,
         * also create and add GUDP BSN packet with a random sequence number
         */

        if (endPoint == null) {
            // New endpoint
            endPoint = new GUDPEndPoint(remoteEndPoint.getAddress(), remoteEndPoint.getPort());

            if (debug) {
                System.out.println("GUDPSocket: Created new endpoint for " + remoteEndPoint.toString());
            }

            // // Sequence number should be random and not zero or one
            Random rand = new Random();
            int bsn = 2 + rand.nextInt(Short.MAX_VALUE - 2);

            // bsnPacket.setSeqno(bsn);
            endPoint.setBase(bsn);
            endPoint.setNextseqnum(bsn); // initial data packet will be BSN + 1
            endPoint.setLast(bsn); // No DATA packets yet



            // Create BSN packet
            GUDPPacket bsnPacket = createPacket(GUDPPacket.TYPE_BSN, bsn, remoteEndPoint);
            endPoint.add(bsnPacket);

            if (debug) {
                System.out.println("GUDPSocket: Created BSN packet with SEQ: " + bsn);
                System.out.println("GUDPSocket: Endpoint state after BSN -> base=" + endPoint.getBase()
                        + ", nextseqnum=" + endPoint.getNextseqnum()
                        + ", last=" + endPoint.getLast());
            }

            // Add endpoint to sender queue
            senderQueue.add(endPoint);
        }

        /*
         * At this point, senderQueue has an endPoint for this packet.
         * Create and add the packet as GUDP DATA packet to the endPoint,
         */

        // data packet encaptulation
        GUDPPacket gudpDataPacket = GUDPPacket.encapsulate(packet);
        // gudpDataPacket.setSeqno(endPoint.getNextseqnum());
        gudpDataPacket.setSeqno(endPoint.getLast() + 1);

        endPoint.setLast(gudpDataPacket.getSeqno());
        endPoint.add(gudpDataPacket);
        endPoint.setState(GUDPEndPoint.endPointState.SEND);

        // endPoint.setLast(endPoint.getNextseqnum());
        // endPoint.setNextseqnum(endPoint.getNextseqnum() + 1);

        if (debug) {
            System.out.println("GUDPSocket: Added DATA packet with SEQ: " + gudpDataPacket.getSeqno());
            System.out.println("GUDPSocket: Endpoint state after DATA -> base=" + endPoint.getBase()
                    + ", nextseqnum=" + endPoint.getNextseqnum()
                    + ", last=" + endPoint.getLast());
        }

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
            if (debug) {
                System.out.println("GUDPSocket: Started GUDPSender thread");
            }
        }

        if (startReceiver == false) {
            r.start();
            startReceiver = true;
            if (debug) {
                System.out.println("GUDPSocket: Started GUDPReceiver thread");
            }
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

                    // Only deliver DATA packets to the application
                    if (gpacket.getType() != GUDPPacket.TYPE_DATA) {
                        if (debug) {
                            System.out.println("GUDPSocket: Skipping non-DATA packet (type="
                                    + gpacket.getType() + ", seq=" + gpacket.getSeqno()
                                    + ") from " + ep.getRemoteEndPoint());
                        }
                        continue; // skip BSN, FIN, ACK
                    }

                    if (debug) {
                        System.out.println("GUDPSocket: Delivering DATA packet with SEQ: "
                                + gpacket.getSeqno() + " from " + ep.getRemoteEndPoint());
                    }

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

            // Create FIN packet
            int finSeq = ep.getLast() + 1;
            GUDPPacket finPacket = createPacket(GUDPPacket.TYPE_FIN, finSeq, ep.getRemoteEndPoint());
            ep.add(finPacket);
            ep.setLast(finSeq);
            //ep.setNextseqnum(finSeq + 1);
            ep.setState(GUDPEndPoint.endPointState.SEND);

            if (debug) {
                System.out.println("GUDPSocket: Created FIN packet with SEQ: " + finPacket.getSeqno() + " for endpoint "
                        + ep.getRemoteEndPoint().toString());
            }
        }

        /*
         * Wait for the GUDPSender thread to finish sending all packets
         * Sleep 1s and periodically check whether senderQueue is empty
         */
        long start = System.currentTimeMillis();
        boolean allDone;

        do {
            try {
                Thread.sleep(1000);

                if (debug) {
                    System.out.println("GUDPSocket: Waiting for GUDPSender to finish sending all packets...");
                }

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Finish interrupted", e);
            }

            allDone = true;
            for (GUDPEndPoint ep : senderQueue) {
                if (!ep.isEmptyQueue()) {
                    if (debug) {
                        System.out.println("GUDPSocket: Still waiting for endpoint " +
                                ep.getRemoteEndPoint().toString() + " to finish sending packets...");
                    }
                    allDone = false;
                    break;
                }
            }

            if (System.currentTimeMillis() - start > FIN_TIMEOUT) {
                throw new IOException("FIN waiting for ACK timeout");
            }

        } while (!allDone);

        if (debug) {
            System.out.println("GUDPSocket: All FIN packets sent and ACKed");
        }

        // Cleanup
        for (GUDPEndPoint ep : senderQueue) {
            ep.setFinished(true);
            ep.clear();

            if (debug) {
                System.out.println("GUDPSocket: Cleared endpoint " + ep.getRemoteEndPoint().toString());
            }
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

        if (debug) {
            System.out.println("GUDPSocket: Stopped GUDPSender thread");
        }

        r.stopGUDPReceiver();
        startReceiver = false;

        if (debug) {
            System.out.println("GUDPSocket: Stopped GUDPReceiver thread");
        }

        datagramSocket.close();

        if (debug) {
            System.out.println("GUDPSocket: Closed DatagramSocket");
        }
    }

    private GUDPEndPoint findSenderEndPoint(InetSocketAddress remoteEndPoint) {
        for (GUDPEndPoint endPoint : senderQueue) {
            if (endPoint.getRemoteEndPoint().equals(remoteEndPoint)) {
                return endPoint;
            }
        }
        return null;
    }

    private GUDPPacket createPacket(short type, int seqNum, InetSocketAddress remoteEndPoint) {
        try {
            // Create an empty UDP packet to hold the metadata
            byte[] content = new byte[0]; // empty payload
            DatagramPacket udpPacket = new DatagramPacket(content, content.length, remoteEndPoint);

            // Allocate buffer for GUDP packet
            ByteBuffer bb = ByteBuffer.allocate(content.length + GUDPPacket.HEADER_SIZE);
            bb.order(ByteOrder.BIG_ENDIAN);

            // Create GUDPPacket
            GUDPPacket gudpPacket = new GUDPPacket(bb);
            gudpPacket.setPayload(udpPacket.getData());
            gudpPacket.setPayloadLength(0);
            gudpPacket.setVersion(GUDPPacket.GUDP_VERSION);
            gudpPacket.setSeqno(seqNum);
            gudpPacket.setType(type);
            gudpPacket.setSocketAddress(remoteEndPoint);

            return gudpPacket;
        } catch (Exception e) {
            throw new RuntimeException("Failed to create packet", e);
        }
    }
}
