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
        InetAddress addr = packet.getAddress();
        int port = packet.getPort();
        GUDPEndPoint endPoint = null;

        // find or create sender endpoint
        for (GUDPEndPoint ep : senderQueue) {
            if (ep.getRemoteEndPoint().getAddress().equals(addr) && ep.getRemoteEndPoint().getPort() == port) {
                endPoint = ep;
                break;
            }
        }

        if (endPoint == null) {
            endPoint = new GUDPEndPoint(addr, port);
            senderQueue.add(endPoint);

            // create BSN packet
            ByteBuffer buffer = ByteBuffer.allocate(GUDPPacket.HEADER_SIZE);
            buffer.order(ByteOrder.BIG_ENDIAN);
            GUDPPacket bsnPacket = new GUDPPacket(buffer);
            bsnPacket.setType(GUDPPacket.TYPE_BSN);
            bsnPacket.setVersion(GUDPPacket.GUDP_VERSION);
            bsnPacket.setPayloadLength(0);
            bsnPacket.setSocketAddress(new InetSocketAddress(addr, port));
            Random rand = new Random();
            int bsn = 2 + rand.nextInt(Short.MAX_VALUE - 2);
            bsnPacket.setSeqno(bsn);
            endPoint.setBase(bsn);
            endPoint.setNextseqnum(bsn + 1);
            endPoint.add(bsnPacket);
        }

        // wrap payload as DATA packet
        int nextSeq = endPoint.getNextseqnum();
        endPoint.setNextseqnum(nextSeq + 1);
        GUDPPacket dataPacket = GUDPPacket.encapsulate(packet);
        dataPacket.setType(GUDPPacket.TYPE_DATA);
        dataPacket.setSeqno(nextSeq);
        dataPacket.setSocketAddress(new InetSocketAddress(addr, port)); // crucial!
        endPoint.add(dataPacket);

        // start threads if not running
        if (!startSender) {
            s.start();
            startSender = true;
        }
        if (!startReceiver) {
            r.start();
            startReceiver = true;
        }
    }

    public void receive(DatagramPacket packet) throws IOException {
        if (!startReceiver) {
            r.start();
            startReceiver = true;
        }

        while (true) {
            boolean foundPacket = false;
            for (GUDPEndPoint ep : receiverQueue) {
                if (ep == null)
                    continue;
                GUDPPacket gpacket = ep.remove();
                if (gpacket != null && gpacket.getType() == GUDPPacket.TYPE_DATA) {
                    gpacket.decapsulate(packet);
                    foundPacket = true;
                    break;
                }
            }
            if (foundPacket)
                break;

            try {
                Thread.sleep(SLEEP_TIME);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    public void finish() throws IOException {
        for (GUDPEndPoint ep : senderQueue) {
            ByteBuffer buffer = ByteBuffer.allocate(GUDPPacket.HEADER_SIZE);
            buffer.order(ByteOrder.BIG_ENDIAN);
            GUDPPacket finPacket = new GUDPPacket(buffer);
            finPacket.setType(GUDPPacket.TYPE_FIN);
            finPacket.setVersion(GUDPPacket.GUDP_VERSION);
            finPacket.setPayloadLength(0);
            finPacket.setSocketAddress(ep.getRemoteEndPoint());

            int finNum = ep.getNextseqnum();
            finPacket.setSeqno(finNum);
            ep.setNextseqnum(finNum + 1);

            ep.add(finPacket);
            if (ep.getBase() == finNum) {
                ep.startTimer();
            }
        }

        long start = System.currentTimeMillis();
        boolean allDone;
        do {
            try {
                Thread.sleep(1000);
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

        for (GUDPEndPoint ep : senderQueue) {
            ep.setFinished(true);
            ep.clear();
        }
        senderQueue.clear();
    }

    public void close() throws IOException {
        s.stopGUDPSender();
        startSender = false;

        r.stopGUDPReceiver();
        startReceiver = false;

        datagramSocket.close();
    }
}
