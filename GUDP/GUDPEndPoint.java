import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.Iterator;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.TimerTask;
import java.util.Timer;

@SuppressWarnings("unused")
class GUDPEndPoint {
    /* Pre-defined constant values for key variables */
    public static final int MAX_WINDOW_SIZE = 3;
    public static final long TIMEOUT_DURATION = 3000L; // 3 seconds
    public static final int MAX_RETRY = 9;

    /* Variables for the control block */
    private InetSocketAddress remoteEndPoint;
    private LinkedBlockingQueue<GUDPPacket> bufferQueue = new LinkedBlockingQueue<>();

    private int windowSize;
    private long timeoutDuration;
    private int maxRetry;
    private int retry;

    /* GBN sender */
    private int base;            // seq of sent packet not yet acked (i.e., base)
    private int nextseqnum;      // seq of next packet to send (i.e., nextseqnum)
    private int last;            // seq of last packet in bufferQueue

    /* GBN receiver */
    private int expectedseqnum;  // seq of next packet to receive

    private boolean finished = false;        // indicate communication finished

    /* for testing drop packets */    
    private boolean dropSend = false;        // for drop send packet
    private boolean dropReceive = false;     // for drop receive packet
    private double chance = 0.15;            // drop probability
    
    /* States for GBN sender and receiver combined in one */
    protected enum endPointState {
        INIT,
        WAIT,
        SEND,
	RCV,
        TIMEOUT,
        DEFAULT
    }
    private endPointState state = endPointState.INIT;

    public GUDPEndPoint(InetAddress addr, int port) {
        setRemoteEndPoint(addr, port);
        this.windowSize = MAX_WINDOW_SIZE;
        this.timeoutDuration = TIMEOUT_DURATION;
        this.maxRetry = MAX_RETRY;
	this.retry = 0;
    }
 
    public InetSocketAddress getRemoteEndPoint() {
        return this.remoteEndPoint;
    }

    public void setRemoteEndPoint(InetAddress addr, int port) {
        this.remoteEndPoint = new InetSocketAddress(addr, port);
    }

    public int getWindowSize() {
        return this.windowSize;
    }

    public void setWindowSize(int size) {
        this.windowSize = size;
    }

    public int getMaxRetry() {
        return this.maxRetry;
    }

    public void setMaxRetry(int retry) {
        this.maxRetry = retry;
    }

    public long getTimeoutDuration() {
        return this.timeoutDuration;
    }

    public void setTimeoutDuration(long duration) {
        this.timeoutDuration = duration;
    }

    public int getRetry() {
        return this.retry;
    }

    public void setRetry(int retry) {
        this.retry = retry;
    }

    public int getBase() {
        return this.base;
    }

    public void setBase(int base) {
        this.base = base;
    }

    public int getNextseqnum() {
        return this.nextseqnum;
    }

    public void setNextseqnum(int seq) {
        this.nextseqnum = seq;
    }

    public int getLast() {
        return this.last;
    }

    public void setLast(int last) {
        this.last = last;
    }

    public int getExpectedseqnum() {
        return this.expectedseqnum;
    }

    public void setExpectedseqnum(int seq) {
        this.expectedseqnum = seq;
    }

    public boolean getFinished() {
        return this.finished;
    }

    public void setFinished(boolean value) {
        this.finished = value;
    }

    public boolean getDropSend() {
        return this.dropSend;
    }

    public void setDropSend(boolean value) {
        this.dropSend = value;
    }

    public boolean getDropReceive() {
        return this.dropReceive;
    }

    public void setDropReceive(boolean value) {
        this.dropReceive = value;
    }

    public double getChance() {
        return this.chance;
    }

    public void setChance(double value) {
        this.chance = value;
    }

    public endPointState getState() {
        return this.state;
    }

    public void setState(endPointState state) {
        this.state = state;
    }

    public void add(GUDPPacket gpacket) {
        bufferQueue.add(gpacket);
    }

    public void remove(GUDPPacket gpacket) {
    	bufferQueue.remove(gpacket);
    }

    /*
     * Retrieve and remove the first packet from the bufferQueue
     */
    public GUDPPacket remove() {
    	return bufferQueue.poll();
    }

    /*
     * Get the packet with the given sequence number from bufferQueue
     * IMPORTANT: the packet is still in the bufferQueue!
     */
    public GUDPPacket getPacket(int seq) {
        Iterator<GUDPPacket> iter = bufferQueue.iterator();
        while(iter.hasNext()) {
	    GUDPPacket p = iter.next();
            if (p.getSeqno() == seq) {
                return p;
            }
        }
	return null;
    }

    /*
     * Remove all packets with sequence number less than or equal ACKed from bufferQueue
     * Assuming those packets were successfully received
     */
    public void removeAllACK(int acked) {
        while ((!isEmptyQueue()) && (bufferQueue.peek().getSeqno() <= acked)) {
            bufferQueue.poll();
        }
    }

    /*
     * Remove all packets from bufferQueue and reset all variables
     */
    public void clear() {
        bufferQueue.clear();
	this.setRetry(0);
	this.setBase(0);
	this.setNextseqnum(0);
	this.setLast(0);
	this.setExpectedseqnum(0);
	this.setFinished(false);
	this.setDropReceive(false);
    }

    public boolean isEmptyQueue() {
        return bufferQueue.isEmpty();
    }

    public int queueSize() {
        return bufferQueue.size();
    }

    /*
     * Timer uses for sending timeout
     */
    Timer timer;
    public void startTimer() {
        timer = new Timer("Timer");
        TimerTask task = new TimerTask() {
            public void run() {
                System.out.println("TIMEOUT " + getRetry() + ":\t" 
				+ remoteEndPoint.getAddress() + ":" + remoteEndPoint.getPort());
                setState(endPointState.TIMEOUT);
		timer.cancel();
            }
        };
        timer.schedule(task, timeoutDuration);
    }

    public void stopTimer() {
        timer.cancel();
    }

}
