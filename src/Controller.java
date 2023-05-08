import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.*;

public class Controller {

    /**
     * Set consisting of the active threads that are listening to dstores
     */
    protected static final HashSet<NetworkController.DstoreThread> activeDstores = new HashSet<>();

    /**
     * Set mapping each file with its properties (size, status, and dstores that have it)
     */
    protected static final ConcurrentHashMap<String, FileProperties> index = new ConcurrentHashMap<>();

    /**
     * Port at which the server socket will be listening for incoming client and dstore connections
     */
    protected static int cport;

    /**
     * Replication factor: number of times a file is replicated over different dstores
     */
    protected static int r;

    /**
     * How long to wait (in ms) when a process expects a response from another process
     */
    protected static int timeout;

    /**
     * How long to wait (in seconds) to start the rebalance operation
     */
    protected static int rebalancePeriod;

    /**
     * Messages received from the connection threads that need to be handled
     */
    protected static final ConcurrentLinkedQueue<Message> tasks = new ConcurrentLinkedQueue<>();

    /**
     * Store operations that have not yet been completed
     */
    protected static final HashMap<Message, CountDownLatch> currentStoreOps = new HashMap<>();

    /**
     * Load operations that have not yet been completed
     */
    protected static final HashMap<Message, Integer> currentLoadOps = new HashMap<>();

    /**
     * Remove operations that have not yet been completed
     */
    protected static final HashMap<Message, CountDownLatch> currentRemoveOps = new HashMap<>();

    public static void main(String[] args) {

        cport = Integer.parseInt(args[0]);
        r = Integer.parseInt(args[1]);
        timeout = Integer.parseInt(args[2]);
        rebalancePeriod = Integer.parseInt(args[3]);

        // We start a thread that will constantly listen to all incoming connections
        Thread incomingConnections = new Thread(new NetworkController(cport, tasks));
        incomingConnections.start();

        // This is the main execution loop
        while (true) {
            Message msgInfo = tasks.poll();

            if (msgInfo != null) {

                try {
                    handleMessage(msgInfo);
                } catch (Exception e) {
                    System.err.println("Could not handle message: " + e);
                }
            }
        }
    }
    private static void handleMessage(Message msg) throws Exception {

        if (msg.getContent().startsWith(Protocol.STORE_TOKEN)) {
            storeOp(msg);
        } else if (msg.getContent().startsWith(Protocol.LOAD_TOKEN)) {
            loadOp(msg, 0);
        } else if (msg.getContent().startsWith(Protocol.REMOVE_TOKEN)) {
            removeOp(msg);
        } else if (msg.getContent().equals(Protocol.LIST_TOKEN)) {
            listOp(msg);
        } else if (msg.getContent().startsWith(Protocol.STORE_ACK_TOKEN)) {
            handleStoreAck(msg);
        } else if (msg.getContent().startsWith(Protocol.RELOAD_TOKEN)) {
            handleReload(msg);
        } else if (msg.getContent().startsWith(Protocol.REMOVE_ACK_TOKEN)) {
            handleRemoveAck(msg);
        } else {
            throw new Exception("Unknown operation token");
        }
    }

    private static void storeOp(Message msg) {
        String fileName = msg.getContent().split(" ")[1];
        int fileSize = Integer.parseInt(msg.getContent().split(" ")[2]);

        index.put(fileName, new FileProperties(fileSize, FileProperties.FileStatus.STORE_IN_PROGRESS, new ArrayList<>()));

        StringBuilder ports = new StringBuilder();

        // get the first r active dstores
        for (NetworkController.DstoreThread dstore : activeDstores.stream().limit(r).toList()) {
            ports.append(dstore.getPort()).append(" ");
        }

        // send the ports of those dstores to the client
        msg.getSender().communicate(Protocol.STORE_TO_TOKEN + " " + ports);

        // handle acks
        currentStoreOps.put(msg, new CountDownLatch(r));
    }

    public static void loadOp(Message msg, Integer i) {
        String fileName = msg.getContent().split(" ")[1];
        FileProperties fp = index.get(fileName);
        int fileSize = fp.getFileSize();

        int dstorePort = fp.getDstores().get(i).getPort();
        msg.getSender().communicate(Protocol.LOAD_FROM_TOKEN + " " + dstorePort + " " + fileSize);

        // in case dstore 0 fails, we will try with 1
        currentLoadOps.put(msg, i+1);
    }


    public static void removeOp(Message msg) {
        String fileName = msg.getContent().split(" ")[1];

        // get all the dstores
        for (NetworkController.DstoreThread dstore : index.get(fileName).getDstores()) {
            dstore.communicate(Protocol.REMOVE_TOKEN + " " + fileName);
        }

        currentRemoveOps.put(msg, new CountDownLatch(r));
    }

    public static void listOp(Message msg) {
        StringBuilder fileList = new StringBuilder();

        for (String fileName : index.keySet()) {
            fileList.append(fileName).append(" ");
        }

        msg.getSender().communicate(Protocol.LIST_TOKEN + " " + fileList);
    }

    public static void handleStoreAck(Message msg) {
        String fileName = msg.getContent().split(" ")[1];

        try {

            boolean found = false;
            // this for loop looks for the fileName in a current STORE op.
            for (Message opMsg : currentStoreOps.keySet()) {
                String opFileName = opMsg.getContent().split(" ")[1];

                if (fileName.equals(opFileName)) {
                    currentStoreOps.get(opMsg).countDown();
                    found = true;

                    // if the countdown has reached 0, we will send store complete, update the index, and we will get
                    // rid of the operation from currentRemoveOps.
                    if (currentStoreOps.get(opMsg).getCount() == 0) {
                        opMsg.getSender().communicate(Protocol.STORE_COMPLETE_TOKEN);
                        currentStoreOps.remove(opMsg);
                        index.get(fileName).setStatus(FileProperties.FileStatus.STORE_COMPLETE);
                    }
                    break;
                }
            }

            if (!found) {
                throw new Exception("Could not find " + fileName + " in current operations");
            }
        } catch (Exception e) {
            System.out.println("Warning: Received unexpected ACK"); // perhaps we need to deal with this in a different way
        }
    }

    public static void handleReload(Message msg) {
        loadOp(msg, currentLoadOps.get(msg));
    }

    public static void handleRemoveAck(Message msg) {
        String fileName = msg.getContent().split(" ")[1];

        try {

            boolean found = false;
            // this for loop looks for the fileName in a current REMOVE op.
            for (Message opMsg : currentRemoveOps.keySet()) {
                String opFileName = opMsg.getContent().split(" ")[1];

                if (fileName.equals(opFileName)) {
                    currentRemoveOps.get(opMsg).countDown();
                    found = true;

                    // if the countdown has reached 0, we will send remove complete, and we will get rid of
                    // the operation from currentRemoveOps, as well as the fileName from index
                    if (currentRemoveOps.get(opMsg).getCount() == 0) {
                        opMsg.getSender().communicate(Protocol.REMOVE_COMPLETE_TOKEN);
                        currentRemoveOps.remove(opMsg);
                        index.remove(fileName);
                    }
                    break;
                }
            }

            if (!found) {
                throw new Exception("Could not find " + fileName + " in current operations");
            }
        } catch (Exception e) {
            System.out.println("Warning: Received unexpected ACK"); // perhaps we need to deal with this in a different way
        }
    }

}
