import java.awt.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class Controller {


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
     * Set consisting of the active threads that are listening to dstores
     */
    protected static final HashSet<NetworkController.DstoreThread> activeDstores = new HashSet<>();

    /**
     * Set mapping each file with its properties (size, status, and dstores that have it)
     */
    protected static final ConcurrentHashMap<String, FileProperties> index = new ConcurrentHashMap<>();

    /**
     * Messages received from the connection threads that need to be handled
     */
    protected static final ConcurrentLinkedQueue<Message> tasks = new ConcurrentLinkedQueue<>();

    /**
     * Load operations that have not yet been completed
     */
    protected static final HashMap<Message, Integer> currentLoadOps = new HashMap<>();

    public static void main(String[] args) {

        // init logger
        ControllerLogger.init(Logger.LoggingType.ON_TERMINAL_ONLY);

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
                    ControllerLogger.getInstance().couldNotHandleMessage(msgInfo.getContent());
                }
            }
        }
    }

    /**
     * Handles a message from tasks
     * @param msg message to handle
     * @throws Exception if anything goes wrong
     */
    public static void handleMessage(Message msg) throws Exception {

        if (msg.getContent().startsWith(Protocol.STORE_TOKEN)) {
            if (canPerformStoreOp(msg))  storeOp(msg);
        } else if (msg.getContent().startsWith(Protocol.LOAD_TOKEN)) {
            if (canPerformRemoveLoadOp(msg)) loadOp(msg, 0);
        } else if (msg.getContent().startsWith(Protocol.REMOVE_TOKEN)) {
            if (canPerformRemoveLoadOp(msg))  removeOp(msg);
        } else if (msg.getContent().equals(Protocol.LIST_TOKEN)) {
            if (canPerformListOp(msg))  listOp(msg);
        } else if (msg.getContent().startsWith(Protocol.STORE_ACK_TOKEN)) {
//            handleStoreAck(msg);
        } else if (msg.getContent().startsWith(Protocol.RELOAD_TOKEN)) {
            handleReload(msg);
        } else if (msg.getContent().startsWith(Protocol.REMOVE_ACK_TOKEN)) {
//            handleRemoveAck(msg);
        } else {
            throw new Exception("Unknown operation token");
        }
    }

    public static boolean canPerformStoreOp(Message msg) {
        String fileName = msg.getContent().split(" ")[1];

        if (activeDstores.size() < r) {
            msg.getSender().communicate(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
            return false;
        }
        if ((index.get(fileName) != null) && (!index.get(fileName).removeIsCompleted())) { // check whether the second is necessary
            msg.getSender().communicate(Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN);
            return false;
        }
        return true;
    }

    public static boolean canPerformRemoveLoadOp(Message msg) {
        String fileName = msg.getContent().split(" ")[1];

        if (activeDstores.size() < r) {
            msg.getSender().communicate(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
            return false;
        }
        if ((index.get(fileName) == null) || (index.get(fileName).storeIsInProgress()) || (index.get(fileName).removeIsInProgress())) {
            msg.getSender().communicate(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
            return false;
        }
        return true;
    }

    public static boolean canPerformListOp(Message msg) {
        if (activeDstores.size() < r) {
            msg.getSender().communicate(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
            return false;
        }
        return true;
    }

    public static void storeOp(Message msg) {
        String fileName = msg.getContent().split(" ")[1];
        int fileSize = Integer.parseInt(msg.getContent().split(" ")[2]);

        // we update index, so when the Controller receives a store request for the same file from another client,
        // it will know that there's already a store operation in progress for that file.
        index.put(fileName, new FileProperties(
                fileSize,
                FileProperties.FileStatus.STORE_IN_PROGRESS,
                new ArrayList<>()
        ));

        // this should be always equal to r, but just in case
        ArrayList<NetworkController.DstoreThread> dstoresToBeUsed = new ArrayList<>(activeDstores.stream().limit(r).toList());

        StringBuilder ports = new StringBuilder();

        for (NetworkController.DstoreThread dstore : index.get(fileName).getDstores()) {
            ports.append(dstore.getPort()).append(" ");
        }

        // service that will start a timeout on each dstore and update the index & communicate STORE_COMPLETE
        // to the client if successful.
        ExecutorService handleStoreAcks = Executors.newSingleThreadExecutor();
        handleStoreAcks.submit(() -> {

            CountDownLatch latch = new CountDownLatch(dstoresToBeUsed.size());
            AtomicBoolean timeoutHappened = new AtomicBoolean(false);

            ExecutorService timeoutThreads = Executors.newFixedThreadPool(dstoresToBeUsed.size());
            for (NetworkController.DstoreThread dstore : index.get(fileName).getDstores()) {
                timeoutThreads.submit(() -> {
                    try {
                        // we call this method to set a timeout for receiving the STORE_ACKs
                        dstore.startStoreTimeout(fileName, timeout);
                    } catch (TimeoutException e) {
                        timeoutHappened.set(true);
                        ControllerLogger.getInstance().timeoutExpiredWhileReading(dstore.getPort());
                        ControllerLogger.getInstance().storeToDstoreFailed(fileName, dstore.getPort());
                        return;
                    }

                    index.get(fileName).addDstore(dstore);
                    latch.countDown();
                    ControllerLogger.getInstance().storeToDstoreCompleted(fileName, dstore.getPort());
                });
            }

            while (true) {
                if (timeoutHappened.get()) {
                    index.remove(fileName);
                    break;
                }

                if (latch.getCount() == 0) {
                    index.get(fileName).setStatus(FileProperties.FileStatus.STORE_COMPLETE);
                    msg.getSender().communicate(Protocol.STORE_COMPLETE_TOKEN);
                    ControllerLogger.getInstance().storeCompleted(fileName);
                    break;
                }
            }

            timeoutThreads.shutdown();
        });

        // send the ports of those dstores to the client
        msg.getSender().communicate(Protocol.STORE_TO_TOKEN + " " + ports);
    }

    public static void loadOp(Message msg, Integer i) {
        String fileName = msg.getContent().split(" ")[1];
        FileProperties fp = index.get(fileName);
        int fileSize = fp.getFileSize();

        try {
            int dstorePort = fp.getDstores().get(i).getPort();
            msg.getSender().communicate(Protocol.LOAD_FROM_TOKEN + " " + dstorePort + " " + fileSize);

            // in case dstore 0 fails, we will try with 1
            currentLoadOps.put(msg, i+1);
        } catch (IndexOutOfBoundsException e) {
            System.err.println("Index larger than number of dstores");
            msg.getSender().communicate(Protocol.ERROR_LOAD_TOKEN);
        }
    }

    public static void removeOp(Message msg) {
        String fileName = msg.getContent().split(" ")[1];

        index.get(fileName).setStatus(FileProperties.FileStatus.REMOVE_IN_PROGRESS);

        // this should be always equal to r, but just in case
        int nDstores = index.get(fileName).getDstores().size();

        ExecutorService handleRemoveAcks = Executors.newSingleThreadExecutor();
        handleRemoveAcks.submit(() -> {
            CountDownLatch latch = new CountDownLatch(nDstores);
            AtomicBoolean timeoutHappened = new AtomicBoolean(false);

            ExecutorService timeoutThreads = Executors.newFixedThreadPool(nDstores);
            for (NetworkController.DstoreThread dstore : index.get(fileName).getDstores()) {
                timeoutThreads.submit(() -> {
                    try {
                        dstore.startRemoveTimeout(fileName, timeout);
                    } catch (TimeoutException e) {
                        timeoutHappened.set(true);
                        System.err.println("Remove ack timeout happened");
                        e.printStackTrace();
                        return;
                    }

                    index.get(fileName).removeDstore(dstore);
                    latch.countDown();
                });

                while (true) {
                    if (timeoutHappened.get()) {
                        // we will not update the file status, it should stay as REMOVE_IN_PROGRESS
                        System.err.println("Remove op failed: " + fileName);
                        break;
                    }

                    if (latch.getCount() == 0) {
                        index.get(fileName).setStatus(FileProperties.FileStatus.REMOVE_COMPLETE);
                        msg.getSender().communicate(Protocol.REMOVE_COMPLETE_TOKEN); // do we need to remove it from index?
                        System.out.println("Remove op completed: " + fileName);
                        break;
                    }
                }

                timeoutThreads.shutdown();
            }
        });

        // get all the dstores
        for (NetworkController.DstoreThread dstore : index.get(fileName).getDstores()) {
            dstore.communicate(Protocol.REMOVE_TOKEN + " " + fileName);
        }
    }

    public static void listOp(Message msg) {
        StringBuilder fileList = new StringBuilder();

        for (String fileName : index.keySet()) {
            if (index.get(fileName).storeIsCompleted()) {
                fileList.append(fileName).append(" ");
            }
        }

        msg.getSender().communicate(Protocol.LIST_TOKEN + " " + fileList);
    }

    public static void handleReload(Message msg) {
        loadOp(msg, currentLoadOps.get(msg));
    }

    public static void addDstore(NetworkController.DstoreThread dstore) {
        activeDstores.add(dstore);

        // call rebalance here
    }

    public static void removeDstore(NetworkController.DstoreThread dstore) {

        for (FileProperties fp : index.values()) {
            if (fp.getDstores().contains(dstore)) {
                fp.removeDstore(dstore);
            }
        }

        activeDstores.remove(dstore);
    }
}
