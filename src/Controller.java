import java.util.*;
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
    protected static final HashMap<ConnectionThread, HashMap<String, Integer>> fileIndexToBeLoad = new HashMap<>();

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
            Message msg = tasks.poll();

            if (msg != null) {

                try {
                    handleMessage(msg);
                } catch (Exception e) {
                    ControllerLogger.getInstance().couldNotHandleMessage(msg.getContent());
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
            if (canPerformRemoveLoadOp(msg))  loadOp(msg);
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

        if (((index.get(fileName)) != null) && (!index.get(fileName).storeIsCompleted())) {
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
        List<NetworkController.DstoreThread> dstoresToBeUsed = getRActiveDstoresSorted();

        StringBuilder ports = new StringBuilder();

        for (NetworkController.DstoreThread dstore : dstoresToBeUsed) {
            ports.append(dstore.getPort()).append(" ");
        }

        // send the ports of those dstores to the client
        msg.getSender().communicate(Protocol.STORE_TO_TOKEN + " " + ports.toString().trim());

        CountDownLatch latch = new CountDownLatch(dstoresToBeUsed.size());
        AtomicBoolean timeoutHappened = new AtomicBoolean(false);

        // service that will start a timeout on each dstore and update the index & communicate STORE_COMPLETE
        // to the client if successful.
        ExecutorService handleStoreAcks = Executors.newFixedThreadPool(dstoresToBeUsed.size());
        for (NetworkController.DstoreThread dstore : dstoresToBeUsed) {
            handleStoreAcks.submit(() -> {
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

        ExecutorService watchdog = Executors.newSingleThreadExecutor();
        watchdog.submit(() -> {
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
        });
    }

    public static void loadOp(Message msg) {

        String fileName = msg.getContent().split(" ")[1];

        if (!fileIndexToBeLoad.containsKey(msg.getSender())) {
            fileIndexToBeLoad.put(msg.getSender(), new HashMap<>());
        }

        fileIndexToBeLoad.get(msg.getSender()).put(fileName, 0);

        load(msg, 0);
    }

    public static void handleReload(Message msg) {

        String fileName = msg.getContent().split(" ")[1];
        int currentIndex = fileIndexToBeLoad.get(msg.getSender()).get(fileName);

        fileIndexToBeLoad.get(msg.getSender()).put(fileName, currentIndex + 1);
        load(msg, currentIndex + 1);
    }

    public static void load(Message msg, int i) {
        String fileName = msg.getContent().split(" ")[1];
        FileProperties fp = index.get(fileName);
        int fileSize = fp.getFileSize();

        try {
            int dstorePort = fp.getDstores().get(i).getPort();
            msg.getSender().communicate(Protocol.LOAD_FROM_TOKEN + " " + dstorePort + " " + fileSize);
        } catch (IndexOutOfBoundsException e) {
            System.err.println("Index larger than number of dstores");
            msg.getSender().communicate(Protocol.ERROR_LOAD_TOKEN);
        }
    }

    public static void removeOp(Message msg) {
        String fileName = msg.getContent().split(" ")[1];

        try {
            index.get(fileName).setStatus(FileProperties.FileStatus.REMOVE_IN_PROGRESS);
        } catch (NullPointerException e) {
            msg.getSender().communicate(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
        }

        // tell all the dstores to remove a file
        for (NetworkController.DstoreThread dstore : index.get(fileName).getDstores()) {
            dstore.communicate(Protocol.REMOVE_TOKEN + " " + fileName);
        }

        // this should be always equal to r, but just in case
        int nDstores = index.get(fileName).getDstores().size();

        CountDownLatch latch = new CountDownLatch(nDstores);
        AtomicBoolean timeoutHappened = new AtomicBoolean(false);

        ExecutorService handleRemoveAcks = Executors.newFixedThreadPool(nDstores);
        for (NetworkController.DstoreThread dstore : index.get(fileName).getDstores()) {
            handleRemoveAcks.submit(() -> {
                try {
                    dstore.startRemoveTimeout(fileName, timeout);
                } catch (TimeoutException e) {
                    timeoutHappened.set(true);
                    ControllerLogger.getInstance().timeoutExpiredWhileReading(dstore.getPort());
                    ControllerLogger.getInstance().removeFailed(fileName);
                    return;
                }

                index.get(fileName).removeDstore(dstore);
                latch.countDown();
                ControllerLogger.getInstance().removeFromDstoreCompleted(fileName, dstore.getPort());
            });
        }

        ExecutorService watchdog = Executors.newSingleThreadExecutor();
        watchdog.submit(() -> {
            while (true) {
                if (timeoutHappened.get()) {
                    // we will not update the file status, it should stay as REMOVE_IN_PROGRESS
                    ControllerLogger.getInstance().removeFailed(fileName);
                    break;
                }

                if (latch.getCount() == 0) {
                    index.get(fileName).setStatus(FileProperties.FileStatus.REMOVE_COMPLETE);
                    msg.getSender().communicate(Protocol.REMOVE_COMPLETE_TOKEN); // do we need to remove it from index?
                    index.remove(fileName);
                    ControllerLogger.getInstance().removeComplete(fileName);
                    break;
                }
            }
        });
    }

    public static void listOp(Message msg) {
        StringBuilder fileList = new StringBuilder();

        for (String fileName : index.keySet()) {
            if (index.get(fileName).storeIsCompleted()) {
                fileList.append(fileName).append(" ");
            }
        }

        msg.getSender().communicate(Protocol.LIST_TOKEN + " " + fileList.toString().trim());
    }

    public static void addDstore(NetworkController.DstoreThread dstore) {
        activeDstores.add(dstore);

        // call rebalance here
    }


    /**
     * Removes a Dstore from every entry in the index
     * @param dstore dstore to be removed from the system
     */
    public static void removeDstore(NetworkController.DstoreThread dstore) {

        for (FileProperties fp : index.values()) {
            if (fp.getDstores().contains(dstore)) {
                fp.removeDstore(dstore);
            }
        }

        activeDstores.remove(dstore);
    }

    public static List<NetworkController.DstoreThread> getRActiveDstoresSorted() {

        HashMap<NetworkController.DstoreThread, Integer> dstoresToNFiles = new HashMap<>();

        // add dstores in the index (i.e. storing at least one file)
        for (FileProperties fp : index.values()) {
            for (NetworkController.DstoreThread dstore : fp.getDstores()) {
                if (dstoresToNFiles.containsKey(dstore)) {
                    dstoresToNFiles.put(dstore, dstoresToNFiles.get(dstore) + 1);
                } else {
                    dstoresToNFiles.put(dstore, 1);
                }
            }
        }

        // add dstores in activeDstores, but not in the index (i.e. storing no files)
        // this should have the highest priority
        for (NetworkController.DstoreThread dstore : activeDstores) {
            if (!dstoresToNFiles.containsKey(dstore)) {
                dstoresToNFiles.put(dstore, 0);
            }
        }

        return dstoresToNFiles
                .keySet()
                .stream()
                .sorted(Comparator.comparing(dstoresToNFiles::get))
                .limit(r)
                .toList();
    }
}
