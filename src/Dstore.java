import java.util.HashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class Dstore {

    /**
     * Port on which the Dstore will listen to
     */
    protected static int port;

    /**
     * Controller's port
     */
    protected static int cport;

    /**
     * How long to wait (in ms) when a process expects a response from another process
     */
    protected static int timeout;

    /**
     * Where to store the data locally
     */
    protected static String fileFolder;

    /**
     * Messages received from the connection threads that need to be handled
     */
    protected static final ConcurrentLinkedQueue<Message> tasks = new ConcurrentLinkedQueue<>();

    protected static final HashMap<ConnectionThread, String> filesToStore = new HashMap<>();

    public static void main(String[] args) {

        port = Integer.parseInt(args[0]);
        cport = Integer.parseInt(args[1]);
        timeout = Integer.parseInt(args[2]); // TODO: do something with timeout
        fileFolder = args[3];

        // We start a thread that will constantly listen to all incoming connections
        Thread incomingConnections = new Thread(new NetworkDstore(port, cport));
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

    private static void handleMessage(Message msg) {
        if (msg.getContent().startsWith(Protocol.STORE_TOKEN)) {
            store(msg);
        }
    }

    private static void store(Message msg) {
        String fileName = msg.getContent().split(" ")[1];
        int fileSize = Integer.parseInt(msg.getContent().split(" ")[2]);

        filesToStore.put(msg.getSender(), fileName);
        msg.getSender().communicate(Protocol.ACK_TOKEN);
    }

    private static void handleFileTransfer() {
        // ...

    }
}
