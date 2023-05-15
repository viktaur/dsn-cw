import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
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

    protected static final FileStoredListener fileStoredListener = new FileStoredListener() {
        @Override
        public void fileStored() {

        }
    };

    public static void main(String[] args) {

        port = Integer.parseInt(args[0]);
        cport = Integer.parseInt(args[1]);
        timeout = Integer.parseInt(args[2]); // TODO: do something with timeout
        fileFolder = args[3];

        // We start a thread that will constantly listen to all incoming connections
        Thread incomingConnections = new Thread(new NetworkDstore(port, cport, tasks));
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

        // We'll start a new thread to listen for the client's file transfer and then tell the Controller, so it
        // can update the index.
        Thread storeThread = new Thread(new StoreThread(msg));
        storeThread.start();

        msg.getSender().communicate(Protocol.ACK_TOKEN);
    }

    static class StoreThread implements Runnable {

        private final Message msg;
        private final String fileName;
        private final int fileSize;

        public StoreThread(Message msg) {
            this.msg = msg;
            this.fileName = msg.getContent().split(" ")[1];
            this.fileSize = Integer.parseInt(msg.getContent().split(" ")[2]);
        }

        @Override
        public void run() {
            final byte[] data = new byte[fileSize];

            try {
                // we create a new file
                File file = new File(fileFolder + "/" + fileName);
                if (file.createNewFile()) {
                    System.out.println("File created " + file);
                } else {
                    System.out.println("File already exists");
                }

                // we read the data from the inputStream
                InputStream inputStream = msg.getSender().getSocket().getInputStream();
                inputStream.readNBytes(data, 0, fileSize);

                // we write the data to the file
                Files.write(file.toPath(), data);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            // tell the controller that we're done

        }
    }
}
