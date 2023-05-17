import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Date;
import java.util.concurrent.*;

/**
 * Networking end of the Controller class
 */
public class NetworkController implements Runnable {

    /**
     * Controller's port
     */
    protected final int cport;

    /**
     * Messages received from the connection threads that need to be handled by the Controller
     */
    protected final ConcurrentLinkedQueue<Message> tasks;

    public NetworkController(int cport, ConcurrentLinkedQueue<Message> tasks) {
        this.cport = cport;
        this.tasks = tasks;
    }

    /**
     * Creates a ServerSocket and continuously accepts connections from Clients and Dstores.
     * <p>
     * For each new connection, adds the port-socket binding to portsToSocket and start a new thread to listen for
     * incoming messages.
     */
    @Override
    public void run() {
        ServerSocket ss = null;

        try {
            ss = new ServerSocket(cport);

            // we are constantly accepting new connections
            while (true) {
                try {
                    // a socket will be created whenever a new Client / Dstore requests to make a connection
                    Socket socket = ss.accept();
                    BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    PrintWriter out = new PrintWriter(socket.getOutputStream(), true);

                    // every dstore connection is a client until proven otherwise
                    Thread clientThread = new Thread(new ClientThread(socket, tasks, in, out));
                    clientThread.start();

                } catch (Exception e) {
                    System.err.println("error: " + e);
                }
            }
        } catch (Exception e) {
            System.err.println("error: " + e);
        } finally {
            if (ss != null) {
                try {
                    ss.close();
                } catch (IOException e) {
                    System.err.println("error: " + e);
                }
            }
        }
    }

    static class ClientThread extends ConnectionThread implements Runnable {

        private final ConcurrentLinkedQueue<Message> tasks;

        public ClientThread(Socket socket, ConcurrentLinkedQueue<Message> tasks, BufferedReader in, PrintWriter out) {
            super(socket, in, out);
            this.tasks = tasks;
        }

        @Override
        public void run() {
            System.out.println("New ClientThread started");

            // constantly listen for incoming messages and add them to tasks
            try {
                String msg;

                while ((msg = in.readLine()) != null) {

                    // if any message starts with JOIN, we will start a new dstore thread and interrupt the current client one
                    if (msg.startsWith(Protocol.JOIN_TOKEN)) {
                        int port = Integer.parseInt(msg.split(" ")[1]);
                        Thread dstoreThread = new Thread(new DstoreThread(socket, port, tasks, in, out));
                        dstoreThread.start();
                        Thread.currentThread().interrupt();
                        break;
                    }

                    System.out.println("Received from client: " + msg);
                    tasks.add(new Message(msg, this));
                }
            } catch (Exception e) {
                System.err.println("Could not read message from Client");
            }
        }
    }

    static class DstoreThread extends ConnectionThread implements Runnable {

        /**
         * Port at which the Dstore's ServerSocket will be listening for incoming Client connections.
         */
        private final int port;

        private final ConcurrentLinkedQueue<Message> tasks;

        private final ConcurrentLinkedQueue<String> storeAcks;
        private final ConcurrentLinkedQueue<String> removeAcks;


        public DstoreThread(Socket socket, int port, ConcurrentLinkedQueue<Message> tasks, BufferedReader in, PrintWriter out) {
            super(socket, in, out);
            this.port = port;
            this.tasks = tasks;
            this.storeAcks = new ConcurrentLinkedQueue<>();
            this.removeAcks = new ConcurrentLinkedQueue<>();
        }

        public int getPort() {
            return port;
        }

        @Override
        public void run() {

            Controller.addDstore(this);

            System.out.println("New DstoreThread started");

            // constantly listen for incoming messages and add them to tasks
            try {
                String msg;

                while ((msg = in.readLine()) != null) {
                    System.out.println("Received from Dstore: " + msg);
                    String fileName = msg.split(" ")[1];

                     if (msg.startsWith(Protocol.STORE_ACK_TOKEN)) {
                        storeAcks.add(fileName);
                     } else if (msg.startsWith(Protocol.REMOVE_ACK_TOKEN)) {
                         removeAcks.add(fileName);
                     } else {
                         tasks.add(new Message(msg, this));
                     }
                }
            } catch (IOException e) {
                System.err.println("Could not read message from Dstore");
                Controller.removeDstore(this);
            }
        }

        public void startStoreTimeout(String fileName, int timeout) throws TimeoutException {
            timeout(fileName, timeout, removeAcks);
        }

        public void startRemoveTimeout(String fileName, int timeout) throws TimeoutException {
            timeout(fileName, timeout, storeAcks);
        }

        public void timeout(String fileName, int timeout, ConcurrentLinkedQueue<String> acks) throws TimeoutException {
            long startTime = System.currentTimeMillis();
            long elapsedTime = 0L;

            while (elapsedTime < timeout) {
                if (acks.contains(fileName)) {
                    acks.remove(fileName);
                    return;
                }
                elapsedTime = (new Date()).getTime() - startTime;
            }

            throw new TimeoutException();
        }
    }
}
