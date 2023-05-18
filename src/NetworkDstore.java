import java.io.*;
import java.net.*;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Networking end of the Dstore class
 */
public class NetworkDstore implements Runnable {

    /**
     * Dstore's port for clients
     */
    private final int port;

    /**
     * Controller's port
     */
    private final int cport;

    private final int timeout;

    /**
     * Messages received from the connection threads that need to be handled by the Dstore
     */
    private final ConcurrentLinkedQueue<Message> tasks;

    public NetworkDstore(int port, int cport, int timeout, ConcurrentLinkedQueue<Message> tasks) {
        this.port = port;
        this.cport = cport;
        this.timeout = timeout;
        this.tasks = tasks;
    }

    @Override
    public void run() {
        Socket socketToController;
        try {
            // Create a new socket to cport (the Controller) and set the timeout
            socketToController = new Socket(InetAddress.getLoopbackAddress(), cport);

            // Initialise I/O
            BufferedReader in = new BufferedReader(new InputStreamReader(socketToController.getInputStream()));
            PrintWriter out = new PrintWriter(socketToController.getOutputStream(), true);

            // Start a new thread that will take care of the Controller connection
            Thread controllerConnection = new Thread(new ControllerThread(socketToController, in, out, port, tasks));
            controllerConnection.start();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // Create a ServerSocket that accepts connections from Clients in port
        try (ServerSocket ss = new ServerSocket(port)) {

            // we are constantly accepting new connections from clients
            while (true) {
                try {
                    Socket client = ss.accept(); // this will block until new client connects

                    // set socket timeout
                    client.setSoTimeout(timeout);

                    BufferedReader in = new BufferedReader(new InputStreamReader(client.getInputStream()));
                    PrintWriter out = new PrintWriter(client.getOutputStream(), true);

                    Thread clientThread = new Thread(new ClientThread(client, in, out, tasks));
                    clientThread.start();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    static class ControllerThread extends ConnectionThread implements Runnable {

        /**
         * Port at which the Dstore's ServerSocket will be listening for incoming Client connections. We need to
         * send this to the Controller, so it can send it to the Client.
         */
        private final int port;

        private final ConcurrentLinkedQueue<Message> tasks;

        private final DstoreListener dstoreListener;

        private final ControllerThread ct;

        public ControllerThread(
                Socket socket,
                BufferedReader in,
                PrintWriter out,
                int port,
                ConcurrentLinkedQueue<Message> tasks
        ) {
            super(socket, in, out);
            this.port = port;
            this.tasks = tasks;
            this.ct = this;
            this.dstoreListener = new DstoreListener() {
                @Override
                public void fileStored(String fileName) {
                    ct.communicate(Protocol.STORE_ACK_TOKEN + " " + fileName);
                }

                @Override
                public void fileRemoved(String fileName) {
                    ct.communicate(Protocol.REMOVE_ACK_TOKEN + " " + fileName);
                }

                @Override
                public void errorFileDoesNotExist(String fileName) {
                    ct.communicate(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN + " " + fileName);
                }
            };

            Dstore.setDstoreListener(this.dstoreListener);
        }

        @Override
        public void run() {
            // We send JOIN port to the Controller
            this.communicate(Protocol.JOIN_TOKEN + " " + port);

            String msg;
            try {
                while ((msg = in.readLine()) != null) {
                    System.out.println("Received from Controller: " + msg);
                    tasks.add(new Message(msg, this));
                }
            } catch (Exception e) {
                System.err.println("Could not read message from Controller");
            }
        }
    }

    static class ClientThread extends ConnectionThread implements Runnable {

        private final ConcurrentLinkedQueue<Message> tasks;

        public ClientThread(Socket socket, BufferedReader in, PrintWriter out, ConcurrentLinkedQueue<Message> tasks) {
            super(socket, in, out);
            this.tasks = tasks;
        }

        @Override
        public void run() {

            String msg;

            try {
                // it should only run once, for the STORE or LOAD command
                while ((msg = in.readLine()) != null) {

                    // we will ensure it's a STORE or LOAD command
                    if ((msg.startsWith(Protocol.STORE_TOKEN)) || (msg.startsWith(Protocol.LOAD_TOKEN))) {
                        System.out.println("Received from Client: " + msg);
                        tasks.add(new Message(msg, this));
                        break;
                    }
                }

                // we will close the BufferedReader, as we will no longer need it
//                in.close();
            } catch (IOException e) {
                System.err.println("Could not read message from Client");
            }
        }
    }
}
