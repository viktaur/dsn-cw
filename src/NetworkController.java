import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class NetworkController implements Runnable {

    protected final HashMap<Integer, Socket> portsToSockets;
    protected final int cport;
    protected final ConcurrentLinkedQueue<Message> tasks;

    public NetworkController(int cport, ConcurrentLinkedQueue<Message> tasks) {
        this.portsToSockets = new HashMap<>();
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
//                    portsToSockets.put(socket.getPort(), socket);
                    BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    PrintWriter out = new PrintWriter(socket.getOutputStream(), true);

                    String firstMessage = in.readLine();

                    if (firstMessage.startsWith(Protocol.JOIN_TOKEN)) {
                        int port = Integer.parseInt(firstMessage.split(" ")[1]);
                        Thread dstoreThread = new Thread(new DstoreThread(socket, port, in, out));
                        dstoreThread.start();
                    } else {
                        Thread clientThread = new Thread(new ClientThread(socket, in, out));
                        clientThread.start();
                    }

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

    class ConnectionThread {

        protected final Socket socket;
        protected final BufferedReader in;
        protected final PrintWriter out;

        public ConnectionThread(Socket socket, BufferedReader in, PrintWriter out) {
            this.socket = socket;
            this.in = in;
            this.out = out;
        }

        public void communicate(String message) {
            out.println(message);
            System.out.println("Sending: " + message);
        }
    }

    class ClientThread extends ConnectionThread implements Runnable {

        public ClientThread(Socket socket, BufferedReader in, PrintWriter out) {
            super(socket, in, out);
        }

        @Override
        public void run() {
            // constantly listen for incoming messages
            try {
                System.out.println("New ClientThread started");
                String msg;

                while ((msg = in.readLine()) != null) {
                    System.out.println("Received from client: " + msg);
                    tasks.add(new Message(msg, this));
                }
            } catch (Exception e) {

            }
            // add them to the queue
        }
    }

    class DstoreThread extends ConnectionThread implements Runnable {

        /**
         * Port at which the Dstore's ServerSocket will be listening for incoming Client connections.
         */
        private final int port;

        public DstoreThread(Socket socket, int port, BufferedReader in, PrintWriter out) {
            super(socket, in, out);
            this.port = port;
        }

        public int getPort() {
            return port;
        }

        @Override
        public void run() {

        }
    }
}
