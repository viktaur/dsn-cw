import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
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

    /**
     * Messages received from the connection threads that need to be handled by the Dstore
     */
    private final ConcurrentLinkedQueue<Message> tasks;

    private final FileStoredListener fileStoredListener;

    public NetworkDstore(int port, int cport, ConcurrentLinkedQueue<Message> tasks, FileStoredListener fileStoredListener) {
        this.port = port;
        this.cport = cport;
        this.tasks = tasks;
        this.fileStoredListener = fileStoredListener;
    }

    @Override
    public void run() {
        // Create a new socket to cport
        try {
            Socket socket = new Socket(InetAddress.getLoopbackAddress(), cport);
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);

            Thread controllerConnection = new Thread(new ControllerThread(socket, in, out, port, tasks, fileStoredListener));
            controllerConnection.start();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // Create a ServerSocket that accepts connections from Clients in port
        try (ServerSocket ss = new ServerSocket(port)) {

            // we are constantly accepting new connections from clients
            while (true) {
                try {
                    Socket client = ss.accept();
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

        private final FileStoredListener fileStoredListener;

        public ControllerThread(
                Socket socket,
                BufferedReader in,
                PrintWriter out,
                int port,
                ConcurrentLinkedQueue<Message> tasks,
                FileStoredListener fileStoredListener) {
            super(socket, in, out);
            this.port = port;
            this.tasks = tasks;
            this.fileStoredListener = fileStoredListener;
        }

        @Override
        public void run() {
            // We send JOIN port to the Controller
            this.communicate(Protocol.JOIN_TOKEN + " " + port);

            String msg;
            try {
                while ((msg = in.readLine()) != null) {
                    System.out.println("Received from controller: " + msg);
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
                while ((msg = in.readLine()) != null) {
                    System.out.println("Received from controller: " + msg);
                    tasks.add(new Message(msg, this));
                }
            } catch (Exception e) {
                System.err.println("Could not read message from Client");
            }
        }
    }
}
