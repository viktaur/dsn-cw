import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;

public class Controller {

    /**
     * Set consisting of the threads representing the current active clients (excluding dstores)
     */
    protected static final HashSet<ClientThread> activeClients = new HashSet<>();

    /**
     * Set consisting of the threads representing the current active dstores
     */
    protected static final ConcurrentHashMap<DstoreThread,ConcurrentHashMap<File, FileStatus>> index = new ConcurrentHashMap<>();

    public static void main(String[] args) {

        // Port at which the server socket will be listening for incoming client and dstore connections
        final int cport = Integer.parseInt(args[0]);

        // Replication factor: number of times a file is replicated over different dstores
        final int r = Integer.parseInt(args[1]);

        // How long to wait (in ms) when a process expects a response from another process
        final int timeout = Integer.parseInt(args[2]);

        // How long to wait (in seconds) to start the rebalance operation
        final int rebalancePeriod = Integer.parseInt(args[3]);

        ServerSocket ss = null;

        try {
            ss = new ServerSocket(cport);

            // we are constantly accepting new connections
            while (true) {
                try {
                    // a socket will be created whenever a new Client / Dstore requests to make a connection
                    Socket socket = ss.accept();

                    // we will start a new thread for each client or dstore
                    startThread(socket);
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

    /**
     * Creates a new Dstore or Client thread depending on the first message sent
     * @param socket represents the connection with the client or dstore
     * @throws IOException
     */
    public static void startThread(Socket socket) throws IOException {
        BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        String firstMessage = in.readLine();
        System.out.println("First message is: " + firstMessage);
        if (firstMessage.startsWith(Protocol.JOIN_TOKEN)) {
            int port = Integer.parseInt(firstMessage.split(" ")[1]);
            new Thread(new DstoreThread(socket, port)).start();
        } else {
            new Thread(new ClientThread(socket)).start();
        }
    }

    static class ClientThread implements Runnable {

        private final Socket clientSocket;

        public ClientThread(Socket clientSocket) {
            this.clientSocket = clientSocket;
        }

        @Override
        public void run() {
            synchronized (activeClients) {
                activeClients.add(this);
                System.out.println(this + " added to activeClients");
            }

            try {
                System.out.println("New thread created");
                BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);

                String line;

                while ((line = in.readLine()) != null) {
                    System.out.println("Received from a client: ");

                    try {
                        handleMessage(line, out);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }

                clientSocket.close();

                synchronized (activeClients) {
                    activeClients.remove(this);
                    System.out.println(this + " removed from activeClients");
                }

            } catch (Exception e) {
                System.err.println("error: " + e);
            }
        }

        private void handleMessage(String line, PrintWriter out) throws Exception {
            if (line.startsWith(Protocol.STORE_TOKEN)) {
                String fileName = line.split(" ")[1];
                int fileSize = Integer.parseInt(line.split(" ")[2]);

            } else if (line.startsWith(Protocol.LOAD_TOKEN)) {
                String fileName = line.split(" ")[1];

            } else if (line.startsWith(Protocol.REMOVE_TOKEN)) {
                String fileName = line.split(" ")[1];

            } else if (line.equals(Protocol.LIST_TOKEN)) {

            } else {
                throw new Exception("Could not handle the message");
            }
        }
    }


    static class DstoreThread implements Runnable {
        private final Socket dstoreSocket;
        private final int port;

        public DstoreThread(Socket dstoreSocket, int port) {
            this.dstoreSocket = dstoreSocket;
            this.port = port;
        }

        @Override
        public void run() {
            index.put(this, new ConcurrentHashMap<>());
            System.out.println(this + " added to index");

            try {
                System.out.println("New Dstore thread created. Its port is " + port);
                BufferedReader in = new BufferedReader(new InputStreamReader(dstoreSocket.getInputStream()));
                String line;

                while ((line = in.readLine()) != null) {
                    System.out.println("Received from a dstore: " + line);

                    // handle each Dstore operations with if/else statements.
                }

                dstoreSocket.close();

                index.remove(this);
                System.out.println(this + " removed from index");

            } catch (Exception e) {
                System.err.println(e);
            }
        }
    }

    enum FileStatus {
        STORE_IN_PROGRESS,
        STORE_COMPLETE,
        REMOVE_IN_PROGRESS,
        REMOVE_COMPLETE
    }
}
