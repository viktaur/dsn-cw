import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;

public class Controller {

    /**
     * Set consisting of the sockets representing the current active clients (excluding dstores)
     */
    protected static final HashSet<Socket> activeClients = new HashSet<>();

    /**
     * Map linking an active Dstore with the list of files it's storing
     */
    protected static final ConcurrentHashMap<Socket, ArrayList<File>> index = new ConcurrentHashMap<>();
    // might need to be ArrayList<ConcurrentHashMap<File, Status>> instead

    public static void main(String[] args) {

        // Port at which the server socket will be listening for incoming client connections
        final int cport = Integer.parseInt(args[0]);

        // Replication factor: number of times a file is replicated over different dstores
        final int r = Integer.parseInt(args[1]);

        // How long to wait (in seconds) when a process expects a response from another process
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

                    // we will start a new thread for each client
                    try {
                        startThread(socket);
                    } catch (Exception e) {
                        System.err.println(e);
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

    /**
     * Creates a new Dstore or Client thread depending on the first message sent
     * @param socket represents the client connection
     * @throws IOException
     */
    public static void startThread(Socket socket) throws IOException {
        BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        String firstMessage = in.readLine();
        if (firstMessage.startsWith(Protocol.JOIN_TOKEN)) {
            int port = Integer.parseInt(firstMessage.split(" ")[1]);
            new Thread(new DstoreThread(socket, port)).start();
        } else {
            new Thread(new ClientThread(socket)).start();
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
            synchronized (index) {
                index.put(dstoreSocket, new ArrayList<>());
            }

            try {
                System.out.println("New Dstore thread created. Its port is " + port);
                BufferedReader in = new BufferedReader(new InputStreamReader(dstoreSocket.getInputStream()));
                String line;
                while (dstoreSocket.isConnected()) {
                    line = in.readLine();
                    // handle each Dstore operations with if/else statements.
                }

                dstoreSocket.close();

                synchronized (index) {
                    index.remove(dstoreSocket);
                }

            } catch (Exception e) {
                System.err.println(e);
            }
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
                activeClients.add(clientSocket);
            }

            try {
                System.out.println("New thread created");
                BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                String line;
                while ((line = in.readLine()) != null) {
                    System.out.println(line + " received");
                }
                clientSocket.close();

                synchronized (activeClients) {
                    activeClients.remove(clientSocket);
                }

            } catch (Exception e) {
                System.err.println("error: " + e);
            }
        }
    }
}
