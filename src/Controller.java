import javax.print.attribute.HashAttributeSet;
import java.io.*;
import java.lang.reflect.Array;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class Controller {

    /**
     * Set consisting of the threads representing the current active clients (excluding dstores)
     */
    protected static final HashSet<ClientThread> activeClients = new HashSet<>();

    /**
     * Set consisting of the threads representing the current active dstores
     */
    protected static final HashSet<DstoreThread> activeDstores = new HashSet<>();

    /**
     * Set mapping each file with its properties (size, status, and dstores that have it)
     */
    protected static final ConcurrentHashMap<File, FileProperties> index = new ConcurrentHashMap<>();

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
            }
            System.out.println(this + " added to activeClients");

            try {
                System.out.println("New thread created");
                BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);

                String line;

                while ((line = in.readLine()) != null) {
                    System.out.println("Received from a client: ");

                    try {
                        handleMessage(line, in, out);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }

                clientSocket.close();

                synchronized (activeClients) {
                    activeClients.remove(this);
                }
                System.out.println(this + " removed from activeClients");

            } catch (Exception e) {
                System.err.println("error: " + e);
            }
        }

        private void handleMessage(String line, BufferedReader in, PrintWriter out) throws Exception {
            if (line.startsWith(Protocol.STORE_TOKEN)) {
                storeOp(line, out);
            } else if (line.startsWith(Protocol.LOAD_TOKEN)) {
                loadOp(line, out);
            } else if (line.startsWith(Protocol.REMOVE_TOKEN)) {
                removeOp(line, in, out);
            } else if (line.equals(Protocol.LIST_TOKEN)) {
                listOp(line, out);
            } else {
                throw new Exception("Could not handle the message");
            }
        }

        private void storeOp(String line, PrintWriter out) {
            String fileName = line.split(" ")[1];
            int fileSize = Integer.parseInt(line.split(" ")[2]);

            File file = new File(fileName); // TODO: Check paths and folder structure
            StringBuilder ports = new StringBuilder();

            for (DstoreThread dstore : index.get(file).getDstores()) {
                ports.append(dstore.getPort()).append(" ");
            }

            out.println(Protocol.STORE_TO_TOKEN + " " + ports);
            System.out.println("Sending: STORE TO " + " " + ports);


        }

        public void loadOp(String line, PrintWriter out) {
            String fileName = line.split(" ")[1];
            File file = new File(fileName);
            int fileSize = index.get(file).getFileSize();

            for (DstoreThread dstore : index.get(file).getDstores()) {
                int dstorePort = dstore.getPort();
                out.println(Protocol.LOAD_FROM_TOKEN + " " + dstorePort + " " + fileSize);

                // if success
                break;
            }
        }

        public void removeOp(String line, BufferedReader in, PrintWriter out) {
            String fileName = line.split(" ")[1];

            // get all the dstores

            // communicate with each DstoreThread and tell them to remove the file

            // tell the client the remove op is complete
            out.println(Protocol.REMOVE_COMPLETE_TOKEN);
        }

        public void listOp(String line, PrintWriter out) {
            StringBuilder fileList = new StringBuilder();

            for (File file : index.keySet()) {
                fileList.append(file.getName()).append(" ");
            }

            out.println(Protocol.LIST_TOKEN + " " + fileList);
            System.out.println("Sending: " + Protocol.LIST_TOKEN + " " + fileList);
        }
    }


    static class DstoreThread implements Runnable {
        private final Socket dstoreSocket;
        private final int port;

        public DstoreThread(Socket dstoreSocket, int port) {
            this.dstoreSocket = dstoreSocket;
            this.port = port;
        }

        public Socket getDstoreSocket() {
            return dstoreSocket;
        }

        public int getPort() {
            return port;
        }

        @Override
        public void run() {
            synchronized (activeDstores) {
                activeDstores.add(this);
            }
            System.out.println(this + " added to activeDstores");


            try {
                System.out.println("New Dstore thread created. Its port is " + port);
                BufferedReader in = new BufferedReader(new InputStreamReader(dstoreSocket.getInputStream()));
                String line;

                while ((line = in.readLine()) != null) {
                    System.out.println("Received from a dstore: " + line);

                    // handle each Dstore operations with if/else statements.
                }

                dstoreSocket.close();

                synchronized (activeDstores) {
                    activeDstores.remove(this);
                }
                System.out.println(this + " removed from activeDstores");

            } catch (Exception e) {
                System.err.println(e);
            }
        }
    }

    static class FileProperties {

        private final int fileSize;
        private FileStatus status;
        private ArrayList<DstoreThread> dstores;

        public FileProperties(int fileSize, FileStatus status, ArrayList<DstoreThread> dstores) {
            this.fileSize = fileSize;
            this.status = status;
            this.dstores = dstores;
        }

        enum FileStatus {
            STORE_IN_PROGRESS,
            STORE_COMPLETE,
            REMOVE_IN_PROGRESS,
            REMOVE_COMPLETE
        }

        public int getFileSize() {
            return fileSize;
        }

        public FileStatus getStatus() {
            return status;
        }

        public void setStatus(FileStatus status) {
            this.status = status;
        }

        public ArrayList<DstoreThread> getDstores() {
            return dstores;
        }

        public boolean addDstore(DstoreThread dstore) {
            return this.dstores.add(dstore);
        }

        public boolean removeDstore(DstoreThread dstore) {
            return this.dstores.remove(dstore);
        }

        public int getCount() {
            return dstores.size();
        }
    }
}
