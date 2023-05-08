import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.Socket;

public class Dstore {

    public static void main(String[] args) {

        // Port on which the Dstore will listen to
        final int port = Integer.parseInt(args[0]);

        // Controller's port
        final int cport = Integer.parseInt(args[1]);

        // How long to wait (in ms) when a process expects a response from another process
        final int timeout = Integer.parseInt(args[2]);

        // Where to store the data locally
        final String fileFolder = args[3];

        Socket socket = null;
        try {
            // we will be using localhost, since the Controller is on the same machine
            InetAddress address = InetAddress.getLocalHost();

            // socket connecting to the Controller
            socket = new Socket(address, cport);
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);

            out.println("JOIN " + port);
            System.out.println("Sending JOIN " + port);
            Thread.sleep(timeout);

            String line;

            while ((line = in.readLine()) != null) {
                System.out.println("Received from Controller: " + line);
                handleMessage(line, out);
            }

        } catch (Exception e) {
            System.err.println("error: " + e);
        } finally {
            if (socket != null) {
                try {
                    socket.close();
                } catch (Exception e) {
                    System.err.println("error: " + e);
                }
            }
        }
    }

    private static void handleMessage(String line, PrintWriter out) {
        if (line.startsWith(Protocol.STORE_TOKEN)) {
            String fileName = line.split(" ")[1];
            int fileSize = Integer.parseInt(line.split(" ")[2]);

            if (store(fileName, fileSize)) {
                out.println(Protocol.ACK_TOKEN);
            }
        }
    }

    private static boolean store(String fileName, int fileSize) {
        return true;
    }
}
