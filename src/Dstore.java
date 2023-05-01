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

        // How long to wait (in seconds) when a process expects a response from another process
        final int timeout = Integer.parseInt(args[2]);

        // Where to store the data locally
        final String fileFolder = args[3];

        Socket socket = null;
        try {
            InetAddress address = InetAddress.getLocalHost();
            socket = new Socket(address, cport);
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            out.println("JOIN " + port);
            System.out.println("Sending JOIN " + port);
            Thread.sleep(1000);
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
}
