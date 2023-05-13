import java.io.BufferedReader;
import java.io.PrintWriter;
import java.net.Socket;

public class ConnectionThread {

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
