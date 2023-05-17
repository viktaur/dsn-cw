import java.io.BufferedReader;
import java.io.IOException;
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
//        ControllerLogger.getInstance().messageSent(socket.getPort(), message);
        System.out.println("Sending: " + message);
    }

    public Socket getSocket() {
        return socket;
    }

    public void closeConnection() throws IOException {
        this.socket.close();
    }

    public void writeData(byte[] data) throws IOException {
        this.socket.getOutputStream().write(data);
    }

    public int readData(byte[] data, int off, int len) throws IOException {
        return this.socket.getInputStream().readNBytes(data, off, len);
    }
}
