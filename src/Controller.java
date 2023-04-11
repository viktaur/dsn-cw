import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.Buffer;

public class Controller {
    public static void main(String[] args) {
        final int cport = Integer.parseInt(args[0]);
        final int r = Integer.parseInt(args[1]);
        final int timeout = Integer.parseInt(args[2]);
        final int rebalancePeriod = Integer.parseInt(args[3]);

        ServerSocket ss = null;
        try {
            ss = new ServerSocket(cport);
            while(true) {
                try {
                    Socket dstore = ss.accept();
                    new Thread(new ServiceThread(dstore)).start();
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

    static class ServiceThread implements Runnable {

        private final Socket client;

        public ServiceThread(Socket client) {
            this.client = client;
        }

        @Override
        public void run() {
            try {
                BufferedReader in = new BufferedReader(new InputStreamReader(client.getInputStream()));
                String line;
                while((line = in.readLine()) != null) {
                    System.out.println(line + " received");
                }
                client.close();
            } catch (Exception e) {
                System.err.println("error: " + e);
            }
        }
    }

}
