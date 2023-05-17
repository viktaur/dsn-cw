import org.junit.jupiter.api.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.Buffer;


class ControllerTest {

    private static NetworkController.ClientThread clientSender;

    @BeforeAll
    static void init() {
        int cport = 3333;

        try (ServerSocket ss = new ServerSocket(cport) ){
            new Thread(() -> {
                while (true) {
                    try {
                        ss.accept();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            });

            Socket socket = new Socket(InetAddress.getLoopbackAddress(), cport);
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);

            clientSender = new NetworkController.ClientThread(socket, Controller.tasks, in, out);
            new Thread(clientSender).start();

        } catch (Exception e) {
            System.err.println("Error");
            fail();
        }
    }

    @Test
    void handleMessageTest() {
    }

    @Test
    void storeOpTest() {

    }
}