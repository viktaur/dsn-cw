import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 *  This is just an example of how to use the provided client library. You are expected to customise this class and/or 
 *  develop other client classes to test your Controller and Dstores thoroughly. You are not expected to include client 
 *  code in your submission.
 */
public class ClientMainTest {

    static final int cport = 3000;
    static final int timeout = 30000;

    static File downloadFolder;
    static File uploadFolder;

    @BeforeAll
    static void init() {

        startSystem();

        // this client expects a 'downloads' folder in the current directory; all files loaded from the store will be stored in this folder
        downloadFolder = new File("downloads");
        if (!downloadFolder.exists())
            if (!downloadFolder.mkdir()) throw new RuntimeException("Cannot create download folder (folder absolute path: " + downloadFolder.getAbsolutePath() + ")");

        // this client expects a 'to_store' folder in the current directory; all files to be stored in the store will be collected from this folder
        uploadFolder = new File("to_store");
        if (!uploadFolder.exists())
            throw new RuntimeException("to_store folder does not exist");

        // launch a single client
//        testClient();

        // launch a number of concurrent clients, each doing the same operations

    }

    @Test
    void singleClientTest() {
        testClient();
    }

    @Test
    void multipleClientsTest() {
        for (int i = 0; i < 10; i++) {
            new Thread(ClientMainTest::test2Client).start();
        }
    }

    public static void startSystem() {
        ExecutorService service = Executors.newFixedThreadPool(4);

        service.submit(() -> Controller.main(new String[] {"3000", "2", "30000", "10"}));
        service.submit(() -> Dstore.main(new String[] {"5556", "3000", "30000", "m1"}));
        service.submit(() -> Dstore.main(new String[] {"5557", "3000", "30000", "m2"}));
        service.submit(() -> Dstore.main(new String[] {"5558", "3000", "30000", "m3"}));
    }

    public static void test2Client() {
        Client client = null;

        try {
            client = new Client(cport, timeout, Logger.LoggingType.ON_FILE_AND_TERMINAL);
            client.connect();
            Random random = new Random(System.currentTimeMillis() * System.nanoTime());

            File[] fileList = uploadFolder.listFiles();
            for (int i = 0; i < Objects.requireNonNull(fileList).length/2; i++) {
                File fileToStore = fileList[random.nextInt(fileList.length)];
                try {
                    client.store(fileToStore);
                } catch (Exception e) {
                    System.out.println("Error storing file " + fileToStore);
                    e.printStackTrace();
                }
            }

            String[] list = null;
            try { list = list(client); } catch(IOException e) { e.printStackTrace(); }

            for (int i = 0; i < Objects.requireNonNull(list).length/4; i++) {
                String fileToRemove = list[random.nextInt(list.length)];
                try {
                    client.remove(fileToRemove);
                } catch (Exception e) {
                    System.out.println("Error remove file " + fileToRemove);
                    e.printStackTrace();
                }
            }

            try { list = list(client); } catch(IOException e) { e.printStackTrace(); }

        } catch(IOException e) {
            e.printStackTrace();
        } finally {
            if (client != null)
                try { client.disconnect(); } catch(Exception e) { e.printStackTrace(); }
        }
    }

    public static void testClient() {
        Client client = null;

        try {

            client = new Client(cport, timeout, Logger.LoggingType.ON_FILE_AND_TERMINAL);

            try { client.connect(); } catch(IOException e) { e.printStackTrace(); return; }

            try { list(client); } catch(IOException e) { e.printStackTrace(); }

            // store first file in the to_store folder twice, then store second file in the to_store folder once
            File[] fileList = uploadFolder.listFiles();
            assert fileList != null;
            if (fileList.length > 0) {
                try { client.store(fileList[0]); } catch(IOException e) { e.printStackTrace(); }
                try { client.store(fileList[0]); } catch(IOException e) { e.printStackTrace(); }
            }
            if (fileList.length > 1) {
                try { client.store(fileList[1]); } catch(IOException e) { e.printStackTrace(); }
            }

            String[] list = null;
            try { list = list(client); } catch(IOException e) { e.printStackTrace(); }

            if (list != null)
                for (String filename : list)
                    try { client.load(filename, downloadFolder); } catch(IOException e) { e.printStackTrace(); }

            if (list != null)
                for (String filename : list)
                    try { client.remove(filename); } catch(IOException e) { e.printStackTrace(); }
            if (list != null && list.length > 0)
                try { client.remove(list[0]); } catch(IOException e) { e.printStackTrace(); }

            try { list(client); } catch(IOException e) { e.printStackTrace(); }

        } finally {
            if (client != null)
                try { client.disconnect(); } catch(Exception e) { e.printStackTrace(); }
        }
    }

    public static String[] list(Client client) throws IOException {
        System.out.println("Retrieving list of files...");
        String[] list = client.list();

        System.out.println("Ok, " + list.length + " files:");
        int i = 0;
        for (String filename : list)
            System.out.println("[" + i++ + "] " + filename);

        return list;
    }

}