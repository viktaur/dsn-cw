public class Dstore {

    private final int port;
    private final int cport;
    private final int timeout;
    private final String fileFolder;

    public Dstore(int port, int cport, int timeout, String fileFolder) {
        this.port = port;
        this.cport = cport;
        this.timeout = timeout;
        this.fileFolder = fileFolder;
    }

    public static void main(String[] args) {
        final int port = Integer.parseInt(args[0]);
        final int cport = Integer.parseInt(args[1]);
        final int timeout = Integer.parseInt(args[2]);
        final String fileFolder = args[3];

    }
}
