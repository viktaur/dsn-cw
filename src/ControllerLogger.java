public class ControllerLogger extends Logger {

    private static ControllerLogger controllerLogger = null;

    public static synchronized void init(Logger.LoggingType loggingType) {
        if (controllerLogger == null) {
            controllerLogger = new ControllerLogger(loggingType);
        }
    }
    protected ControllerLogger(LoggingType loggingType) {
        super(loggingType);
    }

    public static ControllerLogger getInstance() {
        if (controllerLogger == null) {
            throw new RuntimeException("ControllerLogger has not been initialised yet");
        } else {
            return controllerLogger;
        }
    }

    @Override
    protected String getLogFileSuffix() {
        return "controller";
    }

    protected void couldNotHandleMessage(String msg) {
        this.log("Could not handle message " + msg);
    }

    public void dstoreWhereToLoadFrom(String filename, int dstorePort, int filesize) {
        this.log("Controller replied to load file " + filename + " (size: " + filesize + " bytes) from Dstore " + dstorePort);
    }

    public void storeToDstoreFailed(String filename, int dstorePort) {
        this.log("Store of file " + filename + " to Dstore " + dstorePort + " failed");
    }
    public void storeToDstoreCompleted(String filename, int dstorePort) {
        this.log("Store of file " + filename + " to Dstore " + dstorePort + " successfully completed");
    }

    public void removeFromDstoreCompleted(String filename, int dstorePort) {
        this.log("Remove of file " + filename + " from Dstore " + dstorePort + " successfully completed");
    }

    public void storeCompleted(String filename) {
        this.log("Store operation for file " + filename + " completed");
    }

    public void loadFromDstore(String filename, int dstorePort) {
        this.log("Loading file " + filename + " from Dstore " + dstorePort);
    }

    public void loadFromDstoreFailed(String filename, int dstorePort) {
        this.log("Load operation for file " + filename + " from Dstore " + dstorePort + " failed");
    }

    public void loadFailed(String filename, int dstoreCount) {
        this.log("Load operation for file " + filename + " failed after having contacted " + dstoreCount + " different Dstores");
    }

    public void fileToLoadDoesNotExist(String filename) {
        this.log("Load operation failed because file does not exist (filename: " + filename + ")");
    }

    public void loadCompleted(String filename, int dstoreCount) {
        this.log("Load operation of file " + filename + " from Dstore " + dstoreCount + " successfully completed");
    }

    public void removeStarted(String filename) {
        this.log("Remove operation for file " + filename + " started");
    }

    public void fileToRemoveDoesNotExist(String filename) {
        this.log("Remove operation failed because file does not exist (filename: " + filename + ")");
    }

    public void removeComplete(String filename) {
        this.log("Remove operation for file " + filename + " successfully completed");
    }

    public void removeFailed(String filename) {
        this.log("Remove operation for file " + filename + " not completed successfully");
    }

    public void listStarted() {
        this.log("List operation started");
    }

    public void listFailed() {
        this.log("List operation failed");
    }

    public void listCompleted() {
        this.log("List operation successfully completed");
    }
}
