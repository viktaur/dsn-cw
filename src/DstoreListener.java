/**
 * Used to communicate with the Controller from the Dstore outside the NetworkDstore class
 */
public interface DstoreListener {

    void fileStored(String fileName);
    void fileRemoved(String fileName);
    void errorFileDoesNotExist(String fileName);
}
