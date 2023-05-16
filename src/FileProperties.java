import java.net.Socket;
import java.util.ArrayList;

public class FileProperties {
    private final int fileSize;
    private FileStatus status;
    private ArrayList<NetworkController.DstoreThread> dstores;

    public FileProperties(int fileSize, FileStatus status, ArrayList<NetworkController.DstoreThread> dstores) {
        this.fileSize = fileSize;
        this.status = status;
        this.dstores = dstores;
    }

    enum FileStatus {
        STORE_IN_PROGRESS,
        STORE_COMPLETE,
        REMOVE_IN_PROGRESS,
        REMOVE_COMPLETE
    }

    public int getFileSize() {
        return fileSize;
    }

    public FileStatus getStatus() {
        return status;
    }

    public void setStatus(FileStatus status) {
        this.status = status;
    }

    public ArrayList<NetworkController.DstoreThread> getDstores() {
        return dstores;
    }

    public boolean addDstore(NetworkController.DstoreThread dstore) {
        return this.dstores.add(dstore);
    }

    public boolean removeDstore(NetworkController.DstoreThread dstore) {
        return this.dstores.remove(dstore);
    }

    public int getCount() {
        return dstores.size();
    }

    public boolean storeIsInProgress() {
        return this.status == FileStatus.STORE_IN_PROGRESS;
    }

    public boolean storeIsCompleted() {
        return this.status == FileStatus.STORE_COMPLETE;
    }

    public boolean removeIsInProgress() {
        return this.status == FileStatus.REMOVE_IN_PROGRESS;
    }

    public boolean removeIsCompleted() {
        return this.status == FileStatus.REMOVE_COMPLETE;
    }
}
