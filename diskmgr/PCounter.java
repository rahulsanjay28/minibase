package diskmgr;

public class PCounter {

    private int readCount;
    private int writeCount;

    private static PCounter mInstance;

    private PCounter() {

    }

    public static PCounter getInstance() {
        if (mInstance == null) {
            mInstance = new PCounter();
        }
        return mInstance;
    }

    public void readIncrement() {
        readCount++;
    }

    public void writeIncrement() {
        writeCount++;
    }

    public int getReadCount() {
        return readCount;
    }

    public int getWriteCount() {
        return writeCount;
    }

}
