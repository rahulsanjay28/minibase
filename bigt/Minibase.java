package bigt;

import btree.AddFileEntryException;
import btree.BTreeFile;
import btree.ConstructPageException;
import btree.GetFileEntryException;
import global.AttrType;
import global.SystemDefs;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;

import java.io.IOException;

public class Minibase {

    private static Minibase mInstance;
    private BigT bigT;
    private BTreeFile bTreeFile;
    private BTreeFile bTreeFile1;

    private int maxRowKeyLength;
    private int maxColumnKeyLength;
    private int maxTimeStampLength;
    private int maxValueLength;
    private int numberOfIndexPages = 0;
    private int maxKeyEntrySize = Integer.MAX_VALUE;
    private int distinctRowCount;
    private int distinctColumnCount;

    private AttrType[] attrTypes;
    private short[] attrSizes;

    private Minibase() {

    }

    public static Minibase getInstance() {
        if (mInstance == null) {
            mInstance = new Minibase();
        }
        return mInstance;
    }

    public BigT getBigTable() {
        return bigT;
    }

    public void init(String name, int type, int numBuf) {
        String dbpath = "/tmp/" + name + type + ".bigtable-db";
        SystemDefs systemDefs = new SystemDefs(dbpath, 20000, numBuf, "Clock");

        attrTypes = new AttrType[4];
        attrTypes[0] = new AttrType(AttrType.attrString);
        attrTypes[1] = new AttrType(AttrType.attrString);
        attrTypes[2] = new AttrType(AttrType.attrInteger);
        attrTypes[3] = new AttrType(AttrType.attrString);

        attrSizes = new short[3];
        attrSizes[0] = (short) (maxRowKeyLength);
        attrSizes[1] = (short) (maxColumnKeyLength);
        attrSizes[2] = (short) (maxValueLength);

        try {
            bigT = new BigT(name, type);
        } catch (HFException e) {
            e.printStackTrace();
        } catch (HFBufMgrException e) {
            e.printStackTrace();
        } catch (HFDiskMgrException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        int keySize = -1;
        if (type == 2) {
            keySize = maxRowKeyLength;
        } else if (type == 3) {
            keySize = maxColumnKeyLength;
        } else if (type == 4) {
            keySize = maxColumnKeyLength + maxRowKeyLength;
        } else if (type == 5) {
            keySize = maxRowKeyLength + maxValueLength;
        }

        if (type != 0) {
            try {
                bTreeFile = new BTreeFile(name + type + "_index", AttrType.attrString, keySize, 1);
                BTreeFile.traceFilename("TRACE");
            } catch (GetFileEntryException | ConstructPageException | IOException | AddFileEntryException e) {
                e.printStackTrace();
            }
        }

        if (type == 4 || type == 5) {
            try {
                bTreeFile1 = new BTreeFile(name + type + "_index_1", AttrType.attrInteger, 4, 1);
            } catch (GetFileEntryException | ConstructPageException | IOException | AddFileEntryException e) {
                e.printStackTrace();
            }
        }
    }

    public BTreeFile getBTree() {
        return bTreeFile;
    }

    public BTreeFile getSecondaryBTree() {
        return bTreeFile1;
    }

    public void setMaxRowKeyLength(int maxRowKeyLength) {
        this.maxRowKeyLength = maxRowKeyLength;
    }

    public void setMaxColumnKeyLength(int maxColumnKeyLength) {
        this.maxColumnKeyLength = maxColumnKeyLength;
    }

    public void setMaxTimeStampLength(int maxTimeStampLength) {
        this.maxTimeStampLength = maxTimeStampLength;
    }

    public void setMaxValueLength(int maxValueLength) {
        this.maxValueLength = maxValueLength;
    }

    public int getMaxRowKeyLength() {
        return maxRowKeyLength;
    }

    public int getMaxColumnKeyLength() {
        return maxColumnKeyLength;
    }

    public int getMaxTimeStampLength() {
        return maxTimeStampLength;
    }

    public int getMaxValueLength() {
        return maxValueLength;
    }

    public int getNumberOfIndexPages() {
        return numberOfIndexPages;
    }

    public void setNumberOfIndexPages(int numberOfIndexPages) {
        this.numberOfIndexPages = numberOfIndexPages;
    }

    public void incrementNumberOfIndexPages() {
        ++numberOfIndexPages;
    }

    public void setMaxKeyEntrySize(int size) {
        if (size < maxKeyEntrySize) {
            maxKeyEntrySize = size;
        }
    }

    public int getMaxKeyEntrySize() {
        return maxKeyEntrySize;
    }

    public AttrType[] getAttrTypes() {
        return attrTypes;
    }

    public short[] getAttrSizes() {
        return attrSizes;
    }

    public int getDistinctRowCount() {
        return distinctRowCount;
    }

    public void setDistinctRowCount(int distinctRowCount) {
        this.distinctRowCount = distinctRowCount;
    }

    public int getDistinctColumnCount() {

        return distinctColumnCount;
    }

    public void setDistinctColumnCount(int distinctColumnCount) {
        this.distinctColumnCount = distinctColumnCount;
    }
}
