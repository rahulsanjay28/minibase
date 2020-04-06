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

import java.io.*;

public class Minibase {

    private static Minibase mInstance;
    private BigT bigT;
    private BTreeFile bTreeFile;

    private int maxRowKeyLength = 19;
    private int maxColumnKeyLength = 17;
    private int maxTimeStampLength = 5;
    private int maxValueLength = 5;

    private int numberOfIndexPages = 0;
    private int maxKeyEntrySize = Integer.MAX_VALUE;
    private int distinctRowCount;
    private int distinctColumnCount;

    private AttrType[] attrTypes;
    private short[] attrSizes;

    private int orderType;
    private boolean CHECK_VERSIONS_ENABLED = false;

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

    public void init(String dataFileName, String name, int type, int numBuf) {
//        if (dataFileName != null && dataFileName.length() != 0) {
//            findMaxKeyLengths(dataFileName);
//        }

        String dbpath = "/tmp/big_db";
        SystemDefs systemDefs = new SystemDefs(dbpath, 100000, numBuf, "Clock");

        System.out.println("maxRowKeyLength: " + maxRowKeyLength);
        System.out.println("maxColumnKeyLength: " + maxColumnKeyLength);
        System.out.println("maxTimeStampLength: " + maxTimeStampLength);
        System.out.println("maxValueLength: " + maxValueLength);

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
        } catch (HFException | HFBufMgrException | HFDiskMgrException | IOException e) {
            e.printStackTrace();
        }

        int keySize = -1;
        if (type == 2) {
            keySize = maxRowKeyLength + 2;
        } else if (type == 3) {
            keySize = maxColumnKeyLength + 2;
        } else if (type == 4) {
            keySize = maxColumnKeyLength + maxRowKeyLength + 4;
        } else if (type == 5) {
            keySize = maxRowKeyLength + maxValueLength + 4;
        }

        if (type != 0) {
            try {
                bTreeFile = new BTreeFile(name + type + "_index", AttrType.attrString, keySize, 0);
            } catch (GetFileEntryException | ConstructPageException | IOException | AddFileEntryException e) {
                e.printStackTrace();
            }
        }
    }

    private void findMaxKeyLengths(String dataFileName) {
        //Finding the max lengths of rowKey, columnKey, timeStamp and value for the data provided
        try {
            String line = "";
            BufferedReader br = new BufferedReader(new FileReader(dataFileName + ".csv"));
            while ((line = br.readLine()) != null) {
                String[] fields = line.split(",");
                updateMaxKeyLengths(fields[0], fields[1], fields[2], fields[3]);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void updateMaxKeyLengths(String rowKey, String columnKey, String value, String timestamp) {
        //update the max lengths of each field in the map to use it indexing

        OutputStream out = new ByteArrayOutputStream();
        DataOutputStream rowStream = new DataOutputStream(out);
        DataOutputStream columnStream = new DataOutputStream(out);
        DataOutputStream timeStampStream = new DataOutputStream(out);
        DataOutputStream valueStream = new DataOutputStream(out);

        try {
            rowStream.writeUTF(rowKey);
            if (rowStream.size() > maxRowKeyLength) {
                maxRowKeyLength = rowStream.size();
            }

            columnStream.writeUTF(columnKey);
            if (columnStream.size() > maxColumnKeyLength) {
                maxColumnKeyLength = columnStream.size();
            }

            timeStampStream.writeUTF(timestamp);
            if (timeStampStream.size() > maxTimeStampLength) {
                maxTimeStampLength = timeStampStream.size();
            }

            valueStream.writeUTF(value);
            if (valueStream.size() > maxValueLength) {
                maxValueLength = valueStream.size();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public BTreeFile getBTree() {
        return bTreeFile;
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

    public int getOrderType() {
        return orderType;
    }

    public void setOrderType(int orderType) {
        this.orderType = orderType;
    }

    public boolean isCheckVersionsEnabled() {
        return CHECK_VERSIONS_ENABLED;
    }

    public void setCheckVersionsEnabled(boolean flag) {
        CHECK_VERSIONS_ENABLED = flag;
    }

    public String getTransformedValue(String value){
        //Tranform the value so that comparisions will be correct
        int numberOfZerosToAppend = Minibase.getInstance().getMaxValueLength() - value.length();
        StringBuilder transFormedValue = new StringBuilder();
        for(int i=0;i<numberOfZerosToAppend;++i){
            transFormedValue.append("0");
        }
        transFormedValue.append(value);
        return transFormedValue.toString();
    }
}
