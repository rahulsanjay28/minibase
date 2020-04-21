package bigt;

import diskmgr.DiskMgrException;
import diskmgr.FileIOException;
import diskmgr.InvalidPageNumberException;
import global.AttrType;
import global.PageId;
import global.SystemDefs;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;

import java.io.*;

public class Minibase {

    private static Minibase mInstance;
    private BigTable bigTable;

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

    public BigTable getBigTable() {
        return bigTable;
    }

    public void init(String bigTableName, int numBuf) {

        String dbpath = "/tmp/big_db";
        SystemDefs systemDefs = new SystemDefs(dbpath, 100000, numBuf, "Clock");

        if (bigTableName.contains("join")) {
            maxColumnKeyLength = maxRowKeyLength + maxColumnKeyLength + 1;
            maxRowKeyLength = maxRowKeyLength * 2 + 1;
        }

//        System.out.println("maxRowKeyLength: " + maxRowKeyLength);
//        System.out.println("maxColumnKeyLength: " + maxColumnKeyLength);
//        System.out.println("maxTimeStampLength: " + maxTimeStampLength);
//        System.out.println("maxValueLength: " + maxValueLength);

        attrTypes = new AttrType[4];
        attrTypes[0] = new AttrType(AttrType.attrString);
        attrTypes[1] = new AttrType(AttrType.attrString);
        attrTypes[2] = new AttrType(AttrType.attrInteger);
        attrTypes[3] = new AttrType(AttrType.attrString);

        attrSizes = new short[3];
        attrSizes[0] = (short) (maxRowKeyLength);
        attrSizes[1] = (short) (maxColumnKeyLength);
        attrSizes[2] = (short) (maxValueLength);

        bigTable = new BigTable();

        if(!bigTableName.isEmpty()) {
            udpateCatalog(bigTableName);
            for (int type = 1; type <= 5; ++type) {
                try {
                    BigT bigT = new BigT(bigTableName, type);
                    bigTable.addBigTablePart(bigT);
                } catch (HFException | HFBufMgrException | HFDiskMgrException | IOException e) {
                    e.printStackTrace();
                }
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

    private void udpateCatalog(String bigTableName){
        boolean bigTableFound = false;
        for (int type = 1; type <= 5; ++type) {
            try {
                PageId tmpId = new PageId();
                tmpId = SystemDefs.JavabaseDB.get_file_entry(bigTableName + type);
                if(tmpId != null){
                    bigTableFound = true;
                }
            } catch (IOException | FileIOException | InvalidPageNumberException | DiskMgrException e) {
                e.printStackTrace();
            }
        }
        if(!bigTableFound){
            try {
                System.out.println("BigTable not found, adding it to the catalog");
                BigTableCatalog.addBigTInfo(new BigTableInfo(bigTableName));
            } catch (Exception e) {
                e.printStackTrace();
            }
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

    public String getTransformedValue(String value) {
        //Tranform the value so that comparisions will be correct
        int numberOfZerosToAppend = Minibase.getInstance().getMaxValueLength() - value.length();
        StringBuilder transFormedValue = new StringBuilder();
        for (int i = 0; i < numberOfZerosToAppend; ++i) {
            transFormedValue.append("0");
        }
        transFormedValue.append(value);
        return transFormedValue.toString();
    }
}
