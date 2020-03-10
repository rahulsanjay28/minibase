import bigt.Map;
import bigt.Minibase;
import diskmgr.PCounter;
import global.AttrType;
import global.RID;
import global.SystemDefs;
import heap.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

/**
 * compile this file using the command "javac BatchInsert.java"
 * Then run using "java BatchInsert datafilename type bigtablename"
 */

public class BatchInsert {

    static int count = 0;
    static RID tempRID = null;
    public static void main(String[] args) {
        batchInsert(args[0], args[1], args[2], args[3]);
    }

    /**
     * Inserting records into the big table
     * @param dataFileName
     * @param type
     * @param bigTableName
     */
    private static void batchInsert(String dataFileName, String type, String bigTableName, String numBuf){
        Minibase.getInstance().init(bigTableName, Integer.parseInt(type), Integer.parseInt(numBuf));

        //Reading data from the csv file
        String line = "";
        try {
            BufferedReader br = new BufferedReader(new FileReader(dataFileName + ".csv"));
            while((line = br.readLine()) != null){
                String[] fields = line.split(",");
                insertMap(fields[0], fields[1], Integer.parseInt(fields[2]), fields[3]);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        // this code snippet gets any map from the bigtable and deserializes it
        if(tempRID != null){
            try {
                System.out.println(tempRID.slotNo + " " + tempRID.pageNo);
                Map map2 = Minibase.getInstance().getBigTable().getMap(tempRID);
                map2.setOffsets(map2.getOffset());
                System.out.println(map2.getRowLabel());
                System.out.println(map2.getColumnLabel());
                System.out.println(map2.getTimeStamp());
                System.out.println(map2.getValue());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        System.out.println("Total number of reads " + PCounter.getInstance().getReadCount());
        System.out.println("Total number of writes " + PCounter.getInstance().getWriteCount());
    }

    /**
     * This method will be called for each row in the data file
     * @param rowKey
     * @param columnKey
     * @param timestamp
     * @param value
     */
    private static void insertMap(String rowKey, String columnKey, int timestamp, String value){
//        System.out.println(rowKey + " " + columnKey + " " + timestamp + " " + value);

        Map map = new Map();
        AttrType[] attrTypes = new AttrType[4];
        attrTypes[0] = new AttrType(AttrType.attrString);
        attrTypes[1] = new AttrType(AttrType.attrString);
        attrTypes[2] = new AttrType(AttrType.attrInteger);
        attrTypes[3] = new AttrType(AttrType.attrString);

        short[] attrSizes = new short[3];
        attrSizes[0] = (short) (rowKey.getBytes().length);
        attrSizes[1] = (short) (columnKey.getBytes().length);
        attrSizes[2] = (short) (value.getBytes().length);

        try {
            map.setHdr((short) 4, attrTypes, attrSizes);
        } catch (Exception e) {
            System.err.println("*** error in Tuple.setHdr() ***");
            e.printStackTrace();
        }

        Map map1 = new Map(map.size());
        try {
            map1.setHdr((short) 4, attrTypes, attrSizes);
        } catch (Exception e) {
            System.err.println("*** error in Tuple.setHdr() ***");
            e.printStackTrace();
        }

        try {
            map1.setRowLabel(rowKey);
            map1.setColumnLabel(columnKey);
            map1.setTimeStamp(timestamp);
            map1.setValue(value);
        } catch (IOException e) {
            e.printStackTrace();
        }

        ++count;
        try {
            RID rid = Minibase.getInstance().getBigTable().insertMap(map1.getMapByteArray());
            if(count == 1000){
                tempRID = rid;
            }
        } catch (InvalidSlotNumberException | InvalidTupleSizeException | SpaceNotAvailableException |
                HFException | HFBufMgrException | HFDiskMgrException | IOException e) {
            e.printStackTrace();
        }
    }
}
