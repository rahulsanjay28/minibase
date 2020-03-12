import bigt.Map;
import bigt.Minibase;
import bigt.Scan;
import bigt.Stream;
import btree.*;
import diskmgr.PCounter;
import global.AttrType;
import global.RID;
import global.SystemDefs;
import heap.*;

import java.io.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;

/**
 * compile this file using the command "javac BatchInsert.java"
 * Then run using "java BatchInsert datafilename type bigtablename"
 */

public class BatchInsert {

    private static int maxRowKeyLength = Integer.MIN_VALUE;
    private static int maxColumnKeyLength = Integer.MIN_VALUE;
    private static int maxTimeStampLength = Integer.MIN_VALUE;
    private static int maxValueLength = Integer.MIN_VALUE;

    public static void main(String[] args) throws Exception{
//        batchInsert(args[0], args[1], args[2], args[3]);
    }

    // This class is used for temporary in-memory sorting to check the number of reads and writes, need to remove this
    // once external sort works
    class Temp {
        String rowKey;
        String columnKey;
        String timestamp;
        String value;

        public Temp(String rowKey, String columnKey, String timestamp, String value) {
            this.rowKey = rowKey;
            this.columnKey = columnKey;
            this.timestamp = timestamp;
            this.value = value;
        }
    }

    /**
     * Inserting records into the big table
     *
     * @param dataFileName
     * @param type
     * @param bigTableName
     */
    public void execute(String dataFileName, String type, String bigTableName, String numBuf) throws Exception{

        String line = "";
        try {
            BufferedReader br = new BufferedReader(new FileReader(dataFileName + ".csv"));
            while ((line = br.readLine()) != null) {
                String[] fields = line.split(",");
                updateMaxKeyLengths(fields[0], fields[1], fields[2], fields[3]);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("maxRowKeyLength: " + maxRowKeyLength);
        System.out.println("maxColumnKeyLength: " + maxColumnKeyLength);
        System.out.println("maxTimeStampLength: " + maxTimeStampLength);
        System.out.println("maxValueLength: " + maxValueLength);

        Minibase.getInstance().setMaxRowKeyLength(maxRowKeyLength);
        Minibase.getInstance().setMaxColumnKeyLength(maxColumnKeyLength);
        Minibase.getInstance().setMaxTimeStampLength(maxTimeStampLength);
        Minibase.getInstance().setMaxValueLength(maxValueLength);
        Minibase.getInstance().init(bigTableName, Integer.parseInt(type), Integer.parseInt(numBuf));

        ArrayList<Temp> maps = new ArrayList<>();

        //check if database is restarted, should not insert again
        if(!SystemDefs.MINIBASE_RESTART_FLAG) {
            //Reading data from the csv file and inserting into the BigTable
            line = "";
            try {
                BufferedReader br = new BufferedReader(new FileReader(dataFileName + ".csv"));
                while ((line = br.readLine()) != null) {
                    String[] fields = line.split(",");
                    maps.add(new Temp(fields[0], fields[1], fields[2], fields[3]));
//                    insertMap(fields[0], fields[1], fields[2], fields[3], Integer.parseInt(type));
                }
            } catch (IOException e) {
                e.printStackTrace();
            }

            maps.sort(Comparator.comparing(o -> o.rowKey));

            for(Temp temp : maps){
                insertMap(temp.rowKey, temp.columnKey, temp.timestamp, temp.value, Integer.parseInt(type));
            }
            System.out.println("Total number of pages " + Minibase.getInstance().getBigTable().getCount());
            System.out.println("Total number of index pages " + Minibase.getInstance().getNumberOfIndexPages());
            System.out.println("Max Key entry size " + Minibase.getInstance().getMaxKeyEntrySize());
            System.out.println("Total number of reads " + PCounter.getInstance().getReadCount());
            System.out.println("Total number of writes " + PCounter.getInstance().getWriteCount());
        }else{
            System.out.println("Database already exists");
        }
    }

    private static void updateMaxKeyLengths(String rowKey, String columnKey, String timestamp, String value){
        //update the max lengths of each field in the map to use it indexing

        OutputStream out = new ByteArrayOutputStream();
        DataOutputStream rowStream = new DataOutputStream(out);
        DataOutputStream columnStream = new DataOutputStream(out);
        DataOutputStream timeStampStream = new DataOutputStream(out);
        DataOutputStream valueStream = new DataOutputStream(out);

        try {
            rowStream.writeUTF(rowKey);
            if(rowStream.size() > maxRowKeyLength){
                maxRowKeyLength = rowStream.size();
            }

            columnStream.writeUTF(columnKey);
            if(columnStream.size() > maxColumnKeyLength){
                maxColumnKeyLength = columnStream.size();
            }

            timeStampStream.writeUTF(timestamp);
            if(timeStampStream.size() > maxTimeStampLength){
                maxTimeStampLength = timeStampStream.size();
            }

            valueStream.writeUTF(value);
            if(valueStream.size() > maxValueLength){
                maxValueLength = valueStream.size();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    /**
     * This method will be called for each row in the data file
     *
     * @param rowKey
     * @param columnKey
     * @param timestamp
     * @param value
     */
    private static void insertMap(String rowKey, String columnKey, String timestamp, String value, int type) throws
            Exception {
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
            map1.setTimeStamp(Integer.parseInt(timestamp));
            map1.setValue(value);
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            RID rid = Minibase.getInstance().getBigTable().insertMap(map1.getMapByteArray());

            //inserting into the index file
            if(type == 2) {
                Minibase.getInstance().getBTree().insert(new StringKey(rowKey), rid);
            }else if(type == 3){
                Minibase.getInstance().getBTree().insert(new StringKey(columnKey), rid);
            }
            else if(type == 4){
                Minibase.getInstance().getBTree().insert(new StringKey(rowKey + columnKey), rid);
                Minibase.getInstance().getSecondaryBTree().insert(new IntegerKey(Integer.parseInt(timestamp)), rid);
            }else if(type == 5){
                Minibase.getInstance().getBTree().insert(new StringKey(rowKey + value), rid);
                Minibase.getInstance().getSecondaryBTree().insert(new IntegerKey(Integer.parseInt(timestamp)), rid);
            }
        } catch (InvalidSlotNumberException | InvalidTupleSizeException | SpaceNotAvailableException |
                HFException | HFBufMgrException | HFDiskMgrException | IOException e) {
            e.printStackTrace();
        }
    }
}
