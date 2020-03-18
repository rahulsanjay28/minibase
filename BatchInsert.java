import bigt.Map;
import bigt.Minibase;
import bigt.Stream;
import btree.IntegerKey;
import btree.StringKey;
import diskmgr.PCounter;
import global.MapOrder;
import global.RID;
import global.SystemDefs;
import heap.*;
import iterator.FileScan;
import iterator.FldSpec;
import iterator.RelSpec;
import iterator.Sort;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;

/**
 * compile this file using the command "javac BatchInsert.java"
 * Then run using "java BatchInsert datafilename type bigtablename"
 */

public class BatchInsert {

    public static void main(String[] args) throws Exception {
        BatchInsert batchInsert = new BatchInsert();
        batchInsert.execute(args[0], args[1], args[2], args[3]);
    }

    /**
     * Inserting records into the big table
     *
     * @param dataFileName
     * @param type
     * @param bigTableName
     */
    public void execute(String dataFileName, String type, String bigTableName, String numBuf) throws Exception {

        //Setting the read and write count to zero for every batch insert
        PCounter.getInstance().setReadCount(0);
        PCounter.getInstance().setWriteCount(0);

        Minibase.getInstance().init(dataFileName, bigTableName, Integer.parseInt(type), Integer.parseInt(numBuf));

        //As we should not use in-memory sorting, we are using sorting tools provided by the minibase
        //This is a temporary heap file used for sorting purposes
        Heapfile tempHeapFile = new Heapfile("batch_insert_temp_heap_file");
        String line = "";
        BufferedReader br = new BufferedReader(new FileReader(dataFileName + ".csv"));
        while ((line = br.readLine()) != null) {
            String[] fields = line.split(",");
            tempHeapFile.insertMap(getMap(fields[0], fields[1], fields[2], fields[3]).getMapByteArray());
        }

        // create an iterator by open a file scan
        FldSpec[] projlist = new FldSpec[4];
        RelSpec rel = new RelSpec(RelSpec.outer);
        projlist[0] = new FldSpec(rel, 1);
        projlist[1] = new FldSpec(rel, 2);
        projlist[2] = new FldSpec(rel, 3);
        projlist[3] = new FldSpec(rel, 4);

        FileScan fscan = null;

        try {
            fscan = new FileScan("batch_insert_temp_heap_file", Minibase.getInstance().getAttrTypes(),
                    Minibase.getInstance().getAttrSizes(), (short) 4, 4, projlist, null);
        } catch (Exception e) {
            e.printStackTrace();
        }

        Sort sort = null;
        try {
            sort = new Sort(Minibase.getInstance().getAttrTypes(), (short) 4, Minibase.getInstance().getAttrSizes()
                    , fscan, 1, new MapOrder(MapOrder.Ascending), Minibase.getInstance().getMaxRowKeyLength(), 10);
        } catch (Exception e) {
            e.printStackTrace();
        }
        HashSet set_row = new HashSet();
        HashSet set_col = new HashSet();

        Map m = sort.get_next();
        Minibase.getInstance().setCheckVersionsEnabled(true);
        while (m != null) {
            m.setHdr((short) 4, Minibase.getInstance().getAttrTypes(), Minibase.getInstance().getAttrSizes());
            checkVersions(m);
            insertMap(m, Integer.parseInt(type));
            set_row.add(m.getRowLabel());
            set_col.add(m.getColumnLabel());
            m = sort.get_next();
        }
        sort.close();
        int row_count = set_row.size();
        Minibase.getInstance().setDistinctRowCount(row_count);
        int col_count = set_col.size();
        Minibase.getInstance().setDistinctColumnCount(col_count);
        //this.getDistinctCount();
        System.out.println("Total number of pages " + Minibase.getInstance().getBigTable().getCount());
        System.out.println("Total number of index pages " + Minibase.getInstance().getNumberOfIndexPages());
        System.out.println("Total number of reads " + PCounter.getInstance().getReadCount());
        System.out.println("Total number of writes " + PCounter.getInstance().getWriteCount());
        //System.out.println("Total number of map count " + Minibase.getInstance().getMapCount());
        //System.out.println("Total number of distinct rows " + Minibase.getInstance().getDistinctRowCount());
        System.out.println("Total number of distinct rows " + Minibase.getInstance().getDistinctRowCount());
        System.out.println("Total number of distinct columns " + Minibase.getInstance().getDistinctColumnCount());
        //System.out.println("Total number of distinct columns " + Minibase.getInstance().getDistinctColumnCount());

        //deleting the temp heap file used for sorting purposes
        tempHeapFile.deleteFile();

        //This ensures flushing all the pages to disk
        SystemDefs.JavabaseBM.setNumBuffers(0);
    }

    private Map getMap(String rowKey, String columnKey, String timestamp, String value) {
        Map map = new Map();
        try {
            map.setHdr((short) 4, Minibase.getInstance().getAttrTypes(), Minibase.getInstance().getAttrSizes());
        } catch (Exception e) {
            System.err.println("*** error in Tuple.setHdr() ***");
            e.printStackTrace();
        }

        Map map1 = new Map(map.size());
        try {
            map1.setHdr((short) 4, Minibase.getInstance().getAttrTypes(), Minibase.getInstance().getAttrSizes());
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

        return map1;
    }

    /**
     * This method will be called for each row in the data file
     *
     * @param map
     * @param type
     */
    private void insertMap(Map map, int type) throws
            Exception {

        try {
            //This method takes care of maintaining only 3 versions of a map at any instant
            //Need to uncomment this once filtering and ordering works

            RID rid = Minibase.getInstance().getBigTable().insertMap(map.getMapByteArray());

            //inserting into the index file
            if (type == 2) {
                Minibase.getInstance().getBTree().insert(new StringKey(map.getRowLabel()), rid);
            } else if (type == 3) {
                Minibase.getInstance().getBTree().insert(new StringKey(map.getColumnLabel()), rid);
            } else if (type == 4) {
                Minibase.getInstance().getBTree().insert(new StringKey(map.getRowLabel() + map.getColumnLabel()),
                        rid);
                Minibase.getInstance().getSecondaryBTree().insert(new IntegerKey(map.getTimeStamp()), rid);
            } else if (type == 5) {
                Minibase.getInstance().getBTree().insert(new StringKey(map.getRowLabel() + map.getValue()), rid);
                Minibase.getInstance().getSecondaryBTree().insert(new IntegerKey(map.getTimeStamp()), rid);
            }
        } catch (InvalidSlotNumberException | InvalidTupleSizeException | SpaceNotAvailableException |
                HFException | HFBufMgrException | HFDiskMgrException | IOException e) {
            e.printStackTrace();
        }
    }

    private void checkVersions(Map newMap) {
        Stream stream = null;
        try {
            stream = Minibase.getInstance().getBigTable().openStream(6, newMap.getRowLabel(), newMap.getColumnLabel(), "*");
            if (stream == null) {
                System.out.println("Yet to initialize the stream");
            } else {
                if (stream.getRidCount() == 3) {
                    Map[] map = new Map[3];
                    RID[] rids = stream.getRids();

                    for (int i = 0; i < 3; i++) {
                        map[i] = Minibase.getInstance().getBigTable().getMap(rids[i]);
                        map[i].setHdr((short) 4, Minibase.getInstance().getAttrTypes(), Minibase.getInstance().getAttrSizes());
                    }
                    RID deleteRID = null;
                    if ((map[0].getTimeStamp() < map[1].getTimeStamp()) && (map[0].getTimeStamp() < map[2].getTimeStamp())) {
                        deleteRID = rids[0];
                    } else if ((map[1].getTimeStamp() < map[0].getTimeStamp()) && (map[1].getTimeStamp() < map[2].getTimeStamp())) {
                        deleteRID = rids[0];
                    } else {
                        deleteRID = rids[0];
                    }
                    stream.findAndDeleteMap(deleteRID);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void getDistinctCount() {
        HashSet set_row = new HashSet();
        HashSet set_col = new HashSet();
        HashSet set_map = new HashSet();
        Stream stream = null;
        try {
            stream = Minibase.getInstance().getBigTable().openStream(1, "*", "*", "*");

            if (stream == null) {
                System.out.println("Yet to initialize the stream");
                return;
            } else {
                Map map = stream.getNext();
                while (map != null) {
                    map.print();
                    set_row.add(map.getRowLabel().trim());
                    set_col.add(map.getColumnLabel().trim());
                    set_map.add(map.getRowLabel().trim() + map.getColumnLabel().trim());
                    map = stream.getNext();
                }

                Minibase.getInstance().setDistinctRowCount(set_row.size());
                Minibase.getInstance().setDistinctColumnCount(set_col.size());
//                Minibase.getInstance().setMapCount(set_map.size());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}


