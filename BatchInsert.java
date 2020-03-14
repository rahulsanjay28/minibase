import bigt.BTreeData;
import bigt.Map;
import bigt.Minibase;
import bigt.Stream;
import btree.*;
import diskmgr.PCounter;
import global.MapOrder;
import global.RID;
import heap.*;
import iterator.*;

import java.io.*;

/**
 * compile this file using the command "javac BatchInsert.java"
 * Then run using "java BatchInsert datafilename type bigtablename"
 */

public class BatchInsert {

    private static int maxRowKeyLength = Integer.MIN_VALUE;
    private static int maxColumnKeyLength = Integer.MIN_VALUE;
    private static int maxTimeStampLength = Integer.MIN_VALUE;
    private static int maxValueLength = Integer.MIN_VALUE;

    public static void main(String[] args) throws Exception {
//        execute(args[0], args[1], args[2], args[3]);
    }

    /**
     * Inserting records into the big table
     *
     * @param dataFileName
     * @param type
     * @param bigTableName
     */
    public void execute(String dataFileName, String type, String bigTableName, String numBuf) throws Exception {

        //Finding the max lengths of rowKey, columnKey, timeStamp and value
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

        //We should set these lengths before calling Minibase.getInstance().init()
        Minibase.getInstance().setMaxRowKeyLength(maxRowKeyLength);
        Minibase.getInstance().setMaxColumnKeyLength(maxColumnKeyLength);
        Minibase.getInstance().setMaxTimeStampLength(maxTimeStampLength);
        Minibase.getInstance().setMaxValueLength(maxValueLength);

        Minibase.getInstance().init(bigTableName, Integer.parseInt(type), Integer.parseInt(numBuf));

        //As we should not use in-memory sorting, we are using sorting tools provided by the minibase
        //This is a temporary heap file used for sorting purposes
        Heapfile tempHeapFile = new Heapfile("tempFile");
        try {
            BufferedReader br = new BufferedReader(new FileReader(dataFileName + ".csv"));
            while ((line = br.readLine()) != null) {
                String[] fields = line.split(",");
                tempHeapFile.insertMap(getMap(fields[0], fields[1], fields[2], fields[3]).getMapByteArray());
            }
        } catch (IOException e) {
            e.printStackTrace();
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
            fscan = new FileScan("tempFile", Minibase.getInstance().getAttrTypes(),
                    Minibase.getInstance().getAttrSizes(), (short) 4, 4, projlist, null);
        } catch (Exception e) {
            e.printStackTrace();
        }

        Sort sort = null;
        try {
            sort = new Sort(Minibase.getInstance().getAttrTypes(), (short) 4, Minibase.getInstance().getAttrSizes()
                    , fscan, 1, new MapOrder(MapOrder.Ascending), Minibase.getInstance().getMaxRowKeyLength(), 240);
        } catch (Exception e) {
            e.printStackTrace();
        }

        Map m = sort.get_next();
        while (m != null) {
            m.setHdr((short) 4, Minibase.getInstance().getAttrTypes(), Minibase.getInstance().getAttrSizes());
            insertMap(m, Integer.parseInt(type));
            m = sort.get_next();
        }

        System.out.println("Total number of pages " + Minibase.getInstance().getBigTable().getCount());
        System.out.println("Total number of index pages " + Minibase.getInstance().getNumberOfIndexPages());
        System.out.println("Max Key entry size " + Minibase.getInstance().getMaxKeyEntrySize());
        System.out.println("Total number of reads " + PCounter.getInstance().getReadCount());
        System.out.println("Total number of writes " + PCounter.getInstance().getWriteCount());
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

    private static void updateMaxKeyLengths(String rowKey, String columnKey, String timestamp, String value) {
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

    /**
     * This method will be called for each row in the data file
     *
     * @param map
     * @param type
     */
    private static void insertMap(Map map, int type) throws
            Exception {
        try {
            //This method takes care of maintaining only 3 versions of a map at any instant
            //Need to uncomment this once filtering and ordering works
//            batchInsert.checkVersions(map1);
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

    private void checkVersions(Map map) {
        int indexType = Minibase.getInstance().getBigTable().getType();
        Stream stream;
        BTreeData bData;
        BTFileScan scan;
        BTreeData[] bDataMaps = new BTreeData[50]; //change it to 3
        //System.out.println("----------------------New map--------------------------");
        //map.print();
        if (Minibase.getInstance().getBigTable().getType() == 1) {

        } else {
            try {
                stream = Minibase.getInstance().getBigTable().openStream(6, map.getRowLabel(), map.getColumnLabel(), "*");
                bData = stream.getNextMap();
                int i = 0;
                while (bData != null) {

                    bDataMaps[i] = bData;
                    //bData.getMap().print(); //To check the index
                    i++;
                    bData = stream.getNextMap();
                }
                if (i == 3) {
                    //delete Map'
                    //System.out.println("---%%%%%--------------i == 3 ----------%%%%%----------");
                    Minibase.getInstance().getBigTable().deleteMap(bDataMaps[0].getRid());

                    //Delete Index of deleted map1
                    if (Minibase.getInstance().getBTree().Delete(bDataMaps[0].getKey(), bDataMaps[0].getRid())) {
                        //System.out.println(" @ @  Deleted " );
                        //bDataMaps[0].getMap().print();

                    /*System.out.println("********* After deleting the index **********");
                    stream = Minibase.getInstance().getBigTable().openStream(6, map.getRowLabel(), map.getColumnLabel(), "*");
                    bData = stream.getNextMap();
                    bData.getMap().print();
                    while (bData != null) {

                        bData.getMap().print();
                        bData = stream.getNextMap();
                    }*/
                    }
                    if (Minibase.getInstance().getBigTable().getType() == 4 || Minibase.getInstance().getBigTable().getType() == 5) {
                        scan = Minibase.getInstance().getSecondaryBTree().new_scan(new StringKey(Integer.toString(bDataMaps[0].getMap().getTimeStamp())), new StringKey(Integer.toString(bDataMaps[0].getMap().getTimeStamp())));
                        KeyDataEntry entry = scan.get_next();
                        bData = null;
                        while (entry != null) {

                            RID rid = ((LeafData) entry.data).getData();
                            if (rid != null) {
                                try {
                                    Map map2 = Minibase.getInstance().getBigTable().getMap(rid);
                                    map2.setOffsets(map2.getOffset());
                                    if (MapUtils.Equal(bDataMaps[0].getMap(), map2)) {
                                        Minibase.getInstance().getSecondaryBTree().Delete(entry.key, rid);
                                    }
                                    entry = scan.get_next();
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                            }

                        }
                    }

                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }
}
