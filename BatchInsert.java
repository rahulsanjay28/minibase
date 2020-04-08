import bigt.Map;
import bigt.Minibase;
import diskmgr.PCounter;
import global.MID;
import global.MapOrder;
import global.SystemDefs;
import heap.*;
import iterator.FileScan;
import iterator.FldSpec;
import iterator.RelSpec;
import iterator.Sort;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * compile this file using the command "javac BatchInsert.java"
 * Then run using "java BatchInsert datafilename type bigtablename"
 */

public class BatchInsert {

    private int numberOfMapsInserted;
    private int type;

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
        this.type = Integer.parseInt(type);

        String UTF8_BOM = "\uFEFF";
        long startTime = System.currentTimeMillis();
        //Setting the read and write count to zero for every batch insert
        PCounter.getInstance().setReadCount(0);
        PCounter.getInstance().setWriteCount(0);

        Minibase.getInstance().init(bigTableName, Integer.parseInt(numBuf));

        //As we should not use in-memory sorting, we are using sorting tools provided by the minibase
        //This is a temporary heap file used for sorting purposes
        Heapfile tempHeapFile = new Heapfile("batch_insert_temp_heap_file");
        String line = "";
        BufferedReader br = new BufferedReader(new FileReader(dataFileName + ".csv"));
        while ((line = br.readLine()) != null) {
            String[] fields = line.split(",");
            if(fields[0].startsWith(UTF8_BOM)){
                fields[0]=fields[0].substring(1).trim();
            }
            tempHeapFile.insertMap(getMap(fields[0],fields[1],fields[2],fields[3]).getMapByteArray());
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

//        This is for the clustering strategy - Sort uses this ordertype to sort the records

        Minibase.getInstance().setOrderType(1);

        Sort sort = null;
        try {
            sort = new Sort(Minibase.getInstance().getAttrTypes(), (short) 4, Minibase.getInstance().getAttrSizes()
                    , fscan, 1, new MapOrder(MapOrder.Ascending), Minibase.getInstance().getMaxRowKeyLength(), 10);
        } catch (Exception e) {
            e.printStackTrace();
        }

        Map m = sort.get_next();
        int count=0;
        m.setHdr((short) 4, Minibase.getInstance().getAttrTypes(), Minibase.getInstance().getAttrSizes());
        Heapfile tempBTFile = new Heapfile("temp_bt_file");
        List<byte[]> mapList = new ArrayList<>(3);
        String oldMapRowKey = null;
        String oldColumnValue = null;
        FileWriter fw = new FileWriter("abc.csv");

        //Minibase.getInstance().setCheckVersionsEnabled(true);
        while (m != null ) {
            ++numberOfMapsInserted;
            oldMapRowKey= m.getRowLabel();
            oldColumnValue = m.getColumnLabel();
            if(mapList.size() == 3){
                mapList.remove(0);
            }
            //m.print();
            mapList.add(m.getMapByteArray());

            m = sort.get_next();
            if(m!=null ){
               m.setHdr((short) 4, Minibase.getInstance().getAttrTypes(), Minibase.getInstance().getAttrSizes());}
               if(m == null ||(!m.getRowLabel().equals(oldMapRowKey) || !m.getColumnLabel().equals(oldColumnValue))){
                    for(byte[] map : mapList){
                        //System.out.println("I-------------");
                        count++;
                        tempBTFile.insertMap(map);
                        Map ma = new Map(map,0,0);
                        ma.setHdr((short) 4, Minibase.getInstance().getAttrTypes(), Minibase.getInstance().getAttrSizes());
                        ma.print();
                        fw.write( "\n"+ma.getRowLabel()+","+ma.getColumnLabel()+","+ma.getValue()+","+ma.getTimeStamp());
                    }
                   //System.out.println("$$$$$$$$$$-------------");
                    mapList.clear();
                }



        }
        System.out.println(count);
        fw.close();

//        Scan sc = tempBTFile.openScan();
//        MID mid = new MID();
//        Map m1 = sc.getNext(mid);
//        int count=0;
//        while(m1!=null){
//            m1.setHdr((short) 4, Minibase.getInstance().getAttrTypes(), Minibase.getInstance().getAttrSizes());
//            m1.print();
//            count++;
//            m1= sc.getNext(mid);
//        }
//        System.out.println(count);

        sort.close();
        long endTime = System.currentTimeMillis();
        System.out.println("Total time taken in minutes " + (endTime - startTime)/(1000*60));
//        System.out.println("Number of maps inserted into the big table in this batch insertion " + numberOfMapsInserted);
        System.out.println("Total maps in the Big Table " + Minibase.getInstance().getBigTable().getMapCount());
        System.out.println("Total number of reads " + PCounter.getInstance().getReadCount());
        System.out.println("Total number of writes " + PCounter.getInstance().getWriteCount());

        //deleting the temp heap file used for sorting purposes
        tempHeapFile.deleteFile();

        //This ensures flushing all the pages to disk
        SystemDefs.JavabaseBM.setNumBuffers(0);
    }

    private Map getMap(String rowKey, String columnKey, String value, String timestamp) {
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
            map1.setValue(Minibase.getInstance().getTransformedValue(value));
        } catch (IOException e) {
            e.printStackTrace();
        }

        return map1;
    }

//    private boolean checkVersions(Map newMap) throws Exception {
//        Stream stream = null;
//        boolean readyToInsert = true;
//        try {
//            stream = Minibase.getInstance().getBigTable().openStream(6, newMap.getRowLabel(), newMap.getColumnLabel(),
//                    "*");
//            if (stream == null) {
//                System.out.println("Yet to initialize the stream");
//            } else {
//                Map[] map = new Map[stream.getMidCount()];
//                MID[] mids = stream.getMids();
//                for (int i = 0; i < stream.getMidCount(); i++) {
//                    map[i] = Minibase.getInstance().getBigTable().getMap(mids[i]);
//                    map[i].setHdr((short) 4, Minibase.getInstance().getAttrTypes(), Minibase.getInstance().getAttrSizes());
//                    if(newMap.getTimeStamp() == map[i].getTimeStamp()){
//                        readyToInsert = false;
//                        --numberOfMapsInserted;
//                    }
//                }
//                    if(readyToInsert && stream.getMidCount() == 3) {
//                        int deleteMID = -1;
//                        if ((map[0].getTimeStamp() < map[1].getTimeStamp()) && (map[0].getTimeStamp() < map[2].getTimeStamp())) {
//                            deleteMID = 0;
//                        } else if ((map[1].getTimeStamp() < map[0].getTimeStamp()) && (map[1].getTimeStamp() < map[2].getTimeStamp())) {
//                            deleteMID = 1;
//                        } else {
//                            deleteMID = 2;
//                        }
//                        stream.findAndDeleteMap(mids[deleteMID]);
//                        --numberOfMapsInserted;
//                    }
//
//                stream.closeStream();
//            }
//        } catch (Exception e) {
//            if(stream != null) {
//                try {
//                    stream.closeStream();
//                } catch (Exception ex) {
//                    ex.printStackTrace();
//                }
//            }
//            e.printStackTrace();
//        }
//        return readyToInsert;
//    }
}


