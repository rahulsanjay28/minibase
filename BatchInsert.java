import Utility.GetMap;
import bigt.Map;
import bigt.Minibase;
import diskmgr.PCounter;
import global.MapOrder;
import global.SystemDefs;
import heap.Heapfile;
import iterator.FileScan;
import iterator.FldSpec;
import iterator.RelSpec;
import iterator.Sort;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
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

        //Initializing Minibase
        Minibase.getInstance().init(bigTableName, Integer.parseInt(numBuf));

        //As we should not use in-memory sorting, we are using sorting tools provided by the minibase
        //This is a temporary heap file used for sorting purposes
        Heapfile tempHeapFile = new Heapfile("batch_insert_temp_heap_file");
        String line = "";
        BufferedReader br = new BufferedReader(new FileReader(dataFileName + ".csv"));
        while ((line = br.readLine()) != null) {
            String[] fields = line.split(",");
            if (fields[0].startsWith(UTF8_BOM)) {
                fields[0] = fields[0].substring(1).trim();
            }
            if (fields[2].startsWith("0")) {
                fields[2] = fields[2].substring(1).trim();
            }
            tempHeapFile.insertMap(GetMap.getMap(fields[0], fields[1], fields[2], fields[3]).getMapByteArray());
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

        //This is for the clustering strategy - Sort uses this ordertype to sort the records
        Minibase.getInstance().setOrderType(1);

        int memory = Minibase.getInstance().getNumberOfBuffersAvailable();
        Sort sort = null;
        try {
            sort = new Sort(Minibase.getInstance().getAttrTypes(), (short) 4, Minibase.getInstance().getAttrSizes()
                    , fscan, 1, new MapOrder(MapOrder.Ascending), Minibase.getInstance().getMaxRowKeyLength(),
                    memory / 2);
        } catch (Exception e) {
            e.printStackTrace();
        }

        Map m = sort.get_next();
        int count = 0;
        m.setHdr((short) 4, Minibase.getInstance().getAttrTypes(), Minibase.getInstance().getAttrSizes());
        List<byte[]> mapList = new ArrayList<>(3);
        String oldMapRowKey = null;
        String oldColumnValue = null;
        FileWriter fw = new FileWriter(dataFileName + "_after_removing_duplicates.csv");

        //Code inside while loop removes duplicates from input CSV data file and keeps most recent 3 versions of maps for a particular row and column key
        while (m != null) {
            ++numberOfMapsInserted;
            oldMapRowKey = m.getRowLabel();
            oldColumnValue = m.getColumnLabel();
            if (mapList.size() == 3) {
                mapList.remove(0);
            }
            mapList.add(m.getMapByteArray());
            m = sort.get_next();
            if (m != null) {
                m.setHdr((short) 4, Minibase.getInstance().getAttrTypes(), Minibase.getInstance().getAttrSizes());
            }
            if (m == null || (!m.getRowLabel().equals(oldMapRowKey) || !m.getColumnLabel().equals(oldColumnValue))) {
                for (byte[] map : mapList) {
                    count++;
                    Map ma = new Map(map, 0, 0);
                    ma.setHdr((short) 4, Minibase.getInstance().getAttrTypes(), Minibase.getInstance().getAttrSizes());
                    fw.write("\n" + ma.getRowLabel() + "," + ma.getColumnLabel() + "," + ma.getValue() + "," + ma.getTimeStamp());
                }
                mapList.clear();
            }
        }
        System.out.println("Maps to be inserted: " + count);
        fw.close();
        sort.close();

        //Method call to insert maps into BigTable
        Minibase.getInstance().getBigTable().insertMap(dataFileName + "_after_removing_duplicates", type);


        long endTime = System.currentTimeMillis();
        System.out.println("Total time taken in minutes " + (endTime - startTime) / (1000 * 60));
        System.out.println("Total maps in the Big Table " + Minibase.getInstance().getBigTable().getMapCount());
        System.out.println("Total number of reads " + PCounter.getInstance().getReadCount());
        System.out.println("Total number of writes " + PCounter.getInstance().getWriteCount());

        //Deleting the temp heap file used for sorting purposes
        tempHeapFile.deleteFile();
        File file = new File(dataFileName + "_after_removing_duplicates.csv");
        //Deleting temp csv file
        file.delete();

        Minibase.getInstance().getBigTable().close();
        //This ensures flushing all the pages to disk
        SystemDefs.JavabaseBM.setNumBuffers(0);
    }
    
}


