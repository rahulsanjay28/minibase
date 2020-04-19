import bigt.*;
import diskmgr.PCounter;
import global.MID;
import global.MapOrder;
import global.SystemDefs;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.Heapfile;
import iterator.FileScan;
import iterator.FldSpec;
import iterator.RelSpec;
import iterator.Sort;

import java.io.IOException;
import java.util.List;


/**
 * compile this file using the command "javac GetCounts.java"
 * Then run using "java GetCounts bigtablename numBuf"
 */
public class GetCounts {
    public static void main(String[] args) throws Exception {
        GetCounts getCounts = new GetCounts();
        getCounts.execute(args[0]);

    }

    public void execute(String numBuf) throws Exception {

        long startTime = System.currentTimeMillis();
        PCounter.getInstance().setReadCount(0);
        PCounter.getInstance().setWriteCount(0);

        Minibase.getInstance().init("", Integer.parseInt(numBuf));
        Heapfile tempHeapFile = new Heapfile("sort_temp_heap_file_for_get_count");


        int totalMapCount=0;
        List<BigTableInfo> bigTableInfoList = BigTableCatalog.getAllBigTablesInBigDB();
        for(BigTableInfo bigTableInfo: bigTableInfoList){
            System.out.println(bigTableInfo.getName());

            for (int type = 1; type <= 5; ++type) {
                try {
                    BigT bigT = new BigT(bigTableInfo.getName(), type);
                    totalMapCount+=bigT.getMapCnt();
                    BigTStream stream = bigT.openStream("*","*","*");
                    MID mid = new MID();
                    Map map = stream.getNext(mid);
                    while(map!=null){
                        tempHeapFile.insertMap(map.getMapByteArray());
                        mid = new MID();
                        map = stream.getNext(mid);
                    }
                    stream.closeStream();

                } catch (HFException | HFBufMgrException | HFDiskMgrException | IOException e) {
                    e.printStackTrace();
                }
            }
        }


        FileScan fscan = null;
        FldSpec[] projlist = new FldSpec[4];
        RelSpec rel = new RelSpec(RelSpec.outer);
        projlist[0] = new FldSpec(rel, 1);
        projlist[1] = new FldSpec(rel, 2);
        projlist[2] = new FldSpec(rel, 3);
        projlist[3] = new FldSpec(rel, 4);

        try {
            fscan = new FileScan("sort_temp_heap_file_for_get_count", Minibase.getInstance().getAttrTypes(),
                    Minibase.getInstance().getAttrSizes(), (short) 4, 4, projlist, null);
        } catch (Exception e) {
            e.printStackTrace();
        }

        Minibase.getInstance().setOrderType(1);

        Sort sort = new Sort(Minibase.getInstance().getAttrTypes(), (short) 4, Minibase.getInstance().getAttrSizes()
                , fscan, 1, new MapOrder(MapOrder.Ascending), Minibase.getInstance().getMaxRowKeyLength(), 10);

        int distinctRowCount = 0;
        Map map = sort.get_next();
        String prevRowKey = "";
        while (map != null) {
            if (!map.getRowLabel().equals(prevRowKey)) {
                ++distinctRowCount;
            }
            prevRowKey = map.getRowLabel();
            map = sort.get_next();
        }
        sort.close();

        try {
            fscan = new FileScan("sort_temp_heap_file_for_get_count", Minibase.getInstance().getAttrTypes(),
                    Minibase.getInstance().getAttrSizes(), (short) 4, 4, projlist, null);
        } catch (Exception e) {
            e.printStackTrace();
        }

        Minibase.getInstance().setOrderType(2);

        sort = new Sort(Minibase.getInstance().getAttrTypes(), (short) 4, Minibase.getInstance().getAttrSizes()
                , fscan, 1, new MapOrder(MapOrder.Ascending), Minibase.getInstance().getMaxRowKeyLength(), 10);

        int distinctColumnCount = 0;
        map = sort.get_next();
        String prevColumnKey = "";
        while (map != null) {
            if (!map.getColumnLabel().equals(prevColumnKey)) {
                ++distinctColumnCount;
            }
            prevColumnKey = map.getColumnLabel();
            map = sort.get_next();
        }
        sort.close();
        tempHeapFile.deleteFile();

        long endTime = System.currentTimeMillis();
        if ((endTime - startTime) > 1000) {
            System.out.println("Total time taken in seconds " + (endTime - startTime) / 1000);
        } else {
            System.out.println("Total time taken in milliseconds " + (endTime - startTime));
        }
        System.out.println("Total Number of maps " + totalMapCount);
        System.out.println("Total number of distinct rows " + distinctRowCount);
        System.out.println("Total number of distinct columns " + distinctColumnCount);
        System.out.println("Total number of reads " + PCounter.getInstance().getReadCount());
        System.out.println("Total number of writes " + PCounter.getInstance().getWriteCount());


        SystemDefs.JavabaseBM.setNumBuffers(0);
    }
}

