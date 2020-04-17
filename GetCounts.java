import bigt.*;
import diskmgr.PCounter;
import global.SystemDefs;

import java.util.List;


/**
 * compile this file using the command "javac GetCounts.java"
 * Then run using "java GetCounts bigtablename numBuf"
 */
public class GetCounts {
    public static void main(String[] args) throws Exception {
        GetCounts getCounts = new GetCounts();
        getCounts.execute(args[0], args[1]);

    }

    public void execute(String bigTableName, String numBuf) throws Exception {

        long startTime = System.currentTimeMillis();
        PCounter.getInstance().setReadCount(0);
        PCounter.getInstance().setWriteCount(0);
        Minibase.getInstance().init(bigTableName, Integer.parseInt(numBuf));

        List<BigTableInfo> bigTableInfoList = BigTableCatalog.getAllBigTablesInBigDB();
        for(BigTableInfo bigTableInfo: bigTableInfoList){
            System.out.println(bigTableInfo.getName());
        }

        //counting distinct rows
        Stream stream = Minibase.getInstance().getBigTable().openStream(1, "*", "*", "*");
        if (stream == null) {
            System.out.println("stream null");
            return;
        }
        int distinctRowCount = 0;
        Map map = stream.getNext();
        String prevRowKey = "";
        while (map != null) {
            if (!map.getRowLabel().equals(prevRowKey)) {
                ++distinctRowCount;
            }
            prevRowKey = map.getRowLabel();
            map = stream.getNext();
        }
        stream.close();

        //counting distinct columns
        stream = Minibase.getInstance().getBigTable().openStream(2, "*", "*", "*");
        if (stream == null) {
            System.out.println("stream null");
            return;
        }
        int distinctColumnCount = 0;
        map = stream.getNext();
        String prevColumnKey = "";
        while (map != null) {
            if (!map.getColumnLabel().equals(prevColumnKey)) {
                ++distinctColumnCount;
            }
            prevColumnKey = map.getColumnLabel();
            map = stream.getNext();
        }
        stream.close();

        long endTime = System.currentTimeMillis();
        if ((endTime - startTime) > 1000) {
            System.out.println("Total time taken in seconds " + (endTime - startTime) / 1000);
        } else {
            System.out.println("Total time taken in milliseconds " + (endTime - startTime));
        }
        System.out.println("Total Number of maps " + Minibase.getInstance().getBigTable().getMapCount());
        System.out.println("Total number of distinct rows " + distinctRowCount);
        System.out.println("Total number of distinct columns " + distinctColumnCount);
        System.out.println("Total number of reads " + PCounter.getInstance().getReadCount());
        System.out.println("Total number of writes " + PCounter.getInstance().getWriteCount());

        SystemDefs.JavabaseBM.setNumBuffers(0);
    }
}

