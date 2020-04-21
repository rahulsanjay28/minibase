import bigt.*;
import diskmgr.PCounter;
import global.AttrType;
import global.SystemDefs;
import heap.Heapfile;

import java.util.List;


/**
 * compile this file using the command "javac GetCounts.java"
 * Then run using "java GetCounts bigtablename numBuf"
 */
public class GetCounts {

    private int maxRowKeyLength = 19;
    private int maxColumnKeyLength = 17;
    private int maxTimeStampLength = 5;
    private int maxValueLength = 5;

    private AttrType[] attrTypes;
    private short[] attrSizes;

    public static void main(String[] args) throws Exception {
        GetCounts getCounts = new GetCounts();
        getCounts.execute(args[0]);

    }

    public void execute(String numBuf) throws Exception {

        long startTime = System.currentTimeMillis();
        PCounter.getInstance().setReadCount(0);
        PCounter.getInstance().setWriteCount(0);

        Minibase.getInstance().init("", Integer.parseInt(numBuf));

        int totalMapCount = 0;
        int distinctRowCount = 0;
        int distinctColumnCount = 0;
        List<BigTableInfo> bigTableInfoList = BigTableCatalog.getAllBigTablesInBigDB();
        for (BigTableInfo bigTableInfo : bigTableInfoList) {
            System.out.println("\nGetting counts for big table " + bigTableInfo.getName());
            distinctRowCount = 0;
            distinctColumnCount = 0;
            Minibase.getInstance().init(bigTableInfo.getName(), Integer.parseInt(numBuf));
            totalMapCount = Minibase.getInstance().getBigTable().getMapCount();

            Stream stream = Minibase.getInstance().getBigTable().openStream(1, "*", "*", "*");
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

            stream = Minibase.getInstance().getBigTable().openStream(2, "*", "*", "*");
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
            System.out.println("----------------------------------------------------");
            System.out.println("Total Number of maps " + totalMapCount);
            System.out.println("Total number of distinct rows " + distinctRowCount);
            System.out.println("Total number of distinct columns " + distinctColumnCount);
            SystemDefs.JavabaseBM.setNumBuffers(0);
        }

        long endTime = System.currentTimeMillis();
        if ((endTime - startTime) > 1000) {
            System.out.println("Total time taken in seconds " + (endTime - startTime) / 1000);
        } else {
            System.out.println("Total time taken in milliseconds " + (endTime - startTime));
        }
        System.out.println("\n\nTotal number of reads " + PCounter.getInstance().getReadCount());
        System.out.println("Total number of writes " + PCounter.getInstance().getWriteCount());
    }
}

