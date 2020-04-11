import bigt.BTRowSort;
import bigt.BigT;
import bigt.Map;
import diskmgr.PCounter;
import global.MapOrder;
import global.SystemDefs;

import java.io.FileWriter;

public class RowSort {

    public static void main(String[] args) throws Exception {
        RowSort rowSort = new RowSort();
        rowSort.execute(args[0], args[1], args[2], args[3], args[4]);
    }

    public void execute(String inputBigTableName, String outputBigTableName, String rowOrder, String columnName,
                        String numBuf) throws Exception {

        PCounter.getInstance().setReadCount(0);
        PCounter.getInstance().setWriteCount(0);

        int order;
        if (rowOrder.equalsIgnoreCase("ascending")) {
            order = MapOrder.Ascending;
        } else {
            order = MapOrder.Descending;
        }

        BTRowSort btRowSort = new BTRowSort(inputBigTableName, order, columnName, numBuf);
        BigT bigT = new BigT(outputBigTableName, 1);
        Map m = btRowSort.getNext();
        while (m != null) {
            bigT.insertMap(m);
            m = btRowSort.getNext();
        }
        btRowSort.close();

        System.out.println("Total number of reads " + PCounter.getInstance().getReadCount());
        System.out.println("Total number of writes " + PCounter.getInstance().getWriteCount());

        SystemDefs.JavabaseBM.setNumBuffers(0);
    }
}
