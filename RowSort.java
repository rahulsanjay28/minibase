import bigt.*;
import diskmgr.PCounter;
import global.MapOrder;
import global.SystemDefs;

/**
 * This is a command line program which implements the RowSort operator
 * Compile using "javac RowSort.java"
 * Execute using "java RowSort big_table_name output_big_table_name ascending/descending column_name NUMBUF"
 */
public class RowSort {

    public static void main(String[] args) throws Exception {
        RowSort rowSort = new RowSort();
        rowSort.execute(args[0], args[1], args[2], args[3], args[4]);
    }

    public void execute(String inputBigTableName, String outputBigTableName, String rowOrder, String columnName,
                        String numBuf) throws Exception {

        PCounter.getInstance().setReadCount(0);
        PCounter.getInstance().setWriteCount(0);

        System.out.println("Performing RowSort");

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

        BigTableCatalog.addBigTInfo(new BigTableInfo(outputBigTableName));
        System.out.println("Total number of reads " + PCounter.getInstance().getReadCount());
        System.out.println("Total number of writes " + PCounter.getInstance().getWriteCount());

        SystemDefs.JavabaseBM.setNumBuffers(0);
    }
}
