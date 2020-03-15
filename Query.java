import bigt.Map;
import bigt.Minibase;
import bigt.Stream;
import diskmgr.PCounter;
import global.SystemDefs;

import java.io.IOException;

/**
 * Compile this class using the command "javac Query.java"
 * Then run "java Query bigtablename type ordertype rowfilter columnfilter valuefilter numbuf"
 */
public class Query {
    public static void main(String[] args) throws Exception {
//        query(args[0], args[1], args[2], args[3], args[4], args[5], args[6]);
    }

    /**
     * querying the big table
     *
     * @param bigTableName
     * @param type
     * @param orderType
     * @param rowFilter
     * @param columnFilter
     * @param valueFilter
     * @param numBuf
     */
    public void execute(String bigTableName, String type, String orderType, String rowFilter, String columnFilter,
                        String valueFilter, String numBuf) throws Exception {
        System.out.println("Executing query");
        PCounter.getInstance().setReadCount(0);
        PCounter.getInstance().setWriteCount(0);
        SystemDefs.JavabaseBM.setNumBuffers(Integer.parseInt(numBuf));

        if (!Minibase.getInstance().getBigTable().getName().equals(bigTableName)) {
            System.out.println("Bigtable name mismatch. Aborting search.");
            return;
        }

        if (Minibase.getInstance().getBigTable().getType() != Integer.parseInt(type)) {
            System.out.println("Bigtable and Query type mismatch. Aborting search.");
            return;
        }

        Stream stream = Minibase.getInstance().getBigTable().openStream(Integer.parseInt(orderType), rowFilter, columnFilter, valueFilter);
        if (stream == null) {
            System.out.println("stream null");
            return;
        }
        Map map = stream.getNext();
        while (map != null) {
            map.print();
            map = stream.getNext();
        }
        stream.unsetScanEntireBigT();
        System.out.println("Total Number of Maps found " + stream.getNumberOfMapsFound());
        System.out.println("Total number of reads " + PCounter.getInstance().getReadCount());
        System.out.println("Total number of writes " + PCounter.getInstance().getWriteCount());

    }
}
