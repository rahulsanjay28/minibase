import bigt.*;
import diskmgr.PCounter;
import global.MID;
import global.SystemDefs;

/**
 * Compile this class using the command "javac Query.java"
 * Then run "java Query bigtablename type ordertype rowfilter columnfilter valuefilter numbuf"
 */
public class Query {
    public static void main(String[] args) throws Exception {
        Query query = new Query();
        if (args.length == 6) {
            query.execute(args[0], args[1], args[2], args[3], args[4], args[5]);
        } else if (args.length == 5) {
            query.execute(args[0], args[1], args[2], args[3], args[4]);
        }
    }

    /**
     * querying the big table
     *
     * @param bigTableName
     * @param orderType
     * @param rowFilter
     * @param columnFilter
     * @param valueFilter
     * @param numBuf
     */
    public void execute(String bigTableName, String orderType, String rowFilter, String columnFilter,
                        String valueFilter, String numBuf) throws Exception {
        System.out.println("Executing query ");

        long startTime = System.currentTimeMillis();

        //Setting read and write count to zero before every query
        PCounter.getInstance().setReadCount(0);
        PCounter.getInstance().setWriteCount(0);
        Minibase.getInstance().init(bigTableName, Integer.parseInt(numBuf));

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

        long endTime = System.currentTimeMillis();
        if ((endTime - startTime) > 1000) {
            System.out.println("Total time taken in seconds " + (endTime - startTime) / 1000);
        } else {
            System.out.println("Total time taken in milliseconds " + (endTime - startTime));
        }
        System.out.println("Total Number of Maps found " + stream.getNumberOfMapsFound());
        System.out.println("Total number of reads " + PCounter.getInstance().getReadCount());
        System.out.println("Total number of writes " + PCounter.getInstance().getWriteCount());

        SystemDefs.JavabaseBM.setNumBuffers(0);
    }

    /**
     * querying the big table
     *
     * @param bigTableName
     * @param rowFilter
     * @param columnFilter
     * @param valueFilter
     * @param numBuf
     */
    public void execute(String bigTableName, String rowFilter, String columnFilter,
                        String valueFilter, String numBuf) throws Exception {
        System.out.println("Executing query ");

        long startTime = System.currentTimeMillis();

        //Setting read and write count to zero before every query
        PCounter.getInstance().setReadCount(0);
        PCounter.getInstance().setWriteCount(0);
        Minibase.getInstance().init(bigTableName, Integer.parseInt(numBuf));

        int count = 0;
        for (int type = 1; type <= 5; type++) {
            BigT bigT = Minibase.getInstance().getBigTable().getBigTableParts().get(type);
            BigTStream bigTStream = bigT.openStream(rowFilter, columnFilter, valueFilter);
            MID mid = new MID();
            Map map = bigTStream.getNext(mid);
            while (map != null) {
                map.print();
                mid = new MID();
                map = bigTStream.getNext(mid);
            }
            count += bigTStream.getNumberOfMapsFound();
            bigTStream.closeStream();
        }

        long endTime = System.currentTimeMillis();
        if ((endTime - startTime) > 1000) {
            System.out.println("Total time taken in seconds " + (endTime - startTime) / 1000);
        } else {
            System.out.println("Total time taken in milliseconds " + (endTime - startTime));
        }
        System.out.println("Total Number of Maps found " + count);
        System.out.println("Total number of reads " + PCounter.getInstance().getReadCount());
        System.out.println("Total number of writes " + PCounter.getInstance().getWriteCount());

        SystemDefs.JavabaseBM.setNumBuffers(0);
    }
}
