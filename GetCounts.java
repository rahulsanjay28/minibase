import bigt.Map;
import bigt.Minibase;
import bigt.Stream;
import diskmgr.PCounter;


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
        Stream stream = Minibase.getInstance().getBigTable().openStream(1, "*", "*", "*");
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
    }
}

