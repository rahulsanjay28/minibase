import bigt.Map;
import bigt.Minibase;
import bigt.Stream;
import btree.BTFileScan;
import btree.KeyDataEntry;
import btree.LeafData;
import btree.StringKey;
import diskmgr.PCounter;
import global.RID;
import global.SystemDefs;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;

/**
 * Compile this class using the command "javac Query.java"
 * Then run "java Query bigtablename type ordertype rowfilter columnfilter valuefilter numbuf"
 */
public class Query {
    public static void main(String[] args) throws Exception{
//        query(args[0], args[1], args[2], args[3], args[4], args[5], args[6]);
    }

    /**
     * querying the big table
     * @param bigTableName
     * @param type
     * @param orderType
     * @param rowFilter
     * @param columnFilter
     * @param valueFilter
     * @param numBuf
     */
    public void execute(String bigTableName, String type, String orderType, String rowFilter, String columnFilter,
                              String valueFilter, String numBuf) throws Exception{
        System.out.println("Executing query");
        PCounter.getInstance().setReadCount(0);
        PCounter.getInstance().setWriteCount(0);
        HashSet set = new HashSet();
        SystemDefs.JavabaseBM.setNumBuffers(Integer.parseInt(numBuf));

        if(Minibase.getInstance().getBigTable().getType()!= Integer.parseInt(type))
        {
            System.out.println("Bigtable and Query type mismatch. Aborting search.");
            return;
        }

        Stream stream = Minibase.getInstance().getBigTable().openStream(Integer.parseInt(orderType), rowFilter, columnFilter, valueFilter);
        if(stream == null){
            System.out.println("stream null");
            return;
        }
        Map map = stream.getNext();
        while(map != null){

            if(filterOutput(map, stream.sanitizefilter(rowFilter), stream.sanitizefilter(columnFilter),stream.sanitizefilter(valueFilter))
            ) {
                System.out.println(map.getRowLabel() + " " + map.getColumnLabel() + " " +
                        map.getTimeStamp() + " " + map.getValue());
                set.add(map.getRowLabel());
            }
            map = stream.getNext();
            if(map == null){
                System.out.println("map is null");
            }
        }
        System.out.println(set.size());
        stream.unsetScanEntireBigT();
        System.out.println("Total Number of Maps found " + stream.getNumberOfMapsFound());
        System.out.println("Total number of reads " + PCounter.getInstance().getReadCount());
        System.out.println("Total number of writes " + PCounter.getInstance().getWriteCount());

    }

    public boolean filterOutput(Map map, String[] rowFilter, String[] columnFilter,
                                String[] valueFilter) throws IOException {



        if(rowFilter.length==1)
        {
            if(rowFilter[0].compareTo("*")!=0 && map.getRowLabel().compareTo(rowFilter[0])!=0)
                return false;
        }
        else
        {
            //System.out.println("ROWFILTER " + map.getRowLabel() + " -- " + rowFilter[0] + " -- " + rowFilter[1] + " -- " + map.getRowLabel().compareTo(rowFilter[0]) + map.getRowLabel().compareTo(rowFilter[1]));
            if(map.getRowLabel().compareTo(rowFilter[0])<0 || map.getRowLabel().compareTo(rowFilter[1])>0)
            {
                //System.out.println("ROWFILTER " + map.getRowLabel() + " -- " + rowFilter[0] + " -- " + rowFilter[1] + " -- " + map.getRowLabel().compareTo(rowFilter[0]) + map.getRowLabel().compareTo(rowFilter[1]));

                return false;
            }
        }

        if(columnFilter.length==1)
        {
            if(columnFilter[0].compareTo("*")!=0 && map.getColumnLabel().compareTo(columnFilter[0])!=0)
                return false;
        }
        else
        {
            if(map.getColumnLabel().compareTo(columnFilter[0])<0 || map.getColumnLabel().compareTo(columnFilter[1])>0)
            {
                //System.out.println("COLFILTER " + map.getColumnLabel() + " -- " + columnFilter[0] + " -- " + columnFilter[1] + " -- " + map.getColumnLabel().compareTo(columnFilter[0]) + map.getColumnLabel().compareTo(columnFilter[1]));
                return false;
            }
        }

        if(valueFilter.length==1)
        {
            if(valueFilter[0].compareTo("*")!=0 && map.getValue().compareTo(valueFilter[0])!=0)
                return false;
        }
        else
        {
            if(map.getValue().compareTo(valueFilter[0])<0 || map.getValue().compareTo(valueFilter[1])>0)
            {
                //System.out.println("VALFILTER " + map.getValue() + " -- " + valueFilter[0] + " -- " + valueFilter[1] + " -- " + map.getValue().compareTo(valueFilter[0]) + map.getValue().compareTo(valueFilter[1]));

                return false;
            }
        }

        return true;
    }
}
