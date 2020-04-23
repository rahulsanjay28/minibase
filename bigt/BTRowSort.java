package bigt;

import Utility.GetMap;
import global.MapOrder;
import heap.Heapfile;
import iterator.FileScan;
import iterator.FldSpec;
import iterator.RelSpec;
import iterator.Sort;

/**
 * This class is used by the command line program RowSort.java
 */
public class BTRowSort {

    private Stream stream = null;
    private Heapfile tempHeapFile = null;
    private Sort sort = null;

    /**
     * The constructor extracts the latest maps with the given column name and calls the Minibase sort
     * @param bigTableName
     * @param order
     * @param columnName
     * @param numBuf
     * @throws Exception
     */
    public BTRowSort(String bigTableName, int order, String columnName, String numBuf) throws Exception {
        Minibase.getInstance().init(bigTableName, Integer.parseInt(numBuf));

        stream = Minibase.getInstance().getBigTable().openStream(1, "*", "*", "*");
        if (stream == null) {
            System.out.println("stream null");
            return;
        }

        Map map = stream.getNext();
        String prevRowKey = "";
        String latestValue = "";

        tempHeapFile = new Heapfile("row_sort_temp_heap_file");

        while (map != null) {
            if (!map.getRowLabel().equals(prevRowKey)) {
                if (latestValue.isEmpty()) {
                    if (order == MapOrder.Ascending) {
                        latestValue = "99999";
                    } else {
                        latestValue = "00000";
                    }
                }
                tempHeapFile.insertMap(GetMap.getMap(prevRowKey, "temp_col", latestValue, "1").getMapByteArray());
                prevRowKey = map.getRowLabel();
                latestValue = "";
            }
            if (map.getColumnLabel().equals(columnName)) {
                latestValue = map.getValue();
            }
            map = stream.getNext();
        }
        stream.close();
        tempHeapFile.insertMap(GetMap.getMap(prevRowKey, "temp_col", latestValue, "1").getMapByteArray());

        FldSpec[] projlist = new FldSpec[4];
        RelSpec rel = new RelSpec(RelSpec.outer);
        projlist[0] = new FldSpec(rel, 1);
        projlist[1] = new FldSpec(rel, 2);
        projlist[2] = new FldSpec(rel, 3);
        projlist[3] = new FldSpec(rel, 4);

        FileScan fscan = null;

        try {
            fscan = new FileScan("row_sort_temp_heap_file", Minibase.getInstance().getAttrTypes(),
                    Minibase.getInstance().getAttrSizes(), (short) 4, 4, projlist, null);
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (order == MapOrder.Ascending) {
            Minibase.getInstance().setOrderType(8);
        } else {
            Minibase.getInstance().setOrderType(9);
        }
        int memory = Minibase.getInstance().getNumberOfBuffersAvailable();
        try {
            sort = new Sort(Minibase.getInstance().getAttrTypes(), (short) 4, Minibase.getInstance().getAttrSizes()
                    , fscan, 4, new MapOrder(MapOrder.Ascending), Minibase.getInstance().getMaxValueLength(),
                    memory / 2);
        } catch (Exception e) {
            e.printStackTrace();
        }

        Map m = sort.get_next();
        stream = Minibase.getInstance().getBigTable().openStream(0, m.getRowLabel(),
                "*", "*");
    }

    /**
     * This interface is used to get the maps in the RowSorted order based on the specified column
     * @return
     * @throws Exception
     */
    public Map getNext() throws Exception {
        Map m = stream.getNext();
        if (m == null) {
            stream.close();
            Map value = sort.get_next();
            if (value == null) {
                return null;
            }
            stream = Minibase.getInstance().getBigTable().openStream(0, value.getRowLabel(),
                    "*", "*");
            m = stream.getNext();
            if (m == null) {
                stream.close();
            }
        }
        return m;
    }

    /**
     * This method should be called after the RowSort operation is done
     * @throws Exception
     */
    public void close() throws Exception {
        sort.close();
        tempHeapFile.deleteFile();
    }
}
