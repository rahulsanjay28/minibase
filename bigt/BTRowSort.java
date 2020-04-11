package bigt;

import Utility.GetMap;
import btree.*;
import global.AttrType;
import global.MID;
import global.MapOrder;
import heap.Heapfile;
import iterator.FileScan;
import iterator.FldSpec;
import iterator.RelSpec;
import iterator.Sort;

import java.io.IOException;

public class BTRowSort {

    private BTFileScan scan = null;
    private Heapfile tempHeapFile = null;
    private BTreeFile tempBtreeFile = null;
    private Heapfile tempHeapFile1 = null;
    private Sort sort = null;

    public BTRowSort(String bigTableName, int order, String columnName, String numBuf) throws Exception {
        Minibase.getInstance().init(bigTableName, Integer.parseInt(numBuf));

        Stream stream = Minibase.getInstance().getBigTable().openStream(1, "*", "*", "*");
        if (stream == null) {
            System.out.println("stream null");
            return;
        }

        tempHeapFile = new Heapfile("row_sort_temp_heap_file");
        try {
            tempBtreeFile = new BTreeFile("row_sort_temp_index_file", AttrType.attrString,
                    Minibase.getInstance().getMaxRowKeyLength() + 2, 0);
        } catch (GetFileEntryException | ConstructPageException | IOException | AddFileEntryException e) {
            e.printStackTrace();
        }

        Map map = stream.getNext();
        String prevRowKey = "";
        String latestValue = "";

        tempHeapFile1 = new Heapfile("row_sort_temp_heap_file1");

        while (map != null) {
            if (!map.getRowLabel().equals(prevRowKey)) {
                if (latestValue.isEmpty()) {
                    if (order == MapOrder.Ascending) {
                        latestValue = "99999";
                    } else {
                        latestValue = "-1";
                    }
                }
                tempHeapFile1.insertMap(GetMap.getMap(prevRowKey, "temp_col", latestValue, "1").getMapByteArray());
                prevRowKey = map.getRowLabel();
                latestValue = "";
            }
            if (map.getColumnLabel().equals(columnName)) {
                latestValue = map.getValue();
            }
            MID mid = tempHeapFile.insertMap(map.getMapByteArray());
            tempBtreeFile.insert(new StringKey(map.getRowLabel()), mid);
            map = stream.getNext();
        }
        tempHeapFile1.insertMap(GetMap.getMap(prevRowKey, "temp_col", latestValue, "1").getMapByteArray());

        FldSpec[] projlist = new FldSpec[4];
        RelSpec rel = new RelSpec(RelSpec.outer);
        projlist[0] = new FldSpec(rel, 1);
        projlist[1] = new FldSpec(rel, 2);
        projlist[2] = new FldSpec(rel, 3);
        projlist[3] = new FldSpec(rel, 4);

        FileScan fscan = null;

        try {
            fscan = new FileScan("row_sort_temp_heap_file1", Minibase.getInstance().getAttrTypes(),
                    Minibase.getInstance().getAttrSizes(), (short) 4, 4, projlist, null);
        } catch (Exception e) {
            e.printStackTrace();
        }
        Minibase.getInstance().setOrderType(8);
        try {
            sort = new Sort(Minibase.getInstance().getAttrTypes(), (short) 4, Minibase.getInstance().getAttrSizes()
                    , fscan, 3, new MapOrder(order), Minibase.getInstance().getMaxValueLength(), 10);
        } catch (Exception e) {
            e.printStackTrace();
        }

        Map m = sort.get_next();
        scan = tempBtreeFile.new_scan(new StringKey(m.getRowLabel()), new StringKey(m.getRowLabel()));
    }

    public Map getNext() throws Exception {
        Map m = getNextFromBTree();
        if (m == null) {
            scan.DestroyBTreeFileScan();
            Map value = sort.get_next();
            if (value == null) {
                return null;
            }
            scan = tempBtreeFile.new_scan(new StringKey(value.getRowLabel()), new StringKey(value.getRowLabel()));
            m = getNextFromBTree();
        }
        return m;
    }

    private Map getNextFromBTree() throws Exception {
        KeyDataEntry entry = scan.get_next();
        if (entry != null) {
            MID mid = ((LeafData) entry.data).getData();
            if (mid != null) {
                try {
                    Map map1 = tempHeapFile.getMap(mid);
                    map1.setHdr((short) 4, Minibase.getInstance().getAttrTypes(), Minibase.getInstance().getAttrSizes());
                    return map1;
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        return null;
    }

    public void close() throws Exception {
        sort.close();
        tempHeapFile1.deleteFile();
        tempHeapFile.deleteFile();
        tempBtreeFile.destroyFile();
    }
}
