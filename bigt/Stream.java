package bigt;

import diskmgr.OutOfSpaceException;
import global.MapOrder;
import heap.Heapfile;
import iterator.FileScan;
import iterator.FldSpec;
import iterator.RelSpec;
import iterator.Sort;

public class Stream {

    private Sort filteredAndSortedData;
    private Heapfile tempHeapFile;
    private int numberOfMapsFound;

    public Stream(int orderType, String rowFilter, String columnFilter, String valueFilter) throws Exception {
        tempHeapFile = new Heapfile("query_temp_heap_file");

        for (BigT bigT : Minibase.getInstance().getBigTable().getBigTableParts()) {
            BigTStream bigTStream = bigT.openStream(rowFilter, columnFilter, valueFilter);
            Map map = bigTStream.getNext();
            while (map != null) {
                tempHeapFile.insertMap(map.getMapByteArray());
                map = bigTStream.getNext();
            }
            bigTStream.closeStream();
        }

        //Now temp heap file contains all the filtered data which we have to sort based on the order type
        // create an iterator by open a file scan
        FldSpec[] projlist = new FldSpec[4];
        RelSpec rel = new RelSpec(RelSpec.outer);
        projlist[0] = new FldSpec(rel, 1);
        projlist[1] = new FldSpec(rel, 2);
        projlist[2] = new FldSpec(rel, 3);
        projlist[3] = new FldSpec(rel, 4);

        FileScan fscan = null;

        try {
            fscan = new FileScan("query_temp_heap_file", Minibase.getInstance().getAttrTypes(),
                    Minibase.getInstance().getAttrSizes(), (short) 4, 4, projlist, null);
        } catch (Exception e) {
            e.printStackTrace();
        }
        Minibase.getInstance().setOrderType(orderType);
        int sortField = -1;
        int maxLength = -1;
        switch (orderType) {
            case 1:
            case 3:
                sortField = 1;
                maxLength = Minibase.getInstance().getMaxRowKeyLength();
                break;
            case 2:
            case 4:
                sortField = 2;
                maxLength = Minibase.getInstance().getMaxColumnKeyLength();
                break;
            case 6:
                sortField = 3;
                maxLength = Minibase.getInstance().getMaxTimeStampLength();
                break;
        }
        try {
            filteredAndSortedData = new Sort(Minibase.getInstance().getAttrTypes(), (short) 4, Minibase.getInstance().getAttrSizes()
                    , fscan, sortField, new MapOrder(MapOrder.Ascending), maxLength, 10);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public Map getNext() throws Exception {
        if (filteredAndSortedData == null) {
            System.out.println("something is wrong");
            return null;
        }
        Map m = null;
        try {
            m = filteredAndSortedData.get_next();
        } catch (OutOfSpaceException e) {
            e.printStackTrace();
        }
        if (m == null) {
            System.out.println("Deleting temp file used for sorting");
            tempHeapFile.deleteFile();
            filteredAndSortedData.close();
            return null;
        }
        ++numberOfMapsFound;
        return m;
    }

    public int getNumberOfMapsFound(){
        return numberOfMapsFound;
    }
}
