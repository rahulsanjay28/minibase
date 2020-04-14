package bigt;

import global.MID;
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
    private heap.Scan scan;

    public Stream(int orderType, String rowFilter, String columnFilter, String valueFilter) throws Exception {
        tempHeapFile = new Heapfile("query_temp_heap_file");

        BigT bigT = null;
        for (int i = 1; i < Minibase.getInstance().getBigTable().getBigTableParts().size(); i++) {
            bigT = Minibase.getInstance().getBigTable().getBigTableParts().get(i);
            BigTStream bigTStream = bigT.openStream(rowFilter, columnFilter, valueFilter);
            MID mid = new MID();
            Map map = bigTStream.getNext(mid);
            while (map != null) {
                tempHeapFile.insertMap(map.getMapByteArray());
                mid = new MID();
                map = bigTStream.getNext(mid);
            }
            bigTStream.closeStream();
        }

        if(orderType == 0){
            scan = tempHeapFile.openScan();
            return;
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
        if(scan == null){
            Map m = filteredAndSortedData.get_next();
            if(m != null){
                ++numberOfMapsFound;
            }
            return m;
        }else{
            MID mid = new MID();
            return scan.getNext(mid);
        }
    }

    public int getNumberOfMapsFound() {
        return numberOfMapsFound;
    }

    public void close() throws Exception {
        tempHeapFile.deleteFile();
        if(filteredAndSortedData != null) {
            filteredAndSortedData.close();
        }
        if(scan != null){
            scan.closescan();
        }
    }
}
