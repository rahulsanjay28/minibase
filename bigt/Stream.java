package bigt;

import btree.BTFileScan;
import btree.KeyDataEntry;
import btree.LeafData;
import btree.StringKey;
import global.MapOrder;
import global.RID;
import heap.Heapfile;
import iterator.FileScan;
import iterator.FldSpec;
import iterator.RelSpec;
import iterator.Sort;

import java.io.IOException;

public class Stream {
    private BTFileScan scan, scan2;
    private int numberOfMapsFound;
    private Scan scanBigT;
    private BigT bigT;
    private boolean scanEntireBigT;
    private Sort filteredAndSortedData;
    private Heapfile tempHeapFile;

    public Stream(BigT bigtable, int orderType, String rowFilter, String columnFilter, String valueFilter) throws Exception {
        this.numberOfMapsFound = 0;
        this.bigT = bigtable;

        scanBigT = new Scan(bigtable);

        String rowFilters[] = sanitizefilter(rowFilter);
        String columnFilters[] = sanitizefilter(columnFilter);
        String valueFilters[] = sanitizefilter(valueFilter);

        switch (bigT.getType()) {
            case 2:
                if (rowFilters[0].compareTo("*") != 0) {
                    scanEntireBigT = false;

                    if (rowFilters.length == 1) {
                        scan = Minibase.getInstance().getBTree().new_scan(new StringKey(rowFilter), new StringKey(rowFilter));
                    } else {
                        scan = Minibase.getInstance().getBTree().new_scan(new StringKey(rowFilters[0]), new StringKey(rowFilters[1]));
                    }
                } else {
                    //Scan everything
                    scanEntireBigT = true;
                }
                break;
            case 3:
                if (columnFilters[0].compareTo("*") != 0) {
                    scanEntireBigT = false;
                    if (columnFilters.length == 1) {
                        scan = Minibase.getInstance().getBTree().new_scan(new StringKey(columnFilter), new StringKey(columnFilter));
                    } else {
                        scan = Minibase.getInstance().getBTree().new_scan(new StringKey(columnFilters[0]), new StringKey(columnFilters[1]));
                    }
                } else {
                    scanEntireBigT = true;
                }
                break;
            case 4:
                if (rowFilters[0].compareTo("*") != 0 && columnFilters[0].compareTo("*") != 0) {
                    scanEntireBigT = false;
                    if (columnFilters.length == 1 && rowFilters.length == 1) {
                        scan = Minibase.getInstance().getBTree().new_scan(new StringKey(rowFilter + columnFilter), new StringKey(rowFilter + columnFilter));
                    }
                    else if (rowFilters.length == 1 && columnFilters.length != 1) {
                        scan = Minibase.getInstance().getBTree().new_scan(new StringKey(rowFilter + columnFilters[0]), new StringKey(rowFilter + columnFilters[1]));
                    }
                    else if (rowFilters.length != 1 && columnFilters.length == 1) {
                        scan = Minibase.getInstance().getBTree().new_scan(new StringKey(rowFilters[0] + columnFilter), new StringKey(rowFilters[1] + columnFilter));
                    }
                    else {
                        scan = Minibase.getInstance().getBTree().new_scan(new StringKey(rowFilters[0] + columnFilters[0]), new StringKey(rowFilters[1] + columnFilters[1]));
                    }
                    scan2 = Minibase.getInstance().getSecondaryBTree().new_scan(null, null);
                } else {
                    scanEntireBigT = true;
                }
                break;
            case 5:
                if (rowFilters[0].compareTo("*") != 0 && valueFilters[0].compareTo("*") != 0) {
                    scanEntireBigT = false;
                    scan = Minibase.getInstance().getBTree().new_scan(new StringKey(rowFilter + valueFilter), new StringKey(rowFilter + valueFilter));
                    scan2 = Minibase.getInstance().getSecondaryBTree().new_scan(null, null);
                } else {
                    scanEntireBigT = true;
                }
                break;
            default:
                scanEntireBigT = true;
                break;
        }

        filterAndSortByOrderType(orderType, rowFilters, columnFilters, valueFilters);
    }

    private void filterAndSortByOrderType(int orderType, String[] rowFilters, String[] columnFilters,
                                          String[] valueFilters) throws Exception{
        tempHeapFile = new Heapfile("tempfile1");
        if (scanEntireBigT) {
            //System.out.println("Scanning entire big t");
            RID rid = new RID();
            Map map = scanBigT.getNext(rid);
            while(map != null) {
                map.setHdr((short) 4, Minibase.getInstance().getAttrTypes(), Minibase.getInstance().getAttrSizes());
                if(filterOutput(map, rowFilters, columnFilters, valueFilters)) {
                    tempHeapFile.insertMap(map.getMapByteArray());
                }
                map = scanBigT.getNext(rid);
            }
        }else {
            KeyDataEntry entry = scan.get_next();
            while(entry != null) {
                RID rid = ((LeafData) entry.data).getData();
                if (rid != null) {
                    try {
                        Map map = Minibase.getInstance().getBigTable().getMap(rid);
                        map.setHdr((short) 4, Minibase.getInstance().getAttrTypes(), Minibase.getInstance().getAttrSizes());
                        if(filterOutput(map, rowFilters, columnFilters, valueFilters)) {
                            tempHeapFile.insertMap(map.getMapByteArray());
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                entry = scan.get_next();
            }
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
            fscan = new FileScan("tempfile1", Minibase.getInstance().getAttrTypes(),
                    Minibase.getInstance().getAttrSizes(), (short) 4, 4, projlist, null);
        } catch (Exception e) {
            e.printStackTrace();
        }

        Minibase.getInstance().setOrderType(orderType);
        int sortField = -1;
        int maxLength = -1;
        switch (orderType){
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
                    , fscan, sortField, new MapOrder(MapOrder.Ascending), maxLength, 240);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public void unsetScanEntireBigT() {
        scanEntireBigT = false;
    }

    public String[] sanitizefilter(String filter) {
        String s[];
        if (filter.startsWith("[")) {
            s = filter.substring(1, filter.length() - 1).split(",");
            if (s[0].compareTo(s[1]) == 0) {
                String t[] = new String[1];
                t[0] = s[0];
                return t;
            }
        } else {
            s = new String[1];
            s[0] = filter;
        }
        return s;
    }

    public boolean filterOutput(Map map, String[] rowFilter, String[] columnFilter,
                                String[] valueFilter) throws IOException {
        if (rowFilter.length == 1) {
            if (rowFilter[0].compareTo("*") != 0 && map.getRowLabel().compareTo(rowFilter[0]) != 0)
                return false;
        } else {
            if (map.getRowLabel().compareTo(rowFilter[0]) < 0 || map.getRowLabel().compareTo(rowFilter[1]) > 0) {
                return false;
            }
        }
        if (columnFilter.length == 1) {
            if (columnFilter[0].compareTo("*") != 0 && map.getColumnLabel().compareTo(columnFilter[0]) != 0)
                return false;
        } else {
            if (map.getColumnLabel().compareTo(columnFilter[0]) < 0 || map.getColumnLabel().compareTo(columnFilter[1]) > 0) {
                return false;
            }
        }
        if (valueFilter.length == 1) {
            if (valueFilter[0].compareTo("*") != 0 && map.getValue().compareTo(valueFilter[0]) != 0)
                return false;
        } else {
            if (map.getValue().compareTo(valueFilter[0]) < 0 || map.getValue().compareTo(valueFilter[1]) > 0) {
                return false;
            }
        }
        return true;
    }

    public void closeStream() {

    }

    public RID getFirstRID() throws Exception {
        KeyDataEntry entry = scan.get_next();
        if (entry == null) {
            return null;
        }
        RID rid = ((LeafData) entry.data).getData();
        return rid;
    }

    public Map getNext() throws Exception {
        Map m = filteredAndSortedData.get_next();
        if(m == null){
            System.out.println("Deleting temp file used for sorting");
            tempHeapFile.deleteFile();
            return null;
        }
        ++numberOfMapsFound;
        return m;
    }

    public BTreeData getNextMap() throws Exception {
        KeyDataEntry entry = scan.get_next();
        BTreeData bData;
        if (entry == null) {
            return null;
        }
        RID rid = ((LeafData) entry.data).getData();
        if (rid != null) {
            try {
                Map map2 = Minibase.getInstance().getBigTable().getMap(rid);
                map2.setOffsets(map2.getOffset());
                bData = new BTreeData(rid, map2, entry.key);
                return bData;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return null;
    }


//    public void scanBigTCode() throws Exception{
//        System.out.println("Scanning the big table");
//        Scan scan = new Scan(Minibase.getInstance().getBigTable());
//        RID rid = new RID();
//        Map map = scan.getNext(rid);
//        while(map != null){
//            map.setOffsets(map.getOffset());
//            System.out.println(map.getRowLabel() + " " + map.getColumnLabel() + " " +
//                    map.getTimeStamp() + " " + map.getValue());
//            map = scan.getNext(rid);
//        }
//    }

//    public void temp(){
//                BTLeafPage leafPage;
//        RID curRid = new RID();  // iterator
//        leafPage = Minibase.getInstance().getBTree().findRunStart(new StringKey("Michigan"), curRid);  // find first page,rid of key
//        if (leafPage == null) {
//            System.out.println("Error in findRunStart");
//        }else {
//            KeyDataEntry entry = leafPage.getCurrent(curRid);
//            tempRID = ((LeafData)entry.data).getData();
//        }
//    }

    public int getNumberOfMapsFound() {
        return numberOfMapsFound;
    }
}
