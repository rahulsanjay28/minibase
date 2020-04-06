package bigt;

import btree.*;
import diskmgr.OutOfSpaceException;
import global.MapOrder;
import global.MID;
import heap.*;
import iterator.FileScan;
import iterator.FldSpec;
import iterator.RelSpec;
import iterator.Sort;

import java.io.IOException;

public class Stream {
    private BTFileScan scan;
    private int numberOfMapsFound;
    private Scan scanBigT;
    private BigT bigT;
    private boolean scanEntireBigT;
    private Sort filteredAndSortedData;
    private Heapfile tempHeapFile;

    private MID[] mids;
    private int midCount;

    public Stream(BigT bigtable, int orderType, String rowFilter, String columnFilter, String valueFilter) throws Exception {
        this.numberOfMapsFound = 0;
        this.bigT = bigtable;
        mids = new MID[3];
        midCount = 0;
        scanBigT = new Scan(bigtable);

        String[] rowFilters = sanitizefilter(rowFilter);
        String[] columnFilters = sanitizefilter(columnFilter);
        String[] valueFilters = sanitizefilter(valueFilter);

        if(valueFilters.length == 1){
            if(!valueFilters[0].equals("*")){
                valueFilters[0] = Minibase.getInstance().getTransformedValue(valueFilters[0]);
            }
        }else{
            valueFilters[0] = Minibase.getInstance().getTransformedValue(valueFilters[0]);
            valueFilters[1] = Minibase.getInstance().getTransformedValue(valueFilters[1]);
        }

        switch (bigT.getType()) {
            case 2:
                if (rowFilters[0].compareTo("*") != 0) {
                    scanEntireBigT = false;

                    if (rowFilters.length == 1) {
                        scan = Minibase.getInstance().getBTree().new_scan(new StringKey(rowFilter), new StringKey(rowFilter));
                    } else {
                        scan = Minibase.getInstance().getBTree().new_scan(new StringKey(rowFilters[0]), new StringKey(rowFilters[1] + 'Z'));
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
                        scan = Minibase.getInstance().getBTree().new_scan(new StringKey(columnFilters[0]), new StringKey(columnFilters[1] + 'Z'));
                    }
                } else {
                    scanEntireBigT = true;
                }
                break;
            case 4:
                if(rowFilters.length == 1){
                    if (rowFilters[0].compareTo("*") != 0) {
                        scanEntireBigT = false;
                        if(columnFilters.length == 1){
                            if (columnFilters[0].compareTo("*") != 0) {
                                scan = Minibase.getInstance().getBTree().new_scan(new StringKey(rowFilter + columnFilter), new StringKey(rowFilter + columnFilter + 'Z'));
                            } else {
                                scan = Minibase.getInstance().getBTree().new_scan(new StringKey(rowFilter), new StringKey(rowFilter + 'Z'));
                            }
                        }else{
                            scan = Minibase.getInstance().getBTree().new_scan(new StringKey(rowFilter + columnFilters[0]), new StringKey(rowFilter + columnFilters[1] + 'Z'));
                        }
                    }else{
                        scanEntireBigT = true;
                    }
                }else{
                    scanEntireBigT = false;
                    if(columnFilters.length == 1){
                        if(columnFilters[0].compareTo("*") != 0){
                            scan = Minibase.getInstance().getBTree().new_scan(new StringKey(rowFilters[0] + columnFilter), new StringKey(rowFilters[1] + columnFilter + 'Z'));
                        }else{
                            scan = Minibase.getInstance().getBTree().new_scan(new StringKey(rowFilters[0]), new StringKey(rowFilters[1]+'Z'));
                        }
                    }else{
                        scan = Minibase.getInstance().getBTree().new_scan(new StringKey(rowFilters[0] + columnFilters[0]), new StringKey(rowFilters[1] + columnFilters[1] + 'Z'));
                    }
                }
                break;
            case 5:
                if(rowFilters.length == 1){
                    if (rowFilters[0].compareTo("*") != 0) {
                        scanEntireBigT = false;
                        if(valueFilters.length == 1){
                            if (valueFilters[0].compareTo("*") != 0) {
                                scan = Minibase.getInstance().getBTree().new_scan(new StringKey(rowFilter + valueFilter), new StringKey(rowFilter + valueFilter + 'Z'));
                            } else {
                                scan = Minibase.getInstance().getBTree().new_scan(new StringKey(rowFilter), new StringKey(rowFilter + 'Z'));
                            }
                        }else{
                            scan = Minibase.getInstance().getBTree().new_scan(new StringKey(rowFilter + valueFilters[0]), new StringKey(rowFilter + valueFilters[1] + 'Z'));
                        }
                    }else{
                        scanEntireBigT = true;
                    }
                }else{
                    scanEntireBigT = false;
                    if(valueFilters.length == 1){
                        if(valueFilters[0].compareTo("*") != 0){
                            scan = Minibase.getInstance().getBTree().new_scan(new StringKey(rowFilters[0] + valueFilter), new StringKey(rowFilters[1] + valueFilter + 'Z'));
                        }else{
                            scan = Minibase.getInstance().getBTree().new_scan(new StringKey(rowFilters[0]), new StringKey(rowFilters[1]+'Z'));
                        }
                    }else{
                        scan = Minibase.getInstance().getBTree().new_scan(new StringKey(rowFilters[0] + valueFilters[0]), new StringKey(rowFilters[1] + valueFilters[1] + 'Z'));
                    }
                }
                break;
            default:
                scanEntireBigT = true;
                break;
        }

        filterAndSortByOrderType(orderType, rowFilters, columnFilters, valueFilters);
    }

    private void filterAndSortByOrderType(int orderType, String[] rowFilters, String[] columnFilters,
                                          String[] valueFilters) throws Exception {
        if (!Minibase.getInstance().isCheckVersionsEnabled()) {
            tempHeapFile = new Heapfile("query_temp_heap_file");
        }
        if (scanEntireBigT) {
            //System.out.println("Scanning entire big t");
            MID mid = new MID();
            Map map = scanBigT.getNext(mid);
            while (map != null) {
                map.setHdr((short) 4, Minibase.getInstance().getAttrTypes(), Minibase.getInstance().getAttrSizes());
                if (filterOutput(map, rowFilters, columnFilters, valueFilters)) {
                    if (orderType == 6 && midCount < 3) {
                        //map.print();
                        MID vcMid = new MID(mid.pageNo, mid.slotNo);
                        mids[midCount++] = vcMid;
                    }
                    if (!Minibase.getInstance().isCheckVersionsEnabled()) {
                        tempHeapFile.insertMap(map.getMapByteArray());
                    }
                }
                mid = new MID();
                map = scanBigT.getNext(mid);
            }
        } else {
            KeyDataEntry entry = scan.get_next();
            while (entry != null) {
                MID mid = ((LeafData) entry.data).getData();
                if (mid != null) {
                    try {
                        Map map = Minibase.getInstance().getBigTable().getMap(mid);
                        map.setHdr((short) 4, Minibase.getInstance().getAttrTypes(), Minibase.getInstance().getAttrSizes());
                        if (filterOutput(map, rowFilters, columnFilters, valueFilters)) {
                            if (orderType == 6 && midCount < 3) {
                                //map.print();
                                MID vcMid = new MID(mid.pageNo, mid.slotNo);
                                mids[midCount++] = vcMid;
                            }
                            //System.out.println(map.getRowLabel());
                            if (!Minibase.getInstance().isCheckVersionsEnabled()) {
                                tempHeapFile.insertMap(map.getMapByteArray());
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                entry = scan.get_next();
            }
        }

        if (!Minibase.getInstance().isCheckVersionsEnabled()) {
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

    public void closeStream() throws Exception {
        if (filteredAndSortedData != null) {
            filteredAndSortedData.close();
        }
        if (scanBigT != null) {
            scanBigT.closescan();
        }
        if (scan != null) {
            scan.DestroyBTreeFileScan();
        }
    }

    public Map getNext() throws Exception {
        if (filteredAndSortedData == null) {
            System.out.println("something is wrong, might be check versions flag is enabled");
            return null;
        }
        Map m = null;
        try {
            m = filteredAndSortedData.get_next();
        } catch (OutOfSpaceException e) {
            closeStream();
        }
        if (m == null) {
            System.out.println("Deleting temp file used for sorting");
            tempHeapFile.deleteFile();
            closeStream();
            return null;
        }
        ++numberOfMapsFound;
        return m;
    }

    public void findAndDeleteMap(MID deleteMID) throws Exception {
        Map m = Minibase.getInstance().getBigTable().getMap(deleteMID);
        m.setHdr((short) 4, Minibase.getInstance().getAttrTypes(), Minibase.getInstance().getAttrSizes());
        Minibase.getInstance().getBigTable().deleteMap(deleteMID);
        if (Minibase.getInstance().getBigTable().getType() > 1) {
            switch (bigT.getType()) {
                case 2:
                    //System.out.println("Inside the 2 index type to Delete the record");
                    Minibase.getInstance().getBTree().Delete(new StringKey(m.getRowLabel()), deleteMID);
                    break;
                case 3:
                    //System.out.println("Inside the 3 index type to Delete the record");
                    Minibase.getInstance().getBTree().Delete(new StringKey(m.getColumnLabel()), deleteMID);
                    break;
                case 4:
                    //System.out.println("Inside the 4 index type to Delete the record");
                    Minibase.getInstance().getBTree().Delete(new StringKey(m.getRowLabel() + m.getColumnLabel()), deleteMID);
                    break;
                case 5:
                    //System.out.println("Inside the 5 index type to Delete the record");
                    Minibase.getInstance().getBTree().Delete(new StringKey(m.getRowLabel() + m.getValue()), deleteMID);
                    break;
            }
        }
    }

    public int getNumberOfMapsFound() {
        return numberOfMapsFound;
    }

    public MID[] getMids() {
        return mids;
    }

    public int getMidCount() {
        return midCount;
    }
}
