package bigt;

import btree.BTFileScan;
import btree.KeyDataEntry;
import btree.LeafData;
import btree.StringKey;
import global.MID;

import java.io.IOException;

public class BigTStream {
    private BTFileScan scan;
    private int numberOfMapsFound;
    private Scan scanBigT;
    private BigT bigT;
    private boolean scanEntireBigT;

    private String[] rowFilters;
    private String[] columnFilters;
    private String[] valueFilters;

    public BigTStream(BigT bigt, String rowFilter, String columnFilter, String valueFilter) throws Exception {
        this.numberOfMapsFound = 0;
        this.bigT = bigt;
        scanBigT = new Scan(bigT);

        rowFilters = sanitizefilter(rowFilter);
        columnFilters = sanitizefilter(columnFilter);
        valueFilters = sanitizefilter(valueFilter);

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
                        scan = bigT.getBTree().new_scan(new StringKey(rowFilter), new StringKey(rowFilter));
                    } else {
                        scan = bigT.getBTree().new_scan(new StringKey(rowFilters[0]), new StringKey(rowFilters[1] + 'Z'));
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
                        scan = bigT.getBTree().new_scan(new StringKey(columnFilter), new StringKey(columnFilter));
                    } else {
                        scan = bigT.getBTree().new_scan(new StringKey(columnFilters[0]), new StringKey(columnFilters[1] + 'Z'));
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
                                scan = bigT.getBTree().new_scan(new StringKey(rowFilter + columnFilter), new StringKey(rowFilter + columnFilter + 'Z'));
                            } else {
                                scan = bigT.getBTree().new_scan(new StringKey(rowFilter), new StringKey(rowFilter + 'Z'));
                            }
                        }else{
                            scan = bigT.getBTree().new_scan(new StringKey(rowFilter + columnFilters[0]), new StringKey(rowFilter + columnFilters[1] + 'Z'));
                        }
                    }else{
                        scanEntireBigT = true;
                    }
                }else{
                    scanEntireBigT = false;
                    if(columnFilters.length == 1){
                        if(columnFilters[0].compareTo("*") != 0){
                            scan = bigT.getBTree().new_scan(new StringKey(rowFilters[0] + columnFilter), new StringKey(rowFilters[1] + columnFilter + 'Z'));
                        }else{
                            scan = bigT.getBTree().new_scan(new StringKey(rowFilters[0]), new StringKey(rowFilters[1]+'Z'));
                        }
                    }else{
                        scan = bigT.getBTree().new_scan(new StringKey(rowFilters[0] + columnFilters[0]), new StringKey(rowFilters[1] + columnFilters[1] + 'Z'));
                    }
                }
                break;
            case 5:
                if(rowFilters.length == 1){
                    if (rowFilters[0].compareTo("*") != 0) {
                        scanEntireBigT = false;
                        if(valueFilters.length == 1){
                            if (valueFilters[0].compareTo("*") != 0) {
                                scan = bigT.getBTree().new_scan(new StringKey(rowFilter + valueFilter), new StringKey(rowFilter + valueFilter + 'Z'));
                            } else {
                                scan = bigT.getBTree().new_scan(new StringKey(rowFilter), new StringKey(rowFilter + 'Z'));
                            }
                        }else{
                            scan = bigT.getBTree().new_scan(new StringKey(rowFilter + valueFilters[0]), new StringKey(rowFilter + valueFilters[1] + 'Z'));
                        }
                    }else{
                        scanEntireBigT = true;
                    }
                }else{
                    scanEntireBigT = false;
                    if(valueFilters.length == 1){
                        if(valueFilters[0].compareTo("*") != 0){
                            scan = bigT.getBTree().new_scan(new StringKey(rowFilters[0] + valueFilter), new StringKey(rowFilters[1] + valueFilter + 'Z'));
                        }else{
                            scan = bigT.getBTree().new_scan(new StringKey(rowFilters[0]), new StringKey(rowFilters[1]+'Z'));
                        }
                    }else{
                        scan = bigT.getBTree().new_scan(new StringKey(rowFilters[0] + valueFilters[0]), new StringKey(rowFilters[1] + valueFilters[1] + 'Z'));
                    }
                }
                break;
            default:
                scanEntireBigT = true;
                break;
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
        if (scanBigT != null) {
            scanBigT.closescan();
        }
        if (scan != null) {
            scan.DestroyBTreeFileScan();
        }
    }

    public Map getNext() throws Exception {
        if (scanEntireBigT) {
            MID mid = new MID();
            Map map = scanBigT.getNext(mid);
            while (map != null) {
                map.setHdr((short) 4, Minibase.getInstance().getAttrTypes(), Minibase.getInstance().getAttrSizes());
                if (filterOutput(map, rowFilters, columnFilters, valueFilters)) {
                    ++numberOfMapsFound;
                    return map;
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
                        Map map = bigT.getMap(mid);
                        map.setHdr((short) 4, Minibase.getInstance().getAttrTypes(), Minibase.getInstance().getAttrSizes());
                        if (filterOutput(map, rowFilters, columnFilters, valueFilters)) {
                            ++numberOfMapsFound;
                            return map;
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                entry = scan.get_next();
            }
        }
        return null;
    }

    public int getNumberOfMapsFound() {
        return numberOfMapsFound;
    }
}
