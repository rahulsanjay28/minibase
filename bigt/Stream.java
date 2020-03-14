package bigt;

import btree.BTFileScan;
import btree.KeyDataEntry;
import btree.LeafData;
import btree.StringKey;
import btree.IntegerKey;
import global.AttrType;
import global.RID;

public class Stream {
    private BTFileScan scan,scan2;
    private int numberOfMapsFound;
    private Scan scanBigT;
    private RID rid;
    private BigT bigT;
    private boolean scanEntireBigT = false;

    public Stream(BigT bigtable, int orderType, String rowFilter, String columnFilter, String valueFilter) throws Exception {
        this.numberOfMapsFound = 0;
        this.bigT = bigtable;

        scanBigT = new Scan(bigtable);

        String rowFilters[] = sanitizefilter(rowFilter);
        String columnFilters[] = sanitizefilter(columnFilter);
        String valueFilters[] = sanitizefilter(valueFilter);
        //scanBigT = new Scan(bigT);

        switch (bigT.getType()) {
            case 2:
                //System.out.println("rowFilter: " + rowFilter);

                if(rowFilters.length==1)
                    scan = Minibase.getInstance().getBTree().new_scan(new StringKey(rowFilter), new StringKey(rowFilter));
                else
                    scan = Minibase.getInstance().getBTree().new_scan(new StringKey(rowFilters[0]), new StringKey(rowFilters[1]));
//                rid = getFirstRID();
//                System.out.println("position call " + scanBigT.position(rid));
                break;
            case 3:
                if(columnFilters.length==1)
                    scan = Minibase.getInstance().getBTree().new_scan(new StringKey(columnFilter), new StringKey(columnFilter));
                else
                    scan = Minibase.getInstance().getBTree().new_scan(new StringKey(columnFilters[0]), new StringKey(columnFilters[1]));
                break;
            case 4:

                if(rowFilters.length==1 && columnFilters[0].compareTo("*")==0)
                {
                    scanEntireBigT = true;
                }
                if(columnFilters.length==1 && rowFilters.length==1)
                    scan = Minibase.getInstance().getBTree().new_scan(new StringKey(rowFilter + columnFilter), new StringKey(rowFilter + columnFilter));

                else if(rowFilters.length==1 && columnFilters.length!=1)
                    scan = Minibase.getInstance().getBTree().new_scan(new StringKey(rowFilter + columnFilters[0]), new StringKey(rowFilter + columnFilters[1]));

                else if(rowFilters.length!=1 && columnFilters.length==1)
                    scan = Minibase.getInstance().getBTree().new_scan(new StringKey(rowFilters[0] + columnFilter), new StringKey(rowFilters[1] + columnFilter));

                else
                    scan = Minibase.getInstance().getBTree().new_scan(new StringKey(rowFilters[0] + columnFilters[0]), new StringKey(rowFilters[1] + columnFilters[1]));

                //scan = Minibase.getInstance().getBTree().new_scan(new StringKey(rowFilter + columnFilter), new StringKey(rowFilter + columnFilter));
                scan2 = Minibase.getInstance().getSecondaryBTree().new_scan(null, null);


                break;
            case 5:
                scan = Minibase.getInstance().getBTree().new_scan(new StringKey(rowFilter + valueFilter), new StringKey(rowFilter + valueFilter));
                scan2 = Minibase.getInstance().getSecondaryBTree().new_scan(null, null);

                break;
            default:
                //need to change this, normal scan will have less read cost than this
                scanBigT = new Scan(bigT);
                break;
        }
    }

    public void unsetScanEntireBigT()
    {
        scanEntireBigT = false;
    }

    public String[] sanitizefilter(String filter)
    {
        String s[];
        if(filter.startsWith("["))
        {
            s = filter.substring(1, filter.length()-1).split(",");
            if(s[0].compareTo(s[1])==0)
            {
                String t[] = new String[1];
                t[0]=s[0];
                return t;
            }
        }
        else
        {
            s = new String[1];
            s[0] = filter;
        }
        return s;
    }

    public void closeStream() {

    }

    public RID getFirstRID() throws Exception{
        KeyDataEntry entry = scan.get_next();
        if (entry == null) {
            return null;
        }
        RID rid = ((LeafData) entry.data).getData();
        return rid;
    }

    public Map getNext() throws Exception {
        if(bigT.getType() == 1 || scanEntireBigT){
            //System.out.println("Scanning entire big t");
            RID rid = new RID();
            Map map = scanBigT.getNext(rid);
            if(map == null) {
                return null;
            }
            map.setOffsets(map.getOffset());
            return map;
        }

        KeyDataEntry entry = scan.get_next();
        if (entry == null) {
            return null;
        }
        RID rid = ((LeafData) entry.data).getData();
        if (rid != null) {
            try {
                ++numberOfMapsFound;
                Map map2 = Minibase.getInstance().getBigTable().getMap(rid);
                map2.setHdr((short) 4, Minibase.getInstance().getAttrTypes(), Minibase.getInstance().getAttrSizes());
                return map2;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    public BTreeData getNextMap() throws Exception {
        KeyDataEntry entry = scan.get_next();
        BTreeData bData;
        if (entry == null) {
            return null;
        }
        RID rid = ((LeafData) entry.data).getData();
        if (rid != null) {
            try{
                Map map2 = Minibase.getInstance().getBigTable().getMap(rid);
                map2.setOffsets(map2.getOffset());
                bData = new BTreeData(rid,map2,entry.key);
                return  bData;
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
