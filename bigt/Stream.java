package bigt;

import btree.BTFileScan;
import btree.KeyDataEntry;
import btree.LeafData;
import btree.StringKey;
import global.RID;

public class Stream {

    private BTFileScan scan;
    private int numberOfMapsFound;
    private Scan scanBigT;
    private RID rid;

    public Stream(BigT bigtable, int orderType, String rowFilter, String columnFilter, String valueFilter) throws Exception {
        this.numberOfMapsFound = 0;
        scanBigT = new Scan(bigtable);
        switch (bigtable.getType()) {
            case 2:
                System.out.println("rowFilter: " + rowFilter);
                scan = Minibase.getInstance().getBTree().new_scan(new StringKey(rowFilter), new StringKey(rowFilter));
//                rid = getFirstRID();
//                System.out.println("position call " + scanBigT.position(rid));
                break;
            case 3:
                scan = Minibase.getInstance().getBTree().new_scan(new StringKey(columnFilter), new StringKey(columnFilter));
                break;
            case 4:
                scan = Minibase.getInstance().getBTree().new_scan(new StringKey(rowFilter + columnFilter), new StringKey(rowFilter + columnFilter));
                break;
            case 5:
                scan = Minibase.getInstance().getBTree().new_scan(new StringKey(rowFilter + valueFilter), new StringKey(rowFilter + valueFilter));
                break;
            default:
                //need to change this, normal scan will have less read cost than this
                scan = Minibase.getInstance().getBTree().new_scan(null, null);
                break;
        }
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
        KeyDataEntry entry = scan.get_next();
        if (entry == null) {
            return null;
        }
        RID rid = ((LeafData) entry.data).getData();
        if (rid != null) {
            try {
                ++numberOfMapsFound;
                Map map2 = Minibase.getInstance().getBigTable().getMap(rid);
                map2.setOffsets(map2.getOffset());
                return map2;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return null;
//        RID rid = new RID();
//        Map map = scanBigT.getNext(rid);
//        map.setOffsets(map.getOffset());
//        return map;
    }

//    public void scanBigTCode(){
//        System.out.println("Scanning the big table");
//        PCounter.getInstance().setWriteCount(0);
//        PCounter.getInstance().setReadCount(0);
//        Scan scan = new Scan(Minibase.getInstance().getBigTable());
//        RID rid = new RID();
//        Map map = scan.getNext(rid);
//        while(map != null){
//            map.setOffsets(map.getOffset());
//            System.out.println(map.getRowLabel() + " " + map.getColumnLabel() + " " +
//                    map.getTimeStamp() + " " + map.getValue());
//            map = scan.getNext(rid);
//        }
//        System.out.println("Total number of reads " + PCounter.getInstance().getReadCount());
//        System.out.println("Total number of writes " + PCounter.getInstance().getWriteCount());
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
