package bigt;

import Utility.GetMap;
import global.MID;
import heap.Heapfile;

import java.util.ArrayList;
import java.util.List;

public class BigTableCatalog {

    public static void addBigTInfo(BigTableInfo bigTableInfo) throws Exception {
        Heapfile bigTableCatalog = new Heapfile("big_table_catalog");
        bigTableCatalog.insertMap(GetMap.getMap(bigTableInfo.getName(),
                "0", "0", "0").getMapByteArray());
    }

    public static List<BigTableInfo> getAllBigTablesInBigDB() throws Exception {

        List<BigTableInfo> bigTableInfoList = new ArrayList<>();
        Heapfile bigTableCatalog = new Heapfile("big_table_catalog");
        heap.Scan scan = bigTableCatalog.openScan();
        MID mid = new MID();
        Map m = scan.getNext(mid);
        while(m != null){
            m.setHdr((short) 4, Minibase.getInstance().getAttrTypes(), Minibase.getInstance().getAttrSizes());
            bigTableInfoList.add(new BigTableInfo(m.getRowLabel()));
            mid = new MID();
            m = scan.getNext(mid);
        }
        scan.closescan();
        return bigTableInfoList;
    }
}
