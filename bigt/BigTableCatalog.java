package bigt;

import Utility.GetMap;
import global.AttrType;
import global.MID;
import heap.Heapfile;

import java.util.ArrayList;
import java.util.List;

public class BigTableCatalog {

    private static final int maxRowKeyLength = 15;
    private static final int maxColumnKeyLength = 1;
    private static final int maxValueLength = 1;

    private static AttrType[] attrTypes = null;
    private static short[] attrSizes = null;

    public static void addBigTInfo(BigTableInfo bigTableInfo) throws Exception {
        Heapfile bigTableCatalog = new Heapfile("big_table_catalog");
        bigTableCatalog.insertMap(GetMap.getCatalogMap(bigTableInfo.getName(),
                "0", "0", "0").getMapByteArray());
    }

    public static List<BigTableInfo> getAllBigTablesInBigDB() throws Exception {

        List<BigTableInfo> bigTableInfoList = new ArrayList<>();
        Heapfile bigTableCatalog = new Heapfile("big_table_catalog");
        heap.Scan scan = bigTableCatalog.openScan();
        MID mid = new MID();
        Map m = scan.getNext(mid);
        while (m != null) {
            m.setHdr((short) 4, getAttrTypes(), getAttrSizes());
            bigTableInfoList.add(new BigTableInfo(m.getRowLabel()));
            mid = new MID();
            m = scan.getNext(mid);
        }
        scan.closescan();
        return bigTableInfoList;
    }

    public static AttrType[] getAttrTypes() {
        if (attrTypes == null) {
            attrTypes = new AttrType[4];
            attrTypes[0] = new AttrType(AttrType.attrString);
            attrTypes[1] = new AttrType(AttrType.attrString);
            attrTypes[2] = new AttrType(AttrType.attrInteger);
            attrTypes[3] = new AttrType(AttrType.attrString);
        }
        return attrTypes;
    }

    public static short[] getAttrSizes() {
        if (attrSizes == null) {
            attrSizes = new short[3];
            attrSizes[0] = (short) (maxRowKeyLength);
            attrSizes[1] = (short) (maxColumnKeyLength);
            attrSizes[2] = (short) (maxValueLength);
        }
        return attrSizes;
    }
}
