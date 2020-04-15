package Utility;

import bigt.Map;
import bigt.Minibase;

import java.io.IOException;

public class GetMap {

    public static Map getMap(String rowKey, String columnKey, String value, String timestamp) {
        Map map = new Map();
        try {
            map.setHdr((short) 4, Minibase.getInstance().getAttrTypes(), Minibase.getInstance().getAttrSizes());
        } catch (Exception e) {
            System.err.println("*** error in Tuple.setHdr() ***");
            e.printStackTrace();
        }

        Map map1 = new Map(map.size());
        try {
            map1.setHdr((short) 4, Minibase.getInstance().getAttrTypes(), Minibase.getInstance().getAttrSizes());
        } catch (Exception e) {
            System.err.println("*** error in Tuple.setHdr() ***");
            e.printStackTrace();
        }

        try {
            map1.setRowLabel(rowKey);
            map1.setColumnLabel(columnKey);
            map1.setTimeStamp(Integer.parseInt(timestamp));
            map1.setValue(Minibase.getInstance().getTransformedValue(value));
        } catch (IOException e) {
            e.printStackTrace();
        }

        return map1;
    }

    public static Map getJoinMap(String rowKey, String columnKey, String value, String timestamp) {

        short[] attrSizes = new short[3];
        attrSizes[0] = (short) (Minibase.getInstance().getMaxRowKeyLength()*2 + 1);
        attrSizes[1] = (short) (Minibase.getInstance().getMaxRowKeyLength() +
                Minibase.getInstance().getMaxColumnKeyLength() + 1);
        attrSizes[2] = (short) (Minibase.getInstance().getMaxValueLength());

        Map map = new Map();
        try {
            map.setHdr((short) 4, Minibase.getInstance().getAttrTypes(), attrSizes);
        } catch (Exception e) {
            System.err.println("*** error in Map.setHdr() ***");
            e.printStackTrace();
        }

        Map map1 = new Map(map.size());
        try {
            map1.setHdr((short) 4, Minibase.getInstance().getAttrTypes(), attrSizes);
        } catch (Exception e) {
            System.err.println("*** error in Map.setHdr() ***");
            e.printStackTrace();
        }

        try {
            map1.setRowLabel(rowKey);
            map1.setColumnLabel(columnKey);
            map1.setTimeStamp(Integer.parseInt(timestamp));
            map1.setValue(Minibase.getInstance().getTransformedValue(value));
        } catch (IOException e) {
            e.printStackTrace();
        }

        return map1;
    }
}
