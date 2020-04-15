package bigt;

import Utility.GetMap;
import global.AttrOperator;
import global.AttrType;
import global.MapOrder;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.Heapfile;
import iterator.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class BTRowJoin {

    private Heapfile leftTempHeapFile;
    private Heapfile rightTempHeapFile;
    private String joinColumnName;
    private BigTable rightBigTable;
    private BigT outBigT;

    public BTRowJoin(Stream leftStream, String rightBigTableName, String columnName, String outBigTableName) throws Exception {

        this.joinColumnName = columnName;

        leftTempHeapFile = new Heapfile("row_join_temp_heap_file_left");
        rightTempHeapFile = new Heapfile("row_join_temp_heap_file_right");

        outBigT = new BigT(outBigTableName, 1);

        //get all latest maps having the given columnName from the leftStream
        Map map = leftStream.getNext();
        Map latestMap = new Map(map);
        String prevRowKey = map.getRowLabel();
        while (map != null) {
            if (!map.getRowLabel().equals(prevRowKey)) {
                if (latestMap.getColumnLabel().equals(columnName)) {
                    leftTempHeapFile.insertMap(latestMap.getMapByteArray());
                }
                prevRowKey = map.getRowLabel();
                latestMap = new Map(map);
            }
            if (map.getColumnLabel().equals(columnName)) {
                latestMap = new Map(map);
            }
            map = leftStream.getNext();
        }
        if (latestMap.getColumnLabel().equals(columnName)) {
            leftTempHeapFile.insertMap(latestMap.getMapByteArray());
        }
        leftStream.close();

        rightBigTable = new BigTable();

        for (int type = 1; type <= 5; ++type) {
            try {
                BigT bigT = new BigT(rightBigTableName, type);
                rightBigTable.addBigTablePart(bigT);
            } catch (HFException | HFBufMgrException | HFDiskMgrException | IOException e) {
                e.printStackTrace();
            }
        }

        Stream rightStream = rightBigTable.openStream(1, "*",
                "*", "*");

        //get all latest maps having the given columnName from the leftStream
        map = rightStream.getNext();
        latestMap = new Map(map);
        prevRowKey = map.getRowLabel();
        while (map != null) {
            if (!map.getRowLabel().equals(prevRowKey)) {
                if (latestMap.getColumnLabel().equals(columnName)) {
                    rightTempHeapFile.insertMap(latestMap.getMapByteArray());
                }
                prevRowKey = map.getRowLabel();
                latestMap = new Map(map);
            }
            if (map.getColumnLabel().equals(columnName)) {
                latestMap = new Map(map);
            }
            map = rightStream.getNext();
        }
        if (latestMap.getColumnLabel().equals(columnName)) {
            rightTempHeapFile.insertMap(latestMap.getMapByteArray());
        }
        rightStream.close();

        sortMerge();
    }

    private void sortMerge() throws Exception {
        CondExpr[] outFilter = new CondExpr[2];
        outFilter[0] = new CondExpr();
        outFilter[1] = new CondExpr();

        Join_CondExpr(outFilter);

        FldSpec[] projlist = new FldSpec[4];
        RelSpec rel = new RelSpec(RelSpec.outer);
        projlist[0] = new FldSpec(rel, 1);
        projlist[1] = new FldSpec(rel, 2);
        projlist[2] = new FldSpec(rel, 3);
        projlist[3] = new FldSpec(rel, 4);

        Iterator left = null;
        try {
            left = new FileScan("row_join_temp_heap_file_left", Minibase.getInstance().getAttrTypes(),
                    Minibase.getInstance().getAttrSizes(),
                    (short) 4, (short) 4,
                    projlist, null);
        } catch (Exception e) {
            System.err.println("" + e);
        }

        Iterator right = null;
        try {
            right = new FileScan("row_join_temp_heap_file_right", Minibase.getInstance().getAttrTypes(),
                    Minibase.getInstance().getAttrSizes(),
                    (short) 4, (short) 4,
                    projlist, null);
        } catch (Exception e) {
            System.err.println("" + e);
        }

        MapOrder order = new MapOrder(MapOrder.Ascending);
        SortMerge sm = null;
        try {
            sm = new SortMerge(Minibase.getInstance().getAttrTypes(), 4, Minibase.getInstance().getAttrSizes(),
                    Minibase.getInstance().getAttrTypes(), 4, Minibase.getInstance().getAttrSizes(),
                    4, 4,
                    4, 4,
                    20,
                    left, right,
                    false, false, order,
                    outFilter, projlist, 1);
        } catch (Exception e) {
            System.err.println("" + e);
        }

        Map map = sm.get_next();
        while (map != null) {
            joinTwoRows(map.getRowLabel(), map.getColumnLabel());
            map = sm.get_next();
        }

        sm.close();
    }

    private void joinTwoRows(String rowKeyLeft, String rowKeyRight) throws Exception {
        Stream leftStream = Minibase.getInstance().getBigTable().openStream(0, rowKeyLeft,
                "*", "*");

        Map map = leftStream.getNext();
        List<Map> commonMaps = new ArrayList<>();
        while (map != null) {
            if (map.getColumnLabel().equals(joinColumnName)) {
                commonMaps.add(map);
            } else {
                Map m = GetMap.getJoinMap(rowKeyLeft + ":" + rowKeyRight, rowKeyLeft + "_" +
                        map.getColumnLabel(), map.getValue(), Integer.toString(map.getTimeStamp()));
                outBigT.insertMap(m);
            }
            map = leftStream.getNext();
        }
        leftStream.close();

        Stream rightStream = rightBigTable.openStream(0, rowKeyRight,
                "*", "*");

        map = rightStream.getNext();
        while (map != null) {
            if (map.getColumnLabel().equals(joinColumnName)) {
                commonMaps.add(map);
            } else {
                Map m = GetMap.getJoinMap(rowKeyLeft + ":" + rowKeyRight, rowKeyRight + "_" +
                        map.getColumnLabel(), map.getValue(), Integer.toString(map.getTimeStamp()));
                outBigT.insertMap(m);
            }
            map = rightStream.getNext();
        }
        rightStream.close();

        eliminateDuplicates(commonMaps);
        for (Map commonMap : commonMaps) {
            Map m = GetMap.getJoinMap(rowKeyLeft + ":" + rowKeyRight,
                    commonMap.getColumnLabel(), commonMap.getValue(), Integer.toString(commonMap.getTimeStamp()));
            outBigT.insertMap(m);
        }
    }

    private void eliminateDuplicates(List<Map> maps) throws Exception {
        if (maps.size() <= 3) {
            return;
        }

        do {
            int minIndex = 0;
            int minTimeStamp = maps.get(0).getTimeStamp();
            for (int i = 1; i < maps.size(); i++) {
                if (maps.get(i).getTimeStamp() < minTimeStamp) {
                    minTimeStamp = maps.get(i).getTimeStamp();
                    minIndex = i;
                }
            }

            maps.remove(minIndex);
        } while (maps.size() != 3);

    }

    private void Join_CondExpr(CondExpr[] expr) {
        expr[0].next = null;
        expr[0].op = new AttrOperator(AttrOperator.aopEQ);
        expr[0].type1 = new AttrType(AttrType.attrSymbol);
        expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 4);
        expr[0].type2 = new AttrType(AttrType.attrSymbol);
        expr[0].operand2.symbol = new FldSpec(new RelSpec(RelSpec.innerRel), 4);
        expr[1] = null;
    }

    public void close() throws Exception {
        leftTempHeapFile.deleteFile();
        rightTempHeapFile.deleteFile();
    }
}
