package bigt;

import Utility.GetMap;
import Utility.TimeStampMapMID;
import btree.StringKey;
import global.MID;
import global.MapOrder;
import heap.Heapfile;
import iterator.FileScan;
import iterator.FldSpec;
import iterator.RelSpec;
import iterator.Sort;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

/**
 * This class represents a single big table in the BigDB
 * It holds all the different types in a big table as bigTableParts
 */
public class BigTable {
    private List<BigT> bigTableParts;

    Set<Integer> emptyBigT;

    public BigTable() {
        bigTableParts = new ArrayList<>();
        bigTableParts.add(null);
    }

    public void insertMap(Map map, int type) throws Exception {
        //need to iterate through the bigTableParts list and check for versions
        bigTableParts.get(type).insertMap(map);
    }

    public void insertSingleMap(List<Map> mapList, int type) throws Exception {
        emptyBigT = new HashSet<Integer>();
        for (int i = 1; i <= 5; i++) {
            if (bigTableParts.get(i).getMapCnt() == 0) {
                emptyBigT.add(i);
            }
        }
        insertMapUtil(mapList, type);
        SortRecords(type);

//        BigTStream stream=null;
//        for(int i = 1; i<bigTableParts.size(); i++){
//            System.out.println("PRITING FOR TYPE------ " + i);
//            stream = bigTableParts.get(i).openStream("*","*","*");
//            MID mid = new MID();
//            Map map = stream.getNext(mid);
//            while(map!=null){
//                map.print();
//                mid = new MID();
//                map = stream.getNext(mid);
//            }
//            stream.closeStream();
//        }
    }

    public void insertMap(String dataFileName, String typeStr) throws Exception {

        emptyBigT = new HashSet<Integer>();
        for (int i = 1; i <= 5; i++) {
            if (bigTableParts.get(i).getMapCnt() == 0) {
                emptyBigT.add(i);
            }
        }


        int type = Integer.parseInt(typeStr);
        String line = "";
        String UTF8_BOM = "\uFEFF";
        BufferedReader br = new BufferedReader(new FileReader(dataFileName + ".csv"));
        List<Map> list = new ArrayList<>(3);

        while ((line = br.readLine()) != null) {
            String[] fields = line.split(",");
            if (fields.length == 4) {
                if (fields[0].startsWith(UTF8_BOM)) {
                    fields[0] = fields[0].substring(1).trim();
                }

                if (list.size() == 0 || (list.get(0).getRowLabel().compareTo(fields[0]) == 0 && list.get(0).getColumnLabel().compareTo(fields[1]) == 0)) {
                    list.add(GetMap.getMap(fields[0], fields[1], fields[2], fields[3]));
                } else {

                    insertMapUtil(list, type);
                    list.clear();
                    list.add(GetMap.getMap(fields[0], fields[1], fields[2], fields[3]));
                }
            }
        }
        if (list.size() != 0) {
            insertMapUtil(list, type);
        }
        System.out.println("ALL MAPS INSERTED");

        SortRecords(type);

        System.out.println("ALL MAPS SORTED");

//        BigTStream stream=null;
//        for(int i = 1; i<bigTableParts.size(); i++){
//            System.out.println("PRITING FOR TYPE------ " + i);
//            stream = bigTableParts.get(i).openStream("*","*","*");
//            MID mid = new MID();
//            Map map = stream.getNext(mid);
//            while(map!=null){
//                map.print();
//                mid = new MID();
//                map = stream.getNext(mid);
//            }
//            stream.closeStream();
//        }
    }

    public void SortRecords(int type) throws Exception {

        Heapfile tempHeapFile = new Heapfile("sort_temp_heap_file");
        BigTStream stream = bigTableParts.get(type).openStream("*", "*", "*");
        MID mid = new MID();
        Map map = stream.getNext(mid);
        while (map != null) {
            tempHeapFile.insertMap(map.getMapByteArray());
            mid = new MID();
            map = stream.getNext(mid);
        }
        stream.closeStream();

        FileScan fscan = null;
        FldSpec[] projlist = new FldSpec[4];
        RelSpec rel = new RelSpec(RelSpec.outer);
        projlist[0] = new FldSpec(rel, 1);
        projlist[1] = new FldSpec(rel, 2);
        projlist[2] = new FldSpec(rel, 3);
        projlist[3] = new FldSpec(rel, 4);

        try {
            fscan = new FileScan("sort_temp_heap_file", Minibase.getInstance().getAttrTypes(),
                    Minibase.getInstance().getAttrSizes(), (short) 4, 4, projlist, null);
        } catch (Exception e) {
            e.printStackTrace();
        }

        switch (type) {
            case 3:
            case 4:
                Minibase.getInstance().setOrderType(2);
                break;
            case 5:
                Minibase.getInstance().setOrderType(7);
                break;
            case 2:
                Minibase.getInstance().setOrderType(1);
            default:
                Minibase.getInstance().setOrderType(1);
        }

        String name = bigTableParts.get(type).getName();
        if (type != 1) {
            bigTableParts.get(type).getBTree().destroyFile();
        }
        bigTableParts.get(type).deleteBigt();
        BigT newbigT = new BigT(name, type);
        bigTableParts.set(type, newbigT);
        int memory = Minibase.getInstance().getNumberOfBuffersAvailable();
        Sort sort = new Sort(Minibase.getInstance().getAttrTypes(), (short) 4, Minibase.getInstance().getAttrSizes()
                , fscan, 1, new MapOrder(MapOrder.Ascending), Minibase.getInstance().getMaxRowKeyLength(),
                memory / 2);

        map = sort.get_next();
        while (map != null) {
            bigTableParts.get(type).insertMap(map);
            map = sort.get_next();
        }

        tempHeapFile.deleteFile();
        sort.close();
    }

    public void insertMapUtil(List<Map> mapList, int type) throws Exception {
        int MAP_LIMIT = 3;
        String rowKey = mapList.get(0).getRowLabel(), colKey = mapList.get(0).getColumnLabel();
        BigT bigT = null;
        BigTStream stream = null;

        List<TimeStampMapMID> currentMaps = new ArrayList<>(MAP_LIMIT);
        for (int i = 1; i < bigTableParts.size(); i++) {
            if (!emptyBigT.contains(i)) {
                bigT = bigTableParts.get(i);
                stream = bigT.openStream(rowKey, colKey, "*");
                MID mid = new MID();
                Map map = stream.getNext(mid);
                while (map != null) {
                    currentMaps.add(new TimeStampMapMID(map.getTimeStamp(), i, map, mid));
                    mid = new MID();
                    map = stream.getNext(mid);
                }
                stream.closeStream();
            }
        }

        Collections.sort(currentMaps, Comparator.comparingInt(TimeStampMapMID::getTimeStamp));

        for (int i = 0; mapList.size() + currentMaps.size() > 3 && currentMaps.size() != 0; i++) {
            //System.out.println("REMOVING " + mapList.size() + "--"+currentMaps.size() + "--" + currentMaps.get(0).getTimeStamp() + "--" + currentMaps.get(0).getBigTType());
            //System.out.println("BigTable Type "+ bigTableParts.get(currentMaps.get(0).getBigTType()).getType());
            bigTableParts.get(currentMaps.get(0).getBigTType()).deleteMap(currentMaps.get(0).getMid());
            if (currentMaps.get(0).getBigTType() != 1) {
                bigTableParts.get(currentMaps.get(0).getBigTType()).getBTree().Delete(getKey(currentMaps.get(0).getBigTType(), currentMaps.get(0).getMap()), currentMaps.get(0).getMid());
            }
            currentMaps.remove(0);
        }

        for (Map m : mapList) {
            bigTableParts.get(type).insertMap(m);
        }
    }


    public StringKey getKey(int type, Map map) throws IOException {
        if (type == 2) {
            return new StringKey(map.getRowLabel());
        } else if (type == 3) {
            return new StringKey(map.getColumnLabel());
        } else if (type == 4) {
            return new StringKey(map.getRowLabel() + map.getColumnLabel());
        } else if (type == 5) {
            return new StringKey(map.getRowLabel() + map.getValue());
        }
        return null;
    }

    public Stream openStream(int orderType, String rowFilter, String columnFilter, String valueFilter) throws Exception {
        //need to iterate through the bigTableParts list, get the maps from each BigT and sort
        return new Stream(this, orderType, rowFilter, columnFilter, valueFilter);
    }

    public void addBigTablePart(BigT bigT) {
        bigTableParts.add(bigT);
    }

    public int getMapCount() throws Exception {
        int mapCount = 0;
        BigT bigT = null;
        for (int i = 1; i < bigTableParts.size(); i++) {
            bigT = bigTableParts.get(i);
            mapCount += bigT.getMapCnt();
        }
        return mapCount;
    }

    public List<BigT> getBigTableParts() {
        return bigTableParts;
    }

    public void close() throws Exception {
        for (int i = 1; i <= 5; ++i) {
            if (bigTableParts.get(i).getBTree() != null) {
                bigTableParts.get(i).getBTree().close();
            }
        }
    }
}
