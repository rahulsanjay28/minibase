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

    //This set stores all BigT indexes which has 0 Maps so that while searching we don't open unnecessary streams.
    Set<Integer> emptyBigT;

    public BigTable() {
        bigTableParts = new ArrayList<>();
        bigTableParts.add(null);
    }

    //This method is not being used anymore
    public void insertMap(Map map, int type) throws Exception {
        //need to iterate through the bigTableParts list and check for versions
        bigTableParts.get(type).insertMap(map);
    }

    //This method is used to insert single map for MapInsert functionality
    public void insertSingleMap(List<Map> mapList, int type) throws Exception {

        emptyBigT = new HashSet<Integer>();
        for (int i = 1; i <= 5; i++) {

            //Adding index into set if map count is 0 for particular BigT
            if (bigTableParts.get(i).getMapCnt() == 0) {
                emptyBigT.add(i);
            }
        }

        //Inserting single map
        insertMapUtil(mapList, type);

        //Sorting BigT according to proper storage type
        SortRecords(type);
    }

    //This method will be used for BatchInsert
    public void insertMap(String dataFileName, String typeStr) throws Exception {

        emptyBigT = new HashSet<Integer>();
        for (int i = 1; i <= 5; i++) {
            //Adding index into set if map count is 0 for particular BigT
            if (bigTableParts.get(i).getMapCnt() == 0) {
                emptyBigT.add(i);
            }
        }


        int type = Integer.parseInt(typeStr);
        String line = "";
        String UTF8_BOM = "\uFEFF";
        BufferedReader br = new BufferedReader(new FileReader(dataFileName + ".csv"));
        List<Map> list = new ArrayList<>(3);

        //Reading CSV file which has only latest 3 maps per specific row and column key. This file will be sorted according to RowKey
        while ((line = br.readLine()) != null) {
            String[] fields = line.split(",");
            if (fields.length == 4) {
                if (fields[0].startsWith(UTF8_BOM)) {
                    fields[0] = fields[0].substring(1).trim();
                }

                //If current Map's Row and Column key is same as previous one or this is first Map for Row and Column key, we'll add it to map list.
                if (list.size() == 0 || (list.get(0).getRowLabel().compareTo(fields[0]) == 0 && list.get(0).getColumnLabel().compareTo(fields[1]) == 0)) {
                    list.add(GetMap.getMap(fields[0], fields[1], fields[2], fields[3]));
                } else {
                    //We'll call map insert for this list which will have at most 3 maps.
                    insertMapUtil(list, type);
                    list.clear();
                    list.add(GetMap.getMap(fields[0], fields[1], fields[2], fields[3]));
                }
            }
        }
        //We'll call map insert on last set of maps
        if (list.size() != 0) {
            insertMapUtil(list, type);
        }
        System.out.println("ALL MAPS INSERTED");

        //After inserting we'll sort particular BigT
        SortRecords(type);

        System.out.println("ALL MAPS SORTED");
    }

    public void SortRecords(int type) throws Exception {

        //This heapfile is used to hold Maps temporarily and sort Maps.
        Heapfile tempHeapFile = new Heapfile("sort_temp_heap_file");

        //Opening stream on BigT which needs to be sorted. We'll use * * * filter so that we read every record.
        BigTStream stream = bigTableParts.get(type).openStream("*", "*", "*");
        MID mid = new MID();
        Map map = stream.getNext(mid);
        while (map != null) {

            //Inserting Map in temp file.
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

        //Setting ordertype according to type of BigT
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
            //Removing BTree file corrosponding to BigT as indexes needs to be made again.
            bigTableParts.get(type).getBTree().destroyFile();
        }
        //Deleting BigT
        bigTableParts.get(type).deleteBigt();

        //Creating new BigT in which we'll insert data in particular order
        BigT newbigT = new BigT(name, type);
        bigTableParts.set(type, newbigT);
        int memory = Minibase.getInstance().getNumberOfBuffersAvailable();
        Sort sort = new Sort(Minibase.getInstance().getAttrTypes(), (short) 4, Minibase.getInstance().getAttrSizes()
                , fscan, 1, new MapOrder(MapOrder.Ascending), Minibase.getInstance().getMaxRowKeyLength(),
                memory / 2);

        map = sort.get_next();
        while (map != null) {
            //Inserting sorted maps to BigT. This will also insert map in Btree
            bigTableParts.get(type).insertMap(map);
            map = sort.get_next();
        }

        //Deleting temp file used for sorting
        tempHeapFile.deleteFile();
        sort.close();
    }

    //This method will add most recent Maps which were there in input and removed older maps with particular row and column key
    public void insertMapUtil(List<Map> mapList, int type) throws Exception {
        int MAP_LIMIT = 3;
        String rowKey = mapList.get(0).getRowLabel(), colKey = mapList.get(0).getColumnLabel();
        BigT bigT = null;
        BigTStream stream = null;

        List<TimeStampMapMID> currentMaps = new ArrayList<>(MAP_LIMIT);
        //This loop will iterate on all type of BigT and will get Maps with particular row and column keys.
        for (int i = 1; i < bigTableParts.size(); i++) {
            if (!emptyBigT.contains(i)) {
                bigT = bigTableParts.get(i);
                //Opening stream with rowkey and colkey which needs to be inserted.
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

        // As we need most recent 3 Map in BigT at particular time, we're sorting current map according to timestamp
        Collections.sort(currentMaps, Comparator.comparingInt(TimeStampMapMID::getTimeStamp));

        //We'll remove maps such that we have 3 Maps for particluar row and column key.
        for (int i = 0; mapList.size() + currentMaps.size() > 3 && currentMaps.size() != 0; i++) {
            //Removing Map from BigT
            bigTableParts.get(currentMaps.get(0).getBigTType()).deleteMap(currentMaps.get(0).getMid());
            if (currentMaps.get(0).getBigTType() != 1) {
                //Removing Map entry from BTree
                bigTableParts.get(currentMaps.get(0).getBigTType()).getBTree().Delete(getKey(currentMaps.get(0).getBigTType(), currentMaps.get(0).getMap()), currentMaps.get(0).getMid());
            }
            currentMaps.remove(0);
        }

        for (Map m : mapList) {
            //Adding map to BigT. This will also add Map entry to BTree
            bigTableParts.get(type).insertMap(m);
        }
    }


    //This function  will return key for BTree according to BTree type
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
