package bigt;

import global.MID;
import heap.Heapfile;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Scan;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class BigTable {
    private List<BigT> bigTableParts;

    BigTable(){
        bigTableParts = new ArrayList<>();
    }

    public void insertMap(Map map, int type) throws Exception{
        //need to iterate through the bigTableParts list and check for versions
        bigTableParts.get(type).insertMap(map);
    }

    public void insertMap(Heapfile dataFile) throws InvalidTupleSizeException, IOException, InvalidTypeException {
        Scan sc = dataFile.openScan();
        MID mid = new MID();
        Map m1 = sc.getNext(mid);

        int count=0;
        while(m1!=null){
            m1.setHdr((short) 4, Minibase.getInstance().getAttrTypes(), Minibase.getInstance().getAttrSizes());
            //System.out.println("MY MAP");
            m1.print();
            count++;
            m1= sc.getNext(mid);
        }
        System.out.println(count);
    }

    public Stream openStream(int orderType, String rowFilter, String columnFilter, String valueFilter) throws Exception{
        //need to iterate through the bigTableParts list, get the maps from each BigT and sort
        Stream stream = new Stream(orderType, rowFilter, columnFilter, valueFilter);
        return stream;
    }

    public void addBigTablePart(BigT bigT){
        bigTableParts.add(bigT);
    }

    public int getMapCount() throws Exception{
        int mapCount = 0;
        for(BigT bigT : bigTableParts){
            mapCount += bigT.getMapCnt();
        }
        return mapCount;
    }

    public List<BigT> getBigTableParts(){
        return bigTableParts;
    }
}
