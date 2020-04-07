package bigt;

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
