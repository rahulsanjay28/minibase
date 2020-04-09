import bigt.Map;
import bigt.Minibase;
import Utility.GetMap;

import java.util.ArrayList;
import java.util.List;

public class MapInsert {


    public static void main(String[] args) throws Exception {
        MapInsert mapInsert = new MapInsert();
        mapInsert.execute(args[0], args[1], args[2], args[3], args[4], args[5], args[6]);
    }
    public void execute(String rowLabel, String columnLabel, String value, String timeStamp, String type, String bigTableName, String numBuf) throws Exception {
        Minibase.getInstance().init(bigTableName, Integer.parseInt(numBuf));
        List<Map> list = new ArrayList<>();
        list.add(GetMap.getMap(rowLabel, columnLabel, value, timeStamp));
        Minibase.getInstance().getBigTable().insertSingleMap(list, Integer.parseInt(type));
    }
}
