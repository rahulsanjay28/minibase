package bigt;

import btree.KeyClass;
import global.RID;
import btree.KeyDataEntry;

public class BTreeData{
    private RID rid;
    private Map map;
    private KeyClass key;

    public BTreeData(RID rid,Map map,KeyClass key){
        this.rid =rid;
        this.map = map;
        this.map.setOffsets(map.getOffset());
        this.key = key;
    }
    public RID getRid(){return this.rid;}
    public  Map getMap(){return  this.map;}
    public  KeyClass getKey(){return  this.key;}

}
