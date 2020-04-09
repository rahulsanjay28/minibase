package Utility;

import bigt.Map;
import global.MID;

public class TimeStampMapMID {

    int timeStamp;
    int bigTType;
    Map map;
    MID mid;

    public TimeStampMapMID(int timeStamp, int bigTType, Map map, MID mid) {
        this.timeStamp = timeStamp;
        this.bigTType = bigTType;
        this.map = map;
        this.mid = mid;
    }

    public int getBigTType() {
        return bigTType;
    }

    public void setBigTType(int bigTType) {
        this.bigTType = bigTType;
    }

    public int getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(int timeStamp) {
        this.timeStamp = timeStamp;
    }

    public Map getMap() {
        return map;
    }

    public void setMap(Map map) {
        this.map = map;
    }

    public MID getMid() {
        return mid;
    }

    public void setMid(MID mid) {
        this.mid = mid;
    }
}
