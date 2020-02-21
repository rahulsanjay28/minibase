package bigt;


public class Map {

    private String RowLabel;
    private String ColumnLabel;
    private int TimeStamp;
    private String Value;

    public Map() {

    }

    public Map(byte[] amap, int offset) {

    }

    public Map(Map fromMap) {

    }

    public String getRowLabel() {
        return RowLabel;
    }

    public void setRowLabel(String rowlabel) {
        RowLabel = rowlabel;
    }

    public String getColumnLabel() {
        return ColumnLabel;
    }

    public void setColumnLabel(String columnlabel) {
        ColumnLabel = columnlabel;
    }

    public int getTimeStamp() {
        return TimeStamp;
    }

    public void setTimeStamp(int timestamp) {
        TimeStamp = timestamp;
    }

    public String getValue() {
        return Value;
    }

    public void setValue(String value) {
        Value = value;
    }

    public byte[] getMapByteArray() {
        return null;
    }

    public void print() {

    }

    public int size() {
        return 0;
    }

    public Map mapCopy(Map fromMap) {
        return null;
    }

    public Map mapInit(byte[] amap, int offset) {
        return null;
    }

    public void mapSet(byte[] frommap, int offset) {

    }

}
