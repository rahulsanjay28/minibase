package bigt;

import java.io.*;
import java.lang.*;

import global.*;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;

public class Map1 implements GlobalConst {

    /**
     * Maximum size of any tuple
     */
    public static final int max_size = MINIBASE_PAGESIZE;

    /**
     * a byte array to hold data
     */
    private byte[] data; //need to discuss

    /**
     * start position of this tuple in data[]
     */
    private int map_offset;

    private String rowLabel; // Need to discuss
    private String columnLabel; // Need to discuss
    private int timeStamp; // Need to discuss
    private String value; // Need to discuss

    private short[] fldOffset;

    private int map_length;

    private static short numFlds = 4;
    /**
     * Class constructor
     * Creat a new tuple with length = max_size,tuple offset = 0.
     */

    public Map1() {
        rowLabel = "";
        columnLabel = "";
        timeStamp = -1; // currentTimeStamp
        map_offset = 0;
        data = new byte[max_size];
        map_length = max_size;
    }

    /**
     * Constructor
     *
     * @param amap   a byte array which contains the tuple
     * @param offset the offset of the tuple in the byte array
     */
    public Map1(byte[] amap, int offset, int length) {
        data = amap;
        map_offset = offset;
        map_length = length;
    }

    /**
     * Constructor(used as tuple copy)
     *
     * @param fromMap a byte array which contains the tuple
     */
    public Map1(Map1 fromMap) {
        data = fromMap.getMapByteArray();
        map_offset = 0;
        fldOffset = fromMap.copyFldOffset();
        map_length = fromMap.getLength();
    }

    /**
     * Copy the tuple byte array out
     *
     * @return byte[], a byte array contains the Map
     * the length of byte[] = length of the Map
     */

    public byte[] getMapByteArray() {
        byte[] mapcopy = new byte[map_length];
        System.arraycopy(data, map_offset, mapcopy, 0, map_length);
        return mapcopy;
    }

    public void print() {
        System.out.println(rowLabel + " , " + columnLabel + " , " + timeStamp + " , " + value);
    }

    /**
     * get the length of a tuple, call this method if you did
     * call setHdr () before
     *
     * @return size of this tuple in bytes
     */
    public short size() {
        return ((short) (fldOffset[4] - map_offset));
    }


    /**
     * Copy a tuple to the current tuple position
     * you must make sure the tuple lengths must be equal
     *
     * @param fromMap the tuple being copied
     */
    public void mapCopy(Map1 fromMap) {
        byte[] temparray = fromMap.getMapByteArray();
        System.arraycopy(temparray, 0, data, map_offset, map_length);
        fldOffset = fromMap.copyFldOffset();
    }

    /**
     * This is used when you don't want to use the constructor
     *
     * @param aMap   a byte array which contains the Map
     * @param offset the offset of the tuple in the byte array
     * @param length the length of the tuple
     */

    public void mapInit(byte[] aMap, int offset, int length) {
        data = aMap;
        map_offset = offset;
        map_length = length;

    }

    /**
     * Set a tuple with the given tuple length and offset
     *
     * @param record a byte array contains the Map
     * @param offset the offset of the tuple ( =0 by default)
     * @param length the length of the tuple
     */
    public void MapSet(byte[] record, int offset, int length) {
        System.arraycopy(record, offset, data, 0, length);
        map_offset = 0;
        map_length = length;
    }


    /**
     * Makes a copy of the fldOffset array
     *
     * @return a copy of the fldOffset arrray
     */

    public short[] copyFldOffset() {
        short[] newFldOffset = new short[5];
        for (int i = 0; i <= 5; i++) {
            newFldOffset[i] = fldOffset[i];
        }

        return newFldOffset;
    }

    /**
     * get the length of a tuple, call this method if you did not
     * call setHdr () before
     *
     * @return length of this tuple in bytes
     */

    public int getLength() {
        return map_length;
    }

    /**
     * get the offset of a tuple
     *
     * @return offset of the tuple in byte array
     */

    public int getOffset() {
        return map_offset;
    }


    /**
     * return the data byte array
     *
     * @return data byte array
     */

    public byte[] returnMapByteArray() {
        return data;
    }

    /**
     * get the timestamp of the map
     *
     * @return timeStamp
     * @throws IOException
     */
    public int getTimeStamp() throws IOException {
        return Convert.getIntValue(fldOffset[2], data);
    }

    /**
     * get the row label of the map
     *
     * @return rowLabel
     * @throws IOException
     */
    public String getRowLabel() throws IOException {
        return Convert.getStrValue(fldOffset[0], data,
                fldOffset[1] - fldOffset[0]); //strlen+2
    }


    /**
     * get the column label of the map
     *
     * @return columnLabel
     * @throws IOException
     */
    public String getColumnLabel() throws IOException {
        return Convert.getStrValue(fldOffset[1], data,
                fldOffset[2] - fldOffset[1]); //strlen+2
    }

    /**
     * get the row label of the map
     *
     * @return rowLabel
     * @throws IOException
     */
    public String getValue() throws IOException {
        return Convert.getStrValue(fldOffset[3], data,
                fldOffset[4] - fldOffset[3]); //strlen+2
    }

    /**
     * @param val
     * @return
     * @throws IOException
     */
    public Map1 setTimeStamp(int val) throws IOException {
        Convert.setIntValue(val, fldOffset[2], data);
        return this;
    }

    public Map1 setRowLabel(String val) throws IOException {
        Convert.setStrValue(val, fldOffset[0], data);
        return this;
    }

    public Map1 setColumnLabel(String val) throws IOException {
        Convert.setStrValue(val, fldOffset[1], data);
        return this;
    }

    public Map1 setValue(String val) throws IOException {
        Convert.setStrValue(val, fldOffset[3], data);
        return this;
    }


    /**
     *
     * @param strSizes
     * @throws IOException
     * @throws InvalidTypeException
     * @throws InvalidTupleSizeException
     */
    public void setHdr(short strSizes[])
            throws IOException, InvalidTypeException, InvalidTupleSizeException {
        if ((numFlds + 2) * 2 > max_size)
            throw new InvalidTupleSizeException(null, "MAP: MAP_TOOBIG_ERROR");

        Convert.setShortValue(numFlds, map_offset, data);
        fldOffset = new short[numFlds + 1];
        int pos = map_offset + 2;  // start position for fldOffset[]

        //sizeof short =2  +2: array siaze = numFlds +1 (0 - numFilds) and
        //another 1 for fldCnt
        // Here in Map, as we are not storing field count, it can be numFlds + 1
        fldOffset[0] = (short) ((numFlds + 1) * 2 + map_offset);

        Convert.setShortValue(fldOffset[0], pos, data);
        pos += 2;
        short strCount = 0;
        short incr;
        int i;

        AttrType[] mapFieldTypes = new AttrType[4];
        mapFieldTypes[0] = new AttrType(AttrType.attrString);
        mapFieldTypes[1] = new AttrType(AttrType.attrString);
        mapFieldTypes[2] = new AttrType(AttrType.attrInteger);
        mapFieldTypes[3] = new AttrType(AttrType.attrString);

        for (i = 1; i < numFlds; i++) {
            switch (mapFieldTypes[i - 1].attrType) {

                case AttrType.attrInteger:
                    incr = 4;
                    break;

                case AttrType.attrString:
                    incr = (short) (strSizes[strCount] + 2);  //strlen in bytes = strlen +2
                    strCount++;
                    break;

                default:
                    throw new InvalidTypeException(null, "MAP: MAP_TYPE_ERROR");
            }
            fldOffset[i] = (short) (fldOffset[i - 1] + incr);
            Convert.setShortValue(fldOffset[i], pos, data);
            pos += 2;

        }
        switch (mapFieldTypes[numFlds - 1].attrType) {

            case AttrType.attrInteger:
                incr = 4;
                break;

            case AttrType.attrString:
                incr = (short) (strSizes[strCount] + 2);  //strlen in bytes = strlen +2
                break;

            default:
                throw new InvalidTypeException(null, "MAP: MAP_TYPE_ERROR");
        }

        fldOffset[numFlds] = (short) (fldOffset[i - 1] + incr);
        Convert.setShortValue(fldOffset[numFlds], pos, data);

        map_length = fldOffset[numFlds] - map_offset;

        if (map_length > max_size)
            throw new InvalidTupleSizeException(null, "MAP: MAP_TOOBIG_ERROR");
    }
}

