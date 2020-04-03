/* File Map.java */

package bigt;

import global.AttrType;
import global.Convert;
import global.GlobalConst;
import heap.FieldNumberOutOfBoundException;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;

import java.io.IOException;


public class Map implements GlobalConst {


    /**
     * Maximum size of any Map
     */
    public static final int max_size = MINIBASE_PAGESIZE;

    /**
     * a byte array to hold data
     */
    private byte[] data;

    /**
     * start position of this Map in data[]
     */
    private int map_offset;

    /**
     * length of this Map
     */
    private int map_length;

    /**
     * private field
     * Number of fields in this Map
     */
    private short fldCnt = 4;

    /**
     * private field
     * Array of offsets of the fields
     */

    private short[] fldOffset;

    /**
     * Class constructor
     * Creat a new Map with length = max_size,Map offset = 0.
     */

    public Map() {
        // Creat a new Map
        data = new byte[max_size];
        map_offset = 0;
        map_length = max_size;
    }

    /**
     * Constructor
     *
     * @param aMap a byte array which contains the Map
     * @param offset the offset of the Map in the byte array
     * @param length the length of the Map
     */

    public Map(byte[] aMap, int offset, int length) {
        data = aMap;
        map_offset = offset;
        map_length = length;
    }

    public void setOffsets(int offset){
        try {
            fldCnt = Convert.getShortValue(offset, data);
            fldOffset = new short[fldCnt+1];
            int start = 2;
            for(int i=0;i<fldCnt+1;i++){
                fldOffset[i] = Convert.getShortValue(offset + start, data);
                start += 2;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Constructor(used as Map copy)
     *
     * @param fromMap a byte array which contains the Map
     */
    public Map(Map fromMap) {
        data = fromMap.getMapByteArray();
        map_length = fromMap.getLength();
        map_offset = 0;
        fldCnt = fromMap.noOfFlds();
        fldOffset = fromMap.copyFldOffset();
    }

    /**
     * Class constructor
     * Creat a new Map with length = size,Map offset = 0.
     */

    public Map(int size) {
        // Creat a new Map
        data = new byte[size];
        map_offset = 0;
        map_length = size;
    }

    /**
     * Copy a Map to the current Map position
     * you must make sure the Map lengths must be equal
     *
     * @param fromMap the Map being copied
     */
    public void mapCopy(Map fromMap) {
        byte[] temparray = fromMap.getMapByteArray();
        System.arraycopy(temparray, 0, data, map_offset, map_length);
        fldCnt = fromMap.noOfFlds(); 
        fldOffset = fromMap.copyFldOffset(); 
    }

    /**
     * This is used when you don't want to use the constructor
     *
     * @param aMap a byte array which contains the Map
     * @param offset the offset of the Map in the byte array
     * @param length the length of the Map
     */

    public void mapInit(byte[] aMap, int offset, int length) {
        data = aMap;
        map_offset = offset;
        map_length = length;
    }

    /**
     * Set a Map with the given Map length and offset
     *
     * @param    record    a byte array contains the Map
     * @param    offset the offset of the Map ( =0 by default)
     * @param    length    the length of the Map
     */
    public void mapSet(byte[] record, int offset, int length) {
        System.arraycopy(record, offset, data, 0, length);
        map_offset = 0;
        map_length = length;
    }

    /**
     * get the length of a Map, call this method if you did not
     * call setHdr () before
     *
     * @return length of this Map in bytes
     */
    public int getLength() {
        return map_length;
    }

    /**
     * get the length of a Map, call this method if you did
     * call setHdr () before
     *
     * @return size of this Map in bytes
     */
    public short size() {
        return ((short) (fldOffset[fldCnt] - map_offset));
    }

    /**
     * get the offset of a Map
     *
     * @return offset of the Map in byte array
     */
    public int getOffset() {
        return map_offset;
    }

    /**
     * Copy the Map byte array out
     *
     * @return byte[], a byte array contains the Map
     * the length of byte[] = length of the Map
     */

    public byte[] getMapByteArray() {
        byte[] mapCopy = new byte[map_length];
        System.arraycopy(data, map_offset, mapCopy, 0, map_length);
        return mapCopy;
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
     * Convert this field into integer
     *
     * @throws IOException                    I/O errors
     * @throws FieldNumberOutOfBoundException Map field number out of bound
     * @param    fldNo    the field number
     * @return the converted integer if success
     */

    public int getIntFld(int fldNo)
            throws IOException, FieldNumberOutOfBoundException {
        int val;
        if ((fldNo > 0) && (fldNo <= fldCnt)) {
            val = Convert.getIntValue(fldOffset[fldNo - 1], data);
            return val;
        } else
            throw new FieldNumberOutOfBoundException(null, "Map:Map_FLDNO_OUT_OF_BOUND");
    }

    /**
     * Convert this field in to float
     *
     * @param fldNo the field number
     * @return the converted float number  if success
     * @throws IOException                    I/O errors
     * @throws FieldNumberOutOfBoundException Map field number out of bound
     */

    public float getFloFld(int fldNo)
            throws IOException, FieldNumberOutOfBoundException {
        float val;
        if ((fldNo > 0) && (fldNo <= fldCnt)) {
            val = Convert.getFloValue(fldOffset[fldNo - 1], data);
            return val;
        } else
            throw new FieldNumberOutOfBoundException(null, "Map:Map_FLDNO_OUT_OF_BOUND");
    }


    /**
     * Convert this field into String
     *
     * @param fldNo the field number
     * @return the converted string if success
     * @throws IOException                    I/O errors
     * @throws FieldNumberOutOfBoundException Map field number out of bound
     */

    public String getStrFld(int fldNo)
            throws IOException, FieldNumberOutOfBoundException {
        String val;
        if ((fldNo > 0) && (fldNo <= fldCnt)) {
            val = Convert.getStrValue(fldOffset[fldNo - 1], data,
                    fldOffset[fldNo] - fldOffset[fldNo - 1]); //strlen+2
            return val;
        } else
            throw new FieldNumberOutOfBoundException(null, "Map:Map_FLDNO_OUT_OF_BOUND");
    }

    /**
     * Convert this field into a character
     *
     * @param fldNo the field number
     * @return the character if success
     * @throws IOException                    I/O errors
     * @throws FieldNumberOutOfBoundException Map field number out of bound
     */

    public char getCharFld(int fldNo)
            throws IOException, FieldNumberOutOfBoundException {
        char val;
        if ((fldNo > 0) && (fldNo <= fldCnt)) {
            val = Convert.getCharValue(fldOffset[fldNo - 1], data);
            return val;
        } else
            throw new FieldNumberOutOfBoundException(null, "Map:Map_FLDNO_OUT_OF_BOUND");

    }

    /**
     * Set this field to integer value
     *
     * @throws IOException                    I/O errors
     * @throws FieldNumberOutOfBoundException Map field number out of bound
     * @param    fldNo    the field number
     * @param    val    the integer value
     */

    public Map setIntFld(int fldNo, int val)
            throws IOException, FieldNumberOutOfBoundException {
        if ((fldNo > 0) && (fldNo <= fldCnt)) {
            Convert.setIntValue(val, fldOffset[fldNo - 1], data);
            return this;
        } else
            throw new FieldNumberOutOfBoundException(null, "Map:Map_FLDNO_OUT_OF_BOUND");
    }

    /**
     * Set this field to float value
     *
     * @param fldNo the field number
     * @param val   the float value
     * @throws IOException                    I/O errors
     * @throws FieldNumberOutOfBoundException Map field number out of bound
     */

    public Map setFloFld(int fldNo, float val)
            throws IOException, FieldNumberOutOfBoundException {
        if ((fldNo > 0) && (fldNo <= fldCnt)) {
            Convert.setFloValue(val, fldOffset[fldNo - 1], data);
            return this;
        } else
            throw new FieldNumberOutOfBoundException(null, "Map:Map_FLDNO_OUT_OF_BOUND");

    }

    /**
     * Set this field to String value
     *
     * @param fldNo the field number
     * @param val   the string value
     * @throws IOException                    I/O errors
     * @throws FieldNumberOutOfBoundException Map field number out of bound
     */

    public Map setStrFld(int fldNo, String val)
            throws IOException, FieldNumberOutOfBoundException {
        if ((fldNo > 0) && (fldNo <= fldCnt)) {
            Convert.setStrValue(val, fldOffset[fldNo - 1], data);
            return this;
        } else
            throw new FieldNumberOutOfBoundException(null, "Map:Map_FLDNO_OUT_OF_BOUND");
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
    public Map setTimeStamp(int val) throws IOException {
        Convert.setIntValue(val, fldOffset[2], data);
        return this;
    }

    public Map setRowLabel(String val) throws IOException {
        Convert.setStrValue(val, fldOffset[0], data);
        return this;
    }

    public Map setColumnLabel(String val) throws IOException {
        Convert.setStrValue(val, fldOffset[1], data);
        return this;
    }

    public Map setValue(String val) throws IOException {
        Convert.setStrValue(val, fldOffset[3], data);
        return this;
    }
    
    /**
     * setHdr will set the header of this Map.
     *
     * @throws IOException               I/O errors
     * @throws InvalidTypeException      Invalid tupe type
     * @throws InvalidTupleSizeException Map size too big
     * @param    numFlds     number of fields
     * @param    types    contains the types that will be in this Map
     * @param    strSizes contains the sizes of the string
     */

    public void setHdr(short numFlds, AttrType types[], short strSizes[])
            throws IOException, InvalidTypeException, InvalidTupleSizeException {
        if ((numFlds + 2) * 2 > max_size)
            throw new InvalidTupleSizeException(null, "Map: Map_TOOBIG_ERROR");

        fldCnt = numFlds;
        Convert.setShortValue(numFlds, map_offset, data);
        fldOffset = new short[numFlds + 1];
        int pos = map_offset + 2;  // start position for fldOffset[]

        //sizeof short =2  +2: array siaze = numFlds +1 (0 - numFilds) and
        //another 1 for fldCnt
        fldOffset[0] = (short) ((numFlds + 2) * 2 + map_offset);

        Convert.setShortValue(fldOffset[0], pos, data);
        pos += 2;
        short strCount = 0;
        short incr;
        int i;

        for (i = 1; i < numFlds; i++) {
            switch (types[i - 1].attrType) {

                case AttrType.attrInteger:
                    incr = 4;
                    break;

                case AttrType.attrReal:
                    incr = 4;
                    break;

                case AttrType.attrString:
                    incr = (short) (strSizes[strCount] + 2);  //strlen in bytes = strlen +2
                    strCount++;
                    break;

                default:
                    throw new InvalidTypeException(null, "Map: Map_TYPE_ERROR");
            }
            fldOffset[i] = (short) (fldOffset[i - 1] + incr);
            Convert.setShortValue(fldOffset[i], pos, data);
            pos += 2;

        }
        switch (types[numFlds - 1].attrType) {

            case AttrType.attrInteger:
                incr = 4;
                break;

            case AttrType.attrReal:
                incr = 4;
                break;

            case AttrType.attrString:
                incr = (short) (strSizes[strCount] + 2);  //strlen in bytes = strlen +2
                break;

            default:
                throw new InvalidTypeException(null, "Map: Map_TYPE_ERROR");
        }

        fldOffset[numFlds] = (short) (fldOffset[i - 1] + incr);
        Convert.setShortValue(fldOffset[numFlds], pos, data);

        map_length = fldOffset[numFlds] - map_offset;

        if (map_length > max_size)
            throw new InvalidTupleSizeException(null, "Map: Map_TOOBIG_ERROR");
    }


    /**
     * Returns number of fields in this Map
     *
     * @return the number of fields in this Map
     */

    public short noOfFlds() {
        return fldCnt;
    }

    /**
     * Makes a copy of the fldOffset array
     *
     * @return a copy of the fldOffset arrray
     */

    public short[] copyFldOffset() {
        short[] newFldOffset = new short[fldCnt + 1];
        for (int i = 0; i <= fldCnt; i++) {
            newFldOffset[i] = fldOffset[i];
        }

        return newFldOffset;
    }
    /**
     * Print out the Map
     *
     *
     */
    public void print(){
        try {
            System.out.println(this.getRowLabel() +
                    String.format("%"+ (Minibase.getInstance().getMaxRowKeyLength() - this.getRowLabel().length() + 2)+"s", "") +
                    this.getColumnLabel() +
                    String.format("%"+ (Minibase.getInstance().getMaxColumnKeyLength() - this.getColumnLabel().length() + 2)+"s", "")
                    + this.getValue().replaceFirst("^0*", "")+
                    String.format("%"+ (Minibase.getInstance().getMaxValueLength() -
                            this.getValue().replaceFirst("^0*", "").length() + 2)+"s", "")
                    +
                    this.getTimeStamp());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    /**
     * Print out the Map
     *
     * @param type the types in the Map
     * @Exception IOException I/O exception
     */
    public void print(AttrType type[])
            throws IOException {
        int i, val;
        float fval;
        String sval;

        System.out.print("[");
        for (i = 0; i < fldCnt - 1; i++) {
            switch (type[i].attrType) {

                case AttrType.attrInteger:
                    val = Convert.getIntValue(fldOffset[i], data);
                    System.out.print(val);
                    break;

                case AttrType.attrReal:
                    fval = Convert.getFloValue(fldOffset[i], data);
                    System.out.print(fval);
                    break;

                case AttrType.attrString:
                    sval = Convert.getStrValue(fldOffset[i], data, fldOffset[i + 1] - fldOffset[i]);
                    System.out.print(sval);
                    break;

                case AttrType.attrNull:
                case AttrType.attrSymbol:
                    break;
            }
            System.out.print(", ");
        }

        switch (type[fldCnt - 1].attrType) {

            case AttrType.attrInteger:
                val = Convert.getIntValue(fldOffset[i], data);
                System.out.print(val);
                break;

            case AttrType.attrReal:
                fval = Convert.getFloValue(fldOffset[i], data);
                System.out.print(fval);
                break;

            case AttrType.attrString:
                sval = Convert.getStrValue(fldOffset[i], data, fldOffset[i + 1] - fldOffset[i]);
                System.out.print(sval);
                break;

            case AttrType.attrNull:
            case AttrType.attrSymbol:
                break;
        }
        System.out.println("]");

    }

    /**
     * private method
     * Padding must be used when storing different types.
     *
     * @param type the type of Map
     * @return short typle
     * @param    offset
     */

    private short pad(short offset, AttrType type) {
        return 0;
    }
}

