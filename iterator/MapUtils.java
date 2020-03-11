package iterator;


import heap.*;
import global.*;
import java.io.*;
import bigt.*;
import java.lang.*;

/**
 *some useful method when processing Map
 */
public class MapUtils
{

    /**
     * This function compares a map with another map in respective field, and
     *  returns:
     *
     *    0        if the two are equal,
     *    1        if the map is greater,
     *   -1        if the map is smaller,
     *
     *@param    fldType   the type of the field being compared.--ask
     *@param    m1        one Map.
     *@param    m2        another Map.
     *@param    map_fld_no the field numbers in the maps to be compared.
     *@exception UnknowAttrType don't know the attribute type.-----------------ask
     *@exception IOException some I/O fault-----------------ask
     *@exception mapUtilsException exception from this class-----------ask
     *@return   0        if the two are equal,
     *          1        if the map is greater,
     *         -1        if the map is smaller,
     */
    public static int CompareMapWithMap(Map m1, Map m2, int map_fld_no)
            throws IOException,
            UnknowAttrType,
            TupleUtilsException
    {
        String m1_s,  m2_s;
        int m1_i,m2_i;
        switch (map_fld_no)
        {
            case 1:            // Compare two strings.
            case 2:
            case 4:
                try {
                    if(map_fld_no==1){
                        m1_s=m1.getRowLabel();
                        m2_s=m2.getRowLabel();
                    }
                    else if(map_fld_no==2){
                        m1_s=m1.getColumnLabel();
                        m2_s=m2.getColumnLabel();
                    }
                    else if(map_fld_no==4){
                        m1_s=m1.getValue();
                        m2_s=m2.getValue();
                    }
                    }catch (FieldNumberOutOfBoundException e){
                    throw new TupleUtilsException(e, "FieldNumberOutOfBoundException is caught by mapUtils.java");
                }
                if(m1_s.compareTo( m2_s)>0)return 1;
                else if (m1_s.compareTo( m2_s)<0)return -1;
                     else return 0;

            case 3:                // Compare two integers
                try {
                    m1_i = m1.getTimeStamp();
                    m2_i = m2.getTimeStamp();
                }catch (FieldNumberOutOfBoundException e){
                    throw new TupleUtilsException(e, "FieldNumberOutOfBoundException is caught by mapUtils.java");
                }
                if (m1_i == m2_i) return  0;
                else if (m1_i <  m2_i) return -1;
                else if (m1_i >  m2_i) return  1;

        }
    }



    /**
     * This function  compares  map1 with another map2 whose
     * field number is same as the map1
     *
     *@param    fldType   the type of the field being compared.
     *@param    t1        one map
     *@param    value     another Map.
     *@param    t1_fld_no the field numbers in the maps to be compared.
     *@return   0        if the two are equal,
     *          1        if the map is greater,
     *         -1        if the map is smaller,
     *@exception UnknowAttrType don't know the attribute type
     *@exception IOException some I/O fault
     *@exception mapUtilsException exception from this class
     */
    public static int CompareMapWithValue(AttrType fldType,
                                            Map  m1, int m1_fld_no,
                                            Map  m2)
            throws IOException,
            UnknowAttrType,
            TupleUtilsException
    {
        return CompareMapWithMap(m1,m2,m1_fld_no);
    }

    /**
     *This function Compares two map inn all fields
     * @param t1 the first map
     * @param t2 the secocnd map
     * @param type[] the field types
     * @param len the field numbers
     * @return  0        if the two are not equal,
     *          1        if the two are equal,
     *@exception UnknowAttrType don't know the attribute type
     *@exception IOException some I/O fault
     *@exception mapUtilsException exception from this class
     */

    public static boolean Equal(Map m1, Map m2)
            throws IOException,UnknowAttrType,TupleUtilsException
    {
        int i;

        for (i = 1; i <= 4; i++)
            if (CompareMapWithMap( m1, m2, i) != 0)
                return false;
        return true;
    }

    /**
     *get the string specified by the field number
     *@param map the map
     *@param fidno the field number
     *@return the content of the field number
     *@exception IOException some I/O fault
     *@exception mapUtilsException exception from this class
     */
    public static String Value(Map m1, int fldno)
            throws IOException,
            TupleUtilsException
    {
        String temp;
        try{
            temp = m1.getStrFld(fldno);
        }catch (FieldNumberOutOfBoundException e){
            throw new TupleUtilsException(e, "FieldNumberOutOfBoundException is caught by mapUtils.java");
        }
        return temp;
    }
    /**
     *set up a map in specified field from a map
     *@param value the map to be set
     *@param map the given map
     *@param fld_no the field number
     *@param fldType the map attr type
     *@exception UnknowAttrType don't know the attribute type
     *@exception IOException some I/O fault
     *@exception mapUtilsException exception from this class
     */
    public static void SetValue(Map m1, Map m2, int map_fld_no,AttrType fldType)
            throws IOException,
            UnknowAttrType,
            TupleUtilsException {
        String m1_s, m2_s;
        int m1_i, m2_i;
        switch (map_fld_no) {
            case 1:
            case 2:
            case 4:
                try {
                    if (map_fld_no == 1) {
                        m1.setRowLabel(m2.getRowLabel());
                    } else if (map_fld_no == 2) {
                        m1.setColumnLabel(m2.getColumnLabel());
                    } else if (map_fld_no == 4) {
                        m1.setValue(m2.getValue());
                    }
                } catch (FieldNumberOutOfBoundException e) {
                    throw new TupleUtilsException(e, "FieldNumberOutOfBoundException is caught by mapUtils.java");
                }
            case 3:
                m1.setValue(m2.getValue());
        }
    }
}