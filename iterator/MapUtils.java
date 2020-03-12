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
    /**
     *set up the Jtuple's attrtype, string size,field number for using join
     *@param Jtuple  reference to an actual tuple  - no memory has been malloced
     *@param res_attrs  attributes type of result tuple
     *@param in1  array of the attributes of the tuple (ok)
     *@param len_in1  num of attributes of in1
     *@param in2  array of the attributes of the tuple (ok)
     *@param len_in2  num of attributes of in2
     *@param t1_str_sizes shows the length of the string fields in S
     *@param t2_str_sizes shows the length of the string fields in R
     *@param proj_list shows what input fields go where in the output tuple
     *@param nOutFlds number of outer relation fileds
     *@exception IOException some I/O fault
     *@exception TupleUtilsException exception from this class
     */
    public static short[] setup_op_tuple(Map Jmap, AttrType[] res_attrs,
                                         AttrType in1[], int len_in1, AttrType in2[],
                                         int len_in2, short t1_str_sizes[],
                                         short t2_str_sizes[],
                                         FldSpec proj_list[], int nOutFlds)
            throws IOException,
            TupleUtilsException
    {
        short [] sizesT1 = new short [len_in1];
        short [] sizesT2 = new short [len_in2];
        int i, count = 0;

        for (i = 0; i < len_in1; i++)
            if (in1[i].attrType == AttrType.attrString)
                sizesT1[i] = t1_str_sizes[count++];

        for (count = 0, i = 0; i < len_in2; i++)
            if (in2[i].attrType == AttrType.attrString)
                sizesT2[i] = t2_str_sizes[count++];

        int n_strs = 0;
        for (i = 0; i < nOutFlds; i++)
        {
            if (proj_list[i].relation.key == RelSpec.outer)
                res_attrs[i] = new AttrType(in1[proj_list[i].offset-1].attrType);
            else if (proj_list[i].relation.key == RelSpec.innerRel)
                res_attrs[i] = new AttrType(in2[proj_list[i].offset-1].attrType);
        }

        // Now construct the res_str_sizes array.
        for (i = 0; i < nOutFlds; i++)
        {
            if (proj_list[i].relation.key == RelSpec.outer && in1[proj_list[i].offset-1].attrType == AttrType.attrString)
                n_strs++;
            else if (proj_list[i].relation.key == RelSpec.innerRel && in2[proj_list[i].offset-1].attrType == AttrType.attrString)
                n_strs++;
        }

        short[] res_str_sizes = new short [n_strs];
        count         = 0;
        for (i = 0; i < nOutFlds; i++)
        {
            if (proj_list[i].relation.key == RelSpec.outer && in1[proj_list[i].offset-1].attrType ==AttrType.attrString)
                res_str_sizes[count++] = sizesT1[proj_list[i].offset-1];
            else if (proj_list[i].relation.key == RelSpec.innerRel && in2[proj_list[i].offset-1].attrType ==AttrType.attrString)
                res_str_sizes[count++] = sizesT2[proj_list[i].offset-1];
        }
        try {
            Jmap.setHdr((short)nOutFlds, res_attrs, res_str_sizes);
        }catch (Exception e){
            throw new TupleUtilsException(e,"setHdr() failed");
        }
        return res_str_sizes;
    }


    /**
     *set up the Jtuple's attrtype, string size,field number for using project
     *@param Jtuple  reference to an actual tuple  - no memory has been malloced
     *@param res_attrs  attributes type of result tuple
     *@param in1  array of the attributes of the tuple (ok)
     *@param len_in1  num of attributes of in1
     *@param t1_str_sizes shows the length of the string fields in S
     *@param proj_list shows what input fields go where in the output tuple
     *@param nOutFlds number of outer relation fileds
     *@exception IOException some I/O fault
     *@exception TupleUtilsException exception from this class
     *@exception InvalidRelation invalid relation
     */

    public static short[] setup_op_tuple(Map Jmap, AttrType res_attrs[],
                                         AttrType in1[], int len_in1,
                                         short t1_str_sizes[],
                                         FldSpec proj_list[], int nOutFlds)
            throws IOException,
            TupleUtilsException,
            InvalidRelation
    {
        short [] sizesT1 = new short [len_in1];
        int i, count = 0;

        for (i = 0; i < len_in1; i++)
            if (in1[i].attrType == AttrType.attrString)
                sizesT1[i] = t1_str_sizes[count++];

        int n_strs = 0;
        for (i = 0; i < nOutFlds; i++)
        {
            if (proj_list[i].relation.key == RelSpec.outer)
                res_attrs[i] = new AttrType(in1[proj_list[i].offset-1].attrType);

            else throw new InvalidRelation("Invalid relation -innerRel");
        }

        // Now construct the res_str_sizes array.
        for (i = 0; i < nOutFlds; i++)
        {
            if (proj_list[i].relation.key == RelSpec.outer
                    && in1[proj_list[i].offset-1].attrType == AttrType.attrString)
                n_strs++;
        }

        short[] res_str_sizes = new short [n_strs];
        count         = 0;
        for (i = 0; i < nOutFlds; i++) {
            if (proj_list[i].relation.key ==RelSpec.outer
                    && in1[proj_list[i].offset-1].attrType ==AttrType.attrString)
                res_str_sizes[count++] = sizesT1[proj_list[i].offset-1];
        }

        try {
            Jmap.setHdr((short)nOutFlds, res_attrs, res_str_sizes);
        }catch (Exception e){
            throw new TupleUtilsException(e,"setHdr() failed");
        }
        return res_str_sizes;
    }
}