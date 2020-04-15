package iterator;

import bigt.Map;
import global.AttrOperator;
import global.AttrType;
import heap.FieldNumberOutOfBoundException;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;

import java.io.IOException;

public class PredEval {
    /**
     * predicate evaluate, according to the condition ConExpr, judge if
     * the two tuple can join. if so, return true, otherwise false
     *
     * @param p   single select condition array
     * @param m1  compared tuple1
     * @param m2  compared tuple2
     * @param in1 the attribute type corespond to the t1
     * @param in2 the attribute type corespond to the t2
     * @return true or false
     * @throws IOException                    some I/O error
     * @throws UnknowAttrType                 don't know the attribute type
     * @throws InvalidTupleSizeException      size of tuple not valid
     * @throws InvalidTypeException           type of tuple not valid
     * @throws FieldNumberOutOfBoundException field number exceeds limit
     * @throws PredEvalException              exception from this method
     */
    public static boolean Eval(CondExpr p[], Map m1, Map m2, AttrType in1[],
                               AttrType in2[])
            throws IOException,
            UnknowAttrType,
            InvalidTupleSizeException,
            InvalidTypeException,
            FieldNumberOutOfBoundException,
            PredEvalException {
        CondExpr temp_ptr;
        int i = 0;
        Map map1 = null, map2 = null;
        int fld1, fld2;
        Map value = new Map();
        short[] str_size = new short[1];
        AttrType[] val_type = new AttrType[1];

        AttrType comparison_type = new AttrType(AttrType.attrInteger);
        int comp_res;
        boolean op_res = false, row_res = false, col_res = true;

        if (p == null) {
            return true;
        }

        while (p[i] != null) {
            temp_ptr = p[i];
            while (temp_ptr != null) {
                val_type[0] = new AttrType(temp_ptr.type1.attrType);
                fld1 = 1;
                switch (temp_ptr.type1.attrType) {
                    case AttrType.attrInteger:
                        value.setHdr((short) 1, val_type, null);
                        value.setIntFld(1, temp_ptr.operand1.integer);
                        map1 = value;
                        comparison_type.attrType = AttrType.attrInteger;
                        break;
                    case AttrType.attrReal:
                        value.setHdr((short) 1, val_type, null);
                        value.setFloFld(1, temp_ptr.operand1.real);
                        map1 = value;
                        comparison_type.attrType = AttrType.attrReal;
                        break;
                    case AttrType.attrString:
                        str_size[0] = (short) (temp_ptr.operand1.string.length() + 1);
                        value.setHdr((short) 1, val_type, str_size);
                        value.setStrFld(1, temp_ptr.operand1.string);
                        map1 = value;
                        comparison_type.attrType = AttrType.attrString;
                        break;
                    case AttrType.attrSymbol:
                        fld1 = temp_ptr.operand1.symbol.offset;
                        if (temp_ptr.operand1.symbol.relation.key == RelSpec.outer) {
                            map1 = m1;
                            comparison_type.attrType = in1[fld1 - 1].attrType;
                        } else {
                            map1 = m2;
                            comparison_type.attrType = in2[fld1 - 1].attrType;
                        }
                        break;
                    default:
                        break;
                }

                // Setup second argument for comparison.
                val_type[0] = new AttrType(temp_ptr.type2.attrType);
                fld2 = 1;
                switch (temp_ptr.type2.attrType) {
                    case AttrType.attrInteger:
                        value.setHdr((short) 1, val_type, null);
                        value.setIntFld(1, temp_ptr.operand2.integer);
                        map2 = value;
                        break;
                    case AttrType.attrReal:
                        value.setHdr((short) 1, val_type, null);
                        value.setFloFld(1, temp_ptr.operand2.real);
                        map2 = value;
                        break;
                    case AttrType.attrString:
                        str_size[0] = (short) (temp_ptr.operand2.string.length() + 1);
                        value.setHdr((short) 1, val_type, str_size);
                        value.setStrFld(1, temp_ptr.operand2.string);
                        map2 = value;
                        break;
                    case AttrType.attrSymbol:
                        fld2 = temp_ptr.operand2.symbol.offset;
                        if (temp_ptr.operand2.symbol.relation.key == RelSpec.outer)
                            map2 = m1;
                        else
                            map2 = m2;
                        break;
                    default:
                        break;
                }


                // Got the arguments, now perform a comparison.
                try {
                    comp_res = MapUtils.CompareMapWithMap(comparison_type, map1, fld1, map2, fld2);
                } catch (TupleUtilsException e) {
                    throw new PredEvalException(e, "TupleUtilsException is caught by PredEval.java");
                }
                op_res = false;

                switch (temp_ptr.op.attrOperator) {
                    case AttrOperator.aopEQ:
                        if (comp_res == 0) op_res = true;
                        break;
                    case AttrOperator.aopLT:
                        if (comp_res < 0) op_res = true;
                        break;
                    case AttrOperator.aopGT:
                        if (comp_res > 0) op_res = true;
                        break;
                    case AttrOperator.aopNE:
                        if (comp_res != 0) op_res = true;
                        break;
                    case AttrOperator.aopLE:
                        if (comp_res <= 0) op_res = true;
                        break;
                    case AttrOperator.aopGE:
                        if (comp_res >= 0) op_res = true;
                        break;
                    case AttrOperator.aopNOT:
                        if (comp_res != 0) op_res = true;
                        break;
                    default:
                        break;
                }

                row_res = row_res || op_res;
                if (row_res == true)
                    break;                        // OR predicates satisfied.
                temp_ptr = temp_ptr.next;
            }
            i++;

            col_res = col_res && row_res;
            if (col_res == false) {

                return false;
            }
            row_res = false;                        // Starting next row.
        }


        return true;

    }
}

