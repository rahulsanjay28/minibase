package iterator;

import bigt.Map;
import global.AttrType;
import heap.FieldNumberOutOfBoundException;

import java.io.IOException;

/**
 * Jtuple has the appropriate types.
 * Jtuple already has its setHdr called to setup its vital stas.
 */

public class Projection
{
    /**
     *Map map1 will be projected
     *the result will be stored in Tuple Jtuple
     *@param t1 The Tuple will be projected
     *@param type1[] The array used to store the each attribute type
     *@param Jtuple the returned Tuple
     *@param perm_mat[] shows what input fields go where in the output tuple
     *@param nOutFlds number of outer relation field
     *@exception UnknowAttrType attrbute type doesn't match
     *@exception WrongPermat wrong FldSpec argument
     *@exception FieldNumberOutOfBoundException field number exceeds limit
     *@exception IOException some I/O fault
     */

    public static void Project(Map  map1, AttrType type1[],
                               Map Jmap, FldSpec perm_mat[]
    )
            throws UnknowAttrType,
            WrongPermat,
            FieldNumberOutOfBoundException,
            IOException
    {


        for (int i = 0; i < 4; i++)
        {
            switch (perm_mat[i].relation.key)
            {
                case RelSpec.outer:      // Field of outer (t1)
                    switch (type1[perm_mat[i].offset-1].attrType)
                    {
                        case AttrType.attrInteger:
                            Jmap.setIntFld(i+1, map1.getIntFld(perm_mat[i].offset));
                            break;
                        case AttrType.attrString:
                            Jmap.setStrFld(i+1, map1.getStrFld(perm_mat[i].offset));
                            break;
                        default:

                            throw new UnknowAttrType("Don't know how to handle attrSymbol, attrNull");

                    }
                    break;

                default:

                    throw new WrongPermat("something is wrong in perm_mat");

            }
        }
        return;
    }

}

