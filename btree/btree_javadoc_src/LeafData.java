package btree;
import global.*;

/**  IndexData: It extends the DataClass.
 *   It defines the data "rid" for leaf node in B++ tree.
 */
public class LeafData extends DataClass {
  private MID myMID;

  public String toString() {
     String s;
     s="[ "+ (new Integer(myMID.pageNo.pid)).toString() +" "
              + (new Integer(myMID.slotNo)).toString() + " ]";
     return s;
  }

  /** Class constructor
   *  @param    mid  the data rid
   */
  LeafData(MID mid) {
      myMID = new MID(mid.pageNo, mid.slotNo);};

  /** get a copy of the rid
  *  @return the reference of the copy 
  */
  public MID getData() {return new MID(myMID.pageNo, myMID.slotNo);};

  /** set the mid
   */ 
  public void setData(MID mid) { myMID = new MID(mid.pageNo, mid.slotNo);};
}   
