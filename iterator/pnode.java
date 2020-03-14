package iterator;

import bigt.Map;

public class pnode {
    /**
     * which run does this tuple belong
     */
    public int run_num;

    /**
     * the tuple reference
     */
    public Map map;

    /**
     * class constructor, sets <code>run_num</code> to 0 and <code>tuple</code>
     * to null.
     */
    public pnode() {
        run_num = 0;  // this may need to be changed
        map = null;
    }

    /**
     * class constructor, sets <code>run_num</code> and <code>tuple</code>.
     *
     * @param runNum the run number
     * @param m      the tuple
     */
    public pnode(int runNum, Map m) {
        run_num = runNum;
        map = m;
    }
}

