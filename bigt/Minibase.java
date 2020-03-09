package bigt;

import global.SystemDefs;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;

import java.io.IOException;

public class Minibase {

    private static Minibase mInstance;
    private int NUM_BUF;
    private SystemDefs systemDefs;
    private String dbpath;
    private BigT bigT;

    private Minibase(){

    }

    public static Minibase getInstance(){
        if(mInstance == null){
            mInstance = new Minibase();
        }
        return mInstance;
    }

    public BigT getBigTable(){
        return bigT;
    }

    public void init(String name, int type, int numBuf){
        this.NUM_BUF = numBuf;
        dbpath = "/tmp/"+ name + type + ".bigtable-db";
        systemDefs = new SystemDefs(dbpath, 3000, numBuf, "Clock");
        try {
            bigT = new BigT(name, type);
        } catch (HFException e) {
            e.printStackTrace();
        } catch (HFBufMgrException e) {
            e.printStackTrace();
        } catch (HFDiskMgrException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
