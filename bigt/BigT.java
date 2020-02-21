package bigt;

import global.MID;


public class BigT {

    private String name;
    private int type;

    // Defining different cluster strategy type
    public BigT(String name, int type) {


    }

    // Delete the bigtable from the database
    public void deleteBigt() {

    }

    // Return number of maps in the bigtable
    public int getMapCnt() {
        return 0;
    }

    // Return number of distinct row labels in the bigtable
    public int getRowCnt() {
        return 0;
    }

    // Return number of distinct column labels in the bigtable
    public int getColumnCnt() {
        return 0;
    }

    // Insert map into the big table, return its Mid
    public MID insertMap(byte[] mapPtr) {
        return null;
    }

    // Initialize a stream of maps
    public Stream openStream(int orderType, String rowFilter, String columnFilter, String valueFilter) {
        return null;
    }
}
