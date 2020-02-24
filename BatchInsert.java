import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

/**
 * compile this file using the command "javac BatchInsert.java"
 * Then run using "java BatchInsert datafilename type bigtablename"
 */

public class BatchInsert {

    public static void main(String[] args) {
        batchInsert(args[0], args[1], args[2]);
    }

    /**
     * Inserting records into the big table
     * @param dataFileName
     * @param type
     * @param bigTableName
     */
    private static void batchInsert(String dataFileName, String type, String bigTableName){
        //Reading data from the csv file
        String line = "";
        try {
            BufferedReader br = new BufferedReader(new FileReader(dataFileName + ".csv"));
            while((line = br.readLine()) != null){
                String[] fields = line.split(",");
                insertMap(fields[0], fields[1], Integer.parseInt(fields[2]), fields[3]);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * This method will be called for each row in the data file
     * @param rowKey
     * @param columnKey
     * @param timestamp
     * @param value
     */
    private static void insertMap(String rowKey, String columnKey, int timestamp, String value){
        System.out.println(rowKey + " " + columnKey + " " + timestamp + " " + value);
    }
}
