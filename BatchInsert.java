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
        System.out.println("BigTable Name: " + bigTableName);
    }
}
