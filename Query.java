/**
 * Compile this class using the command "javac Query.java"
 * Then run "java Query bigtablename type ordertype rowfilter columnfilter valuefilter numbuf"
 */
public class Query {
    public static void main(String[] args) {
        query(args[0], args[1], args[2], args[3], args[4], args[5], args[6]);
    }

    /**
     * querying the big table
     * @param bigTableName
     * @param type
     * @param orderType
     * @param rowFilter
     * @param columnFilter
     * @param valueFilter
     * @param numBuf
     */
    private static void query(String bigTableName, String type, String orderType, String rowFilter, String columnFilter,
                              String valueFilter, String numBuf) {
        System.out.println("Executing query");
    }
}
