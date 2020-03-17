import java.io.File;

public class Temp {
    public static void main(String[] args) {
        String name = "abc";
        String type = "2";
        String dbname = "/tmp/" + name + type + ".bigtable-db";
        File file = new File(dbname);
        if (file.exists()) {
            file.delete();
            System.out.println("dbfile exists, deleting");
        } else {
            System.out.println("dbfile does not exist");
        }
    }


}
