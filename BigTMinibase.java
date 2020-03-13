import java.util.Scanner;

public class BigTMinibase {
    public static void main(String[] args) throws Exception{
        int option = -1;
        String args1;
        Scanner sc = new Scanner(System.in);
        do {
            System.out.println("Press 1 for BatchInsert\nPress 2 for Query\nPress any other key to exit\n");
            option = sc.nextInt();
            if(option == 1){
                System.out.println("Enter DATAFILENAME TYPE BIGTABLENAME NUMBUF");
                args1 = sc.nextLine();
                args1 = sc.nextLine();
                String[] args2 = args1.split("\\s");
                BatchInsert batchInsert = new BatchInsert();
                batchInsert.execute(args2[0], args2[1], args2[2], args2[3]);
            }else if(option == 2){
                System.out.println("Enter BIGTABLENAME TYPE ORDERTYPE ROWFILTER COLUMNFILTER VALUEFILTER NUMBUF");
                args1 = sc.nextLine();
                args1 = sc.nextLine();

                //TODO: Sanitize input for cases like [50, SPACE 56]. Split will not work correctly if there's space between range
                String[] args2 = args1.split(" ");
                Query query = new Query();
                query.execute(args2[0], args2[1], args2[2], args2[3], args2[4], args2[5], args2[6]);

            }
        }while(option == 1 || option == 2);
    }

    //don't delete this method, for future reference
    private void delete(){
//        String dbpath = "/tmp/" + bigTableName + type + ".bigtable-db";
//        File DBfile = new File(dbpath);
//        System.out.println(DBfile.exists());
//        DBfile.delete();
    }
}
