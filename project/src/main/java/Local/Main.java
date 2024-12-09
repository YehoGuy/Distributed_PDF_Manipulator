package Local;

public class Main {

    public static AwsService aws = AwsService.getInstance();

    public static void main(String[] args) {
        
        // handle args from user
        //if (args.length < 2) {
        //    System.err.println("Error - not enough arguments provided. Usage: java Main <file_path> <n> [terminate]");
        //    return;
        //}
        //String filePath = args[0];
        String filePath  = "src/main/java/Local/files/input-sample-1-cop.txt";
        String filename = "instructions.txt";
        //int n = Integer.parseInt(args[1]);
        //boolean terminate = args.length == 3 && args[2].equalsIgnoreCase("terminate");
        try {
            aws.downloadFileFromS3(filename);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

        

    }
    

    /* 
    public static void main(String[] args) {
        AwsService.getInstance().createBucketIfNotExists();
    }
    */
    

    private static void sendTerminateMessage() {
        // Implement the logic to send a terminate message to the Manager
        System.out.println("Terminate message sent to the Manager.");
    }
}