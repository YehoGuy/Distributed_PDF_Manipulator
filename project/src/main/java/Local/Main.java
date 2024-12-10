package Local;

public class Main {

    private static final AwsLocalService aws = AwsLocalService.getInstance();

    public static void main(String[] args) {
        if(aws!=null){
        /* 
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
        // upload the instructions file to S3.
        try {
            aws.uploadFileToS3(filePath, filename);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        // send SQS message with the files location
        try {
            aws.sendMessageToSqs(filename);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        */
        } else {
            System.err.println("Error - failed to initialize AWS service.");
        }
        

        

    }
    

    /* 
    public static void main(String[] args) {
        AwsService.getInstance().createBucketIfNotExists();
    }
    */
    

    private static void sendTerminateMessage() {
        try {
            aws.sendTerminationMessage();
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
    }
}