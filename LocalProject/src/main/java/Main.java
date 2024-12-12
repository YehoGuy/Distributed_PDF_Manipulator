

public class Main {

    private static final AwsLocalService aws = AwsLocalService.getInstance();

    public static void main(String[] args) {
        try {
        if(aws!=null){
        // handle args from user
        if (args.length < 2) {
            System.err.println("Error - not enough arguments provided. Usage: java Main <file_path> <n> [terminate]");
            return;
        }
        String filePath = args[0];
        String filename = "instructions.txt";
        int n = Integer.parseInt(args[1]);
        boolean terminate = args.length == 3 && args[2].equalsIgnoreCase("terminate");
        // upload the instructions file to S3.
        try {
            aws.uploadFileToS3(filePath, filename);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        // SQS requires some time to initialize, so we wait for 1 second.
        try {
            Thread.sleep(1000); // Pause for 1 second (1000 milliseconds)
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt(); // Reset the interrupted status
            System.err.println("Thread was interrupted: " + e.getMessage());
        }
        // send SQS message with the files location and num of workers
        try{
            aws.sendMessageToManager("instructions.txt,"+n);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        if(terminate){
            sendTerminateMessage();
        }
        // busy wait for answer
        String output = aws.receiveMessageFromManager();
        while(output == null){
            try{
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt(); // Reset the interrupted status
                System.err.println("Thread was interrupted: " + e.getMessage());
            }
            output = aws.receiveMessageFromManager();
        }
        System.out.println(output);
        
    }


        
        

    } catch (Exception e) {
        System.err.println(e.getMessage());
    }
    

}
    

    private static void sendTerminateMessage() {
        try {
            aws.sendTerminationMessage();
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
    }
}