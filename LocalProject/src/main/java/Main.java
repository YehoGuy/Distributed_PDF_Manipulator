import java.util.Scanner;

public class Main {

    private static AwsLocalService aws = null;
    private static int clientId = -1;

    public static void main(String[] args) {
        try {
            // handle args from user
            if (args.length < 2) {
                System.err.println("Error - not enough arguments provided. Usage: java Main <file_path> <n> [terminate]");
                return;
            }
            String filePath = args[0];
            int n = Integer.parseInt(args[1]);
            boolean terminate = args.length == 3 && args[2].equalsIgnoreCase("terminate");
            // get instance id from user
            System.out.println("Enter a numerical Id for your client (must be uniqe and not -1): ");
            try (Scanner scanner = new Scanner(System.in)) {
                clientId = scanner.nextInt();
            } catch(Exception e){
                System.err.println("failed to recieve instance id: " + e.getMessage());
                return;
            }
            // init aws service with client id
            // aws initialization handles:
            // 1. creating the S3 bucket
            // 2. creating the UpStream SQS queue
            // 3. creating the DownStream SQS queue
            // 4. uploading Manager & Worker jars to S3
            // 5.  ensuring Manager's existance
            aws = new AwsLocalService(clientId);
            if(!aws.init()){
                System.err.println("failed to initialize aws service");
                return;
            }
            // upload the instructions file to S3.
            String filename = "instructions"+clientId+".txt";
            try {
                aws.uploadFileToS3(filePath, filename);
            } catch (Exception e) {
                System.out.println("failed to upload instructions to S3: "+e.getMessage());
                return;
            }
            // SQS requires some time to initialize, so we wait for 1 second.
            try {
                Thread.sleep(1000); // Pause for 1 second (1000 milliseconds)
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt(); // Reset the interrupted status
                System.err.println("Thread was interrupted: " + e.getMessage());
            }
            // send SQS message with the files location and num of workers
            // message format: <filename>,<filesPerWorker>,<clientId>
            try{
                aws.sendMessageToManager(filename+","+n+","+clientId);
            } catch (Exception e) {
                System.out.println(e.getMessage());
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
            // if terminate flag is on, send terminate message
            if(terminate){
                sendTerminateMessage();
            }
            
            
            
        


        
        

    } catch (Exception e) {
        System.err.println("[LocalError] an error occoured: " + e.getMessage());
    }
    

}
    

    private static void sendTerminateMessage() {
        try {
            // Pause for 1 second (1000 milliseconds)
            // to minimize the probabbility of the termination 
            // message arriving before the task message.
            Thread.sleep(1000); 
            aws.sendTerminationMessage();
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
    }
}