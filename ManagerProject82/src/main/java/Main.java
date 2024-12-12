import java.util.ArrayList;
import java.util.List;

public class Main {

    private static final AwsManagerService aws = AwsManagerService.getInstance();
    private static String instructionsFileS3Key;
    private static int n;

    public static void main(String[] args) {
        try{
            if(aws!=null){

                List<String> operations = new ArrayList<String>();
                List<String> urls = new ArrayList<String>();

                // read first message, hoping for filename,number_of_workers
                String message = aws.receiveMessageFromLocalSqs();
                String[] splits = message.split(",");
                if(splits.length != 2){
                    throw new Exception("failed to handle first message from Local - expected 2 parts, got "+splits.length);
                }
                else{
                    instructionsFileS3Key = splits[0];
                    n = Integer.parseInt(splits[1]);
                }

                // download the instructions file from S3.
                try {
                    aws.downloadFileFromS3(instructionsFileS3Key);
                } catch (Exception e) {
                    throw new Exception("failed to download "+instructionsFileS3Key+" instructions file from S3: "+e.getMessage());
                }

                // SQS requires some time to initialize, so we wait for 1 second.
                try {
                    Thread.sleep(1000); // Pause for 1 second (1000 milliseconds)
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt(); // Reset the interrupted status
                    throw new Exception("Thread was interrupted when trying to rest for a sec: " + e.getMessage());
                }

                aws.sendMessageToLocalSqs("all good");

                /*

                try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        String[] parts = line.split("\t");
                        if (parts.length == 2) {
                            operations.add(parts[0]);
                            urls.add(parts[1]);
                            //System.out.println(parts[0] + " " + parts[1]);
                        }
                    }
                } catch (IOException e) {
                    System.err.println("Local Error reading input file: " + e.getMessage());
                    return;
                }


                int totalFiles = urls.size();
                int workers = (int) Math.ceil((double) totalFiles / n);

                for (int i = 0; i < workers; i++) {
                    int start = i * n;
                    int end = Math.min(start + n, totalFiles);
                    List<String> workerUrls = urls.subList(start, end);
                    List<String> workerOperations = operations.subList(start, end);
                    // Process workerUrls and workerOperations
                }
                */

            } else {
                throw new Exception("ManagerError - failed to initialize AWS service.");
            }
        } catch(Exception e){
            System.err.println("ManagerError - failed to run program: "+e.getMessage());
            try {
                aws.sendMessageToLocalSqs("ManagerError - failed to run program: "+e.getMessage());
            } catch (Exception innerException) {
                System.err.println("ManagerError - failed to send error message to Local: "+innerException.getMessage());
            }

        }
    }

}
