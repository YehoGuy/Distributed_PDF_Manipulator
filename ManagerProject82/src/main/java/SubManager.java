import java.util.ArrayList;
import java.util.List;


// Each SubManager is responsible for a single client
public class SubManager implements Runnable {

    private final String instructionsFileS3Key;
    private final int filesPerWorker;
    private final int clientId;
    private final int maxNumberOfWorkers;
    private final AwsSubManagerService aws;

    public SubManager(String[] args, int maxNumberOfWorkers){
        this.instructionsFileS3Key = args[0];
        this.filesPerWorker = Integer.parseInt(args[1]);
        this.clientId = Integer.parseInt(args[2]);
        this.maxNumberOfWorkers = maxNumberOfWorkers;
        this.aws = new AwsSubManagerService(this.clientId);
    }


    @Override
    public void run() {
        try{
            System.out.println("SubManager "+this.clientId+" started");
            // download instructions file
            List<String> instructions = aws.downloadFileFromS3(this.instructionsFileS3Key);
            int numOfWorkers = (instructions.size() / this.filesPerWorker) + 1;
            // enforce filesPerWorker with max number of workers limit
            if(numOfWorkers > this.maxNumberOfWorkers){
                numOfWorkers = this.maxNumberOfWorkers;
            }
            ensureWorkers(this.aws, numOfWorkers);
            // establish workers ==> subManager queue
            aws.createWorkersToSMQueue();
            // send instructions to workers
            for(int i=0 ; i<instructions.size() ; i++){
                aws.sendInstructionToWorkers(instructions.get(i)+"\t"+clientId);
            }
            // busy collect answers
            List<String> results = new ArrayList<>();
            while(results.size() < instructions.size()){
                String result = aws.receiveResultFromWorkers();
                if(result!=null){
                    results.add(result);
                    System.out.println("SubManager "+this.clientId+" received result: "+result);
                }
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            
            // upload results to S3 & send url to local
            aws.sendMessageToLocal(aws.uploadSummaryHtmlToS3(results));
            
            aws.deleteDownStreamQueue();
        } catch (Exception e){
            System.out.println("[SubManagerError] - failed to run SubManager "+this.clientId+": "+e.getMessage());
        }
    }

            
    

    //important that is static for sync
    public static synchronized void ensureWorkers(AwsSubManagerService aws, int numOfWorkers){
        try {
            for(int i=0 ; i<numOfWorkers ; i++){
                aws.ensureWorkerInstance(i);
            }
        } catch (Exception e) {
        }
    }


    

    
}
