import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Main {

    private static final AwsManagerService aws = AwsManagerService.getInstance();
    
    private static boolean terminate = false;

    private static int maxIterations = 200; // for not accidently wasting my AWS money (:D)
    private static int restTime = 3000; // 3 seconds

    private static final int MAX_PARALLEL_CLIENTS = 20; // T3_SMALL is Optimal for 20 parallel Threads
    private static final int MAX_PARALLEL_WORKERS = 8;

    private static final ExecutorService THREAD_POOL = Executors.newFixedThreadPool(MAX_PARALLEL_CLIENTS);

    

    public static void main(String[] args) {
        try{
            if(aws!=null){

                while(!terminate){
                    String message = aws.receiveMessageFromLocalSqs();
                    // no message received
                    if(message == null){
                        try {
                            Thread.sleep(restTime); // Pause for 1 second (1000 milliseconds)
                            continue;
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt(); // Reset the interrupted status
                            throw new Exception("Thread was interrupted when trying to rest for a sec: " + e.getMessage());
                        }
                    } else {
                        // message received
                        if(message.equals("terminate")){
                            terminate = true;
                            break;
                        } else {
                            String[] splits = message.split(",");
                            if(splits.length != 3){
                            throw new Exception("failed to handle first message from Local - expected 3 parts, got "+splits.length);
                            }
                            else{
                                execute(splits);
                            }
                        }
                    } 
                }

                System.out.println("manager terminating...");
                System.out.println("Termination result: "+terminate());

            } 
            else {
                throw new Exception("failed to initialize AWS service.");
            }

        } catch(Exception e){
            System.err.println("ManagerError - failed to run program: "+e.getMessage());
        }
    }

    private static boolean terminate(){
        try {
            THREAD_POOL.awaitTermination(1200, TimeUnit.SECONDS);
            return terminateAllWorkers();
        } catch (Exception e) {
            return false;
        }
    }
        

    public static boolean terminateAllWorkers(){
        System.out.println("Starting termination of all workers");
        boolean result = true;
        for(int i=0 ; i<MAX_PARALLEL_WORKERS ; i++){
            try {
                aws.terminateWorkerInstance(i);
            } catch (Exception e) {
                System.err.println("ManagerError - failed to terminate worker "+i+": "+e.getMessage());
                result = false;
            }
        }
        return result;
    }

    private static void execute(String[] args){
        SubManager subManager = new SubManager(args, MAX_PARALLEL_WORKERS);
        THREAD_POOL.execute(subManager);
    }

}
