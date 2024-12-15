

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.DescribeInstancesRequest;
import software.amazon.awssdk.services.ec2.model.DescribeInstancesResponse;
import software.amazon.awssdk.services.ec2.model.Ec2Exception;
import software.amazon.awssdk.services.ec2.model.Filter;
import software.amazon.awssdk.services.ec2.model.TerminateInstancesRequest;
import software.amazon.awssdk.services.ec2.model.TerminateInstancesResponse;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.SqsException;

public class AwsManagerService {
    // instance values
    private final S3Client s3;
    private final SqsClient sqs;
    private final Ec2Client ec2;
    

    public static final String ami = "ami-00e95a9222311e8ed";

    public static final Region region1 = Region.US_WEST_2;
    public static final Region region2 = Region.US_EAST_1;

    private static final String LOCAL_MANAGER_Q = "LocalManagerQueue.fifo";
    private static final String MANAGER_WORKERS_Q = "ManagerWorkersQueue.fifo";
    



    private static final AwsManagerService instance = new AwsManagerService();

    private AwsManagerService() {
        s3 = S3Client.builder().region(region1).build();
        sqs = SqsClient.builder().region(region1).build();
        ec2 = Ec2Client.builder().region(region2).build();
    }

    public static AwsManagerService getInstance() {
        if(instance.init()){
            return instance;
        } else{
            return null;
        }
    }

    private boolean init(){
        return createManagerToWorkersQueue();
    }

    //---------------------- S3 Operations -------------------------------------------

    

    //---------------------- EC2 Operations -------------------------------------------

    /**
     * Terminates an EC2 instance based on the given instance ID.
     *
     * @param instanceId The ID of the EC2 instance to terminate.
     */
    public void terminateWorkerInstance(int n) throws Exception{
        // Create an EC2 client
        try {
            String instanceId = getWorkerInstanceId(n);
            if(instanceId == null)
                return;
            // Build the termination request
            TerminateInstancesRequest terminateRequest = TerminateInstancesRequest.builder()
                    .instanceIds(instanceId)
                    .build();

            // Send the termination request
            TerminateInstancesResponse response = ec2.terminateInstances(terminateRequest);

            // Print the result
            response.terminatingInstances().forEach(instanceStateChange -> {
                System.out.println("Worker Instance " + instanceStateChange.instanceId() + 
                        " is now in state: " + instanceStateChange.currentState().name());
            });

        } catch (Exception e) {
            throw new Exception("[SubManagerError] Failed to terminate instance " + n + ": " + e.getMessage());
        }
    }

    /**
     * Gets the ID of the Manager instance if it exists.
     * @param n reffers to the n'th worker - corresponds to tag: workern
     * @return The instance ID of the Manager, or null if no instance is found.
     */
    private String getWorkerInstanceId(int n) throws Exception {
        try {
            DescribeInstancesRequest request = DescribeInstancesRequest.builder()
                    .filters(
                            Filter.builder()
                                    .name("tag:Name")
                                    .values("worker"+n)
                                    .build()
                    )
                    .build();

            DescribeInstancesResponse response = ec2.describeInstances(request);

            return response.reservations().stream()
                    .flatMap(reservation -> reservation.instances().stream())
                    .findFirst()
                    .map(instance -> instance.instanceId())
                    .orElse(null);
        } catch (Ec2Exception e) {
            throw new Exception("[ERROR] Failed to get Worker "+n+" instance's Id: " + e.getMessage());
        }
    }


    //---------------------- SQS Operations -------------------------------------------

    /**
     * Creates a fifo SQS queue with the specified name.
     */
    private boolean createSqsQueue(String queueName) {
        try {
            // Define the FIFO-specific attributes
            Map<QueueAttributeName, String> attributes = new HashMap<>();
            attributes.put(QueueAttributeName.FIFO_QUEUE, "true"); // Mark as FIFO queue
            attributes.put(QueueAttributeName.CONTENT_BASED_DEDUPLICATION, "true"); // Optional: Enable content-based deduplication
            CreateQueueRequest createQueueRequest = CreateQueueRequest.builder()
                    .queueName(queueName)
                    .attributes(attributes)
                    .build();
            sqs.createQueue(createQueueRequest);
            System.out.println("[DEBUG] Queue created successfully: " + queueName);
            return true;
        } catch (SqsException e) {
            System.err.println("[ERROR] creating "+queueName+" SQS: " + e.getMessage());
            return false;
        }

    }

    private boolean createManagerToWorkersQueue() {
        return createSqsQueue(MANAGER_WORKERS_Q);
    }


    /**
     * Receives a message from the SQS queue.
     * @return The received message body, or null if no message is available.
     */
    public String receiveMessageFromLocalSqs() throws Exception {
        try {
            // Get the queue's URL
            GetQueueUrlRequest getQueueUrlRequest = GetQueueUrlRequest.builder()
                    .queueName(LOCAL_MANAGER_Q)
                    .build();
            String queueUrl = sqs.getQueueUrl(getQueueUrlRequest).queueUrl();

            // Receive the message
            ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .maxNumberOfMessages(1)
                    .waitTimeSeconds(10)
                    .build();
            List<Message> messages = sqs.receiveMessage(receiveMessageRequest).messages();

            if (!messages.isEmpty()) {
                Message message = messages.get(0);
                deleteMessageFromSqs(queueUrl, message.receiptHandle());
                System.out.println("[DEBUG] Message received from LocalToManagerSQS: " + message.body());
                return message.body();
            }
        } catch (SqsException e) {
            throw new Exception("[ERROR] Message receiving from LocalToManagerSQS failed: " + e.getMessage());
        }
        return null;
    }


    /**
     * Deletes a message from the specified SQS queue.
     * helper for receiveMessageFromSqs
     *
     * @param queueUrl     URL of the SQS queue.
     * @param receiptHandle Receipt handle of the message to delete.
     */
    private void deleteMessageFromSqs(String queueUrl, String receiptHandle) {
        try {
            DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .receiptHandle(receiptHandle)
                    .build();

            sqs.deleteMessage(deleteMessageRequest);
            System.out.println("[DEBUG] Message deleted from SQS: " + receiptHandle);
        } catch (SqsException e) {
            System.err.println("[ERROR] " + e.getMessage());
        }
    }


}


