package Local;

import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.CreateTagsRequest;
import software.amazon.awssdk.services.ec2.model.DescribeInstancesRequest;
import software.amazon.awssdk.services.ec2.model.DescribeInstancesResponse;
import software.amazon.awssdk.services.ec2.model.Ec2Exception;
import software.amazon.awssdk.services.ec2.model.Filter;
import software.amazon.awssdk.services.ec2.model.IamInstanceProfileSpecification;
import software.amazon.awssdk.services.ec2.model.InstanceType;
import software.amazon.awssdk.services.ec2.model.RunInstancesRequest;
import software.amazon.awssdk.services.ec2.model.RunInstancesResponse;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.BucketLocationConstraint;
import software.amazon.awssdk.services.s3.model.CreateBucketConfiguration;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SqsException;

/// When running
/// start a new lab
/// copy temp credentials
/// edit the credentials file using nano ~/.aws/credentials
/// verify changes using 
/// cat ~/.aws/credentials & aws s3 ls

public class AwsService {
    private final S3Client s3;
    private final SqsClient sqs;
    private final Ec2Client ec2;
    private String managerInstanceId;

    public static final String ami = "ami-00e95a9222311e8ed";

    public static final Region region1 = Region.US_WEST_2;
    public static final Region region2 = Region.US_EAST_1;

    private static final String BUCKET_NAME = "guyss3bucketfordistributedsystems";
    private static final String LOCAL_MANAGER_Q = "LocalManagerQueue.fifo";
    private static final String LOCAL_MANAGER_MESSAGE_GROUP_ID = "LocaToManagerGroup";
    private static final String MANAGER_LOCAL_Q = "ManagerLocalQueue.fifo";
    private static final String MANAGER_LOCAL_MESSAGE_GROUP_ID = "ManagerToLocalGroup";
 


    private static final AwsService instance = new AwsService();

    private AwsService() {
        s3 = S3Client.builder().region(region1).build();
        sqs = SqsClient.builder().region(region1).build();
        ec2 = Ec2Client.builder().region(region2).build();
    }

    public static AwsService getInstance() {
        if(instance.init()){
            return instance;
        } else{
            return null;
        }
    }

    private boolean init(){
        return this.createUpStreamQueue() && 
                this.ensureManagerInstance() && 
                this.createDownStreamQueue();
    }

    //---------------------- S3 Operations -------------------------------------------

    /**
     * Creates an S3 bucket if it does not already exist.
     *
     * @param bucketName Name of the S3 bucket to create.
          * @throws Exception 
          */
    private void createBucketIfNotExists(){
        try {
            s3.createBucket(CreateBucketRequest
                    .builder()
                    .bucket(BUCKET_NAME)
                    .createBucketConfiguration(
                            CreateBucketConfiguration.builder()
                                    .locationConstraint(BucketLocationConstraint.US_WEST_2)
                                    .build())
                    .build());
            s3.waiter().waitUntilBucketExists(HeadBucketRequest.builder()
                    .bucket(BUCKET_NAME)
                    .build());
            System.out.println("[DEBUG] Bucket created successfully: " + BUCKET_NAME);
        } catch (S3Exception e) {}
    }

    /**
     * Uploads a file to the specified S3 bucket.
     *
     * @param filePath Path of the file to upload.
     * @param filename Name of the file in the S3 bucket.
     * return The key (path) of the uploaded file in the S3 bucket.
     */
    public String uploadFileToS3(String filePath, String filename) throws Exception {
        String s3Key = filename;
        try {
            createBucketIfNotExists();
            PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                    .bucket(BUCKET_NAME)
                    .key(s3Key)
                    .build();

            PutObjectResponse response = s3.putObject(putObjectRequest, Paths.get(filePath));
            System.out.println("[DEBUG] File uploaded to S3: " + s3Key);
        } catch (S3Exception e) {
            throw new Exception("[ERROR] " + e.getMessage());
        }
        return s3Key;
    }

    /**
     * Downloads a file from S3 and saves it locally.
     *
     * @param s3Key          Key (path) of the file in the S3 bucket.
     * @throws Exception 
     */
    public void downloadFileFromS3(String s3Key) throws Exception {
        String destinationPath = "src/main/java/Local/files/" + s3Key;
        try {
            GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                    .bucket(BUCKET_NAME)
                    .key(s3Key)
                    .build();

            s3.getObject(getObjectRequest, Paths.get(destinationPath));
            System.out.println("[DEBUG] File downloaded from S3 and saved to: " + destinationPath);
        } catch (S3Exception e) {
            throw new Exception("[ERROR] File downloaded from S3" + s3Key + "failed: " + e.getMessage());
        }
    }

    //---------------------- EC2 Operations -------------------------------------------

    /**
     * Ensures that a Manager EC2 instance exists and is running.
     * If no Manager exists, it creates and runs one.
     */
    private boolean ensureManagerInstance() {
        try {
        this.managerInstanceId = getManagerInstanceId();
        
        if (managerInstanceId != null) {
            System.out.println("[INFO] Manager instance found: " + managerInstanceId);
            
            if (!isInstanceRunning(managerInstanceId)) {
                System.out.println("[INFO] Manager instance is not running. Starting it...");
                startInstance(managerInstanceId);
            } else {
                System.out.println("[INFO] Manager instance is already running.");
            }
        } else {
            System.out.println("[INFO] No Manager instance found. Creating one...");
            managerInstanceId = createManager();
            
            if (managerInstanceId != null) {
                System.out.println("[INFO] Manager instance created and running: " + managerInstanceId);
            } else {
                throw new Exception("[ERROR] Failed to create Manager instance.");
            }
        }
        return true;
        } catch (Exception e) {
            System.err.println(e.getMessage());
            return false;
        }
    }

    /**
     * Gets the ID of the Manager instance if it exists.
     *
     * @return The instance ID of the Manager, or null if no instance is found.
     */
    private String getManagerInstanceId() throws Exception {
        try {
            DescribeInstancesRequest request = DescribeInstancesRequest.builder()
                    .filters(
                            Filter.builder()
                                    .name("tag:Name")
                                    .values("manager")
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
            throw new Exception("[ERROR] Failed to get Manager instance's Id: " + e.getMessage());
        }
    }

    /**
     * Checks if a given EC2 instance is running.
     *
     * @param instanceId The instance ID to check.
     * @return true if the instance is running, false otherwise.
     */
    private boolean isInstanceRunning(String instanceId) throws Exception {
        try {
            DescribeInstancesRequest request = DescribeInstancesRequest.builder()
                    .instanceIds(instanceId)
                    .build();

            DescribeInstancesResponse response = ec2.describeInstances(request);

            return response.reservations().stream()
                    .flatMap(reservation -> reservation.instances().stream())
                    .anyMatch(instance -> instance.state().nameAsString().equals("running"));
        } catch (Ec2Exception e) {
            throw new Exception("[ERROR] Failed to check instance state: " + e.getMessage());
        }
    }

    /**
     * Starts an EC2 instance.
     *
     * @param instanceId The ID of the instance to start.
     */
    private void startInstance(String instanceId) throws Exception {
        try {
            ec2.startInstances(startInstancesRequest -> startInstancesRequest.instanceIds(instanceId));
            System.out.println("[INFO] Instance started: " + instanceId);
        } catch (Ec2Exception e) {
            throw new Exception("[ERROR] Failed to start instance: " + e.getMessage());
        }
    }


    /**
     * Creates a T2_MICRO EC2 instance with the <Name, manager> tag.
     *
     * @return The Instance ID of the created Manager instance.
     */
    private String createManager() throws Exception{
        try {
            IamInstanceProfileSpecification role = IamInstanceProfileSpecification.builder()
                    .name("LabInstanceProfile") // Ensure this profile has required permissions
                    .build();

            // Create the EC2 instance
            RunInstancesRequest runRequest = RunInstancesRequest.builder()
                    .imageId(ami) // Use the predefined AMI
                    .instanceType(InstanceType.T2_MICRO)
                    .maxCount(1)
                    .minCount(1)
                    .iamInstanceProfile(role)
                    .build();

            RunInstancesResponse response = ec2.runInstances(runRequest);
            String instanceId = response.instances().get(0).instanceId();

            // Add a tag <Name, manager>
            Tag tag = Tag.builder()
                    .key("Name")
                    .value("manager")
                    .build();

            CreateTagsRequest tagRequest = CreateTagsRequest.builder()
                    .resources(instanceId)
                    .tags(tag)
                    .build();
            ec2.createTags(tagRequest);

            System.out.printf("[DEBUG] Manager instance created: %s\n", instanceId);
            return instanceId;

        } catch (Ec2Exception e) {
            throw new Exception("[ERROR] Failed to create Manager instance: " + e.getMessage());
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

    private boolean createUpStreamQueue() {
        return createSqsQueue(LOCAL_MANAGER_Q);
    }

    private boolean createDownStreamQueue() {
        return createSqsQueue(MANAGER_LOCAL_Q);
    }


    /**
     * Sends a message to the specified SQS queue.
     *
     * @param messageBody Message to send.
     */
    public void sendMessageToSqs(String messageBody) throws Exception {
        try {
            // Get the queue's URL
            GetQueueUrlRequest getQueueUrlRequest = GetQueueUrlRequest.builder()
                .queueName(LOCAL_MANAGER_Q)
                .build();
            String queueUrl = sqs.getQueueUrl(getQueueUrlRequest).queueUrl();
            // send the message
            SendMessageRequest sendMessageRequest = SendMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .messageBody(messageBody)
                    .messageGroupId(LOCAL_MANAGER_MESSAGE_GROUP_ID)
                    .build();
            sqs.sendMessage(sendMessageRequest);
            System.out.println("[DEBUG] Message sent to SQS: " + messageBody);
        } catch (SqsException e) {
            throw new Exception("[ERROR] Message send to SQS failed: " + e.getMessage());
        }
    }

    /**
     * Receives a message from the SQS queue.
     * @return The received message body, or null if no message is available.
     */
    public String receiveMessageFromSqs() throws Exception{
        try {
            // Get the queue's URL
            GetQueueUrlRequest getQueueUrlRequest = GetQueueUrlRequest.builder()
                .queueName(MANAGER_LOCAL_Q)
                .build();
            String queueUrl = sqs.getQueueUrl(getQueueUrlRequest).queueUrl();
            // receive the message
            ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .maxNumberOfMessages(1)
                    .waitTimeSeconds(10)
                    .build();
            var messages = sqs.receiveMessage(receiveMessageRequest).messages();
            if (!messages.isEmpty()) {
                var message = messages.get(0);
                deleteMessageFromSqs(queueUrl, message.receiptHandle());
                System.out.println("[DEBUG] Message received from SQS: " + message.body());
                return message.body();
            }
        } catch (SqsException e) {
            throw new Exception("[ERROR] Messsage recieving from SQS failed: " + e.getMessage());
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

    //---------------------- Termination Message -------------------------------------------
    /**
     * Sends a termination message to the Manager via SQS.
     *
     * @param queueUrl The SQS queue URL for sending the termination message.
     */
    public void sendTerminationMessage() throws Exception{
        try{
        sendMessageToSqs("terminate");
        System.out.println("[DEBUG] Termination message sent to Manager.");
        } catch (Exception e) {
            throw new Exception("[ERROR] " + e.getMessage());
        }
    }

}
