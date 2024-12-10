package Local;

import java.nio.file.Paths;
import java.util.Base64;
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

    public static String ami = "ami-00e95a9222311e8ed";

    public static Region region1 = Region.US_WEST_2;
    public static Region region2 = Region.US_EAST_1;

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
        this.createUpStreamQueue();
        this.createDownStreamQueue();
    }

    public static AwsService getInstance() {
        return instance;
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
     * Launches an EC2 instance with the specified parameters.
     *
     * @param script           User data script to bootstrap the instance.
     * @param tagName          Tag name to assign to the instance.
     * @param numberOfInstances Number of instances to launch.
     * @return Instance ID of the first launched instance.
     */
    public String createEC2(String script, String tagName, int numberOfInstances) {
        try {
            RunInstancesRequest runRequest = RunInstancesRequest.builder()
                    .instanceType(InstanceType.M4_LARGE)
                    .imageId(ami)
                    .maxCount(numberOfInstances)
                    .minCount(1)
                    .keyName("vockey")
                    .iamInstanceProfile(IamInstanceProfileSpecification.builder().name("LabInstanceProfile").build())
                    .userData(Base64.getEncoder().encodeToString((script).getBytes()))
                    .build();

            RunInstancesResponse response = ec2.runInstances(runRequest);
            String instanceId = response.instances().get(0).instanceId();

            Tag tag = Tag.builder()
                    .key("Name")
                    .value(tagName)
                    .build();

            CreateTagsRequest tagRequest = CreateTagsRequest.builder()
                    .resources(instanceId)
                    .tags(tag)
                    .build();

            ec2.createTags(tagRequest);
            System.out.printf("[DEBUG] Successfully started EC2 instance %s based on AMI %s\n", instanceId, ami);
            return instanceId;
        } catch (Ec2Exception e) {
            System.err.println("[ERROR] " + e.getMessage());
            return null;
        }
    }

    /**
     * Checks if the Manager EC2 instance is active by filtering instances with a specific tag and state.
     *
     * @param managerTagKey   The tag key used to identify the Manager node.
     * @param managerTagValue The tag value used to identify the Manager node.
     * @return true if the Manager instance is active (running), false otherwise.
     */
    public boolean isManagerActive(String managerTagKey, String managerTagValue) {
        try {
            DescribeInstancesRequest request = DescribeInstancesRequest.builder()
                    .filters(
                            Filter.builder()
                                    .name("tag:" + managerTagKey) // Filter by tag key
                                    .values(managerTagValue) // Filter by tag value
                                    .build(),
                            Filter.builder()
                                    .name("instance-state-name") // Filter by instance state
                                    .values("running") // Only consider running instances
                                    .build()
                    )
                    .build();

            DescribeInstancesResponse response = ec2.describeInstances(request);
            return response.reservations().stream()
                    .anyMatch(reservation -> !reservation.instances().isEmpty());
        } catch (Ec2Exception e) {
            System.err.println("[ERROR] Checking Manager instance: " + e.getMessage());
            return false;
        }
    }


    //---------------------- SQS Operations -------------------------------------------

    /**
     * Creates a fifo SQS queue with the specified name.
     */
    private void createSqsQueue(String queueName) {
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
        } catch (SqsException e) {}

    }

    private void createUpStreamQueue() {
        createSqsQueue(LOCAL_MANAGER_Q);
    }

    private void createDownStreamQueue() {
        createSqsQueue(MANAGER_LOCAL_Q);
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
