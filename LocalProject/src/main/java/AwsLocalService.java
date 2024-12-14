

import java.nio.file.Paths;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
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
import software.amazon.awssdk.services.sqs.model.Message;
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

public class AwsLocalService {

    // instance values
    private int clientId;
    

    // AWS general values
    public static final String ami = "ami-00e95a9222311e8ed";
    public static final Region region1 = Region.US_WEST_2;
    public static final Region region2 = Region.US_EAST_1;

    // S3 values
    private final S3Client s3;
    private static final String BUCKET_NAME = "guyss3bucketfordistributedsystems";
    private static final String PATH_TO_MANAGER_PROJECT_JAR = "mwJars/ManagerProject.jar";
    private static final String PATH_TO_WORKER_PROJECT_JAR = "mwJars/WorkerProject.jar";
    private static final String MANAGER_PROGRAM_S3KEY = "ManagerProgram.jar";
    private static final String WORKER_PROGRAM_S3KEY = "WorkerProgram.jar";

    // EC2 values
    private final Ec2Client ec2;

    // SQS values
    private final SqsClient sqs;
    private static final String UPSTREAM_Q = "LocalManagerQueue.fifo";
    private static final String UPSTREAM_MESSAGE_GROUP_ID = "LocalToManagerGroup";
    private static final String MANAGER_LOCAL_MESSAGE_GROUP_ID = "ManagerToLocalGroup";
    private String downStreamQueue;
    
    

    private static String managerInstanceId;


    public AwsLocalService(int clientId) {
        s3 = S3Client.builder().region(region1).build();
        sqs = SqsClient.builder().region(region1).build();
        ec2 = Ec2Client.builder().region(region2).build();
        this.clientId = clientId;
        this.downStreamQueue = "SMToLocal"+clientId+".fifo";

    }

    private boolean init() {
        //sequential order
        return  this.ensureS3Bucket() &&
                this.createUpStreamQueue() &&  
                this.createDownStreamQueue() &&
                this.uploadManagersProgramToS3() && 
                this.uploadWorkersProgramToS3() &&
                this.ensureManagerInstance();
    }

    //---------------------- S3 Operations -------------------------------------------

    private boolean ensureS3Bucket() {
        try {
            // Check if the bucket exists
            if (!doesBucketExist(BUCKET_NAME)) {
                System.out.println("[INFO] S3 bucket does not exist. Creating bucket: " + BUCKET_NAME);
                if (createBucketIfNotExists()) {
                    System.out.println("[INFO] S3 bucket created successfully: " + BUCKET_NAME);
                } else {
                    throw new Exception("[ERROR] Failed to create S3 bucket: " + BUCKET_NAME);
                }
            } else {
                System.out.println("[INFO] S3 bucket already exists: " + BUCKET_NAME);
            }
            return true;
        } catch (Exception e) {
            System.err.println(e.getMessage());
            return false;
        }
    }

    /**
     * Checks if the S3 bucket exists.
     *
     * @param bucketName Name of the S3 bucket to check.
     * @return true if the bucket exists, false otherwise.
     */
    private boolean doesBucketExist(String bucketName) {
        try {
            s3.headBucket(HeadBucketRequest.builder().bucket(bucketName).build());
            return true;
        } catch (S3Exception e) {
            if (e.statusCode() == 404) {
                return false; // Bucket does not exist
            }
            System.err.println("[ERROR] Failed to check bucket existence: " + e.getMessage());
            return false;
        }
    }
    
    /**
     * Creates an S3 bucket if it does not already exist.
     *
     * @param bucketName Name of the S3 bucket to create.
          * @throws Exception 
          */
    private boolean createBucketIfNotExists(){
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
            return true;
        } catch (S3Exception e) {
            System.err.println("[ERROR] creating bucket: " + e.getMessage());
            return false;
        }
    }

    /**
     * Uploads a file to the specified S3 bucket.
     *
     * @param filePath Path of the file to upload.
     * @param filename Name of the file in the S3 bucket.
     * @return The key (path) of the uploaded file in the S3 bucket.
     */
    public String uploadFileToS3(String filePath, String filename) throws Exception {
        String s3Key = filename;
        try {
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
        String destinationPath = "files/" + s3Key;
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

            String instanceState = getInstanceState(managerInstanceId);
            switch (instanceState) {
                case "running":
                    System.out.println("[INFO] Manager instance is already running.");
                    break;

                case "stopped":
                    System.out.println("[INFO] Manager instance is stopped. Starting it...");
                    startInstance(managerInstanceId);
                    break;

                case "terminated":
                    System.out.println("[INFO] Manager instance is terminated. Creating a new one...");
                    managerInstanceId = createManager();
                    break;

                default:
                    System.out.println("[WARN] Manager instance is in an unsupported state: " + instanceState);
                    break;
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
            // Generate the User Data script
            String userDataScript = generateManagerDataScript(BUCKET_NAME, MANAGER_PROGRAM_S3KEY);

            // Encode the User Data script in Base64 as required by AWS
            String userDataEncoded = Base64.getEncoder().encodeToString(userDataScript.getBytes());

            IamInstanceProfileSpecification role = IamInstanceProfileSpecification.builder()
                    .name("LabInstanceProfile") // Ensure this profile has required permissions
                    .build();

            // Create the EC2 instance
            RunInstancesRequest runRequest = RunInstancesRequest.builder()
                    .imageId(ami) // Use the predefined AMI
                    .instanceType(InstanceType.T2_MICRO) //TODO T3_SMALL for server, T2_Micro for worker
                    .maxCount(1)
                    .minCount(1)
                    .iamInstanceProfile(role)
                    .userData(userDataEncoded)
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

    // Make sure userScript is correct
    private String generateManagerDataScript(String s3BucketName, String jarFileName) {
        return "#!/bin/bash\n" +
               "echo Manager jar running\n" +
               "mkdir ManagerFiles\n" +
               "aws s3 cp s3://" + s3BucketName + "/" + jarFileName + " ./ManagerFiles/"+jarFileName+"\n" +
               "java -jar ./ManagerFiles/" + jarFileName + "\n";
    }
    

    /**
    * Retrieves the current state of an EC2 instance.
    *
    * @param instanceId The ID of the instance.
    * @return The state of the instance (e.g., "running", "stopped", "terminated").
    */
    private String getInstanceState(String instanceId) throws Exception {
        try {
            DescribeInstancesRequest request = DescribeInstancesRequest.builder()
                    .instanceIds(instanceId)
                    .build();

            DescribeInstancesResponse response = ec2.describeInstances(request);

            return response.reservations().stream()
                    .flatMap(reservation -> reservation.instances().stream())
                    .findFirst()
                    .map(instance -> instance.state().nameAsString())
                    .orElse("unknown");

        } catch (Ec2Exception e) {
            throw new Exception("[ERROR] Failed to get instance state: " + e.getMessage());
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
        return createSqsQueue(UPSTREAM_Q);
    }

    private boolean createDownStreamQueue() {
        return createSqsQueue(this.downStreamQueue);
    }


    /**
     * Sends a message to the specified SQS queue.
     *
     * @param messageBody Message to send.
     */
    public void sendMessageToManager(String messageBody) throws Exception {
        try {
            // Get the queue's URL
            GetQueueUrlRequest getQueueUrlRequest = GetQueueUrlRequest.builder()
                .queueName(UPSTREAM_Q)
                .build();
            String queueUrl = sqs.getQueueUrl(getQueueUrlRequest).queueUrl();
            // send the message
            SendMessageRequest sendMessageRequest = SendMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .messageBody(messageBody)
                    .messageGroupId(UPSTREAM_MESSAGE_GROUP_ID)
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
    public String receiveMessageFromManager() throws Exception{
        try {
            // Get the queue's URL
            GetQueueUrlRequest getQueueUrlRequest = GetQueueUrlRequest.builder()
                .queueName(this.downStreamQueue)
                .build();
            String queueUrl = sqs.getQueueUrl(getQueueUrlRequest).queueUrl();
            // receive the message
            ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .maxNumberOfMessages(1)
                    .waitTimeSeconds(10)
                    .build();
            List<Message> messages = sqs.receiveMessage(receiveMessageRequest).messages();
            if (!messages.isEmpty()) {
                Message message = messages.get(0);
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
        sendMessageToManager("terminate");
        System.out.println("[DEBUG] Termination message sent to Manager.");
        } catch (Exception e) {
            throw new Exception("[ERROR] " + e.getMessage());
        }
    }

    //---------------------- Program Upload -------------------------------------------
    
    private boolean uploadManagersProgramToS3() {
        try {
            String filePath = PATH_TO_MANAGER_PROJECT_JAR;
            return uploadFileToS3(filePath, MANAGER_PROGRAM_S3KEY) != null;
        } catch (Exception e) {
            System.err.println("[ERROR] " + e.getMessage());
            return false;
        }
    }

    private boolean uploadWorkersProgramToS3() {
        try {
            String filePath = PATH_TO_WORKER_PROJECT_JAR;
            return uploadFileToS3(filePath, WORKER_PROGRAM_S3KEY) != null;
        } catch (Exception e) {
            System.err.println("[ERROR] " + e.getMessage());
            return false;
        }
    }


    



}
