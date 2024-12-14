

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Paths;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
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

public class AwsSubManagerService {
    // instance attributes
    private final int clientId;
    private final String workersToSMQ;
    
    // shared attributes
    public static final String ami = "ami-00e95a9222311e8ed";

    public static final Region region1 = Region.US_WEST_2;
    public static final Region region2 = Region.US_EAST_1;

    private static final S3Client s3 = S3Client.builder().region(region1).build();
    private static final SqsClient sqs = SqsClient.builder().region(region1).build();
    private static final Ec2Client ec2 = Ec2Client.builder().region(region2).build();

    private static final String BUCKET_NAME = "guyss3bucketfordistributedsystems";
    private static final String WORKER_PROGRAM_S3KEY = "WorkerProgram.jar";

    private static final String MANAGER_TO_WORKERS_Q = "ManagerWorkersQueue.fifo";
    private static final String MANAGER_WORKERS_MESSAGE_GROUP_ID = "ManagerToWorkersGroup";
    

    public AwsSubManagerService(int clientId){
        this.clientId = clientId;
        this.workersToSMQ = "WorkersToSM"+this.clientId+".fifo";
    }
     // --------------------- ec2 operations ---------------------

     public boolean ensureWorkerInstance(int n) throws Exception{
        return AwsSubManagerService.ensureWorkerInstanceSt(n);
     }
     /**
     * Ensures that the n'th exists and is running.
     * synchronized to avoid server errors. e.g creating workern twice
     * @param n reffers to the n'th worker - corresponds to tag: workern
     * @return true iff operation succeeded.
     * @throws Exception with detailed message if operation failed.
     */
    private static synchronized boolean ensureWorkerInstanceSt(int n) throws Exception{
        try {
            String workerInstanceId = getWorkerInstanceId(n);

            if (workerInstanceId != null) {
                System.out.println("[INFO] Worker "+n+"  instance found: " + workerInstanceId);

                String instanceState = getInstanceState(workerInstanceId);
                switch (instanceState) {
                    case "running":
                        System.out.println("[INFO] Worker "+n+" instance is already running.");
                        break;

                    case "stopped":
                        System.out.println("[INFO] Worker "+n+" instance is stopped. Starting it...");
                        startInstance(workerInstanceId);
                        break;

                    case "terminated":
                        System.out.println("[INFO] Worker "+n+" instance is terminated. Creating a new one...");
                        workerInstanceId = createWorker(n);
                        break;

                    default:
                        System.out.println("[WARN] Worker "+n+"  instance is in an unsupported state: " + instanceState);
                        break;
                }
            } else {
                System.out.println("[INFO] No Worker "+n+"  instance found. Creating one...");
                workerInstanceId = createWorker(n);

                if (workerInstanceId != null) {
                    System.out.println("[INFO] worker "+n+" instance created and running: " + workerInstanceId);
                } else {
                    throw new Exception("[ERROR] Failed to create worker "+n+" instance. ");
                }
            }
            return true;
        } catch (Exception e) {
            throw new Exception("[ERROR] Failed to ensure Worker "+n+" instance: " + e.getMessage());
        }
    }

    /**
     * Gets the ID of the Manager instance if it exists.
     * @param n reffers to the n'th worker - corresponds to tag: workern
     * @return The instance ID of the Manager, or null if no instance is found.
     */
    private static String getWorkerInstanceId(int n) throws Exception {
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


    /**
     * Starts an EC2 instance.
     *
     * @param instanceId The ID of the instance to start.
     */
    private static void startInstance(String instanceId) throws Exception {
        try {
            ec2.startInstances(startInstancesRequest -> startInstancesRequest.instanceIds(instanceId));
            System.out.println("[INFO] Instance started: " + instanceId);
        } catch (Ec2Exception e) {
            throw new Exception("[ERROR] Failed to start instance: " + e.getMessage());
        }
    }



    /**
     * Creates a T2_MICRO EC2 instance with the <Name, manager> tag.
     * @param n reffers to the n'th worker - corresponds to tag: workern
     * @return The Instance ID of the created Manager instance.
     */
    private static String createWorker(int n) throws Exception{
        try {
            // Generate the User Data script
            String userDataScript = generateWorkerDataScript(BUCKET_NAME, WORKER_PROGRAM_S3KEY);

            // Encode the User Data script in Base64 as required by AWS
            String userDataEncoded = Base64.getEncoder().encodeToString(userDataScript.getBytes());

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
                    .userData(userDataEncoded)
                    .build();

            RunInstancesResponse response = ec2.runInstances(runRequest);
            String instanceId = response.instances().get(0).instanceId();

            // Add a tag <Name, manager>
            Tag tag = Tag.builder()
                    .key("Name")
                    .value("worker"+n)
                    .build();

            CreateTagsRequest tagRequest = CreateTagsRequest.builder()
                    .resources(instanceId)
                    .tags(tag)
                    .build();
            ec2.createTags(tagRequest);

            System.out.printf("[DEBUG] Worker "+n+" instance created: %s\n", instanceId);
            return instanceId;

        } catch (Ec2Exception e) {
            throw new Exception("[ERROR] Failed to create Worker"+n+" instance: " + e.getMessage());
        }
    }

    // Make sure userScript is correct
    private static String generateWorkerDataScript(String s3BucketName, String jarFileName) {
        return "#!/bin/bash\n" +
               "echo Worker jar running\n" +
               "mkdir WorkrFiles\n" +
               "aws s3 cp s3://" + s3BucketName + "/" + jarFileName + " ./WorkerFiles/"+jarFileName+"\n" +
               "java -jar ./WorkerFiles/" + jarFileName + "\n";
    }




    /**
     * Retrieves the current state of an EC2 instance.
     *
     * @param instanceId The ID of the instance.
     * @return The state of the instance (e.g., "running", "stopped", "terminated").
     */
    private static String getInstanceState(String instanceId) throws Exception {
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

    
    //--------------------- sqs operations ---------------------

    /**
     * Creates a fifo SQS queue with the specified name.
     */
    private void createSqsQueue(String queueName) throws Exception {
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
        } catch (SqsException e) {
            throw new Exception("[ERROR] creating "+queueName+" SQS: " + e.getMessage());
        }

    }

    public void createWorkersToSMQueue() throws Exception{
        createSqsQueue(this.workersToSMQ);
    }

    /**
     * Sends a message to the specified SQS queue.
     *
     * @param messageBody Message to send.
     */
    public void sendInstructionToWorkers(String messageBody) throws Exception {
        try {
            // Get the queue's URL
            GetQueueUrlRequest getQueueUrlRequest = GetQueueUrlRequest.builder()
                    .queueName(MANAGER_TO_WORKERS_Q)
                    .build();
            String queueUrl = sqs.getQueueUrl(getQueueUrlRequest).queueUrl();
            // send the message
            SendMessageRequest sendMessageRequest = SendMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .messageBody(this.workersToSMQ+"    "+messageBody)
                    .messageGroupId(MANAGER_WORKERS_MESSAGE_GROUP_ID)
                    .build();
            sqs.sendMessage(sendMessageRequest);
            System.out.println("[DEBUG] Message sent to ManagerToLocalSQS: " + messageBody);
        } catch (SqsException e) {
            throw new Exception("[ERROR] Message send to ManagerToLocalSQS failed: " + e.getMessage());
        }
    }

    /**
     * Receives a message from the SQS queue.
     * @return The received message body, or null if no message is available.
     */
    public String receiveResultFromWorkers() throws Exception{
        try {
            // Get the queue's URL
            GetQueueUrlRequest getQueueUrlRequest = GetQueueUrlRequest.builder()
                    .queueName(this.workersToSMQ)
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
                System.out.println("[DEBUG] Message received from WorkersToManagerSQS: " + message.body());
                return message.body();
            }
        } catch (SqsException e) {
            throw new Exception("[ERROR] Messsage recieving from WorkersToManagerSQS failed: " + e.getMessage());
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
    private void deleteMessageFromSqs(String queueUrl, String receiptHandle) throws Exception {
        try {
            DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .receiptHandle(receiptHandle)
                    .build();

            sqs.deleteMessage(deleteMessageRequest);
            System.out.println("[DEBUG] Message deleted from SQS: " + receiptHandle);
        } catch (SqsException e) {
            throw new Exception("[ERROR] deleting message from workersToSM queue: " + e.getMessage());
        }
    }

    
    //--------------------- s3 operations ----------------------

    /**
     * Uploads a file to the specified S3 bucket.
     *
     * @param filePath Path of the file to upload.
     * @param filename Name of the file in the S3 bucket.
     * @return The key (path) of the uploaded file in the S3 bucket.
     */
    public String uploadFileToS3(String filePath, String filename) throws Exception {
        String s3Key = "client"+this.clientId+"/"+filename;
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
     *
     * @return The lines of the file as a list of strings.
     */
    public List<String> downloadFileFromS3(String s3Key) throws Exception {
        try {
            GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                    .bucket(BUCKET_NAME)
                    .key(s3Key)
                    .build();

            InputStream is = s3.getObject(getObjectRequest);
            BufferedReader bufferReader= new BufferedReader(new InputStreamReader(is));
            return bufferReader.lines().collect(Collectors.toList());
        } catch (Exception e) {
            throw new Exception("[ERROR] File downloaded from S3" + s3Key + "failed: " + e.getMessage());
        }
    }




}


