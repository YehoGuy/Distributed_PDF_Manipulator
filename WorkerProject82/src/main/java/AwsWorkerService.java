import java.util.List;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SqsException;


public class AwsWorkerService {

    // AWS general values
    public static final String ami = "ami-00e95a9222311e8ed";
    public static final Region region1 = Region.US_WEST_2;
    public static final Region region2 = Region.US_EAST_1;

    // S3 values
    private static final S3Client s3 = S3Client.builder().region(region1).build();
    private static final String BUCKET_NAME = "guyss3bucketfordistributedsystems";

    // SQS values
    private static final SqsClient sqs = SqsClient.builder().region(region1).build();
    private static final String MANAGER_WORKERS_Q = "ManagerWorkersQueue.fifo";
    private static final String WORKERS_SM_Q_BASENAME = "WorkersToSM";



    // --------------------- S3 -----------------------------
    /**
     * Uploads a file to the specified S3 bucket.
     *
     * @param bucketName Name of the S3 bucket.
     * @param key        Key of the object in the bucket.
     * @param data       Data to upload.
     * @return The URL of the uploaded object.
     */
    public static String uploadToS3(String key, byte[] data) throws Exception {
        try {
            s3.putObject(
                PutObjectRequest.builder()
                    .bucket(BUCKET_NAME)
                    .key(key)
                    .build(),
                RequestBody.fromBytes(data)
            );
            return "https://" + BUCKET_NAME + ".s3.amazonaws.com/" + key;
        } catch (Exception e) {
            throw new Exception("Failed to upload to S3: " + e.getMessage());
        }
    }

    // --------------------- SQS -----------------------------

    /**
     * Sends a message to the specified SQS queue.
     *
     * @param messageBody Message to send.
     */
    public static void sendMessageToSM(String messageBody, int clientId) throws Exception {
        try {
            // Get the queue's URL
            GetQueueUrlRequest getQueueUrlRequest = GetQueueUrlRequest.builder()
                .queueName(WORKERS_SM_Q_BASENAME + clientId + ".fifo")
                .build();
            String queueUrl = sqs.getQueueUrl(getQueueUrlRequest).queueUrl();
            // send the message
            SendMessageRequest sendMessageRequest = SendMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .messageBody(messageBody)
                    .messageGroupId("1")
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
    public static String receiveMessageFromManager() throws Exception{
        try {
            // Get the queue's URL
            GetQueueUrlRequest getQueueUrlRequest = GetQueueUrlRequest.builder()
                .queueName(MANAGER_WORKERS_Q)
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
    private static void deleteMessageFromSqs(String queueUrl, String receiptHandle) {
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
