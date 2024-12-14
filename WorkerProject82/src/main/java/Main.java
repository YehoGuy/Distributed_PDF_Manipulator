import org.apache.pdfbox.pdmodel.PDDocument;

public class Main {
    private static int cId = -1;
    public static void main(String[] args) {
            while (true) { 
                try{
                // Receive a message from the SQS queue
                String message = AwsWorkerService.receiveMessageFromManager();
                if (message == null) 
                    continue;
                String[] splits = message.split("\t");
                if(splits.length != 3) {
                    throw new Exception("[ERROR] Invalid message format: " + message);
                }
                String op = splits[0];
                String URL = splits[1];
                int clientId = Integer.parseInt(splits[2]);
                cId = clientId;
                String fileName = URL.substring(URL.lastIndexOf('/') + 1);  
                System.out.println("[DEBUG] Working on message: " + message);
                switch(op){
                    case "ToImage":
                        try {
                            PDDocument document = PDFConverter.loadPDF(URL);
                            byte[] imageData = PDFConverter.ToImage(document);
                            String resultAdress = AwsWorkerService.uploadToS3("client"+clientId+"/"+fileName+".png", imageData);
                            String resultMessageBody = op + "\t" + URL + "\t" + resultAdress;
                            sendResultToSM(resultMessageBody, clientId);
                            System.out.println("[DEBUG] Image conversion successful for: " + fileName + ". sent to SM");
                        } catch (Exception e) {
                            sendResultToSM("Image conversion failed for file "+fileName+": " + e.getMessage(), clientId);
                            System.out.println("[ERROR] Image conversion failed for: " + fileName + ". error sent to SM");
                        }
                        break;
                    case "ToHTML":
                        try {
                            PDDocument document = PDFConverter.loadPDF(URL);
                            byte[] htmlData = PDFConverter.ToHTML(document);
                            String resultAdress = AwsWorkerService.uploadToS3("client"+clientId+"/"+fileName+".html", htmlData);
                            String resultMessageBody = op + "\t" + URL + "\t" + resultAdress;
                            sendResultToSM(resultMessageBody, clientId);
                            System.out.println("[DEBUG] HTML conversion successful for: " + fileName + ". sent to SM");
                        } catch (Exception e) {
                            sendResultToSM("HTML conversion failed for: "+fileName+" " + e.getMessage(), clientId);
                            System.out.println("[ERROR] HTML conversion failed for: " + fileName + ". error sent to SM");
                        }
                        break;
                    case "ToText":
                        try {
                            PDDocument document = PDFConverter.loadPDF(URL);
                            byte[] textData = PDFConverter.ToText(document);
                            String resultAdress = AwsWorkerService.uploadToS3("client"+clientId+"/"+fileName+".txt", textData);
                            String resultMessageBody = op + "\t" + URL + "\t" + resultAdress;
                            sendResultToSM(resultMessageBody, clientId);
                            System.out.println("[DEBUG] Text conversion successful for: " + fileName + ". sent to SM");
                        } catch (Exception e) {
                            sendResultToSM("Text conversion failed for: "+fileName+" " + e.getMessage(), clientId);
                            System.out.println("[ERROR] Text conversion failed for: " + fileName + ". error sent to SM");
                        }
                        break;
                    default:
                        sendResultToSM("unknown operation for file "+fileName+" op: "+op, clientId);
                        throw new Exception("unknown operation "+op);
                    }
                } catch (Exception e) {
                    System.out.println("[SUPERBAD ERROR] " + e.getMessage());
                    sendResultToSM("error processing file: "+e.getMessage(), cId);
                }   
        } 
    }
        
    

    public static void sendResultToSM(String message, int clientId) {
        try {
            AwsWorkerService.sendMessageToSM(message, clientId);
        } catch (Exception e) {
            System.out.println("[ERROR] failed to send result back to SubManager: "+e.getMessage());
        }
    }

    

    
}
