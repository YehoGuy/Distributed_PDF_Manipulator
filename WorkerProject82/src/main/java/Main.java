import org.apache.pdfbox.pdmodel.PDDocument;

public class Main {
    public static void main(String[] args) {
        try {
            while (true) { 
                // Receive a message from the SQS queue
                String message = AwsWorkerService.receiveMessageFromManager();
                String[] splits = message.split("\t");
                if(splits.length != 3) {
                    System.out.println("[ERROR] Invalid message format: " + message);
                    continue;
                }
                String op = splits[0];
                String URL = splits[1];
                int clientId = Integer.parseInt(splits[2]);
                String fileName = URL.substring(URL.lastIndexOf('/') + 1);  
                System.out.println("[DEBUG] Working on message: " + message);
                switch(op){
                    case "toImage":
                        try {
                            PDDocument document = PDFConverter.loadPDF(URL);
                            byte[] imageData = PDFConverter.ToImage(document);
                            String resultAdress = AwsWorkerService.uploadToS3("client"+clientId+"/"+fileName+".png", imageData);
                            String resultMessageBody = op + "\t" + URL + "\t" + resultAdress;
                            sendResultToSM(resultMessageBody, clientId);
                        } catch (Exception e) {
                            sendResultToSM("Image conversion failed: " + e.getMessage(), clientId);
                        }
                        break;
                    case "toHTML":
                        try {
                            PDDocument document = PDFConverter.loadPDF(URL);
                            byte[] htmlData = PDFConverter.ToHTML(document);
                            String resultAdress = AwsWorkerService.uploadToS3("client"+clientId+"/"+fileName+".html", htmlData);
                            String resultMessageBody = op + "\t" + URL + "\t" + resultAdress;
                            sendResultToSM(resultMessageBody, clientId);
                        } catch (Exception e) {
                            sendResultToSM("HTML conversion failed: " + e.getMessage(), clientId);
                        }
                        break;
                    case "toText":
                        try {
                            PDDocument document = PDFConverter.loadPDF(URL);
                            byte[] textData = PDFConverter.ToText(document);
                            String resultAdress = AwsWorkerService.uploadToS3("client"+clientId+"/"+fileName+".txt", textData);
                            String resultMessageBody = op + "\t" + URL + "\t" + resultAdress;
                            sendResultToSM(resultMessageBody, clientId);
                        } catch (Exception e) {
                            sendResultToSM("Text conversion failed: " + e.getMessage(), clientId);
                        }
                        break;
                }
                
                
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
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
