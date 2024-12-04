package Worker;
import org.apache.pdfbox.pdmodel.PDDocument;

public class Main {
    public static void main(String[] args) {
        System.out.println("Current working directory: " + System.getProperty("user.dir"));
        try {
            String pdfUrl = "input/Assignment1.pdf"; // Replace with your PDF URL
            String outputDirectory = "output/";

            // Fetch the PDF from the URL
            PDDocument document = PDFConverter.loadLocalPDF(pdfUrl);

            // Ensure the document is loaded
            if (document != null) {
                // Convert to PNG image
                PDFConverter.ToImage(document, outputDirectory + "page1.png");

                // Convert to HTML
                PDFConverter.ToHTML(document, outputDirectory + "page1.html");

                // Convert to Text
                PDFConverter.ToText(document, outputDirectory + "page1.txt");

                // Close the document
                document.close();
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }
}
