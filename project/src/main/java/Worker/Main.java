package Worker;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

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

    private static void generateHtmlFile(String outputFilePath, List<String> results) throws Exception {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFilePath))) {
            writer.write("<html><body>");
            writer.write("<h1>PDF Processing Results</h1>");
            writer.write("<ul>"); //unordered list
            for (String result : results) {
                writer.write("<li>" + result + "</li>"); //list item
            }
            writer.write("</ul>");
            writer.write("</body></html>");

            System.out.println("Output HTML file generated: " + outputFilePath);
        } catch (IOException e) {
            throw new Exception("Worker Error generating HTML file: " + e.getMessage());
        }
    }
}
