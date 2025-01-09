import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import javax.imageio.ImageIO;

import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.rendering.PDFRenderer;
import org.apache.pdfbox.text.PDFTextStripper;

public class PDFConverter {


    public static PDDocument loadPDF(String pdfUrl) throws Exception {
        try (InputStream in = URI.create(pdfUrl).toURL().openStream()) {
            return PDDocument.load(in);
        } catch (IOException e) {
            throw new Exception("Failed to load PDF from URL: " + e.getMessage());
        }
    }

    // in memory
    public static byte[] ToImage(PDDocument document) throws Exception {
        try {
            PDFRenderer renderer = new PDFRenderer(document);
            BufferedImage image = renderer.renderImage(0); // Render the first page
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            ImageIO.write(image, "png", outputStream); // Write the image to a byte stream
            byte[] imageData = outputStream.toByteArray(); // to byte array
            return imageData;
        } catch (IOException e) {
            throw new Exception("Failed to convert PDF to image: " + e.getMessage());
        }
    }

    // in memory
    public static byte[] ToHTML(PDDocument document) throws Exception {
        try {
            PDFTextStripper stripper = new PDFTextStripper();
            stripper.setStartPage(1); // Set to first page
            stripper.setEndPage(1);
            String text = stripper.getText(document);
            String htmlContent = "<html><body><pre>" + text + "</pre></body></html>";
            byte[] htmlData = htmlContent.getBytes();
            return htmlData;
        } catch (IOException e) {
            throw new Exception("Failed to convert PDF to HTML: " + e.getMessage());
        }
    }
    
    // in memory
    public static byte[] ToText(PDDocument document) throws Exception {
        try {
            PDFTextStripper stripper = new PDFTextStripper();
            stripper.setStartPage(1); // Set to first page
            stripper.setEndPage(1);
            String text = stripper.getText(document);
            byte[] textData = text.getBytes();
            return textData;
        } catch (IOException e) {
            throw new Exception("Failed to convert PDF to text: " + e.getMessage());
        }
    }
    
    

    
}
