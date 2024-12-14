package Worker;

import java.awt.image.BufferedImage;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.Writer;
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

    public static void ToImage(PDDocument document, String outputPath) throws Exception {
        try {
            PDFRenderer renderer = new PDFRenderer(document);
            BufferedImage image = renderer.renderImage(0); // Render the first page (index 0)

            File outputFile = new File(outputPath);
            ImageIO.write(image, "png", outputFile);

            System.out.println("First page converted to image: " + outputPath);
        } catch (IOException e) {
            throw new Exception("Failed to convert PDF to image: " + e.getMessage());
        }
    }

    public static void ToHTML(PDDocument document, String outputPath) throws Exception {
        try (Writer writer = new FileWriter(outputPath)) {
            PDFTextStripper stripper = new PDFTextStripper();
            stripper.setStartPage(1); // Set to first page
            stripper.setEndPage(1);

            String text = stripper.getText(document);

            writer.write("<html><body>");
            writer.write("<pre>" + text + "</pre>");
            writer.write("</body></html>");

            System.out.println("First page converted to HTML: " + outputPath);
        } catch (IOException e) {
            throw new Exception("Failed to convert PDF to HTML: " + e.getMessage());
        }
    }

    public static void ToText(PDDocument document, String outputPath) throws Exception {
        try (Writer writer = new FileWriter(outputPath)) {
            PDFTextStripper stripper = new PDFTextStripper();
            stripper.setStartPage(1); // Set to first page
            stripper.setEndPage(1);

            String text = stripper.getText(document);
            writer.write(text);

            System.out.println("First page converted to Text: " + outputPath);
        } catch (IOException e) {
            throw new Exception("Failed to convert PDF to text: " + e.getMessage());
        }
    }

    public static PDDocument loadLocalPDF(String filePath) throws Exception {
        try {
            File file = new File(filePath);
            if (!file.exists()) {
                System.err.println("File not found: " + filePath);
                return null;
            }
            return PDDocument.load(file);
        } catch (IOException e) {
            throw new Exception("Failed to load PDF from file: " + e.getMessage());
        }
    }
}
