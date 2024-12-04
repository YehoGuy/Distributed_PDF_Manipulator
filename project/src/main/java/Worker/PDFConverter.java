package Worker;

import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.rendering.PDFRenderer;
import org.apache.pdfbox.text.PDFTextStripper;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.*;
import java.net.URL;

public class PDFConverter {


    public static PDDocument loadPDF(String pdfUrl) {
        try (InputStream in = new URL(pdfUrl).openStream()) {
            return PDDocument.load(in);
        } catch (IOException e) {
            System.err.println("Failed to load PDF from URL: " + e.getMessage());
            return null;
        }
    }

    public static void ToImage(PDDocument document, String outputPath) {
        try {
            PDFRenderer renderer = new PDFRenderer(document);
            BufferedImage image = renderer.renderImage(0); // Render the first page (index 0)

            File outputFile = new File(outputPath);
            ImageIO.write(image, "png", outputFile);

            System.out.println("First page converted to image: " + outputPath);
        } catch (IOException e) {
            System.err.println("Failed to convert PDF to image: " + e.getMessage());
        }
    }

    public static void ToHTML(PDDocument document, String outputPath) {
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
            System.err.println("Failed to convert PDF to HTML: " + e.getMessage());
        }
    }

    public static void ToText(PDDocument document, String outputPath) {
        try (Writer writer = new FileWriter(outputPath)) {
            PDFTextStripper stripper = new PDFTextStripper();
            stripper.setStartPage(1); // Set to first page
            stripper.setEndPage(1);

            String text = stripper.getText(document);
            writer.write(text);

            System.out.println("First page converted to Text: " + outputPath);
        } catch (IOException e) {
            System.err.println("Failed to convert PDF to Text: " + e.getMessage());
        }
    }

    public static PDDocument loadLocalPDF(String filePath) {
        try {
            File file = new File(filePath);
            if (!file.exists()) {
                System.err.println("File not found: " + filePath);
                return null;
            }
            return PDDocument.load(file);
        } catch (IOException e) {
            System.err.println("Failed to load local PDF: " + e.getMessage());
            return null;
        }
    }
}
