/**
 * FileName Pic2Pdf.java
 * Author Zorance
 * Date 2023/7/1 11:26
 */

package com.zorance;

import java.io.File;
import java.io.FileInputStream;
import java.util.Iterator;
import java.util.UUID;

import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.pdmodel.PDPageContentStream;
import org.apache.pdfbox.pdmodel.common.PDRectangle;
import org.apache.pdfbox.pdmodel.graphics.image.PDImageXObject;
import org.springframework.web.multipart.MultipartFile;

import javax.imageio.ImageIO;
import javax.imageio.ImageReader;
import javax.imageio.stream.ImageInputStream;

/**
 * @author DengYuZhu
 * @version 2.0
 * @description
 * @date 2023/7/1 11:26
 */
public class Pic2Pdf {

    public static void main(String[] args) throws Exception {
        testPdf();
        System.out.println("end.");
    }

    public static void testPdf() throws Exception{

        boolean is = false;

        File file = new File("C:\\Users\\Administrator\\Desktop\\test\\a.jpg");

        FileInputStream fileInputStream = new FileInputStream(file);

        MultipartFile multipartFile = new MockMultipartFile("copy"+file.getName(), file.getName(), "application/octet-stream", fileInputStream);

        try {
            PDDocument document = new PDDocument();

       /* PDPage page = new PDPage();
        PDRectangle rectangle = new PDRectangle(468, 450);
        page.setMediaBox(rectangle);*/
//这里是设置pdf的大小,大小固定,所有不推荐,下面是根据图片大小动态生成pdf大小

            String filename = multipartFile.getOriginalFilename();
            String fileSuffix = filename.substring(filename.lastIndexOf(".") + 1);

            Iterator readers = ImageIO.getImageReadersByFormatName(fileSuffix);
            ImageReader reader = (ImageReader) readers.next();
            ImageInputStream input = ImageIO.createImageInputStream(multipartFile.getInputStream());
            reader.setInput(input, true);

            int width = reader.getWidth(0)+300;
            int height = reader.getHeight(0)+500;
            PDPage pdPage = new PDPage(new PDRectangle(width, height));
//根据图片大小动态生成pdf大小

            document.addPage(pdPage);
            PDImageXObject pdImageXObject = PDImageXObject.createFromByteArray(document, multipartFile.getBytes(), "构建图片错误");

            PDPageContentStream contentStream = new PDPageContentStream(document, pdPage);
            //写入图片
            contentStream.drawImage(pdImageXObject, 0, 0);
            contentStream.close();

            document.save("C:\\Users\\Administrator\\Desktop\\test\\" + UUID.randomUUID().toString() +  " .pdf");

//pdf输出
            document.close();

            is = true;
        } catch (Exception e) {
            is = false;
        }

        System.out.println(is);

    }

}
