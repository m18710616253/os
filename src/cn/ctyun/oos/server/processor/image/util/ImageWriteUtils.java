/**
 * 
 */
package cn.ctyun.oos.server.processor.image.util;

import java.awt.image.BufferedImage;
import java.awt.image.RenderedImage;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Iterator;

import javax.imageio.IIOImage;
import javax.imageio.ImageIO;
import javax.imageio.ImageTypeSpecifier;
import javax.imageio.ImageWriteParam;
import javax.imageio.ImageWriter;
import javax.imageio.metadata.IIOMetadata;
import javax.imageio.stream.ImageOutputStream;

import org.w3c.dom.Node;

import cn.ctyun.oos.server.processor.image.ImageEvent.ImageDetail;

/**
 * @author zhaowentao
 *
 */
public class ImageWriteUtils {
    public static void write(ImageDetail imageDetail, OutputStream os)
            throws IOException {

        if (ImageFormat.GIF == imageDetail.format) {
            writeGif(imageDetail, os);

        } else if (ImageFormat.JPEG == imageDetail.format) {
            writeJPEG(imageDetail, os);
        } else if (ImageFormat.PNG == imageDetail.format
                || ImageFormat.BMP == imageDetail.format
                || ImageFormat.WEBP == imageDetail.format) {
            writeDefault(imageDetail, os);
        } else {
            throw new IOException(
                    "Unsupported output format, only JPEG, BMP, GIF, PNG and WEBP are ok");
        }
    }

    private static void writeJPEG(ImageDetail imageDetail, OutputStream os)
            throws IOException {
        // TODO Auto-generated method stub
        writeDefault(imageDetail, os);
    }

    private static void writeDefault(ImageDetail imageDetail, OutputStream os)
            throws IOException {
        // TODO Auto-generated method stub
        ImageOutputStream imageOut = null;
        ImageWriter writer = null;
        BufferedImage image = imageDetail.bufferedImages[0];
        try {
            imageOut = ImageIO.createImageOutputStream(os);
            //System.out.println("writers:"+Arrays.toString(ImageIO.getWriterFileSuffixes()));
            String format =  imageDetail.format.getDesc();
           
            Iterator<ImageWriter> writers = ImageIO
                    .getImageWritersByFormatName(format);
            if (writers.hasNext()) {
                writer = writers.next();
            }
            if (writer == null) {
                throw new IllegalStateException(String.format("No %S writer matched",format));
            }
            writer.setOutput(imageOut);
            ImageWriteParam param = writer.getDefaultWriteParam();
            if (imageDetail.quality > 0 && param.canWriteCompressed() ) {
                param.setCompressionMode(ImageWriteParam.MODE_EXPLICIT);
                param.setCompressionQuality(imageDetail.quality / 100f);
            }
            writer.write(null, new IIOImage(image, null, null), param);
            imageOut.flush();
        } catch (IOException e) {
            throw e;
        } finally {
            if (writer != null) {
                writer.dispose();
            }
            ImageUtils.closeQuietly(imageOut);

        }
    }

    private static void writeGif(ImageDetail imageDetail, OutputStream os)
            throws IOException {
        // TODO Auto-generated method stub
        ImageOutputStream imageOut = null;
        ImageWriter writer = null;

        BufferedImage[] images = imageDetail.bufferedImages;
        Node[] metadatas = imageDetail.gifMetaDatas;

        if (metadatas == null || imageDetail.gifMetaStream == null) {
            try {
                imageOut = ImageIO.createImageOutputStream(os);
                RenderedImage img = images[0];
//                if (IndexImageBuilder.needConvertToIndex(images[0])) {
//                    img = IndexImageBuilder.createIndexedImage(images[0],
//                            QuantAlgorithm.OctTree);
//                }
                ImageIO.write(img, "GIF", imageOut);
            } catch (IOException e) {
                throw e;
            } finally {
                ImageUtils.closeQuietly(imageOut);
            }

            return;
        }

        try {
            imageOut = ImageIO.createImageOutputStream(os);

            Iterator<ImageWriter> writers = ImageIO
                    .getImageWritersByFormatName("GIF");
            while (writers.hasNext()) {
                writer = writers.next();
                if (writer.canWriteSequence()) {
                    break;
                }
            }

            if (writer == null || !writer.canWriteSequence()) {
                throw new IllegalStateException("No GIF writer matched");
            }

            writer.setOutput(imageOut);

            ImageWriteParam param = writer.getDefaultWriteParam();

            IIOMetadata streamMeta = writer.getDefaultStreamMetadata(param);
            // merge stream metadata
            streamMeta.mergeTree(ImageDetail.GIF_STREAM_METADATA_NAME,
                    imageDetail.gifMetaStream);
            writer.prepareWriteSequence(streamMeta);
            for (int i = 0; i < images.length; i++) {
                ImageTypeSpecifier imageType = new ImageTypeSpecifier(
                        images[i].getColorModel(), images[i].getSampleModel());
                RenderedImage renderedImg = images[i];
//                if (IndexImageBuilder.needConvertToIndex(renderedImg)) {
//                    NodeUtils.removeChild(metadatas[i], "LocalColorTable");
//                    renderedImg = IndexImageBuilder.createIndexedImage(
//                            renderedImg, QuantAlgorithm.OctTree);
//                }
                IIOMetadata meta = writer.getDefaultImageMetadata(imageType,
                        param);
                meta.mergeTree(ImageDetail.GIF_IMAGE_METADATA_NAME,
                        metadatas[i]);

                IIOImage img = new IIOImage(renderedImg, null, meta);
                writer.writeToSequence(img, param);
            }
            writer.endWriteSequence();

            imageOut.flush();
        } catch (IOException e) {
            throw e;
        } finally {
            ImageUtils.closeQuietly(imageOut);
            if (writer != null) {
                writer.dispose();
            }
            

        }
    }

}
