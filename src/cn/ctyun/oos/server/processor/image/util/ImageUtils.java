package cn.ctyun.oos.server.processor.image.util;

import java.io.IOException;

import javax.imageio.ImageReader;
import javax.imageio.stream.ImageInputStream;
import javax.imageio.stream.ImageOutputStream;


public final class ImageUtils {


    private ImageUtils(){

    }

    public static void closeQuietly(ImageInputStream inStream) {
        if (inStream != null) {
            try {
                inStream.close();
            } catch (IOException ignore) {

            }
        }
    }

    public static void closeQuietly(ImageOutputStream outStream) {
        if (outStream != null) {
            try {
                outStream.close();
            } catch (IOException ignore) {

            }
        }
    }


    public static void closeQuietly(ImageReader reader) {
        if (reader != null) {
            try {
                reader.dispose();
            } catch (Exception igonre) {

            }
        }
    }

}
