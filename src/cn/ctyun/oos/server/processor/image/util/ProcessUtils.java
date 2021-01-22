package cn.ctyun.oos.server.processor.image.util;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.net.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;

public class ProcessUtils {

    private static final List<String> IMAGE_OPES = Arrays.asList("w", "h", "e", "c",
            "l", "p", "P", "a", "q", "Q", "wh");
    private static final List<String> WATER_MARK_OPES = Arrays.asList("t", "p", "x",
            "y", "voffset", "object", "bucket", "p", "P", "w", "h", "type", "color",
            "size", "s");
    /**
     * 基于base64安全编码
     * @param source
     * @return
     */
    public static String base64Encode(String source){
        
        if(StringUtils.isBlank(source))
            return null;
        String str = Base64.encodeBase64URLSafeString(Bytes.toBytes(source));
        while(str.lastIndexOf("=") == str.length() - 1)
            str = str.substring(0, str.length() - 1);
        return str;
    }
    
    /**
     * 基于base64解码
     * @param source
     * @return
     */
    public static String base64Decode(String source){
        
        if(StringUtils.isBlank(source))
            return null;
        byte[] str = Base64.decodeBase64(source);
        return Bytes.toString(str);
    }
    
    /**
     * 获取图像处理可用操作
     * @return
     */
    public static List<String> getImageOpts(){
        return IMAGE_OPES;
    }
    
    /**
     * 获取水印处理的可用操作
     * @return
     */
    public static List<String> getWaterMarkOpts(){
        return WATER_MARK_OPES;
    }
    
    /**
     * 获取图片正则表达式
     * @return
     */
    public static String getImageRegex(){
        
        return "(([0-9]*|infoexif|([0-9]*-){3}[0-9]*)[a-zA-Z]+_?)*(\\.([a-zA-Z]+))?";
    }
    
    public static void main(String[] args) {
        
    }
}
