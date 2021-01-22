package cn.ctyun.oos.server.formpost.exceptions;

import java.io.IOException;
import java.math.BigDecimal;
public class MetadataTooLargeException extends IOException{
    /**
     * 
     */
    private static final long serialVersionUID = -7428135361555475190L;
    public long maxSizeAllowed = 0;
    public String code = "";
    public String message = "";
    public MetadataTooLargeException(String code,String message,long maxSizeAllowed) {
        this.maxSizeAllowed = maxSizeAllowed;
        this.message = message;
        this.code = code;
    }
//    public static String getFileLength(long length) {
//        float lengthFloat = 0;
//        float k = 1024.00f;
//        String result = "0";
//
//        if (length > 1024 * 1024) {
//            lengthFloat = length / (1048576.00f);
//            BigDecimal b = new BigDecimal(lengthFloat);
//            lengthFloat = b.setScale(2, BigDecimal.ROUND_HALF_UP).floatValue();
//            result = lengthFloat + "MB";
//            // result = round(lengthFloat, 2, BigDecimal.ROUND_UP) + "MB";
//        } else if (length > 1024) {
//            lengthFloat = length / k;
//            BigDecimal b = new BigDecimal(lengthFloat);
//            lengthFloat = b.setScale(2, BigDecimal.ROUND_HALF_UP).floatValue();
//            result = lengthFloat + "KB";
//        } else {
//            result = length + "B";
//        }
//        return result;
//    }

}