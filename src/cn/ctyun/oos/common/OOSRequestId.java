package cn.ctyun.oos.common;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Random;
import java.util.UUID;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import common.util.HexUtils;
/**
 * 
 * @author xiaojun
 * 生成和解析API RequestId
 * date 2019.10.25
 */
public class OOSRequestId {
    private static final Log log = LogFactory.getLog(OOSRequest.class);
    private static String host = null;
  
    /**
     * 取一个255内的随机数，目的是对相同的host串码不同。 
     * 对原始字符串的二进制byte，逐个ASCII码值进行串码
     * 每处理一位，串码加一乘以2，目的使得同样的字符串码不一样 
     * @param src
     * @return hostname编码后的16进制字符串
     */
    private static String encode(String src) {
        Random ra = new Random();
        int code = ra.nextInt(255);
        if (src == null)
            src = "";
        byte[] buf = src.getBytes();
        byte[] bufResult = new byte[buf.length + 1];
        bufResult[0] = (byte) code;
        for (int i = 0; i < buf.length; i++) {
            bufResult[i + 1] = (byte) (buf[i] + bufResult[0] + i*2);
        }
        return HexUtils.toHexString(bufResult);
    }
    /**
     *  解码hostname，解码方式为编码的逆过程
     * @param src
     * @return
     */
    private static String decode(String src) {
        byte[] buf = HexUtils.toByteArray(src);
        byte[] bufResult = new byte[buf.length - 1];
        for (int i = 0; i < buf.length - 1; i++) {
            bufResult[i] = (byte) (buf[i + 1] - buf[0] - i*2);
        }
        return new String(bufResult);
    }
    /**
     *  生成requestid 生成规则 uuid(16位) + encode(host)（34位或以上）
     * @param 无
     * @return requestid 
     */
    public static String generateRequestId() {
        try {
            if (null == host) {
                host = InetAddress.getLocalHost().getHostName();
            }
        } catch (UnknownHostException e) {
            log.error("get host name fail : " + e.getMessage(), e);
            host = "";
        }
        String uuid = Long .toHexString(UUID.randomUUID().getMostSignificantBits());
        if (host.length() < 16) {
            host = StringUtils.rightPad(host, 16, " ");
        }
        return uuid + encode(host);
    }
    /**
    *  根据requestid解析出原始的hostname
    * @param 无
    * @return hostname
    */
    public static String parseRequestId(String reqId) {
        if(null == reqId || reqId.length() < 50)
            return "";
        return decode(reqId.substring(16)).trim();
    }
}
