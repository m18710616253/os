package cn.ctyun.oos.server;

import java.security.SecureRandom;
import java.util.HashMap;

import javax.servlet.http.HttpServletRequest;

import com.amazonaws.services.s3.model.BucketVersioningConfiguration;

import cn.ctyun.oos.common.Utils;
import cn.ctyun.oos.metadata.BucketMeta;
import cn.ctyun.oos.metadata.ObjectMeta;
import common.tuple.Pair;

/**
 * 需结合hbase的多版本特性实现OOS的多版本功能，OOS的版本ID跟根据时间戳生成的随机字符串，时间戳与OOS的版本ID是互逆的，时间戳为nanoTime
 * @author dongck
 *
 */
public class MultiVersionsController {
    /**
     * 生成版本ID<Long, String>，包括版本的Long型和String型
     * @return
     */
    public static Pair<Long, String> generateVersoin() {
        long nanoTime = Utils.getNanoTime();
        String v = toStringVersion(nanoTime);
        return new Pair<>(nanoTime, v);
    }
    
    private static long getLongVersion(String s) {
        s.substring(0, s.length() - 4);
        long v = 0;
        for(int i = s.length() - 1; i >= 0; i--) {
            int index = indexs.get(s.charAt(i));
            v <<= 6;
            v |= index;
        }
        long base = (v >>> Long.SIZE - Byte.SIZE) & 0xFF;
        long v2 = 0;
        for(int i = 0; i < Long.BYTES - 1; i++) {
            v2 <<= Byte.SIZE;
            v2 |= (base ^ (v & 0xFF));
            v >>>= Byte.SIZE;
        }
        v2 <<= Byte.SIZE;
        v2 |= base;
        return v2;
    }
    
    public static void parseVersionInfo(ObjectMeta objMeta, HttpServletRequest req) throws Exception{
        String gwVersion = req.getHeader("x-ctyun-gw-version");
        // S3的get对象接口多版本参数加在URL query
        String version = req.getParameter("versionId");
        long lVersion = -1;
        String sVersion = null;
        if(gwVersion != null) {
            if (!gwVersion.contains(",")) {
                lVersion = Long.valueOf(gwVersion);
                sVersion = gwVersion;
                objMeta.timestamp = lVersion;
                objMeta.version = sVersion;
            } else {
                sVersion = gwVersion;
                objMeta.version = sVersion;
            }
        } else if(version != null) {
            sVersion = version;
            lVersion = getLongVersion(sVersion);
            objMeta.timestamp = lVersion;
            objMeta.version = sVersion;
        }
    }
    
    /**
     * 只允许网关指定版本ID，其他用户的版本ID内部生成
     * @param objMeta
     * @param req
     */
    public static void setVersionInfo(ObjectMeta objMeta, HttpServletRequest req) {
        String gwVersion = req.getHeader("x-ctyun-gw-version");
        long lVersion = -1;
        String sVersion = null;
        if(gwVersion != null) {
            lVersion = Long.valueOf(gwVersion);
            sVersion = gwVersion;
        } else {
            lVersion = Utils.getNanoTime();
            sVersion = toStringVersion(lVersion);
        }
        objMeta.timestamp = lVersion;
        objMeta.version = sVersion;
    }
    
    /**
     * 网关指定操作类型，其他用户不能指定，默认为EQUAL
     * @param req
     * @return
     * @throws Exception
     */
    public static MultiVersionsOpt getMultiVersionsOpt(HttpServletRequest req) throws Exception{
        String opt = req.getHeader("x-ctyun-version-opt");
        if(opt == null)
            return MultiVersionsOpt.EQUAL;
        else 
            return MultiVersionsOpt.valueOf(opt);
    }
    
    /**
     * 判断是否为网关多版本，（暂不支持兼容S3的多版本，即使bucket已经开启了多版本）
     * @param req
     * @param dbBucket
     * @return
     */
    public static boolean gwMultiVersionEnabled(HttpServletRequest req, BucketMeta dbBucket) {
        String gwVersion = req.getHeader("x-ctyun-gw-version");
        if (dbBucket.versioning != null && dbBucket.versioning.getStatus().equalsIgnoreCase(BucketVersioningConfiguration.ENABLED) && gwVersion != null) {
            return true;
        }
        return false;
    }
    
    public static enum MultiVersionsOpt {
        EQUAL,
        EQUAL_OR_LESS,
        EQUAL_AND_LESS,
        RANGE;
    }
    
    public static void main(String[] args) {
//        System.out.println(MultiVersionsOpt.valueOf("EQUAL"));
        Pair<Long, String> p = generateVersoin();
        System.out.println(p.first());
        System.out.println(p.second());
    }
    
    private static SecureRandom rand = new SecureRandom();
    
    private static String toStringVersion(long v) {
        long v2 = 0;
        long base = v & 0xFF;
        v2 = base;
        v >>>= Byte.SIZE;
        for(int i = 1; i < Long.BYTES; i++) {
            v2 <<= Byte.SIZE;
            v2 |= (v & 0xFF) ^ base;
            v >>>= Byte.SIZE;
        }
        StringBuilder sb = new StringBuilder();
        for(int i = 0; i < (Long.SIZE / 6 + 1); i++) {
            int index = (int)(v2 & 63);
            sb.append(digits[index]);
            v2 >>>= 6;
        }
        for(int i=0;i<5;i++)
            sb.append(digits[rand.nextInt(63)]);
        return sb.toString();
    }
    
    final static char[] digits = {
            '0' , '1' , '2' , '3' , '4' , '5' ,
            '6' , '7' , '8' , '9' , 'a' , 'b' ,
            'c' , 'd' , 'e' , 'f' , 'g' , 'h' ,
            'i' , 'j' , 'k' , 'l' , 'm' , 'n' ,
            'o' , 'p' , 'q' , 'r' , 's' , 't' ,
            'u' , 'v' , 'w' , 'x' , 'y' , 'z' ,
            'A' , 'B' , 'C' , 'D' , 'E' , 'F' , 
            'G' , 'H' , 'I' , 'J' , 'K' , 'L' ,
            'M' , 'N' , 'O' , 'P' , 'Q' , 'R' ,
            'S' , 'T' , 'U' , 'V' , 'W' , 'X' ,
            'Y' , 'Z' , '.' , '_'
        };
    final static HashMap<Character, Integer> indexs = new HashMap<>();
    static {
        for(int i=0;i<digits.length;i++) {
            indexs.put(digits[i], i);
        }
    }
}
