package cn.ctyun.oos.server.log;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.net.URLEncoder;
import java.util.Date;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.Header;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpPut;

import com.amazonaws.auth.SigningAlgorithm;
import com.amazonaws.services.s3.internal.RestUtils;

import cn.ctyun.common.BaseException;
import cn.ctyun.common.Consts;
import cn.ctyun.common.utils.HostUtils;
import cn.ctyun.oos.common.OOSRequest;
import cn.ctyun.oos.common.Utils;
import cn.ctyun.oos.hbase.MetaClient;
import cn.ctyun.oos.metadata.AkSkMeta;
import cn.ctyun.oos.metadata.BucketMeta;
import cn.ctyun.oos.metadata.ObjectMeta;
import cn.ctyun.oos.server.conf.LogConfig;
import common.io.StreamUtils;
import common.time.TimeUtils;
import common.util.HexUtils;


public class Util {
    public static String srcLogNameSep = "-";   // fecth目录time文件夹分隔符
    public static String tmpFileNameSep = "|";  // 上传bucket临时目录路径文件分隔符
    private static Log log = LogFactory.getLog(Util.class);
    public enum ePutObjectResult{ 
        SUCCESS,        // 发送成功
        ERR_PARAM,  // 发送所需参数错误，不可重发
        ERR_SEND      // 发送失败
        };
    /** makeDir控制是否创建目录, makeDir为false，返回值可以是一个文件，为true，返回值是已创建好的目录*/
    public static File mkDstDir(String rootdir, boolean makeDir, String... dirs) {
        //rootLogDir的一级目录
        File rootLogDir = new File(rootdir);
        if(makeDir && !rootLogDir.exists()){
            rootLogDir.mkdirs();
        }

        for (String dir : dirs) {
            File newFile = new File(rootLogDir, dir);
            if (makeDir) {
                newFile.mkdirs();
            }
            rootLogDir = newFile;
        }

        return rootLogDir;
    }

    /**
     * 生成完成的目录树（包括空目录）,压缩文件中存放相对目录 压缩过程在ZipOutpuStream中放一个ZipEntry(代表压缩项)，再放其数据.
     * 
     * @param src
     * @param zos
     * @param prefix 压缩项的名字前缀
     * @throws IOException
     */
    public static void zipFile(File src, ZipOutputStream zos, String prefix)
            throws IOException {
        ZipEntry zipEntry;
        if (src.isDirectory()) {
            File[] files = src.listFiles();
            if (files.length == 0) {
                //压缩空目录
                zipEntry = new ZipEntry(prefix + src.getName() + "/");
                zos.putNextEntry(zipEntry);
                zos.closeEntry();
            } else {
                for (File file : files) {
                    zipFile(file, zos, prefix + src.getName() + File.separator);
                }
            }
        } else if (src.isFile()) {
            zipEntry = new ZipEntry(prefix + src.getName());
            zos.putNextEntry(zipEntry);
            FileInputStream fin = new FileInputStream(src);
            try {
                byte[] buf = new byte[1024];
                int r;
                while ((r = fin.read(buf)) != -1) {
                    zos.write(buf, 0, r);
                }
            } finally {
                fin.close();
            }
            zos.closeEntry();
        }
    }

    /** 递归删除一个目录 */
    public static boolean deleteFile(File file) {
        if (file == null || !file.exists()) {
            return false;
        }
        if (file.isFile()) {
            return file.delete();
        } else if (file.isDirectory()) {
            //删除目录下文件与目录
            for (File x : file.listFiles()) {
                if (!deleteFile(x)) {
                    return false;
                }
            }
            //删除该目录
            return file.delete();
        }
        return true;
    }
    
    public static String getPwd(String hexStr) {
        if (hexStr == null)
            return null;
        byte[] buf = HexUtils.toByteArray(hexStr);
        for (int i = 0; i < buf.length / 2; i++) {
            byte tmp = buf[i];
            buf[i] = buf[buf.length - 1 - i];
            buf[buf.length - 1 - i] = tmp;
        }
        return new String(buf, Consts.CS_UTF8);
    }
    
    public static String setPwd(String pwd) {
        if (pwd == null)
            pwd = "";
        byte[] buf = pwd.getBytes(Consts.CS_UTF8);
        for (int i = 0; i < buf.length / 2; i++) {
            byte tmp = buf[i];
            buf[i] = buf[buf.length - 1 - i];
            buf[buf.length - 1 - i] = tmp;
        }
        return HexUtils.toHexString(buf);
    }
    public static String encode(String str) {
        return HexUtils.toHexString(str.getBytes(Consts.CS_UTF8));
    }
    public static String decode(String HexPath) {
        return new String(HexUtils.toByteArray(HexPath), Consts.CS_UTF8);
    }
    /**
     * 发送对象到OOS
     * @param f  待发送文件
     * @param client 访问hbase实例
     * @return ePutObjectResult 见具体说明
     */
    public static ePutObjectResult putObject(File f, MetaClient client ) {
        String[] fileMeta = f.getName().split("\\" + tmpFileNameSep); // owner id | owner name | target bucket | {targetPrefix}UTCyyyy-mm-dd-HH-MM-SS | objectNameSuffix
        if (fileMeta.length <4 || fileMeta.length > 5) {
            log.error("file name illegal file："+ f.getName());
            return ePutObjectResult.ERR_PARAM;
        }
        BucketMeta bucket = new BucketMeta(fileMeta[2]);
        try {
            if (!client.bucketSelect(bucket)) {
                log.error("bucket:" + fileMeta[2] + " doest not exist. file: " + f.getName());
                return ePutObjectResult.ERR_PARAM;
            }
        } catch (IOException e) {
            log.error("IOException when query bucket bucket name："+ fileMeta[2] + " file: " + f.getName(), e);
            return ePutObjectResult.ERR_PARAM;
        }

        InputStream input = null;
        try {
            input = new FileInputStream(f);
            // object name:{targetPrefix}UTCyyyy-MM-dd-HH-mm-ss-{optinal str}_id
            String objetcName = "";
            String id = String.valueOf(LogConfig.getId(HostUtils.getHostName()));
            String firstLine = readOneLine(input);
            String targetBucketPrefix = decode(firstLine.trim());
            String objPrefix = targetBucketPrefix + decode(fileMeta[3]);
            if(fileMeta.length == 5){
                String suffix = fileMeta[4];
                if(suffix.startsWith(FileGenerator.MORE_SEP)) {  // 如果只有切片后缀，去掉多余的-
                    suffix = suffix.replaceAll(FileGenerator.MORE_SEP, "");
                }else {
                    suffix = suffix.replaceAll(FileGenerator.MORE_SEP, "_");
                }
                objetcName = objPrefix + "-" + suffix + "_" + id;
            }else{
                objetcName = objPrefix + "_" + id;
            }
            ObjectMeta object = new ObjectMeta(objetcName, fileMeta[2], bucket.metaLocation);
            object.contentType = "text/plain";
            AkSkMeta asKey = new AkSkMeta();
            asKey.ownerId = Long.parseLong(fileMeta[0]);
            boolean res = client.akskSelectPrimaryKeyByOwnerId(asKey);
            if (!res) {
                log.error("no log user: "+asKey.ownerId + " file: " + f.getName());
                return ePutObjectResult.ERR_PARAM;
            }
            boolean bSend = Util.putObjectToOOS(fileMeta[2], objetcName, asKey.accessKey, asKey.getSecretKey(),
                    input, f.length() - firstLine.length());
            if(!bSend) {
                return ePutObjectResult.ERR_PARAM;
            }
        } catch (FileNotFoundException e) {
            log.error("file " + f.getAbsolutePath() + " not found");
            return ePutObjectResult.ERR_PARAM;
        } catch (Exception e) {
            log.error("Exception when put objetc into oos file: " + f.getName(), e);
            return ePutObjectResult.ERR_SEND;
        } finally {
            if (input != null) {
                try {
                    input.close();
                } catch (Throwable e) {
                    log.error("IOException when close input stream file: " + f.getName(), e);
                }
            }
        }
        return ePutObjectResult.SUCCESS;
    }
    /**
     * 发送对象流到oos
     * @param bucketName  
     * @param objectName
     * @param ak
     * @param sk
     * @param input    // 发送文件流
     * @param length  // 发送内容长度
     * @return  true 发送成功  false 发送失败
     * @throws BaseException  服务器返回200、403、404以外的错误码
     */
    public static boolean putObjectToOOS(String bucketName, String objectName, String ak, String sk,
            InputStream input, long length) throws Exception {
        int port = LogConfig.getOosPort();
        String host = LogConfig.getDomainSuffix()[0];
        String path = "/" + bucketName + "/" + URLEncoder.encode(objectName, Consts.STR_UTF8);
        URL url = new URL("http", host, port, path);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        HttpPut httpPut = new HttpPut();
        httpPut.setURI(URI.create(path));
        String date = TimeUtils.toGMTFormat(new Date());
        String resourcePath = Utils.toResourcePath(bucketName, objectName, false);
        httpPut.addHeader("Date", date);
        httpPut.addHeader("Host", host + ":" + port);
        httpPut.addHeader("User-Agent", "");
        @SuppressWarnings("unchecked")
        String canonicalString = RestUtils.makeS3CanonicalString("PUT", resourcePath,
                new OOSRequest(httpPut), null);
        String signature = Utils.sign(canonicalString, sk, SigningAlgorithm.HmacSHA1);
        String authorization = "AWS " + ak + ":" + signature;
        conn.setRequestProperty("Authorization", authorization);
        for (Header e : httpPut.getAllHeaders()) {
            conn.setRequestProperty(e.getName(), e.getValue());
        }
        conn.setRequestProperty("Connection", "close");
        conn.setRequestMethod("PUT");
        conn.setDoOutput(true);
        conn.setConnectTimeout(30 * 1000);
        conn.setReadTimeout(30 * 1000);
        conn.setFixedLengthStreamingMode(length);
        conn.connect();
        try (OutputStream out = conn.getOutputStream()) {
            if (input != null) {
                StreamUtils.copy(input, out, length);
            }
        }
        int oosCode = conn.getResponseCode();
        //oosCode = 404;
        if (oosCode == 200) {
            log.info("put object to oos success, bucket:" + bucketName + " object:" + objectName);
            return true;
        } else if (oosCode == 403 || oosCode == 404) {
            log.info("put object to oos failed, the error code is: " + oosCode
                    + ", but not put the object again.");
            return false;
        } else {
            log.error("put object to oos error:" + objectName + " " + oosCode);
            throw new BaseException(500, "InternalError");
        }
    }

    public static boolean isObjectExists(String bucketName, String objectName, String ak, String sk)
            throws Exception {
        int port = LogConfig.getOosPort();
        String host = LogConfig.getDomainSuffix()[0];
        String path = "/" + bucketName + "/" + URLEncoder.encode(objectName, Consts.STR_UTF8);
        URL url = new URL("http", host, port, path);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        HttpHead httpHead = new HttpHead();
        httpHead.setURI(URI.create(path));
        String date = TimeUtils.toGMTFormat(new Date());
        String resourcePath = Utils.toResourcePath(bucketName, objectName, false);
        httpHead.addHeader("Date", date);
        httpHead.addHeader("Host", host + ":" + port);
        httpHead.addHeader("User-Agent", "");
        @SuppressWarnings("unchecked")
        String canonicalString = RestUtils.makeS3CanonicalString("HEAD", resourcePath,
                new OOSRequest(httpHead), null);
        String signature = Utils.sign(canonicalString, sk, SigningAlgorithm.HmacSHA1);
        String authorization = "AWS " + ak + ":" + signature;
        conn.setRequestProperty("Authorization", authorization);
        for (Header e : httpHead.getAllHeaders()) {
            conn.setRequestProperty(e.getName(), e.getValue());
        }
        conn.setRequestProperty("Connection", "close");
        conn.setRequestMethod("HEAD");
        conn.setConnectTimeout(30 * 1000);
        conn.setReadTimeout(30 * 1000);
        conn.connect();
        int oosCode = conn.getResponseCode();
        if (oosCode == 200) {
            log.info("object alread exists: " + objectName);
            return true;
        } else
            return false;
    }
    /**
     * 移动文件到指定目录，目录不存在会进行创建
     * @param f  拷贝源
     * @param rootDir   拷贝目的根目录
     * @param newDir   拷贝目的文件名
     * @param newFileName
     */
    public synchronized static void move(File f, String rootDir, String newDir, String newFileName) {
        try {
            // 检查目录不存在创建目录
            File dstDir = new File(rootDir, newDir);
            if(!dstDir.exists()) {
                dstDir =Util.mkDstDir(rootDir, true, newDir);
            }
            if(!dstDir.exists() || !dstDir.isDirectory()){
                log.error("directory " + dstDir.getAbsolutePath() + " is not a directory to place logs");
                return;
            }
            // 拷贝文件到指定目录
            File newFile = new File(dstDir, newFileName);
            if (!f.renameTo(newFile)) {
                log.error("failed to rename " + f.getAbsolutePath() + " to "+ newFile );
            }
        }catch(Exception e) {
            log.error("move err: " + e.getMessage(), e);
        }
    
    }

    /**
     * @param inputStream
     * @return
     */
    private static String readOneLine(InputStream inputStream) {
        String line = "";
        try {
            StringBuilder builder = new StringBuilder();
            byte[] bs = new byte[1024];
            int cc=0;
            while (true) {
                int b = inputStream.read();
                if (b == -1) {
                    throw new IOException("read stream error");
                }
                bs[cc] = (byte) b;
                cc++;
                if (cc == 1024) {
                    builder.append(new String(bs));
                    cc = 0;
                    bs = new byte[1024];
                }
                if (b == '\n') {
                    builder.append(new String(bs,0, cc));
                    line = builder.toString();
                    break;
                }
            }
        } catch (IOException ie) {
            log.error(ie);
        }
        return line;
    }
    
//    public static void main(String[] args) throws Exception{
//        putObjectToOOS("test-bucket-name18","1.txt","test_user1_6463084869102845087@a.cn11","secretKey11",new FileInputStream("d:\\test\\client_in.txt"),6L);
//    }
}
