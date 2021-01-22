package cn.ctyun.oos.tempo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.io.FileOutputStream;
import java.security.SecureRandom;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import cn.ctyun.oos.ostor.utils.CRCUtils;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import common.io.StreamUtils;
import common.threadlocal.ThreadLocalBytes;
import common.util.BlockingExecutor;
import common.util.HexUtils;

public class SimpleTest {
    static String OOS_DOMAIN = "http://oos-sh.ctyunapi.cn:8080";
    static String bucketName = "yanke";
    static BlockingExecutor pool = new BlockingExecutor(100, 100, 100, 5000,
            "SimpleTest-ThreadPool-");
    private static final Log log = LogFactory.getLog(SimpleTest.class);
    private static ClientConfiguration cc = new ClientConfiguration();
    static AmazonS3Client client;
    
    public static void generateRandomFile(File file, long length) throws Exception {
        ThreadLocalRandom rand = ThreadLocalRandom.current();
        FileOutputStream f = new FileOutputStream(file);
        byte[] bytes = ThreadLocalBytes.current().get1KBytes();
        long sent = 0;
        try {
            while (sent < length) {
                long remains = length - sent;
                int block = (int) ((remains < bytes.length) ? remains : bytes.length);
                rand.nextBytes(bytes);
                f.write(bytes, 0, block);
                sent += block;
            }
        } finally {
            if (f != null)
                f.close();
        }
    }
    
    public static String generateFileName() {
        byte[] buf = new byte[32];
        new Random().nextBytes(buf);
        return HexUtils.toHexString(buf);
    }
    
    public static int getLength() {
        int rint = tlrand.get().nextInt(1024 * 1);
        int length = rint * 1024 * 5;
        return length;
    }
    
    public static String getStorageClass(String sts) {
        String[] st = sts.split(",");
        int i = tlrand.get().nextInt(st.length);
        return st[i].toString();
    }
    
    // domain,bucket,ak,sk,storageClass
    // 传一个参数，是某个文件的路径，程序会上传这个文件，然后下载，并校验crc，最后删除object
    public static void main(String[] args) throws Exception {
        OOS_DOMAIN = args[0];
        bucketName = args[1];
        final String ak = args[2];
        final String sk = args[3];
        final String sts = args[4];
        cc.setConnectionTimeout(60000);
        cc.setSocketTimeout(60000);
        client = new AmazonS3Client(new AWSCredentials() {
            public String getAWSAccessKeyId() {
                return ak;
            }
            
            public String getAWSSecretKey() {
                return sk;
            }
        }, cc);
        client.setEndpoint(OOS_DOMAIN);
        int t = Integer.valueOf(args[5]);// 并发多少个线程
        final int count = Integer.valueOf(args[6]);// 每个线程执行多少次
        log.info("total:" + t);
        final AtomicLong total = new AtomicLong();
        final AtomicLong readSuc = new AtomicLong();
        final AtomicLong delSuc = new AtomicLong();
        Thread t2 = new Thread() {
            @Override
            public void run() {
                for (;;) {
                    log.info("Total = " + total.get());
                    log.info("readSuc = " + readSuc.get());
                    log.info("deleteSuc = " + delSuc.get());
                    try {
                        Thread.sleep(3000);
                    } catch (InterruptedException e) {
                    }
                }
            }
        };
        t2.setDaemon(true);
        t2.start();
        for (int i = 0; i < t; i++) {
            pool.execute(new Runnable() {
                public void run() {
                    for (int i = 0; i < count; i++) {
                        try {
                            String key = "simple_" + generateFileName();
                            generateRandomFile(new File("simple", key), getLength());
                            File file = new File("simple", key);
                            PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName,
                                    key, file);
                            ObjectMetadata metadata = new ObjectMetadata();
                            metadata.addUserMetadata("test", "metadata");
                            String storage = getStorageClass(sts);
                            putObjectRequest.setStorageClass(storage);
                            putObjectRequest.setMetadata(metadata);
                            client.putObject(putObjectRequest);
                            S3Object object = client.getObject(bucketName, key);
                            S3ObjectInputStream input = object.getObjectContent();
                            File file2 = new File("simple2", key);
                            FileOutputStream fos = new FileOutputStream(file2);
                            try {
                                StreamUtils.copy(input, fos, object.getObjectMetadata()
                                        .getContentLength());
                            } finally {
                                try {
                                    if (fos != null)
                                        fos.close();
                                } finally {
                                    if (input != null)
                                        input.close();
                                }
                            }
                            long crc1 = CRCUtils.getCRC32Checksum(file);
                            long crc2 = CRCUtils.getCRC32Checksum(file2);
                            if (crc1 == crc2) {
                                log.info("download success " + key + " length:" + file.length()
                                        + " " + storage);
                                readSuc.getAndIncrement();
                            } else
                                log.error("download fail " + key + " length:" + file.length() + " "
                                        + storage);
                            client.deleteObject(bucketName, key);
                            try {
                                client.getObject(bucketName, key);
                            } catch (AmazonServiceException e) {
                                assertEquals(e.getErrorCode(), "NoSuchKey");
                                assertEquals(e.getMessage(), "the request object is " + key);
                                assertNotNull(e.getRequestId());
                                assertEquals(e.getServiceName(), "Amazon S3");
                                assertEquals(e.getStatusCode(), 404);
                                log.info("delete success " + key);
                                delSuc.getAndIncrement();
                            }
                            file.delete();
                            file2.delete();
                        } catch (Exception e) {
                            log.error(e.getMessage(), e);
                        } finally {
                            total.getAndIncrement();
                        }
                    }
                }
            });
        }
        pool.shutdown();
        pool.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
        log.info("Total = " + total.get());
        log.info("readSuc = " + readSuc.get());
        log.info("delSuc = " + delSuc.get());
    }
    
    private static ThreadLocal<SecureRandom> tlrand = new ThreadLocal<SecureRandom>() {
        @Override
        protected SecureRandom initialValue() {
            return new SecureRandom();
        }
    };
}
