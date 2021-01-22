package cn.ctyun.oos.tempo;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import common.threadlocal.ThreadLocalBytes;
import common.util.BlockingExecutor;
class Common {
    final static String OOS_DOMAIN = "http://oos-fj.ctyunapi.cn:8080";
    final static String bucketName = "test";
    private static ClientConfiguration cc = new ClientConfiguration();
    static AmazonS3Client client;
    static {
        cc.setConnectionTimeout(60000 * 3);
        cc.setSocketTimeout(60000 * 3);
//        client = new AmazonS3Client(new AWSCredentials() {
//            public String getAWSAccessKeyId() {
//                return "562e0b5cf26edb2ef9b4";// "0a4b5f7825612b1936ed";//
//            }
//            
//            public String getAWSSecretKey() {
//                return "62b825b0cd0fd077b0d93c56c512b97da8834048";// "25b5e44d4a2b320aeeb2b92a7983f25fca78bacf";//
//            }
//        }, cc);
        client.setEndpoint(OOS_DOMAIN);
    }
}
// 先put，然后list object, get下来校验crc
public class PutGetTest2 {
    // final static String OOS_DOMAIN = "http://oos-sh.ctyunapi.cn";
    // final static String bucketName = "12345";
    private static final Log log = LogFactory.getLog(PutGetTest2.class);
    public static String mountPath = "/mnt/s3";
    public static AtomicLong totalBytes = new AtomicLong();
    static {
        System.setProperty("log4j.log.app", "putget");
    }
    
    // static Random rand = new Random(Long.MAX_VALUE);
    // 参数put 10(put的总数) 10(线程数)/ get 10(线程数)/ del /one objectName
    public static void main(String[] args) throws Exception {
        String param = args[0];
        final AtomicLong total = new AtomicLong();
        final AtomicLong readSuc = new AtomicLong();
        final AtomicLong putSuc = new AtomicLong();
        final AtomicLong delSuc = new AtomicLong();
        long timeStart = 0;
        long timeEnd = 0;
        Thread t2 = new Thread() {
            @Override
            public void run() {
                for (;;) {
                    log.info("Total = " + total.get());
                    log.info("readSuc = " + readSuc.get());
                    log.info("PutSuc = " + putSuc.get());
                    log.info("DelSuc = " + delSuc.get());
                    try {
                        Thread.sleep(3000);
                    } catch (InterruptedException e) {
                    }
                }
            }
        };
        t2.setDaemon(true);
        t2.start();
        long length;
        
        BlockingExecutor executor = null;
        if (param.equals("put")) {
            if(args.length>3 && args[3]!=null)
                length=Long.valueOf(args[3]);
            else
                length=SimpleTest.getLength();
            timeStart = System.currentTimeMillis();
            executor = new BlockingExecutor(Integer.valueOf(args[2]), Integer.valueOf(args[2]),
                    Integer.valueOf(args[2]), 3000, "PutThread_1");
            int putNum = Integer.valueOf(args[1]); // put多少个object
            for (int i = 0; i < putNum; i++) {
                executor.execute(new PutThread_1(putSuc, total,length));
            }
        } else if (param.equals("del")) {// 删除所有Object
            executor = new BlockingExecutor(Integer.valueOf(args[1]), Integer.valueOf(args[1]),
                    Integer.valueOf(args[1]), 3000, "DelThread_1");
            ListObjectsRequest request = new ListObjectsRequest();
            request.setBucketName(Common.bucketName);
            request.setPrefix("jiazg_putget_");
            ObjectListing os = Common.client.listObjects(request);
//            ObjectListing os = Common.client.listObjects(Common.bucketName);
            int size = 0;
            timeStart = System.currentTimeMillis();
            do {
                for (S3ObjectSummary o : os.getObjectSummaries()) {
                    executor.execute(new DelThread_1(delSuc, total, o.getKey(), o.getSize()));
                }
                while (true) {
                    log.info(executor.getActiveCount());
                    if (executor.getActiveCount() > 0)
                        Thread.sleep(100);
                    else
                        break;
                }
                os = Common.client.listObjects(Common.bucketName);
                size = os.getObjectSummaries().size();
            } while (size != 0);
        } else if (param.equals("get")) {// get object
            executor = new BlockingExecutor(Integer.valueOf(args[1]), Integer.valueOf(args[1]),
                    Integer.valueOf(args[1]), 3000, "GetThread_1");
            ListObjectsRequest request = new ListObjectsRequest();
            request.setBucketName(Common.bucketName);
            request.setPrefix("jiazg_putget_");
            request.setMaxKeys(100);
            ObjectListing os = Common.client.listObjects(request);
            timeStart = System.currentTimeMillis();
            int size = 0;
            do {
                for (S3ObjectSummary o : os.getObjectSummaries()) {
                    executor.execute(new GetThread_1(readSuc, total, o.getKey(), o.getStorageClass(),
                            o.getSize()));
                }
                log.info(os.getNextMarker() + " " + os.isTruncated());
                if (!os.isTruncated())
                    break;
                request.setMarker(os.getNextMarker());
                os = Common.client.listObjects(request);
                size = os.getObjectSummaries().size();
            } while (size != 0);
        } else {
            String key = args[1];
            for (int i = 0; i < 1000; i++) {
                S3Object object = Common.client.getObject(Common.bucketName, key);
                S3ObjectInputStream input = object.getObjectContent();
                try {
                    long crc1 = Long.parseLong(object.getObjectMetadata().getUserMetadata()
                            .get("crc"));
                    long crc2 = GetThread_1.getCRC32Checksum(input);
                    if (crc1 == crc2) {
                        log.info("download success " + key + " length:"
                                + object.getObjectMetadata().getContentLength() + " storage:"
                                + object.getObjectMetadata().getUserMetadata().get("storage"));
                        readSuc.getAndIncrement();
                    } else
                        log.error("download fail " + key + " length:"
                                + object.getObjectMetadata().getContentLength() + " storage:"
                                + object.getObjectMetadata().getUserMetadata().get("storage")
                                + " crc1:" + crc1 + " crc2" + crc2);
                } finally {
                    if (input != null)
                        input.close();
                }
            }
        }
        while (true) {
            log.info(executor.getActiveCount());
            if (executor.getActiveCount() > 0)
                Thread.sleep(100);
            else
                break;
        }
        timeEnd = System.currentTimeMillis();
        log.info("totalBytes = " + totalBytes.get() + ", time = " + (timeEnd - timeStart));
        log.info("Total = " + total.get());
        log.info("readSuc = " + readSuc.get());
        log.info("PutSuc = " + putSuc.get());
        log.info("Bandwidth = " + String.format("%.2f", ((double)totalBytes.get()) 
                / 1024 / 1024 / (((double)(timeEnd - timeStart)) / 1000)));
    }
}

class PutThread_1 implements Runnable {
    AtomicLong total;
    AtomicLong putSuc;
    long length;
    private static final Log log = LogFactory.getLog(PutThread_1.class);
    
    public PutThread_1(AtomicLong putSuc, AtomicLong total,long length) {
        this.total = total;
        this.putSuc = putSuc;
        this.length=length;
    }
    
    @Override
    public void run() {
        String key = "jiazg_putget_" + SimpleTest.generateFileName();
        File file = new File(PutGetTest2.mountPath, key);
        System.out.println(file.getAbsolutePath());
        try {
            file.createNewFile();
            SimpleTest.generateRandomFile(file, length);
            log.info("put success:" + key + " length:" + file.length() + " ");
            putSuc.getAndIncrement();
            PutGetTest2.totalBytes.getAndAdd(length);
        } catch (Exception e) {
            log.error("put error:" + key + " length:" + file.length() + " ", e);
        } finally {
            total.getAndIncrement();
        }
    }
}

class DelThread_1 implements Runnable {
    private static final Log log = LogFactory.getLog(PutThread_1.class);
    AtomicLong delSuc;
    AtomicLong total;
    long size;
    String key;
    
    public DelThread_1(AtomicLong delSuc, AtomicLong total, String key, long size) {
        this.key = key;
        this.delSuc = delSuc;
        this.total = total;
        this.size = size;
    }
    
    @Override
    public void run() {
        try {
            Common.client.deleteObject(Common.bucketName, key);
            PutGetTest2.totalBytes.getAndAdd(size);
            delSuc.getAndIncrement();
            log.info("delete success:" + key);
        } catch (Exception e) {
            log.error("delete faild:" + key, e);
        } finally{
            total.getAndIncrement();
        }
    }
}

class GetThread_1 implements Runnable {
    AtomicLong total;
    AtomicLong readSuc;
    String key;
    String stor;
    long size;
    private static final Log log = LogFactory.getLog(GetThread_1.class);
    
    public GetThread_1(AtomicLong readSuc, AtomicLong total, String key, String stor, long size) {
        this.total = total;
        this.readSuc = readSuc;
        this.key = key;
        this.stor = stor;
        this.size = size;
    }
    
    public static long getCRC32Checksum(InputStream input) throws IOException {
        CheckedInputStream cis = null;
        long checksum = 0;
        try {
            cis = new CheckedInputStream(input, new CRC32());
            byte[] buffer = new byte[64];
            while (cis.read(buffer) != -1)
                ;
            checksum = cis.getChecksum().getValue();
        } finally {
            if (null != cis) {
                cis.close();
            }
        }
        return checksum;
    }
    
    @Override
    public void run() {
        try {
            File file = new File(PutGetTest2.mountPath, key);
            FileInputStream f = null;
            try {
                f = new FileInputStream(file);
                byte[] bytes = ThreadLocalBytes.current().get1KBytes();
                long get = 0;
                while(get < size){
                    long len = f.read(bytes);
                    get += len;
                }
                assert(size == get);
                PutGetTest2.totalBytes.getAndAdd(size);
                readSuc.getAndIncrement();
                log.info("get success " + key + " length:" + size);
            } catch (Exception e) {
                log.error("get faild " + key + " length:" + size, e);
            } finally{
                if(null != f)
                    try {
                        f.close();
                    } catch (IOException e) {
                        log.error(e.getMessage(), e);
                    }
            }
        } catch (Exception e) {
            log.error("download fail " + key + " length:" + size + " storage:" + stor);
            log.error(e.getMessage(), e);
        } finally {
            total.getAndIncrement();
        }
    }
}
