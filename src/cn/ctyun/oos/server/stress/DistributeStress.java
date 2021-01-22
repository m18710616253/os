package cn.ctyun.oos.server.stress;

import java.io.IOException;
import java.io.InputStream;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.emc.esu.api.Identifier;
import com.emc.esu.api.rest.EsuRestApi;

import common.time.TimeStat;
import common.util.GetOpt;
import common.util.MD5Hash;

import cn.ctyun.common.Program;

/**
 * @author Dongchk 压力测试oos
 */
public class DistributeStress implements Program {
    private static Log log = LogFactory.getLog(DistributeStress.class);
    private static AmazonS3Client asc;
    private int tnum;
    private int pgratio;
    private static String bucket = "12345";
    private static String keyPrefix = "DSkeyPrefix-";
    final static AtomicLong total = new AtomicLong();
    final static AtomicLong writeSuc = new AtomicLong();
    final static AtomicLong readSuc = new AtomicLong();
    final TimeStat stat = new TimeStat();
    
    public String usage() {
        return null;
    }
    
    public static void main(String[] args) {
        try {
            new DistributeStress().exec(args);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }
    
    public void exec(String[] args) throws Exception {
        GetOpt opts = new GetOpt("[help]:[tnum]:[pgratio]:[url]:[uid]:[sk]:[pord]:", args);
        tnum = opts.getInt("tnum", 1000);
        pgratio = opts.getInt("pgratio", 2);
        String url = opts.getOpt("url");
        final String uid = opts.getOpt("uid");
        final String sk = opts.getOpt("sk");
        if (url == null || uid == null || sk == null)
            throw new IllegalArgumentException("ip、uid、sk should not be null.");
        ClientConfiguration configuration = new ClientConfiguration();
        configuration.setMaxErrorRetry(0);
        try {
            asc = new AmazonS3Client(new AWSCredentials() {
                public String getAWSAccessKeyId() {
                    return uid;
                }
                
                public String getAWSSecretKey() {
                    return sk;
                }
            }, configuration);
            asc.setEndpoint(url);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw e;
        }
        int pord = opts.getInt("pord", 0);
        Thread t = new Thread() {
            @Override
            public void run() {
                for (;;) {
                    synchronized (stat) {
                        log.info("Stat =" + stat);
                    }
                    log.info("Total = " + total.get());
                    log.info("writeSuc = " + writeSuc.get());
                    log.info("readSuc = " + readSuc.get());
                    try {
                        Thread.sleep(3000);
                    } catch (InterruptedException e) {
                    }
                }
            }
        };
        t.setDaemon(true);
        t.start();
        if (pord == 0) {
            bucketCreate(bucket);
            for (int i = 0; i < tnum; i++)
                new PutReadStress().start();
        } else
            for (int i = 0; i < tnum; i++)
                new DeleteStress().start();
    }
    
    static Random rand = new Random(Long.MAX_VALUE);
    
    class PutReadStress extends Thread {
        public void run() {
            while (true) {
                try {
                    String key = null;
                    for (int i = 0; i < pgratio; i++) {
                        key = getKey();
                        objectPut(getKey(), bucket, getLength());
//                        log.info(key);
                    }
                    if (pgratio < 10)
                        objectGet(key, bucket);
                } catch (Throwable t) {
                    log.error(t.getMessage(), t);
                }
            }
        }
    }
    
    private static final int KB = 1024;
    private static final int MB = 1024 * 1024;
    
    private static long getLength() {
        int rint = rand.nextInt(100);
        long length;
        if (rint < 5)
            length = 4 * KB;
        else if (rint < 6)
            length = 8 * KB;
        else if (rint < 8)
            length = 16 * KB;
        else if (rint < 14)
            length = 32 * KB;
        else if (rint < 24)
            length = 64 * KB;
        else if (rint < 31)
            length = 128 * KB;
        else if (rint < 39)
            length = 256 * KB;
        else if (rint < 49)
            length = 512 * KB;
        else if (rint < 61)
            length = MB;
        else if (rint < 88)
            length = 2 * MB;
        else if (rint < 94)
            length = 4 * MB;
        else if (rint < 95)
            length = 12 * MB;
        else
            length = 24 * MB;
        return length;
    }
    
    private static String getKey() {
        return keyPrefix + MD5Hash.digest(String.valueOf(rand.nextLong())).toString();
    }
    
    private static void bucketCreate(String bucket) throws IOException {
        boolean isExists = asc.doesBucketExist(bucket);
        if (!isExists) {
            CreateBucketRequest createBucketRequest = new CreateBucketRequest(bucket);
            createBucketRequest.setCannedAcl(CannedAccessControlList.PublicRead);
            try {
                asc.createBucket(createBucketRequest);
            } catch (Throwable t) {
                log.error("CreateBucketFailed.", t);
            }
        }
    }
    
    private static void objectPut(String object, String bucket, final long length)
            throws IOException {
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(length);
        try {
            final AtomicLong start = new AtomicLong(System.currentTimeMillis());
            asc.putObject(bucket, object, new InputStream() {
                int pos = 0;
                int v;
                
                public int read() throws IOException {
                    v = (pos++ >= length) ? -1 : 1;
                    if (pos == length) {
                        start.set(System.currentTimeMillis());
                    }
                    return v;
                }
            }, metadata);
            writeSuc.getAndIncrement();
        } catch (Throwable t) {
            log.error("PutObjectFailed.", t);
        }
        total.getAndIncrement();
    }
    
    private static void objectGet(String object, String bucket) throws IOException {
        S3Object s3Object = new S3Object();
        try {
            s3Object = asc.getObject(bucket, object);
            if (s3Object != null) {
                InputStream is = s3Object.getObjectContent();
                try {
                    while (is.read() != -1)
                        ;
                } finally {
                    is.close();
                }
            }
            readSuc.getAndIncrement();
        } catch (Throwable t) {
            log.error("GetObjectFailed.", t);
        }
        total.getAndIncrement();
    }
    
    private static void objectDelete(String object, String bucket) throws IOException {
        DeleteObjectRequest deleteObjectRequest = new DeleteObjectRequest(bucket, object);
        try {
            asc.deleteObject(deleteObjectRequest);
        } catch (Throwable t) {
            log.error("DeleteObjectFailed.", t);
        }
    }
    
    class DeleteStress extends Thread {
        public void run() {
            while (true) {
                try {
                    objectDelete(getKey(), bucket);
                } catch (Throwable t) {
                    log.error("DeleteObjectFailed.", t);
                }
            }
        }
    }
}