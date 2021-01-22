package cn.ctyun.oos.tempo;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import common.util.BlockingExecutor;

public class DeleteObjectFromOOS {
    private static final Log log = LogFactory.getLog(DeleteObjectFromOOS.class);
    static {
        System.setProperty("log4j.log.app", "delete");
    }
    private static ClientConfiguration cc = new ClientConfiguration();
    static AmazonS3Client client;
    final static String OOS_DOMAIN = "http://oos-nm2.ctyunapi.cn";
    static {
        cc.setConnectionTimeout(30000);
        cc.setSocketTimeout(30000);
    }
    
    // 删除某个bucket下面的所有数据
    // ak,sk,bucketName,并发
    public static void main(String[] args) throws InterruptedException {
        final String ak = args[0];
        final String sk = args[1];
        String bucketName = args[2];
        client = new AmazonS3Client(new AWSCredentials() {
            public String getAWSAccessKeyId() {
                return ak;
            }
            
            public String getAWSSecretKey() {
                return sk;
            }
        }, cc);
        client.setEndpoint(OOS_DOMAIN);
        final AtomicLong delSuc = new AtomicLong();
        Thread t2 = new Thread() {
            @Override
            public void run() {
                for (;;) {
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
        BlockingExecutor executor = new BlockingExecutor(Integer.valueOf(args[3]),
                Integer.valueOf(args[3]), Integer.valueOf(args[3]), 3000, "DelThread");
        ObjectListing os = client.listObjects(bucketName);
        int size = 0;
        do {
            for (S3ObjectSummary o : os.getObjectSummaries()) {
                executor.execute(new DelThread2(client, bucketName, o.getKey(), delSuc));
            }
            while (true) {
                if (executor.getActiveCount() > 0)
                    Thread.sleep(1000);
                else
                    break;
            }
            os = client.listObjects(bucketName);
            size = os.getObjectSummaries().size();
        } while (size != 0);
        while (true) {
            if (executor != null) {
                log.info(executor.getActiveCount());
                if (executor.getActiveCount() > 0)
                    Thread.sleep(100);
                else
                    break;
            }
        }
        log.info("DelSuc = " + delSuc.get());
    }
}

class DelThread2 implements Runnable {
    private static final Log log = LogFactory.getLog(DelThread2.class);
    String key;
    String bucket;
    AmazonS3Client client;
    AtomicLong delSuc;
    
    public DelThread2(AmazonS3Client client, String bucket, String key, AtomicLong delSuc) {
        this.key = key;
        this.bucket = bucket;
        this.client = client;
        this.delSuc = delSuc;
    }
    
    @Override
    public void run() {
        try {
            client.deleteObject(bucket, key);
            log.info("delete success:" + key);
            delSuc.getAndIncrement();
        } catch (Throwable e) {
            log.error(e.getMessage(), e);
        }
    }
}
