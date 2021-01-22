package cn.ctyun.oos.tempo;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;

/**
 * 这个程序用来简单测试一下线上服务是否正常。
 * 
 * @author: Jiang Feng
 */
public class OosOpTest {

    private static final Log l4j = LogFactory.getLog(OosOpTest.class);
    private static String host = "oos.ctyunapi.cn";
    private static int port = 8080;
    private static int threadNum = 1000;
    private static int reqRate = 500;
    private static int getNumPerThread = 1000;
    private static long length = 0;
    private static String bucket = null;
    private static String hostname = null;

    private static final AtomicInteger putSuccess = new AtomicInteger();
    private static final AtomicInteger putFail = new AtomicInteger();
    private static final AtomicInteger getSuccess = new AtomicInteger();
    private static final AtomicInteger getFail = new AtomicInteger();
    private static final AtomicInteger totalThreadNum = new AtomicInteger();

    private final Random r = new Random();

    private AmazonS3 client = null;
    private static String awsSecretKey = null;
    private static String awsAccessKeyId = null;
    private static AWSCredentials awsCredentials = null;
    private static final ClientConfiguration cc = new ClientConfiguration();

    public static void main(String[] args) throws IOException {
        new OosOpTest().start();
    }

    public void start() {

        Thread ts[] = new Thread[threadNum];
        for (int i = 0; i < ts.length; i++) {
            final String suffix = "-" + i;
            Thread t = new Thread() {
                public void run() {
                    l4j.info("current thread： " + totalThreadNum.addAndGet(1));
                    try {
                        Thread.sleep(r.nextInt(2 * 1000 * threadNum / reqRate));
                    } catch (InterruptedException e2) {
                        // TODO Auto-generated catch block
                        l4j.error(e2);
                    }
                    // client = new AmazonS3Client(awsCredentials, cc);
                    // client.setEndpoint("http://" + host + ":" + port);
                    String key = hostname + "-" + bucket + suffix;
                    boolean success = false;
                    while (success == false) {
                        try {
                            if (!client.doesBucketExist(bucket)) {
                                client.createBucket(bucket);
                            }
                            success = true;
                        } catch (Exception e) {
                            l4j.error(e);
                            try {
                                Thread.sleep(r.nextInt(2 * 1000 * threadNum / reqRate));
                            } catch (InterruptedException e1) {
                                // TODO Auto-generated catch block
                                l4j.error(e1);
                            }
                        }
                    }

                    success = false;
                    ObjectMetadata meta = new ObjectMetadata();
                    meta.setContentLength(length);
                    while (success == false) {
                        try {
                            client.putObject(bucket, key, new MyInputStream(
                                    length), meta);
                            success = true;
                            l4j.info("put success: " + putSuccess.addAndGet(1));
                        } catch (Exception e) {
                            l4j.error("put fail: " + putFail.addAndGet(1));
                            try {
                                Thread.sleep(r.nextInt(2 * 1000 * threadNum
                                        / reqRate));
                            } catch (InterruptedException e1) {
                                // TODO Auto-generated catch block
                                l4j.error(e1);
                            }
                        }
                    }

                    for (int j = 0; j < getNumPerThread; j++) {
                        try {
                            Thread.sleep(r.nextInt(2 * 1000 * threadNum
                                    / reqRate));
                        } catch (InterruptedException e2) {
                            // TODO Auto-generated catch block
                            l4j.error(e2);
                        }
                        InputStream is = null;
                        try {
                            S3Object object = client.getObject(bucket, key);
                            is = object.getObjectContent();
                            while (is.read() != -1)
                                ;
                            l4j.info("get success: " + getSuccess.addAndGet(1));
                        } catch (Exception e) {
                            l4j.info("get fail: " + getFail.addAndGet(1));
                            l4j.error(e);
                        } finally {
                            try {
                                if (is != null)
                                    is.close();
                            } catch (IOException e1) {
                                l4j.error(e1);
                            }
                        }
                    }
                }
            };
            t.start();
        }
    }

    OosOpTest() throws FileNotFoundException {
        InputStream is = null;

        Properties p = new Properties();
        is = new FileInputStream(System.getProperty("user.dir")
                + "/conf/OosOpTest.conf");
        try {
            p.load(is);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            l4j.error(e);
        }

        host = p.getProperty("host");
        port = Integer.parseInt(p.getProperty("port"));
        length = parseLength(p.getProperty("length"));
        threadNum = Integer.parseInt(p.getProperty("threadNum"));
        getNumPerThread = Integer.parseInt(p.getProperty("getNumPerThread"));
        reqRate = Integer.parseInt(p.getProperty("reqRate"));

        bucket = p.getProperty("bucket");
        try {
            hostname = InetAddress.getLocalHost().toString();
        } catch (UnknownHostException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        awsSecretKey = p.getProperty("awsSecretKey");
        awsAccessKeyId = p.getProperty("awsAccessKeyId");

        // cc.setMaxConnections(200);// 设置最大连接数
        // cc.setConnectionTimeout(150 * 10000);// 设置超时时间
        // cc.setSocketTimeout(150 * 1000);// 设置超时时间
        // cc.setMaxErrorRetry(3);// 设置出错重试次数

        awsCredentials = new AWSCredentials() {
            public String getAWSSecretKey() {
                return awsSecretKey;
            }

            public String getAWSAccessKeyId() {
                return awsAccessKeyId;
            }
        };
        client = new AmazonS3Client(awsCredentials, cc);
        client.setEndpoint("http://" + host + ":" + port);
    }

    private long parseLength(String length) {
        Character c = length.charAt(length.length() - 1);
        long result = 0;
        if (Character.isLetter(c)) {
            if (length.endsWith("k") || length.endsWith("K")) {
                result = 1024 * Integer.parseInt(length.substring(0,
                        length.length() - 1));
            } else if (length.endsWith("m") || length.endsWith("M")) {
                result = 1024 * 1024 * Integer.parseInt(length.substring(0,
                        length.length() - 1));
            } else if (length.endsWith("g") || length.endsWith("G")) {
                result = 1024 * 1024 * 1024 * Integer.parseInt(length
                        .substring(0, length.length() - 1));
            }
        } else {
            result = Integer.parseInt(length);
        }
        return result;
    }
}
