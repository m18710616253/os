package cn.ctyun.oos.server.stress;

import java.io.IOException;
import java.io.InputStream;
import java.util.Random;

import org.apache.log4j.Logger;

import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3Object;

public class ConcurrentTest {
    final static String endPointURL = "http://oos.ctyunapi.cn:7070";
    final static String awsCredentials = "AwsCredentials.properties";
    private static final Logger l4j = Logger.getLogger(ConcurrentTest.class);

    public static void main(String[] args) throws Exception {
        // MyInputStreamTest();
        // getBinomialTest(100, 0.4);
        if (args.length < 3) {
            System.out
                    .println("usage: java -cp oos-server-v1.0.1.jar:lib/log4j-1.2.17.jar:lib/aws-java-sdk-1.3.14.jar:lib/commons-logging-1.1.1.jar:lib/httpclient-4.1.jar:lib/httpcore-4.1.jar:lib/commons-codec-1.6.jar cn.ctyun.oos.server.stress.ConcurrentTest threadNum writeRatio writeSize");
            System.out
                    .println("example: java -cp oos-server-v1.0.1.jar:lib/log4j-1.2.17.jar:lib/aws-java-sdk-1.3.14.jar:lib/commons-logging-1.1.1.jar:lib/httpclient-4.1.jar:lib/httpcore-4.1.jar:lib/commons-codec-1.6.jar cn.ctyun.oos.server.stress.ConcurrentTest 100 0.2 4K");
            System.exit(0);
        }
        // concurrentPutTest(Integer.parseInt(args[1]), args[2]);
        // concurrentGetTest(1);
        concurrentMixedGetPutTest(Integer.parseInt(args[0]),
                Double.parseDouble(args[1]), args[2]);
    }

    private static void concurrentPutTest(int num, final String size)
            throws Exception {
        l4j.info("100% Write test, Write: " + num);
        MyThread.zeroReadWriteNum();
        MyThread[] t = new MyThread[num];
        for (int i = 0; i < num; i++) {
            t[i] = new MyThread(true, size);
            t[i].setObjectNameNum(i);
            t[i].start();
        }
        for (int i = 0; i < num; i++) {
            t[i].join();
        }
        l4j.info("Write: " + MyThread.getwriteNum() + ", Read: "
                + MyThread.getReadNum() + ", Total: " + MyThread.getTotal());
    }

    private static void concurrentGetTest(int num) throws Exception {
        l4j.info("100% Read test, Read: " + num);
        MyThread.zeroReadWriteNum();
        MyThread[] t = new MyThread[num];
        for (int i = 0; i < num; i++) {
            t[i] = new MyThread(false, "4K");
            t[i].setObjectNameNum(i);
            t[i].start();
        }
        for (int i = 0; i < num; i++) {
            t[i].join();
        }
        l4j.info("Write: " + MyThread.getwriteNum() + ", Read: "
                + MyThread.getReadNum() + ", Total: " + MyThread.getTotal());
    }

    private static void concurrentMixedGetPutTest(int num, double writeRatio,
            String writeSize) throws Exception {
        l4j.info(writeRatio * 100 + "% Write test");
        double[] binomial = getBinomial(num, writeRatio);
        MyThread.zeroReadWriteNum();
        MyThread[] t = new MyThread[num];
        for (int i = 0; i < num; i++) {
            t[i] = new MyThread(binomial[i] <= writeRatio, "4K");
            t[i].setObjectNameNum(i);
            t[i].start();
        }
        for (int i = 0; i < num; i++) {
            t[i].join();
        }
        l4j.info("Write: " + MyThread.getwriteNum() + ", Read: "
                + MyThread.getReadNum() + ", Total: " + MyThread.getTotal());
    }

    private static void MyInputStreamTest() throws IOException {
        MyInputStream myInputStream = new MyInputStream(1024);
        try{
            int len = 0, total = 0;
            while (myInputStream.read() >= 0) {
                total++;
            }
            l4j.info("total: " + total);
        }finally{
            myInputStream.close();
        }
    }

    private static double[] getBinomial(int num, double p) {
        Random random = new Random(System.currentTimeMillis());
        double[] binomial = new double[num];
        for (int i = 0; i < num; i++) {
            binomial[i] = random.nextDouble();
        }
        return binomial;
    }

    private static void getBinomialTest(int num, double p) {
        double[] binomial = getBinomial(num, p);
        int one = 0;
        for (int i = 0; i < num; i++) {
            if (binomial[i] <= p)
                one++;
        }
        l4j.info(one);
    }
}

class MyInputStream extends InputStream {
    private Random r = new Random();
    private long length;
    private long total = 0;

    MyInputStream(long length) {
        super();
        this.length = length;
    }

    @Override
    public int read() throws IOException {
        // synchronized (total) {
        total++;
        if (total <= length)
            return r.nextInt(256);
        else
            return -1;
        // }
    }
}

class MyThread extends Thread {
    static AmazonS3Client awsS3Client = null;
    static int writeSize = 0;
    boolean isWrite = false;
    static Integer writeNum = 0;
    static Integer readNum = 0;
    String bucketName = "yanke";
    String objectNamePrefix = "test";
    int objectNameNum = 0;
    final static String endPointURL = "http://oos.ctyunapi.cn:7070";
    final static String awsCredentials = "AwsCredentials.properties";
    private static final Logger l4j = Logger.getLogger(MyThread.class);

    MyThread(boolean isWrite, String writeSize) throws IOException {
        super();
        if (awsS3Client == null) {
            awsS3Client = new AmazonS3Client(new PropertiesCredentials(
                    MyThread.class.getResourceAsStream(awsCredentials)));
            awsS3Client.setEndpoint(endPointURL);
        }
        this.isWrite = isWrite;
        Character c = writeSize.charAt(writeSize.length() - 1);
        if (Character.isLetter(c)) {
            if (writeSize.endsWith("k") || writeSize.endsWith("K")) {
                this.writeSize = 1024 * Integer.parseInt(writeSize.substring(0,
                        writeSize.length() - 1));
            } else if (writeSize.endsWith("m") || writeSize.endsWith("M")) {
                this.writeSize = 1024 * 1024 * Integer.parseInt(writeSize
                        .substring(0, writeSize.length() - 1));
            } else if (writeSize.endsWith("g") || writeSize.endsWith("G")) {
                this.writeSize = 1024 * 1024 * 1024 * Integer
                        .parseInt(writeSize.substring(0, writeSize.length() - 1));
            }
        } else {
            this.writeSize = Integer.parseInt(writeSize);
        }
    }

    @Override
    public void run() {
        if (isWrite) {
            ObjectMetadata objectMetadata = new ObjectMetadata();
            objectMetadata.setContentLength(writeSize);
            PutObjectResult putResult = awsS3Client.putObject(bucketName,
                    objectNamePrefix + objectNameNum, new MyInputStream(
                            writeSize), objectMetadata);
            if (putResult != null) {
                synchronized (writeNum) {
                    l4j.info("time: " + writeNum + ", bucket: " + bucketName
                            + ", object: " + objectNamePrefix + objectNameNum
                            + ", size: " + writeSize + "bytes put success");
                    writeNum++;
                }
            }
        } else {
            S3Object readResult = awsS3Client.getObject(bucketName,
                    objectNamePrefix + objectNameNum);
            if (readResult != null) {
                InputStream inputStream = readResult.getObjectContent();
                byte[] buf = new byte[1024];
                int n = 0;
                int total = 0;
                try {
                    while ((n = inputStream.read(buf, 0, 1024)) > 0) {
                        total += n;
                    }
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                synchronized (readNum) {
                    l4j.info("time: " + readNum + ", bucket: " + bucketName
                            + ", object: " + objectNamePrefix + objectNameNum
                            + ", " + total + " bytes read, get success");
                    readNum++;
                }
            }
        }
    }

    public static int getTotal() {
        return readNum + writeNum;
    }

    public static int getReadNum() {
        return readNum;
    }

    public static int getwriteNum() {
        return writeNum;
    }

    public static void zeroReadWriteNum() {
        writeNum = 0;
        readNum = 0;
    }

    public void setObjectNamePrefix(String objectNamePrefix) {
        this.objectNamePrefix = objectNamePrefix;
    }

    public void setObjectNameNum(int num) {
        this.objectNameNum = num;
    }
}