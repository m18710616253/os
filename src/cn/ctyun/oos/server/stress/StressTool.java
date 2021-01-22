package cn.ctyun.oos.server.stress;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import cn.ctyun.common.Program;

import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.S3Object;
import common.util.BlockingExecutor;
import common.util.GetOpt;

public class StressTool implements Program {
    public static void main(String[] args) throws Exception {
        new StressTool().exec(args);
    }

    public static void objectGet(AmazonS3 asc, int suffix) throws IOException {
        S3Object obj = asc.getObject(bucketName, objectName + suffix);
        InputStream is = obj.getObjectContent();
        try {
            while (is.read() != -1)
                ;
        } finally {
            is.close();
        }
    }

    public static void objectPut(AmazonS3 asc, final int size, int suffix) {
        final Random r = new Random();
        asc.putObject(bucketName, objectName + suffix, new InputStream() {
            int size2 = size;

            public int read() throws IOException {
                if (size2-- > 0)
                    return r.nextInt(255);
                return -1;
            }
        }, null);
    }

    public static void objectDeleteTest(AmazonS3 asc, int suffix) {
        asc.deleteObject(bucketName, objectName + suffix);
    }

    @Override
    public String usage() {
        // It's not necessary input bucketName and objectName for end technical
        // user
        return "-u: url address\n -pnum: numbers of putTest operate \n "
                + "-size: the size of object you want to put/get/delete\n"
                + "-gsn(option): the number you want to get same object, always get the first object.\n"
                + "The example for operate in command line like this. For example '-u http://www.site.com -pnum 30 -size 1048576 option(-gsn 100)'";
    }

    @Override
    public void exec(String[] args) throws Exception {
        GetOpt opts = new GetOpt("u:[pnum]:[size]:[gsn]:", args);
        int gsn = 0, putNum = 0;
        final String url = opts.getOpt("u");
        if ((opts.getOpt("pnum")) != null)
            putNum = Integer.parseInt(opts.getOpt("pnum"));
        final int size = Integer.parseInt(opts.getOpt("size"));
        if (opts.getOpt("gsn") != null)
            gsn = Integer.parseInt(opts.getOpt("gsn"));
        final AmazonS3 asc = new AmazonS3Client(new PropertiesCredentials(
                new FileInputStream(System.getProperty("user.dir")
                        + "/conf/TestAlive.properties")));
        asc.setEndpoint(url);
        if (!asc.doesBucketExist(bucketName)) {
            asc.createBucket(bucketName);
        }
        System.out
                .println("There are "
                        + putNum
                        + " concurrently executing threads to do put/get/delete operates.");
        // first thread pool
        long start = System.currentTimeMillis();
        BlockingExecutor exec = new BlockingExecutor(100, 100, 100, 10000,
                "dbclient-emulator");
        for (int i = 0; i < putNum; i++) {
            final int suffix = i;
            exec.execute(new Runnable() {
                @Override
                public void run() {
                    objectPut(asc, size, suffix);
                }
            });
        }
        exec.shutdown();
        exec.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        long end = System.currentTimeMillis();
        System.out.println("Put cost: " + (end - start) / 1000 + " seconds.");
        // second thread pool
        start = System.currentTimeMillis();
        BlockingExecutor exec1 = new BlockingExecutor(100, 100, 100, 10000,
                "dbclient-emulator");
        for (int i = 0; i < putNum; i++) {
            final int suffix = i;
            exec1.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        objectGet(asc, suffix);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            });
        }
        exec1.shutdown();
        exec1.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        end = System.currentTimeMillis();
        System.out.println("Get different object cost: " + (end - start) / 1000
                + " seconds.");
        // third thread pool
        start = System.currentTimeMillis();
        BlockingExecutor exec2 = new BlockingExecutor(100, 100, 100, 10000,
                "dbclient-emulator");
        for (int i = 0; i < gsn; i++) {
            final int suffix = 0;
            exec2.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        objectGet(asc, suffix);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            });
        }
        exec2.shutdown();
        exec2.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        end = System.currentTimeMillis();
        if (gsn > 0)
            System.out.println("Get same object cost: " + (end - start) / 1000
                    + " seconds.");
        // fourth thread pool
        start = System.currentTimeMillis();
        BlockingExecutor exec3 = new BlockingExecutor(100, 100, 100, 10000,
                "dbclient-emulator");
        for (int i = 0; i < putNum; i++) {
            final int suffix = i;
            exec3.execute(new Runnable() {
                @Override
                public void run() {
                    objectDeleteTest(asc, suffix);
                }
            });
        }
        exec3.shutdown();
        exec3.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        end = System.currentTimeMillis();
        System.out
                .println("Delete cost: " + (end - start) / 1000 + " seconds.");
    }

    private static String objectName = "object-name-for-stress-test-6463084869102845087-";
    private static String bucketName = "bucket-name-for-stress-test-7845589720135487968";
}