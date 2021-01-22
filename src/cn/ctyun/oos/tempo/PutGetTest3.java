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

import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import common.threadlocal.ThreadLocalBytes;
import common.util.BlockingExecutor;

// 先put，然后list object, get下来校验crc
public class PutGetTest3 {
    // final static String OOS_DOMAIN = "http://oos-sh.ctyunapi.cn";
    // final static String bucketName = "12345";
    private static final Log log = LogFactory.getLog(PutGetTest3.class);
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
        BlockingExecutor executor = null;
        if (param.equals("put")) {
            if(args.length < 6){
                throw new RuntimeException("Usage:put count threads size prefix directory");
            }
            int count = Integer.valueOf(args[1]);
            int threads = Integer.valueOf(args[2]);
            long size = Long.parseLong(args[3]);
            String prefix = args[4];
            String path = args[5];
            
            timeStart = System.currentTimeMillis();
            executor = new BlockingExecutor(threads, threads, threads, 3000, "PutThread_2");
            for (int i = 0; i < count; i++) {
                executor.execute(new PutThread_2(putSuc, total, size, prefix, path));
            }
        } else if (param.equals("del")) {// 删除所有Object
            
            if(args.length < 3){
                throw new RuntimeException("Usage:del threads prefix");
            }
            int threads = Integer.valueOf(args[1]);
            String prefix = args[2];
            executor = new BlockingExecutor(threads, threads, threads, 3000, "DelThread_2");
            ListObjectsRequest request = new ListObjectsRequest();
            request.setBucketName(Common.bucketName);
            request.setPrefix(prefix);
            ObjectListing os = Common.client.listObjects(request);
            int size = 0;
            timeStart = System.currentTimeMillis();
            do {
                for (S3ObjectSummary o : os.getObjectSummaries()) {
                    executor.execute(new DelThread_2(delSuc, total, o.getKey(), o.getSize()));
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
            if(args.length < 3){
                throw new RuntimeException("Usage:get threads prefix");
            }
            int threads = Integer.valueOf(args[1]);
            String prefix = args[2];
            
            executor = new BlockingExecutor(threads, threads, threads, 3000, "GetThread_2");
            ListObjectsRequest request = new ListObjectsRequest();
            request.setBucketName(Common.bucketName);
            request.setPrefix(prefix);
            request.setMaxKeys(100);
            ObjectListing os = Common.client.listObjects(request);
            timeStart = System.currentTimeMillis();
            int size = 0;
            do {
                for (S3ObjectSummary o : os.getObjectSummaries()) {
                    executor.execute(new GetThread_2(readSuc, total, o.getKey(), o.getStorageClass(),
                            o.getSize()));
                }
                log.info(os.getNextMarker() + " " + os.isTruncated());
                if (!os.isTruncated())
                    break;
                request.setMarker(os.getNextMarker());
                os = Common.client.listObjects(request);
                size = os.getObjectSummaries().size();
            } while (size != 0);
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

class PutThread_2 implements Runnable {
    AtomicLong total;
    AtomicLong putSuc;
    String prefix;
    String path;
    long length;
    private static final Log log = LogFactory.getLog(PutThread_2.class);
    
    public PutThread_2(AtomicLong putSuc, AtomicLong total,long length,
            String prefix, String path) {
        this.total = total;
        this.putSuc = putSuc;
        this.length=length;
        this.prefix = prefix;
        this.path = path;
    }
    
    @Override
    public void run() {
        String key = prefix + SimpleTest.generateFileName();
        File file = new File(path, key);
        System.out.println(file.getAbsolutePath());
        try {
            file.createNewFile();
            SimpleTest.generateRandomFile(file, length);
            log.info("put success:" + key + " length:" + file.length() + " ");
            putSuc.getAndIncrement();
            PutGetTest3.totalBytes.getAndAdd(length);
        } catch (Exception e) {
            log.error("put error:" + key + " length:" + file.length() + " ", e);
        } finally {
            total.getAndIncrement();
        }
    }
}

class DelThread_2 implements Runnable {
    private static final Log log = LogFactory.getLog(PutThread_2.class);
    AtomicLong delSuc;
    AtomicLong total;
    long size;
    String key;
    
    public DelThread_2(AtomicLong delSuc, AtomicLong total, String key, long size) {
        this.key = key;
        this.delSuc = delSuc;
        this.total = total;
        this.size = size;
    }
    
    @Override
    public void run() {
        try {
            Common.client.deleteObject(Common.bucketName, key);
            PutGetTest3.totalBytes.getAndAdd(size);
            delSuc.getAndIncrement();
            log.info("delete success:" + key);
        } catch (Exception e) {
            log.error("delete faild:" + key, e);
        } finally{
            total.getAndIncrement();
        }
    }
}

class GetThread_2 implements Runnable {
    AtomicLong total;
    AtomicLong readSuc;
    String key;
    String stor;
    long size;
    String path;
    private static final Log log = LogFactory.getLog(GetThread_2.class);
    
    public GetThread_2(AtomicLong readSuc, AtomicLong total, String key, 
            String stor, long size) {
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
            File file = new File(PutGetTest3.mountPath, key);
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
                PutGetTest3.totalBytes.getAndAdd(size);
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