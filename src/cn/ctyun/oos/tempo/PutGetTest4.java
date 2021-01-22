package cn.ctyun.oos.tempo;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;

import cn.ctyun.common.ReplicaMode;
import cn.ctyun.oos.hbase.MetaClient;
import cn.ctyun.oos.metadata.ObjectMeta;
import cn.ctyun.oos.metadata.UploadMeta;
import cn.ctyun.oos.ostor.OstorProxy.OstorObjectMeta;
import cn.ctyun.oos.ostor.Route;
import cn.ctyun.oos.ostor.Route.RouteTable;
import cn.ctyun.oos.ostor.common.HttpParams;
import cn.ctyun.oos.ostor.http.HttpHead;
import cn.ctyun.oos.ostor.utils.CRCUtils;
import common.io.StreamUtils;
import common.util.BlockingExecutor;
import common.util.HexUtils;

// 先put，然后list object, get下来校验crc
public class PutGetTest4 {
    // final static String OOS_DOMAIN = "http://oos-sh.ctyunapi.cn";
    // final static String bucketName = "12345";
    static {
        System.setProperty("log4j.log.app", "putget");
    }
    private static final Log log = LogFactory.getLog(PutGetTest4.class);
    
    static String OOS_DOMAIN = "http://oos-sh.ctyunapi.cn:8080";
    static String bucketName = "aaa";
    private static ClientConfiguration cc = new ClientConfiguration();
    static AmazonS3Client client;
    private static ThreadLocal<SecureRandom> tlrand = new ThreadLocal<SecureRandom>() {
        @Override
        protected SecureRandom initialValue() {
            return new SecureRandom();
        }
    };
    
    public static int getBigLength() {
        int rint, length = 0;
        while (true) {
            rint = tlrand.get().nextInt(1024 * 1);
            length = rint * 1024 * 15;
            if (length > 5 * 1024 * 1024)
                break;
        }
        return length;
    }
    
    // domain, bucketName,ak,sk,storageClass
    // static Random rand = new Random(Long.MAX_VALUE);
    // 参数put 10(put的总数) 10(线程数)/ get 10(线程数)/ del /one objectName /mulput
    // 10(put的总数) 10(线程数)
    // update 10(update的总object 数) 10(线程数)
    public static void main(String[] args) throws Exception {
        OOS_DOMAIN = args[0];
        bucketName = args[1];
        final String ak = args[2];
        final String sk = args[3];
        String sts = args[4];
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
        // args=new String[]{"del","10"};
        String param = args[5];
        final AtomicLong total = new AtomicLong();
        final AtomicLong readSuc = new AtomicLong();
        final AtomicLong checkSuc = new AtomicLong();
        final AtomicLong putSuc = new AtomicLong();
        final AtomicLong delSuc = new AtomicLong();
        Thread t2 = new Thread() {
            @Override
            public void run() {
                for (;;) {
                    log.info("Total = " + total.get());
                    log.info("readSuc = " + readSuc.get());
                    log.info("PutSuc = " + putSuc.get());
                    log.info("DelSuc = " + delSuc.get());
                    log.info("CheckSuc = " + checkSuc.get());
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
        String Type= null;
        String Region = null;
        String Strategy = null;
        BlockingExecutor executor = null;
        if (param.equals("put")) {
            executor = new BlockingExecutor(Integer.valueOf(args[7]), Integer.valueOf(args[7]),
                    Integer.valueOf(args[7]), 3000, "PutThread");
            int putNum = Integer.valueOf(args[6]); // put多少个object
            for (int i = 0; i < putNum; i++) {
            	length = SimpleTest.getLength();
                if (args.length > 8 && args[8] != null)
                	Type = String.valueOf(args[8]);
                if (args.length > 9 && args[9] != null)
                	Region = String.valueOf(args[9]);
                if (args.length > 10 && args[10] != null)
                	Strategy = String.valueOf(args[10]);
                executor.execute(new PutThread(putSuc, total, length, client, bucketName, sts, null,
                		 Type, Region, Strategy));
            }
        } else if (param.equals("del")) {// 删除所有Object
            executor = new BlockingExecutor(Integer.valueOf(args[6]), Integer.valueOf(args[6]),
                    Integer.valueOf(args[6]), 3000, "DelThread");
            ObjectListing os = client.listObjects(bucketName);
            int size = 0;
            do {
                for (S3ObjectSummary o : os.getObjectSummaries()) {
                    executor.execute(new DelThread(delSuc, o.getKey(), client, bucketName));
                }
                while (true) {
                    log.info(executor.getActiveCount());
                    if (executor.getActiveCount() > 0)
                        Thread.sleep(100);
                    else
                        break;
                }
                os = client.listObjects(bucketName);
                size = os.getObjectSummaries().size();
            } while (size != 0);
        } else if (param.equals("update")) {// update object
            executor = new BlockingExecutor(Integer.valueOf(args[7]), Integer.valueOf(args[7]),
                    Integer.valueOf(args[7]), 3000, "UpdateThread");
            int totalUpdate = Integer.valueOf(args[6]);
            int i = 0;
            ListObjectsRequest request = new ListObjectsRequest();
            request.setBucketName(bucketName);
            request.setPrefix("putget_");
            request.setMaxKeys(100);
            ObjectListing os = client.listObjects(request);
            int size = 0;
            do {
                for (S3ObjectSummary o : os.getObjectSummaries()) {
                	length = SimpleTest.getLength();
                    if (args.length > 8 && args[8] != null)
                    	Type = String.valueOf(args[8]);
                    if (args.length > 9 && args[9] != null)
                    	Region = String.valueOf(args[9]);
                    if (args.length > 10 && args[10] != null)
                    	Strategy = String.valueOf(args[10]);
                    executor.execute(new PutThread(putSuc, total, length, client, bucketName, sts,
                            o.getKey(), Type, Region, Strategy));
                    i++;
                    if (i >= totalUpdate)
                        break;
                }
                while (true) {
                    log.info(executor.getActiveCount());
                    if (executor.getActiveCount() > 0)
                        Thread.sleep(100);
                    else
                        break;
                }
                if (!os.isTruncated())
                    break;
                request.setMarker(os.getNextMarker());
                os = client.listObjects(request);
                size = os.getObjectSummaries().size();
            } while (size != 0);
        } else if (param.equals("get")) {// get object
            executor = new BlockingExecutor(Integer.valueOf(args[6]), Integer.valueOf(args[6]),
                    Integer.valueOf(args[6]), 3000, "GetThread");
            ListObjectsRequest request = new ListObjectsRequest();
            request.setBucketName(bucketName);
            request.setPrefix("putget_");
            request.setMaxKeys(100);
            ObjectListing os = client.listObjects(request);
            int size = 0;
            do {
                for (S3ObjectSummary o : os.getObjectSummaries()) {
                    executor.execute(new GetThread(readSuc, total, o.getKey(), o.getStorageClass(),
                            o.getSize(), client, bucketName, Boolean.parseBoolean(args[7])));
                }
                log.info(os.getNextMarker() + " " + os.isTruncated());
                if (!os.isTruncated())
                    break;
                request.setMarker(os.getNextMarker());
                os = client.listObjects(request);
                size = os.getObjectSummaries().size();
            } while (size != 0);
        } else if(param.equals("checkReplica")){
            executor = new BlockingExecutor(Integer.valueOf(args[6]), Integer.valueOf(args[6]),
                    Integer.valueOf(args[6]), 3000, "GetThread");
            String metaRegionName=args[7];
            ListObjectsRequest request = new ListObjectsRequest();
            request.setBucketName(bucketName);
            request.setPrefix("putget_");
            request.setMaxKeys(100);
            ObjectListing os = client.listObjects(request);
            int size = 0;
            
            do {
                for (S3ObjectSummary o : os.getObjectSummaries()) {
                    executor.execute(new CheckRepliaThread(checkSuc, total, o.getKey(), o.getStorageClass(),
                            o.getSize(), client, bucketName, metaRegionName));
                }
                log.info(os.getNextMarker() + " " + os.isTruncated());
                if (!os.isTruncated())
                    break;
                request.setMarker(os.getNextMarker());
                os = client.listObjects(request);
                size = os.getObjectSummaries().size();
            } while (size != 0);
        }else if (param.equals("mulput")) {
            executor = new BlockingExecutor(Integer.valueOf(args[7]), Integer.valueOf(args[7]),
                    Integer.valueOf(args[7]), 3000, "MulPutThread");
            int putNum = Integer.valueOf(args[6]); // put多少个object
            for (int i = 0; i < putNum; i++) {
            	length = SimpleTest.getLength();
           	 if (args.length > 8 && args[8] != null)
                	Type = String.valueOf(args[8]);
                if (args.length > 9 && args[9] != null)
                	Region = String.valueOf(args[9]);
                if (args.length > 10 && args[10] != null)
                	Strategy = String.valueOf(args[10]);
               executor.execute(new MulPutThread(putSuc, total, length, client, bucketName, sts,
               		 Type, Region, Strategy));
            }
        } else {
            String key = args[6];
            for (int i = 0; i < 1; i++) {
                S3Object object = client.getObject(bucketName, key);
                S3ObjectInputStream input = object.getObjectContent();
                try {
                    if (object.getObjectMetadata().getUserMetadata().get("crc") != null) {
                        long crc1 = Long.parseLong(object.getObjectMetadata().getUserMetadata()
                                .get("crc"));
                        long crc2 = GetThread.getCRC32Checksum(input);
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
                    } else {
                        File file2 = new File("simple2", key);
                        FileOutputStream fos = new FileOutputStream(file2);
                        try {
                            StreamUtils.copy(input, fos, object.getObjectMetadata()
                                    .getContentLength());
                        } finally {
                            if (fos != null)
                                fos.close();
                        }
                    }
                } finally {
                    if (input != null)
                        input.close();
                }
            }
        }
        while (true) {
            if (executor != null) {
                log.info(executor.getActiveCount());
                if (executor.getActiveCount() > 0)
                    Thread.sleep(100);
                else
                    break;
            }
        }
        log.info("Total = " + total.get());
        log.info("readSuc = " + readSuc.get());
        log.info("PutSuc = " + putSuc.get());
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
}

class PutThread_4 implements Runnable {
    AtomicLong total;
    AtomicLong putSuc;
    long length;
    AmazonS3Client client;
    String bucketName;
    private static final Log log = LogFactory.getLog(PutThread.class);
    String sts;
    String key;
    
    public PutThread_4(AtomicLong putSuc, AtomicLong total, long length, AmazonS3Client client,
            String bucketName, String sts, String key) {
        this.total = total;
        this.putSuc = putSuc;
        this.length = length;
        this.client = client;
        this.bucketName = bucketName;
        this.sts = sts;
        if (key != null)
            this.key = key;
        else
            this.key = "putget_" + SimpleTest.generateFileName();
    }
    
    @Override
    public void run() {
        File file = new File("simple", key);
        try {
            SimpleTest.generateRandomFile(new File("simple", key), length);
            PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName, key, file);
            ObjectMetadata metadata = new ObjectMetadata();
            if (!sts.equals("standard")) {
                String storage = SimpleTest.getStorageClass(sts);
                if (storage.equals("EC_2_0_2"))
                    storage = "STANDARD";
                if (storage.equals("EC_1_0_1"))
                    storage = "REDUCED_REDUNDANCY";
                // metadata.addUserMetadata("storage", storage);
                putObjectRequest.setStorageClass(storage);
            }
            metadata.addUserMetadata("crc", String.valueOf(CRCUtils.getCRC32Checksum(file)));
            putObjectRequest.setMetadata(metadata);
            client.putObject(putObjectRequest);
            log.info("put success:" + key + " length:" + file.length() + " ");
            putSuc.getAndIncrement();
        } catch (Exception e) {
            log.error("put error:" + key + " length:" + file.length() + " ", e);
        } finally {
            total.getAndIncrement();
            file.delete();
        }
    }
}

class MulPutThread_4 implements Runnable {
    AtomicLong total;
    AtomicLong putSuc;
    long length;
    AmazonS3Client client;
    String bucketName;
    private static final Log log = LogFactory.getLog(PutThread.class);
    String sts;
    static int size5M = 5 * 1024 * 1024;
    
    public MulPutThread_4(AtomicLong putSuc, AtomicLong total, long length, AmazonS3Client client,
            String bucketName, String sts) {
        this.total = total;
        this.putSuc = putSuc;
        this.length = length;
        this.client = client;
        this.bucketName = bucketName;
        this.sts = sts;
    }
    
    @Override
    public void run() {
        String key = "putget_mul_" + SimpleTest.generateFileName();
        File file = new File("simple", key);
        try {
            SimpleTest.generateRandomFile(new File("simple", key), length);
            InitiateMultipartUploadRequest request = new InitiateMultipartUploadRequest(bucketName,
                    key);
            ObjectMetadata metadata = new ObjectMetadata();
            metadata.addUserMetadata("crc", String.valueOf(CRCUtils.getCRC32Checksum(file)));
            String storage = SimpleTest.getStorageClass(sts);
            if (storage.equals("EC_2_0_2"))
                storage = "STANDARD";
            if (storage.equals("EC_1_0_1"))
                storage = "REDUCED_REDUNDANCY";
            // metadata.addUserMetadata("storage", storage.substring(0,5));
            request.setObjectMetadata(metadata);
            request.setStorageClass(storage);
            InitiateMultipartUploadResult result = client.initiateMultipartUpload(request);
            String uploadId = result.getUploadId();
            UploadPartRequest request2 = new UploadPartRequest();
            request2.setFile(file);
            request2.setPartNumber(1);
            request2.setUploadId(uploadId);
            request2.setBucketName(bucketName);
            request2.setKey(key);
            request2.setPartSize(size5M);
            UploadPartResult result2 = client.uploadPart(request2);
            request2.setFile(file);
            request2.setFileOffset(size5M);
            request2.setPartNumber(2);
            request2.setUploadId(uploadId);
            request2.setBucketName(bucketName);
            request2.setKey(key);
            request2.setPartSize(file.length() - size5M);
            UploadPartResult result3 = client.uploadPart(request2);
            List<PartETag> parts = new ArrayList<PartETag>();
            PartETag e = new PartETag(1, result2.getPartETag().getETag());
            parts.add(e);
            e = new PartETag(2, result3.getPartETag().getETag());
            parts.add(e);
            CompleteMultipartUploadRequest request4 = new CompleteMultipartUploadRequest(
                    bucketName, key, uploadId, parts);
            client.completeMultipartUpload(request4);
            log.info("multi put success:" + key + " length:" + file.length() + " ");
            putSuc.getAndIncrement();
        } catch (Exception e) {
            log.error("multi put error:" + key + " length:" + file.length() + " ", e);
        } finally {
            total.getAndIncrement();
            file.delete();
        }
    }
}

class DelThread_4 implements Runnable {
    private static final Log log = LogFactory.getLog(DelThread.class);
    String key;
    AmazonS3Client client;
    String bucketName;
    
    public DelThread_4(String key, AmazonS3Client client, String bucketName) {
        this.key = key;
        this.client = client;
        this.bucketName = bucketName;
    }
    
    @Override
    public void run() {
        client.deleteObject(bucketName, key);
        log.info("delete success:" + key);
    }
}

class GetThread_4 implements Runnable {
    AtomicLong total;
    AtomicLong readSuc;
    String key;
    String stor;
    long size;
    AmazonS3Client client;
    String bucketName;
    boolean checkEtag;
    private static final Log log = LogFactory.getLog(GetThread.class);
    
    public GetThread_4(AtomicLong readSuc, AtomicLong total, String key, String stor, long size,
            AmazonS3Client client, String bucketName, boolean checkEtag) {
        this.total = total;
        this.readSuc = readSuc;
        this.key = key;
        this.stor = stor;
        this.size = size;
        this.client = client;
        this.bucketName = bucketName;
        this.checkEtag = checkEtag;
        if (checkEtag)
            System.clearProperty("com.amazonaws.services.s3.disableGetObjectMD5Validation");
        else
            System.setProperty("com.amazonaws.services.s3.disableGetObjectMD5Validation", "true");
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
            S3Object object = client.getObject(bucketName, key);
            S3ObjectInputStream input = object.getObjectContent();
            try {
                long crc1 = Long.parseLong(object.getObjectMetadata().getUserMetadata().get("crc"));
                long crc2 = getCRC32Checksum(input);
                ObjectListing ol = client.listObjects(bucketName, key);
                if (crc1 == crc2) {
                    if (ol.getObjectSummaries().get(0).getStorageClass()
                            .equals(object.getObjectMetadata().getUserMetadata().get("storage"))) {
                        log.info("download success " + key + " length:"
                                + object.getObjectMetadata().getContentLength() + " storage:"
                                + object.getObjectMetadata().getUserMetadata().get("storage"));
                        readSuc.getAndIncrement();
                    } else if (object.getObjectMetadata().getUserMetadata().get("storage") == null) {
                        log.info("download success " + key + " length:"
                                + object.getObjectMetadata().getContentLength() + " real storage:"
                                + ol.getObjectSummaries().get(0).getStorageClass());
                        readSuc.getAndIncrement();
                    } else
                        log.error("download fail " + key + " length:"
                                + object.getObjectMetadata().getContentLength() + " meta storage:"
                                + object.getObjectMetadata().getUserMetadata().get("storage")
                                + " real storage:"
                                + ol.getObjectSummaries().get(0).getStorageClass() + " crc1:"
                                + crc1 + " crc2:" + crc2);
                } else
                    log.error("download fail " + key + " length:"
                            + object.getObjectMetadata().getContentLength() + " meta storage:"
                            + object.getObjectMetadata().getUserMetadata().get("storage")
                            + " real storage:" + ol.getObjectSummaries().get(0).getStorageClass()
                            + " crc1:" + crc1 + " crc2:" + crc2);
            } finally {
                if (input != null)
                    input.close();
            }
        } catch (Exception e) {
            log.error("download fail " + key + " length:" + size + " storage:" + stor);
            log.error(e.getMessage(), e);
        } finally {
            total.getAndIncrement();
        }
    }
}

class CheckRepliaThread implements Runnable {
    AtomicLong total;
    AtomicLong checkSuc;
    String key;
    String stor;
    long size;
    AmazonS3Client client;
    String bucketName;
    String metaRegionName;
    boolean checkEtag;
    private static final Log log = LogFactory.getLog(GetThread.class);
    private static long PAGE_SIZE = 512*1024;//8MB/512k=16;
    private static RouteTable route = new RouteTable();
    static{
        route.loadAndListen();
    }
    private static MetaClient oosClient = MetaClient.getGlobalClient();
    private static cn.ctyun.oos.ostor.hbase.OstorHBaseClient ostorClient = new cn.ctyun.oos.ostor.hbase.OstorHBaseClient();
    
    public void checkStorageId(String ostorId, long size) throws Exception{
        log.info("check ostorId:" + ostorId);
        OstorObjectMeta ostorMeta = new OstorObjectMeta(HexUtils.toByteArray(ostorId));
        ostorClient.objectMetaGet(ostorMeta);
        if(ostorMeta.length != size){
            log.info("Bug1: ostorId " + ostorId);
            System.exit(0);
        }

        ReplicaMode replica = ostorMeta.replica;
        int pageNum=-1;
        int fullReplica = -1;
        if(replica.ec_m == 0){
            pageNum = (int) (ostorMeta.length / PAGE_SIZE);
            pageNum = (ostorMeta.length%PAGE_SIZE==0) ? pageNum : pageNum + 1;
            fullReplica = replica.ec_n;
        }else{
            int group = (int) (PAGE_SIZE * replica.ec_n);
            pageNum = (int) (ostorMeta.length / group);
            pageNum = (ostorMeta.length%group==0) ? pageNum : pageNum + 1;
            fullReplica = replica.ec_m + replica.ec_n;
        }
        boolean ostorOk = true;
        for(int page=0; page<pageNum; page++){
            byte[] pageKey = Route.getPageKey(HexUtils.toByteArray(ostorId), page);
            int vdisk = Route.getVDisk(pageKey);
            List<Long> pdisks = route.getVPTable().get(vdisk);
            boolean lost = false;
            for(int i=0; i<fullReplica; i++){
                 long pdisk = pdisks.get(i);
                 if(! pageExist(vdisk, pdisk, ostorMeta.replica, HexUtils.toHexString(pageKey))){
                     lost = true;
                     String host = route.getPDTable().get(pdisk);
                     log.error("find lost page.vdisk:"+vdisk+" pdisk:" + pdisk + " host:" + host + " pageKey:"+ HexUtils.toHexString(pageKey));
                     break;
                 }
            }
            if(lost){
                ostorOk = false;
                log.error("check bad page:" + HexUtils.toHexString(pageKey));
                break;
            }else{
                log.info("check ok page:" + HexUtils.toHexString(pageKey));
            }
        }
        if(ostorOk){
            checkSuc.get();
        }
    }
    private boolean pageExist(int vdisk, long pdiskId, ReplicaMode mode, 
            String pageKey) throws Exception{
        
        boolean exist = false;
        int respCode = 0;
        String host = null;
        StringBuilder reqUrl = new StringBuilder(30);
        try {
            host = route.getPDTable().get(pdiskId);
            if(null == host){
                throw new RuntimeException("Host not exist of pdisk:" + pdiskId);
            }
            reqUrl.append("/?")
                    .append(HttpParams.PARAM_PDISK).append("=").append(pdiskId)
                    .append("&").append(HttpParams.PARAM_VDISK).append("=")
                    .append(vdisk).append("&")
                    .append(HttpParams.PARAM_REPLICATE).append("=")
                    .append(mode.toString()).append("&")
                    .append(HttpParams.PARAM_PAGEKEY).append("=")
                    .append(pageKey);

            respCode = new HttpHead(toInetSocketAddress(host), reqUrl.toString(), 5000, 5000).call();
            if(respCode == HttpParams.GET_CODE){
                exist = true;
            } else if(respCode != HttpParams.NOT_FOUNT_CODE){
                throw new RuntimeException("Check replication exist faild. code:" + respCode);
            }
        } catch (Exception e) {
            log.error("Check Replication exist faild: pageKey=" + pageKey + " pdiskId=" + pdiskId + " host= " + host + " vdisk=" + vdisk);
            throw e;
        }
        return exist;
    }
    private InetSocketAddress toInetSocketAddress(String hp){
        int pos = hp.indexOf(":");
        String host = hp;
        int port = 80;
        if(pos != -1 && pos != hp.length() - 1){
            host = hp.substring(0, pos);
            port = Integer.parseInt(hp.substring(pos + 1));
        }
        return new InetSocketAddress(host, port);
    }
    public CheckRepliaThread(AtomicLong readSuc, AtomicLong total, String key, String stor, long size,
            AmazonS3Client client, String bucketName, String metaRegionName) {
        this.total = total;
        this.checkSuc = readSuc;
        this.key = key;
        this.stor = stor;
        this.size = size;
        this.client = client;
        this.bucketName = bucketName;
        this.metaRegionName=metaRegionName;
    }
    
    @Override
    public void run() {   
        try{
            ObjectMeta object = new ObjectMeta(key, bucketName, metaRegionName);
            if(oosClient.objectSelect(object)){
                if(object.ostorId == null || object.ostorId.equalsIgnoreCase("")){
                    System.out.println(bucketName+"/"+key + " multipart upload");
                    
                    UploadMeta uploadMeta = new UploadMeta(metaRegionName, String.valueOf(object.lastModified), -1);
                    List<UploadMeta> uploads = oosClient.uploadSelectByUploadId(uploadMeta);
                    for(UploadMeta upload : uploads){
                        String ostorId = upload.storageId.split("::")[2];
                        
                        checkStorageId(ostorId, upload.size);
                    }
                }else{
                    log.info(bucketName+"/"+key + " not multipart");
                    
                    checkStorageId(object.storageId.split("::")[2], object.size);
                }
            }else{
                log.error(bucketName+"/"+key+" does not exist");
            }
        }catch(Throwable e){
            log.error(e.getMessage(), e);
        }
    }
}