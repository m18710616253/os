package cn.ctyun.oos.tempo;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.security.SecureRandom;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.HttpMultipartMode;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.entity.mime.content.StringBody;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.SigningAlgorithm;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.Headers;
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

import cn.ctyun.common.Consts;
import cn.ctyun.oos.common.Utils;
import cn.ctyun.oos.ostor.utils.CRCUtils;
import common.io.StreamUtils;
import common.util.BlockingExecutor;
import common.util.HexUtils;

// 先put，然后list object, get下来校验crc
public class PutGetTest {
    // final static String OOS_DOMAIN = "http://oos-sh.ctyunapi.cn";
    // final static String bucketName = "12345";
    static {
        System.setProperty("log4j.log.app", "putget");
    }
    private static final Log log = LogFactory.getLog(PutGetTest.class);
    
    static String OOS_DOMAIN = "https://oos-js.ctyunapi.cn";
    static String bucketName = "test";
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
    
    private static String generateFilename(String prefix, Random rand) {
        byte[] tmp = new byte[32];
        rand.nextBytes(tmp);
        return prefix + HexUtils.toHexString(tmp);
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
        final AtomicLong putSuc = new AtomicLong();
        final AtomicLong postSuc = new AtomicLong();
        final AtomicLong delSuc = new AtomicLong();
        Thread t2 = new Thread() {
            @Override
            public void run() {
                for (;;) {
                    log.info("Total = " + total.get());
                    log.info("ReadSuc = " + readSuc.get());
                    log.info("PutSuc = " + putSuc.get());
                    log.info("PostSuc = " + postSuc.get());
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
        
        String type= null;
        String region = null;
        String strategy = null;
        BlockingExecutor executor = null;
        if (param.equals("put")) {
            int putNum = Integer.valueOf(args[6]);       // put多少个object
            int threadNum = Integer.valueOf(args[7]);
            long seed = Long.valueOf(args[8]);
            if (args.length > 9 && args[9] != null) {
                type = String.valueOf(args[9]);
            }
            if (args.length > 10 && args[10] != null) {
                region = String.valueOf(args[10]);
            }
            if (args.length > 11 && args[11] != null) {
                strategy = String.valueOf(args[11]);
            }
            Random rand = new Random(seed);
            executor = new BlockingExecutor(threadNum, threadNum, threadNum, 3000, "PutThread");
            for (int i = 0; i < putNum; i++) {
                long length = SimpleTest.getLength();
                String objectName = generateFilename("putget_", rand);
                executor.execute(new PutThread(putSuc, total, length, client, bucketName, sts, objectName,
                        type, region, strategy));
            }
        } else if (param.equals("post")) {
            int putNum = Integer.valueOf(args[6]); // post多少个object
            int threadNum = Integer.valueOf(args[7]);
            long seed = Long.valueOf(args[8]);
            if (args.length > 9 && args[9] != null) {
                type = String.valueOf(args[9]);
            }
            if (args.length > 10 && args[10] != null) {
                region = String.valueOf(args[10]);
            }
            if (args.length > 11 && args[11] != null) {
                strategy = String.valueOf(args[11]);
            }
            Random rand = new Random(seed);
            executor = new BlockingExecutor(threadNum, threadNum, threadNum, 3000, "PutThread");
            for (int i = 0; i < putNum; i++) {
                long length = SimpleTest.getLength();
                String objectName = generateFilename("putget_post_", rand);
                executor.execute(new PostThread(postSuc, total, length, client, bucketName, sts, objectName, 
                        ak, sk, OOS_DOMAIN, type, region, strategy));
            }
        } else if (param.equals("del")) {// 删除所有Object
            int threadN = Integer.valueOf(args[6]);
            BlockingQueue<S3ObjectSummary> objs = new LinkedBlockingQueue<>();
            AtomicBoolean isDone = new AtomicBoolean(false);
            new Thread() {
                public void run() {
                    ListObjectsRequest request = new ListObjectsRequest();
                    request.setBucketName(bucketName);
                    if (args.length > 7)
                        request.setPrefix(args[7]);
                    request.setMaxKeys(1000);
                    do{
                        ObjectListing os = client.listObjects(request);
                        List<S3ObjectSummary> list = os.getObjectSummaries();
                        if(!list.isEmpty())
                            for(S3ObjectSummary o : list)
                                try {
                                    objs.put(o);
                                } catch (InterruptedException e) {
                                    e.printStackTrace();
                                    System.exit(-1);
                                }
                        else {
                            isDone.set(true);
                            break;
                        }
                        request.setMarker(os.getNextMarker());
                    } while(true);
                };
            }.start();
            
            final CountDownLatch latch  = new CountDownLatch(threadN);
            for(int i = 0; i < threadN; i++) {
                new Thread() {
                    public void run() {
                        while(true) {
                            S3ObjectSummary obj = null;
                            try {
                                obj = objs.poll(1000, TimeUnit.MILLISECONDS);
                            } catch (InterruptedException e) {
                                continue;
                            }
                            if(obj == null && isDone.get()) {
                                latch.countDown();
                                break;
                            } else if(obj != null)
                                new DelThread(delSuc, obj.getKey(), client, bucketName).run();
                        }
                        
                    };
                }.start();
            }
            while(true){
                try {
                    latch.await();
                    break;
                } catch (InterruptedException e) {
                    continue;
                }
            }
        } else if (param.equals("delPrefix")) {
            int threadNum = Integer.valueOf(args[6]);
            String prefix = args[7];
            int objectNum = Integer.valueOf(args[8]);
            long seed = Long.valueOf(args[9]);
            Random rand = new Random(seed);
            final CountDownLatch latch  = new CountDownLatch(threadNum);
            AtomicInteger counter = new AtomicInteger();
            for (int i = 0; i < threadNum; i++) {
                new Thread() {
                    public void run() {
                        while(true) {
                            int num = counter.getAndAdd(1);
                            if (num < objectNum) {
                                String objectName;
                                synchronized (rand) {
                                    objectName = generateFilename(prefix, rand);
                                }
                                new DelThread(delSuc, objectName, client, bucketName).run();
                            } else {
                                latch.countDown();
                                return;
                            }
                        }
                    };
                }.start();
            }
            while (true) {
                try {
                    latch.await();
                    break;
                } catch (InterruptedException e) {
                    continue;
                }
            }
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
                    long length = SimpleTest.getLength();
                    if (args.length > 8 && args[8] != null)
                        type = String.valueOf(args[8]);
                    if (args.length > 9 && args[9] != null)
                        region = String.valueOf(args[9]);
                    if (args.length > 10 && args[10] != null)
                        strategy = String.valueOf(args[10]);
                    executor.execute(new PutThread(putSuc, total, length, client, bucketName, sts,
                            o.getKey(), type, region, strategy));
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
        } else if (param.equals("mulput")) {
            int putNum = Integer.valueOf(args[6]); // put多少个object
            int threadNum = Integer.valueOf(args[7]);
            long seed = Long.valueOf(args[8]);
            if (args.length > 9 && args[9] != null) {
                type = String.valueOf(args[9]);
            }
            if (args.length > 10 && args[10] != null) {
                region = String.valueOf(args[10]);
            }
            if (args.length > 11 && args[11] != null) {
                strategy = String.valueOf(args[11]);
            }
            Random rand = new Random(seed);
            executor = new BlockingExecutor(threadNum, threadNum, threadNum, 3000, "MulPutThread");
            for (int i = 0; i < putNum; i++) {
                long length = getBigLength();
                String objectName = generateFilename("putget_mul_", rand);
                executor.execute(new MulPutThread(putSuc, total, length, client, bucketName, sts, type, region, 
                        strategy, objectName));
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
        
        if (executor != null) {
            while (true) {
                log.info(executor.getActiveCount());
                if (executor.getActiveCount() > 0)
                    Thread.sleep(1000);
                else
                    break;
            }
        }
        log.info("Total = " + total.get());
        log.info("readSuc = " + readSuc.get());
        log.info("PutSuc = " + putSuc.get());
        log.info("PostSuc = " + postSuc.get());
        log.info("DelSuc = " + delSuc.get());
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

class PutThread implements Runnable {
    AtomicLong total;
    AtomicLong putSuc;
    long length;
    AmazonS3Client client;
    String bucketName;
    private static final Log log = LogFactory.getLog(PutThread.class);
    String sts;
    String key;
    String type;
    String region;
    String strategy;
    
    public PutThread(AtomicLong putSuc, AtomicLong total, long length, AmazonS3Client client,
            String bucketName, String sts, String key,String Type,
            String Region,String Strategy) {
        this.total = total;
        this.putSuc = putSuc;
        this.length = length;
        this.client = client;
        this.bucketName = bucketName;
        this.sts = sts;
        if(Type!=null)
            this.type = Type;
        if(Region!=null)
            this.region = Region;
        if(Strategy!=null)
            this.strategy = Strategy;
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
            
            if (sts.equals("STANDARD") || sts.equals("REDUCED_REDUNDANCY") || sts.equals("STANDARD_IA") ||!sts.equals("NotDefined")) {
                String storage = SimpleTest.getStorageClass(sts);
                putObjectRequest.setStorageClass(storage);
            }
            metadata.addUserMetadata("crc", String.valueOf(CRCUtils.getCRC32Checksum(file)));
            if((type!=null) && (region!=null) && (strategy!=null))
                metadata.setHeader("x-ctyun-data-location", "type="+type+",location="+region+",scheduleStrategy="+strategy);
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

class PostThread implements Runnable {
    AtomicLong total;
    AtomicLong postSuc;
    long length;
    AmazonS3Client client;
    String bucketName;
    private static final Log log = LogFactory.getLog(PutThread.class);
    String sts;
    String key;
    String accessKey;
    String secretKey;
    String domain;
    String type;
    String region;
    String strategy;
    ContentType contentType = ContentType.create("text/plain", Consts.CS_UTF8);
    CloseableHttpClient httpclient = HttpClients.createDefault();
    
    public PostThread(AtomicLong postSuc, AtomicLong total, long length, AmazonS3Client client,
            String bucketName, String sts, String key, String ak, String sk, String domain,String Type,
            String Region,String Strategy) {
        this.total = total;
        this.postSuc = postSuc;
        this.length = length;
        this.client = client;
        this.bucketName = bucketName;
        if(Type!=null)
            this.type = Type;
        if(Region!=null)
            this.region = Region;
        if(Strategy!=null)
            this.strategy = Strategy;
        this.sts = sts;
        if (key != null)
            this.key = key;
        else
            this.key = "putget_post_" + SimpleTest.generateFileName();
        this.accessKey = ak;
        this.secretKey = sk;  
        this.domain = domain;
    }
    
    protected String getBucketUrl(){
        return domain + "/" + bucketName;
    }
    
    @Override
    public void run() {
        File file = new File("simple", key);
        try {
            SimpleTest.generateRandomFile(new File("simple", key), length);
            Map<String,String> headers = new HashMap<String,String>();
            if (sts.equals("STANDARD") || sts.equals("REDUCED_REDUNDANCY") || sts.equals("STANDARD_IA") ||!sts.equals("NotDefined")) {
                String storage = SimpleTest.getStorageClass(sts);
                headers.put(Headers.STORAGE_CLASS, storage);
            }
            HttpResponse response = postObjectWithRequestBody(key, file, headers);
            if(response.getStatusLine().getStatusCode()==204){
                log.info("post success:" + key + " length:" + file.length() + " ");
                postSuc.getAndIncrement();
            }
            else{
                String errorMessage = StringEscapeUtils.unescapeHtml(IOUtils.toString(response.getEntity().getContent()));
                log.error("post error:" + key + " length:" + file.length() + " error message " + errorMessage);
            }  
        } catch (Exception e) {
            log.error("post error:" + key + " length:" + file.length() + " ", e);
        } finally {
            total.getAndIncrement();
            file.delete();
        }
    }
    
    public HttpResponse postObjectWithRequestBody(String key, File file, Map<String,String> headers) throws JSONException, ClientProtocolException, IOException{
        String crc32 = String.valueOf(CRCUtils.getCRC32Checksum(file));
        PolicyBodyBuilder policyBodyBuilder  = new PolicyBodyBuilder()
                .add("bucket", bucketName)
                .add("key", key)
                .add("x-amz-meta-crc", crc32);
        for(Entry<String,String> entry : headers.entrySet()) {
            policyBodyBuilder.add(entry.getKey(), entry.getValue());
        }
        String policy = policyBodyBuilder.build(false, false);
        PostRequestBodyBuilder request = new PostRequestBodyBuilder(key);
        request.AWSAccessKeyId(accessKey)
        .policy(policy)
        .userDefinedParameter("x-amz-meta-crc", crc32);
        for(Entry<String,String> entry : headers.entrySet()) {
            request.userDefinedParameter(entry.getKey(), entry.getValue());
        }
        request.signature(Utils.sign(policy, secretKey, SigningAlgorithm.HmacSHA1));
        
        HttpPost httpPost = new HttpPost(getBucketUrl());
        httpPost.setConfig(RequestConfig.custom()
                .setRedirectsEnabled(false).build());
        MultipartEntityBuilder entity = request.build();
        entity.addBinaryBody("file", file, contentType, key);
        entity.addPart("submit", new StringBody("upload to oos", ContentType.DEFAULT_TEXT));
        httpPost.setEntity(entity.build());
        if((type!=null)&&(region!=null)&&(strategy!=null))
            httpPost.setHeader("x-ctyun-data-location", "type="+type+",location="+region+",scheduleStrategy="+strategy);
        return httpclient.execute(httpPost);
    }
    
    public String buildPolicy(Date expiration, HashMap<String, String> values, boolean useEqual, boolean useStartWith) throws JSONException{
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.sss'Z'");
        String time = sdf.format(expiration);
        JSONObject jo = new JSONObject();
        jo.put("expiration", time);
        JSONArray ja = new JSONArray();
        for(Map.Entry<String, String> entry: values.entrySet()){
            if(entry.getKey().equals("content-length-range")){
                String[] sizes = entry.getValue().split("/");
                String min = sizes[0];
                String max = sizes.length>1?sizes[1]:min;
                JSONArray condition = new JSONArray().put("content-length-range").put(min).put(max);
                ja.put(condition);
                continue;
            }
            if(useEqual){
                JSONArray condition = new JSONArray().put("eq").put(entry.getKey()).put(entry.getValue());
                ja.put(condition);
            }else if(useStartWith){
                JSONArray condition = new JSONArray().put("starts-with").put(entry.getKey()).put(entry.getValue());
                ja.put(condition);
            }
            else{
                JSONObject condition = new JSONObject().put(entry.getKey(), entry.getValue());
                ja.put(condition);
            }
        }
        jo.put("conditions", ja);
        log.info(jo.toString());
        String ps = jo.toString();
        byte[] policyBytes = Base64.encodeBase64(ps.getBytes(Charset.forName("UTF-8")));
        String policyStr = new String(policyBytes, Charset.forName("UTF-8"));
        return policyStr;
        
    }
    
    class PostRequestBodyBuilder{
        String key = null;
        String AWSAccessKeyId = null;
        String cacheControl = null;
        String contentMD5 = null;
        String contentType = null;
        String contentDisposition = null;
        String cotentEncoding = null;
        String expires = null;
        String policy = null;
        String signature = null;
        String successActionRedirect = null;
        String redirect = null;
        String successActionStatus = null;
        String xAmzStorageClass  = null;
        String xAmzWebsiteRedirectLocation = null;
        
        HashMap<String, String> userDefinedParameter = new LinkedHashMap<String, String>();
        
        public PostRequestBodyBuilder(String key){
            this.key = key;
        }
        
        public PostRequestBodyBuilder AWSAccessKeyId(String accessKeyId){
            this.AWSAccessKeyId = accessKeyId;
            return this;
        }
        
        public PostRequestBodyBuilder cacheControl(String cacheControl){
            this.cacheControl = cacheControl;
            return this;
        }
        
        public PostRequestBodyBuilder contentMD5(String contentMD5){
            this.contentMD5 = contentMD5;
            return this;
        }
        
        public PostRequestBodyBuilder contentType(String contentType){
            this.contentType = contentType;
            return this;
        }
        
        public PostRequestBodyBuilder contentDisposition(String contentDisposition){
            this.contentDisposition = contentDisposition;
            return this;
        }
        
        public PostRequestBodyBuilder contentEncoding(String cotentEncoding){
            this.cotentEncoding = cotentEncoding;
            return this;
        }
        
        public PostRequestBodyBuilder expires(String expires){
            this.expires = expires;
            return this;
        }
        
        public PostRequestBodyBuilder policy(String policy){
            this.policy = policy;
            return this;
        }
        
        public PostRequestBodyBuilder signature(String signature){
            this.signature = signature;
            return this;
        }
        
        public PostRequestBodyBuilder successActionRedirect(String successActionRedirect){
            this.successActionRedirect = successActionRedirect;
            return this;
        }
        
        public PostRequestBodyBuilder redirect(String redirect){
            this.redirect = redirect;
            return this;
        }
        
        public PostRequestBodyBuilder successActionStatus(String successActionStatus){
            this.successActionStatus = successActionStatus;
            return this;
        }
        
        public PostRequestBodyBuilder xAmzStorageClass(String xAmzStorageClass){
            this.xAmzStorageClass = xAmzStorageClass;
            return this;
        }
        
        public PostRequestBodyBuilder xAmzWebsiteRedirectLocation(String xAmzWebsiteRedirectLocation){
            this.xAmzWebsiteRedirectLocation = xAmzWebsiteRedirectLocation;
            return this;
        }
        
        public PostRequestBodyBuilder userDefinedParameter(String name, String value){
            this.userDefinedParameter.put(name, value);
            return this;
        }
        
        public MultipartEntityBuilder build(){
            MultipartEntityBuilder entityBuilder = MultipartEntityBuilder.create();
            entityBuilder.setMode(HttpMultipartMode.BROWSER_COMPATIBLE);
            entityBuilder.setCharset(Charset.forName("utf8"));
            ContentType contentType = ContentType.create("text/plain", Charset.forName("utf8"));
            if(null!=this.key)entityBuilder.addPart("key", new StringBody(this.key, contentType));
            if(null!=this.AWSAccessKeyId)entityBuilder.addPart("AWSAccessKeyId", new StringBody(this.AWSAccessKeyId, contentType));
            if(null!=this.cacheControl)entityBuilder.addPart("Cache-Control", new StringBody(this.cacheControl, contentType));
            if(null!=this.contentMD5)entityBuilder.addPart("Content-MD5", new StringBody(this.contentMD5, contentType));
            if(null!=this.contentType)entityBuilder.addPart("Content-Type", new StringBody(this.contentType, contentType));
            if(null!=this.cotentEncoding)entityBuilder.addPart("Content-Encoding", new StringBody(this.cotentEncoding, contentType));
            if(null!=this.contentDisposition)entityBuilder.addPart("Content-Disposition", new StringBody(this.contentDisposition, contentType));
            if(null!=this.expires)entityBuilder.addPart("Expires", new StringBody(this.expires, contentType));
            if(null!=this.policy)entityBuilder.addPart("Policy", new StringBody(this.policy, contentType));
            if(null!=this.signature)entityBuilder.addPart("Signature", new StringBody(this.signature, contentType));
            if(null!=this.successActionRedirect)entityBuilder.addPart("success_action_redirect", new StringBody(this.successActionRedirect, contentType));
            if(null!=this.redirect)entityBuilder.addPart("redirect", new StringBody(this.redirect, contentType));
            if(null!=this.successActionStatus)entityBuilder.addPart("success_action_status", new StringBody(this.successActionStatus, contentType));
            if(null!=this.xAmzStorageClass)entityBuilder.addPart("x-amz-storage-class", new StringBody(this.xAmzStorageClass, contentType));
            if(null!=this.xAmzWebsiteRedirectLocation)entityBuilder.addPart("x-amz-website-redirect-location", new StringBody(this.xAmzWebsiteRedirectLocation, contentType));
            for(Map.Entry<String, String> entry: this.userDefinedParameter.entrySet()){
                entityBuilder.addPart(entry.getKey(), new StringBody(entry.getValue(), contentType));
            }
            return entityBuilder;
        }
    }
    
    class PolicyBodyBuilder{
        HashMap<String, String> conditions = new LinkedHashMap<String, String>();
        
        public PolicyBodyBuilder add(String fieldName, String fieldValue){
            conditions.put(fieldName, fieldValue);
            return this;
        }
        
        public String build(boolean eq, boolean startsWith) throws JSONException{
            Calendar cal = Calendar.getInstance();
            cal.add(Calendar.DAY_OF_MONTH, 1);
            String policy = buildPolicy(cal.getTime(), conditions, eq, startsWith);
            return policy;
        }
        
        public String build(boolean eq, boolean startsWith, Date date) throws JSONException{
            String policy = buildPolicy(date, conditions, eq, startsWith);
            return policy;
        }
    }
}

class MulPutThread implements Runnable {
    AtomicLong total;
    AtomicLong putSuc;
    long length;
    AmazonS3Client client;
    String bucketName;
    private static final Log log = LogFactory.getLog(PutThread.class);
    String sts;
    static int size5M = 5 * 1024 * 1024;
    String type;
    String region;
    String strategy;
    String key;
    
    public MulPutThread(AtomicLong putSuc, AtomicLong total, long length, AmazonS3Client client,
            String bucketName, String sts,String Type,String Region,String Strategy) {
        this(putSuc, total, length, client, bucketName, sts, Type, Region, Strategy, null);
    }
    
    public MulPutThread(AtomicLong putSuc, AtomicLong total, long length, AmazonS3Client client,
            String bucketName, String sts,String Type,String Region,String Strategy, String key) {
        this.total = total;
        this.putSuc = putSuc;
        this.length = length;
        this.client = client;
        this.bucketName = bucketName;
        this.sts = sts;
        if(Type!=null)
            this.type = Type;
        if(Region!=null)
            this.region = Region;
        if(Strategy!=null)
            this.strategy = Strategy;
        if (key == null) {
            this.key = "putget_mul_" + SimpleTest.generateFileName();
        } else {
            this.key = key;
        }
    }
    
    @Override
    public void run() {
        File file = new File("simple", key);
        try {
            SimpleTest.generateRandomFile(new File("simple", key), length);
            InitiateMultipartUploadRequest request = new InitiateMultipartUploadRequest(bucketName,
                    key);
            ObjectMetadata metadata = new ObjectMetadata();
            metadata.addUserMetadata("crc", String.valueOf(CRCUtils.getCRC32Checksum(file)));
            if((type!=null)&&(region!=null)&&(strategy!=null))
                metadata.setHeader("x-ctyun-data-location", "type="+type+",location="+region+",scheduleStrategy="+strategy);
            request.setObjectMetadata(metadata);
            if (sts.equals("STANDARD") || sts.equals("REDUCED_REDUNDANCY") ||!sts.equals("NotDefined")) {
                String storage = SimpleTest.getStorageClass(sts);
                request.setStorageClass(storage);
            }
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

class DelThread implements Runnable {
    private static final Log log = LogFactory.getLog(DelThread.class);
    String key;
    AmazonS3Client client;
    String bucketName;
    AtomicLong delSuc;
    
    public DelThread(AtomicLong delSuc, String key, AmazonS3Client client, String bucketName) {
        this.delSuc = delSuc;
        this.key = key;
        this.client = client;
        this.bucketName = bucketName;
    }
    
    @Override
    public void run() {
        try {
            client.deleteObject(bucketName, key);
            log.info("delete success:" + key);
            delSuc.incrementAndGet();
        } catch (Throwable e) {
            log.error(e.getMessage(), e);
        }
    }
}

class GetThread implements Runnable {
    AtomicLong total;
    AtomicLong readSuc;
    String key;
    String stor;
    long size;
    AmazonS3Client client;
    String bucketName;
    boolean checkEtag;
    private static final Log log = LogFactory.getLog(GetThread.class);
    
    public GetThread(AtomicLong readSuc, AtomicLong total, String key, String stor, long size,
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