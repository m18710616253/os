package cn.ctyun.oos.server.stress;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import cn.ctyun.common.Consts;
import cn.ctyun.common.Program;
import cn.ctyun.common.utils.MailUtils;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.util.concurrent.AtomicDouble;

public class OOSSlaMonitor implements Program {

    final static int length = 5 * 1024 * 1024;
    private static int runInter;
    private static String bucketName = "-bucket-name-for-sla-monitor";

    private static String url;
    private static String username;
    private static String password;
    private static int logSize;
    private static String logName;
    private static AmazonS3 asc = null;
    private static String resourcePool = null;

    private static FileWriter fw;

    private static long last = System.currentTimeMillis();
    private static final long ONE_DAY = 24 * 60 * 60 * 1000;
    private static final long ThreeHOUR = 3 * 60 * 60 * 1000;

    private final static Log LOG = LogFactory.getLog(OOSSlaMonitor.class);
    static {
        Properties p = new Properties();
        InputStream is = null;
        try {
            is = new FileInputStream(System.getProperty("user.dir")
                    + "/conf/oosSlaMonitor.conf");
            p.load(is);
            username = p.getProperty("username");
            password = p.getProperty("password");
            url = p.getProperty("url");
            runInter = Integer.parseInt(p.getProperty("runInter"));
            logSize = Integer.parseInt(p.getProperty("logSize"));
            logName = p.getProperty("logName");
            resourcePool = p.getProperty("resourcePool");
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            try {
                if(is != null)
                    is.close();
            } catch (IOException e) {
                LOG.error(e.getMessage(), e);
            }
        }
        ClientConfiguration configuration = new ClientConfiguration();
        configuration.setMaxErrorRetry(0);
        try {
            asc = new AmazonS3Client(new AWSCredentials() {
                public String getAWSAccessKeyId() {
                    return username;
                }

                public String getAWSSecretKey() {
                    return password;
                }
            }, configuration);
            asc.setEndpoint(url);
            
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            throw new Error(e);
        }
    }

    static class Task extends TimerTask {

        static char[] ch = { '1', '2', '3', '4', '5', '6', '7', '8', '9', '0', 
                'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 
                'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', 
                'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L',
                'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X',
                'Y', 'Z'};

        static int len = ch.length;
        static Random rand = new Random();

        public void run() {
            try {
                fw = createNewFile(logName);
            } catch (IOException e) {
                LOG.error(e.getMessage(), e);
            }
            
            List<Bucket> buckets = null;
            try {
                buckets = listBuckets();
            } catch (SLAException e) {
                LOG.error("List error", e);
            } catch (Throwable t){
                LOG.error(t.getMessage(), t);
            }
            
            if(buckets != null)
                do{
                    List<Bucket> tmp = new ArrayList<Bucket>();
                    for(Bucket bucket : buckets){
                        try {
                            deleteBucket(bucket.getName());
                        } catch (SLAException e) {
                            LOG.error("BucketDelete error", e);
                            tmp.add(bucket);
                        } catch (Throwable t){
                            LOG.error(t.getMessage(), t);
                        }
                    }
                    buckets = tmp;
                }while(buckets.size() == 10);
            
            String bucket = null;
            while(true){
                bucket = buildBucketName();
                try {
                    bucketCreate(bucket);
                    break;
                } catch (SLAException e) {
                    LOG.error("BucketCreate error", e);
                    try{
                        Thread.sleep(1000);
                    }catch(Throwable t){
                        ;
                    }
                } catch (Throwable t){
                    LOG.error(t.getMessage(), t);
                    break;
                }
            }
            
            List<String> objects = buildObjectNames();
            for (String object : objects) {
                try {
                    objectPut(object, bucket);
                } catch (SLAException e) {
                    LOG.error("PutObject error", e);
                } catch (Throwable t){
                    LOG.error(t.getMessage(), t);
                }
                try {
                    objectGet(object, bucket);
                } catch (SLAException e) {
                    LOG.error("GetObject error", e);
                } catch (Throwable t){
                    LOG.error(t.getMessage(), t);
                }
            }
            
            ObjectListing result = null;
            try {
                result = listObjects(bucket);
            } catch (SLAException e) {
                LOG.error("ListObjects error", e);
            } catch (Throwable t){
                LOG.error(t.getMessage(), t);
            }
            
            if (result == null) {
                for (String object : objects) {
                    try {
                        objectDelete(object, bucket);
                    } catch (SLAException e) {
                        LOG.error("DeleteObject error", e);
                    } catch (Throwable t){
                        LOG.error(t.getMessage(), t);
                    }
                }
            } else {
                List<S3ObjectSummary> objs = result.getObjectSummaries();
                for (S3ObjectSummary obj : objs) {
                    try {
                        objectDelete(obj.getKey(), bucket);
                    } catch (SLAException e) {
                        LOG.error("DeleteObject error", e);
                    } catch (Throwable t){
                        LOG.error(t.getMessage(), t);
                    }
                }
            }
            
        }

        private List<String> buildObjectNames() {
            List<String> list = new ArrayList<String>();
            for (int i = 0; i < 10; i++) {
                StringBuilder sb = new StringBuilder();
                for (int j = 0; j < 10; j++) {
                    sb.append(ch[rand.nextInt(len)]);
                }
                if (i % 2 == 0)
                    sb.append("/");
                list.add(sb.toString());
            }
            return list;
        }

        private String buildBucketName() {
            StringBuilder sb = new StringBuilder();
            for (int j = 0; j < 10; j++) {
                sb.append(ch[rand.nextInt(36)]);
            }
            return sb.toString() + bucketName;
        }

    }

    private static Runtime exec = Runtime.getRuntime();
    @SuppressWarnings("finally")
    private static void bucketCreate(String bucket) throws IOException, SLAException {
        boolean isExists = false;
        try{
            isExists = asc.doesBucketExist(bucket);
        }catch(Throwable e){
            throw new SLAException(e);
        }
        if (!isExists) {
            CreateBucketRequest createBucketRequest = new CreateBucketRequest(
                    bucket);
            createBucketRequest
                    .setCannedAcl(CannedAccessControlList.PublicRead);
            long start = System.currentTimeMillis();
            try{
                asc.createBucket(createBucketRequest);
            }catch(Throwable t){
                try{
                    writeOneLine("CreateBucketError");
                }finally{
                    throw new SLAException(t);
                }
            }
            long end = System.currentTimeMillis();
            double time = (double) (end - start) / 1000;
            writeOneLine("CreateBucket Time: " + time
                    + " s");
            try{
                Process p = exec.exec("/usr/local/ganglia/bin/gmetric -n BucketCreate -v " + time + " -tfloat -u seconds -g oos-sla");
                InputStream is = p.getErrorStream();
                if(is.available()>0)
                    try{
                        byte[] error = new byte[1024];
                        int size = is.read(error);
                        LOG.error(new String(error, 0, size,Consts.STR_UTF8));
                    }finally{
                        is.close();
                    }
                
            }catch(Throwable t){
                LOG.error("Write to ganglia failed.", t);
            }
            LOG.info("CreateBucket seccess at " + getDate());
        }else
            throw new SLAException(new RuntimeException("BucketExistsError"));
    }

    @SuppressWarnings("finally")
    private static void objectPut(String object, String bucket)
            throws IOException, SLAException {
        final ThreadLocalRandom r = ThreadLocalRandom.current();
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(length);
        long start = System.currentTimeMillis();
        try{
            asc.putObject(bucket, object, new InputStream() {
                int pos = 0;
                public int read() throws IOException {
                    return (pos++ >= length) ? -1 : (r.nextInt() & 0xFF);
                }
            }, metadata);
        }catch(Throwable t){
            try{
                writeOneLine("PutObjectError");
            }finally{
                throw new SLAException(t);
            }
        }
        long end = System.currentTimeMillis();
        double time = (double) (end - start) / 1000;
        writeOneLine("PutObject Time: " + time + " s");
        try{
            Process p = exec.exec("/usr/local/ganglia/bin/gmetric -n ObjectPut -v " + time + " -tfloat -u seconds -g oos-sla");
            InputStream is = p.getErrorStream();
            if(is.available()>0)
                try{
                    byte[] error = new byte[1024];
                    int size = is.read(error);
                    LOG.error(new String(error, 0, size,Consts.STR_UTF8));
                }finally{
                    is.close();
                }
            
        }catch(Throwable t){
            LOG.error("Write to ganglia failed.", t);
        }
        LOG.info("PutObject seccess at " + getDate());
    }

    @SuppressWarnings("finally")
    private static void objectGet(String object, String bucket)
            throws IOException, SLAException {
        GetObjectRequest getObjectRequest = new GetObjectRequest(bucket, object);
        S3Object s3Object = new S3Object();
        long start = System.currentTimeMillis();
        try{
            s3Object = asc.getObject(getObjectRequest);
            if (s3Object != null) {
                InputStream is = s3Object.getObjectContent();
                try {
                    while (is.read() != -1)
                        ;
                } finally {
                    is.close();
                }
                LOG.info("GetObject success at " + getDate());
            }
        }catch(Throwable t){
            try{
                writeOneLine("GetObjectError");
            }finally{
                throw new SLAException(t);
            }
        }
        long end = System.currentTimeMillis();
        double time = (double) (end - start) / 1000;
        writeOneLine("GetObject Time: " + time + " s");
        try{
            Process p = exec.exec("/usr/local/ganglia/bin/gmetric -n ObjectGet -v " + time + " -tfloat -u seconds -g oos-sla");
            InputStream is = p.getErrorStream();
            if(is.available()>0)
                try{
                    byte[] error = new byte[1024];
                    int size = is.read(error);
                    LOG.error(new String(error, 0, size,Consts.STR_UTF8));
                }finally{
                    is.close();
                }
            
        }catch(Throwable t){
            LOG.error("Write to ganglia failed.", t);
        }
    }

    @SuppressWarnings("finally")
    private static void objectDelete(String object, String bucket)
            throws IOException, SLAException {
        DeleteObjectRequest deleteObjectRequest = new DeleteObjectRequest(
                bucket, object);
        long start = System.currentTimeMillis();
        try{
            asc.deleteObject(deleteObjectRequest);
        }catch(Throwable t){
            try{
                writeOneLine("DeleteObjectError");
            }finally{
                throw new SLAException(t);
            }
        }
        long end = System.currentTimeMillis();
        double time = (double) (end - start) / 1000;
        writeOneLine("DeleteObject Time: " + time
                + " s");

        try{
            Process p = exec.exec("/usr/local/ganglia/bin/gmetric -n ObjectDelete -v " + time + " -tfloat -u seconds -g oos-sla");
            InputStream is = p.getErrorStream();
            if(is.available()>0)
                try{
                    byte[] error = new byte[1024];
                    int size = is.read(error);
                    LOG.error(new String(error, 0, size,Consts.STR_UTF8));
                }finally{
                    is.close();
                }
            
        }catch(Throwable t){
            LOG.error("Write to ganglia failed.", t);
        }
        LOG.info("DeleteObject success at " + getDate());
    }

    @SuppressWarnings("finally")
    private static List<Bucket> listBuckets() throws IOException, SLAException {
        List<Bucket> buckets = null;
        long start = System.currentTimeMillis();
        try{
            buckets = asc.listBuckets();
        }catch(Throwable t){
            try{
                writeOneLine("ListBucketError");
            }finally{
                throw new SLAException(t);
            }
        }
        long end = System.currentTimeMillis();
        LOG.info("ListBuckets success at " + getDate());

        double time = (double) (end - start) / 1000;
        try{
            Process p = exec.exec("/usr/local/ganglia/bin/gmetric -n BucketList -v " + time + " -tfloat -u seconds -g oos-sla");
            InputStream is = p.getErrorStream();
            if(is.available()>0)
                try{
                    byte[] error = new byte[1024];
                    int size = is.read(error);
                    LOG.error(new String(error, 0, size,Consts.STR_UTF8));
                }finally{
                    is.close();
                }
            
        }catch(Throwable t){
            LOG.error("Write to ganglia failed.", t);
        }
        try{
            writeOneLine("ListBuckets Time: " + time + " s");
        }finally{
            return buckets;
        }
    }

    @SuppressWarnings("finally")
    private static void deleteBucket(String bucket) throws IOException, SLAException {
            ObjectListing objs = null;
            try{
                objs = listObjects(bucket);
            }catch(IOException e){
                ;
            }
            if(objs == null)
                throw new SLAException("");
            if(!objs.getObjectSummaries().isEmpty())
               for(S3ObjectSummary object : objs.getObjectSummaries())
                   try{
                       objectDelete(object.getKey(), bucket);
                   }catch(IOException e){
                       ;
                   }
        long start = System.currentTimeMillis();
        try{
            asc.deleteBucket(bucket);
        }catch(Throwable t){
            try{
                writeOneLine("DeleteBucketError");
            }finally{
                throw new SLAException(t);
            }
        }
        long end = System.currentTimeMillis();
        double time = (double) (end - start) / 1000;
        try{
            Process p = exec.exec("/usr/local/ganglia/bin/gmetric -n BucketDelete -v " + time + " -tfloat -u seconds -g oos-sla");
            InputStream is = p.getErrorStream();
            if(is.available()>0)
                try{
                    byte[] error = new byte[1024];
                    int size = is.read(error);
                    LOG.error(new String(error, 0, size,Consts.STR_UTF8));
                }finally{
                    is.close();
                }
            
        }catch(Throwable t){
            LOG.error("Write to ganglia failed.", t);
        }
        writeOneLine("DeleteBucket Time: " + time
                + " s");
        LOG.info("DeleteBucket success at " + getDate());
    }

    @SuppressWarnings("finally")
    private static ObjectListing listObjects(String bucket)
            throws IOException, SLAException {
        ObjectListing result;
        try {
            ListObjectsRequest request = new ListObjectsRequest();
            request.setBucketName(bucket);
            request.setDelimiter("/");
            long start = System.currentTimeMillis();
            try{
                result = asc.listObjects(request);
            }catch(Throwable t){
                try{
                    writeOneLine("ListObjectError");
                }finally{
                    throw new SLAException(t);
                }
            }
            long end = System.currentTimeMillis();
            double time = (double) (end - start) / 1000;
            try{
                Process p = exec.exec("/usr/local/ganglia/bin/gmetric -n ObjectList -v " + time + " -tfloat -u seconds -g oos-sla");
                InputStream is = p.getErrorStream();
                if(is.available()>0)
                    try{
                        byte[] error = new byte[1024];
                        int size = is.read(error);
                        LOG.error(new String(error, 0, size,Consts.STR_UTF8));
                    }finally{
                        is.close();
                    }
                
            }catch(Throwable t){
                LOG.error("Write to ganglia failed.", t);
            }
            writeOneLine("ListObjects Time: "
                    + time + " s");
            /*
             * List<String> cp = result.getCommonPrefixes();
             * List<S3ObjectSummary> os = result.getObjectSummaries();
             */
            /*
             * if(cp.size() + os.size() != objects.size())
             * log.info("List Object Size Error"); else
             * log.info("List success");
             */
            LOG.info("ListObjects By Delimiter success at " + getDate());
        } catch(Throwable t){
            LOG.error("ListObjects Error", t);
        } finally {
            ListObjectsRequest request = new ListObjectsRequest();
            request.setBucketName(bucket);
            long start = System.currentTimeMillis();
            try{
                result = asc.listObjects(request);
            }catch(Throwable t){
                try{
                    writeOneLine("ListObjectError");
                }finally{
                    throw new SLAException(t);
                }
            }
            long end = System.currentTimeMillis();
            try{
                LOG.info("ListObjects success " + getDate());
                writeOneLine("ListObjects Time: " + (double) (end - start) / 1000 + " s");
            }finally{
                return result;
            }
            /*
             * List<String> cp = result.getCommonPrefixes();
             * List<S3ObjectSummary> os = result.getObjectSummaries();
             */
            /*
             * if(cp.size() + os.size() != objects.size())
             * log.info("List Object Size Error"); else
             * log.info("List success");
             */
            //LOG.info("ListObjects success " + getDate());
        }
        
    }

    private static void writeOneLine(String line) throws IOException {
        fw.write(line);
        fw.write(System.lineSeparator());
        fw.flush();
    }

    private static FileWriter createNewFile(String name) throws IOException {
        DateFormat yyyyMMdd = new SimpleDateFormat("yyyy_MM_dd_HH",
                Locale.ENGLISH);
        File file = new File(name);
        if (!file.exists()) {
            if (!file.getParentFile().exists())
                if (!file.getParentFile().mkdirs()) {
                    LOG.error("create file failed, name is: " + name);
                    return null;
                }

            if (!file.createNewFile()) {
                throw new RuntimeException("Create LogFile failed.");
            }
        } else if (System.currentTimeMillis() - last > ThreeHOUR) {
            if (fw != null)
                fw.close();
            final String newFile = name + "_" + yyyyMMdd.format(new Date());
            boolean flag = file.renameTo(new File(newFile));
            if (flag){
                LOG.info("Rename file " + name + " to file " + newFile
                        + " success.");
                final long d = System.currentTimeMillis() - last;
                last = System.currentTimeMillis();
                new HandleLog(newFile, d).start();
            }else {
                LOG.info("Rename file " + name + " to file " + newFile
                        + " failed.");
            }
            boolean success = file.createNewFile();
            if (!success)
                LOG.error("create file failed, name is: " + name);
            else
                LOG.info("Create new file " + name + " success.");
        }
        return new FileWriter(name, true);
    }

    private static String getDate() {
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return df.format(new Date());
    }
    
    static class SLAException extends Throwable{
        private static final long serialVersionUID = 2505376009586422250L;

        SLAException(Throwable t){
            super(t);
        }
        
        SLAException(String t){
            super(t);
        }
    }
    
    static class HandleLog extends Thread{
        String fileName;
        long time;
        HandleLog(String fileName, long time){
            this.fileName = fileName;
            this.time = time;
        }
        static String[] opts = {"PutObject", "PutObject_num","PutObjectError","PutObject_0.3","PutObject_0.5","PutObject_1.0","PutObject_1.5","PutObject_2.0","PutObject_5.0"
            ,"PutObject_10.0","PutObject_11","GetObject", "GetObject_num","GetObjectError","GetObject_0.3","GetObject_0.5","GetObject_1.0","GetObject_1.5","GetObject_2.0","GetObject_5.0"
            ,"GetObject_10.0","GetObject_11","DeleteObject", "DeleteObject_num","DeleteObjectError","DeleteObject_0.3","DeleteObject_0.5","DeleteObject_1.0","DeleteObject_1.5","DeleteObject_2.0","DeleteObject_5.0"
            ,"DeleteObject_10.0","DeleteObject_11","ListObjects", "ListObjects_num","ListObjectError","ListObjects_0.3","ListObjects_0.5","ListObjects_1.0","ListObjects_1.5","ListObjects_2.0","ListObjects_5.0"
            ,"ListObjects_10.0","ListObjects_11","CreateBucket", "CreateBucket_num","CreateBucketError","CreateBucket_0.3","CreateBucket_0.5","CreateBucket_1.0","CreateBucket_1.5","CreateBucket_2.0","CreateBucket_5.0"
            ,"CreateBucket_10.0","CreateBucket_11","DeleteBucket", "DeleteBucket_num","DeleteBucketError","DeleteBucket_0.3","DeleteBucket_0.5","DeleteBucket_1.0","DeleteBucket_1.5","DeleteBucket_2.0","DeleteBucket_5.0"
            ,"DeleteBucket_10.0","DeleteBucket_11","ListBuckets", "ListBuckets_num","ListBucketError","ListBuckets_0.3","ListBuckets_0.5","ListBuckets_1.0","ListBuckets_1.5","ListBuckets_2.0","ListBuckets_5.0"
            ,"ListBuckets_10.0","ListBuckets_11"};
        public void run(){
            try {
                String line;
                String[] parts;
                Map<String, AtomicLong> sum = new HashMap<String, AtomicLong>();
                Map<String, AtomicDouble> sum2 = new HashMap<String, AtomicDouble>();
                for(String ss : opts){
                    sum.put(ss, new AtomicLong(0));
                }
                
                BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(fileName)));
                try{
                    while((line = br.readLine()) != null){
                        parts = line.split(" ", -1);
                        String opt = parts[0];
                        double v = 0;
                        if(parts.length == 1){
                            sum.get(opt).incrementAndGet();
                            //sum.get(opt.substring(0, opt.indexOf("Error")) + "_num").incrementAndGet();
                            continue;
                        }else{
                            if(parts.length != 4)
                                continue;
                            v = Double.parseDouble(parts[2]);
                        }
                        if(!sum2.containsKey(opt))
                            sum2.put(opt, new AtomicDouble(v));
                        else 
                            sum2.get(opt).addAndGet(v);
                        
                        if(v < 0.3)
                                sum.get(opt + "_0.3").incrementAndGet();
                        if(v < 0.5)
                                sum.get(opt + "_0.5").incrementAndGet();
                        if(v < 1.0)
                                sum.get(opt + "_1.0").incrementAndGet();
                        if(v < 1.5)
                                sum.get(opt + "_1.5").incrementAndGet();
                        if(v < 2.0)
                                sum.get(opt + "_2.0").incrementAndGet();
                        if(v < 5.0)
                                sum.get(opt + "_5.0").incrementAndGet();
                        if(v < 10.0){
                                sum.get(opt + "_10.0").incrementAndGet();
                        }else{
                                sum.get(opt + "_11").incrementAndGet();
                        }
                        sum.get(opt + "_num").incrementAndGet();
                    }
                }finally{
                    br.close();
                }
                
                DecimalFormat df1 = new DecimalFormat("00.00000000");
                DecimalFormat df2 = new DecimalFormat("00.000");
                StringBuilder body = new StringBuilder("在过去的");
                body.append(time / 3600000).append("小时").append(time % 3600000 / 60000).append("分内，共进行了")
                .append(sum.get("PutObject_num").get()).append("次Put Object，").append(sum.get("GetObject_num").get()).append("次Get Object，")
                .append(sum.get("DeleteObject_num").get()).append("次Delete Object，").append(sum.get("ListObjects_num").get()).append("次List Objects，")
                .append(sum.get("CreateBucket_num").get()).append("次Create Bucket，").append(sum.get("DeleteBucket_num").get()).append("次Delete Bucket，")
                .append(sum.get("ListBuckets_num").get()).append("次List Buckets。具体统计如下：").append("</br>");
                body.append("<table  height=\"304\" border=\"1\">").append("<tr>").append("<td width=\"97\">&nbsp;</td>").append("<td width=\"64\">总次数</td>")
                .append("<td width=\"64\">错误率</td>").append("<td width=\"64\">平均时间</td>").append("<td width=\"64\">&lt;300ms</td>").append("<td width=\"64\">&lt;500ms</td>")
                .append("<td width=\"64\">&lt;1.0s</td>").append("<td width=\"64\">&lt;1.5s</td>").append("<td width=\"64\">&lt;2.0s</td>")
                .append("<td width=\"64\">&lt;5.0s</td>").append("<td width=\"65\">&lt;10.0s</td>").append("<td width=\"64\">&gt;=10.0s</td>")
                .append("</tr>");
                body.append("<tr>").append("<td>Put Object</td>").append("<td>").append(sum.get("PutObject_num").get() + sum.get("PutObjectError").get()).append("</td>")
                .append("<td>").append(df2.format((double)sum.get("PutObjectError").get() / (sum.get("PutObject_num").get() + sum.get("PutObjectError").get()) * 100)).append("%</td>")
                .append("<td>").append(df2.format((double)(sum2.get("PutObject").get()/sum.get("PutObject_num").get()))).append("s").append("</td>")
                .append("<td>").append(df1.format((double)sum.get("PutObject_0.3").get() / sum.get("PutObject_num").get() * 100)).append("%</td>")
                .append("<td>").append(df1.format((double)(sum.get("PutObject_0.5").get()) / sum.get("PutObject_num").get() * 100)).append("%</td>")
                .append("<td>").append(df1.format((double)(sum.get("PutObject_1.0").get()) / sum.get("PutObject_num").get() * 100)).append("%</td>")
                .append("<td>").append(df1.format((double)(sum.get("PutObject_1.5").get()) / sum.get("PutObject_num").get() * 100)).append("%</td>")
                .append("<td>").append(df1.format((double)(sum.get("PutObject_2.0").get()) / sum.get("PutObject_num").get() * 100)).append("%</td>")
                .append("<td>").append(df1.format((double)(sum.get("PutObject_5.0").get()) / sum.get("PutObject_num").get() * 100)).append("%</td>")
                .append("<td>").append(df1.format((double)(sum.get("PutObject_10.0").get()) / sum.get("PutObject_num").get() * 100)).append("%</td>")
                .append("<td>").append(df1.format((double)(sum.get("PutObject_11").get()) / sum.get("PutObject_num").get() * 100)).append("%</td>").append("</tr>");
                
                body.append("<tr>").append("<td>Get Object</td>").append("<td>").append(sum.get("GetObject_num").get() + sum.get("GetObjectError").get()).append("</td>")
                .append("<td>").append(df2.format((double)sum.get("GetObjectError").get() / (sum.get("GetObject_num").get() + sum.get("GetObjectError").get()) * 100)).append("%</td>")
                .append("<td>").append(df2.format((double)(sum2.get("GetObject").get()/sum.get("GetObject_num").get()))).append("s").append("</td>")
                .append("<td>").append(df1.format((double)sum.get("GetObject_0.3").get() / sum.get("GetObject_num").get() * 100)).append("%</td>")
                .append("<td>").append(df1.format((double)(sum.get("GetObject_0.5").get()) / sum.get("GetObject_num").get() * 100)).append("%</td>")
                .append("<td>").append(df1.format((double)(sum.get("GetObject_1.0").get()) / sum.get("GetObject_num").get() * 100)).append("%</td>")
                .append("<td>").append(df1.format((double)(sum.get("GetObject_1.5").get()) / sum.get("GetObject_num").get() * 100)).append("%</td>")
                .append("<td>").append(df1.format((double)(sum.get("GetObject_2.0").get()) / sum.get("GetObject_num").get() * 100)).append("%</td>")
                .append("<td>").append(df1.format((double)(sum.get("GetObject_5.0").get()) / sum.get("GetObject_num").get() * 100)).append("%</td>")
                .append("<td>").append(df1.format((double)(sum.get("GetObject_10.0").get()) / sum.get("GetObject_num").get() * 100)).append("%</td>")
                .append("<td>").append(df1.format((double)(sum.get("GetObject_11").get()) / sum.get("GetObject_num").get() * 100)).append("%</td>").append("</tr>");
                
                body.append("<tr>").append("<td>Delete Object</td>").append("<td>").append(sum.get("DeleteObject_num").get() + sum.get("DeleteObjectError").get()).append("</td>")
                .append("<td>").append(df2.format((double)sum.get("DeleteObjectError").get() / (sum.get("DeleteObject_num").get() + sum.get("DeleteObjectError").get()) * 100)).append("%</td>")
                .append("<td>").append(df2.format((double)(sum2.get("DeleteObject").get()/sum.get("DeleteObject_num").get()))).append("s").append("</td>")
                .append("<td>").append(df1.format((double)sum.get("DeleteObject_0.3").get() / sum.get("DeleteObject_num").get() * 100)).append("%</td>")
                .append("<td>").append(df1.format((double)(sum.get("DeleteObject_0.5").get()) / sum.get("DeleteObject_num").get() * 100)).append("%</td>")
                .append("<td>").append(df1.format((double)(sum.get("DeleteObject_1.0").get()) / sum.get("DeleteObject_num").get() * 100)).append("%</td>")
                .append("<td>").append(df1.format((double)(sum.get("DeleteObject_1.5").get()) / sum.get("DeleteObject_num").get() * 100)).append("%</td>")
                .append("<td>").append(df1.format((double)(sum.get("DeleteObject_2.0").get()) / sum.get("DeleteObject_num").get() * 100)).append("%</td>")
                .append("<td>").append(df1.format((double)(sum.get("DeleteObject_5.0").get()) / sum.get("DeleteObject_num").get() * 100)).append("%</td>")
                .append("<td>").append(df1.format((double)(sum.get("DeleteObject_10.0").get()) / sum.get("DeleteObject_num").get() * 100)).append("%</td>")
                .append("<td>").append(df1.format((double)(sum.get("DeleteObject_11").get()) / sum.get("DeleteObject_num").get() * 100)).append("%</td>").append("</tr>");
                
                body.append("<tr>").append("<td>List Objects</td>").append("<td>").append(sum.get("ListObjects_num").get() + sum.get("ListObjectError").get()).append("</td>")
                .append("<td>").append(df2.format((double)sum.get("ListObjectError").get() / (sum.get("ListObjects_num").get() + sum.get("ListObjectError").get()) * 100)).append("%</td>")
                .append("<td>").append(df2.format((double)(sum2.get("ListObjects").get()/sum.get("ListObjects_num").get()))).append("s").append("</td>")
                .append("<td>").append(df1.format((double)sum.get("ListObjects_0.3").get() / sum.get("ListObjects_num").get() * 100)).append("%</td>")
                .append("<td>").append(df1.format((double)(sum.get("ListObjects_0.5").get()) / sum.get("ListObjects_num").get() * 100)).append("%</td>")
                .append("<td>").append(df1.format((double)(sum.get("ListObjects_1.0").get()) / sum.get("ListObjects_num").get() * 100)).append("%</td>")
                .append("<td>").append(df1.format((double)(sum.get("ListObjects_1.5").get()) / sum.get("ListObjects_num").get() * 100)).append("%</td>")
                .append("<td>").append(df1.format((double)(sum.get("ListObjects_2.0").get()) / sum.get("ListObjects_num").get() * 100)).append("%</td>")
                .append("<td>").append(df1.format((double)(sum.get("ListObjects_5.0").get()) / sum.get("ListObjects_num").get() * 100)).append("%</td>")
                .append("<td>").append(df1.format((double)(sum.get("ListObjects_10.0").get()) / sum.get("ListObjects_num").get() * 100)).append("%</td>")
                .append("<td>").append(df1.format((double)(sum.get("ListObjects_11").get()) / sum.get("ListObjects_num").get() * 100)).append("%</td>").append("</tr>");
                
                body.append("<tr>").append("<td>Create bucket</td>").append("<td>").append(sum.get("CreateBucket_num").get() + sum.get("CreateBucketError").get()).append("</td>")
                .append("<td>").append(df2.format((double)sum.get("CreateBucketError").get() / (sum.get("CreateBucket_num").get() + sum.get("CreateBucketError").get()) * 100)).append("%</td>")
                .append("<td>").append(df2.format((double)(sum2.get("CreateBucket").get()/sum.get("CreateBucket_num").get()))).append("s").append("</td>")
                .append("<td>").append(df1.format((double)sum.get("CreateBucket_0.3").get() / sum.get("CreateBucket_num").get() * 100)).append("%</td>")
                .append("<td>").append(df1.format((double)(sum.get("CreateBucket_0.5").get()) / sum.get("CreateBucket_num").get() * 100)).append("%</td>")
                .append("<td>").append(df1.format((double)(sum.get("CreateBucket_1.0").get()) / sum.get("CreateBucket_num").get() * 100)).append("%</td>")
                .append("<td>").append(df1.format((double)(sum.get("CreateBucket_1.5").get()) / sum.get("CreateBucket_num").get() * 100)).append("%</td>")
                .append("<td>").append(df1.format((double)(sum.get("CreateBucket_2.0").get()) / sum.get("CreateBucket_num").get() * 100)).append("%</td>")
                .append("<td>").append(df1.format((double)(sum.get("CreateBucket_5.0").get()) / sum.get("CreateBucket_num").get() * 100)).append("%</td>")
                .append("<td>").append(df1.format((double)(sum.get("CreateBucket_10.0").get()) / sum.get("CreateBucket_num").get() * 100)).append("%</td>")
                .append("<td>").append(df1.format((double)(sum.get("CreateBucket_11").get()) / sum.get("CreateBucket_num").get() * 100)).append("%</td>").append("</tr>");
                
                body.append("<tr>").append("<td>Delete bucket</td>").append("<td>").append(sum.get("DeleteBucket_num").get() + sum.get("DeleteBucketError").get()).append("</td>")
                .append("<td>").append(df2.format((double)sum.get("DeleteBucketError").get() / (sum.get("DeleteBucket_num").get() + sum.get("DeleteBucketError").get()) * 100)).append("%</td>")
                .append("<td>").append(df2.format((double)(sum2.get("DeleteBucket").get()/sum.get("DeleteBucket_num").get()))).append("s").append("</td>")
                .append("<td>").append(df1.format((double)sum.get("DeleteBucket_0.3").get() / sum.get("DeleteBucket_num").get() * 100)).append("%</td>")
                .append("<td>").append(df1.format((double)(sum.get("DeleteBucket_0.5").get()) / sum.get("DeleteBucket_num").get() * 100)).append("%</td>")
                .append("<td>").append(df1.format((double)(sum.get("DeleteBucket_1.0").get()) / sum.get("DeleteBucket_num").get() * 100)).append("%</td>")
                .append("<td>").append(df1.format((double)(sum.get("DeleteBucket_1.5").get()) / sum.get("DeleteBucket_num").get() * 100)).append("%</td>")
                .append("<td>").append(df1.format((double)(sum.get("DeleteBucket_2.0").get()) / sum.get("DeleteBucket_num").get() * 100)).append("%</td>")
                .append("<td>").append(df1.format((double)(sum.get("DeleteBucket_5.0").get()) / sum.get("DeleteBucket_num").get() * 100)).append("%</td>")
                .append("<td>").append(df1.format((double)(sum.get("DeleteBucket_10.0").get()) / sum.get("DeleteBucket_num").get() * 100)).append("%</td>")
                .append("<td>").append(df1.format((double)(sum.get("DeleteBucket_11").get()) / sum.get("DeleteBucket_num").get() * 100)).append("%</td>").append("</tr>");
                
                body.append("<tr>").append("<td>List Buckets</td>").append("<td>").append(sum.get("ListBuckets_num").get() + sum.get("ListBucketError").get()).append("</td>")
                .append("<td>").append(df2.format((double)sum.get("ListBucketError").get() / (sum.get("ListBuckets_num").get() + sum.get("ListBucketError").get()) * 100)).append("%</td>")
                .append("<td>").append(df2.format((double)(sum2.get("ListBuckets").get()/sum.get("ListBuckets_num").get()))).append("s").append("</td>")
                .append("<td>").append(df1.format((double)sum.get("ListBuckets_0.3").get() / sum.get("ListBuckets_num").get() * 100)).append("%</td>")
                .append("<td>").append(df1.format((double)(sum.get("ListBuckets_0.5").get()) / sum.get("ListBuckets_num").get() * 100)).append("%</td>")
                .append("<td>").append(df1.format((double)(sum.get("ListBuckets_1.0").get()) / sum.get("ListBuckets_num").get() * 100)).append("%</td>")
                .append("<td>").append(df1.format((double)(sum.get("ListBuckets_1.5").get()) / sum.get("ListBuckets_num").get() * 100)).append("%</td>")
                .append("<td>").append(df1.format((double)(sum.get("ListBuckets_2.0").get()) / sum.get("ListBuckets_num").get() * 100)).append("%</td>")
                .append("<td>").append(df1.format((double)(sum.get("ListBuckets_5.0").get()) / sum.get("ListBuckets_num").get() * 100)).append("%</td>")
                .append("<td>").append(df1.format((double)(sum.get("ListBuckets_10.0").get()) / sum.get("ListBuckets_num").get() * 100)).append("%</td>")
                .append("<td>").append(df1.format((double)(sum.get("ListBuckets_11").get()) / sum.get("ListBuckets_num").get() * 100)).append("%</td>").append("</tr>");
                
                body.append("</table>");
                body.append("</br>").append("注：所以的测试都是在没有错误重试的情况下进行的，错误率=错误次数/操作总次数；平均时间=成功操作总时间/成功操作总次数；<300ms的比率=（<300ms的总次数，其中不包含失败的操作）/（成功操作的总次数）；<500ms，<1.0s等等的计算方式与<300ms的计算方式相同。");
                MailUtils.sendMail(resourcePool + " SLA", resourcePool + " SLA Monitor", body.toString());
            } catch (Throwable t) {
                LOG.error("Handle slaLog Error", t);
            }
        }
    }

    @Override
    public String usage() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void exec(String[] args) throws Exception {
        Timer timer = new Timer();
        timer.schedule(new Task(), 0, runInter);
    }
    
    public static void main(String[] args) {
        Timer timer = new Timer();
        timer.schedule(new Task(), 0, runInter);
    }
}
