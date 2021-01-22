package cn.ctyun.oos.server.stress;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import cn.ctyun.common.Program;
import cn.ctyun.common.utils.MailUtils;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
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

public class OOSSHETESlaMonitor implements Program {

    final static int length = 5 * 1024 * 1024;
    private static int runInter;

    private static String url;
    private static String username;
    private static String password;
    private static AmazonS3 asc = null;
    private static String resourcePool = null;
    private static String bucketName = "test2";

    private static final long ThreeHOUR = 3 * 60 * 60 * 1000;

    private final static Log LOG = LogFactory.getLog(OOSSHETESlaMonitor.class);
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
    
    static LogOneLine oneLine = new LogOneLine(System.getProperty("user.dir")
            + "/slalogs/sla.log", ThreeHOUR);
    
    static ExecutorService exec = Executors.newFixedThreadPool(10);

    static class Task {//extends TimerTask {

        static char[] ch = { '1', '2', '3', '4', '5', '6', '7', '8', '9', '0', 
                'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 
                'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', 
                'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L',
                'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X',
                'Y', 'Z'};

        static int len = ch.length;
        static Random rand = new Random();

        public void run() {
            List<String> objects = buildObjectNames();
            for (String object : objects) {
                try {
                    objectPut(object, bucketName);
                    objectGet(object, bucketName);
                } catch (Throwable t){
                    LOG.error(t.getMessage(), t);
                }
            }
            
            try {
                ObjectListing result = listObjects(bucketName);
                List<S3ObjectSummary> objs = result.getObjectSummaries();
                for (S3ObjectSummary obj : objs) {
                    try {
                        objectDelete(obj.getKey(), bucketName);
                    } catch (Throwable t){
                        LOG.error(t.getMessage(), t);
                    }
                }
            } catch (Throwable t){
                LOG.error(t.getMessage(), t);
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


    }

    private static void bucketCreate(String bucket) throws IOException{
        boolean isExists = asc.doesBucketExist(bucket);
        if (!isExists) {
            CreateBucketRequest createBucketRequest = new CreateBucketRequest(
                    bucket);
            createBucketRequest
                    .setCannedAcl(CannedAccessControlList.PublicRead);
            long start = System.currentTimeMillis();
            try{
                asc.createBucket(createBucketRequest);
                long end = System.currentTimeMillis();
                double time = (double) (end - start) / 1000;
                oneLine.write("CreateBucket Time: " + time + " s");
            }catch(Throwable t){
                oneLine.write("CreateBucketError");
            }
        }
    }


    private static void objectPut(String object, String bucket)
            throws IOException {
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(length);
        try{
            final AtomicLong start = new AtomicLong(System.currentTimeMillis());
            asc.putObject(bucket, object, new InputStream() {
                int pos = 0;
                int v;
                public int read() throws IOException {
                    v = (pos++ >= length) ? -1 : 1;
                    if(pos == length){
                        start.set(System.currentTimeMillis());
                    }
                    return v;
                }
            }, metadata);
            long end = System.currentTimeMillis();
            double time = (double) (end - start.get()) / 1000;
            oneLine.write("PutObject Time: " + time + " s");
        }catch(Throwable t){
            oneLine.write("PutObjectError");
            throw t;
        }
    }

    private static void objectGet(String object, String bucket)
            throws IOException {
        GetObjectRequest getObjectRequest = new GetObjectRequest(bucket, object);
        S3Object s3Object = new S3Object();
        long start = System.currentTimeMillis();
        try{
            s3Object = asc.getObject(getObjectRequest);
            long end = System.currentTimeMillis();
            if (s3Object != null) {
                InputStream is = s3Object.getObjectContent();
                is.read();
                end = System.currentTimeMillis();
                try {
                    while (is.read() != -1)
                        ;
                } finally {
                    is.close();
                }
            }
            double time = (double) (end - start) / 1000;
            oneLine.write("GetObject Time: " + time + " s");
        }catch(Throwable t){
            oneLine.write("GetObjectError");
            throw t;
        }
    }

    private static void objectDelete(String object, String bucket)
            throws IOException {
        DeleteObjectRequest deleteObjectRequest = new DeleteObjectRequest(
                bucket, object);
        long start = System.currentTimeMillis();
        try{
            asc.deleteObject(deleteObjectRequest);
            long end = System.currentTimeMillis();
            double time = (double) (end - start) / 1000;
            oneLine.write("DeleteObject Time: " + time
                    + " s");
        }catch(Throwable t){
            oneLine.write("DeleteObjectError");
            throw t;
        }

    }


    private static ObjectListing listObjects(String bucket)
            throws IOException {
        ObjectListing result;
        try {
            ListObjectsRequest request = new ListObjectsRequest();
            request.setBucketName(bucket);
            request.setDelimiter("/");
            long start = System.currentTimeMillis();
            try{
                result = asc.listObjects(request);
                long end = System.currentTimeMillis();
                double time = (double) (end - start) / 1000;
                oneLine.write("ListObjects Time: "
                        + time + " s");
            }catch(Throwable t){
                oneLine.write("ListObjectError");
                throw t;
            }
        } finally {
            ListObjectsRequest request = new ListObjectsRequest();
            request.setBucketName(bucket);
            long start = System.currentTimeMillis();
            try{
                result = asc.listObjects(request);
                long end = System.currentTimeMillis();
                oneLine.write("ListObjects Time: " + (double) (end - start) / 1000 + " s");
            }catch(Throwable t){
                oneLine.write("ListObjectError");
                throw t;
            }
        }
        return result;
    }

    @Override
    public String usage() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void exec(String[] args) throws Exception {
        bucketCreate(bucketName);
//        Timer timer = new Timer();
//        timer.schedule(new Task(), 0, runInter);
        final Task task = new Task();
        while(true){
            try{
                Future<Object> ft = exec.submit(new Callable<Object>(){
                    public Object call() throws Exception {
                        task.run();
                        return null;
                    }
                    
                });
                int sleepTime = 0;
                while(!ft.isDone()){
                    try{
                        Thread.sleep(200);
                    }catch(Throwable t){
                        ;
                    }
                    sleepTime += 200;
                    if(sleepTime == 100000){
                        ft.cancel(true);
                        break;
                    }
                }
            }catch(Throwable t){
                LOG.error(t.getMessage(), t);
            }
        }
    }
    
    static class LogOneLine {
        private FileWriter fw;
        private long interval = 24 * 60 * 60 * 1000;
        private long last = System.currentTimeMillis();
        private Log log = LogFactory.getLog(LogOneLine.class);
        private String name = System.getProperty("user.dir")
                + "/slalogs/sla.log";

        public LogOneLine(String fileName, long generateNewFileInterval) {
            this.name = fileName;
            this.interval = generateNewFileInterval;
        }

        public synchronized void write(String line) {
            try {
                createNewFile(name);
                try {
                    fw.write(line + System.lineSeparator());
                    fw.flush();
                } catch (IOException e) {
                    log.error("WriteOneLine Error", e);
                }
            } catch (Throwable t) {
                log.error(t.getMessage(), t);
            }
        }

        private void createNewFile(String name) {
            try {
                DateFormat yyyyMMdd = new SimpleDateFormat("yyyy_MM_dd_HH",
                        Locale.ENGLISH);
                File file = new File(name);
                if (!file.exists()) {
                    if (!file.getParentFile().exists())
                        if (!file.getParentFile().mkdirs()) {
                            log.error("Create dir failed, name is: " + name);
                        }
                    if (!file.createNewFile()) {
                        throw new RuntimeException("Create LogFile failed.");
                    }
                    fw = new FileWriter(name, true);
                } else if (System.currentTimeMillis() - last > interval) {
                    if (fw != null)
                        fw.close();
                    try {
                        final String newFile = name + "_"
                                + yyyyMMdd.format(new Date());
                        boolean flag = file.renameTo(new File(newFile));
                        if (flag) {
                            log.info("Rename file " + name + " to file " + newFile
                                    + " success.");
                            new HandleLog(newFile, System.currentTimeMillis() - last).start();
                            last = System.currentTimeMillis();
                        } else {
                            log.info("Rename file " + name + " to file " + newFile
                                    + " failed.");
                        }
                        boolean success = file.createNewFile();
                        if (!success)
                            log.error("Create file failed, name is: " + name);
                        else
                            log.info("Create new file " + name + " success.");
                    } finally {
                        fw = new FileWriter(name, true);
                    }
                } else{
                    if(fw == null)
                    fw = new FileWriter(name, true);
                }
                
            } catch (IOException e) {
                log.error(e.getMessage(), e);
            }
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
            ,"ListObjects_10.0","ListObjects_11"};
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
                .append("。具体统计如下：").append("</br>");
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
                
                body.append("</table>");
                body.append("</br>").append("注：所有的测试都是在没有错误重试的情况下进行的，错误率=错误次数/操作总次数；平均时间=成功操作总时间/成功操作总次数；<300ms的比率=（<300ms的总次数，其中不包含失败的操作）/（成功操作的总次数）；<500ms，<1.0s等等的计算方式与<300ms的计算方式相同。");
                MailUtils.sendMail(resourcePool + "-SLA", resourcePool + " SLA Monitor", body.toString());
            } catch (Throwable t) {
                LOG.error("Handle slaLog Error", t);
            }
        }
    }
    
}

