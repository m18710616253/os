package cn.ctyun.oos.server.stress;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
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
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;

public class OOSHQETESlaMonitor implements Program {

    private final static Log LOG = LogFactory.getLog(OOSHQETESlaMonitor.class);
    private static String bucketName = "yanke";
    static LogOneLine oneLine = new LogOneLine(null);

    static Map<String, List<String>> objects = new HashMap<String, List<String>>();
    static int[] objectBuff = new int[Config.objectSize];
    static Random rand = new Random();
    static{
        for(int i=0;i<Config.objectSize;i++)
            objectBuff[i] = rand.nextInt() & 0xFF;
        for(String host:Config.hosts){
            objects.put(host, new ArrayList<String>(Config.listSize));
        }
    }

    static class Task {//extends TimerTask {
        static ExecutorService exec = Executors.newFixedThreadPool(Config.maxThreadNum);

        static char[] ch = { '1', '2', '3', '4', '5', '6', '7', '8', '9', '0', 
                'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 
                'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', 
                'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L',
                'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X',
                'Y', 'Z'};

        static int len = ch.length;

        public void run() {
            final AtomicLong count = new AtomicLong(0);
            for(final String hp : Config.hosts){
                exec.execute(new Thread(){
                    public void run(){
                        try {
                            ClientConfiguration configuration = new ClientConfiguration();
                            configuration.setMaxErrorRetry(0);
                                AmazonS3Client asc = new AmazonS3Client(new AWSCredentials() {
                                    public String getAWSAccessKeyId() {
                                        return Config.ak;
                                    }

                                    public String getAWSSecretKey() {
                                        return Config.sk;
                                    }
                                }, configuration);
                                asc.setEndpoint(hp);
                            List<String> objs = buildObjectNames();
                            for(String obj:objs){
                                objectPut(asc, hp, obj, bucketName);
                                if(objects.get(hp).size() <= Config.listSize)
                                    return;
                                obj = objects.get(hp).remove(ThreadLocalRandom.current().nextInt(Config.listSize));
                                try{
                                    objectGet(asc, hp, obj, bucketName);
                                }finally{
                                    objectDelete(asc, hp, obj, bucketName);
                                }
                                
                            }
                            
                        } catch (Throwable e) {
                            LOG.error(e.getMessage(), e);
                        } finally{
                            count.incrementAndGet();
                        }
                    }
                }); 
            }
            while(count.get() < Config.hosts.length)
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    LOG.error("SleepError", e);
                }
            
        }

        private List<String> buildObjectNames() {
            Random rand = new Random();
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

    private static void bucketCreate(AmazonS3 asc, String bucket) throws IOException{
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


    private static void objectPut(AmazonS3 asc, String hp, String object, String bucket)
            throws IOException {
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(Config.objectSize);
        try{
            final AtomicLong start = new AtomicLong(System.currentTimeMillis());
            asc.putObject(bucket, object, new InputStream() {
                int pos = 0;
                int v;
                public int read() throws IOException {
                    v = (pos++ >= Config.objectSize) ? -1 : objectBuff[pos-1];
                    if(pos == Config.objectSize){
                        start.set(System.currentTimeMillis());
                    }
                    return v;
                }
            }, metadata);
            long end = System.currentTimeMillis();
            oneLine.write("PutObject to " + hp + " Time: " + (end - start.get()) + " ms");
        }catch(Throwable t){
            oneLine.write("PutObjectError to "+hp);
            throw t;
        }
    }

    private static void objectGet(AmazonS3 asc, String hp, String object, String bucket)
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
            oneLine.write("GetObject from " + hp + " Time: " + (end - start) + " ms");
        }catch(Throwable t){
            oneLine.write("GetObjectError to "+hp);
            throw t;
        }
    }

    private static void objectDelete(AmazonS3 asc, String hp, String object, String bucket)
            throws IOException {
        DeleteObjectRequest deleteObjectRequest = new DeleteObjectRequest(
                bucket, object);
        long start = System.currentTimeMillis();
        try{
            asc.deleteObject(deleteObjectRequest);
            long end = System.currentTimeMillis();
            oneLine.write("DeleteObject from " + hp + " Time: " + (end - start) + " ms");
        }catch(Throwable t){
            oneLine.write("DeleteObjectError to "+hp);
            throw t;
        }

    }

    @Override
    public String usage() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void exec(String[] args) throws Exception {
//        Timer timer = new Timer();
//        timer.schedule(new Task(), 0, Config.runInterval);
        final ExecutorService exec = Executors.newFixedThreadPool(1);
        final Task task = new Task();
        long start = System.currentTimeMillis();
        long end = System.currentTimeMillis();
        while(true){
            start = System.currentTimeMillis();
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
            end = System.currentTimeMillis();
            try{
                long inter = end - start;
                Thread.sleep(inter>Config.runInterval?0:Config.runInterval-start);
            }catch(Throwable t){
                ;
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
            ,"DeleteObject_10.0","DeleteObject_11"};
        public void run(){
            try {
                String body = HandleSlaLog.handleLog(fileName, time, Config.hosts, Config.which, Config.objectSize);
                MailUtils.sendMail(Config.title, Config.title + " Monitor", body);
            } catch (Throwable t) {
                LOG.error("Handle slaLog Error", t);
            }
        }
    }
    static DateFormat yyyyMMdd = new SimpleDateFormat("yyyy-MM-dd HH",
            Locale.ENGLISH);
    
    static class LogOneLine {
        private FileWriter fw;
        private long last = System.currentTimeMillis();
        private Log log = LogFactory.getLog(LogOneLine.class);
        private String name = System.getProperty("user.dir")
                + "/slalogs/sla.log";

        public LogOneLine(String fileName) {
            if(fileName != null)
                this.name = fileName;
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
                } else if (System.currentTimeMillis() - last > Config.sendInter) {
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
                            File p = file.getParentFile();
                            String[] files = p.list();
                            if(files != null && files.length > 30){
                                List<String> list = new ArrayList<String>();
                                for(String ss : files)
                                    list.add(ss);
                                Collections.sort(list);
                                for(int i=0;i< files.length - 30;i++)
                                    new File(list.get(i)).delete();
                            }
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

    static class Config {

        public static String ak="7e23b083bb364ad3909174d7e112793a/test1";
        public static String sk="EgCST9gezHW9sx0eeEz6nkXmjg8=";

        public static String[] hosts;

        public static int runInterval=1000;
        public static int maxThreadNum=5;
        public static String mailHost="192.168.214.41";
        public static int mailPort=25;
        public static String userName="ctyun@chinatelecom.com.cn";
        public static String passwd="ctyun";
        public static String from="ctyun@chinatelecom.com.cn";
        public static String to="jiangfeng@chinatelecom.com.cn,fjiang@yeah.net,cuimeng@chinatelecom.com.cn,guochy@chinatelecom.com.cn,yanke@chinatelecom.com.cn,liuyueteng@chinatelecom.com.cn,dongchk@chinatelecom.com.cn";
        public static long sendInter=3600000;

        public static int objectSize = 2097152;
        public static String title="ATMOS-HQ-ETE-SLA";
        public static String which="ATMOS";
        public static int listSize;
        
        static {
            Properties p = new Properties();
            InputStream is = null;
            try {
                is = new FileInputStream(System.getProperty("user.dir")
                        + "/conf/monitor/slaETEConfig.txt");
                p.load(is);
                ak = p.getProperty("ak").trim();
                sk = p.getProperty("sk").trim();
                hosts = p.getProperty("hosts").split(",");
                runInterval = Integer.parseInt(p.getProperty("runInter"));
                maxThreadNum = Integer.parseInt(p.getProperty("maxThreadNum"));
                mailHost = p.getProperty("mailHost").trim();
                mailPort = Integer.parseInt(p.getProperty("mailPort"));
                userName = p.getProperty("userName").trim();
                passwd = p.getProperty("passwd").trim();
                from = p.getProperty("from").trim();
                to = p.getProperty("to").trim();
                sendInter = Long.parseLong(p.getProperty("sendInter"));
                title = p.getProperty("title").trim();
                objectSize = Integer.parseInt(p.getProperty("objSize"));
                which = p.getProperty("which").trim();
                listSize = Integer.parseInt(p.getProperty("listSize"));
                
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
        }
    }
}