package cn.ctyun.oos.server.stress;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;

import cn.ctyun.common.Program;
import cn.ctyun.oos.common.MailServer;
import cn.ctyun.oos.common.MailServer.Mail;

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

import common.util.BlockingExecutor;

/**
 * @author Dongchk
 * 
 * 监控服务的可用性，如有异常发短信、邮件告警
 *
 */
public class MonitorAliveAlert  implements Program {

    private static Log log = LogFactory.getLog(MonitorAliveAlert.class);

    static int[] objectBuff = new int[Config.objectSize];
    static Random rand = new Random();
    static {
        for (int i = 0; i < Config.objectSize; i++)
            objectBuff[i] = rand.nextInt() & 0xFF;
    }

    public String usage() {
        return null;
    }

    public static void main(String[] args) throws Exception {
        MonitorAliveAlert ma = new MonitorAliveAlert();
        ma.exec(args);
    }

    public void exec(String[] args) throws Exception {
        final Task task = new Task();
        long start = System.currentTimeMillis();
        long end = System.currentTimeMillis();
        while (true) {
            start = System.currentTimeMillis();
            try{
                task.run();
            } catch (Throwable t) {
                log.error(t.getMessage(), t);
            }
            end = System.currentTimeMillis();
            try {
                long inter = end - start;
                Thread.sleep(inter > Config.runInter ? 0
                        : Config.runInter - start);
            } catch (Throwable t) {
                ;
            }
        }
    }

    static class Task {//extends TimerTask {
        private static long lastSendAlive = System.currentTimeMillis();
        private static long lastSendAlert = 0;
        private static boolean normalFlag = true;
        private static long oneDay = 86400000;
        private static boolean isSendAbnormal = false;
        private static BlockingExecutor pool = new BlockingExecutor(Config.maxThreadNum, Config.maxThreadNum, 100000, 0, "Execute Task");
        private static DateFormat format = new SimpleDateFormat(
                "yyyy-MM-dd HH:mm:ss");
        private static Executor alert = new BlockingExecutor(5, 10, 100000, 0, "Send Alert");

        public void run() {
            List<Future<?>> jobs = new ArrayList<Future<?>>(
                    Config.optNum);
            try{
                boolean flag = true;
                final AtomicInteger count = new AtomicInteger(0);
                final AtomicInteger bdCount = new AtomicInteger(0);
                long startTime = System.currentTimeMillis();
                // StringBuilder body = new StringBuilder();
                String body = null;
                String subject = null;
                
                ClientConfiguration configuration = new ClientConfiguration();
                /*configuration.setConnectionTimeout(10000);
                configuration.setSocketTimeout(10000);*/
                configuration.setMaxErrorRetry(0);
                final AmazonS3 asc = new AmazonS3Client(new AWSCredentials() {
                    public String getAWSAccessKeyId() {
                        return Config.s3userName;
                    }

                    public String getAWSSecretKey() {
                        return Config.s3pwd;
                    }
                }, configuration);
                asc.setEndpoint(Config.ip);
                
                inition(asc, Config.bucketName);
                
                for (int i = 0; i < Config.optNum; i++) {
                    jobs.add(pool.submit(new Thread() {
                        public void run() {
                            try {
                                does(asc);
                            }catch (Throwable e2) {
                                log.error(Config.nick + "::" + e2.getMessage(), e2);
                                if(Thread.interrupted())
                                    return;
                                if (!checkNetstat())
                                    bdCount.incrementAndGet();
                                count.incrementAndGet();
                            }
                        }
                    }));
                }
                while(true){
                    try{
                        Thread.sleep(5000);
                    }catch(Exception e){}
                    
                    Iterator<Future<?>> itr = jobs.iterator();
                    while(itr.hasNext()){
                        Future<?> job = itr.next();
                        if(job.isDone())
                            itr.remove();
                    }
                    if(jobs.isEmpty() || System.currentTimeMillis() - startTime > 600000)
                        break;
                }
                for(Future<?> job : jobs){
                    job.cancel(true);
                }
                if(!jobs.isEmpty()){
                    log.error("Execute " + Config.optNum + " tasks take over 10 mins. " + jobs.size() + " tasks is undone.");
                    count.addAndGet(jobs.size());
                }

                long endTime = System.currentTimeMillis();
                long time = (endTime - startTime) / 1000;

                log.info("Execute " + (Config.optNum - jobs.size()) + " tasks take " + time + " s");
                
                if (count.get() > Config.optNum * 40 / 100) {
                    subject = "Fatal";
                    body = buildBody(0, Config.nick, time, Config.optNum,
                            count.get(), bdCount.get());
                    normalFlag = false;
                    flag = false;
                } else if (count.get() > Config.optNum * 20 / 100) {
                    subject = "Error";
                    body = buildBody(1, Config.nick, time, Config.optNum,
                            count.get(), bdCount.get());
                    normalFlag = false;
                    flag = false;
                } else if (count.get() > Config.optNum * 10 / 100) {
                    subject = "Warning";
                    body = buildBody(2, Config.nick, time, Config.optNum,
                            count.get(), bdCount.get());
                    normalFlag = false;
                    flag = false;
                } else {
                    subject = "OK";
                    body = buildBody(3, Config.nick, time, Config.optNum,
                            count.get(), bdCount.get());
                }

                boolean shouldSendM = false;
                if (!normalFlag) {
                    if (flag) {
                        if (isSendAbnormal) {
                            
                            shouldSendM = true;
                            isSendAbnormal = false;
                        }
                        normalFlag = true;
                    } else if (System.currentTimeMillis() - lastSendAlert >= Config.sendEmailInter) {
                        shouldSendM = true;
                        lastSendAlert = System.currentTimeMillis();
                        lastSendAlive = System.currentTimeMillis();
                        isSendAbnormal = true;
                    }

                } else {
                    if (System.currentTimeMillis() - lastSendAlive >= oneDay) {
                        subject = "OK";
                        body = buildBody(4, Config.nick, 0, 0, 0, 0);
                        shouldSendM = true;
                        lastSendAlive = System.currentTimeMillis();
                    }
                }
                if(shouldSendM){
                    final String entity = body.toString();
                    final String subj = subject;
                    alert.execute(new Thread(){
                        public void run(){
                            MailServer mailServer = new MailServer(Config.smtpHost,
                                    Config.smtpPort, Config.mailUserName,
                                    Config.mailPasswd);
                            Mail mail = mailServer.createMail(Config.from,
                                    Config.to, "Status: "+subj, Config.nick,
                                    entity);
                            mail.sendMail();
                            for(String phone:Config.phoneNums){
                                SMS.sendSMS(Config.nick+"-"+subj, phone);
                            }
                        }
                    });
                }
            }catch(Throwable t){
                log.error(t.getMessage(), t);
            }

        }

        private static String buildBody(int type, String nick, long time,
                int total, int failure, int bdFailure) {
            StringBuilder sb = new StringBuilder(
                    "<table width=\"358\" border=\"0\" cellspacing=\"5\" style=\"font-size:14px\">")
                    .append("<tr>")
                    .append("<td width=\"225\" align=\"left\" valign=\"middle\">监控源 ：</td>")
                    .append("<td width=\"137\" align=\"left\" valign=\"middle\">");
            if (type == 0) {
                sb.append(nick).append("</td>").append("</tr>");
                sb.append("<tr>")
                        .append("<td>报警级别：</td>")
                        .append("<td bgcolor=\"#FFFF33\"><strong style=\"color:#C63\">致命</strong></td>")
                        .append("</tr>");
                sb.append("<tr>").append("<td>失败率：</td>")
                        .append("<td>超过百分之八十</td>").append("</tr>");
                sb.append("<tr>").append("<td>耗时：</td>").append("<td>")
                        .append(time).append("s</td>").append("</tr>");
                sb.append("<tr>").append("<td>尝试次数：</td>").append("<td>")
                        .append(total).append("</td>").append("</tr>");
                sb.append("<tr>").append("<td>失败次数：</td>").append("<td>")
                        .append(failure).append("</td>").append("</tr>");
                sb.append("<tr>").append("<td>测试机本身网络问题导致的失败次数：</td>")
                        .append("<td>").append(bdFailure).append("</td>")
                        .append("</tr>");
            } else if (type == 1) {
                sb.append(nick).append("</td>").append("</tr>");
                sb.append("<tr>")
                        .append("<td>报警级别：</td>")
                        .append("<td bgcolor=\"#FFFF33\"><strong>严重</strong></td>")
                        .append("</tr>");
                sb.append("<tr>").append("<td>失败率：</td>")
                        .append("<td>超过百分之五十</td>").append("</tr>");
                sb.append("<tr>").append("<td>耗时：</td>").append("<td>")
                        .append(time).append("s</td>").append("</tr>");
                sb.append("<tr>").append("<td>尝试次数：</td>").append("<td>")
                        .append(total).append("</td>").append("</tr>");
                sb.append("<tr>").append("<td>失败次数：</td>").append("<td>")
                        .append(failure).append("</td>").append("</tr>");
                sb.append("<tr>").append("<td>测试机本身网络问题导致的失败次数：</td>")
                        .append("<td>").append(bdFailure).append("</td>")
                        .append("</tr>");
            } else if (type == 2) {
                sb.append(nick).append("</td>").append("</tr>");
                sb.append("<tr>")
                        .append("<td>报警级别：</td>")
                        .append("<td bgcolor=\"#FFFF33\"><strong>警告</strong></td>")
                        .append("</tr>");
                sb.append("<tr>").append("<td>失败率：</td>")
                        .append("<td>超过百分之二十</td>").append("</tr>");
                sb.append("<tr>").append("<td>耗时：</td>").append("<td>")
                        .append(time).append("s</td>").append("</tr>");
                sb.append("<tr>").append("<td>尝试次数：</td>").append("<td>")
                        .append(total).append("</td>").append("</tr>");
                sb.append("<tr>").append("<td>失败次数：</td>").append("<td>")
                        .append(failure).append("</td>").append("</tr>");
                sb.append("<tr>").append("<td>测试机本身网络问题导致的失败次数：</td>")
                        .append("<td>").append(bdFailure).append("</td>")
                        .append("</tr>");
            } else if (type == 3) {
                sb.append(nick).append("</td>").append("</tr>");
                sb.append("<tr>").append("<td>报警级别：</td>")
                        .append("<td><strong>恢复正常</strong></td>")
                        .append("</tr>");
                sb.append("<tr>").append("<td>失败率：</td>")
                        .append("<td>不超过百分之二十</td>").append("</tr>");
                sb.append("<tr>").append("<td>耗时：</td>").append("<td>")
                        .append(time).append("s</td>").append("</tr>");
                sb.append("<tr>").append("<td>尝试次数：</td>").append("<td>")
                        .append(total).append("</td>").append("</tr>");
                sb.append("<tr>").append("<td>失败次数：</td>").append("<td>")
                        .append(failure).append("</td>").append("</tr>");
                sb.append("<tr>").append("<td>测试机本身网络问题导致的失败次数：</td>")
                        .append("<td>").append(bdFailure).append("</td>")
                        .append("</tr>");
            } else if (type == 4) {
                sb.append(nick).append("</td>").append("</tr>");
                sb.append("<tr>").append("<td>报警级别：</td>")
                        .append("<td><strong>正常</strong></td>").append("</tr>");
                sb.append("<tr>").append("<td>持续时间：</td>")
                        .append("<td>过去的24小时</td>").append("</tr>");
            }
            sb.append("<tr>").append("<td>邮件发送时间：</td>").append("<td>")
                    .append(format.format(new Date())).append("</td>")
                    .append("</tr>");

            sb.append("</table>");
            sb.append("<p><em>注：</em></p>");
            sb.append(
                    "<table width=\"653\" height=\"284\" border=\"1\" style=\"font-size:13px\">")
                    .append("<tr>")
                    .append("<td width=\"80\" rowspan=\"6\">报警级别 </td>")
                    .append("<td width=\"563\">正常/恢复正常 &lt; 警告 &lt; 严重 &lt; 致命</td>")
                    .append("</tr>")
                    .append("<tr>")
                    .append("<td>正常（在过去的24小时中，OOS的状态都是正常的）</td>")
                    .append("</tr>")
                    .append("<tr>")
                    .append("<td>恢复正常（在上次监控的失败率 &gt; 20%情况下，本次监控的失败率 &lt; 20%）</td>")
                    .append("</tr>")
                    .append("<tr>")
                    .append("<td>警告（失败率 &gt; 20%）</td>")
                    .append("</tr>")
                    .append("<tr>")
                    .append("<td>严重（失败率 &gt; 50%）</td>")
                    .append("</tr>")
                    .append("<tr>")
                    .append("<td>致命（失败率 &gt; 80%）</td>")
                    .append("</tr>")
                    .append("<tr>")
                    .append("<td>监控原则</td>")
                    .append("<td><p>&nbsp;&nbsp;&nbsp;&nbsp;监控程序")
                    .append("每隔")
                    .append(Config.runInter / 1000)
                    .append("秒运行一次，每次监控都并发访问")
                    .append(Config.optNum)
                    .append("次服务，每次服务包括put/get/delete object，然后进行统计分析，并根据不同的失败率发送不同级别的报警邮件；每次发送报警邮件之前都会判断上次发送报警邮件的时间是否已经超过十分钟，如果超过则发送报警邮件，否则不发送；如果监控程序已经超过24小时未出现失败率 &lt; 10%的情况，则会发送一封正常邮件。</p><p>&nbsp;&nbsp;&nbsp;&nbsp;在发送警告/严重/致命邮件之前，都会判断上次发送此类级别邮件的时间是否已经超过10分钟，只有时间间隔超过10分钟，才会再次发送此类级别的报警邮件。</p></td>")
                    .append("</tr>").append("</table>");

            return sb.toString();
        }

        private static void objectPut(AmazonS3 asc, String bucket, String object, final int length) {
            try {
                ObjectMetadata metadata = new ObjectMetadata();
                metadata.setContentLength(length);
                asc.putObject(bucket, object, new InputStream() {
                    int pos = 0;

                    public int read() throws IOException {
                        int data = (pos++ >= length) ? -1 : objectBuff[pos - 1];
                        return data;
                    }
                }, metadata);
                log.info("Create object in " + Config.nick + " success.");
            } catch (Throwable t) {
                log.error("Create object in " + Config.nick + " error.", t);
                throw t;
            }
        }

        private static void objectGet(AmazonS3 asc, String bucket, String object) throws Throwable {
            try {
                GetObjectRequest getObjectRequest = new GetObjectRequest(bucket, object);
                S3Object s3Object = new S3Object();
                s3Object = asc.getObject(getObjectRequest);
                if (s3Object != null) {
                    InputStream is = s3Object.getObjectContent();
                    try {
                        while (is.read() != -1)
                            ;
                    } finally {
                        is.close();
                    }
                    log.info("Read object in " + Config.nick + " success.");
                }
            } catch (Throwable t) {
                log.error("Create object in " + Config.nick + " error.", t);
                throw t;
            }
        }

        private static void objectDelete(AmazonS3 asc, String bucket, String object)
                throws Exception {
            try {
                DeleteObjectRequest deleteObjectRequest = new DeleteObjectRequest(
                        bucket, object);
                asc.deleteObject(deleteObjectRequest);
                log.info("Delete object in " + Config.nick + " success.");
            } catch (Throwable t) {
                log.error("Delete object in " + Config.nick + " error.", t);
                throw t;
            }
        }

        private static char[] chs = { 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h',
                'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't',
                'u', 'v', 'w', 'x', 'y', 'z' };

        private static String getString(String prefix) {
            for (int i = 0; i < 20; i++)
                prefix += chs[rand.nextInt(chs.length)];
            return prefix + "_" + System.currentTimeMillis();
        }

        private static void does(AmazonS3 asc) throws Throwable {
            String key = getString(Config.objectName);
            objectPut(asc, Config.bucketName, key, Config.objectSize);
            if (Thread.interrupted())
                throw new InterruptedException("timeout");
            objectGet(asc, Config.bucketName, key);
            if (Thread.interrupted())
                throw new InterruptedException("timeout");
            objectDelete(asc, Config.bucketName, key);
        }
        
        private static void inition(AmazonS3 asc, String bucket)
                throws Exception {
            if (!asc.doesBucketExist(bucket)) {
                CreateBucketRequest createBucketRequest = new CreateBucketRequest(
                        bucket);
                createBucketRequest
                        .setCannedAcl(CannedAccessControlList.PublicRead);
                asc.createBucket(createBucketRequest);
            }
        }

        private static boolean checkNetstat() {
            boolean flag = true;
            try {
                HttpGet hg = new HttpGet("http://www.baidu.com");
                HttpResponse response = new DefaultHttpClient().execute(hg);
                if (response.getStatusLine().getStatusCode() != 200)
                    throw new RuntimeException();
            } catch (Throwable e) {
                log.error(e.getMessage(), e);
                HttpGet hg = new HttpGet("http://www.sina.com.cn");
                HttpResponse response;
                try {
                    response = new DefaultHttpClient().execute(hg);
                    if (response.getStatusLine().getStatusCode() != 200)
                        throw new RuntimeException();
                } catch (Throwable e2) {
                    log.error(e2.getMessage(), e2);
                    flag = false;
                }
            }
            return flag;
        }

    }
    
    static class SMS {
        public static String prefix = "java -jar ./lib/sendsms.jar ";

        public static boolean sendSMS(String message, String phone) {
            String command = prefix + phone + " " + message;
            Process process = null;
            try {
                process = Runtime.getRuntime().exec(command);
                InputStream is = process.getInputStream();
                try {
                    byte[] buff = new byte[1024];
                    int len = 0;
                    while ((len = is.read(buff)) != -1) {
                        String info = new String(buff, 0, len);
                        if (info.contains("Status:0")) {
                            log.info("Send message to " + phone
                                    + " success.");
                            return true;
                        }
                    }
                    InputStream errorIs = process.getErrorStream();
                    try {
                        buff = new byte[1024];
                        len = 0;
                        StringBuilder error = new StringBuilder();
                        while ((len = errorIs.read(buff)) != -1) {
                            error.append(new String(buff, 0, len)).append("\n");
                        }
                        log.info("Send message to " + phone
                                + " failed.\n" + error.toString());
                    } finally {
                        errorIs.close();
                    }
                } finally {
                    is.close();
                }
            } catch (Exception e) {
                log.error(e.getMessage());
            }
            return false;
        }
    }

    static class Config {
        public static String s3userName;
        public static String s3pwd;
        public static String bucketName;
        
        public static int maxThreadNum = 10;
        public static int optNum = 400;

        public static int objectSize = 1024;
        public static String nick = "OOSMINI-SH-STATUS";
        public static int runInter = 10;
        public static String objectName = "object-monitor-6463084869102846986";

        public static String ip;
        public static int port = 80;

        public static String smtpHost = "";
        public static int smtpPort = 25;
        public static String mailUserName = "";
        public static String mailPasswd = "";
        public static String from = "";
        public static String to = "";
        public static int sendEmailInter = 0;

        public static String keyPool = "";
        public static String[] phoneNums;
        static {
            Properties p = new Properties();
            InputStream is = null;
            try {
                is = new FileInputStream(System.getProperty("user.dir")
                        + "/conf/monitor/monitorAlive.conf");
                p.load(is);
                s3userName = p.getProperty("s3userName");
                s3pwd = p.getProperty("s3pwd");
                bucketName = p.getProperty("bucketName");
                ip = p.getProperty("ip");
                port = Integer.parseInt(p.getProperty("port"));
                nick = p.getProperty("nick");
                runInter = Integer.parseInt(p.getProperty("runInter"));
                sendEmailInter = Integer.parseInt(p
                        .getProperty("sendEmailInter"));
                smtpHost = p.getProperty("smtpHost");
                smtpPort = Integer.parseInt(p.getProperty("smtpPort"));
                mailUserName = p.getProperty("mailUserName");
                mailPasswd = p.getProperty("mailPasswd");
                from = p.getProperty("from");
                to = p.getProperty("to");
                objectSize = Integer.parseInt(p.getProperty("objectSize"));
                keyPool = p.getProperty("keyPool");
                maxThreadNum = Integer.parseInt(p.getProperty("maxThreadNum"));
                optNum = Integer.parseInt(p.getProperty("optNum"));
                objectName = p.getProperty("objectName");
                phoneNums = p.getProperty("phoneNums").split(",");

            } catch (FileNotFoundException e) {
                log.error(e.getMessage(), e);
            } catch (IOException e) {
                log.error(e.getMessage(), e);
            } finally {
                try {
                    is.close();
                } catch (IOException e) {
                    log.error(e.getMessage(), e);
                }
            }
        }
    }
}
