package cn.ctyun.oos.monitor;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;

import cn.ctyun.common.Program;
import cn.ctyun.oos.monitor.Utils.MailServer;
import cn.ctyun.oos.monitor.Utils.MailServer.Mail;
import cn.ctyun.oos.monitor.Utils.SMS;

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

public class OOSMonitor implements Program {
    static{
        System.setProperty("log4j.log.app", "oosmonitor");
    }
    private static Log log = LogFactory.getLog(OOSMonitor.class);

    private static AmazonS3 asc = null;

    static int[] objectBuff = new int[Config.objectSize];
    static Random rand = new Random();
    static {
        for (int i = 0; i < Config.objectSize; i++)
            objectBuff[i] = rand.nextInt() & 0xFF;
    }

    private final static Log LOG = LogFactory.getLog(OOSMonitor.class);
    static {
        ClientConfiguration configuration = new ClientConfiguration();
        configuration.setMaxErrorRetry(0);
        try {
            asc = new AmazonS3Client(new AWSCredentials() {
                public String getAWSAccessKeyId() {
                    return Config.apiAK;
                }

                public String getAWSSecretKey() {
                    return Config.apiSK;
                }
            }, configuration);
            asc.setEndpoint(Config.apiAddress);

        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            throw new Error(e);
        }
    }

    static class Task {
        private static boolean isSendOK = false;
        private static long lastSendAlert = 0;
        private static boolean normalFlag = true;
        private static boolean isSendAbnormal = false;
        private static Executor alert = new BlockingExecutor(5, 10, 100000, 0,
                "Send Alert");
        static char[] ch = { '1', '2', '3', '4', '5', '6', '7', '8', '9', '0',
                'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l',
                'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x',
                'y', 'z', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J',
                'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V',
                'W', 'X', 'Y', 'Z' };

        static int len = ch.length;
        static Random rand = new Random();

        private static AtomicLong putTime = new AtomicLong(0);
        private static AtomicLong getTime = new AtomicLong(0);
        private static AtomicLong deleteTime = new AtomicLong(0);

        private static AtomicLong putErrors = new AtomicLong(0);
        private static AtomicLong getErrors = new AtomicLong(0);
        private static AtomicLong deleteErrors = new AtomicLong(0);

        private static AtomicLong putNum = new AtomicLong(0);
        private static AtomicLong getNum = new AtomicLong(0);
        private static AtomicLong deleteNum = new AtomicLong(0);

        private static DecimalFormat df = new DecimalFormat("0.00");
        private static BlockingExecutor pool = new BlockingExecutor(
                Config.maxThreadNum, Config.maxThreadNum, 100000, 0,
                "Execute Task");
        
        public void run() {
            List<Thread> list = new ArrayList<Thread>();
            List<Future<?>> jobs = new ArrayList<Future<?>>(Config.optNum);
            try {
                boolean flag = true;
                final AtomicInteger count = new AtomicInteger(0);
                final AtomicInteger bdCount = new AtomicInteger(0);
                long startTime = System.currentTimeMillis();
                // StringBuilder body = new StringBuilder();
                String body = null;
                String subject = null;

                for (int i = 0; i < Config.optNum; i++) {
                    Thread th = new Thread() {
                        public void run() {
                            try {
                                does(asc);
                            } catch (Throwable e2) {
                                log.error(Config.nick + "::" + e2.getMessage(),
                                        e2);
                                if (Thread.interrupted())
                                    return;
                                if (!checkNetstat())
                                    bdCount.incrementAndGet();
                                count.incrementAndGet();
                            }
                        }
                    };
                    list.add(th);
                    jobs.add(pool.submit(th));
                }
                while (true) {
                    try {
                        Thread.sleep(5000);
                    } catch (Exception e) {
                    }

                    Iterator<Future<?>> itr = jobs.iterator();
                    while (itr.hasNext()) {
                        Future<?> job = itr.next();
                        if (job.isDone())
                            itr.remove();
                    }
                    if (jobs.isEmpty()
                            || System.currentTimeMillis() - startTime > Config.execTime)
                        break;
                }
                for (Future<?> job : jobs) {
                    job.cancel(true);
                }
                if (!jobs.isEmpty()) {
                    log.error("Execute " + Config.optNum + " tasks take over "
                            + Config.execTime / 1000 + " s. " + jobs.size()
                            + " tasks is undone.");
                    count.addAndGet(jobs.size());
                }

                long endTime = System.currentTimeMillis();
                long time = (endTime - startTime) / 1000;

                log.info("Execute " + (Config.optNum - jobs.size())
                        + " tasks take " + time + " s");
                for(Thread th : list)
                    pool.remove(th);
                jobs.clear();
                
                if (count.get() > Config.optNum * 40 / 100) {
                    subject = "Fatal";
                    body = Utils.buildMessageBody(0, Config.nick, time,
                            Config.optNum, count.get(), bdCount.get());
                    normalFlag = false;
                    flag = false;
                } else if (count.get() > Config.optNum * 20 / 100) {
                    subject = "Error";
                    body = Utils.buildMessageBody(1, Config.nick, time,
                            Config.optNum, count.get(), bdCount.get());
                    normalFlag = false;
                    flag = false;
                } else if (count.get() > Config.optNum * 10 / 100) {
                    subject = "Warning";
                    body = Utils.buildMessageBody(2, Config.nick, time,
                            Config.optNum, count.get(), bdCount.get());
                    normalFlag = false;
                    flag = false;
                } else {
                    subject = "OK";
                    body = Utils.buildMessageBody(3, Config.nick, time,
                            Config.optNum, count.get(), bdCount.get());
                }

                boolean shouldSendM = false;
                if (!normalFlag) {
                    if (flag) {
                        if (isSendAbnormal) {
                            shouldSendM = true;
                            isSendAbnormal = false;

                            Date now = new Date();
                            int hour = now.getHours();
                            if (hour == Config.sendOKTime)
                                isSendOK = true;
                        }
                        normalFlag = true;
                    } else if (System.currentTimeMillis() - lastSendAlert >= Config.sendEmailInter) {
                        shouldSendM = true;
                        lastSendAlert = System.currentTimeMillis();
                        isSendAbnormal = true;
                    }

                } else {
                    Date now = new Date();
                    int hour = now.getHours();
                    if (hour == Config.sendOKTime && !isSendOK) {
                        subject = "OK";
                        body = Utils.buildMessageBody(4, Config.nick, 0, 0, 0,
                                0);
                        shouldSendM = true;
                        isSendOK = true;
                    }
                    if (hour != Config.sendOKTime)
                        isSendOK = false;
                }

                if (shouldSendM) {
                    final String entity = body.toString();
                    final String subj = subject;
                    alert.execute(new Thread() {
                        public void run() {
                            if(Config.smtpHost != null && Config.smtpHost.length() != 0){
                                MailServer mailServer = new MailServer(
                                        Config.smtpHost, Config.smtpPort,
                                        Config.mailUserName, Config.mailPasswd);
                                Mail mail = mailServer.createMail(Config.from,
                                        Config.to, "Status: " + subj, Config.nick,
                                        entity);
                                mail.sendMail();
                            }
                            
                            if(Config.phoneNums != null && Config.phoneNums.length != 0)
                                for (String phone : Config.phoneNums) {
                                    SMS.sendSMS(Config.nick + "-" + subj, phone);
                                }
                        }
                    });
                }
                
                final String v1 = (putNum.get() == 0 ? "0" : df.format((double)putErrors.get() * 100 / putNum.get()));
                final String v2 = (getNum.get() == 0 ? "0" : df.format((double)getErrors.get() * 100 / getNum.get()));
                final String v3 = (deleteNum.get() == 0 ? "0" : df.format((double)deleteErrors.get() * 100 / deleteNum.get()));
                final long v5 = ((putNum.get() - putErrors.get() == 0) ? 0 : putTime.get() / (putNum.get() - putErrors.get()));
                final long v6 = ((getNum.get() - getErrors.get()) == 0 ? 0 : getTime.get() / (getNum.get() - getErrors.get()));
                final long v7 = ((deleteNum.get() - deleteErrors.get()) == 0 ? 0 : putTime.get() / (deleteNum.get() - deleteErrors.get()));
                if(Config.gangliaAddr != null && Config.gangliaAddr.length() != 0)
                    alert.execute(new Thread(){
                        public void run(){
                            String cmd = Config.gangliaAddr + "/bin/gmetric -n PutErrorRatio -v " + v1 + " -tfloat -u % -g hbase-monitor";
                            String rs = Utils.writeToGanglia(cmd);
                            if(rs != null)
                                log.error(rs);
                            cmd = Config.gangliaAddr + "/bin/gmetric -n GetErrorRatio -v " + v2 + " -tfloat -u % -g hbase-monitor";
                            rs = Utils.writeToGanglia(cmd);
                            if(rs != null)
                                log.error(rs);
                            cmd = Config.gangliaAddr + "/bin/gmetric -n DeleteErrorRatio -v " + v3 + " -tfloat -u % -g hbase-monitor";
                            rs = Utils.writeToGanglia(cmd);
                            if(rs != null)
                                log.error(rs);
                            cmd = Config.gangliaAddr + "/bin/gmetric -n PutAverTime -v " + v5 + " -tfloat -u ms -g hbase-monitor";
                            rs = Utils.writeToGanglia(cmd);
                            if(rs != null)
                                log.error(rs);
                            cmd = Config.gangliaAddr + "/bin/gmetric -n GetAverTime -v " + v6 + " -tfloat -u ms -g hbase-monitor";
                            rs = Utils.writeToGanglia(cmd);
                            if(rs != null)
                                log.error(rs);
                            cmd = Config.gangliaAddr + "/bin/gmetric -n DeleteAverTime -v " + v7 + " -tfloat -u ms -g hbase-monitor";
                            rs = Utils.writeToGanglia(cmd);
                            if(rs != null)
                                log.error(rs);
                        }
                    });
                
                putNum.set(0);getNum.set(0);deleteNum.set(0);
                putErrors.set(0);getErrors.set(0);deleteErrors.set(0);
                putTime.set(0);getTime.set(0);deleteTime.set(0);

            } catch (Throwable t) {
                log.error(t.getMessage(), t);
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

        private static void bucketCreate() throws Exception {
            boolean isExists = asc.doesBucketExist(Config.bucketName);
            if (!isExists) {
                CreateBucketRequest createBucketRequest = new CreateBucketRequest(
                        Config.bucketName);
                createBucketRequest
                        .setCannedAcl(CannedAccessControlList.PublicRead);
                try {
                    asc.createBucket(createBucketRequest);
                    LOG.info("CreateBucket: " + Config.bucketName + " seccess");
                } catch (Exception e) {
                    LOG.info("CreateBucket: " + Config.bucketName + " failed");
                }
            }
        }

        private static void objectPut(AmazonS3 asc, String bucket,
                String object, final int length) {
            putNum.incrementAndGet();
            try {
                long st = System.currentTimeMillis();
                ObjectMetadata metadata = new ObjectMetadata();
                metadata.setContentLength(length);
                asc.putObject(bucket, object, new InputStream() {
                    int pos = 0;

                    public int read() throws IOException {
                        int data = (pos++ >= length) ? -1 : objectBuff[pos - 1];
                        return data;
                    }
                }, metadata);
                long end = System.currentTimeMillis();
                putTime.addAndGet(end - st);
                log.info("Create object in " + Config.nick + " success takes" + (end - st) + "ms");
            } catch (Throwable t) {
                putErrors.incrementAndGet();
                log.error("Create object in " + Config.nick + " error.", t);
                throw t;
            }
        }

        private static void objectGet(AmazonS3 asc, String bucket, String object)
                throws Throwable {
            getNum.incrementAndGet();
            try {
                long st = System.currentTimeMillis();
                GetObjectRequest getObjectRequest = new GetObjectRequest(
                        bucket, object);
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
                long end = System.currentTimeMillis();
                getTime.addAndGet(end - st);
                log.info("Get object in " + Config.nick + " success takes" + (end - st) + "ms");
            } catch (Throwable t) {
                getErrors.incrementAndGet();
                log.error("Create object in " + Config.nick + " error.", t);
                throw t;
            }
        }

        private static void objectDelete(AmazonS3 asc, String bucket,
                String object) throws Exception {
            deleteNum.incrementAndGet();
            try {
                long st = System.currentTimeMillis();
                DeleteObjectRequest deleteObjectRequest = new DeleteObjectRequest(
                        bucket, object);
                asc.deleteObject(deleteObjectRequest);
                long end = System.currentTimeMillis();
                deleteTime.addAndGet(end - st);
                log.info("Delete object in " + Config.nick + " success takes" + (end - st) + "ms");
            } catch (Throwable t) {
                deleteErrors.incrementAndGet();
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

    }

    static class Config {
        public static String apiAddress;
        public static String apiAK;
        public static String apiSK;
        public static String bucketName;
        public static String objectName = "object-out-monitor-";

        public static int maxThreadNum = 10;
        public static int optNum = 400;
        public static int execTime = 600000;

        public static int objectSize = 1024;
        public static String nick = "OOS-STATUS";
        public static int runInter = 10;

        public static String smtpHost;
        public static int smtpPort;
        public static String mailUserName;
        public static String mailPasswd;
        public static String from;
        public static String to;
        public static int sendEmailInter;
        public static String[] phoneNums;

        public static String gangliaAddr;
        public static int sendOKTime;

        static {
            Properties p = new Properties();
            InputStream is = null;
            try {
                is = new FileInputStream(System.getProperty("user.dir")
                        + "/conf/monitor/oosMonitor.conf");
                p.load(is);
                apiAK = p.getProperty("apiAK");
                apiSK = p.getProperty("apiSK");
                bucketName = p.getProperty("bucketName");
                apiAddress = p.getProperty("apiAddress");
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
                maxThreadNum = Integer.parseInt(p.getProperty("maxThreadNum"));
                optNum = Integer.parseInt(p.getProperty("optNum"));
                objectName = p.getProperty("objectName");
                phoneNums = p.getProperty("phoneNums").split(",");
                sendOKTime = Integer.parseInt(p.getProperty("sendOKTime"));
                gangliaAddr = p.getProperty("gangliaAddr");
            } catch (Throwable e) {
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

    public String usage() {
        return null;
    }

    public void exec(String[] args) throws Exception {
        final Task task = new Task();
        Task.bucketCreate();
        long start = System.currentTimeMillis();
        long end = System.currentTimeMillis();
        while (true) {
            start = System.currentTimeMillis();
            try {
                task.run();
            } catch (Throwable t) {
                log.error(t.getMessage(), t);
            }
            end = System.currentTimeMillis();
            try {
                long inter = end - start;
                Thread.sleep(inter > Config.runInter ? 0 : Config.runInter
                        - inter);
            } catch (Throwable t) {
                ;
            }
        }
    }
}
