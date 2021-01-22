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
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;

import common.util.BlockingExecutor;
import cn.ctyun.common.Program;
import cn.ctyun.oos.hbase.HBaseConnectionManager;
import cn.ctyun.oos.monitor.Utils.MailServer;
import cn.ctyun.oos.monitor.Utils.MailServer.Mail;
import cn.ctyun.oos.monitor.Utils.SMS;

public class HBaseMonitor  implements Program{
    static{
        System.setProperty("log4j.log.app", "hbasemonitor");
    }
    private static Log log = LogFactory.getLog(HBaseMonitor.class);
    private static HConnection conn;
    private static Configuration conf;
    static {
        try {
//            conf = new Configuration();
//            conf.addResource("hht-site.xml");
            conf = HBaseConfiguration.create();
            conn = HBaseConnectionManager.createConnection(conf);
        } catch (IOException e) {
            throw new RuntimeException("Create connection to hbase failed.", e);
        }
    }
    private static byte[] tableName = Bytes.toBytes("TableForMonitor");
    private static byte[] fyName = Bytes.toBytes("fy");
    private static byte[] clName = Bytes.toBytes("v");

    public String usage() {
        return null;
    }

    public void exec(String[] args) throws Exception {
        Task.createTable();
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
    
    public static void main(String[] args) throws Exception{
    }
    
    static class Task{
        private static boolean isSendOK = false;
        private static long lastSendAlert = 0;
        private static boolean normalFlag = true;
        private static boolean isSendAbnormal = false;
        private static Executor alert = new BlockingExecutor(5, 10, 100000, 0, "Send Alert");
        
        private static AtomicLong putTime = new AtomicLong(0);
        private static AtomicLong getTime = new AtomicLong(0);
        private static AtomicLong deleteTime = new AtomicLong(0);
        private static AtomicLong listTime = new AtomicLong(0);
        
        private static AtomicLong putErrors = new AtomicLong(0);
        private static AtomicLong getErrors = new AtomicLong(0);
        private static AtomicLong deleteErrors = new AtomicLong(0);
        private static AtomicLong listErrors = new AtomicLong(0);
        
        private static AtomicLong putNum = new AtomicLong(0);
        private static AtomicLong getNum = new AtomicLong(0);
        private static AtomicLong deleteNum = new AtomicLong(0);
        private static AtomicLong listNum = new AtomicLong(0);

        private static DecimalFormat df = new DecimalFormat("0.00");
        private static BlockingExecutor pool = new BlockingExecutor(Config.maxThreadNum, Config.maxThreadNum, 100000, 0, "Execute Task");

        public void run() {
            List<Future<?>> jobs = new ArrayList<Future<?>>(
                    Config.optNum);
            List<Thread> list = new ArrayList<Thread>();
            try{
                boolean flag = true;
                final AtomicInteger count = new AtomicInteger(0);
                final AtomicInteger bdCount = new AtomicInteger(0);
                long startTime = System.currentTimeMillis();
                // StringBuilder body = new StringBuilder();
                String body = null;
                String subject = null;
                
                for (int i = 0; i < Config.optNum; i++) {
                    Thread thread =new Thread() {
                        public void run() {
                            try {
                                does();
                            }catch (Throwable e2) {
                                log.error(Config.nick + "::" + e2.getMessage(), e2);
                                if(Thread.interrupted())
                                    return;
                                if (!checkNetstat())
                                    bdCount.incrementAndGet();
                                count.incrementAndGet();
                            }
                        }
                    };
                    jobs.add(pool.submit(thread));
                    list.add(thread);
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
                    if(jobs.isEmpty() || System.currentTimeMillis() - startTime > Config.execTime)
                        break;
                }
                for(Future<?> job : jobs){
                    job.cancel(true);
                }
                
                if(!jobs.isEmpty()){
                    log.error("Execute " + Config.optNum + " tasks take over" + Config.execTime / 1000 + " s. " + jobs.size() + " tasks is undone.");
                    count.addAndGet(jobs.size());
                }

                long endTime = System.currentTimeMillis();
                long time = (endTime - startTime) / 1000;

                log.info("Execute " + (Config.optNum - jobs.size()) + " tasks take " + time + " s");
                for(Thread th : list)
                    pool.remove(th);
                jobs.clear();
                
                if (count.get() > Config.optNum * 40 / 100) {
                    subject = "Fatal";
                    body = Utils.buildMessageBody(0, Config.nick, time, Config.optNum,
                            count.get(), bdCount.get());
                    normalFlag = false;
                    flag = false;
                } else if (count.get() > Config.optNum * 20 / 100) {
                    subject = "Error";
                    body = Utils.buildMessageBody(1, Config.nick, time, Config.optNum,
                            count.get(), bdCount.get());
                    normalFlag = false;
                    flag = false;
                } else if (count.get() > Config.optNum * 10 / 100) {
                    subject = "Warning";
                    body = Utils.buildMessageBody(2, Config.nick, time, Config.optNum,
                            count.get(), bdCount.get());
                    normalFlag = false;
                    flag = false;
                } else {
                    subject = "OK";
                    body = Utils.buildMessageBody(3, Config.nick, time, Config.optNum,
                            count.get(), bdCount.get());
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
                        body = Utils.buildMessageBody(4, Config.nick, 0, 0, 0, 0);
                        shouldSendM = true;
                        isSendOK = true;
                    }
                    if (hour != Config.sendOKTime)
                        isSendOK = false;
                }
                if(shouldSendM){
                    final String entity = body.toString();
                    final String subj = subject;
                    alert.execute(new Thread(){
                        public void run(){
                            try {
                                if(Config.smtpHost != null && Config.smtpHost.length() != 0){
                                    MailServer mailServer = new MailServer(Config.smtpHost,
                                            Config.smtpPort, Config.mailUserName,
                                            Config.mailPasswd);
                                    Mail mail = mailServer.createMail(Config.from,
                                            Config.to, "Status: "+subj, Config.nick,
                                            entity);
                                    mail.sendMail();
                                }
                                if(Config.phoneNums != null && Config.phoneNums.length != 0)
                                    for(String phone:Config.phoneNums){
                                        SMS.sendSMS(Config.nick+"-"+subj, phone);
                                    }
                            } catch (Throwable t) {
                            }
                        }
                    });
                }
                
                final String v1 = (putNum.get() == 0 ? "0" : df.format((double)putErrors.get() * 100 / putNum.get()));
                final String v2 = (getNum.get() == 0 ? "0" : df.format((double)getErrors.get() * 100 / getNum.get()));
                final String v3 = (deleteNum.get() == 0 ? "0" : df.format((double)deleteErrors.get() * 100 / deleteNum.get()));
                final String v4 = (listNum.get() == 0 ? "0" : df.format((double)listErrors.get() * 100 / listNum.get()));
                final long v5 = ((putNum.get() - putErrors.get() == 0) ? 0 : putTime.get() / (putNum.get() - putErrors.get()));
                final long v6 = ((getNum.get() - getErrors.get()) == 0 ? 0 : getTime.get() / (getNum.get() - getErrors.get()));
                final long v7 = ((deleteNum.get() - deleteErrors.get()) == 0 ? 0 : putTime.get() / (deleteNum.get() - deleteErrors.get()));
                final long v8 = ((listNum.get() - listErrors.get()) == 0 ? 0 : listTime.get() / (listNum.get() - listErrors.get()));
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
                            cmd = Config.gangliaAddr + "/bin/gmetric -n ListErrorRatio -v " + v4 + " -tfloat -u % -g hbase-monitor";
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
                            cmd = Config.gangliaAddr + "/bin/gmetric -n ListAverTime -v " + v8 + " -tfloat -u ms -g hbase-monitor";
                            rs = Utils.writeToGanglia(cmd);
                            if(rs != null)
                                log.error(rs);
                        }
                    });
                
                putNum.set(0);getNum.set(0);deleteNum.set(0);listNum.set(0);
                putErrors.set(0);getErrors.set(0);deleteErrors.set(0);listErrors.set(0);
                putTime.set(0);getTime.set(0);deleteTime.set(0);listTime.set(0);
            }catch(Throwable t){
                log.error(t.getMessage(), t);
            }
            
        }
        
        static Random rand = new Random();
        
        public static void does() throws IOException{
//            createTable();
            byte[] row = new byte[128];
            ThreadLocalRandom.current().nextBytes(row);
            byte[] value = new byte[256];
            ThreadLocalRandom.current().nextBytes(value);
            put(row, value);
            get(row);
            list();
        }
        
        public static void createTable() throws IOException{
            HBaseAdmin hadmin = new HBaseAdmin(conf);
            try {
                if(hadmin.tableExists(tableName))
                    return;
                HTableDescriptor hdesc = new HTableDescriptor(TableName.valueOf(tableName));
                HColumnDescriptor hfamily = new HColumnDescriptor("fy");
                hdesc.addFamily(hfamily);
                hadmin.createTable(hdesc);
            } finally {
                hadmin.close();
            }
        }
        
        public static void put(byte[] row, byte[] value) throws IOException{
            putNum.incrementAndGet();
            try {
                HTableInterface htable = conn.getTable(tableName);
                try {
                    long st = System.currentTimeMillis();
                    Put put = new Put(row);
                    put.add(fyName, clName, value);
                    htable.put(put);
                    long end = System.currentTimeMillis();
                    putTime.addAndGet(end - st);
                    log.info("put success takes " + (end - st) + "ms");
                } finally {
                    htable.close();
                }
            } catch(Exception e){
                log.info("put failed");
                putErrors.incrementAndGet();
                throw e;
            }
        }
        
        public static byte[] get(byte[] row) throws IOException{
            getNum.incrementAndGet();
            try {
                HTableInterface htable = conn.getTable(tableName);
                try {
                    long st = System.currentTimeMillis();
                    Get get = new Get(row);
                    Result rs = htable.get(get);
                    byte[] value = rs.getValue(fyName, clName);
                    long end = System.currentTimeMillis();
                    getTime.addAndGet(end - st);
                    log.info("get success takes " + (end - st) + "ms");
                    return value;
                } finally {
                    htable.close();
                }
            } catch(Exception e){
                log.info("get failed");
                getErrors.incrementAndGet();
                throw e;
            } 
        }
        
        public static void delete(byte[] row) throws IOException{
            deleteNum.incrementAndGet();
            try {
                HTableInterface htable = conn.getTable(tableName);
                try {
                    long st = System.currentTimeMillis();
                    Delete delete = new Delete(row);
                    htable.delete(delete);
                    long end = System.currentTimeMillis();
                    deleteTime.addAndGet(end - st);
                    log.info("delete success takes " + (end - st) + "ms");
                } finally {
                    htable.close();
                }
            } catch(Exception e){
                log.info("delete failed");
                deleteErrors.incrementAndGet();
                throw e;
            }
        }
        
        public static void list() throws IOException {
            listNum.incrementAndGet();
            try {
                HTableInterface htable = conn.getTable(tableName);
                try {
                    long st = System.currentTimeMillis();
                    Scan scan = new Scan();
                    scan.addColumn(fyName, clName);
                    byte[] startRow = new byte[16];
                    byte[] stopRow = new byte[16];
                    ThreadLocalRandom.current().nextBytes(startRow);
                    ThreadLocalRandom.current().nextBytes(stopRow);
                    scan.setStartRow(startRow);
                    scan.setStopRow(stopRow);
                    scan.setCaching(100);
                    
                    ResultScanner rscanner = htable.getScanner(scan);
                    Iterator<Result> rsItr = rscanner.iterator();
                    List<byte[]> rows = new ArrayList<byte[]>();
                    while(rsItr.hasNext()){
                        rows.add(rsItr.next().getRow());
                    }
                    long end = System.currentTimeMillis();
                    listTime.addAndGet(end - st);
                    log.info("list success takes " + (end - st) + "ms");
                    for(byte[] row : rows)
                        delete(row);
                } finally {
                    htable.close();
                }
            } catch(Exception e){
                log.info("list failed");
                listErrors.incrementAndGet();
                throw e;
            }
        }
        
        public void dropTable() throws IOException{
            HBaseAdmin hadmin = new HBaseAdmin(conf);
            try {
                hadmin.disableTable(tableName);
                hadmin.deleteTable(tableName);
            } finally {
                hadmin.close();
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
    
    static class Config {
        public static String address;
        
        public static int maxThreadNum = 10;
        public static int optNum = 400;
        public static int execTime = 600000;

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
                        + "/conf/monitor/hbaseMonitor.conf");
                p.load(is);
                address = p.getProperty("address");
                nick = p.getProperty("nick");
                runInter = Integer.parseInt(p.getProperty("runInter"));
                sendEmailInter = Integer.parseInt(p.getProperty("sendEmailInter"));
                smtpHost = p.getProperty("smtpHost");
                smtpPort = Integer.parseInt(p.getProperty("smtpPort"));
                mailUserName = p.getProperty("mailUserName");
                mailPasswd = p.getProperty("mailPasswd");
                from = p.getProperty("from");
                to = p.getProperty("to");
                maxThreadNum = Integer.parseInt(p.getProperty("maxThreadNum"));
                optNum = Integer.parseInt(p.getProperty("optNum"));
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
}
