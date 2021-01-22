package cn.ctyun.oos.bssAdapter.userpackage;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.SQLException;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.auth.SigningAlgorithm;
import com.amazonaws.services.s3.internal.RestUtils;

import cn.ctyun.common.Program;
import cn.ctyun.common.conf.GlobalHHZConfig;
import cn.ctyun.common.dsync.DLock;
import cn.ctyun.common.dsync.DSyncService;
import cn.ctyun.common.node.OosZKNode;
import cn.ctyun.common.utils.HostUtils;
import cn.ctyun.oos.common.Utils;
import cn.ctyun.oos.server.conf.BssAdapterConfig;
import cn.ctyun.oos.server.conf.BssConfig;
import cn.ctyun.oos.server.conf.SynchroConfig;
import cn.ctyun.oos.server.db.DB;
import common.time.TimeUtils;
import common.tuple.Pair;

/**
 * 
 * 每天凌晨在指定的时间将本地资源池的商用用户套餐信息上传到中心资源池； 
 * 第一步：先从MySQL数据库userPackage表中取出电商用户的哟农户套餐信息放入内存队列中；
 * 第二部：将内存队列中的话单内容实例化成实际的TXT（csv）文件； 
 * 第三部：文件实例化完成以后回调函数通知控制台进行上传操作；
 * 
 * 关于启动参数： 
 * 1）服务启动不需要参数，对接当前用户的套餐余量；
 * 
 * 关于配置文件： 
 * 1）tempFilePath 应用启动所在服务器中的临时地址，用于生成临时文件； 
 * 2）userPackageSenderStartTime每天开始对接的时间
 */
public class BSSUserPackageSender implements Program {
    static {
        System.setProperty("log4j.log.app", "BSSUserPackageSender");
    }
    private static final Log log = LogFactory.getLog(BSSUserPackageSender.class);
    final ExecutorService readerExecutor = Executors.newFixedThreadPool(10); // 10

    private List<Future<Boolean>> readerFutures = new ArrayList<Future<Boolean>>();

    private static ScheduledThreadPoolExecutor scheduleThreadPool = new ScheduledThreadPoolExecutor(1);
    private static DateTimeFormatter format_time = DateTimeFormatter.ofPattern("HHmm");
    private static DateTimeFormatter format_day = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    private static String tempFileRoot;
    
    public BSSUserPackageSender() {
    }

    private static String getEndDate() {
        return LocalDate.now().plusDays(-1).format(format_day);
    }

    /*
     * 根据数据库记录数量计算开启线程数
     */
    private int getThreadCount() throws SQLException {
        int pageCount = DB.getInstance().getUserPackagePageCount();
        log.info("get " + pageCount + " pages from DB.");
        return pageCount;
    }

    /*
     * 分批次多线程从mySQL数据库中读取话单信息放入内存；
     */
    private UserCustomerFile getUserPackageData(int threadNum) throws SQLException,
            InterruptedException, ExecutionException, TimeoutException, IOException {

        // 从DB读取数据
        UserCustomerFile file = null;
        try {
            file = new UserCustomerFile().init(tempFileRoot, true);
            for (int i = 0; i < threadNum; i++) {
                PackageTask job = new PackageTask(i,file);
                Future<Boolean> feture = readerExecutor.submit(job);
                readerFutures.add(feture);
            }
            
            for(Future<Boolean> feture :readerFutures) {
                feture.get();
            }
        }finally {
            file.close();
        }
                
        return file;
    }

    private void uploadStart(UserCustomerFile files) {
        
        for(Pair<String,Integer> filePath:files.getResultFileList()) {
            File aFile = new File(filePath.first());
            
            if (!aFile.exists()) continue;
            
            int repeatTimes = 3;
            try {
                while(repeatTimes>0) {
                    try {
                        HttpURLConnection conn = (HttpURLConnection) (new URL(
                                BssAdapterConfig.protocol,
                                BssAdapterConfig.ip,
                                BssAdapterConfig.port,
                                "/?Action=SubmitUserPackage").openConnection());
    
                        conn.setUseCaches(false);
                        conn.setRequestProperty("Date",TimeUtils.toGMTFormat(new Date()));
                        conn.setRequestProperty("Host",BssAdapterConfig.ip + ":" + BssAdapterConfig.port);
                        conn.setRequestProperty("Content-Type","application/json");
                        conn.setRequestProperty("Content-Length",String.valueOf(filePath.second())); 
                        conn.setRequestProperty("FileName", aFile.getName());
                        conn.setRequestMethod("PUT");
                        conn.setDoOutput(true);
                        @SuppressWarnings("unchecked")
                        String canonicalString = RestUtils.makeS3CanonicalString("PUT",
                                Utils.toResourcePath(null, null,false),
                                new cn.ctyun.oos.common.OOSRequest(conn),null);
                        String signature = Utils.sign(canonicalString,BssAdapterConfig.localPool.getSK(),
                                SigningAlgorithm.HmacSHA1);
                        String authorization = "AWS " + BssAdapterConfig.localPool.ak + ":"+ signature;
                        conn.setRequestProperty("Authorization",authorization);
    
                        conn.connect();
                        try (DataOutputStream ds = new DataOutputStream(conn.getOutputStream());
                                FileInputStream fStream = new FileInputStream(aFile)) {                        
                            byte[] buffer = new byte[1024];
                            int length = -1;
                            while ((length = fStream.read(buffer)) != -1) {
                                ds.write(buffer, 0, length);
                            }
                            ds.flush();
                        }
    
                        int code = conn.getResponseCode();
                        if (code != 200) {
                            log.error("Fail to transform user packageFile to Server.");
                            Thread.sleep(3* 1000);
                        } else {
                            break;
                        }
                    } catch (Exception e) {
                        log.error(e);
                    } finally {
                        repeatTimes--;
                    }
                }                
            } finally {
                aFile.delete();
            }
        }
    }

    @Override
    public void exec(String[] args) throws Exception {
        HostUtils.writePid(new File(System.getProperty("user.dir"),
                "pid/oos_bssUserPackageSender.pid").getAbsolutePath());
        
        DSyncService dsync = new DSyncService(GlobalHHZConfig.getQuorumServers(),
                GlobalHHZConfig.getSessionTimeout());
        try {
            DLock lock = dsync.createLock(OosZKNode.bssUserPackageSenderLock, null);
            lock.lock();
            try {
                scheduleThreadPool.scheduleAtFixedRate(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            try {
                                //获取临时文件目录
                                tempFileRoot = BssConfig.tempFilePath;
                                if (!tempFileRoot.endsWith(File.pathSeparator)) {
                                    tempFileRoot += File.separator;
                                }
                                tempFileRoot += "userPackageSender";

                                // 启动时间校验
                                String sendTime = BssConfig.userPackageSenderStartTime;

                                if (sendTime == null || "".equals(sendTime))
                                    sendTime = "0130";
                                // 未到指定时间不执行
                                if (LocalDateTime.now().format(format_time).compareTo(sendTime) < 0) {
                                    log.info("User package interface in wrong time:" + LocalDateTime.now());
                                    return;
                                }

                                // 每5分钟执行一次，系统当前时间与指定时间相差1小时之内可以执行；
                                Duration duration = Duration.between(
                                    LocalDateTime.of(LocalDateTime.now().getYear(),
                                    LocalDateTime.now().getMonth(),
                                    LocalDateTime.now().getDayOfMonth(),
                                    Integer.parseInt(sendTime.substring(0, 2)),
                                    Integer.parseInt(sendTime.substring(2))),
                                    LocalDateTime.now());
                                if (duration.toMinutes() >= 5) {
                                    log.info("User package interface in wrong time:" + LocalDateTime.now());
                                    return;
                                }
                            } catch (Exception e) {
                                log.error(e.getMessage(), e);
                                System.exit(0);
                            }

                            log.info("User package interface begin:" + LocalDateTime.now());
                            batchJob();
                        } catch (Exception e) {
                            log.error(e.getMessage(), e);
                        }
                    }
                }, 0, 5, TimeUnit.MINUTES);

                Thread.currentThread().join();
            } finally {
                lock.unlock();
            }
        } finally {
            dsync.close();
        }
    }

    private void batchJob() {
        try {
            if(!SynchroConfig.propertyLocalPool.synchroUserPackage) {
                log.info(BssAdapterConfig.localPool.name + "does not need to send user package file.");
                return;
            }
            
            // 初始化
            readerFutures.clear();

            //统计传送数据记录条数
            int threadCount = getThreadCount();
            if (threadCount == 0) {
                log.info("no user package data");
                return;
            }

            //汇总数据；
            UserCustomerFile file = getUserPackageData(threadCount);
            
            //上传文件
            uploadStart(file);
        } catch (SQLException | InterruptedException | ExecutionException
                | TimeoutException | IOException e) {
            log.error("Bss user package interface error:", e);
        }
    }

    public static void main(String[] args) throws Exception {
        new BSSUserPackageSender().exec(args);
    }

    @Override
    public String usage() {
        return "Usage: BSSUserPackageSender \n";
    }
}
