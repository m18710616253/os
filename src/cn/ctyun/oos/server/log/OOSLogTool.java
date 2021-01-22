package cn.ctyun.oos.server.log;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONException;

import cn.ctyun.common.Consts;
import cn.ctyun.common.Program;
import cn.ctyun.common.conf.DataRegion;
import cn.ctyun.common.conf.OOSConfig;
import cn.ctyun.common.conf.RegionHHZConfig;
import cn.ctyun.common.dsync.DLock;
import cn.ctyun.common.dsync.DSyncService;
import cn.ctyun.common.ganglia.GangliaConsts;
import cn.ctyun.common.ganglia.GangliaMan;
import cn.ctyun.common.ganglia.GangliaPlugin;
import cn.ctyun.common.node.OosZKNode;
import cn.ctyun.common.utils.HostUtils;
import cn.ctyun.common.utils.LogUtils;
import cn.ctyun.oos.hbase.MetaClient;
import cn.ctyun.oos.server.conf.LogConfig;
import cn.ctyun.oos.server.log.logstats.OOSLogSLA;
import cn.ctyun.oos.server.log.logstats.PerfCheck;
import cn.ctyun.oos.server.log.logstats.SLAStat;
import common.time.TimeUtils;
import common.tuple.Triple;
import common.util.BlockingExecutor;
import common.util.JsonUtils;
import common.util.MD5Hash;

public class OOSLogTool implements Program {
    static {
        System.setProperty("log4j.log.app", "oosLogTool");
    }
    private static Log log = LogFactory.getLog(OOSLogTool.class);
    private final DateTimeFormatter oosNameFormat = DateTimeFormatter.ofPattern("'UTC'yyyy-MM-dd-HH-mm-ss");
    private final DateTimeFormatter logNameFormat = DateTimeFormatter.ofPattern("'UTC'yyyyMMddHHmmss");
    private SLAStat slaStat = new SLAStat();// 全局变量
    private PerfCheck perf = new PerfCheck();// 全局变量
    private OOSLogSLA logSla = new OOSLogSLA();
    private BlockingExecutor exe = new BlockingExecutor(LogConfig.getProcessCoreSize(), LogConfig.getProcessCoreSize()*2, 0, 5000, "processlog");
    private Map<String, String> fileName2Objectname = new ConcurrentHashMap<>();
    private static MetaClient client = MetaClient.getGlobalClient();
    private String id = String.valueOf(LogConfig.getId(HostUtils.getHostName()));
    private String dstLocation = "";  // 本周起内的路径，保证本周期内路径不变。
    public OOSLogTool() {
    }
    /**
     * 获取resend目录的大小
     * @return 返回目录大小 单位 MB
     */
    private long getReSendDirSize() {
        File reSendDir = new File(dstLocation, Consts.PATH_RESEND);
        long reSendSize = 0;
        if(reSendDir.exists() && reSendDir.isDirectory()) {
            File[] files = reSendDir.listFiles();
            for(File f : files) {
                if(f.exists() && f.isFile()) {
                    reSendSize += f.length();
                }
            }
        }
        return reSendSize/(1024*1024); // 转化MB
    }
    /**
     * 扫描所有accesslogs目录下的文件
     * @param unprocessedFiles 扫描后文件结果集合
     */
    private void ScanAllFetchedFiles(List<File> unprocessedFiles ) {   
        // 判断 accesslogs/fetched 目录是否存在
        File fetchedDir = new File(dstLocation, Consts.PATH_ACCESSLOGS );
        if (!fetchedDir.exists()) {
            String errorMessage = "ScanAllFetchedFiles log dir:" + fetchedDir.getAbsolutePath() + " doesnot exist";
            log.error(errorMessage);
            return;
        }
        try {
            Files.walkFileTree(fetchedDir.toPath(), new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path path, BasicFileAttributes attrs) {
                    Objects.requireNonNull(path);
                    Objects.requireNonNull(attrs);
                    // 判断过滤到的文件是否是fetched后缀且包含UTC，合法会添加到待处理队列
                    if(checkLegalFile(path.toFile())){
                        unprocessedFiles.add(path.toFile());
                    }
                    return FileVisitResult.CONTINUE;
                }
            });
        } catch (IOException e) {
            log.error(e.getMessage(), e);
            return;
        }
    }
    /**
     * 处理整个周期的扫描、分析、上传、统计所有的工作
     */
    private void upload2OOSBucket() {
        // 更新本周期的根目录
        dstLocation = LogConfig.getDstLocation();
        // 如果reSend目录中有超过阈值的未上传数据，不在处理文件，等待下一个周期在处理，防止需重传的文件堆积
        long reSendSize = getReSendDirSize();
        if(reSendSize > LogConfig.getProcessReSendSizeLimit()) {
            log.warn("resend dir size too large. stop process fetch file.  reSend dir size :" + reSendSize);
            return;
        }
       
        File tmpChildDirForSend = null;
        try{
            // 遍历获取accesslogs/fetched目录下所有的fetched后缀的文件
            final List<File> unprocessedFiles = new ArrayList<File>();// accesslog下的二级目录 accesslogs/UTC20140107/09500/xxx.fetched 一级目录f1:accesslogs/UTC20140107 二级目录f2:accesslogs/UTC20140107/09500
            ScanAllFetchedFiles(unprocessedFiles);
            
            // 如果待处理队列不为空，处理待处理队列中的文件
            if(unprocessedFiles.size() > 0){
                
                // 准备发送临时目录 accesslogs/send 
                File sendDir = new File(dstLocation, Consts.PATH_SEND);
                if(!sendDir.exists()) {
                    sendDir = Util.mkDstDir(dstLocation, true, Consts.PATH_SEND);
                }
                
                // 创建临时发送目录用于存放发送到用户bucket的日志
                tmpChildDirForSend = new File(sendDir, "." + Util.encode(TimeUtils.toYYYYMMddHHmmss(new Date())));
                try {
                    Files.createDirectories(tmpChildDirForSend.toPath());
                } catch (IOException e) {
                    log.error(e.getMessage(), e);
                    return;
                }
                
                if(!processAllLog(unprocessedFiles, tmpChildDirForSend)) {
                    log.error("upload2OOSBucket processAllLog fail.");
                    return;
                }
            }else {
                log.info("upload2OOSBucket no fount new process logs. wait for next round.");
                return;
            }
           
            // 处理本周期内统计信息
            if(unprocessedFiles.size() > 0){//如果有要处理的文件,才发送新值，否则的话，值为0
                if (LogConfig.sendToGanglia() == 1) {
                    slaStat.sendToGangliaAndAlarm();
                    perf.sendToGanglia();
                }
                if(LogConfig.writeSlaToLog() == 1){
                    logSla.checkTime();
                }
                slaStat.clear();
                perf.clear();
            }
            
        }finally{
            // 本周期结束，删除临时文件夹
            Util.deleteFile(tmpChildDirForSend);
        }
    }
    /**
     * 分析扫描后的accesslogs目录，生成上传用户bucket的日志文件
     * @param unprocessedFiles  输入待分析的fetched文件集合
     * @param tmpChildDirForSend  生成的待上传的日志文件流集合
     * @param trace  痕迹信息实例
     * @return true 成功；false 失败
     */
    private boolean generateUploadFile(List<File> unprocessedFiles , final File tmpChildDirForSend, ProcessTraceInfo trace) {
        final ConcurrentHashMap<String, FileGenerator> key2WriteObject = new ConcurrentHashMap<>();
         AtomicInteger waitCount = new AtomicInteger(0);
        List<Triple<File, String, String>> listMove = Collections.synchronizedList(new ArrayList<Triple<File, String, String>>());
        AtomicBoolean bExitProcess = new AtomicBoolean(false);
        for(final File f : unprocessedFiles){
            if(bExitProcess.get()) {
                break;
            }
            waitCount.addAndGet(1);
            exe.submit(new Runnable() {
                @Override
                public void run() {
                    try{
                        String newDstDirChild = new String("");  // 处理后的放置的子文件夹
                        String newFileName = new String(""); // 处理后的文件名后缀
                        // 处理文件
                        if(processOneFile(f, tmpChildDirForSend, key2WriteObject, dstLocation)) { // 移动处理后文件到processed目录
                            newDstDirChild = Consts.PATH_PROCESSED;
                            newFileName = f.getName().replaceAll(Consts.SUFFIX_FETCHED, Consts.SUFFIX_PROCESSED);
                            trace.processSucCount.addAndGet(1);
                        }else { // 移动处理不了的文件到damaged目录
                            newDstDirChild = Consts.PATH_DAMAGED;
                            newFileName = f.getName();
                            trace.processErrMoveDamagedCount.addAndGet(1);
                        }
                        // 生成目录目录同时携带fetched下的date和time目录
                        String newDstDir = newDstDirChild + File.separator  + f.getParentFile().getParentFile().getName() 
                                + File.separator + f.getParentFile().getName();
                        listMove.add(new Triple<File, String, String>(f, newDstDir, newFileName));
                    }catch(Throwable e){
                        bExitProcess.set(true);
                        trace.processErrNoMoveCount.addAndGet(1);
                        log.error("processOnefile fail. file: " + f.getAbsolutePath(), e);
                    }finally{
                        waitCount.addAndGet(-1);
                    }
                }
            });
        }
        // 等待所有fetched文件处理完毕
        while(waitCount.get() != 0){
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                log.error(e.getMessage(), e);
                continue;
            }
        }
        if(bExitProcess.get()) {
            log.error("Failed to process this cycle file.");
            return false; 
        }
        // 关闭所有上传用户bucket的临时对象的流。
        for (FileGenerator fw : key2WriteObject.values()) {
                try {
                    fw.close();
                } catch (Throwable e) {
                    log.error("when close file writer", e);              
                }
        }
        
        // 移动处理过的文件到对应的目录
        for(Triple<File, String, String> triple : listMove) {
            Util.move(triple.first(), dstLocation, triple.second(), triple.third());
        }
        return true;
    }
    /**
     * 上传所有的用户bucket日志
     * @param tmpChildDirForSend  待上传文件夹临时路径
     * @param trace  痕迹信息
     */
    private void sendAllBucketLog(final File tmpChildDirForSend, ProcessTraceInfo trace) {
        // 遍历发送所有的用户日志到用户指定bucket
        File[] uploadFiles = tmpChildDirForSend.listFiles();
        trace.generateCount = uploadFiles.length;
        int generateCount = uploadFiles.length;
        final CountDownLatch putObjectLatch  = new CountDownLatch(uploadFiles.length);
        for (final File f : uploadFiles) {
            if(f.getName().endsWith(Consts.SUFFIX_TMP)) {
                log.warn("file suffix invalid file: " + f.getName());
                putObjectLatch.countDown();
                continue;
            }
            exe.submit(new Runnable() {
                @Override
                public void run() {
                    try{
                        String newDstDir = "";
                        Util.ePutObjectResult eRet = Util.putObject(f, client);
                        switch (eRet) {
                        case SUCCESS:  // 发送成功，删除文件
                            trace.sendSucCount.addAndGet(1);
                            break;
                        case ERR_PARAM:
                            newDstDir = File.separator + Consts.PATH_DAMAGED;
                            trace.sendErrMoveDamagedCount.addAndGet(1);
                            break;
                        case ERR_SEND:
                            newDstDir = File.separator + Consts.PATH_RESEND;
                            trace.sendErrMoveResendCount.addAndGet(1);
                            break;
                        default:
                            log.error("unkonw put result");
                        }
                        if(!newDstDir.isEmpty()) {
                            Util.move(f, dstLocation, newDstDir, f.getName());
                        }
                    }catch(Throwable e){
                        log.error("send to user bucket err：" + e.getMessage(), e);
                        // 该文件因为未知原因失败，移动到resend目录等待重试
                        try {
                            trace.sendErrMoveResendCount.addAndGet(1);
                            String newDstDir = File.separator + Consts.PATH_RESEND;
                            Util.move(f, dstLocation, newDstDir, f.getName());
                        }catch(Throwable t) {
                            log.error("send to user bucket err .  moveto reSend dir err：" + t.getMessage(), t);
                        }
                    }finally{
                        putObjectLatch.countDown();
                    }
                }
            });
        }
        
        // 等待所有文件发送完毕
        while(true){
            try {
                putObjectLatch.await();
                break;
            } catch (InterruptedException e) {
                log.error(e.getMessage(), e);
                continue;
            }
        }
    }
    /**
     * 处理整个周期内的文件分析、用户bucket日志生成和上传
     * @param unprocessedFiles     待处理的fetched文件集合
     * @param tmpChildDirForSend    放置生成的待上传用户bucket的临时目录
     * @return true 处理成功；false 处理失败
     */
    private boolean processAllLog(List<File> unprocessedFiles , final File tmpChildDirForSend){
        
        ProcessTraceInfo trace = new ProcessTraceInfo();
        trace.processCount = unprocessedFiles.size();
        
        log.info("processLog deal start .file count: "+ String.valueOf(unprocessedFiles.size()));
        
       // 生成上传文件
       if(!generateUploadFile(unprocessedFiles, tmpChildDirForSend, trace)) {
           log.error("processLog deal fail " + trace.toString());
           return false;
        }
       
        // 发送所有文件到指定的bucket
        sendAllBucketLog(tmpChildDirForSend, trace);
        
        log.info("processLog deal end " + trace.toString());
        return true;
    }
    /**
     * 分析一个fetched文件
     * @param f  待分析的文件
     * @param tmpDirToPlaceUploadObjects  临时目录，存放生成对象
     * @param key2WriteObject  生成的文件流的集合
     * @param dstLocation  日志根目录
     * @return true 处理成功；false 处理失败
     * @throws IOException
     */
    public boolean processOneFile(File f, File tmpDirToPlaceUploadObjects, 
            ConcurrentHashMap<String, FileGenerator> key2WriteObject, String dstLocation) throws IOException{
        BucketLog bucketLog = new BucketLog(0);
        InputStreamReader in = new InputStreamReader(new FileInputStream(f), Consts.CS_UTF8);
        BufferedReader br = new BufferedReader(in);
        try{
            String timeStap = f.getParentFile().getParentFile().getName() + f.getParentFile().getName();
            
            String line = null;
            while((line = br.readLine()) != null){
                // 日志文件一条记录包含两行 第一行信息为owner id, owner name, target bucket, target prefix,第二行为日志内容
                String[] lineInfo = line.trim().split("\\s+");
                LogEntryMeta logEntryMeta;// 解析一条记录的第一行
                if (lineInfo.length == 3) {
                    logEntryMeta = new LogEntryMeta(lineInfo[0], lineInfo[1], lineInfo[2], "");
                } else if(lineInfo.length == 4){
                    logEntryMeta = new LogEntryMeta(lineInfo[0], lineInfo[1], lineInfo[2], lineInfo[3]);
                }else{
                    log.error("Illegal log entry:" + line + " .file is:" + f.getAbsolutePath());
                    return false;//损坏的文件不再处理此文件
                }
                
                line = br.readLine();
                
                // 获取upload用户bucket的流
                String key = logEntryMeta.ownerId + Util.tmpFileNameSep + logEntryMeta.ownerName + Util.tmpFileNameSep + logEntryMeta.targetName + Util.tmpFileNameSep + createObjectName(timeStap);
                FileGenerator fos = key2WriteObject.get(key);
                if(null == fos){
                    synchronized(key2WriteObject) {
                        fos = key2WriteObject.get(key);
                        if(null == fos) {
                            String objectNameSuffix = getObjectNameSuffix(f, dstLocation);
                            String fileName = key + Util.tmpFileNameSep + objectNameSuffix;
                            fos = new FileGenerator(tmpDirToPlaceUploadObjects, fileName, LogConfig.getProcessUserBucketObjCutSize());
                            key2WriteObject.put(key, fos);
                            //防止文件名过长导致生成临时文件失败，将prefix写入文件第一行
                            fos.write((Util.encode(logEntryMeta.targetPrefix) + "\n").getBytes(Consts.CS_UTF8));
                        }
                    }
                }
                synchronized (fos) {
                    fos.write((line + "\n").getBytes(Consts.CS_UTF8));
                }
                
                // 如果是oos log账号，则统计性能
                if (logEntryMeta.ownerName.equalsIgnoreCase(OOSConfig.getLogUser())) {
                    try {
                        bucketLog.exception=0;
                        JsonUtils.fromJson(bucketLog, line);
                    } catch (JSONException e) {
                        log.error("File broken:" + f.getAbsolutePath() + " JSONException,line:" + line, e);
                        return false;
                    }
                    //内部api请求不纳入统计。 
                    if(StringUtils.isBlank(bucketLog.originRequestId) || bucketLog.originRequestId.equals("-")) {
                        this.slaStat.parseLogLine(bucketLog);
                        //从accesslog文件名中获取hostname
                        String fname = f.getName();
                        String hostName = fname.split("-").length > 4 ? fname.split("-")[3] : "";
                        this.perf.parseLogLine(bucketLog, hostName);
                        this.logSla.parseLogLine(bucketLog);
                    }
                }
            }
        
        }finally{
            try{
                br.close();
            }catch(IOException e){
                log.error(e.getMessage(), e);
            }
        }
        return true;
    }
    
    /**
     * 生成上传bucket的文件后缀，原因防止重名覆盖，导致用户日志内容丢失
     * @param f  当前处理的文件
     * @param dstLocation  日志根目录
     * @return 目录后缀。返回"",该时间刻度没有处理过；否则，返回之前处理过的文件的host的MD5Hash
     */
    private String getObjectNameSuffix(File f, String dstLocation) {
        // 判断是否processed目录下有对应的time文件夹，如果有说明该周期处理过了,取出之前的文件host作为新的文件后缀
        String processedTimeDir = dstLocation + File.separator 
                + Consts.PATH_PROCESSED + File.separator
                + f.getParentFile().getParentFile().getName() + File.separator
                + f.getParentFile().getName();
        File fProcessed = new File(processedTimeDir);
        if(fProcessed.exists() && fProcessed.isDirectory()) {
            File[] list = fProcessed.listFiles();
            if(0 == list.length) {
                return "";
            }
            StringBuilder allHostNames = new StringBuilder();
            for(File fChild : list) {
                if(fChild.exists() && fChild.isFile()) {
                    String[] nameMeta = f.getName().split(Util.srcLogNameSep);
                    if (nameMeta.length < 4) {
                        log.error("illegal file name:" + fChild.getAbsolutePath());
                        continue;
                    }
                    allHostNames.append(nameMeta[3]);
                }
            }
            
            log.info("process time dir is exist "+ processedTimeDir + " file: "+ f.getAbsolutePath());
            return MD5Hash.digest(allHostNames.toString())
                    .toString();
        }
        return "";
    }
    
    /**
     * 创建日志名
     * @param timeStap  时间
     * @return
     */
    private String createObjectName(String timeStap) {
        String newTimeStr = LocalDateTime.parse(timeStap, logNameFormat).format(oosNameFormat);
        return Util.encode(DataRegion.getRegion() + "/" + newTimeStr);
    }
    
    /**
     * 校验要抓取的file,包含.fetched与UTC关键字
     * @param file
     * @return true 合法；false  不合法
     */
    private boolean checkLegalFile(File file) {
        if (file == null || !file.exists() || !file.isFile()) {
            return false;
        }
        String fileName = file.getName();
        if (fileName.endsWith(Consts.SUFFIX_FETCHED) && fileName.contains(Consts.KEYWORD_UTC)) {
            return true;
        }
        return false;
    }
   
    @Override
    public String usage() {
        return "put log in the src log location into buckets of oos";
    }
    
    @Override
    public void exec(String[] args) throws Exception {
        DLock lock = null;
        DSyncService dsync = null;
        try {
            dsync = new DSyncService(RegionHHZConfig.getQuorumServers(), RegionHHZConfig.getSessionTimeout());
            lock = dsync.createLock(OosZKNode.getLogLock(DataRegion.getRegion().getName()) + "_" + id, "OOSLogTool");
            lock.lock();    
        }catch(Throwable e) {
            log.error("create lock fail, OOSLogTool process exit err: "+ e.getMessage(), e);
            System.exit(-1);
        }
        
        LogUtils.startSuccess();
        log.info("log tool starts");
        // 启动重传任务
        new UserBucketLogReSendTask().startTask();
        // 启动ganglia心跳线程
        String gangliaFakeIp = GangliaMan.getFakeIpHost(GangliaMan.OOS_GROUP_NAME);
        new Thread() {
            @Override
            public void run() {
                int heartbearCount = 0; // 记录心跳次数
                for (;;) {
                    // 每30秒发送一次心跳
                    GangliaPlugin.sendToGangliaWithFakeIp(
                            GangliaConsts.LOG_HEARTBEAT + "_" + id, String.valueOf(1),
                            GangliaConsts.YTYPE_INT32, GangliaConsts.YNAME_NUM,
                            GangliaConsts.GROUP_NAME_LOG_HEARTBEAT,
                            "\"OOS log heartbeat\"", gangliaFakeIp);
                    
                    // 每5分钟发送一次damaged目录和resend目录数量
                    if(heartbearCount%10 == 0) {
                        // 发送reSend目录文件数
                        sendDamagedGanglia(gangliaFakeIp);
                        // 发送demaged目录文件数
                        sendReSendGanglia(gangliaFakeIp);
                    }
                    heartbearCount++;
                    try {
                        Thread.sleep(30000);  // 30sec
                    } catch (InterruptedException e) {
                        log.error(e.getMessage(), e);
                    }
                }
            }
        }.start();
        while (true) {
            long processPeriodMills = LogConfig.getProcessPriod() * 1000;
            long startTime = System.currentTimeMillis();
            try {
                upload2OOSBucket();
            } catch (Throwable e) {
                log.error(e.getMessage(), e);
            }
            long endTime = System.currentTimeMillis();
            if (endTime - startTime >= processPeriodMills)
                log.warn("process log lasts times to long to upload to oos bucket");
            else {
                try {
                    Thread.sleep(processPeriodMills - (endTime - startTime));
                } catch (InterruptedException e) {
                    log.warn(e);
                }
            }
        }
    }
    public int getDamageFileCount(File damagedDir) {
        int count = 0;
        File[] files = damagedDir.listFiles();
        for(File f : files) {
            if(f.isDirectory()) { // date dir fetched文件处理失败的文件
                File[] dirTimes = f.listFiles();
                for(File dirTime : dirTimes) {
                    if(dirTime.isDirectory()) {  // time dir
                        count += dirTime.listFiles().length;  // 累加fetched处理失败文件
                    }
                }
            }else {  // send失败，无法重试的文件
                count ++;  
            }
        }
        return count;
    }
    /**
     * 发送demaged目录下文件数到ganglia
     * @param gangliaFakeIp gangliaip信息
     */
    public void sendDamagedGanglia(String gangliaFakeIp) {
        File dir = new File(LogConfig.getDstLocation(), Consts.PATH_DAMAGED);
        int count = 0;
        if ( dir.exists() && dir.isDirectory()) {
            count = getDamageFileCount(dir);
        }else {
            log.warn("send damaged ganlia, dir err: " + dir.getAbsolutePath());
        }
        log.info("ganglia current damaged dir files count:" + String.valueOf(count));
        GangliaPlugin.sendToGangliaWithFakeIp(
                GangliaConsts.LOG_DAMAGED_COUNT + "_" + id,
                String.valueOf(count), GangliaConsts.YTYPE_INT32,
                GangliaConsts.YNAME_NUM,
                GangliaConsts.GROUP_NAME_LOG_DAMAGED_COUNT,
                "\"OOS log damaged dir file count \"", gangliaFakeIp);
    }
    /**
     * 发送resend目录的文件数到ganglia
     * @param gangliaFakeIp gangliaip信息
     */
    public void sendReSendGanglia(String gangliaFakeIp) {
        File dir = new File(LogConfig.getDstLocation(), Consts.PATH_RESEND);
        int count = 0;
        if ( dir.exists() && dir.isDirectory()) {
            count = dir.listFiles().length;
        }else {
            log.warn("send reSend ganlia, dir err: " + dir.getAbsolutePath());
        }
        log.info("ganglia current resend dir files count:" + String.valueOf(count));
        GangliaPlugin.sendToGangliaWithFakeIp(
                GangliaConsts.LOG_RESEND_COUNT + "_" + id,
                String.valueOf(count), GangliaConsts.YTYPE_INT32,
                GangliaConsts.YNAME_NUM,
                GangliaConsts.GROUP_NAME_LOG_RESEND_COUNT,
                "\"OOS log reSend dir file count \"", gangliaFakeIp);
    }
    public static void main(String[] args) {
        try {
            new OOSLogTool().exec(args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

class LogEntryMeta {
    public String ownerId;
    /** Name of owner */
    public String ownerName;
    /** Name of target bucket */
    public String targetName;
    /** prefix parameter which is used to list object Keys */
    public String targetPrefix;
    
    /** ownerId, ownerName, targetName, targetPrefix */
    LogEntryMeta(String ownerId, String ownerName, String targetName, String targetPrefix) {
        this.ownerId = ownerId;
        this.ownerName = ownerName;
        this.targetName = targetName;
        this.targetPrefix = targetPrefix;
    }
}


