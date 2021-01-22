package cn.ctyun.oos.server.log;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.ZipOutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import cn.ctyun.common.BaseException;
import cn.ctyun.common.Consts;
import cn.ctyun.common.Program;
import cn.ctyun.common.conf.DataRegion;
import cn.ctyun.common.conf.OOSConfig;
import cn.ctyun.common.conf.RegionHHZConfig;
import cn.ctyun.common.dsync.DLock;
import cn.ctyun.common.dsync.DSyncService;
import cn.ctyun.common.node.OosZKNode;
import cn.ctyun.common.utils.HostUtils;
import cn.ctyun.common.utils.LogUtils;
import cn.ctyun.oos.hbase.MetaClient;
import cn.ctyun.oos.metadata.AkSkMeta;
import cn.ctyun.oos.server.conf.LogConfig;
import common.util.BlockingExecutor;
import common.util.MD5Hash;

/**
 * 每天运行一次，归档距运行时7天之前的目录。
 * @author liuyt
 *
 */
public class ArchiveTool implements Program {
    static{
        System.setProperty("log4j.log.app", "archiveTool");
    }
    private static Log log = LogFactory.getLog(ArchiveTool.class);
    private static int ONE_DAY_MS = 24 * 60 * 60 * 1000;
    private static String zipaccesslogPrefix = "accesslogzip/";
    private static MetaClient client = MetaClient.getGlobalClient();
    private String id = String.valueOf(LogConfig.getId(HostUtils.getHostName()));
    private BlockingExecutor exeArchive = new BlockingExecutor(
            LogConfig.archiveCoreThreads, LogConfig.archiveMaxThreads,
            LogConfig.archiveQueueSize, LogConfig.archiveAliveTime,
            "archiveLog");
    private BlockingExecutor exeUpload = new BlockingExecutor(
            LogConfig.archiveCoreThreads, LogConfig.archiveMaxThreads,
            LogConfig.archiveQueueSize, LogConfig.archiveAliveTime,
            "uploadZips2oos");

    @Override
    public void exec(String[] args) throws Exception {
        DLock lock = null;
        DSyncService dsync = null;
        try {
            dsync = new DSyncService(RegionHHZConfig.getQuorumServers(), RegionHHZConfig.getSessionTimeout());
            lock = dsync.createLock(OosZKNode.getArchiveLock(DataRegion.getRegion().getName()) + "_" + id, "ArchiveTool");
            lock.lock();
        }catch(Throwable e) {
            log.error("create lock fail, ArchiveTool process exit err: "+ e.getMessage(), e);
            System.exit(-1);
        }
        LogUtils.startSuccess();
        while (true) {
            // long archivePeriodMills = LogConfig.getArchivePriod() * 1000;
            long startTime = System.currentTimeMillis();
            try {
                // 上次上传zip文件至OOS未成功的，本次再上传，上传成功后删除zip文件
                uploadZips2oos();
                archiveLog();
                cleanAccesslogsDir();
            } catch (Throwable e) {
                log.error("archive log", e);
            }
            long endTime = System.currentTimeMillis();
            if (endTime - startTime >= ONE_DAY_MS)
                log.warn("archive log lasts times too long");
            else {
                try {
                    Thread.sleep(ONE_DAY_MS - (endTime - startTime));
                } catch (InterruptedException e) {
                    log.error(e);
                }
            }
        }
    }

    private void archiveLog() {
        File archiveDir = new File(LogConfig.getArchiveDir());
        if (!archiveDir.exists() && !archiveDir.mkdirs()) {
            throw new RuntimeException("mkdir archive dir " + archiveDir.getAbsolutePath() + " failed");
        }
        
        File processedDir = new File(LogConfig.getDstLocation(), Consts.PATH_PROCESSED);
        if(! processedDir.exists() && ! processedDir.isDirectory()){
            log.error(processedDir.getAbsolutePath() + "is not a exist dictionary");
            return;
        }
        
        File[] files = processedDir.listFiles();
        int archiveBatchNum = LogConfig.getArchiveBatchNum();
        AtomicInteger count = new AtomicInteger(0);
        if (files.length > 0) {
            ArrayList<File> listParentDelete = new ArrayList<File>() ;  // 待删除的Date目录
            for (File file : files) {
                if (check7daysAgo(file) && checkAllFileProcessed(file)) {
                    listParentDelete.add(file);
                    File[] lsFiles = file.listFiles();
                    Arrays.sort(lsFiles);
                    // 拆分日志文件，每几个5分钟进行zip压缩
                    TreeMap<String, List<File>> fileMap = new TreeMap<>();
                    for (int i = 0; i < lsFiles.length;) {
                        String key = null;
                        List<File> fileList = new LinkedList<File>();
                        boolean first = true;
                        for (int j = 0; j < archiveBatchNum; j++) {
                            int index = i++;
                            if (index >= lsFiles.length)
                                break;
                            if (first)
                                key = file.getName() + "-" + lsFiles[index].getName();
                            fileList.add(lsFiles[index]);
                            first = false;
                        }
                        if (fileList.size() > 1)
                            key = key + "-" + fileList.get(fileList.size() - 1).getName();
                        fileMap.put(key, fileList);
                    }
                    for (Entry<String, List<File>> map : fileMap.entrySet()) {
                        count.addAndGet(1);
                        exeArchive.submit(new Runnable() {
                            @Override
                            public void run() {
                                try {
                                    doArchive(map, archiveDir);
                                }catch(Exception e) {
                                    log.error("doArchive failed.", e);
                                }finally {
                                    count.addAndGet(-1);
                                }
                            }
                        });
                    }
                }
            }
            while(count.get() != 0) {
                try {
                    log.info("wait for all doArchive task return. now task: " + String.valueOf(count.get()));
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    log.warn(e.getMessage(), e);
                }
            }
            // 清理date 目录
            for(File parentDir: listParentDelete) {
                deleteEmptyDir(parentDir);
            }
        }
    }
    
    private void doArchive(Entry<String, List<File>> map, File archiveDir) {
        String zipName = map.getKey() + Consts.SUFFIX_ARCHIVE;
        File dstZip = new File(archiveDir, zipName);
        ZipOutputStream zos = null;
        boolean zipSuccessed = true;
        try {
            zos = new ZipOutputStream(new FileOutputStream(dstZip));
            //压缩该时间段的原始日志
            for (File logFile:map.getValue()) {
                Util.zipFile(logFile, zos, "");
            }
            log.info("create "+map.getKey()+".zip successfully");
        } catch (FileNotFoundException e) {
            log.error("file " + dstZip + " not found. " + e);
            throw new RuntimeException(e);
        } catch (IOException e) {
            log.error("IOexception " + e);
            zipSuccessed = false;
        } finally {
            if (zos != null) {
                try {
                    zos.close();
                } catch (IOException e) {
                    log.error(e);
                }
            }
        }
        if (zipSuccessed) {
            for (File logFile:map.getValue()) {
                //删除该时间段的原始日志
                try {
                    if (!Util.deleteFile(logFile)) {
                        log.error("remove processed dir failed");
                    }
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }
            }
        }
        try{
            if (uploadZip(dstZip)) {
                // 上传成功的话才删除zip压缩文件
                dstZip.delete();
                log.info(dstZip.getAbsoluteFile() + " upload to oos and then deleted successfully ");
            } else {
                log.error(dstZip.getAbsoluteFile() + " upload to oos failed. ");
            }
        }catch(Exception e){
            log.error(e.getMessage(), e);
        }
    }
    private void cleanAccesslogsDir() {
        File accessLosgDir = new File(LogConfig.getDstLocation(), Consts.PATH_ACCESSLOGS);
        if(!accessLosgDir.exists() || !accessLosgDir.isDirectory()){
            log.error("cleanAccesslog err: " + accessLosgDir.getAbsolutePath() + " is not a exist directory");
            return;
        }
        File[] fileDates = accessLosgDir.listFiles(); // scan accesslogs
        if (fileDates.length > 0) {
            for (File fileDate : fileDates) {
                if(fileDate.exists() && fileDate.isDirectory() && check7daysAgo(fileDate)) {  // 检查accesslogs/date目录是否超期
                  File[] fileTimes = fileDate.listFiles();  // scan date、
                  for(File fileTime : fileTimes) {
                      deleteEmptyDir(fileTime);  // delete timeE
                  }
                  deleteEmptyDir(fileDate); // delete date
                }
            }
        }
    }
    
    private void deleteEmptyDir(File dir) {
        if ( dir.exists() && dir.isDirectory()){
            if (dir.listFiles().length == 0) {
                if(dir.delete()) {
                    log.info("delete empty parentDir" + dir + " deleted.");
                }else {
                    log.error("delete empty parentDir failed. dri: " + dir + " deleted.");
                }
            }else {
                log.error("delete empty parentDir failed. dir: " + dir.getPath() + "is not empty.");
            }
        }else {
            log.error("dir is  not exist or is not a  directory. dir: " + dir.getAbsoluteFile());
        }
    }
    
    private void uploadZips2oos(){
        File archiveDir = new File(LogConfig.getArchiveDir());
        if (!archiveDir.exists() && !archiveDir.mkdirs()) {
            throw new RuntimeException("mkdir archive dir " + archiveDir.getAbsolutePath() + " failed");
        }
        FilenameFilter filter = new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                if(name.endsWith(Consts.SUFFIX_ARCHIVE)){
                    return true;
                }
                return false;
            }
        };
        File[] files = archiveDir.listFiles(filter);
        log.info("Last time zip file number of unupload to OOS:" + files.length);
        for(File zip : files){
            exeUpload.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        if (uploadZip(zip)) {
                            // 上传成功的话才删除zip压缩文件
                            zip.delete();
                            log.info(zip.getAbsoluteFile() + " upload to oos and then deleted successfully ");
                        } else {
                            log.error(zip.getAbsoluteFile() + " upload to oos failed. ");
                        }
                    } catch (Exception e) {
                        log.error(e.getMessage(), e);
                    }
                }
            });
        }
    }

    //通过目录名字判断时候该目录在当前0点时刻7天之前 
    private boolean check7daysAgo(File file) {
        Date today = new Date();
        SimpleDateFormat format = new SimpleDateFormat("'UTC'yyyyMMdd");
        if(! file.getName().contains(Consts.KEYWORD_UTC)){
            return false;
        }
        try {
            Date agoDate = format.parse(file.getName());
            today = format.parse(format.format(today));// 得到今天的0点时刻UTC20131006-000000
            return ((today.getTime() - agoDate.getTime()) >= LogConfig.getArchivePriod() * 1000) ? true
                    : false;//7天
        } catch (ParseException e) {
            log.error("parsing file " + file.getName()
                    + " to check whether it is 7days ago " + e);
            return false;
        }
    }

    /**
     * 递归检查目录下是否全部为processed文件
     * @param processedDayDir 检查的目录
     * @return
     */
    private boolean checkAllFileProcessed(File processedDayDir) {
        if(processedDayDir.exists() && processedDayDir.isDirectory()) {
            File[] lsFiles = processedDayDir.listFiles();
            for (File file : lsFiles) {
                if (file.isFile()) {
                    String fileName = file.getName();
                    if (!fileName.endsWith(Consts.SUFFIX_PROCESSED)) {
                        return false;
                    }
                } else if (file.isDirectory()) {
                    if (!checkAllFileProcessed(file)) {
                        return false;
                    }
                }
            }
        }else {
            log.error("processedDayDir is  not exist or is not a  directory. processedDayDir: " + processedDayDir.getAbsoluteFile());
            return false;
        }
        return true;
    }


    private boolean uploadZip(File fileToUpload) throws Exception {
        long ownerId = BucketLog.logUserId;
        String bucketname = OOSConfig.getLogBucket();
        String objectName = zipaccesslogPrefix + DataRegion.getRegion() + "/"
                + fileToUpload.getName();
        try (InputStream in = new FileInputStream(fileToUpload)) {
            AkSkMeta asKey = new AkSkMeta();
            asKey.ownerId = ownerId;
            boolean res = client.akskSelectPrimaryKeyByOwnerId(asKey);
            if (!res)
                throw new BaseException(500, "no log user");
            int idx = objectName.lastIndexOf(".");
            if (Util.isObjectExists(bucketname, objectName, asKey.accessKey,
                    asKey.getSecretKey())) {
                int index = objectName.lastIndexOf(".");
                if (index != -1)
                    objectName = objectName.substring(0, index) + "."
                            + MD5Hash.digest(UUID.randomUUID().toString()).toString() + "_" + id
                            + objectName.substring(index);
                else
                    objectName += MD5Hash.digest(UUID.randomUUID().toString()).toString() + "_" + id;
            } else {
                if (idx != -1) {
                    objectName = objectName.substring(0, idx) + "_" + id + objectName.substring(idx);
                } else {
                    objectName += "_" + id;
                }
            }
            boolean putSuccessd = Util.putObjectToOOS(bucketname, objectName, asKey.accessKey, asKey.getSecretKey(), in,
                    fileToUpload.length());
            return putSuccessd;
        }
    }

    @Override
    public String usage() {
        return "run once a day, archive log 7 days before";
    }

    public static void main(String[] args) {
        try {
            new ArchiveTool().exec(args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}