package cn.ctyun.oos.server.log;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Vector;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.ChannelSftp.LsEntry;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;

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
import cn.ctyun.oos.server.conf.LogConfig;
import cn.ctyun.oos.server.conf.LogConfig.AdapterEntry;
import common.util.BlockingExecutor;

public class FetchLogTool implements Program {
    static{
        System.setProperty("log4j.log.app", "fetchLogTool");
    }
    private static Log log = LogFactory.getLog(FetchLogTool.class);
    private String hostName = HostUtils.getHostName();
    private String id = String.valueOf(LogConfig.getId(hostName));
    private AdapterEntry[] entries = LogConfig.getAdapterEntries(hostName);
    private int adapterEntryNum = entries.length;
    private BlockingExecutor exe = new BlockingExecutor(adapterEntryNum, adapterEntryNum, 100, 5000, "fetchlog");

    @Override
    public void exec(String[] args) throws Exception {
        DLock lock = null;
        DSyncService dsync = null;
        try {
            dsync = new DSyncService(RegionHHZConfig.getQuorumServers(), RegionHHZConfig.getSessionTimeout());
            lock = dsync.createLock(OosZKNode.getFetchLogLock(DataRegion.getRegion().getName()) + "_" + id, "FetchLog");
            lock.lock();
        }catch(Throwable e) {
            log.error("create lock fail, FetchLogTool process exit err: "+ e.getMessage(), e);
            System.exit(-1);
        }
        LogUtils.startSuccess();
        while (true) {
            long fetchPeriodMills = LogConfig.getFetchPriod() * 1000;
            long startTime = System.currentTimeMillis();
            //防止AdapterEntry发生变化。
            entries = LogConfig.getAdapterEntries(hostName);
            if(adapterEntryNum < entries.length) {
                adapterEntryNum = entries.length;
                exe = new BlockingExecutor(adapterEntryNum, adapterEntryNum, 100, 5000, "fetchlog");
            }
            try {
                fetchLog();
            } catch (Throwable e) {
                log.error("fetch log", e);
            }
            long endTime = System.currentTimeMillis();
            if (endTime - startTime >= fetchPeriodMills)
                log.warn("fetch log lasts times too long");
            else {
                try {
                    Thread.sleep(fetchPeriodMills - (endTime - startTime));
                } catch (InterruptedException e) {
                    log.error(e);
                }
            }
        }
    }

    public void fetchLog() {
        /** 抓取日志 */
        if (entries != null && adapterEntryNum > 0) {
            final CountDownLatch latch  = new CountDownLatch(adapterEntryNum);
            for (AdapterEntry entry : entries) {
                exe.submit(new Runnable() {
                    @Override
                    public void run() {
                        try{
                            copyFromAdapter(entry);
                        } catch (JSchException e) {
                            log.warn(entry, e);
                        } catch (SftpException e) {
                            log.warn(entry, e);
                        } finally{
                            latch.countDown();
                        }
                    }
                });
            }
            while(true){
                try {
                    latch.await();
                    break;
                } catch (InterruptedException e) {
                    log.error(e.getMessage(), e);
                    continue;
                }
            }
        } else {
            log.warn("FetchLog on hostName:"+hostName+" doesn't config AdapterEntries.");
        }
    }

    @Override
    public String usage() {
        return "fetch log from adapter";
    }

    public void copyFromAdapter(final AdapterEntry entry) throws JSchException,
            SftpException {
        JSch client = new JSch();
        
        String privateKeyPath = LogConfig.getPrivateKeyPath();
        if(! new File(privateKeyPath).exists()){
            throw new RuntimeException("private key file:" + privateKeyPath + " doesnot exit.");
        }
        client.addIdentity(privateKeyPath);
        Session session = client.getSession(entry.user, entry.host, entry.port);
        Properties config =  new Properties();
        config.put("StrictHostKeyChecking", "no");
        session.setConfig(config);
        
        session.setTimeout(50000);
        session.connect();
        Channel channel = session.openChannel("sftp");
        channel.connect();
        ChannelSftp sftp = (ChannelSftp) channel;

        doCopyFromAdapter(sftp, entry.host);

        if (sftp != null) {
            sftp.disconnect();
            sftp = null;
        }

        if (session != null) {
            session.disconnect();
            session = null;
        }
    }

    /** adapter的源目录由AdapterConfig.getLogdir()指定，日志收集目录由${logdir}与源目录下文件名提取的时间戳确定 */
    private void doCopyFromAdapter(ChannelSftp sftp, String host) throws SftpException {
        File dstLocation = new File(LogConfig.getDstLocation(), Consts.PATH_ACCESSLOGS);
        if(!dstLocation.exists() && !dstLocation.mkdirs()){
            String errorMessage = dstLocation.getAbsolutePath() + " doesnot exist and fail to mkdir"; 
            log.error(errorMessage);
            throw new RuntimeException(errorMessage);
        }
        
        String cwdRoot = OOSConfig.getCwdRoot();//parent directory of oos current working directory
        Vector<?> cwds = null;
        try{
            cwds = sftp.ls(cwdRoot);
        }catch(SftpException e){
            log.error("On Host " + host + " ,Directory " + cwdRoot + " is not an exist root directory of all multi processes", e);
            return;
        }
        if(cwds == null || cwds.isEmpty()){
            log.error("On Host " + host + " ,Directory " + cwdRoot + " is not an exist root directory of all multi processes");
            return;
        }
        
        for(Object o : cwds){
            if(o instanceof LsEntry){
                LsEntry cwdFileEntry = (LsEntry) o;
                String cwdName = cwdFileEntry.getFilename();
                
                if(cwdName.equals(".") || cwdName.equals("..")){
                    continue;
                }
                String cwdAbsoName = new File(cwdRoot,  cwdName).getAbsolutePath();
                String accessLogDirAbsoName = new File(cwdAbsoName, OOSConfig.getLogdir()).getAbsolutePath();
                
                //now fetch access log from accesslog dir to remote log dir
                Vector<?> remoteFiles = null;
                try{
                    remoteFiles = sftp.ls(accessLogDirAbsoName);
                }catch(SftpException e){
                    log.error("Host " + host + " Working directory " + cwdAbsoName + " do not have " + OOSConfig.getLogdir() + " Directory");
                    continue;
                }
                if(remoteFiles == null || remoteFiles.size() == 0){
                    log.info(accessLogDirAbsoName + "has no files");
                    log.error("Host " + host + " Working directory " + cwdAbsoName + " have no files of Directory " + OOSConfig.getLogdir());
                    continue;
                }
                List<String> fileNames = new ArrayList<String>();
                for (Object file : remoteFiles) {
                    if (file instanceof LsEntry) {
                        LsEntry fileEntry = (LsEntry) file;
                        String fileName = fileEntry.getFilename();
                        if (fileName.equalsIgnoreCase(".")
                                || fileName.equalsIgnoreCase("..")
                                || fileName.equalsIgnoreCase(".svn")) {
                            continue;
                        }
                        if (fileName.contains(Consts.KEYWORD_UTC)
                                && !fileName.endsWith(Consts.SUFFIX_TMP)) {
                            fileNames.add(fileName);
                        }
                    }
                }

                if (fileNames.size() > 0) {
                    for (String fileName : fileNames) {
                        /** 得到远端文件名字,绝对路径 */
                        String srcAbsoluteFileName = (new File(accessLogDirAbsoName, fileName)).getAbsolutePath();

                        /** 确定要拷贝到的目录 */
                        String[] timeStaps = fileName.split("-");
                        if (timeStaps.length < 2) {
                            log.error("file format error:" + srcAbsoluteFileName);
                            continue;
                        }
                        File localDstDir = Util.mkDstDir(LogConfig.getDstLocation(), true, Consts.PATH_ACCESSLOGS, timeStaps[0], timeStaps[1]);
                        if(!localDstDir.exists() || !localDstDir.isDirectory()){
                            log.error("directory " + localDstDir.getAbsolutePath() + " is not a directory to place logs");
                            continue;
                        }

                        /** 将remote log文件拷贝到本地目录*/
                        sftp.get(srcAbsoluteFileName, localDstDir.getAbsolutePath());
                        /** 将本地文件添加.fetched标记， */
                        File localFile = new File(localDstDir, fileName);
                        if (!localFile.renameTo(new File(localDstDir, fileName + Consts.SUFFIX_FETCHED))) {
                            log.error(localFile.getAbsolutePath() + localFile.getName()
                                    + " rename failed");
                        }else{
                            log.info("Move file " + srcAbsoluteFileName + " from host " + host + " to directory " + localDstDir.getAbsolutePath() + " on localhost");
                        }
                        /** 删除remote log文件 */
                        sftp.rm(srcAbsoluteFileName);
                    }
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        new FetchLogTool().exec(args);
    }
}
