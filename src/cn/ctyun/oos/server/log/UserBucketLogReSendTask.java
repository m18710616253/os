package cn.ctyun.oos.server.log;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import cn.ctyun.common.Consts;
import cn.ctyun.oos.hbase.MetaClient;
import cn.ctyun.oos.server.conf.LogConfig;
import common.util.BlockingExecutor;

/**
 * 周期重传reSend目录下的bucketlog到用户指定的bucket
 * @author xiaojun
 *
 */
public class UserBucketLogReSendTask extends Thread{
    private static Log log = LogFactory.getLog(OOSLogTool.class);
    private boolean bExit = false;   // 终止任务标志
    private static MetaClient client = MetaClient.getGlobalClient();
    private BlockingExecutor exe = new BlockingExecutor(LogConfig.getProcessReSendCoreSize(), LogConfig.getProcessReSendCoreSize()*2, 100, 5000, "reSendbucketlog");
    /**
     * 开始重传任务
     */
    public void startTask() {
        moveSendToReSend();
        this.start();
    }
    /**
     * 停止重传任务
     */
    public void stopTask() {
        bExit = true;
        try {
            join();
        } catch (InterruptedException e) {
            log.error(e.getMessage(), e);
        }
    }
    /**
     * 加载resend目录文件
     * @param path  指定路径
     * @param list     文件结果
     */
    private void loadFiles(String path,  ArrayList<File> list) {
        File dir = new File(path);
        if(! dir.exists() && ! dir.isDirectory()){
            log.warn(dir.getAbsolutePath() + "is not a exist dictionary");
            return;
        }
        File[] files = dir.listFiles();  // 遍历第一层
        if (files.length > 0) {
            for (File file : files) {
                if(file.exists() && file.isFile()) {
                        list.add(file);
                }
            }
        }
        // 按照modify时间升序排序
        Collections.sort(list, new Comparator<File>() {
            @Override
            public int compare(File f1, File f2) {
                long diff = f1.lastModified() - f2.lastModified();
                if (diff < 0) {
                    return -1;
                } else if (diff > 0) {
                    return 1;
                } else {
                    return 0;
                }
            }
        });
    }

    /**
     * 移动所有send目录残留文件到resend目录
     */
    private void moveSendToReSend() {
        String dstLocation = LogConfig.getDstLocation();  // 本周期的根目录
        String sendDir = dstLocation  + File.separator + Consts.PATH_SEND;
        String reSendDir = Consts.PATH_RESEND;
        File dir = new File(sendDir);
        if (!dir.exists() && !dir.isDirectory()) {
            log.warn(dir.getAbsolutePath() + "is not a exist dictionary");
            return;
        }
        int count = 0;
        File[] files = dir.listFiles(); // 遍历第一层 临时目录
        if (files.length > 0) {
            for (File file : files) {
                if (file.exists() && file.isDirectory()) {
                    File[] childFiles = file.listFiles(); // 遍历第二层 残留的发送文件                                         // dir 情况
                    for (File fChild : childFiles) {
                        if (fChild.exists() && fChild.isFile()) {  
                            if(fChild.getName().endsWith(Consts.SUFFIX_TMP)) { // 临时文件还会重新处理，删除该文件
                                fChild.delete();
                            } else { // 残留待发送文件，移动到reSend目录
                                Util.move(fChild, dstLocation, reSendDir, fChild.getName());
                                count++;
                            }
                        }
                    }
                    // 删除临时目录
                    file.delete(); 
                }
            }
        }
        log.info("resend task move send dir file to resend dir count: "
                + count);
    }
    /**
     * 循环处理所有resend目录下待发送文件
     */
    private void doReSend() {
        AtomicInteger sendSucCount = new AtomicInteger(0); // 发送成功数量
        AtomicInteger sendErrNoMoveCount = new AtomicInteger(0); // 发送失败数量
        AtomicInteger sendErrMoveDamagedCount = new AtomicInteger(0); // 处理失败移动数量
        String dstLocation = LogConfig.getDstLocation();  // 本周期的根目录
        ArrayList<File> listReSend = new ArrayList<File>();  // 需要上传的bucketlog文件列表
        log.info("resend task start");
       
        // 加载reSend目录下的文件
        String reSendDir = dstLocation + File.separator + Consts.PATH_RESEND;
        loadFiles(reSendDir, listReSend);
        log.info("resend task load reSend dir count: "+ listReSend.size());
        if(listReSend.isEmpty()) {
            log.info("resend task no need resend file ");
            return;
        }
        
        final CountDownLatch putObjectLatch  = new CountDownLatch(listReSend.size());
        for(File file: listReSend) {
            exe.submit(new Runnable() {    
                @Override    
                public void run() {
                    try {
                        // 发送文件
                        Util.ePutObjectResult eRet = Util.putObject(file, client);
                        switch (eRet) {
                        case SUCCESS:  // 删除发送成功的文件
                            try {
                                sendSucCount.addAndGet(1);
                                file.delete();
                            }catch(Throwable e) {
                                log.error("send success . delete file err: " + e.getMessage(), e);
                            }
                            break;
                        case ERR_PARAM:  // 移动到dmaged目录 等待人工处理
                            try {
                                sendErrMoveDamagedCount.addAndGet(1);                           
                                String newDstDir = File.separator  + Consts.PATH_DAMAGED;
                                Util.move(file, dstLocation, newDstDir, file.getName());
                            }catch(Throwable e) {
                                log.error("reSend to user bucket err .  moveto damaged dir err：" + e.getMessage(), e);
                            }
                            break;
                        case ERR_SEND:     //  不处理等待一下个周期重发
                            sendErrNoMoveCount.addAndGet(1);
                            break;
                        default:
                            log.error("unkonw put result");
                        }
                    }catch(Throwable e) {
                        log.error("resend to user bucket err. file: " + file.getName(), e);
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
       
        StringBuilder build = new StringBuilder();
        build.append(" resend total: ").append(listReSend.size());
        build.append(" sucessl: ").append(sendSucCount.get());
        build.append(" err no move: ").append(sendErrNoMoveCount.get());
        build.append(" err to demaged: ").append(sendErrMoveDamagedCount.get());
        
        log.info("resend task end" + build.toString());
    }
    /**
     * 线程运行函数
     */
    public void run() {
        while(!bExit) {
            long reSendPeriodMills = LogConfig.getProcessReSendPriod() * 1000;
            long startTime = System.currentTimeMillis();
            try {
                doReSend();
            } catch (Throwable e) {
                log.error(e.getMessage(), e);
            }
            long endTime = System.currentTimeMillis();
            if (endTime - startTime >= reSendPeriodMills)
                log.warn("reSend log lasts times to long to reSend to oos bucket");
            else {
                try {
                    Thread.sleep(reSendPeriodMills - (endTime - startTime));
                } catch (InterruptedException e) {
                    log.error(e);
                }
            }
        }
    }
}
