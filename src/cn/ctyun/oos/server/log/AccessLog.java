package cn.ctyun.oos.server.log;

/**
 * @author: Cui Meng
 */
import java.io.File;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;

import org.apache.commons.lang3.time.DateUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import cn.ctyun.common.Consts;
import cn.ctyun.common.conf.OOSConfig;
import cn.ctyun.common.utils.HostUtils;
import common.util.BlockingExecutor;

public class AccessLog {
    private static final Log log = LogFactory.getLog(AccessLog.class);
    private String logdir;
    private String prefix = "";
    private String suffix = "";
    private boolean isFirst = true;
    private String nameBill = "";
    private FileOutputStream fwBill;
    private static int min5 = 5;
    private Date logNameTime;
    private Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone(Consts.KEYWORD_UTC),
            Locale.ENGLISH);
    private Calendar calendarlogNameTime = Calendar.getInstance(TimeZone.getTimeZone("UTC"),
            Locale.ENGLISH);
    private static final String tmp = Consts.SUFFIX_TMP;
    private static String pid = HostUtils.getPid();
    private static String separator = System.getProperty("line.separator");
    private static BlockingExecutor logExecutor = new BlockingExecutor(
            OOSConfig.getSmallCoreThreadsNum(), OOSConfig.getSmallMaxThreadsNum(),
            OOSConfig.getSmallQueueSize(), OOSConfig.getSmallAliveTime(), "bucketLogThread");
    private Integer lock = 0;
    static {
        new Thread() {
            public void run() {
                for (;;) {
                    String strStat = String
                            .format("AccessLogThreadPool Stats: ActiveCount:%d, CorePoolSize is:%d, LargestPoolSize is:%d, MaximumPoolSize is:%d, PoolSize is:%d, TaskCount is%d, tostring() is:%s",
                                    logExecutor.getActiveCount(), logExecutor.getCorePoolSize(),
                                    logExecutor.getLargestPoolSize(),
                                    logExecutor.getMaximumPoolSize(), logExecutor.getPoolSize(),
                                    logExecutor.getTaskCount(), logExecutor.toString());
//                    log.info(strStat);
                    try {
                        Thread.sleep(10000);
                    } catch (InterruptedException e) {
                        log.error("AccessLogThreadPool. ", e);
                    }
                    ;
                }
            }
        }.start();
    }
    
    public AccessLog(String logdir, String prefix, String suffix) {
        this.logdir = logdir;
        this.prefix = prefix;
        this.suffix = suffix;
    }
    
    private void getName(Calendar calendar) {
        String p = String.format("%s%sUTC%d%02d%02d-%02d%02d00-", logdir, prefix,
                calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH) + 1,
                calendar.get(Calendar.DATE), calendar.get(Calendar.HOUR_OF_DAY),
                calendar.get(Calendar.MINUTE));
        nameBill = p + Consts.TYPE_BILL + "-" + suffix + tmp;
    }
    
    private void writeOneLine(final String msg) {
        logExecutor.submit(new Runnable() {
            public void run() {
                try {
                    synchronized (lock) {
                        fwBill.write((msg + separator).getBytes(Consts.STR_UTF8));
                        fwBill.flush();
                    }
                } catch (IOException e) {
                    log.error(e.getMessage(), e);
                }
            }
        });
    }
    
    private void createFile() {
        try {
            fwBill = createOneFile(nameBill);
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }
    }
    
    private FileOutputStream createOneFile(String name) throws IOException {
        File file = new File(name);
        if (!file.getParentFile().exists())
            file.getParentFile().mkdirs();
        if (!file.exists()) {
            boolean success = file.createNewFile();
            file.setWritable(true, false);
            if (!success)
                log.error("create file failed, name is:" + name);
        }
        return new FileOutputStream(name, true);
    }
    
    private synchronized void renameOldTmpFile(Date now) {
        File file = new File(logdir);
        FilenameFilter filter = new FilenameFilter() {
            public boolean accept(File dir, String name) {
                if (name.endsWith(tmp)) {
                    return true;
                }
                return false;
            }
        };
        File[] doingFiles = file.listFiles(filter);
        if (doingFiles != null) {
            String currentPid = pid;
            for (int i = 0; i < doingFiles.length; i++) {
                String fileName = doingFiles[i].getName();
                if (doingFiles[i].length() == 0) {
                    boolean success = doingFiles[i].delete();
                    if (!success)
                        log.error("delete file failed, name is:" + doingFiles[i].getName());
                    continue;
                }
                String fields[] = fileName.split("-");
                String pid = fields[fields.length - 1].split("\\.")[0];
                if (!pid.equals(currentPid)) {
                    String name2 = doingFiles[i].getPath().replace(tmp, "");
                    File file2 = new File(name2);
                    boolean success = doingFiles[i].renameTo(file2);
                    if (!success)
                        log.error("rename file failed, name is:" + doingFiles[i].getName());
                }
            }
        }
    }
    
    private void writeOneLineByType(String msg, int type) {
        writeOneLine(msg);
    }
    
    synchronized public void write(String msg, int type) {
        Date now = new Date();
        calendar.setTime(now);
        if (isFirst)
            renameOldTmpFile(now);
        if (isFirst || (DateUtils.addMinutes(logNameTime, min5).compareTo(now) < 0)) {
            synchronized (lock) {
                close();
                int interval = calendar.get(Calendar.MINUTE) % min5;
                if (interval != 0) {
                    now = DateUtils.addMinutes(now, -interval);
                    calendar.setTime(now);
                }
                getName(calendar);
                logNameTime = now;
                calendarlogNameTime.setTime(logNameTime);
                createFile();
            }
            writeOneLineByType(msg, type);
            isFirst = false;
            return;
        }
        if (calendar.get(Calendar.MINUTE) % min5 == 0
                && calendarlogNameTime.get(Calendar.MINUTE) != calendar.get(Calendar.MINUTE)) {
            synchronized (lock) {
                close();
                getName(calendar);
                logNameTime = now;
                calendarlogNameTime.setTime(logNameTime);
                createFile();
            }
            writeOneLineByType(msg, type);
        } else {
            writeOneLineByType(msg, type);
        }
    }
    
    private void closeOne(FileOutputStream fos, String name) {
        if (fos != null)
            try {
                fos.close();
                File file = new File(name);
                if (file.length() == 0) {
                    boolean success = file.delete();
                    if (!success)
                        log.error("delete file failed, name is:" + file.getName());
                    return;
                }
                String name2 = name.replaceAll(tmp, "");
                File file2 = new File(name2);
                boolean success = file.renameTo(file2);
                if (!success)
                    log.error("rename file failed, name is:" + file.getName());
            } catch (IOException e) {
                log.error(e.getMessage(), e);
            }
    }
    
    public synchronized void close() {
        closeOne(fwBill, nameBill);
    }
}
