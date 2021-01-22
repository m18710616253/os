package cn.ctyun.oos.server.usage;

import java.lang.reflect.Field;
import java.security.SecureRandom;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import cn.ctyun.common.Consts;
import cn.ctyun.common.conf.DataRegion;
import cn.ctyun.oos.common.Utils;
import cn.ctyun.oos.hbase.MetaClient;
import cn.ctyun.oos.metadata.MinutesUsageMeta;
import cn.ctyun.oos.metadata.MinutesUsageMeta.AvailableBandwidth;
import cn.ctyun.oos.metadata.MinutesUsageMeta.CodeRequestStats;
import cn.ctyun.oos.metadata.MinutesUsageMeta.Connection;
import cn.ctyun.oos.metadata.MinutesUsageMeta.FlowStats;
import cn.ctyun.oos.metadata.MinutesUsageMeta.RequestStats;
import cn.ctyun.oos.metadata.MinutesUsageMeta.SizeStats;
import cn.ctyun.oos.metadata.MinutesUsageMeta.UsageMetaType;
import cn.ctyun.oos.server.JettyServer;
import cn.ctyun.oos.server.util.Misc;
import sun.misc.Unsafe;

public class FiveMinuteUsage {
    private static final Log log = LogFactory.getLog(FiveMinuteUsage.class);
    private static ConcurrentHashMap<String, MinutesUsageMeta> usageMetas = new ConcurrentHashMap<>();

    private static ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock(
            false);
    private static MetaClient client = MetaClient.getGlobalClient();
    private static long fiveMinSec = 5 * 60 * 1000;
    
    private static Unsafe unsafe = null;
    static {
        try {
            Field field = Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            unsafe = (Unsafe)field.get(null);
        } catch (Exception e) {
            log.error("initialization unsafe error.", e);
        }
    }
    
    /**
     * 只用于更新MinutesUsage.CommonStats中的统计值和连接数
     * 
     * @param fieldName
     * @param v
     * @return
     */
    public static long addAndGet(String regionName, long usrId, String bucketName,
            String fieldName, long v, String storageType) {
        //如果regionName为空，通常为有异常抛出，不再进行记录
        if (regionName == null) {
            return -1;
        }
        //5分钟flush一次时，flush时的date
        long nowTime = System.currentTimeMillis();
        nowTime += fiveMinSec - nowTime % fiveMinSec;
        String day = Misc.formatyyyymmdd(new Date(nowTime));
        StringBuilder k = new StringBuilder();
        if (Utils.STORAGE_TYPES_RESTORE.contains(storageType)) {
            k.append(regionName).append("|").append(usrId).append("|").append(bucketName).append("|").append(storageType).append("|").append(day);
        } else
            k.append(regionName).append("|").append(usrId).append("|").append(bucketName).append("|").append(day);
        rwLock.readLock().lock();
        try {
            MinutesUsageMeta usageMeta = usageMetas.get(k.toString());
            if (usageMeta == null) {
                if (bucketName != null)
                    usageMeta = new MinutesUsageMeta(UsageMetaType.CURRENT_BUCKET,
                            regionName, usrId, bucketName, day, storageType);
                else
                    usageMeta = new MinutesUsageMeta(UsageMetaType.CURRENT_OWNER,
                            regionName, usrId, day, storageType);
            }
            MinutesUsageMeta oldMeta = usageMetas.putIfAbsent(k.toString(), usageMeta);
            if(oldMeta != null)
                usageMeta = oldMeta;
            
            if (fieldName.contains("connections")) {
                // 连接数
                Field f = Connection.class.getDeclaredField(fieldName);
                if (f != null) {
                    if (usageMeta.connection == null)
                        usageMeta.connection = new Connection();
                    long fieldOffset = unsafe.objectFieldOffset(f);
                    return compareAndSetValue(usageMeta.connection, f, fieldOffset, v);
                }
            } else if(fieldName.startsWith("code")) {
                // 返回码 区分的请求次数
                Field f = CodeRequestStats.class.getDeclaredField(fieldName);
                if (f != null) {
                    if (usageMeta.codeRequestStats == null)
                        usageMeta.codeRequestStats = new CodeRequestStats();
                    long fieldOffset = unsafe.objectFieldOffset(f);
                    return compareAndSetValue(usageMeta.codeRequestStats, f, fieldOffset, v);
                }
            } else if(fieldName.startsWith("size")) {
                // 容量
                Field f = SizeStats.class.getDeclaredField(fieldName);
                if (f != null) {
                    if (usageMeta.sizeStats == null)
                        usageMeta.sizeStats = new SizeStats();
                    long fieldOffset = unsafe.objectFieldOffset(f);
                    return compareAndSetValue(usageMeta.sizeStats, f, fieldOffset, v);
                }
            } else if(fieldName.startsWith("flow")) {
                // 流量
                Field f = FlowStats.class.getDeclaredField(fieldName);
                if (f != null) {
                    if (usageMeta.flowStats == null)
                        usageMeta.flowStats = new FlowStats();
                    long fieldOffset = unsafe.objectFieldOffset(f);
                    return compareAndSetValue(usageMeta.flowStats, f, fieldOffset, v);
                }
            } else if(fieldName.startsWith("req")) {
                // 请求次数
                Field f = RequestStats.class.getDeclaredField(fieldName);
                if (f != null) {
                    if (usageMeta.requestStats == null)
                        usageMeta.requestStats = new RequestStats();
                    long fieldOffset = unsafe.objectFieldOffset(f);
                    return compareAndSetValue(usageMeta.requestStats, f, fieldOffset, v);
                }
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        } finally {
            rwLock.readLock().unlock();
        }
        return -1;
    }
    
    static long compareAndSetValue(Object obj, Field f, long offset, long v) throws IllegalArgumentException, IllegalAccessException {
        long v0 = f.getLong(obj);
        long vNew = v0 + v;
        if((f.getName().equals("connections") || f.getName().equals("noNet_connections")) && vNew < 0) {
            vNew = 0;
        }
        if(!unsafe.compareAndSwapLong(obj, offset, v0, vNew))
            return compareAndSetValue(obj, f, offset, v);
        return vNew;
    }

    static {
        final Random random = new SecureRandom();
        final LinkedBlockingQueue<FlushTask> flushQueue = new LinkedBlockingQueue<>();
        // 定期将usageMetas中的数据提交到flushQueue
        new Thread() {
            public void run() {
                while (true) {
                    try {
                        // 不足整5分钟，等待，整5分钟flush数据到hbase
                        long st1 = System.currentTimeMillis();
                        Date date = Misc.getCurrentFiveMinuteDateCeil();
                        long st2 = date.getTime();
                        long deltaTime = st2 - st1;
                        if (deltaTime > 0) {
                            try {
                                Thread.sleep(deltaTime);
                            } catch (InterruptedException e) {
                            }
                        }
                        // 构造版本=时间戳+6位随机数
                        long version = MinutesUsageMeta.buildTimeVersion(st2) + random.nextInt(999999);
                        rwLock.writeLock().lock();
                        ConcurrentHashMap<String, MinutesUsageMeta> tmpUsageMetas;
                        try {
                            tmpUsageMetas = usageMetas;
                            usageMetas = new ConcurrentHashMap<String, MinutesUsageMeta>();
                        } finally {
                            rwLock.writeLock().unlock();
                        }
                        for (Entry<String, MinutesUsageMeta> entry : tmpUsageMetas.entrySet()) {
                            if (entry.getValue().connection == null) {
                                continue;
                            }
                            // 保留大于0的连接数
                            if (entry.getValue().connection.connections > 0) {
                                addAndGet(entry.getValue().regionName, entry.getValue().getUsrId(), entry.getValue().bucketName,
                                        "connections", entry.getValue().connection.connections, Consts.STORAGE_CLASS_STANDARD);
                            }
                            if (entry.getValue().connection.noNet_connections > 0) {
                                addAndGet(entry.getValue().regionName, entry.getValue().getUsrId(), entry.getValue().bucketName,
                                        "noNet_connections", entry.getValue().connection.noNet_connections, Consts.STORAGE_CLASS_STANDARD);
                            }
                        }
                        flushQueue.add(new FlushTask(tmpUsageMetas, version));
                    } catch (Throwable t) {
                        log.error(t.getMessage(), t);
                    }
                }
            };
        }.start();
        // 将flushQueue中的数据flush到数据库
        new Thread() {
            public void run() {
                while (true) {
                    try {
                        FlushTask task = flushQueue.take();
                        task.exec();
                    } catch (Exception e) {
                    }
                }
            };
        }.start();
    }
    
    public static void flushBeforeExit() {
        log.info("start flush stats.");
        Random random = new SecureRandom();
        Date date = Misc.getCurrentFiveMinuteDateCeil();
        long st2 = date.getTime();
        long version = MinutesUsageMeta.buildTimeVersion(st2) + random.nextInt(999999);
        rwLock.writeLock().lock();
        ConcurrentHashMap<String, MinutesUsageMeta> tmpUsageMetas;
        try {
            tmpUsageMetas = usageMetas;
            usageMetas = new ConcurrentHashMap<String, MinutesUsageMeta>();
            if (tmpUsageMetas.size() == 0) {
                log.info("finish flush stats.");
                return;
            }
        } finally {
            rwLock.writeLock().unlock();
        }                
        try {
            new FlushTask(tmpUsageMetas, version).exec();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        log.info("finish flush stats.");
    }

    /**
     * flush 任务
     *
     */
    private static class FlushTask {
        private static AvailableBandwidth lastAbw = new AvailableBandwidth();
        private ConcurrentHashMap<String, MinutesUsageMeta> tmpUsageMetas;
        private long version;

        public FlushTask(ConcurrentHashMap<String, MinutesUsageMeta> tmpUsageMetas,
                long version) {
            this.tmpUsageMetas = tmpUsageMetas;
            this.version = version;
        }

        public void exec() throws Exception {
            List<MinutesUsageMeta> usageMetas = new ArrayList<MinutesUsageMeta>();
            String fiveMinutes = Misc.formatyyyymmddhhmm(new Date(version / 1000000));
            if (fiveMinutes.contains("00:00")) {
                fiveMinutes = LocalDate.parse(fiveMinutes.substring(0, 10), Misc.format_uuuu_MM_dd).minusDays(1).toString() + " 24:00";
            }
            if(tmpUsageMetas.size() == 0) {
                log.info("The MinutesUsage Flush task queue is empty, thread break.");
            }
            //将用户相关的统计信息写到数据库
            for (MinutesUsageMeta meta : tmpUsageMetas.values()) {
                meta.timeVersion = version;
                usageMetas.add(meta);
                if (usageMetas.size() >= 1000) {
                    client.minutesUsageBatchInsert(usageMetas);
                    log.info("Flush MinutesUsageMeta success, num is "
                            + usageMetas.size() + ", version is" + version);
                    usageMetas.clear();
                }
            }
            if (!usageMetas.isEmpty()) {
                client.minutesUsageBatchInsert(usageMetas);
                log.info("Flush MinutesUsageMeta success, num is "
                        + usageMetas.size() + ", version is" + version);
                usageMetas.clear();
            }
            //API流量相关信息写到数据库
            String regionName = DataRegion.getRegion().getName();
            String day = Misc.formatyyyymmdd(new Date(version / 1000000));
            MinutesUsageMeta usage = new MinutesUsageMeta(UsageMetaType.CURRENT_AVAIL_BW, regionName, day, Consts.STORAGE_CLASS_STANDARD);
            usage.timeVersion = version;
            usage.availableBandwidth = new AvailableBandwidth();
            AvailableBandwidth currentAbw = new AvailableBandwidth();
            // 外网、内网可用带宽是指互联网、非互联网可用带宽
            currentAbw.pubInBW = JettyServer.lastNetIncomingBytes;
            currentAbw.pubOutBW = JettyServer.lastNetOutgoingBytes;
            currentAbw.priInBW = JettyServer.lastNoNetIncomingBytes;
            currentAbw.priOutBW = JettyServer.lastNoNetOutgoingBytes;
            usage.availableBandwidth.pubInBW = currentAbw.pubInBW - lastAbw.pubInBW;
            usage.availableBandwidth.pubOutBW = currentAbw.pubOutBW - lastAbw.pubOutBW;
            usage.availableBandwidth.priInBW = currentAbw.priInBW - lastAbw.priInBW;
            usage.availableBandwidth.priOutBW = currentAbw.priOutBW - lastAbw.priOutBW;
            lastAbw = currentAbw;
            client.minutesUsageInsert(usage);
        }
    }
}
