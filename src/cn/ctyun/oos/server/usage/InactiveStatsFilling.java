package cn.ctyun.oos.server.usage;

import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.collect.Lists;

import cn.ctyun.common.Consts;
import cn.ctyun.common.conf.DataRegion;
import cn.ctyun.common.region.DataRegions;
import cn.ctyun.oos.common.Utils;
import cn.ctyun.oos.hbase.MetaClient;
import cn.ctyun.oos.metadata.BucketMeta;
import cn.ctyun.oos.metadata.MinutesUsageMeta;
import cn.ctyun.oos.metadata.MinutesUsageMeta.SizeStats;
import cn.ctyun.oos.metadata.MinutesUsageMeta.UsageMetaType;
import cn.ctyun.oos.metadata.OwnerMeta;
import cn.ctyun.oos.server.conf.PeriodUsageStatsConfig;
import cn.ctyun.oos.server.util.Misc;
import common.util.BlockingExecutor;

/**
 * 补充不活跃统计数据相关方法
 * 
 * @author wushuang
 *
 */
public class InactiveStatsFilling extends Thread{

    private boolean isGlobal = false;

    private static Log log = LogFactory.getLog(InactiveStatsFilling.class);

    /** 上一次补充非活跃数据到哪天 */
    private static LocalDate lastFillDate = null;

    private static MetaClient client = MetaClient.getGlobalClient();

    private static StatsTasksBuilder taskBuilder;
    
    private static LinkedBlockingQueue<StatsTask> taskQueue = new LinkedBlockingQueue<>();
    private static LinkedBlockingQueue<Long> invokeQueue = new LinkedBlockingQueue<>();
    private static BlockingExecutor exec = new BlockingExecutor(100, 100, 10000, 100, "InactiveStatsFilling");
    
    // 将持续不活跃的用户数据缓存 
    /** 上一次不活跃的用量 */
    private static Map<String, MinutesUsageMeta> lastInactiveUsage = new ConcurrentHashMap<>();
    /** 本次补全时的不活跃用量 */
    private static Map<String, MinutesUsageMeta> thisInactiveUsage = new ConcurrentHashMap<>();
    
    private static final int INACTIVE_USAGE_NUM = 500000;
    
    //DataRegions.getAllRegions()是不可变更的，所以需要new一个
    List<String> regions = new ArrayList<String>(DataRegions.getAllRegions());
    
    public InactiveStatsFilling(boolean isGlobal) {
        this.isGlobal = isGlobal;
        taskBuilder = new StatsTasksBuilder();
        taskBuilder.start();
        if (isGlobal) {
            regions.removeAll(PeriodUsageStatsConfig.regionNames);
        } else {
            regions = Lists.newArrayList(DataRegion.getRegion().getName());
        }
    }
    
    public void invoke(long lastStatsTime) {
        invokeQueue.add(lastStatsTime);
    }
    
    public void run() {
        String regionName = null;
        if (!isGlobal) {
            regionName = DataRegion.getRegion().getName();
        }
        while (true) {
            try{
                long lastStatTime = invokeQueue.take();
                log.info("last stats time is " + lastStatTime);
                if(lastFillDate == null) {
                    String date = UsageResult.getLastFillToDate(regionName);
                    lastFillDate = LocalDate.parse(date);
                }
                LocalDate endFillDate = LocalDate.parse(Misc.formatyyyymmdd(new Date(lastStatTime))).minusDays(1);
                while (lastFillDate.isBefore(endFillDate)) {
                    long startTime = System.currentTimeMillis();
                    synchronized(taskBuilder) {
                        taskBuilder.notifyAll();
                    }
                    List<Future<?>> futures = new LinkedList<>();
                    for(int i = 0; i < exec.getCorePoolSize(); i++)
                        futures.add(exec.submit(new StatsTasksHandler()));

                    boolean hasError = false;
                    for(Future<?> f : futures)
                        try {
                            f.get();
                        } catch (Exception e) {
                            log.error("For inactive filling. " + e.getMessage(), e);
                            hasError = true;
                        }
                    
                    if(hasError)
                        break;
                    LocalDate thisFillDate = lastFillDate.plusDays(1);
                    String fillDate = thisFillDate.format(Misc.format_uuuu_MM_dd);
                    UsageResult.putLastFillToDate(regionName, fillDate);
                    //global的统计需要把所有没升级的资源池都插一条，下个版本可以删除
                    if (isGlobal) {
                        for (String region : regions) {
                            UsageResult.putLastFillToDate(region, fillDate);
                        } 
                    }
                    lastFillDate = thisFillDate;
                    lastInactiveUsage.clear();
                    lastInactiveUsage.putAll(thisInactiveUsage);
                    thisInactiveUsage.clear();
                    long endTime = System.currentTimeMillis();
                    log.info("finish fill inactive usage. filling date: " + fillDate + ". takes " + (endTime-startTime) + " millisecond");
                }
            } catch (Throwable t) {
                log.error(t.getMessage(), t);
            }
        }
    }
    
    /**
     * 构建任务，每一批用户去构建rowkey进行查询与补全
     *
     */
    class StatsTasksBuilder extends Thread {
        private transient boolean isDone = true;
        
        public void run() {
            while(true) {
                try{
                    synchronized(this) {
                        wait();
                    }
                    try {
                        isDone = false;
                        String startRow = null;
                        while (true) {
                            if(taskQueue.size() > 30) {
                                try {
                                    Thread.sleep(1000);
                                } catch (Exception e) {
                                }
                                continue;
                            }
                            List<OwnerMeta> owners = client.ownerList(startRow == null ? null : Bytes.toBytes(startRow), 1000);
                            if (owners.isEmpty())
                                break;
                            startRow = buildStatsTasks(owners, lastFillDate);
                        }
                    } catch (Exception e) {
                        log.error(e.getMessage(), e);
                    } finally {
                        isDone = true;
                    }
                } catch (Throwable t) {
                    log.error(t.getMessage(), t);
                }
            }
        }
        
        boolean isDone() {
            return isDone;
        }
        
        /**
         * 构建待补全日期的所有rowkey，10000个rowkey以上时构建一个处理任务放入队列
         * @param owners
         * @param date 已补全日期
         * @return
         * @throws Exception
         */
        String buildStatsTasks(List<OwnerMeta> owners, LocalDate date) throws Exception {
            String k = null;
            List<String> rowKeys = new LinkedList<String>();
            
            //待补全日期
            String currentDate = date.plusDays(1).format(Misc.format_uuuu_MM_dd);
            for(String storageType : Utils.STORAGE_TYPES) {
                for (OwnerMeta owner : owners) {
                    List<BucketMeta> buckets = client.bucketList(owner.getId());
                    for (String region : regions) {
                        String ownerK = region + "|" + UsageMetaType.DAY_OWNER + "|" + owner.getId() + (storageType.equals(Consts.STORAGE_CLASS_STANDARD_IA) ? ("|" + storageType) : "") +"|" + currentDate;
                        rowKeys.add(ownerK);
                        for (BucketMeta bucketMeta : buckets) {
                            String bucketK = region + "|" + UsageMetaType.DAY_BUCKET + "|" + owner.getId() + (storageType.equals(Consts.STORAGE_CLASS_STANDARD_IA) ? ("|" + storageType) : "") + "|" + bucketMeta.getName() + "|" + currentDate;
                            rowKeys.add(bucketK);
                        }
                        if (rowKeys.size() > 10000) {
                            StatsTask task = new StatsTask(rowKeys, date);
                            taskQueue.add(task);
                            rowKeys = new LinkedList<String>();
                        }
                        k = Bytes.toString(owner.getRow());
                    }
                }
            }
            if (rowKeys.size() > 0) {
                StatsTask task = new StatsTask(rowKeys, date);
                taskQueue.add(task);
                rowKeys = new LinkedList<String>();
            }
            return k + Character.MIN_VALUE;
        }
    }
    
    class StatsTasksHandler implements Callable<Boolean> {
        public Boolean call() throws Exception {
            while (true) {
                StatsTask task = taskQueue.poll(10, TimeUnit.SECONDS);
                if (task != null)
                    task.doRun();
                else if (taskBuilder.isDone())
                    break;
            }
            return true;
        }
    }
    
    class StatsTask {
        private List<String> rowKeyList = null;
        private LocalDate fullUsageDate;

        public StatsTask(List<String> rowkeyList, LocalDate fullUsageDate) {
            this.rowKeyList = rowkeyList;
            this.fullUsageDate = fullUsageDate;
        }
        
        public void doRun() throws Exception {
            // 得到待补全日期所有活跃用户数据
            Map<String, MinutesUsageMeta> list = client.minutesUsageBatchSelect(rowKeyList);
            for(String key : list.keySet()) {
                log.info("For inactive Stats. active minutesUsageMetas contains key : " + key);
            }
            // 从所有待补全日期的rowkey中去除活跃数据，得到非活跃rowkey
            rowKeyList.removeAll(list.keySet());
            log.info("For inactive Stats. inactive owner rowkeys size is " + rowKeyList.size() + "at date " + fullUsageDate.format(Misc.format_uuuu_MM_dd));
            // 得到已补全日期的所有非活跃rowkeys
            List<String> inactiveUsageRowKeys = rowKeyList.parallelStream().map(x -> {
                String v = x.substring(0, x.lastIndexOf("|")) + "|" + fullUsageDate.format(Misc.format_uuuu_MM_dd);
                return v;
            }).collect(Collectors.toList());
            // 所有非活跃metas，只保留容量，补全，未select到的数据代表没有容量数据不需要补全
            Map<String, MinutesUsageMeta> inactiveUsageList = new HashMap<>();
            // 从缓存中取上一次补全过的非活跃数据，缓存中取不到的再查询数据库
            List<String> toBatchSelectKeys = new LinkedList<>();
            if(!lastInactiveUsage.isEmpty()) {
                for(String key : inactiveUsageRowKeys) {
                    if(lastInactiveUsage.containsKey(key)) {
                        log.info("For inactive Stats. contained in lastInactiveUsage the rowkey is " + key);
                        inactiveUsageList.put(key, lastInactiveUsage.get(key));
                    } else
                        toBatchSelectKeys.add(key);
                }
            } else
                toBatchSelectKeys = inactiveUsageRowKeys;
            
            for(String key : toBatchSelectKeys) {
                log.info("For inactive Stats. toBatchSelectKeys contains key : " + key);
            }            
            Map<String, MinutesUsageMeta> selectedInactiveUsage = client.minutesUsageBatchSelect(toBatchSelectKeys);
            for(String key : selectedInactiveUsage.keySet()) {
                log.info("For inactive Stats. selectedInactiveUsage contains key : " + key);
            } 
            inactiveUsageList.putAll(selectedInactiveUsage);
            
            List<MinutesUsageMeta> toDelete = new ArrayList<MinutesUsageMeta>();
            // 待补全日期
            fullUsageDate = fullUsageDate.plusDays(1);
            int count = 0;
            for (MinutesUsageMeta meta : inactiveUsageList.values()) {
                if(isEmptySize(meta.sizeStats)) {
                    toDelete.add(meta);
                    count++;
                    if (count < 100) {
                        log.info("For inactive Stats. need to delete meta " + meta.getKey() + " sizeStats is " + (meta.sizeStats == null ? null : meta.sizeStats.toJsonString())  + "at date " + fullUsageDate.format(Misc.format_uuuu_MM_dd));
                    }
                    continue;
                }
                meta.time = fullUsageDate.format(Misc.format_uuuu_MM_dd);
                meta.timeVersion = fullUsageDate.atStartOfDay().toInstant(ZoneOffset.of("+8")).toEpochMilli();
                if(thisInactiveUsage.size() < INACTIVE_USAGE_NUM) {
                    thisInactiveUsage.put(meta.getKey(), meta);
                }
                meta.clearCommonStatsExceptSize();
                meta.sizeStats.size_peak = meta.sizeStats.size_total;
                meta.sizeStats.size_originalPeak = meta.sizeStats.size_originalTotal;
                meta.sizeStats.size_avgTotal = meta.sizeStats.size_total;
                meta.sizeStats.size_avgAlin = meta.sizeStats.size_alin;
                meta.sizeStats.size_avgRedundant = meta.sizeStats.size_redundant;
                meta.sizeStats.size_avgOriginalTotal = meta.sizeStats.size_originalTotal;
            }
            log.info("For inactive Stats. deleted empty size meta size is " + toDelete.size() + "at date " + fullUsageDate.format(Misc.format_uuuu_MM_dd));
            inactiveUsageList.values().removeAll(toDelete);
            List<MinutesUsageMeta> toInsert = new ArrayList<MinutesUsageMeta>(inactiveUsageList.values());
            client.minutesUsageBatchInsert(toInsert);
            for (MinutesUsageMeta meta : toInsert) {
                log.info("For inactive Stats. successed fill inactive meta rowkey is " + meta.getKey());
            }
            log.info("For inactive Stats. successed fill inactive rowkey size is " + inactiveUsageList.values().size() + "at date " + fullUsageDate.format(Misc.format_uuuu_MM_dd));

        }
        
        private boolean isEmptySize(SizeStats size) {
            if (size == null) {
                return true;
            }
            return size.size_total == 0 && size.size_alin == 0 && size.size_redundant == 0 && size.size_originalTotal == 0;
        }
    }
}
