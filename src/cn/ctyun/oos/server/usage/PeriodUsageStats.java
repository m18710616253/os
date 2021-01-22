package cn.ctyun.oos.server.usage;

import cn.ctyun.common.Consts;
import cn.ctyun.common.Program;
import cn.ctyun.common.conf.GlobalHHZConfig;
import cn.ctyun.common.dsync.DLock;
import cn.ctyun.common.dsync.DSyncService;
import cn.ctyun.common.node.OosZKNode;
import cn.ctyun.common.region.DataRegions;
import cn.ctyun.common.utils.LogUtils;
import cn.ctyun.oos.common.Utils;
import cn.ctyun.oos.hbase.MetaClient;
import cn.ctyun.oos.metadata.MinutesUsageMeta;
import cn.ctyun.oos.metadata.MinutesUsageMeta.Connection;
import cn.ctyun.oos.metadata.MinutesUsageMeta.SizeStats;
import cn.ctyun.oos.metadata.MinutesUsageMeta.UsageMetaType;
import cn.ctyun.oos.server.conf.PeriodUsageStatsConfig;
import cn.ctyun.oos.server.util.Misc;
import common.tuple.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.Bytes;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.*;

/**
 * 统计用户用量，包括owner、bucket级别的5分钟粒度、1小时粒度、1天粒度；region级别的5分钟粒度
 * 
 */
public class PeriodUsageStats implements Program {
    static {
        System.setProperty("log4j.log.app", "periodUsageStats");
    }
    private static Log log = LogFactory.getLog(PeriodUsageStats.class);
    private static MetaClient client = MetaClient.getGlobalClient();
    
    private static final long FIVE_MINUTES = 5 * 60 * 1000;

    //1GB的cache空间，算法为每个MinutesUsageMeta按1KB计算，1GB约cache 1000000个MinutesUsageMeta
    private static UsageCache<MinutesUsageMeta> cachedUsageMeta = new UsageCache<MinutesUsageMeta>(50000);

    /** 记录数据域容量增量<region,<fiveMinutesTime,sizeStats>>，在一次统计后经过叠加数据域容量插入数据库 */
    private static Map<String, Map<Long, MinutesUsageMeta>> regionIcreCapacity = new HashMap<String, Map<Long, MinutesUsageMeta>>();
    
    /** 记录数据域容量，通过读取FIVE_MINUTES类型最新数据获得 */
    private static Map<String, SizeStats> regionCapacity = new HashMap<String, MinutesUsageMeta.SizeStats>();
    
    /** 进程启动时读取的最新的FIVE_MINUTES类型数据，即启动时region级别统计数据的截止时间，为了防止异常中断时，region增量数据多加的问题 */
    private static long lastReginFiveMinutesTime;
    
    /** 删除CURRENT类型  保证线程安全*/
    private static List<Pair<byte[], Long>> toDeleteList = Collections.synchronizedList(new ArrayList<Pair<byte[], Long>>());
        
    /** 补全非活跃线程*/
    private static InactiveStatsFilling inactiveStatsFilling = new InactiveStatsFilling(true);
    
    /**重试时间段以及重试次数的map key是starttime,endtime  value是重试了几次*/
    Map<String, Integer> retryMap = new HashMap<String, Integer>();
    
    // 从配置文件中获取starttime，只有重启时生效
    private long lastStatsTime = PeriodUsageStatsConfig.lastStatsTime;                
    // 每隔interval毫秒统计一次，只有重启时生效
    private long interval = PeriodUsageStatsConfig.periodUsageStatsInterval;
    // 每次只统计usageStatsTimeAgo毫秒以前的
    private long usageStatsTimeAgo = PeriodUsageStatsConfig.usageStatsTimeAgo;
    // 每轮计算时一个开始时间和结束时间
    private long startTime;
    private long endTime;
        
    //用于DoStatsTask
    /** 运行统计计算线程的线程池*/
    private static ThreadPoolExecutor es = new ThreadPoolExecutor(10, 100, 1000, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());

    public void exec(String[] args) throws Exception {
        DSyncService dsync = new DSyncService(GlobalHHZConfig.getQuorumServers(), GlobalHHZConfig.getSessionTimeout());
        try {
            DLock lock = dsync.createLock(OosZKNode.periodUsageStats, null);
            lock.lock();
            LogUtils.startSuccess();
            try {
                // 统计带宽95峰值 
                new PeakBandWidthStats(true).start();
                inactiveStatsFilling.start();
                //获取数据域最新的五分钟统计值
                Pair<Long, Map<String, SizeStats>> lastRegionUsage = getLatestFiveMinutesRegionUsage();
                lastReginFiveMinutesTime = lastRegionUsage.first();
                regionCapacity = lastRegionUsage.second();
                log.info("lastReginFiveMinutesTime:" + lastReginFiveMinutesTime);
                for (Entry<String, SizeStats> entry : regionCapacity.entrySet()) {
                    log.info("regionCapacity:" + entry.getKey() + ".size:" + entry.getValue().size_total);
                }
                
                boolean startUp = true;
                for (;;) {
                    long nowTime = System.currentTimeMillis();
                    // 每隔interval毫秒统计一次，只统计usageStatsTimeAgo毫秒以前的
                    if (!startUp && (nowTime - lastStatsTime) < interval + usageStatsTimeAgo) {
                        try {
                            Thread.sleep(300000);
                        } catch (InterruptedException e) {
                        }
                        continue;
                    }
                    //配置文件清除缓存
                    if (PeriodUsageStatsConfig.regionClearCache.contains("global")) {
                        cachedUsageMeta.clear();
                        log.info("clear day meta cache region is : " + "global");
                    }
                    
                    log.info("the regions have to be online is " + PeriodUsageStatsConfig.regionNames);
                    
                    // 遍历表中的UsageMetaType.CURRENT_OWNER类型，统计到UsageMetaType.MINUTES_OWNER类型，每5分钟一条记录。
                    startTime = lastStatsTime;
                    endTime = Math.min((nowTime - usageStatsTimeAgo), (startTime + interval));
                    log.info("start Stats per " + interval + ". from " + startTime + " to "+ endTime);
                    startUp = false;
                    try {                        
                        byte[] nextKey = null;
                        for (;;) {
                            long t1 = System.currentTimeMillis();
                            LinkedHashMap<String, TreeMap<Long, MinutesUsageMeta>> minutesUsages = client
                                    .minutesUsageCurrentAll5Mins(startTime,
                                            endTime, null, -1, null,
                                            UsageMetaType.CURRENT_OWNER,
                                            nextKey, 5000);
                            long t2 = System.currentTimeMillis() - t1;
                            log.info("UsageMetaType=" + UsageMetaType.CURRENT_OWNER + ", minutesUsageCurrentAll5Mins time=" + t2+" minutesUsages.size:"+minutesUsages.size());
                            if (minutesUsages.isEmpty()) {
                                log.info("UsageMetaType=" + UsageMetaType.CURRENT_OWNER + ", statsTime=" + Misc.formatyyyymmddhhmm(new Date(endTime)));
                                break;
                            }
                            long t3 = System.currentTimeMillis();
                            nextKey = doStats(minutesUsages, UsageMetaType.CURRENT_OWNER);
                            long t4 = System.currentTimeMillis() - t3;
                            log.info("UsageMetaType=" + UsageMetaType.CURRENT_OWNER + ", doStats time=" + t4+" minutesUsages.size:"+minutesUsages.size());
                        }
                        // 遍历表中的UsageMetaType.CURRENT_BUCKET类型，统计到UsageMetaType.MINUTES_BUCKET类型，每5分钟一条记录；同时统计到UsageMetaType.DAY_BUCKET，每天一条。
                        nextKey = null;
                        for (;;) {
                            long t1 = System.currentTimeMillis();
                            LinkedHashMap<String, TreeMap<Long, MinutesUsageMeta>> minutesUsages = client
                                    .minutesUsageCurrentAll5Mins(startTime,
                                            endTime, null, -1, null,
                                            UsageMetaType.CURRENT_BUCKET,
                                            nextKey, 5000);
                            long t2 = System.currentTimeMillis() - t1;
                            log.info("UsageMetaType=" + UsageMetaType.CURRENT_BUCKET + ", minutesUsageCurrentAll5Mins time=" + t2+" minutesUsages.size:"+minutesUsages.size());
                            if (minutesUsages.isEmpty()) {
                                log.info("UsageMetaType=" + UsageMetaType.CURRENT_BUCKET + ", statsTime=" + Misc.formatyyyymmddhhmm(new Date(endTime)));
                                break;
                            }
                            long t3 = System.currentTimeMillis();
                            nextKey = doStats(minutesUsages, UsageMetaType.CURRENT_BUCKET);
                            long t4 = System.currentTimeMillis() - t3;
                            log.info("UsageMetaType=" + UsageMetaType.CURRENT_BUCKET + ", doStats time=" + t4+" minutesUsages.size:"+minutesUsages.size());
                        }
                        nextKey = null;
                        for (;;) {
                            long t1 = System.currentTimeMillis();
                            LinkedHashMap<String, TreeMap<Long, MinutesUsageMeta>> minutesUsages = client
                                    .minutesUsageCurrentAll5Mins(startTime,
                                            endTime, null, -1, null,
                                            UsageMetaType.CURRENT_AVAIL_BW,
                                            nextKey, 5000);
                            long t2 = System.currentTimeMillis() - t1;
                            log.info("UsageMetaType=" + UsageMetaType.CURRENT_AVAIL_BW + ", minutesUsageCurrentAll5Mins time=" + t2+" minutesUsages.size:"+minutesUsages.size());
                            if (minutesUsages.isEmpty()) {
                                log.info("UsageMetaType=" + UsageMetaType.CURRENT_AVAIL_BW + ", statsTime=" + Misc.formatyyyymmddhhmm(new Date(endTime)));
                                break;
                            }
                            long t3 = System.currentTimeMillis();
                            nextKey = doAvailableBWStats(minutesUsages, UsageMetaType.CURRENT_AVAIL_BW);
                            long t4 = System.currentTimeMillis() - t3;
                            log.info("UsageMetaType=" + UsageMetaType.CURRENT_AVAIL_BW + ", doStats time=" + t4+" minutesUsages.size:"+minutesUsages.size());
                        }
                        // 插入数据域5分钟的计量                       
                        insertRegionMetas();
                        lastStatsTime = endTime;                        
                        // delete allversions
                        client.minutesUsageBatchDeleteAllVersions(toDeleteList);
                        client.minutesUsageInsertLastPeriodTime(endTime);
                        toDeleteList.clear();
                        log.info("ever five usage stats finished");
                        // 跨天时补全非活跃数据 
                        inactiveStatsFilling.invoke(lastStatsTime);
                    } catch (Throwable t) {
                        log.error(t.getMessage(), t);
                        retryLogic();
                    }
                }
            } finally {
                lock.unlock();
            }
        } finally {
            dsync.close();
        }
    }
    
    /**
     * 每轮失败重试逻辑
     */
    private void retryLogic() {
        //已经执行到更新lasttime，后面删除或者插入lastperiodtime报错则无需重试
        if (lastStatsTime == endTime) {
            return;
        }
        String key = startTime + "," + endTime;
        //超过最大重试次数
        int maxRetryTimes = PeriodUsageStatsConfig.maxRetryTimes;
        if (retryMap.getOrDefault(key, 0) >= maxRetryTimes) {
            lastStatsTime = endTime;
            log.info("stats from starttime is : " + startTime +", endtime is : " + endTime + " has been retried more than maxRetryTimes : " + maxRetryTimes);
            return;
        }
        
        if(retryMap.size() > 1000) {
            retryMap.forEach((k , v) -> log.info("has retried timerange : " + k + ", times : " + v));
            retryMap.clear();
        }
        //更新这个key的重试次数
        retryMap.merge(key, 1, Integer::sum);
        //开始重试
        log.info("start retry Stats from starttime is : " + startTime +", endtime is : " + endTime);
        //清空上一轮失败的时候加入region统计的缓存
        regionIcreCapacity.clear();
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
        }
    }
    
    /**
     * 插入region统计数据
     * @throws Exception
     */
    private void insertRegionMetas() throws Exception {
        List<MinutesUsageMeta> regionMetasToPut = new ArrayList<MinutesUsageMeta>();
        log.info("insert Region Metas. regionIncreCapacity size is " + regionIcreCapacity.size());
        List<MinutesUsageMeta> regionSizeInsert  = new ArrayList<MinutesUsageMeta>();
        for (Entry<String, Map<Long, MinutesUsageMeta>> entry : regionIcreCapacity.entrySet()) {
            String key = entry.getKey();
            String storageType = key.split("\\|")[1];
            String region = key.split("\\|")[0];
            
            SizeStats regionSize = regionCapacity.get(key);
            //day_region 和 five_minutes 计算
            String time = null;
            MinutesUsageMeta dayRegion = new MinutesUsageMeta(UsageMetaType.REGION_DAY, region, time, storageType);
            dayRegion.initStatsExceptConnection();
            for (Entry<Long, MinutesUsageMeta> timeEntry : entry.getValue().entrySet()) {
                MinutesUsageMeta increMeta = timeEntry.getValue();
                if (regionSize != null) {
                    increMeta.sizeStats.sumAll(regionSize);
                }
                if(time == null) { //第一个五分钟region，判断一下之前是否存在它所在day的region数据
                    time = increMeta.time.substring(0, 10);
                    dayRegion.time = time;
                    client.minutesUsageSelect(dayRegion);
                } else if (increMeta.time.substring(0, 10).compareTo(time) > 0) { 
                    //如果这五分钟的日期大于上一个日期，则开启新的一个dayregion，并将上一个insert
                    regionMetasToPut.add(dayRegion);
                    time = increMeta.time.substring(0, 10);
                    dayRegion = new MinutesUsageMeta(UsageMetaType.REGION_DAY, region, -1, time, storageType);
                    dayRegion.initStatsExceptConnection();
                }
                //合并到dayRegion的统计
                mergeToDayUsageMeta(increMeta, dayRegion);    
                dayRegion.time = time;
                regionSize = increMeta.sizeStats;
                regionCapacity.put(key, regionSize);
                regionMetasToPut.add(increMeta);
            }
            regionSizeInsert.add(dayRegion);
            regionMetasToPut.add(dayRegion); //将最后一个dayregion插入数据库
        }
        client.minutesUsageBatchInsert(regionMetasToPut);
        client.minutesUsageBatchInsertRegionSize(regionSizeInsert);
        regionIcreCapacity.clear();
    }
    
    /**
     * 多线程计算current数据
     * @param minutesUsages
     * @param type
     * @return
     * @throws Exception
     */
    private byte[] doStats(LinkedHashMap<String, TreeMap<Long, MinutesUsageMeta>> minutesUsages, UsageMetaType type) throws Exception {
        LinkedHashMap<String, TreeMap<Long, MinutesUsageMeta>> ownerMinutesUsages = new LinkedHashMap<String, TreeMap<Long, MinutesUsageMeta>>();
        String lastKeyExcpetDay = null;
        String lastKey = null;
        List<Future<Boolean>> futures = new LinkedList<>();
        for (Entry<String, TreeMap<Long, MinutesUsageMeta>> entry : minutesUsages.entrySet()) {
            lastKey = entry.getKey();
            //根据CURRENT_OWNER|userId或者CURRENT_BUCKET|userId 对所有统计数据进行分开计算，优化效率，除去日期是因为统计数据需要用到上一天的
            String keyExcpetDay = entry.getKey().substring(0, entry.getKey().lastIndexOf("|"));
            //下一个用户的数据
            if (lastKeyExcpetDay != null && !keyExcpetDay.equals(lastKeyExcpetDay)) {
                log.info("------lastKeyExcpetDay------lastKeyExcpetDay:" + lastKeyExcpetDay + ". minutesUsages size is:" + ownerMinutesUsages.size());
                futures.add(es.submit(new DoStatsTask(ownerMinutesUsages, type)));
                ownerMinutesUsages = new LinkedHashMap<String, TreeMap<Long, MinutesUsageMeta>>();
            }
            lastKeyExcpetDay = keyExcpetDay;
            ownerMinutesUsages.put(entry.getKey(), entry.getValue());
        }
        if (ownerMinutesUsages.size() > 0) {
            futures.add(es.submit(new DoStatsTask(ownerMinutesUsages, type)));
        }
        for(Future<Boolean> f : futures) {
            try {
                f.get();
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }
        log.info("lastKey:" + lastKey + " type:" + type.toString());
        return Bytes.toBytes(lastKey + Character.MIN_VALUE);
    }
    
    
    //统计计算线程
    class DoStatsTask implements Callable<Boolean> {
        
        private LinkedHashMap<String, TreeMap<Long, MinutesUsageMeta>> minutesUsages;
        private UsageMetaType type;

        public DoStatsTask(LinkedHashMap<String, TreeMap<Long, MinutesUsageMeta>> minutesUsages, UsageMetaType type) {
            super();
            this.minutesUsages = minutesUsages;
            this.type = type;
        }

        @Override
        public Boolean call() throws Exception {
            for (Entry<String, TreeMap<Long, MinutesUsageMeta>> entry : minutesUsages.entrySet()) {
                long keyStartStatsTime = System.currentTimeMillis();
                // 行key，格式为CURRENT_XXX|regionName|userId|(bucketName|)day
                //低频和标准混合 CURRENT_XXX|regionName|userId|(bucketName|)(storageType|)day
                String k = entry.getKey();
                // 某个五分钟的MinutesUsageMeta，按时间从小到大排序
                TreeMap<Long, MinutesUsageMeta> v = entry.getValue();
                // 已统计的最新的UsageMetaType.DAY_OWNER、UsageMetaType.DAY_BUCKET统计
                MinutesUsageMeta dayMeta = null;
                // 按小时的统计，一次统计可能包含几个小时的数据
                LinkedList<MinutesUsageMeta> hourMetaList = new LinkedList<MinutesUsageMeta>();
                // 向数据库插入的统计数据，以及要删除的临时增量数据
                List<MinutesUsageMeta> toFlush = new LinkedList<>();
                Pair<byte[], Long> toDelete = new Pair<byte[], Long>(Bytes.toBytes(k), -1l);
                for (Entry<Long, MinutesUsageMeta> usageKV : v.entrySet()) {
                    long fiveMinutesInMills = usageKV.getKey();
                    boolean isZero = fiveMinutesInMills % 86400000 == 57600000;
                    MinutesUsageMeta currentMeta = usageKV.getValue();                    
                    if (PeriodUsageStatsConfig.regionNames.contains(currentMeta.regionName)) {
                        break;
                    }
                    toDelete.second(currentMeta.timeVersion);
                    
                    Date currentMetaDate = Misc.formatyyyymmdd(currentMeta.time);
                    Date currentTVDate = new Date(fiveMinutesInMills);
                    if (currentTVDate.before(currentMetaDate)) {
                        log.info("currentTVDate is before currentMetaDate.");
                    }
                    String dateHourTime = Misc.formatyyyymmddhhmm(new Date(fiveMinutesInMills));
                    String dateTime = Misc.formatyyyymmdd(currentTVDate);
                    if (isZero) {
                        dateTime = LocalDate.parse(dateHourTime.substring(0, 10), Misc.format_uuuu_MM_dd).minusDays(1).toString();
                        dateHourTime = dateTime + " 24:00";
                    }
                    // 如果是global数据域的，此版本不做计算，只删除，等下个版本升级时开始计算
                    if (currentMeta.regionName.equals(Consts.GLOBAL_DATA_REGION))
                        break;


                    UsageMetaType dayType;
                    // 5分钟的用户统计
                    // 构造UsageMetaType.MINUTES_xxx类型的消息，timeVersion为5分钟mills
                    MinutesUsageMeta minutesMeta;
                    if (type == UsageMetaType.CURRENT_OWNER) {
                        dayType = UsageMetaType.DAY_OWNER;
                        minutesMeta = new MinutesUsageMeta(UsageMetaType.MINUTES_OWNER, currentMeta.regionName, currentMeta.getUsrId(), dateHourTime, currentMeta.storageType);
                    } else {
                        dayType = UsageMetaType.DAY_BUCKET;
                        minutesMeta = new MinutesUsageMeta(UsageMetaType.MINUTES_BUCKET, currentMeta.regionName, currentMeta.getUsrId(), currentMeta.bucketName, dateHourTime, currentMeta.storageType);
                    }
                    
                    //将旧api插入的连接数加到新的数据结构中
                    if (client.minutesUsageSelectConnections(minutesMeta)) {
                        if (currentMeta.connection == null) {
                            currentMeta.connection = new Connection();
                        }
                        currentMeta.connection.connections += minutesMeta.connection.connections;
                        currentMeta.connection.noNet_connections += minutesMeta.connection.noNet_connections;
                    }
                    //后面用minutes类型数据计算day类型数据的时候需要dateTime给day类型数据的日期赋值
                    minutesMeta.time = dateTime;
                    
                    if (fiveMinutesInMills > lastReginFiveMinutesTime) {
                        // 不多计算region统计值
                        // 获取5分钟级别的Region统计值， 将5分钟[增量]信息记录叠加
                        if (type.equals(UsageMetaType.CURRENT_OWNER)) {
                            synchronized (regionIcreCapacity) {
                                String key = currentMeta.regionName + "|" + currentMeta.storageType;
                                Map<Long, MinutesUsageMeta> regionMap = regionIcreCapacity.get(key);
                                if (regionMap != null) {
                                    MinutesUsageMeta meta = regionMap.get(fiveMinutesInMills);
                                    if (meta == null) {
                                        meta = new MinutesUsageMeta(UsageMetaType.FIVE_MINUTES, currentMeta.regionName, dateHourTime, currentMeta.storageType);
                                        meta.initCommonStats();
                                        meta.timeVersion = fiveMinutesInMills;
                                        meta.fiveMinutesInMills = currentMeta.fiveMinutesInMills;
                                    }
                                    meta.sumAll(currentMeta);
                                    regionMap.put(fiveMinutesInMills, meta);
                                } else {
                                    regionMap = new TreeMap<Long, MinutesUsageMeta>();
                                    MinutesUsageMeta meta = new MinutesUsageMeta(UsageMetaType.FIVE_MINUTES, currentMeta.regionName, dateHourTime, currentMeta.storageType);
                                    meta.initCommonStats();
                                    meta.timeVersion = fiveMinutesInMills;
                                    meta.fiveMinutesInMills = currentMeta.fiveMinutesInMills;
                                    meta.sumAll(currentMeta);
                                    regionMap.put(fiveMinutesInMills, meta);
                                    regionIcreCapacity.put(currentMeta.regionName + "|" + currentMeta.storageType, regionMap);
                                }
                            }                            
                        }
                    }

                    minutesMeta.timeVersion = fiveMinutesInMills;
                    minutesMeta.fiveMinutesInMills = currentMeta.fiveMinutesInMills;
                    minutesMeta.copyCommonStats(currentMeta);

                    //如果跨天，先把dayMeta更新数据库
                    if (dayMeta != null && dateTime.compareTo(dayMeta.time) > 0) {
                        client.minutesUsageInsert(dayMeta);
                        dayMeta = null;
                    }
                    // 获取上一个按天的统计数据
                    
                    if (dayMeta == null) {
                        if (dayType == UsageMetaType.DAY_OWNER) {
                            dayMeta = new MinutesUsageMeta(dayType, currentMeta.regionName, currentMeta.getUsrId(), dateTime, currentMeta.storageType);
                        } else if (dayType == UsageMetaType.DAY_BUCKET) {
                            dayMeta = new MinutesUsageMeta(dayType, currentMeta.regionName, currentMeta.getUsrId(), currentMeta.bucketName, dateTime, currentMeta.storageType);
                        }
                        long getLastDayMeta = System.currentTimeMillis();
                        if (!client.minutesUsageSelect(dayMeta)) {
                            dayMeta = getLastDayUsageMeta(dayMeta);
                        }
                        long getLastDayMeta2 = System.currentTimeMillis();
                        log.info("get last day usage meta spend time:" + (getLastDayMeta2 - getLastDayMeta));
                        
                    }
                   
                    if (minutesMeta.timeVersion < dayMeta.timeVersion || (!currentTVDate.before(currentMetaDate) && minutesMeta.timeVersion == dayMeta.timeVersion)) {
                        // 当前增量已经计算并持久化过按天统计
                        log.error("For Stats Debug. meta time <= lastMeta time." + " meta.time:" + minutesMeta.time + ". meta timeversion:" + minutesMeta.timeVersion + ". meta.key:" + minutesMeta.getKey() 
                        + ". lastMeta.time:" + dayMeta.time + ". lastMeta timeversion:" + dayMeta.timeVersion + ". lastMeta.key:" + dayMeta.getKey());
                        continue;
                    }
                   
                    // 计算得到5分钟的统计信息
                    computeMinutesMeta(minutesMeta, dayMeta);
                    
                    // 将5分钟[统计]信息增加到当天的统计信息
                    mergeToDayUsageMeta(minutesMeta, dayMeta);                    
                    
                    // 将5分钟[统计]信息叠加到1小时的统计中
                    // 判断list中是否已经有该小时的meta，如果有，则取出该meta在其上叠加，再插入回list；如果没有，则查找数据库中是否有该小时的meta，没有则新建一个该小时的meta
                    MinutesUsageMeta hourMeta = hourMetaList.peek();
                    // 如果当前meta的timeversion小于time，该数据叠加到time零点到一点的数据上
                    long minutesMills = minutesMeta.fiveMinutesInMills;
                    if (currentTVDate.before(currentMetaDate)) {
                        minutesMills = currentMetaDate.getTime();
                    }
                    long ceilHourStartTime = Misc.getCeilHourStartTime(minutesMills);
                    long floorHourStartTime = Misc.getfloorHourStartTime(minutesMills);
                    if (hourMeta != null && hourMeta.fiveMinutesInMills > floorHourStartTime) {
                        hourMeta = hourMetaList.pop();
                    } else {
                        String hourTime = Misc.formatyyyymmddhhmm(new Date(ceilHourStartTime));
                        if (hourTime.contains("00:00")) {
                            hourTime = LocalDate.parse(hourTime.substring(0, 10), Misc.format_uuuu_MM_dd).minusDays(1).toString() + " 24:00";
                        }
                        if (dayType == UsageMetaType.DAY_OWNER) {
                            hourMeta = new MinutesUsageMeta(UsageMetaType.HOUR_OWNER, currentMeta.regionName, currentMeta.getUsrId(), hourTime, currentMeta.storageType);
                        } else if (dayType == UsageMetaType.DAY_BUCKET) {
                            hourMeta = new MinutesUsageMeta(UsageMetaType.HOUR_BUCKET, currentMeta.regionName, currentMeta.getUsrId(), currentMeta.bucketName, hourTime, currentMeta.storageType);
                        }
                        if (!client.minutesUsageSelect(hourMeta)) {
                            hourMeta.timeVersion = ceilHourStartTime;
                            hourMeta.initCommonStats();
                            hourMeta.sizeStats.sumExceptPeakAndAvgAndRestore(dayMeta.sizeStats);
                            hourMeta.sizeStats.size_peak = hourMeta.sizeStats.size_total;
                            hourMeta.sizeStats.size_completePeak = hourMeta.sizeStats.size_completeSize;
                            hourMeta.sizeStats.size_originalPeak = hourMeta.sizeStats.size_originalTotal;
                            hourMeta.sizeStats.size_avgTotal = hourMeta.sizeStats.size_total;
                            hourMeta.sizeStats.size_avgAlin = hourMeta.sizeStats.size_alin;
                            hourMeta.sizeStats.size_avgRedundant = hourMeta.sizeStats.size_redundant;
                            hourMeta.sizeStats.size_avgOriginalTotal = hourMeta.sizeStats.size_originalTotal;
                        }
                        hourMeta.fiveMinutesInMills = Misc.getfloorHourStartTime(hourMeta.timeVersion);
                    }
                    mergeToHourUsageMeta(minutesMeta, hourMeta);
                    hourMetaList.push(hourMeta);

                    // 如果当前meta的timeversion小于time，叠加到按天统计，不记录该5分钟meta，叠加到下一个5分钟meta
                    if (currentTVDate.before(currentMetaDate)) {
                        continue;
                    }
                    minutesMeta.time = dateHourTime;
                    toFlush.add(minutesMeta);
                }

                long flushTime1 = System.currentTimeMillis();
                toFlush.add(dayMeta);
                toFlush.addAll(hourMetaList);
                // 插入5分钟、按小时、按天的统计
                client.minutesUsageBatchInsert(toFlush);
                // 重置时间戳后6位随机数为最大
                Long timeVersion = toDelete.second();
                if (timeVersion != -1) {
                    long ts = MinutesUsageMeta.buildTimeVersion(MinutesUsageMeta.parseTimeVersion(timeVersion)) + 999999;
                    toDelete.second(ts);
                    toDeleteList.add(toDelete);
                }                
                long flushTime2 = System.currentTimeMillis();
                log.info(k + "doStats time:" + (flushTime2 - keyStartStatsTime) + ".flush spend time:" + (flushTime2 - flushTime1));
            }
            return true;
        }
    }
    
    /**
     * 可用带宽统计
     * @param minutesUsages
     * @param type
     * @return
     * @throws Exception
     */
    private byte[] doAvailableBWStats(LinkedHashMap<String, TreeMap<Long, MinutesUsageMeta>> minutesUsages, UsageMetaType type) throws Exception {
        String lastKey = null;
        for(Entry<String, TreeMap<Long, MinutesUsageMeta>> entry : minutesUsages.entrySet()) {
            //行key，格式为CURRENT_AVAIL_BW|regionName|day
            String k = entry.getKey();
            lastKey = k;
            //某个五分钟的MinutesUsageMeta，按时间从小到大排序
            TreeMap<Long, MinutesUsageMeta> v = entry.getValue();
            //已统计的最新的UsageMetaType.DAY_OWNER、UsageMetaType.DAY_BUCKET统计
            List<MinutesUsageMeta> toFlush = new LinkedList<>();
            Pair<byte[], Long> toDelete = new Pair<byte[], Long>(Bytes.toBytes(k), -1l);
            for(Entry<Long, MinutesUsageMeta> usageKV : v.entrySet()) {
                MinutesUsageMeta meta = usageKV.getValue();
                if (PeriodUsageStatsConfig.regionNames.contains(meta.regionName)) {
                    break;
                }
                if (PeriodUsageStatsConfig.getRegionBandwidth(meta.regionName) != null) {
                    meta.type = UsageMetaType.MINUTES_AVAIL_BW;
                    //计算可用带宽
                    meta.availableBandwidth.priInBW = PeriodUsageStatsConfig.getRegionBandwidth(meta.regionName).priInBandwidth - meta.availableBandwidth.priInBW / (FIVE_MINUTES / 1000);
                    meta.availableBandwidth.priOutBW = PeriodUsageStatsConfig.getRegionBandwidth(meta.regionName).priOutBandwidth - meta.availableBandwidth.priOutBW / (FIVE_MINUTES / 1000);
                    meta.availableBandwidth.pubInBW = PeriodUsageStatsConfig.getRegionBandwidth(meta.regionName).pubInBandwidth - meta.availableBandwidth.pubInBW / (FIVE_MINUTES / 1000);
                    meta.availableBandwidth.pubOutBW = PeriodUsageStatsConfig.getRegionBandwidth(meta.regionName).pubOutBandwidth - meta.availableBandwidth.pubOutBW / (FIVE_MINUTES / 1000);
                    toDelete.second(meta.timeVersion);
                    long fiveMinutesInMills = usageKV.getKey();
                    meta.timeVersion = fiveMinutesInMills ;
                    meta.time = Misc.formatyyyymmddhhmm(new Date(fiveMinutesInMills));
                    toFlush.add(meta);
                } else {
                    log.error("getRegionBandwidth is null. Please check " + meta.regionName + " bandwidth config at periodUsageStatsConfig.txt.");
                    continue;
                }
            }
            //插入5分钟的统计，并清理统计过的version
            client.minutesUsageBatchInsert(toFlush);
            // 重置时间戳后6位随机数为最大
            Long timeVersion = toDelete.second();
            if (timeVersion != -1) {
                long ts = MinutesUsageMeta.buildTimeVersion(MinutesUsageMeta.parseTimeVersion(timeVersion)) + 999999;
                toDelete.second(ts);
                client.minutesUsageDeleteAllVersions(toDelete.first(), toDelete.second());
            }            
        }
        return Bytes.toBytes(lastKey + Character.MIN_VALUE);
    }

    /**
     * 计算5分钟的统计数据，容量需要叠加上按天统计的值，流量、请求次数就是5分钟内的增量值
     */
    private void computeMinutesMeta(MinutesUsageMeta minutesMeta, MinutesUsageMeta dayMeta) {
        //计算5分钟的容量值
        //计算totalSize = lastFiveMinutesMeta.totalSize + thisFiveMinutesMeta.totalSize.
        //计算peakSize = lastFiveMinutesMeta.totalSize + thisFiveMinutesMeta.peakSize.
        minutesMeta.sizeStats.sumExceptPeakAndAvgAndRestore(dayMeta.sizeStats);
        minutesMeta.sizeStats.size_peak = dayMeta.sizeStats.size_total + minutesMeta.sizeStats.size_peak;
        minutesMeta.sizeStats.size_originalPeak = dayMeta.sizeStats.size_originalTotal + minutesMeta.sizeStats.size_originalPeak;
        minutesMeta.sizeStats.size_completePeak = dayMeta.sizeStats.size_completeSize + minutesMeta.sizeStats.size_completePeak;
        //修正容量为负数的异常情况（上传对象容量pin没有往数据库写入）
        minutesMeta.sizeStats.repairMinusSize();
    }
    
  
    /**
     * 将5分钟的统计信息加到一小时统计中
     * @param minutesMeta
     * @param dayMeta
     */
    private void mergeToHourUsageMeta(MinutesUsageMeta minutesMeta, MinutesUsageMeta hourMeta) {
        //当前统计的五分钟
        long fiveMinutesInMills = minutesMeta.timeVersion;
        //上一次统计的时间
        long lastStatsTime = hourMeta.fiveMinutesInMills;
        //lastMeta信息
        long lastTotalSize = hourMeta.sizeStats.size_total;
        long lastRedundantSize = hourMeta.sizeStats.size_redundant;
        long lastAlinSize = hourMeta.sizeStats.size_alin;
        long lastOriginalTotalSize = hourMeta.sizeStats.size_originalTotal;
        long lastAvgTotalSize = hourMeta.sizeStats.size_avgTotal;
        long lastAvgAlinSize = hourMeta.sizeStats.size_avgAlin;
        long lastAvgOriginalTotalSize = hourMeta.sizeStats.size_avgOriginalTotal;
        long lastAvgRedundantSize = hourMeta.sizeStats.size_avgRedundant;

        mergeUsageMeta(minutesMeta, hourMeta);
        // 更新时间信息
        if (minutesMeta.fiveMinutesInMills >= hourMeta.fiveMinutesInMills) {
            hourMeta.fiveMinutesInMills = minutesMeta.fiveMinutesInMills;
        } else {
            log.error("For periodUsageStats Debug. meta fiveMinutesInMills < hour lastMeta fiveMinutesInMills."
                    + " minutesMeta.fiveMinutesInMills:" + minutesMeta.fiveMinutesInMills + " hourMeta.fiveMinutesInMills:"
                    + hourMeta.fiveMinutesInMills + " minutesMeta rowkey:" + minutesMeta.getKey() + " hourMeta rowkey:"
                    + hourMeta.getKey());
        }
        int totalFiveMinutesNum = (int) ((fiveMinutesInMills - Misc.getfloorHourStartTime(fiveMinutesInMills)) / FIVE_MINUTES);
        int intervalFiveMinutesNum = (int) ((fiveMinutesInMills - lastStatsTime) / FIVE_MINUTES) - 1;
        int lastStatsFiveMinutesNum = (int) ((lastStatsTime - Misc.getfloorHourStartTime(fiveMinutesInMills)) / FIVE_MINUTES);

        hourMeta.sizeStats.size_avgTotal = computeAvgSize(lastAvgTotalSize, lastStatsFiveMinutesNum, lastTotalSize,
                intervalFiveMinutesNum, minutesMeta.sizeStats.size_total, totalFiveMinutesNum);
        hourMeta.sizeStats.size_avgAlin = computeAvgSize(lastAvgAlinSize, lastStatsFiveMinutesNum, lastAlinSize,
                intervalFiveMinutesNum, minutesMeta.sizeStats.size_alin, totalFiveMinutesNum);
        hourMeta.sizeStats.size_avgOriginalTotal = computeAvgSize(lastAvgOriginalTotalSize, lastStatsFiveMinutesNum, lastOriginalTotalSize, 
                intervalFiveMinutesNum, minutesMeta.sizeStats.size_originalTotal, totalFiveMinutesNum);
        hourMeta.sizeStats.size_avgRedundant = computeAvgSize(lastAvgRedundantSize, lastStatsFiveMinutesNum, lastRedundantSize, 
                intervalFiveMinutesNum, minutesMeta.sizeStats.size_redundant, totalFiveMinutesNum);
    }
    
    /**
     * 将5分钟的[统计]信息加到当天的统计中
     * @param minutesMeta 5分钟的统计
     * @param dayMeta 按天统计
     */
    private void mergeToDayUsageMeta(MinutesUsageMeta minutesMeta, MinutesUsageMeta dayMeta) {
        dayMeta.time = minutesMeta.time;
        long fiveMinutesInMills = minutesMeta.timeVersion;
        //lastMeta信息
        long lastStatsTime = dayMeta.fiveMinutesInMills;
        long lastTotalSize = dayMeta.sizeStats.size_total;
        long lastRedundantSize = dayMeta.sizeStats.size_redundant;
        long lastAlinSize = dayMeta.sizeStats.size_alin;
        long lastOriginalTotalSize = dayMeta.sizeStats.size_originalTotal;
        long lastAvgTotalSize = dayMeta.sizeStats.size_avgTotal;
        long lastAvgAlinSize = dayMeta.sizeStats.size_avgAlin;
        long lastAvgOriginalTotalSize = dayMeta.sizeStats.size_avgOriginalTotal;
        long lastAvgRedundantSize = dayMeta.sizeStats.size_avgRedundant;
        
        mergeUsageMeta(minutesMeta, dayMeta);
        //更新时间信息
        if (minutesMeta.fiveMinutesInMills >= dayMeta.fiveMinutesInMills) {
            dayMeta.fiveMinutesInMills = minutesMeta.fiveMinutesInMills;
            dayMeta.timeVersion = minutesMeta.fiveMinutesInMills;
        } else {
            log.error("For periodUsageStats Debug. meta fiveMinutesInMills < day lastMeta fiveMinutesInMills." 
                        + " minutesMeta.fiveMinutesInMills:" + minutesMeta.fiveMinutesInMills + " dayMeta.fiveMinutesInMills:" + dayMeta.fiveMinutesInMills 
                        +" minutesMeta rowkey:" + minutesMeta.getKey() + " dayMeta rowkey:" + dayMeta.getKey());
        }
        
        // 更新当天的容量平均值，零点时刻的平均容量是前一天的平均容量，峰值容量是
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(new Date(fiveMinutesInMills - 1));
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        //当天00:00:00时刻的millisTime
        long currentDayStartMillis = calendar.getTimeInMillis();
        
        int totalFiveMinutesNum = (int) ((fiveMinutesInMills - currentDayStartMillis) / FIVE_MINUTES);
        int intervalFiveMinutesNum = 0;
        int lastStatsFiveMinutesNum = 0;
        if(lastStatsTime > currentDayStartMillis) { //当天
            intervalFiveMinutesNum = (int) ((fiveMinutesInMills - lastStatsTime) / FIVE_MINUTES - 1);
            lastStatsFiveMinutesNum = (int) ((lastStatsTime - currentDayStartMillis) / FIVE_MINUTES);
        } else { //跨天
            intervalFiveMinutesNum = totalFiveMinutesNum - 1;
            lastStatsFiveMinutesNum = 0;
            //重置peakSize为totalSize
            dayMeta.sizeStats.size_peak = dayMeta.sizeStats.size_total;
            dayMeta.sizeStats.size_originalPeak = dayMeta.sizeStats.size_originalTotal;
            dayMeta.sizeStats.size_completePeak = dayMeta.sizeStats.size_completeSize;
        }
        // 计算公式 (lastAvgTotalSize * lastStatsFiveMinutesNum + lastTotalSize * intervalFiveMinutesNum + meta.commonStats.totalSize) / totalFiveMinutesNum;
        dayMeta.sizeStats.size_avgTotal = computeAvgSize(lastAvgTotalSize, lastStatsFiveMinutesNum, lastTotalSize, 
                intervalFiveMinutesNum, minutesMeta.sizeStats.size_total, totalFiveMinutesNum);
        dayMeta.sizeStats.size_avgAlin = computeAvgSize(lastAvgAlinSize, lastStatsFiveMinutesNum, lastAlinSize,
                intervalFiveMinutesNum, minutesMeta.sizeStats.size_alin, totalFiveMinutesNum);
        dayMeta.sizeStats.size_avgOriginalTotal = computeAvgSize(lastAvgOriginalTotalSize, lastStatsFiveMinutesNum, lastOriginalTotalSize, 
                intervalFiveMinutesNum, minutesMeta.sizeStats.size_originalTotal, totalFiveMinutesNum);
        dayMeta.sizeStats.size_avgRedundant = computeAvgSize(lastAvgRedundantSize, lastStatsFiveMinutesNum, lastRedundantSize, 
                intervalFiveMinutesNum, minutesMeta.sizeStats.size_redundant, totalFiveMinutesNum);
    }
    
    private void mergeUsageMeta(MinutesUsageMeta meta, MinutesUsageMeta lastMeta) {
        //容量已经计算过
        lastMeta.sizeStats.initExceptPeakAndAvg(meta.sizeStats);
        
        //计算峰值容量，如果5分钟的峰值大于之前的按天统计的峰值，则按天峰值等于5分钟峰值
        lastMeta.sizeStats.size_peak = meta.sizeStats.size_peak > lastMeta.sizeStats.size_peak
                ? meta.sizeStats.size_peak : lastMeta.sizeStats.size_peak;
        lastMeta.sizeStats.size_originalPeak = meta.sizeStats.size_originalPeak > lastMeta.sizeStats.size_originalPeak
                ? meta.sizeStats.size_originalPeak : lastMeta.sizeStats.size_originalPeak;
        lastMeta.sizeStats.size_completePeak = meta.sizeStats.size_completePeak > lastMeta.sizeStats.size_completePeak
                ? meta.sizeStats.size_completePeak : lastMeta.sizeStats.size_completePeak;
        //流量、请求次数、按返回码区分的请求次数直接叠加
        lastMeta.flowStats.sumAll(meta.flowStats);
        lastMeta.requestStats.sumAll(meta.requestStats);
        lastMeta.codeRequestStats.sumAll(meta.codeRequestStats);
    }
    
    /**
     * 计算平均容量
     * 计算公式 (lastAvgTotalSize * lastStatsFiveMinutesNum + lastTotalSize * intervalFiveMinutesNum + meta.commonStats.totalSize) / totalFiveMinutesNum;
     */
    private long computeAvgSize(long lastAvgTotalSize, long lastStatsFiveMinutesNum, long lastTotalSize, long intervalFiveMinutesNum, long currentTotalSize, long totalFiveMinutesNum) {
        BigDecimal totalSize = new BigDecimal(0);
        totalSize = totalSize.add((new BigDecimal(lastAvgTotalSize)).multiply(new BigDecimal(lastStatsFiveMinutesNum)))
        .add((new BigDecimal(lastTotalSize)).multiply(new BigDecimal(intervalFiveMinutesNum))).add(new BigDecimal(currentTotalSize));
        return totalSize.divide(new BigDecimal(totalFiveMinutesNum), RoundingMode.DOWN).longValue();
    }
    
    /**
     * 获取上一个按天统计的meta
     * @param meta
     * @return
     * @throws Exception
     */
    private MinutesUsageMeta getLastDayUsageMeta(MinutesUsageMeta meta) throws Exception {
        String lastKey = meta.getKey();
        MinutesUsageMeta lastMeta = cachedUsageMeta.get(lastKey);
        if(lastMeta != null) {
            return lastMeta;
        }        
        // 查询DAY_OWNER/DAY_BUCKET的值，查询从上一次补全日期开始的日期,如果没有则认为没有天的统计值（或统计数据为0不记录在数据库中），返回一个初始化后的meta
        boolean isExists = false;
        boolean isToday = false;
                
        lastMeta = UsageResult.lastMinutesUsageCommonStats(meta.regionName, meta.getUsrId(), meta.bucketName, meta.type, meta.time, meta.storageType);
        if (lastMeta != null) {            
            meta.copyCommonStatsExpectCon(lastMeta);
            if (lastMeta.time.startsWith(meta.time))
                isToday = true;
            meta.time = lastMeta.time;
            meta.timeVersion = lastMeta.timeVersion;
            isExists = true;
        }
        
        if (isExists) {
            // DAY_XXX的timeVersion不带6位随机数
            meta.fiveMinutesInMills = meta.timeVersion;
        } else {
            //如果不存在按天统计，则初始化一个按天统计的meta
            meta.initStatsExceptConnection();
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(Misc.formatyyyymmdd(meta.time));
            meta.fiveMinutesInMills = calendar.getTimeInMillis();
        }
        //如果按天统计不是当天的，需要清除容量之外的其他统计值
        if(!isToday) {
            meta.clearCommonStatsExceptSize();
        }
        cachedUsageMeta.put(lastKey, meta);
        return meta;
    }

    /**
     * 查询上一次补全非活跃的时间，如某资源池从上一次补全非活跃的时间到当天没有数据，则认为是新建资源池 获取最新的region级别容量信息
     * 
     * @return
     * @throws Exception
     */
    public Pair<Long, Map<String, SizeStats>> getLatestFiveMinutesRegionUsage() throws Exception {
        String lastFillToDate = LocalDate.now().minusDays(7).format(Misc.format_uuuu_MM_dd);

        Map<String, SizeStats> regionLatestSize = new HashMap<String, MinutesUsageMeta.SizeStats>();
        // 最新的数据域统计时间
        long latestTime = 0;
        for (String storageType : Utils.STORAGE_TYPES) {
            long temp = bulidRegionLastSizeMap(regionLatestSize, storageType, lastFillToDate);
            latestTime = Math.max(latestTime, temp);
        }
        return new Pair<Long, Map<String, SizeStats>>(latestTime, regionLatestSize);
    }
    
    private long bulidRegionLastSizeMap(Map<String, SizeStats> regionLatestSize, String storageType, String lastFillToDate) throws Exception {
        LocalDate today = LocalDate.now();
        // 最新的数据域统计时间
        long latestTime = 0;
        List<String> rowkeyList = new ArrayList<String>();
        List<String> regions = DataRegions.getAllRegions();
        for (String region : regions) {
            rowkeyList.add(region + "|" + storageType);
        }
        Map<String, MinutesUsageMeta> regionSizeMap = client.minutesUsageBatchSelectRegionSize(rowkeyList);
        Map<String, List<MinutesUsageMeta>> regionUsages = UsageResult.getTotalUsersUsageStatsQuery(lastFillToDate, today.format(Misc.format_uuuu_MM_dd), Utils.BY_DAY, storageType).first();
        
        Map<String, MinutesUsageMeta> tempMap = new LinkedHashMap<String, MinutesUsageMeta>();        
        for (Entry<String, List<MinutesUsageMeta>> entry : regionUsages.entrySet()) {
            String key = entry.getKey() + "|" + storageType;            
            int last = entry.getValue().size() - 1;
            MinutesUsageMeta meta = entry.getValue().get(last);
            //可能存在只包含连接数的five_minutes数据，sizeStats为null
            while (meta.sizeStats == null) {
                last--;
                meta = entry.getValue().get(last);
            }
            tempMap.put(key, entry.getValue().get(last));
        }        
        for (String key : rowkeyList) {
            if (regionSizeMap.containsKey(key)) {
                MinutesUsageMeta meta = regionSizeMap.get(key);
                if (tempMap.containsKey(key) && tempMap.get(key).timeVersion > meta.timeVersion) {
                    regionLatestSize.put(key, tempMap.get(key).sizeStats);
                    latestTime = Math.max(latestTime, tempMap.get(key).timeVersion);
                } else {
                    regionLatestSize.put(key, meta.sizeStats);
                    latestTime = Math.max(latestTime, meta.timeVersion);
                }            
            } else if(tempMap.containsKey(key)) {
                regionLatestSize.put(key, tempMap.get(key).sizeStats);
                latestTime = Math.max(latestTime, tempMap.get(key).timeVersion);
            }
        } 

        return latestTime;
    } 

    public static void main(String[] args) throws Exception {
        new PeriodUsageStats().exec(args);
    }

    public String usage() {
        return null;
    }
}
