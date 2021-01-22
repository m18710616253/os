package cn.ctyun.oos.server.usage;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.ParseException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;

import cn.ctyun.common.Consts;
import cn.ctyun.oos.common.Utils;
import cn.ctyun.oos.hbase.MetaClient;
import cn.ctyun.oos.metadata.MinutesUsageMeta;
import cn.ctyun.oos.metadata.MinutesUsageMeta.BandwidthMeta;
import cn.ctyun.oos.metadata.MinutesUsageMeta.CodeRequestStats;
import cn.ctyun.oos.metadata.MinutesUsageMeta.Connection;
import cn.ctyun.oos.metadata.MinutesUsageMeta.FlowStats;
import cn.ctyun.oos.metadata.MinutesUsageMeta.RequestStats;
import cn.ctyun.oos.metadata.MinutesUsageMeta.UsageMetaType;
import cn.ctyun.oos.server.conf.PeriodUsageStatsConfig;
import cn.ctyun.oos.server.util.Misc;

public class UsageStatsQuery {
 // 当天累计到当前时刻的账号级别使用量
 // 当天某5分钟时刻的账号级别使用量
 // 当天累计到当前时刻的bucket级别使用量
 // 当天某5分钟时刻的bucket级别使用量
 // 历史某天账号级别使用量
 // 历史某5分钟时刻账号级别使用量
 // 历史某天bucket级别使用量
 // 历史某5分钟时刻bucket级别使用量
 // 账号级别某天某整5分钟时刻已用带宽
 // bucket级别某天某整5分钟时刻已用带宽
 // 当天可用带宽数据
 // 历史可用带宽数据
//    private static Log log = LogFactory.getLog(UsageStatsQuery.class);
    private static MetaClient client = MetaClient.getGlobalClient();
    private static UsageCache<List<MinutesUsageMeta>> usageListCache = new UsageCache<List<MinutesUsageMeta>>(100);
    
    private static final long FIVE_MINUTES = 5 * 60 * 1000;
    private static final int ONE_HOUR = 60 * 60 * 1000;
    private static final int ONE_DAY = 24 * 60 * 60 * 1000;
    
    /**
     * 获取某段时间内某用户在某数据域的带宽95峰值统计数据，time格式为yyyy-MM
     * @param dataRegion
     * @param userId
     * @param bucketName
     * @param startTime
     * @param endTime
     * @return
     * @throws Exception
     */
    public static List<MinutesUsageMeta> list95PeakBW(String dataRegion, long userId, String bucketName, String startTime, String endTime, String storageType) throws Exception{
        String key;
        UsageMetaType type;
        if (bucketName != null) {
            type = UsageMetaType.MONTH_BUCKET;
            key = type + "|" + dataRegion + "|" + userId + "|" + bucketName + "|" + storageType + "|" + startTime + "|" + endTime;
        }
        else {
            type = UsageMetaType.MONTH_OWNER;
            key = type + "|" + dataRegion + "|" + userId + "|" + storageType + "|" + startTime + "|" + endTime;
        }
        List<MinutesUsageMeta> cachedUsageList = usageListCache.get(key);
        if(cachedUsageList != null)
            return cachedUsageList;
        List<MinutesUsageMeta> results = new ArrayList<>();
        results = client.peakBW95List(dataRegion, userId, bucketName, type, startTime, endTime, storageType);
        if(results.size() > 0)
            usageListCache.put(key, results, FIVE_MINUTES); //缓存5min
        return results;
    }

    /**
     * 用户级别 按天查询
     * 获取指定时间范围内某用户在某数据域的所有按天统计值，包含startTime， 不包含endTime
     * 时间格式为yyyy-MM-dd，
     * 包括连接数，但不包括可用带宽
     *
     * @param dataRegion
     * @param userId
     * @param startTime
     * @param endTime
     * @return
     * @throws Exception
     */
    public static List<MinutesUsageMeta> listOwerUsagePerDay(String dataRegion, long userId, String startTime, String endTime, String storageType) throws Exception {
        long st1 = System.currentTimeMillis();
        String key = UsageMetaType.DAY_OWNER + "|" + dataRegion + "|" + userId + "|" + startTime + "|" + endTime + "|" + storageType;
        List<MinutesUsageMeta> cachedUsageList = usageListCache.get(key);
        if (cachedUsageList != null)
            return cachedUsageList;
        long st2 = System.currentTimeMillis();
        List<MinutesUsageMeta> usages = client.minutesUsageList(dataRegion, userId, null, UsageMetaType.DAY_OWNER, startTime, endTime, storageType);
        Utils.timingLog("minutesUsageList", startTime + "|" + endTime + "|" + dataRegion + "|" + userId + "|DAY_OWNER", st2);
        Date date = new Date();
        String today = Misc.formatyyyymmdd(date);
        for (MinutesUsageMeta meta : usages) {
            if (!meta.time.contains(today)) {
                computeBandwith(meta, ONE_DAY / 1000);
            } else {
                long time = (meta.timeVersion - Misc.getDayFirstFiveMinuteDate(date).getTime()) / 1000;
                computeBandwith(meta, time);
            }
        }
        usageListCache.put(key, usages, computeCacheLifeCycle(endTime));
        Utils.timingLog("listOwerUsagePerDay", startTime + "|" + endTime + "|" + dataRegion + "|" + userId, st1);
        return usages;
    }

    /**
     * 用户级别 5分钟查询
     * 获取指定时间范围内某用户在某数据域的所有5分钟统计值，包含startTime， 不包含endTime
     * time的格式为yyyy-MM-dd HH:mm,
     * 包括连接数，但不包括可用带宽
     *
     * @param dataRegion
     * @param userId
     * @param startTime
     * @param endTime
     * @return
     * @throws Exception
     */
    public static List<MinutesUsageMeta> listOwerUsagePer5Minutes(String dataRegion, long userId, String startTime, String endTime, String storageType) throws Exception {
        String key = UsageMetaType.MINUTES_OWNER + "|" + dataRegion + "|" + userId + "|" + startTime + "|" + endTime + "|" + storageType;
        List<MinutesUsageMeta> cachedUsageList = usageListCache.get(key);
        if (cachedUsageList != null)
            return cachedUsageList;
        List<MinutesUsageMeta> usages = client.minutesUsageList(dataRegion, userId, null, UsageMetaType.MINUTES_OWNER, startTime, endTime, storageType);
        for (MinutesUsageMeta meta : usages)
            computeBandwith(meta, FIVE_MINUTES / 1000);
        usageListCache.put(key, usages, computeCacheLifeCycle(endTime));
        return usages;
    }

    /**
     * 用户级别 一小时查询
     * 获取指定时间范围内某用户在某数据域的所有一小时统计值，包含startTime， 不包含endTime
     * time的格式为yyyy-MM-dd HH:mm,
     *
     * @param dataRegion
     * @param userId
     * @param startTime
     * @param endTime
     * @return
     * @throws Exception
     */
    public static List<MinutesUsageMeta> listOwerUsagePerHour(String dataRegion, long userId, String startTime, String endTime, String storageType) throws Exception {
        String key = UsageMetaType.HOUR_OWNER + "|" + dataRegion + "|" + userId + "|" + startTime + "|" + endTime + "|" + storageType;
        List<MinutesUsageMeta> cachedUsageList = usageListCache.get(key);
        if (cachedUsageList != null)
            return cachedUsageList;
        List<MinutesUsageMeta> usages = client.minutesUsageList(dataRegion, userId, null, UsageMetaType.HOUR_OWNER, startTime, endTime, storageType);
        for (MinutesUsageMeta meta : usages)
            computeBandwith(meta, ONE_HOUR / 1000);
        usageListCache.put(key, usages, computeCacheLifeCycle(endTime));
        return usages;
    }

    /**
     * bucket级别 按天查询
     * 获取指定时间范围内某用户在某数据域某bucket的所有按天统计值，包含startTime， 不包含endTime
     * 时间格式为yyyy-MM-dd，
     * 包括连接数，但不包括可用带宽
     *
     * @param dataRegion
     * @param userId
     * @param startTime
     * @param endTime
     * @return
     * @throws Exception
     */
    public static List<MinutesUsageMeta> listBucketUsagePerDay(String dataRegion, long userId, String bucket, String startTime, String endTime, String storageType) throws Exception {
        String key = UsageMetaType.DAY_BUCKET + "|" + dataRegion + "|" + userId + "|" + bucket + "|" + startTime + "|" + endTime + "|" + storageType;
        List<MinutesUsageMeta> cachedUsageList = usageListCache.get(key);
        if (cachedUsageList != null)
            return cachedUsageList;
        List<MinutesUsageMeta> usages = client.minutesUsageList(dataRegion, userId, bucket, UsageMetaType.DAY_BUCKET, startTime, endTime, storageType);
        Date date = new Date();
        String today = Misc.formatyyyymmdd(date);
        for (MinutesUsageMeta meta : usages) {
            if (!meta.time.contains(today)) {
                computeBandwith(meta, ONE_DAY / 1000);
            } else {
                long time = (meta.timeVersion - Misc.getDayFirstFiveMinuteDate(new Date()).getTime()) / 1000;
                computeBandwith(meta, time);
            }
        }
        usageListCache.put(key, usages, computeCacheLifeCycle(endTime));
        return usages;
    }

    /**
     * bucket级别 按5分钟查询
     * 获取指定时间范围内某用户在某数据域某bucket的所有5分钟统计值，包含startTime， 不包含endTime
     * time的格式为yyyy-MM-dd HH:mm,
     * 包括连接数，但不包括可用带宽
     *
     * @param dataRegion
     * @param userId
     * @param startTime
     * @param endTime
     * @return
     * @throws Exception
     */
    public static List<MinutesUsageMeta> listBucketUsagePer5Minutes(String dataRegion, long userId, String bucket, String startTime, String endTime, String storageType) throws Exception {
        String key = UsageMetaType.MINUTES_BUCKET + "|" + dataRegion + "|" + userId + "|" + bucket + "|" + startTime + "|" + endTime + "|" + storageType;
        List<MinutesUsageMeta> cachedUsageList = usageListCache.get(key);
        if (cachedUsageList != null)
            return cachedUsageList;
        List<MinutesUsageMeta> usages = client.minutesUsageList(dataRegion, userId, bucket, UsageMetaType.MINUTES_BUCKET, startTime, endTime, storageType);
        for (MinutesUsageMeta meta : usages)
            computeBandwith(meta, FIVE_MINUTES / 1000);
        usageListCache.put(key, usages, computeCacheLifeCycle(endTime));
        return usages;
    }

    /**
     * @param dataRegion
     * @param userId
     * @param bucket
     * @param startTime
     * @param endTime
     * @return
     * @throws Exception
     */
    public static List<MinutesUsageMeta> listBucketUsagePerHour(String dataRegion, long userId, String bucket, String startTime, String endTime, String storageType) throws Exception {
        String key = UsageMetaType.HOUR_BUCKET + "|" + dataRegion + "|" + userId + "|" + bucket + "|" + startTime + "|" + endTime + "|" + storageType;
        List<MinutesUsageMeta> cachedUsageList = usageListCache.get(key);
        if (cachedUsageList != null)
            return cachedUsageList;
        List<MinutesUsageMeta> usages = client.minutesUsageList(dataRegion, userId, bucket, UsageMetaType.HOUR_BUCKET, startTime, endTime, storageType);
        for (MinutesUsageMeta meta : usages)
            computeBandwith(meta, ONE_HOUR / 1000);
        usageListCache.put(key, usages, computeCacheLifeCycle(endTime));
        return usages;
    }

    /**
     * 各region 5分钟查询
     * 获取指定时间范围内各个region内所有用户的5分钟统计信息，包含startTime， 不包含endTime
     * 时间格式为yyyy-MM-dd HH:mm,
     * 包括连接数，但不包括可用带宽
     *
     * @param startTime
     * @param endTime
     * @return
     * @throws Exception
     */
    public static List<MinutesUsageMeta> listAllRegionUsagePer5Minutes(String startTime, String endTime, String storageType) throws Exception {
        long startT = System.currentTimeMillis();
        String key = UsageMetaType.FIVE_MINUTES + "|listPer5Minutes|allRegion|" + startTime + "|" + endTime
                + "|" + storageType;
        List<MinutesUsageMeta> cachedUsageList = usageListCache.get(key);
        if (cachedUsageList != null) {
            return cachedUsageList;
        }
        List<MinutesUsageMeta> usages = client.minutesUsageList(null, -1, null, UsageMetaType.FIVE_MINUTES, startTime, endTime, storageType); //所有时间范围内的FIVE_MINUTES类型

        if (usages.isEmpty()) {
            return null;
        }
        TreeMap<String, List<MinutesUsageMeta>> region2Usages = new TreeMap<>();
        //按region区分
        for (MinutesUsageMeta usage : usages) {
            LocalDateTime tempDateTime = LocalDateTime.parse(usage.time, Misc.format_uuuu_MM_dd_HH_mm);
            if (tempDateTime.getHour() == 0 && tempDateTime.getMinute() == 0) {
                usage.time = tempDateTime.minusDays(1).format(Misc.format_uuuu_MM_dd).concat(" 24:00");
            }
            region2Usages.computeIfAbsent(usage.regionName, k -> new LinkedList<>()).add(usage);
        }
        for (Entry<String, List<MinutesUsageMeta>> entry : region2Usages.entrySet()) {
            for (MinutesUsageMeta meta : entry.getValue()) {
                computeBandwith(meta, FIVE_MINUTES / 1000);
            }
        }
        usageListCache.put(key, usages, computeCacheLifeCycle(endTime));
        Utils.timingLog("listAllRegionUsagePer5Minutes", startTime + "|" + endTime, startT);
        return usages;
    }

    /**
     * 各region 按天查询
     * 获取指定时间范围内各个region内所有用户的按天统计信息，包含startTime， 不包含endTime
     * 时间格式为yyyy-MM-dd,
     * 包括连接数，但不包括可用带宽
     *
     * @param startTime
     * @param endTime
     * @return
     * @throws Exception
     */
    public static List<MinutesUsageMeta> listAllRegionUsagePerDay(String startTime, String endTime, String storageType) throws Exception {
        String key = UsageMetaType.REGION_DAY + "|listPerDay|allRegion|" + startTime + "|" + endTime
                + "|" + storageType;
        List<MinutesUsageMeta> cachedUsageList = usageListCache.get(key);
        if (cachedUsageList != null)
            return cachedUsageList;
        long st = System.currentTimeMillis();
        List<MinutesUsageMeta> usages;
        if (endTime.compareTo(PeriodUsageStatsConfig.updateDate) < 0) {
            // 查询日期都在升级之前
            usages = listAllRegionUsagePerDayBeforeUpdate(startTime, endTime);
        } else if (startTime.compareTo(PeriodUsageStatsConfig.updateDate) >= 0) {
            // 查询日期都在升级之后
            usages = client.minutesUsageList(null, -1, null, UsageMetaType.REGION_DAY, startTime, endTime + Character.MAX_VALUE, storageType); // 时间范围内所有region的FIVE_MINUTES类型数据
        } else {
            // 查询日期包含升级前与升级后
            usages = listAllRegionUsagePerDayBeforeUpdate(startTime, PeriodUsageStatsConfig.updateDate);
            usages.addAll(client.minutesUsageList(null, -1, null, UsageMetaType.REGION_DAY, PeriodUsageStatsConfig.updateDate, endTime + Character.MAX_VALUE, storageType));
        }
        Utils.timingLog("minutesUsageList", startTime + "|" + endTime + "|allRegion|REGION_DAY", st);
        if (usages.isEmpty())
            return null;
        usageListCache.put(key, usages, computeCacheLifeCycle(endTime));
        return usages;
    }
    
    
    /**
     * 用于查询 新版统计上线之前的 各region 按天查询，新版统计的region day改为使用新的数据结构，因此需要做数据查询兼容
     * 获取指定时间范围内各个region内所有用户的按天统计信息，包含startTime， 不包含endTime
     * 时间格式为yyyy-MM-dd,
     * 包括连接数，但不包括可用带宽
     * @param startTime
     * @param endTime
     * @return
     * @throws Exception
     */
    public static List<MinutesUsageMeta> listAllRegionUsagePerDayBeforeUpdate(String startTime, String endTime) throws Exception {
        startTime = Misc.formatyyyymmddhhmm(Misc.formatyyyymmdd(startTime));
        endTime = endTime.concat(" 23:59");
        String key = UsageMetaType.FIVE_MINUTES + "|listPerDay|allRegion|" + startTime + "|" + endTime;
        List<MinutesUsageMeta> cachedUsageList = usageListCache.get(key);
        if (cachedUsageList != null)
            return cachedUsageList;
        List<MinutesUsageMeta> usages = client.minutesUsageList(null, -1, null, UsageMetaType.FIVE_MINUTES, startTime, endTime, Consts.STORAGE_CLASS_STANDARD); // 时间范围内所有region的FIVE_MINUTES类型数据
        TreeMap<String, List<MinutesUsageMeta>> region2Usages = new TreeMap<>(); // 按region分meta
        for (MinutesUsageMeta usage : usages) {
            if (region2Usages.containsKey(usage.regionName)) {
                region2Usages.get(usage.regionName).add(usage);
            } else {
                List<MinutesUsageMeta> usageList = new LinkedList<>();
                usageList.add(usage);
                region2Usages.put(usage.regionName, usageList);
            }
        }
        List<MinutesUsageMeta> results = new LinkedList<>();
        for (Entry<String, List<MinutesUsageMeta>> entry : region2Usages.entrySet()) {
            MinutesUsageMeta lastMeta = entry.getValue().get(0); // 时间范围内第一个FIVE_MINUTES
            // 初始化lastMeta的平均容量
            if (lastMeta.sizeStats != null) {
                lastMeta.sizeStats.size_avgTotal = lastMeta.sizeStats.size_total;
                lastMeta.sizeStats.size_avgRedundant = lastMeta.sizeStats.size_redundant;
                lastMeta.sizeStats.size_avgAlin = lastMeta.sizeStats.size_alin;
                lastMeta.sizeStats.size_avgOriginalTotal = lastMeta.sizeStats.size_originalTotal;
            }
            String lastDay = Misc.formatyyyymmdd(Misc.formatyyyymmddhhmm(lastMeta.time));
            int j = 0;
            for (int i = 1; i < entry.getValue().size(); i++) {
                MinutesUsageMeta meta = entry.getValue().get(i);
                meta.fiveMinutesInMills = meta.timeVersion;
                if (meta.time.contains(lastDay)) {
                    if (lastMeta.sizeStats != null && meta.sizeStats != null) {
                        j++;
                    }
                    merge(meta, lastMeta, j);
                } else {
                    computeBandwith(lastMeta, ONE_DAY / 1000);
                    lastMeta.time = Misc.formatyyyymmdd(Misc.formatyyyymmddhhmm(lastMeta.time));
                    results.add(lastMeta);
                    lastMeta = meta;
                    // 初始化lastMeta的平均容量
                    if (lastMeta.sizeStats != null) {
                        lastMeta.sizeStats.size_avgTotal = lastMeta.sizeStats.size_total;
                        lastMeta.sizeStats.size_avgRedundant = lastMeta.sizeStats.size_redundant;
                        lastMeta.sizeStats.size_avgAlin = lastMeta.sizeStats.size_alin;
                        lastMeta.sizeStats.size_avgOriginalTotal = lastMeta.sizeStats.size_originalTotal;
                    }
                    lastMeta.fiveMinutesInMills = lastMeta.timeVersion;
                    lastDay = Misc.formatyyyymmdd(Misc.formatyyyymmddhhmm(meta.time));
                    j = 0;
                }
            }
            computeBandwith(lastMeta, ONE_DAY / 1000);
            lastMeta.time = Misc.formatyyyymmdd(Misc.formatyyyymmddhhmm(lastMeta.time));
            results.add(lastMeta);
        }
        usageListCache.put(key, results, computeCacheLifeCycle(endTime));
        return results;
    }

    /**
     * 获取某Region指定时间范围内5分钟级别可用带宽，包含startTime， 不包含endTime
     * 时间格式为yyyy-MM-dd HH:mm,
     *
     * @param dataRegion
     * @param time
     * @return
     * @throws Exception
     */
    public static List<MinutesUsageMeta> listAvailableBandwidth(String dataRegion, String startTime, String endTime) throws Exception {
        LocalDateTime startDateTime = LocalDateTime.parse(startTime, Misc.format_uuuu_MM_dd_HH_mm);
        if (startDateTime.getHour() == 0 && startDateTime.getMinute() == 0) {
            startTime = startDateTime.plusMinutes(5).format(Misc.format_uuuu_MM_dd_HH_mm);
        }
        String key = UsageMetaType.MINUTES_AVAIL_BW + "|" + dataRegion + "|" + startTime + "|" + endTime;
        List<MinutesUsageMeta> cachedUsageList = usageListCache.get(key);
        if (cachedUsageList != null)
            return cachedUsageList;
        long st = System.currentTimeMillis();
        List<MinutesUsageMeta> usages = client.minutesUsageList(dataRegion, -1, null, UsageMetaType.MINUTES_AVAIL_BW, startTime, endTime, Consts.STORAGE_CLASS_STANDARD);
        Utils.timingLog("minutesUsageList", startTime + "|" + endTime + "|" + dataRegion + "|MINUTES_AVAIL_BW", st);
        for (MinutesUsageMeta usage : usages) {
            LocalDateTime tempDateTime = LocalDateTime.parse(usage.time, Misc.format_uuuu_MM_dd_HH_mm);
            if (tempDateTime.getHour() == 0 && tempDateTime.getMinute() == 0) {
                usage.time = tempDateTime.minusDays(1).format(Misc.format_uuuu_MM_dd).concat(" 24:00");
            }
            usage.fiveMinutesInMills = usage.timeVersion;
        }
        usageListCache.put(key, usages, computeCacheLifeCycle(endTime));
        return usages;
    }
    
    /**
     * 获取指定时间范围内某用户在某数据域的5分钟之内连接数统计值，包含startTime， 不包含endTime
     * time的格式为yyyy-MM-dd HH:mm,startTime与endTime相等或相差5分钟
     * 只包括连接数
     * @param dataRegion
     * @param userId
     * @param startTime
     * @param endTime
     * @return
     * @throws Exception
     */
    public static List<MinutesUsageMeta> listOwnerConnectionPer5Minutes(String dataRegion, long userId, String startTime, String endTime) throws Exception {
        UsageMetaType type = UsageMetaType.MINUTES_OWNER;
        String key = UsageMetaType.MINUTES_OWNER + "|" + dataRegion + "|" + userId + "|" + startTime + "|" + endTime;
        List<MinutesUsageMeta> cachedUsageList = usageListCache.get(key);
        if(cachedUsageList != null)
            return cachedUsageList;
        List<MinutesUsageMeta> usages = client.minutesUsageConnectionStatsList(dataRegion, userId, null, type, startTime, endTime);
        // 补充每秒连接数
        // 查询起止时间相同
        if (endTime.startsWith(startTime)) {
            if (usages.size() > 0) {
                // 时间显示到秒
                usages.get(0).time = usages.get(0).time+":00";
            } else {
                // 补0
                String time = Misc.formatyyyymmddhhmm(Misc.formatyyyymmddhhmm(startTime))+":00";
                usages.add(new MinutesUsageMeta(type,dataRegion,userId,time,Consts.STORAGE_CLASS_STANDARD));
            }
        } else {
            // 查询起止时间相差5分钟
            if (usages.size() == 2) {
                usages.get(0).time = usages.get(0).time + ":00";
                usages.get(1).time = usages.get(1).time + ":00";
            } else if (usages.size() == 1) {
                if (usages.get(0).time.equals(startTime)) {
                    usages.get(0).time = usages.get(0).time + ":00";
                    usages.add(new MinutesUsageMeta(type, dataRegion, userId, Misc.formatyyyymmddhhmm(Misc.formatyyyymmddhhmm(endTime)) + ":00", Consts.STORAGE_CLASS_STANDARD));
                } else {
                    usages.get(0).time = usages.get(0).time + ":00";
                    MinutesUsageMeta tmpUsage = usages.get(0);
                    usages.remove(0);
                    usages.add(new MinutesUsageMeta(type, dataRegion, userId, Misc.formatyyyymmddhhmm(Misc.formatyyyymmddhhmm(startTime)) + ":00", Consts.STORAGE_CLASS_STANDARD));
                    usages.add(tmpUsage);
                }
            } else {
                // 补0
                usages.add(new MinutesUsageMeta(type,dataRegion, userId, Misc.formatyyyymmddhhmm(Misc.formatyyyymmddhhmm(startTime)) + ":00", Consts.STORAGE_CLASS_STANDARD));
                usages.add(new MinutesUsageMeta(type, dataRegion, userId, Misc.formatyyyymmddhhmm(Misc.formatyyyymmddhhmm(endTime)) + ":00", Consts.STORAGE_CLASS_STANDARD));
            }
            generateRandomDatas(usages, type, dataRegion, userId, null);
        }
    
        usageListCache.put(key, usages);
        return usages;
    }
    
    /**
     * 获取指定时间范围内某用户某bucket在某数据域的5分钟之内连接数统计值，包含startTime， 不包含endTime
     * time的格式为yyyy-MM-dd HH:mm,startTime与endTime相等或相差5分钟
     * 只包括连接数
     * @param dataRegion
     * @param userId
     * @param bucketName
     * @param startTime
     * @param endTime
     * @return
     * @throws Exception
     */
    public static List<MinutesUsageMeta> listBucketConnectionPer5Minutes(String dataRegion, long userId, String bucketName, String startTime, String endTime) throws Exception {
        UsageMetaType type = UsageMetaType.MINUTES_BUCKET;
        String key = UsageMetaType.MINUTES_BUCKET + "|" + dataRegion + "|" + userId + "|" + bucketName + "|" + startTime + "|" + endTime;
        List<MinutesUsageMeta> cachedUsageList = usageListCache.get(key);
        if(cachedUsageList != null)
            return cachedUsageList;
        List<MinutesUsageMeta> usages = client.minutesUsageConnectionStatsList(dataRegion, userId, bucketName, type, startTime, endTime);
        // 补充每秒连接数
        if (endTime.startsWith(startTime)) {
            // 查询起止时间相同
            if (usages.size() > 0) {
                usages.get(0).time = usages.get(0).time+":00";
            } else {
                // 补0
                String time = Misc.formatyyyymmddhhmm(Misc.formatyyyymmddhhmm(startTime))+":00";
                usages.add(new MinutesUsageMeta(type, dataRegion, userId, bucketName, time, Consts.STORAGE_CLASS_STANDARD));
            }
        } else {
            // 查询起止时间相差5分钟
            if (usages.size() == 2) {
                usages.get(0).time = usages.get(0).time + ":00";
                usages.get(1).time = usages.get(1).time + ":00";
            } else if (usages.size() == 1) {
                if (usages.get(0).time.equals(startTime)) {
                    usages.get(0).time = usages.get(0).time + ":00";
                    usages.add(new MinutesUsageMeta(type, dataRegion, userId, bucketName, Misc.formatyyyymmddhhmm(Misc.formatyyyymmddhhmm(endTime)) + ":00", Consts.STORAGE_CLASS_STANDARD));
                } else {
                    usages.get(0).time = usages.get(0).time + ":00";
                    MinutesUsageMeta tmpUsage = usages.get(0);
                    usages.remove(0);
                    usages.add(new MinutesUsageMeta(type, dataRegion, userId, bucketName, Misc.formatyyyymmddhhmm(Misc.formatyyyymmddhhmm(startTime)) + ":00", Consts.STORAGE_CLASS_STANDARD));
                    usages.add(tmpUsage);
                }
            } else {
                // 补0
                usages.add(new MinutesUsageMeta(type,dataRegion, userId, bucketName, Misc.formatyyyymmddhhmm(Misc.formatyyyymmddhhmm(startTime)) + ":00", Consts.STORAGE_CLASS_STANDARD));
                usages.add(new MinutesUsageMeta(type, dataRegion, userId, bucketName, Misc.formatyyyymmddhhmm(Misc.formatyyyymmddhhmm(endTime)) + ":00", Consts.STORAGE_CLASS_STANDARD));
            }
            generateRandomDatas(usages, type, dataRegion, userId, bucketName);
        }
    
        usageListCache.put(key, usages);
        return usages;
    }
    
    /**
     * 补足5分钟之内的每秒连接数，根据起止点算出斜率，在这个斜率基础上产生一些随机点，以补充中间缺少的每秒连接数
     * @param usages
     * @param type
     * @param dataRegion
     * @param userId
     * @throws ParseException
     */
    private static void generateRandomDatas(List<MinutesUsageMeta> usages, UsageMetaType type,String dataRegion, long userId, String bucketName) throws ParseException {
        if (usages != null && usages.size() == 2) {
            if (usages.get(1).connection == null) {
                usages.get(1).connection = new Connection();
            }
            if (usages.get(0).connection == null) {
                usages.get(0).connection = new Connection();
            }
            long diff = usages.get(1).connection.connections - usages.get(0).connection.connections;
            long nonetDiff = usages.get(1).connection.noNet_connections - usages.get(0).connection.noNet_connections;

            double delta = diff*1.0 / 300;
            double nonetDelta = nonetDiff*1.0 / 300;

            for (int i = 1; i<300; i++) {
                double v = usages.get(0).connection.connections + i*delta;
                double max = v + v * PeriodUsageStatsConfig.ratio;
                double min = v - v * PeriodUsageStatsConfig.ratio;
                v = (long) (Math.random() * (max - min) + min);
                if (v < 0)
                    v = 0;
                double nonetV = usages.get(0).connection.noNet_connections + i*nonetDelta;
                double nonetMax = nonetV + nonetV * PeriodUsageStatsConfig.ratio;
                double nonetMin = nonetV - nonetV * PeriodUsageStatsConfig.ratio;
                nonetV = (long) (Math.random() * (nonetMax - nonetMin) + nonetMin);
                if (nonetV < 0)
                    nonetV = 0;
                long time = Misc.formatyyyymmddhhmmss(usages.get(0).time).getTime() + i*1000;
                String timeString = Misc.formatyyyymmddhhmmss(new Date(time));
                MinutesUsageMeta usage = null;
                if (UsageMetaType.MINUTES_OWNER.equals(type)) {
                    usage = new MinutesUsageMeta(type, dataRegion, userId, timeString, Consts.STORAGE_CLASS_STANDARD);
                } else {
                    usage = new MinutesUsageMeta(type, dataRegion, userId, bucketName, timeString, Consts.STORAGE_CLASS_STANDARD);
                }
                usage.connection = new Connection();
                usage.connection.connections = (long)v;
                usage.connection.noNet_connections = (long)nonetV;
                
                usages.add(usage);
            }
            //调整数据顺序
            MinutesUsageMeta tmpUsage = usages.get(1);
            usages.remove(1);
            usages.add(tmpUsage);
        }
    }
    
    /**
     * 利用流量计算已用带宽
     * @param usageMeta
     * @param seconds
     */
    private static void computeBandwith(MinutesUsageMeta usageMeta, long seconds) {
        usageMeta.bandwidthMeta = new BandwidthMeta();
        if(seconds == 0)
            seconds = FIVE_MINUTES / 1000;
        if (usageMeta.flowStats != null) {
            usageMeta.bandwidthMeta.noNetRoamFlow = usageMeta.flowStats.flow_noNetRoamDownload / seconds;
            usageMeta.bandwidthMeta.noNetRoamUpload = usageMeta.flowStats.flow_noNetRoamUpload / seconds;
            usageMeta.bandwidthMeta.noNetTransfer = usageMeta.flowStats.flow_noNetDownload / seconds;
            usageMeta.bandwidthMeta.noNetUpload = usageMeta.flowStats.flow_noNetUpload / seconds;
            usageMeta.bandwidthMeta.roamFlow = usageMeta.flowStats.flow_roamDownload / seconds;
            usageMeta.bandwidthMeta.roamUpload = usageMeta.flowStats.flow_roamUpload / seconds;
            usageMeta.bandwidthMeta.transfer = usageMeta.flowStats.flow_download / seconds;
            usageMeta.bandwidthMeta.upload = usageMeta.flowStats.flow_upload / seconds;
        } 
    }
    
    /**
     * 合并5分钟统计信息
     * @param meta
     * @param lastMeta
     * @param num
     */
    private static void merge(MinutesUsageMeta meta, MinutesUsageMeta lastMeta, int num) {
        if(lastMeta.sizeStats == null && meta.sizeStats != null) {
            lastMeta.sizeStats = meta.sizeStats;
        }
        if (lastMeta.flowStats == null && meta.flowStats != null) {
            lastMeta.flowStats = new FlowStats();
        }
        if (lastMeta.requestStats == null && meta.requestStats != null) {
            lastMeta.requestStats = new RequestStats();
        }
        if (lastMeta.codeRequestStats == null && meta.codeRequestStats != null) {
            lastMeta.codeRequestStats = new CodeRequestStats();
        }
        if(lastMeta.sizeStats != null && meta.sizeStats != null) {
            //计算lastMeta的平均容量
            BigDecimal totalSize = new BigDecimal(0);
            totalSize = totalSize.add((new BigDecimal(lastMeta.sizeStats.size_avgTotal)).multiply(new BigDecimal(num)))
            .add((new BigDecimal(meta.sizeStats.size_total)));
            lastMeta.sizeStats.size_avgTotal = totalSize.divide(new BigDecimal(num + 1), RoundingMode.DOWN).longValue();
            
            BigDecimal redundantSize = new BigDecimal(0);
            redundantSize = redundantSize.add((new BigDecimal(lastMeta.sizeStats.size_avgRedundant)).multiply(new BigDecimal(num)))
            .add((new BigDecimal(meta.sizeStats.size_redundant)));
            lastMeta.sizeStats.size_avgRedundant = redundantSize.divide(new BigDecimal(num + 1), RoundingMode.DOWN).longValue();
            
            BigDecimal alinSize = new BigDecimal(0);
            alinSize = alinSize.add((new BigDecimal(lastMeta.sizeStats.size_avgAlin)).multiply(new BigDecimal(num)))
            .add((new BigDecimal(meta.sizeStats.size_alin)));
            lastMeta.sizeStats.size_avgAlin = alinSize.divide(new BigDecimal(num + 1), RoundingMode.DOWN).longValue();
            
            BigDecimal originalTotalSize = new BigDecimal(0);
            originalTotalSize = originalTotalSize.add((new BigDecimal(lastMeta.sizeStats.size_avgOriginalTotal)).multiply(new BigDecimal(num)))
            .add((new BigDecimal(meta.sizeStats.size_originalTotal)));
            lastMeta.sizeStats.size_avgOriginalTotal = originalTotalSize.divide(new BigDecimal(num + 1), RoundingMode.DOWN).longValue();

            //重置peakSize
            lastMeta.sizeStats.size_peak = meta.sizeStats.size_peak > lastMeta.sizeStats.size_peak
                    ? meta.sizeStats.size_peak
                    : lastMeta.sizeStats.size_peak;
            lastMeta.sizeStats.size_originalPeak = meta.sizeStats.size_originalPeak > lastMeta.sizeStats.size_originalPeak
                    ? meta.sizeStats.size_originalPeak
                    : lastMeta.sizeStats.size_originalPeak;
            lastMeta.sizeStats.size_completePeak = meta.sizeStats.size_completePeak > lastMeta.sizeStats.size_completePeak 
                    ? meta.sizeStats.size_completePeak
                    : lastMeta.sizeStats.size_completePeak;

            //size已经是总容量，重置size
            lastMeta.sizeStats.size_total = meta.sizeStats.size_total;
            lastMeta.sizeStats.size_originalTotal = meta.sizeStats.size_originalTotal;
            lastMeta.sizeStats.size_redundant = meta.sizeStats.size_redundant;
            lastMeta.sizeStats.size_alin = meta.sizeStats.size_alin;
        }
        lastMeta.flowStats.sumAll(meta.flowStats);
        lastMeta.requestStats.sumAll(meta.requestStats);
        if (lastMeta.codeRequestStats == null) {
            lastMeta.codeRequestStats = new CodeRequestStats();
        }
        if (meta.codeRequestStats != null) {
            lastMeta.codeRequestStats.sumAll(meta.codeRequestStats);
        }
        if (lastMeta.connection == null) {
            lastMeta.connection = new Connection();
        }
        if (meta.connection != null) {
            lastMeta.connection.connections += meta.connection.connections;
        }        
        lastMeta.time = meta.time;
        lastMeta.fiveMinutesInMills = meta.fiveMinutesInMills;
    }
    
    //如果是同一天的按天查询则缓存5分钟，同一天的按5分钟查询则缓存10分钟，不是同一天的查询则缓存30分钟，只有当新的缓存插入时，才会移除过期的缓存
    private static long computeCacheLifeCycle(String time) throws ParseException {
        String thisDay = Misc.formatyyyymmdd(new Date());
        if (time.equals(thisDay))
            return 300000;
        else if(time.contains(thisDay))
            return 600000;
        else
            return 1800000;
    }
}
