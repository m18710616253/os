package cn.ctyun.oos.server.usage;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAdjusters;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.collect.Lists;

import cn.ctyun.common.Consts;
import cn.ctyun.common.conf.DataRegion;
import cn.ctyun.oos.common.Utils;
import cn.ctyun.oos.metadata.BucketMeta;
import cn.ctyun.oos.hbase.MetaClient;
import cn.ctyun.oos.metadata.MinutesUsageMeta;
import cn.ctyun.oos.metadata.MinutesUsageMeta.PeakBandWidth;
import cn.ctyun.oos.metadata.MinutesUsageMeta.UsageMetaType;
import cn.ctyun.oos.server.conf.PeriodUsageStatsConfig;
import common.tuple.Triple;
import common.util.BlockingExecutor;

/**
 * 统计带宽95峰值，每月执行一次，执行时间通过配置文件配置，需要统计的用户及其数据域通过配置文件配置
 * 在periodUsageStats统计进程中调用
 * @author wushuang
 *
 */
class PeakBandWidthStats extends Thread{
    
    private boolean isGlobal = false;
    
    /**
     * 是否global的统计进程
     * @param isGlobal
     */
    public PeakBandWidthStats(boolean isGlobal) {
        this.isGlobal = isGlobal;
    }
    
    private static Log log = LogFactory.getLog(PeakBandWidthStats.class);
    
    BlockingExecutor executor = new BlockingExecutor(1, 10, 10000, 100000, "Owner95PeakBWStats");
    public void run() {
        boolean peakBWStatsIniEnabled = PeriodUsageStatsConfig.peakBWStatsIniEnabled;
        String[] fields = PeriodUsageStatsConfig.peakBWStatsTime.split(":");
        int dateOfRunTime = Integer.parseInt(fields[0]);
        int hourOfRunTime = Integer.parseInt(fields[1]);
        int minuteOfRunTime = Integer.parseInt(fields[2]);
        while (true) {
            Instant instant = Instant.now();
            LocalDateTime now = LocalDateTime.ofInstant(instant, ZoneOffset.systemDefault()); //系统时间
            //开启了立即执行或到统计时间
            int sleepDays = now.with(TemporalAdjusters.lastDayOfMonth()).getDayOfMonth() - 1; 
            if (peakBWStatsIniEnabled || (now.getDayOfMonth() == dateOfRunTime
                    && now.getHour() == hourOfRunTime
                    && now.getMinute() < (minuteOfRunTime + 5)
                    && now.getMinute() > minuteOfRunTime)) {
                try {
                    log.info("95peakBandWidth Stats Info: Start 95 peak bandwidth stats. at time: " + now.format(DateTimeFormatter.ofPattern("uuuu-MM-d HH:mm:ss")));
                    Map<Long, List<String>> ownerRegions = Utils.get95PeakBWPermittedOwnerRegions(); // 从配置文件获取需要计算的owner的带宽95峰值统计权限对应的数据域
                    if (ownerRegions != null && ownerRegions.size() > 0) { // 有需要計算的用户
                        for (Entry<Long, List<String>> ownerRegion : ownerRegions.entrySet()) {
                            long ownerId = ownerRegion.getKey();
                            List<String> regions = ownerRegion.getValue();
                            if (isGlobal) {
                                regions.removeAll(PeriodUsageStatsConfig.regionNames);
                            } else {
                                if(regions.contains(DataRegion.getRegion().getName())) {
                                    regions = Lists.newArrayList(DataRegion.getRegion().getName());
                                } else 
                                    continue;
                            }                            
                            for (String storageType : Utils.STORAGE_TYPES) {
                                executor.submit(new Owner95PeakBWStats(ownerId, regions, instant, storageType));
                            }
                        }
                    }
                    Thread.sleep(sleepDays * 24 * 60 * 60 * 1000L + Consts.SLEEP_TIME * 1000); //x天23小时50分钟
                } catch (Throwable e) {
                    log.error(e.getMessage(), e);
                }
                peakBWStatsIniEnabled = false;
            }
            try {
                Thread.sleep(Consts.ONE_MINITE);
            } catch (InterruptedException e) {
                log.error(e);
            }
        }
    }
    
    /**
     * 执行每个owner的带宽95峰值统计
     *
     */
    class Owner95PeakBWStats implements Runnable{
        long ownerId;
        List<String> regions;
        Instant instant;
        String storageType;
        private final MetaClient metaClient = MetaClient.getGlobalClient();
        private static final long FIVE_MINUTES_SEC = 5 * 60;
        
        public Owner95PeakBWStats(long ownerId, List<String> regions, Instant instant, String storageType) {
            this.ownerId = ownerId;
            this.regions = regions;
            this.instant = instant;
            this.storageType = storageType;
        }
        
        @Override
        public void run() {
            try {
                log.info("95peakBandWidth Stats Info: owner 95 peakBandWidth stats start: owner " + ownerId);
                //获取上个月的天数，开始时间（2019-01-01 08:00），结束时间（2019-02-01 08:00）
                Triple<Integer, String, String> numOfDays_MonthStart_End = getMonthBeginEndDate(); 
                //截取上个月开始时间（2019-01-01 08:00），获得上个月（2019-01）
                String lastMonth = numOfDays_MonthStart_End.second().substring(0,7);
                
                // 绑定了角色的owner的所有bucket
                List<String> ownerBuckets = getOwnerBuckets(ownerId); 
                List<MinutesUsageMeta> peakBWMetasToBatchInsert = new ArrayList<>();
                for (String region : regions) {
                    List<MinutesUsageMeta> minutesUsageOwner = metaClient.minutesUsageCommonStatsList(region, ownerId, null, UsageMetaType.MINUTES_OWNER, numOfDays_MonthStart_End.second(), numOfDays_MonthStart_End.third(), storageType); //获取owner级别时间范围内所有minutes类型数据
                    MinutesUsageMeta peakBWOwnerMeta = new MinutesUsageMeta(UsageMetaType.MONTH_OWNER, region, ownerId, lastMonth, storageType);
                    peakBWOwnerMeta.timeVersion = System.currentTimeMillis();
                    peakBWOwnerMeta.peakBandWidth = get95PeakBandWidth(minutesUsageOwner, numOfDays_MonthStart_End.first());
                    peakBWMetasToBatchInsert.add(peakBWOwnerMeta);
                    for (String bucketName : ownerBuckets) { //bucket级别
                        List<MinutesUsageMeta> minutesUsageBucket = metaClient.minutesUsageCommonStatsList(region, ownerId, bucketName, UsageMetaType.MINUTES_BUCKET, numOfDays_MonthStart_End.second(), numOfDays_MonthStart_End.third(), storageType); //获取bucket级别时间范围内所有minutes类型数据
                        MinutesUsageMeta peakBWBucketMeta = new MinutesUsageMeta(UsageMetaType.MONTH_BUCKET, region, ownerId, bucketName, lastMonth, storageType);
                        peakBWBucketMeta.timeVersion = System.currentTimeMillis();
                        peakBWBucketMeta.peakBandWidth = get95PeakBandWidth(minutesUsageBucket, numOfDays_MonthStart_End.first());
                        peakBWMetasToBatchInsert.add(peakBWBucketMeta);
                    }
                }
                metaClient.minutesUsageBatchInsert(peakBWMetasToBatchInsert);
                log.info("95peakBandWidth Stats Info: owner 95 peakBandWidth stats end: owner "+ ownerId);
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }
    
        /**
         * 获取相对于UTC时间的本地时间中的上个月时间，包括上个月的天数、月开始时间（2019-01-01 08：00）、月结束时间（2019-02-01 08：00）
         * @param numOfDays
         * @return Triple<Integer,String,String>(上个月的天数，月开始时间，月结束时间)
         */
        private Triple<Integer,String,String> getMonthBeginEndDate(){
//            DateTimeFormatter monthFormatter = DateTimeFormatter.ofPattern("uuuu-MM");
            DateTimeFormatter minutesFormatter = DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm");
            //statsCalendar是stats的时间，要获取statsCalendar上一个月utc时间对应的北京时间
            ZonedDateTime statsUTCTime = ZonedDateTime.ofInstant(instant, ZoneOffset.UTC); //stats开始时间对应的UTC时间
//            String month = statsUTCTime.minusMonths(1).format(monthFormatter); //上个月 2019-01
            int numOfDays = statsUTCTime.toLocalDate().minusMonths(1).with(TemporalAdjusters.lastDayOfMonth()).getDayOfMonth(); //上个月有多少天
            //上一个月开始时间与结束时间
            LocalDate firstDay = statsUTCTime.toLocalDate().minusMonths(1).with(TemporalAdjusters.firstDayOfMonth());
            LocalDate lastDay = statsUTCTime.toLocalDate().with(TemporalAdjusters.firstDayOfMonth());
            ZonedDateTime startTimeUTC = ZonedDateTime.of(firstDay, LocalTime.of(0, 0, 0), ZoneOffset.UTC); //UTC 2019-01-01 00:00
            ZonedDateTime startTimeLocal = startTimeUTC.withZoneSameInstant(ZoneId.systemDefault());
            String startTime = startTimeLocal.format(minutesFormatter); //2019-01-01 08:00
            ZonedDateTime endTimeUTC = ZonedDateTime.of(lastDay, LocalTime.of(0, 0, 0), ZoneOffset.UTC); //UTC 2019-02-01 00:00
            ZonedDateTime endTimeLocal = endTimeUTC.withZoneSameInstant(ZoneId.systemDefault());
            String endTime = endTimeLocal.format(minutesFormatter); //2019-02-01 08:00
//            log.info("owner: " + ownerId + " beginDate: "+ startTime + " endDate: " + endTime);
            Triple<Integer, String, String> monthStartEnd = new Triple<Integer, String, String>(numOfDays, startTime, endTime);
            return monthStartEnd;
        }
        
        /**
         * 绑定了角色的owner的所有bucket
         * @param ownerRegions
         * @return
         * @throws IOException 
         */
        private List<String> getOwnerBuckets(long ownerId) throws IOException {
            List<String> bucketNames = new ArrayList<>();
            for (BucketMeta bucketMeta : metaClient.bucketList(ownerId)) {
                bucketNames.add(bucketMeta.getName());
            }
            return bucketNames;
        }
        
        private int partition(Long[] a, int low, int high) {
            long pt = a[low];
            while (low < high) {
                //从后往前找比枢轴值大的交换位置
                while (low < high && a[high] < pt)
                    high--;
                a[low] = a[high];
                while (low < high && a[low] >= pt)
                    low++;
                a[high] = a[low];
            }
            a[low] = pt;
            return low;
        }
        
        private long search(Long[] a, int low, int high, int k)
        {
            int m = partition(a, low, high);
            if (m == k - 1) 
                return a[m];
            else if (m > k-1)
                return search(a, low, m-1, k);
            else
                return search(a, m+1, high, k);
        }
        
        /**
         * 获取95峰值上行带宽及下行带宽
         * @param metas
         * @return
         */
        private PeakBandWidth get95PeakBandWidth(List<MinutesUsageMeta> metas, int numOfDays) {
            PeakBandWidth peakBW = new PeakBandWidth();
            int num = (int) Math.ceil(numOfDays * 24 * 12 * 0.05 + 1); //（上一个月天数 * 24 * 12 * 5% 向上取整）
            if(metas.size() < num || metas == null) { //如果扫描结果数量小于（上一个月天数 * 24 * 12 * 5%），则该用户在该region的上个月95峰值带宽为0
                peakBW.pubPeakTfBW = "0";
                peakBW.pubPeakUpBW = "0";
            } else {
                List<Long> uploads = new ArrayList<>();
                List<Long> transfers = new ArrayList<>();
                for (MinutesUsageMeta meta : metas) {
                    if(meta.flowStats != null) {
                        uploads.add(meta.flowStats.flow_upload);
                        transfers.add(meta.flowStats.flow_download);
                    }
                }
                //查找第num大的数
                long peakUp = search(uploads.toArray(new Long[uploads.size()]), 0, uploads.size() - 1, num);
                long peakTrans = search(transfers.toArray(new Long[transfers.size()]), 0, transfers.size() - 1, num);
                
                double upBW = peakUp / (double)FIVE_MINUTES_SEC;
                double tfBW = peakTrans / (double)FIVE_MINUTES_SEC;
                BigDecimal dUp = new BigDecimal(Double.toString(upBW));
                BigDecimal dTf = new BigDecimal(Double.toString(tfBW));
                BigDecimal d = new BigDecimal(Integer.toString(1));
                //四舍五入保留两位小数 bytes/s 
                peakBW.pubPeakUpBW = dUp.divide(d,2,BigDecimal.ROUND_HALF_UP).toString(); 
                peakBW.pubPeakTfBW = dTf.divide(d,2,BigDecimal.ROUND_HALF_UP).toString();
            }
            return peakBW;
        }
    }
    

}
