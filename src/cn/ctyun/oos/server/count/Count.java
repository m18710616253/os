package cn.ctyun.oos.server.count;

import cn.ctyun.common.Consts;
import cn.ctyun.common.Program;
import cn.ctyun.common.conf.GlobalHHZConfig;
import cn.ctyun.common.conf.OOSConfig;
import cn.ctyun.common.dsync.DLock;
import cn.ctyun.common.dsync.DSyncService;
import cn.ctyun.common.ganglia.GangliaConsts;
import cn.ctyun.common.ganglia.GangliaMan;
import cn.ctyun.common.ganglia.GangliaPlugin;
import cn.ctyun.common.node.OosZKNode;
import cn.ctyun.common.region.DataRegions;
import cn.ctyun.common.utils.LogUtils;
import cn.ctyun.oos.common.Utils;
import cn.ctyun.oos.metadata.MinutesUsageMeta;
import cn.ctyun.oos.metadata.MinutesUsageMeta.UsageMetaType;
import cn.ctyun.oos.server.db.dbpay.DBPay;
import cn.ctyun.oos.server.db.dbprice.DBPrice;
import cn.ctyun.oos.server.db.dbprice.DbUserPackage;
import cn.ctyun.oos.server.usage.UsageResult;
import cn.ctyun.oos.server.util.Misc;
import common.time.TimeUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 清理UsageCurrent数据表，更新userPackage表中的套餐用量信息。
 * 在凌晨以后运行，在运行时如果因故停掉，可重新启动，不会出现重复统计的情况。
 * 
 * -------------更新----------------
 * 使用oos-minutesUsage表数据减少用户套餐，不再使用oos-usageCurrent表
 * 根据上次减少过用量的日期开始到上次补全日期之间，减少该段时间内的用户用量
 * 
 * @author Dong chk
 *
 */
public class Count implements Program {
    static {
        System.setProperty("log4j.log.app", "count");
    }
    private static Log log = LogFactory.getLog(Count.class);
    private Calendar calendar = Calendar.getInstance();
    // count进程指标，1表示告警，0表示正常。
    private static AtomicInteger count_error = new AtomicInteger();
    private static String gangliaFakeIp = GangliaMan.getFakeIpHost(GangliaMan.OOS_GROUP_NAME);
    private static int processCount = 0;
    
    private static String today;

    public static void main(String[] args) throws Exception {
        new Count().exec(args);
    }
    
    /**
     * count_error指标发送至ganglia。
     */
    public static void send2GangliaCountStatus() {
        GangliaPlugin.sendToGangliaWithFakeIp(GangliaConsts.COUNT_ERROR,
                String.valueOf(count_error.get()), GangliaConsts.YTYPE_INT32,
                GangliaConsts.YNAME_NUM, GangliaConsts.GROUP_NAME_COUNT_ERROR,
                "\"count error.\"", gangliaFakeIp);
    }

    @Override
    public void exec(String[] args) throws Exception {
        DSyncService dsync = new DSyncService(GlobalHHZConfig.getQuorumServers(),
                GlobalHHZConfig.getSessionTimeout());
        try {
            DLock lock = dsync.createLock(OosZKNode.countLock, null);
            lock.lock();
            LogUtils.startSuccess();
            try {
                send2GangliaCountStatus();
                String day = null;
                String[] fields = OOSConfig.getCountRunTime().split(":");
                int hourOfCountRunTime = Integer.parseInt(fields[0]);
                int minuteOfCountRunTime = Integer.parseInt(fields[1]);
                while (true) {
                    Date now = new Date();
                    calendar.setTime(now);
                    String thisDay = TimeUtils.toYYYY_MM_dd(new Date());
                    if (!thisDay.equals(day)) {
                        boolean isRunTime = calendar.get(Calendar.HOUR_OF_DAY) == hourOfCountRunTime
                                && calendar.get(Calendar.MINUTE) < minuteOfCountRunTime
                                && calendar.get(Calendar.MINUTE) > minuteOfCountRunTime - 5;
                        if (day == null || isRunTime) {
                            try {
                                log.info(thisDay + " count process begin:" + now);
                                // 执行count操作
                                doCount();
                                count_error.getAndSet(0);
                                send2GangliaCountStatus();
                                log.info(thisDay + " count process end:" + new Date());
                                log.info(thisDay + " count total time:" + (System.currentTimeMillis() - now.getTime())/(1000*60) + " minutes.");
                            } catch (Throwable e) {
                                log.error(e.getMessage(), e);
                                count_error.getAndSet(1);
                                send2GangliaCountStatus();
                            }
                            day = thisDay;                            
                        }
                    }
                    try {
                        Thread.sleep(Consts.ONE_MINITE);
                    } catch (InterruptedException e) {
                        log.error(e.getMessage(),e);
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
     * 进行count操作
     * @throws Exception
     */
    public void doCount() throws Exception {
        processCount = 0;
        today = LocalDate.now().format(Misc.format_uuuu_MM_dd);
        LocalDate lastFillLocalDate = LocalDate.now().minusDays(1);
        long lastTime = UsageResult.getLastPeriodTime();
        if (lastTime != 0) {
            Instant instant = Instant.ofEpochMilli(lastTime);
            LocalDateTime ldt = LocalDateTime.ofInstant(instant, ZoneId.systemDefault());
            lastFillLocalDate = ldt.toLocalDate().minusDays(1);
        }

        long dbStart = System.currentTimeMillis();
        List<String> regions = DataRegions.getAllRegions();
        List<String> allRegions = new ArrayList<String>();
        allRegions.addAll(regions);
        allRegions.add(Consts.GLOBAL_DATA_REGION);

        DBPay dbPay = DBPay.getInstance();
        List<Long> owners = dbPay.getAllUsrIdFromUserPackage();
        log.info("select userNum spend times: " + (System.currentTimeMillis() - dbStart) + " ms.");
        updateUserPackage(allRegions, owners, lastFillLocalDate, dbPay, Consts.STORAGE_CLASS_STANDARD);
    }

    
    /**
     * 批量更新用户套餐，需要获取用户上一天global用量，从oos-minutesUsage表取出昨天所有5分钟用量，进行计算得到global的容量峰值
     * @param regions
     * @param owners
     * @return
     * @throws Exception
     */
    public void updateUserPackage(List<String> regions, List<Long> owners, LocalDate lastFillDate, DBPay dbPay, String storageType) throws Exception {
        long startUpdateUsagePackage = System.currentTimeMillis();
        log.info("update user package, count completed num: " + processCount); 
        boolean isThrowToGangliaFlag = false;
        for (Long owner : owners) {
            try {
                DBPrice dbPrice = DBPrice.getInstance();
                DbUserPackage dbUserPackage = new DbUserPackage(owner);
                dbUserPackage.isPaid = 1;
                // 这里会使用当前时间查询当前有效的套餐
                boolean existPackage = dbPrice.userPackageSelect(dbUserPackage);
                // 当前时间点不存在套餐
                if (!existPackage) {
                    // 有过期且没有终止的套餐
                    if (dbPrice.userPackageExpireCount(dbUserPackage) > 0) {
                        // 进行冻结
                        dbPay.frozenUser(owner);
                    }
                    continue;
                }           
                LocalDate updateDate = LocalDate.parse(dbUserPackage.updateTime);
                if (!lastFillDate.isAfter(updateDate)) {
                    continue;
                }       
     
                // 上一次减少到某天的用量，从该天的第二天开始
                String begin = updateDate.plusDays(1).format(Misc.format_uuuu_MM_dd);
                String end = lastFillDate.format(Misc.format_uuuu_MM_dd);

                Map<String, List<MinutesUsageMeta>> regionUsage = UsageResult.getSingleCommonUsageStatsQuery(owner, begin, end, null, Utils.BY_5_MIN, new HashSet<String>(regions), UsageMetaType.MINUTES_OWNER, storageType, true);
                // 只处理全局数据，得到所有5分钟的数据
                List<MinutesUsageMeta> ownerGlobalUsage = regionUsage.get(Consts.GLOBAL_DATA_REGION);
                MinutesUsageMeta usage = new MinutesUsageMeta(UsageMetaType.DAY_OWNER, Consts.GLOBAL_DATA_REGION, owner, today, storageType);
                usage.initStatsExceptConnection();
                if (ownerGlobalUsage == null || ownerGlobalUsage.size() == 0)
                    continue;
                long firstTimeVersion = ownerGlobalUsage.get(0).timeVersion;
                for (MinutesUsageMeta meta : ownerGlobalUsage) {
                    usage.flowStats.sumAll(meta.flowStats);
                    usage.requestStats.sumAll(meta.requestStats);
                    usage.codeRequestStats.sumAll(meta.codeRequestStats);
                    // 获取一天中最后一个5分钟的容量
                    if (meta.timeVersion > firstTimeVersion) {
                        usage.sizeStats.size_total = meta.sizeStats.size_total;
                    }
                    // 峰值容量取一天中容量峰值最大的五分钟的值
                    usage.sizeStats.size_peak = usage.sizeStats.size_peak > meta.sizeStats.size_peak
                            ? usage.sizeStats.size_peak
                            : meta.sizeStats.size_peak;
                }
                // 用量全部为0，不做处理
                if (isEmpty(usage)) {
                    continue;
                }
                
                //更新时间
                dbUserPackage.updateTime = end;
                // 更新用量信息，如果用户有套餐更新用户套餐，初始化用户的usageCurrent数据
                dbPay.updateUsage(usage, dbUserPackage);
                log.info("reduce user " + owner + " package from " + begin + " to " + end);
                processCount++;
            } catch (Exception e) {
                log.error(e.getMessage(),e);
                log.error("update user package error:" + owner);
                isThrowToGangliaFlag = true;
                
            }
        } 
        log.info("count completed num: " + processCount + " and updateuserpackage spend time is "+(System.currentTimeMillis()-startUpdateUsagePackage)); 
        if(isThrowToGangliaFlag) {
            throw new Exception("updateUserPackage has some error");
        }
       
    }

    /**
     * 判断用量数据是否全部为0
     * @param usage
     * @return
     */
    private boolean isEmpty(MinutesUsageMeta usage) {
        // 判断是否产生了用量,全部统计值是0，说明没有用量，不做处理
        return usage.sizeStats.size_total == 0 && usage.sizeStats.size_peak == 0 && usage.flowStats.flow_upload == 0 && usage.flowStats.flow_download == 0 && usage.requestStats.req_get == 0 && usage.requestStats.req_head == 0
                && usage.requestStats.req_other == 0 && usage.flowStats.flow_roamUpload == 0 && usage.flowStats.flow_roamDownload == 0 && usage.sizeStats.size_originalTotal == 0 && usage.sizeStats.size_originalPeak == 0
                && usage.requestStats.req_noNetGet == 0 && usage.requestStats.req_noNetHead == 0 && usage.requestStats.req_noNetOther == 0 && usage.flowStats.flow_noNetDownload == 0 && usage.flowStats.flow_noNetRoamDownload == 0 && usage.flowStats.flow_noNetUpload == 0
                && usage.flowStats.flow_noNetRoamUpload == 0 && usage.sizeStats.size_redundant == 0 && usage.sizeStats.size_alin == 0 && usage.requestStats.req_spam == 0 && usage.requestStats.req_pornReviewFalse == 0
                && usage.requestStats.req_pornReviewTrue == 0 ;
    }

    @Override
    public String usage() {
        return "Usage: \n";
    }
}
