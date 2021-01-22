package cn.ctyun.oos.server.conf;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONArray;
import org.json.JSONObject;

import cn.ctyun.common.Consts;
import cn.ctyun.common.conf.GlobalHHZConfig;
import cn.ctyun.common.dsync.DListener;
import cn.ctyun.common.dsync.DSyncService;
import cn.ctyun.common.dsync.DValue;
import cn.ctyun.common.node.OosZKNode;
import common.util.JsonUtils.UnJsonable;

/**
 * 周期性扫描统计进程的配置文件
 * 
 * @author WangJing
 *
 */
public class PeriodUsageStatsConfig {
    // 每隔interval毫秒统计一次计量数据,默认为每10分钟统计一次
    public static long periodUsageStatsInterval = 600000;
    // 每次只统计usageStatsTimeAgo毫秒以前的，默认统计5分钟以前的
    public static long usageStatsTimeAgo = 300000;
    // 统计服务第一版上线日期，该日期以前的线上bucket不支持bucket级别容量相关查询
    public static String deployDate;
    // 新版统计上线日期，即修改表结构后的上线日期，需根据该日期判断数据域查询
    public static String updateDate;
    // 用于循环查询最后一条统计数据时间间隔，UsageResult.lastMinutesUsageCommonStats
    public static int intervalDays = 30;
    // 是否开启实时统计，默认不开启，以提高页面响应时间
    public static boolean realTimeStatsEnabled = false;
    // 补充5分钟之内的每秒连接数随机数的波动比率,默认为0.1,即10%
    public static double ratio = 0.1;
    public static RegionBandwidth[] regionBandwidths;
    // 每个月统计带宽95峰值时间，默认在北京时间每月1日08：30（UTC时间每月1日0点30分）之后08：35之前开始执行
    public static String peakBWStatsTime = "1:8:35"; //date:hour:minute
    // 是否开启启动执行，如果为true，则在运行时直接执行一次，以后到时间执行；如果为false，则只在到时间时执行
    public static boolean peakBWStatsIniEnabled = false;
    // 需要计算带宽95峰值的用户及其数据域
    public static Map<String, List<String>> peakBWPermitOwnerWithRegions = new HashMap<>();
    // 重启后统计scan的starttime
    public static long lastStatsTime;
    // 最大失败重试次数
    public static int maxRetryTimes = 1;
    // 需要清空daymeta缓存的region，为了减少重启次数
    public static List<String> regionClearCache = new ArrayList<String>();

    //配置已经升级region统计进程的regionName
    public static List<String> regionNames = new ArrayList<String>();
    
    private static final Log log = LogFactory.getLog(PeriodUsageStatsConfig.class);
    private static DSyncService dsync;
    static {
        try {
            dsync = new DSyncService(GlobalHHZConfig.getQuorumServers(),
                    GlobalHHZConfig.getSessionTimeout());
        } catch (IOException e) {
            throw new RuntimeException("Create DSyncService failed.", e);
        }
    }
    private static DValue value;
    static {
        value = dsync.listen(OosZKNode.periodUsageStatsConfigPath, new DListener() {
            public void onChanged() {
                try {
                    synchronized (PeriodUsageStatsConfig.class) {
                        byte[] data = value.get();
                        deserialize(PeriodUsageStatsConfig.class, data);
                    }
                } catch (Throwable e) {
                    log.error(e.getMessage(), e);
                }
            }
        });
        try {
            synchronized (PeriodUsageStatsConfig.class) {
                byte[] data = value.get();
                deserialize(PeriodUsageStatsConfig.class, data);
            }
        } catch (Throwable e) {
            log.error(e.getMessage(), e);
        }
    }
    
    public static RegionBandwidth getRegionBandwidth(String regionName) {
        for (RegionBandwidth region:regionBandwidths) {
            if (region.regionName.equals(regionName)) {
                return region;
            }
        }
        return null;
    }
    
    public static class RegionBandwidth {
        public String regionName = "";
        public long pubOutBandwidth = 0;
        public long pubInBandwidth = 0;
        public long priOutBandwidth = 0;
        public long priInBandwidth = 0;


        public RegionBandwidth(String regionName, long pubOutBandwidth, long pubInBandwidth, long priOutBandwidth, long priInBandwidth) {
            this.regionName = regionName;
            this.pubOutBandwidth = pubOutBandwidth;
            this.pubInBandwidth = pubInBandwidth;
            this.priOutBandwidth = priOutBandwidth;
            this.priInBandwidth = priInBandwidth;
        }

        public RegionBandwidth() {
        }

        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("regionName=").append(regionName).append("\n")
                    .append("pubOutBandwidth=").append(String.valueOf(pubOutBandwidth)).append("\n")
                    .append("pubInBandwidth=").append(String.valueOf(pubInBandwidth)).append("\n")
                    .append("priOutBandwidth=").append(String.valueOf(priOutBandwidth)).append("\n")
                    .append("priInBandwidth=").append(String.valueOf(priInBandwidth));
            return sb.toString();
        }
    }

    private static void deserialize(Class<?> clazz, byte[] data) {
        if (data == null)
            return;
        try {
            JSONObject jo = new JSONObject(new String(data, Consts.CS_UTF8));
            if (jo.has("region2Bandwidth")) {
                JSONArray region2AvailBandwidth = (JSONArray) jo.get("region2Bandwidth");
                regionBandwidths = new RegionBandwidth[region2AvailBandwidth.length()];
                for (int i = 0; i < region2AvailBandwidth.length(); i++) {
                    RegionBandwidth node = new RegionBandwidth();
                    node.regionName = region2AvailBandwidth.getJSONObject(i).getString("regionName");
                    node.pubOutBandwidth = region2AvailBandwidth.getJSONObject(i).getLong("pubOutBandwidth");
                    node.pubInBandwidth = region2AvailBandwidth.getJSONObject(i).getLong("pubInBandwidth");
                    node.priOutBandwidth = region2AvailBandwidth.getJSONObject(i).getLong("priOutBandwidth");
                    node.priInBandwidth = region2AvailBandwidth.getJSONObject(i).getLong("priInBandwidth");
                    regionBandwidths[i] = node;
                }
            } else {
                log.error("periodUsageStatsConfig has no region2AvailBandwidth item. Please config it!");
            }
            //配置文件中配置了需要计算带宽95峰值的用户及其region
            if (jo.has("peakBWPermitOwner")) {
                JSONArray peakBWPermitOwners = (JSONArray) jo.get("peakBWPermitOwner");
                for (int i = 0; i < peakBWPermitOwners.length(); i++) {
                    String owner = peakBWPermitOwners.getJSONObject(i).getString("owner");
                    List<String> regions = new ArrayList<String>();
                    JSONArray regionJson = (JSONArray) peakBWPermitOwners.getJSONObject(i).get("regions");
                    for (int j = 0; j < regionJson.length(); j++) {
                        regions.add(regionJson.getString(j));
                    }
                    peakBWPermitOwnerWithRegions.put(owner, regions);
                }
            } else {
                log.info("PeriodUsageStatsConfig has no peakBWPermitOwner item.");
            }
            
            regionClearCache.clear();
            if (jo.has("regionNeedClearCache")) {
                JSONArray regions = (JSONArray) jo.get("regionNeedClearCache");
                for (int i = 0; i < regions.length(); i++) {                    
                    regionClearCache.add(regions.getString(i));
                }
            } else {
                log.info("PeriodUsageStatsConfig has no regionNeedClearCache item.");
            }
            
            regionNames.clear();
            if (jo.has("regions")) {
                JSONArray regions = (JSONArray) jo.get("regions");
                for (int i = 0; i < regions.length(); i++) {                    
                    regionNames.add(regions.getString(i));
                }
            } else {
                log.info("PeriodUsageStatsConfig has no regions item.");
            }
            Field[] fields = clazz.getDeclaredFields();
            for (Field field : fields) {
                field.setAccessible(true);
                int m = field.getModifiers();
                if (Modifier.isFinal(m) || field.isAnnotationPresent(UnJsonable.class))
                    continue;
                if (jo.has(field.getName())) {
                    if (field.getType().isArray()) {
                        String[] tmp = jo.get(field.getName()).toString().split(",");
                        field.set(clazz, tmp);
                    } else
                        field.set(clazz, jo.get(field.getName()));
                }
            }
            log.info("The data of PeriodUsageStatsConfig has changed.");
            log.info("Now the data of PeriodUsageStatsConfig is:" + System.getProperty("line.separator")
                    + jo.toString());
        } catch (Exception e) {
            throw new RuntimeException("Deserialize Error", e);
        }
    }
}
