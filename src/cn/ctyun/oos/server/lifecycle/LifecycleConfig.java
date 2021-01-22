package cn.ctyun.oos.server.lifecycle;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import cn.ctyun.common.Consts;
import cn.ctyun.common.conf.DataRegion;
import cn.ctyun.common.conf.GlobalHHZConfig;
import cn.ctyun.common.conf.RegionHHZConfig;
import cn.ctyun.common.dsync.DListener;
import cn.ctyun.common.dsync.DSyncService;
import cn.ctyun.common.dsync.DValue;
import cn.ctyun.common.node.OosZKNode;
import common.util.JsonUtils.UnJsonable;

/**
 * 生命周期的配置文件
 * 
 * @author CuiMeng
 * 
 */
public class LifecycleConfig {
    //处理的超时时间
    public static int progressExpireTime = 300000;
    public static int deleteObjThreads = 1000;
    public static int transitionObjThreads = 100;
    //扫描bucket的最小时间间隔单位是ms
    public static long scanIntervalTime = 5 * 60 * 60 * 1000; 
    //MB/S
    public static int maxDeleteSpeed = 500;
    public static int maxTransitionSpeed = 500;
    //限速的间隔时间，单位是ms
    public static int intervalTime = 60000;
    //指定的特殊限速
    public static List<SpeedLimit> limits=new ArrayList<SpeedLimit>();
    //暂停任务。
    public static boolean paused = false;

    //同一bucket 如果连续失败N次则暂时跳过
    public static int skipThreshold = 1;
    
    private static final Log log = LogFactory.getLog(LifecycleConfig.class);
    private static DSyncService dsync;
    static {
        try {
            dsync = new DSyncService(RegionHHZConfig.getQuorumServers(),
                    RegionHHZConfig.getSessionTimeout());
        } catch (IOException e) {
            throw new RuntimeException("Create DSyncService failed.", e);
        }
    }
    private static DValue value;
    static {
        value = dsync.listen(OosZKNode.getLifecycleConfigPath(DataRegion.getRegion().getName()), new DListener() {
            public void onChanged() {
                try {
                    synchronized (LifecycleConfig.class) {
                        byte[] data = value.get();
                        deserialize(LifecycleConfig.class, data);
                    }
                } catch (Throwable e) {
                    log.error(e.getMessage(), e);
                }
            }
        });
        try {
            synchronized (LifecycleConfig.class) {
                byte[] data = value.get();
                deserialize(LifecycleConfig.class, data);
            }
        } catch (Throwable e) {
            log.error(e.getMessage(), e);
        }
    }

    private static void deserialize(Class<?> clazz, byte[] data) {
        if (data == null)
            return;
        try {
            String v = new String(data, 0, data.length, Consts.CS_UTF8);
            JSONObject jo = new JSONObject(v);
            if(jo.has("progressExpireTime"))
                progressExpireTime = jo.getInt("progressExpireTime");
            if(jo.has("deleteObjThreads"))
                deleteObjThreads = jo.getInt("deleteObjThreads");
            if(jo.has("transitionObjThreads"))
                transitionObjThreads = jo.getInt("transitionObjThreads");
            if(jo.has("maxDeleteSpeed"))
                maxDeleteSpeed = jo.getInt("maxDeleteSpeed");
            if(jo.has("intervalTime"))
                intervalTime = jo.getInt("intervalTime");
            if(jo.has("limits")) {
                limits.clear();
                JSONArray ja = jo.getJSONArray("limits");
                for(int i = 0; i < ja.length(); i++) {
                    JSONObject jo2 = ja.getJSONObject(i);
                    if(jo2.has("deleteSpeedLimit")) {
                        DeleteSpeedLimit limit = new DeleteSpeedLimit(jo2.getString("startTime"), 
                                jo2.getString("endTime"), jo2.getInt("deleteSpeedLimit"));
                        limits.add(limit);
                    }else if(jo2.has("transitionSpeedLimit")) {
                        TransitionSpeedLimit limit = new TransitionSpeedLimit(jo2.getString("startTime"), 
                                jo2.getString("endTime"), jo2.getInt("transitionSpeedLimit"));
                        limits.add(limit);
                    }
                }
            }
            if(jo.has("scanIntervalTime"))
                scanIntervalTime = jo.getLong("scanIntervalTime");
            if(jo.has("paused"))
                paused = jo.getBoolean("paused");
            if (jo.has("skipThreshold")) {
                skipThreshold = jo.getInt("skipThreshold");
            }
            log.info("The data of LifecycleConfig has changed.");
            log.info("Now the data of LifecycleConfig is:" + System.getProperty("line.separator")
                    + jo.toString());
        } catch (Exception e) {
            throw new RuntimeException("Deserialize Error", e);
        }
    }
    
    /**
     * 指定时间格式HH:mm:ss(如10:20:10)
     * 指定时间范围有两种情况：一种是startDate < endDate，如08:00:00 < 10:00:00；另一种是startDate > endDate，如20:00:00 > 08:00:00。
     * 第二种情况认为是跨天的，处理时需注意。
     */
    static class SpeedLimit {
        private static ThreadLocal<SimpleDateFormat> dateFormat = new ThreadLocal<SimpleDateFormat>() {
            protected SimpleDateFormat initialValue() {
                SimpleDateFormat format = new SimpleDateFormat("HH:mm:ss");
                return format;
            }
        };
        private static Date DAWN;
        static{
            try {
                DAWN = dateFormat.get().parse("23:59:59");
            } catch (ParseException e) {
            }
        }
        private Date startDate;
        private Date endDate;
        private int limit;
        
        SpeedLimit(String start, String end, int limit) {
            try {
                startDate = dateFormat.get().parse(start);
                endDate = dateFormat.get().parse(end);
            } catch (ParseException e) {
                log.error(e.getMessage(), e);
            }
            this.limit = limit;
        }
        
        /**
         * 如果当前时间在指定的时间段范围内则限速，并返回true，否则返回false
         * @param currentSpeed
         * @return
         * @throws ParseException
         */
        public boolean limit(int currentSpeed, long time) throws ParseException {
            Date now = new Date();
            now = dateFormat.get().parse(dateFormat.get().format(now));
            if((now.before(endDate) && now.after(startDate)) || (endDate.before(startDate) && ((now.after(startDate) && now.before(DAWN)) || now.before(endDate)))) {
                int more = currentSpeed - limit;
                if(more > 0)
                    try {
                        Thread.sleep(more * 1000 * time / limit);
                    } catch (InterruptedException e) {
                    }
                return true;
            }
            return false;
        }
    }
    
    static class DeleteSpeedLimit extends SpeedLimit{
        DeleteSpeedLimit(String start, String end, int limit) {
            super(start, end, limit);
        }
    }
    
    static class TransitionSpeedLimit extends SpeedLimit{
        TransitionSpeedLimit(String start, String end, int limit) {
            super(start, end, limit);
        }
    }
    
    /**
          * 暂停键
     * */
    public static void checkNeedPaused() {
        try {
            while(paused) {
                Thread.sleep(5000L);
            }
        } catch (Exception e) {
            log.warn("lifecycle paused error ", e);
        }
    }
    
    public static void main(String[] args) throws JSONException {
        JSONObject jo = new JSONObject();
        jo.put("progressExpireTime", 20000);
        jo.put("deleteObjThreads", 2000);
        jo.put("maxDeleteSpeed", 2000);
        jo.put("scanIntervalTime", 100000);
        JSONArray ja = new JSONArray();
        JSONObject limitJO = new JSONObject();
        limitJO.put("startTime", "08:00:00");
        limitJO.put("endTime", "10:00:00");
        limitJO.put("deleteSpeedLimit", "2000");
        ja.put(limitJO);
        limitJO = new JSONObject();
        limitJO.put("startTime", "18:00:00");
        limitJO.put("endTime", "22:00:00");
        limitJO.put("deleteSpeedLimit", "4000");
        ja.put(limitJO);
        jo.put("limits", ja);
        System.out.println(jo);
    }
}
