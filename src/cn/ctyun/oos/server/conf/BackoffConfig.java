package cn.ctyun.oos.server.conf;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.Consts;
import org.json.JSONArray;
import org.json.JSONObject;

import com.google.common.collect.Lists;

import cn.ctyun.common.conf.DataRegion;
import cn.ctyun.common.conf.RegionHHZConfig;
import cn.ctyun.common.dsync.DListener;
import cn.ctyun.common.dsync.DSyncService;
import cn.ctyun.common.dsync.DValue;
import cn.ctyun.common.node.OosZKNode;
import common.util.JsonUtils.UnJsonable;

/**
 * Adapter需要的配置，
 * 
 * @author CuiMeng
 * 
 */
public class BackoffConfig {
    private static final Log log = LogFactory.getLog(BackoffConfig.class);
    private static DSyncService dsync;
    static {
        try {
            dsync = new DSyncService(RegionHHZConfig.getQuorumServers(),
                    RegionHHZConfig.getSessionTimeout());
        } catch (IOException e) {
            throw new RuntimeException("Create DSyncService failed.", e);
        }
    }

    public static int getCheckInterval() {
        return checkInterval;
    }

    public static int getBackoffMaxConnections() {
        return backoffMaxConnections;
    }

    public static int getBackoffPublicPutMaxConnections() {
        return backoffPublicPutMaxConnections;
    }

    public static int getBackoffPrivatePutMaxConnections() {
        return backoffPrivatePutMaxConnections;
    }

    public static int getBackoffPublicGetMaxConnections() {
        return backoffPublicGetMaxConnections;
    }

    public static int getBackoffPrivateGetMaxConnections() {
        return backoffPrivateGetMaxConnections;
    }
    
    public static int getBackoffPublicDeleteMaxConnections() {
		return backoffPublicDeleteMaxConnections;
	}

	public static int getBackoffPrivateDeleteMaxConnections() {
		return backoffPrivateDeleteMaxConnections;
	}

	public static String getPrivateIp() {
        return privateIp;
    }

    public static int getBackoffBandwidthPublicRxkB() {
        return backoffBandwidthPublicRxkB;
    }

    public static int getBackoffBandwidthPublicTxkB() {
        return backoffBandwidthPublicTxkB;
    }

    public static int getBackoffBandwidthPrivateRxkB() {
        return backoffBandwidthPrivateRxkB;
    }

    public static int getBackoffBandwidthPrivateTxkB() {
        return backoffBandwidthPrivateTxkB;
    }

    public static String getReadPublicEth() {
        return readPublicEth;
    }

    public static String getWritePublicEth() {
        return writePublicEth;
    }

    public static String getReadPrivateEth() {
        return readPrivateEth;
    }

    public static String getWritePrivateEth() {
        return writePrivateEth;
    }

    public static String getBandwidthFile() {
        return bandwidthFile;
    }

    public static int getBandwidthBackoff() {
        return bandwidthBackoff;
    }

    public static long getMaxOutgoingBytes() {
        return maxOutgoingBytes;
    }

    public static long getMaxIncomingBytes() {
        return maxIncomingBytes;
    }

    public static int getBackoffHeapSize() {
        return backoffHeapSize;
    }

    public static long getObjectBackoffExpiredTime() {
        return objectBackoffExpiredTime;
    }

    public static List<String> getWhiteList() {
        return whiteList;
    }

    public static int getEnableStreamSlowDown() {
        return enableStreamSlowDown;
    }

    public static boolean isEnableStreamSlowDown() {
        return (getEnableStreamSlowDown() == 1) ? true : false;
    }

    public static long getMaxStreamPauseTime() {
        return maxStreamSlowDownPauseTime;
    }
    
    public static Map<String, Map<String, UserLimit>> getUserMaxConnections(){
        return userMaxConnections;
    }
    
    public static Map<String, Map<String, UserLimit>> getUserMaxBandWidth(){
        return userMaxBandwidth;
    }

    private static void deserialize(Class<?> clazz, byte[] data) {
        if (data == null)
            return;
        try {
            String v = toString(data, 0, data.length);
            if (v == null)
                return;
            JSONObject jo = new JSONObject(v);
            Field[] fields = clazz.getDeclaredFields();
            for (Field field : fields) {
                field.setAccessible(true);
                int m = field.getModifiers();
                if (Modifier.isFinal(m) || field.isAnnotationPresent(UnJsonable.class))
                    continue;
                if (jo.has(field.getName())) {
                    if (field.getName().equals("whiteList"))
                        field.set(clazz,
                                Lists.newArrayList(jo.getString(field.getName()).split(",")));
                    else if(field.getName().equals(USER_MAX_CONNECTIONS) && jo.has(USER_MAX_CONNECTIONS)) {
                        JSONArray ja = jo.getJSONArray(USER_MAX_CONNECTIONS);
                        //将配置在更新的时候就整理好，使用时可以减少查找负责度和节省执行时间
                        Map<String, Map<String, UserLimit>> map = new HashMap<String, Map<String, UserLimit>>();
                        for(int i = 0; i < ja.length(); i++) {
                            try {
                                JSONObject jo2 = ja.getJSONObject(i);
                                if(jo2.has("maxPutConnections")) {
                                    configUserLimitMap("PUT", "maxPutConnections", map, jo2);
                                } 
                                if(jo2.has("maxGetConnections")) {
                                    configUserLimitMap("GET", "maxGetConnections", map, jo2);
                                }
                                if(jo2.has("maxDeleteConnections")) {
                                    configUserLimitMap("DELETE", "maxDeleteConnections", map, jo2);
                                }
                            } catch (Throwable e) {
                                //此处异常该如何处理？ 如果配置文件有错误，如何得知？
                                log.error("BackoffConfig Deserialize Error", e);
                            }
                        }
                        userMaxConnections = map;
                    }else if(field.getName().equals(USER_MAX_BAND_WIDTH) && jo.has(USER_MAX_BAND_WIDTH)) {
                        JSONArray ja = jo.getJSONArray(USER_MAX_BAND_WIDTH);
                        Map<String, Map<String, UserLimit>> map = new HashMap<String, Map<String, UserLimit>>();
                        for(int i = 0; i < ja.length(); i++) {
                            try {
                                JSONObject jo2 = ja.getJSONObject(i);
                                if(jo2.has("maxPutBandwidth")) {
                                    configUserLimitMap("PUT", "maxPutBandwidth", map, jo2);
                                } 
                                if(jo2.has("maxGetBandwidth")) {
                                    configUserLimitMap("GET", "maxGetBandwidth", map, jo2);
                                }
                            } catch (Throwable e) {
                                //此处异常该如何处理？ 如果配置文件有错误，如何得知？
                                log.error("BackoffConfig Deserialize Error", e);
                            }
                        }
                        userMaxBandwidth = map;
                    } else 
                        field.set(clazz, jo.get(field.getName()));
                }
            }
            log.info("The data of BackoffConfig has changed.");
            log.info("Now the data of BackoffConfig is:" + System.getProperty("line.separator")
                    + jo.toString());
        } catch (Exception e) {
            throw new RuntimeException("Deserialize Error", e);
        }
    }
    
    private static void configUserLimitMap(String httpType, String configType, 
            Map<String, Map<String, UserLimit>> map, JSONObject jo2) throws Throwable{
        if(null == map.get(httpType))
            map.put(httpType, new HashMap<String, UserLimit>());
        UserLimit connection = new UserLimit(jo2.getString("ownerName"), Integer.valueOf(jo2.getString(configType)));
        map.get(httpType).put(jo2.getString("ownerName"), connection);
    }

    /**
     * @param data
     * @param offset
     * @param size
     * @return
     * @throws UnsupportedEncodingException
     */
    private static String toString(byte[] data, int offset, int size) {
        if (data == null || offset < 0 || size < 0 || data.length == 0)
            return null;
        return new String(data, offset, size, Consts.UTF_8);
    }

    // backoff
    private static int backoffMaxConnections = 10000;
    private static int backoffPublicPutMaxConnections = 5000;
    private static int backoffPublicGetMaxConnections = 5000;
    private static int backoffPublicDeleteMaxConnections = 5000;
    private static int backoffPrivatePutMaxConnections = 5000;
    private static int backoffPrivateGetMaxConnections = 5000;
    private static int backoffPrivateDeleteMaxConnections = 5000;
    private static int backoffBandwidthPublicRxkB = 100000;
    private static int backoffBandwidthPublicTxkB = 100000;
    private static int backoffBandwidthPrivateRxkB = 100000;
    private static int backoffBandwidthPrivateTxkB = 100000;
    private static int checkInterval = 3000;
    private static String privateIp = "127.0.0.1/24";
    private static String readPublicEth = "bond0";
    private static String writePublicEth = "bond0";
    private static String readPrivateEth = "bond0";
    private static String writePrivateEth = "bond0";
    private static String bandwidthFile = "/proc/net/dev";
    private static int bandwidthBackoff = 0;
    private static long maxOutgoingBytes = 10000; // 最大出口带宽
    private static long maxIncomingBytes = 100000; // 最大入口带宽
    private static int backoffHeapSize = 100; // backoff堆的大小
    private static long objectBackoffExpiredTime = 60000; // 对象访问的过期时间
    public static List<String> whiteList = new ArrayList<String>();
    private static int enableStreamSlowDown = 0;
    private static long maxStreamSlowDownPauseTime = 5000;
    /** Map<String: 请求类型、PUT、DELETE、GET，Map<String: username, UserLimit 限制元素>> */
    private static Map<String, Map<String, UserLimit>> userMaxConnections = 
            new HashMap<String, Map<String, UserLimit>>();
    //Map<String: 请求类型、PUT、DELETE、GET，Map<String: username, UserLimit 限制元素>>
    private static Map<String, Map<String, UserLimit>> userMaxBandwidth = 
            new HashMap<String, Map<String, UserLimit>>();
    private static final String USER_MAX_CONNECTIONS = "userMaxConnections";
    private static final String USER_MAX_BAND_WIDTH = "userMaxBandwidth";
    
    @UnJsonable
    private static DValue value;
    static {
        value = dsync.listen(OosZKNode.getBackoffConfigPath(DataRegion.getRegion().getName()),
                new DListener() {
                    public void onChanged() {
                        try {
                            synchronized (BackoffConfig.class) {
                                byte[] data = value.get();
                                deserialize(BackoffConfig.class, data);
                            }
                        } catch (Throwable e) {
                            log.error(e.getMessage(), e);
                        }
                    }
                });
        try {
            synchronized (BackoffConfig.class) {
                byte[] data = value.get();
                deserialize(BackoffConfig.class, data);
            }
        } catch (Throwable e) {
            log.error(e.getMessage(), e);
        }
    }
    
    public static class UserLimit {
        String userName;
        long limitElement;
        
        public UserLimit(String userName, long limitElement) {
            this.userName = userName;
            this.limitElement = limitElement;
        }
        
        public String getUserName() {
            return userName;
        }
        public long getLimitElement() {
            return limitElement;
        }
    }
}
