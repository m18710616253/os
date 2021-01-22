package cn.ctyun.oos.server.conf;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashMap;

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
import common.util.HexUtils;
import common.util.JsonUtils.UnJsonable;

/**
 * 
 * BssAdapterConfig
 * 用于在内存中实例化BssAdapterServer的配置对象，并在zk中创建相应的节点保存改配置内容；
 * 
 * 
 */
public class BssAdapterConfig {
    private static final Log log = LogFactory.getLog(BssAdapterConfig.class);
    private static DSyncService dsync;
    static {
        try {
            dsync = new DSyncService(GlobalHHZConfig.getQuorumServers(),
                    GlobalHHZConfig.getSessionTimeout());
        } catch (IOException e) {
            throw new RuntimeException("Create DSyncService failed.", e);
        }
    }
    public static HashMap<String,Pool> pools;
    public static String ip;
    public static int port;
    public static String protocol;
    public static String name;
    public static Pool localPool = new Pool();
    public static int asynchronizedRetryTimes = 3;
    //线程池参数
    public static int coreThreadsNum = 50;
    public static int maxThreadsNum = 500;
    public static int queueSize = 10000;
    public static int aliveTime = 3000;
    public static int gangliaInternalTime = 30 * 1000;
    public static int connectTimeout = 30 * 1000;
    public static int readTimeout = 30 * 1000;
    public static int userRegionCacheTimeout = 60 * 1000;
    
    @UnJsonable
    private static DValue value;
    static {
        value = dsync.listen(OosZKNode.bssAdapterConfigPath, new DListener() {
            public void onChanged() {
                try {
                    synchronized (BssAdapterConfig.class) {
                        byte[] data = value.get();
                        deserialize(BssAdapterConfig.class, data);
                    }
                } catch (Throwable e) {
                    log.error(e.getMessage(), e);
                }
            }
        });
        try {
            synchronized (BssAdapterConfig.class) {
                byte[] data = value.get();
                deserialize(BssAdapterConfig.class, data);
            }
        } catch (Throwable e) {
            log.error(e.getMessage(), e);
        }
    }

    public static String getSecretKey(String ak) {
    	Pool pool = pools.get(ak);
    	if (pool != null) {
    		return pool.getSK();
    	}
    	
    	return null;
    }

    public static String getPoolName(String ak) {
    	Pool pool = pools.get(ak);
    	if (pool != null) {
    		return pool.name;
    	}
        return null;
    }

    private static byte[] serialize(Class<?> clazz) {
        JSONObject jo = new JSONObject();
        try {
            Field[] fields = clazz.getDeclaredFields();
            for (Field field : fields) {
                field.setAccessible(true);
                int m = field.getModifiers();
                if (Modifier.isFinal(m) || field.isAnnotationPresent(UnJsonable.class))
                    continue;
                if (field.getType().isPrimitive()) {
                    if (field.getInt(clazz) == -1)
                        continue;
                } else if (field.get(clazz) == null)
                    continue;
                jo.put(field.getName(), field.get(clazz));
            }
        } catch (Exception e) {
            throw new RuntimeException("Deserialize Error", e);
        }
        return jo.toString().getBytes(Consts.CS_UTF8);
    }

    public static void sync(Class<?> clazz) throws InterruptedException {
        if (clazz.isAssignableFrom(BssAdapterConfig.class))
            BssAdapterConfig.value.set(serialize(clazz));
        else
            throw new RuntimeException("No such config class.");
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
                if (field.getName().equals("pools") && jo.has("global")
                        && jo.getJSONObject("global").has("pools")) {
                    JSONArray array = jo.getJSONObject("global").getJSONArray("pools");
                    HashMap<String,Pool> tmp = new HashMap<String,Pool>();
                    for (int i = 0; i < array.length(); i++) {
                        Pool pool = new Pool();
                        pool.ak = array.getJSONObject(i).getString("ak").toLowerCase();
                        pool.setSK(array.getJSONObject(i).getString("sk"));
                        pool.name = array.getJSONObject(i).getString("name").toLowerCase();
                        pool.chName = array.getJSONObject(i).getString("chName");
                        pool.url = array.getJSONObject(i).getString("url");
                        pool.proxyPort = array.getJSONObject(i).getInt("proxyPort");
                        pool.trustedPort = array.getJSONObject(i).getInt("trustedPort");
                        pool.managementport = array.getJSONObject(i).getInt("managementport");
                        tmp.put(pool.ak, pool);
                    }
                    field.set(clazz, tmp);
                    continue;
                }
                if ((field.getName().equals("ip") || 
            		 field.getName().equals("port") || 
            		 field.getName().equals("protocol") || 
            		 field.getName().equals("name") || 
            		 field.getName().equals("coreThreadsNum") || 
            		 field.getName().equals("maxThreadsNum") || 
            		 field.getName().equals("queueSize") || 
            		 field.getName().equals("aliveTime")) && jo.has("global")) {
                    JSONObject jo2 = jo.getJSONObject("global");
                    if (jo2.has(field.getName()))
                        field.set(clazz, jo2.get(field.getName()));
                }
                if (field.getName().equals("localPool") && jo.has("local")) {
                    JSONObject jo2 = jo.getJSONObject("local");
                    Pool pool = new Pool();
                    pool.ak = jo2.getString("ak").toLowerCase();
                    pool.setSK(jo2.getString("sk"));
                    pool.name = jo2.getString("name").toLowerCase();
                    pool.chName = jo2.getString("chName");
                    pool.url = jo2.getString("url");
                    pool.proxyPort = jo2.getInt("proxyPort");
                    pool.trustedPort = jo2.getInt("trustedPort");
                    pool.managementport = jo2.getInt("managementport");
                    field.set(clazz, pool);
                }
                field.setAccessible(true);
                int m = field.getModifiers();
                if (Modifier.isFinal(m) || field.isAnnotationPresent(UnJsonable.class))
                    continue;
                if (jo.has(field.getName()))
                    field.set(clazz, jo.get(field.getName()));
            }
            log.info("The data of BucketOwnerConsistencyConfig has changed.");
            log.info("Now the data of BucketOwnerConsistencyConfig is:"
                    + System.getProperty("line.separator") + jo.toString());
        } catch (Exception e) {
            throw new RuntimeException("Deserialize Error", e);
        }
    }

    private static String toString(byte[] data, int offset, int size) {
        if (data == null || offset < 0 || size < 0 || data.length == 0)
            return null;
        return new String(data, offset, size, Consts.CS_UTF8);
    }
    
    public static int getGangliaInternalTime() {
        return gangliaInternalTime;
    }
    
    public static class Pool {
        public String ak;
        private String sk;
        public String name;
        public String chName;
        public String url;
        public int proxyPort;
        public int trustedPort;
        public int managementport;

        public String getSK() {
            if (sk == null)
                return null;
            byte[] buf = HexUtils.toByteArray(sk);
            for (int i = 0; i < buf.length / 2; i++) {
                byte tmp = buf[i];
                buf[i] = buf[buf.length - 1 - i];
                buf[buf.length - 1 - i] = tmp;
            }
            return new String(buf, Consts.CS_UTF8);
        }

        public void setSK(String sk) {
            if (sk == null)
                sk = "";
            byte[] buf = sk.getBytes(Consts.CS_UTF8);
            for (int i = 0; i < buf.length / 2; i++) {
                byte tmp = buf[i];
                buf[i] = buf[buf.length - 1 - i];
                buf[buf.length - 1 - i] = tmp;
            }
            this.sk = HexUtils.toHexString(buf);
        }
    }
    
}
