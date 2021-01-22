package cn.ctyun.oos.server.conf;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONException;
import org.json.JSONObject;

import cn.ctyun.common.conf.GlobalHHZConfig;
import cn.ctyun.common.dsync.DListener;
import cn.ctyun.common.dsync.DSyncService;
import cn.ctyun.common.dsync.DValue;
import cn.ctyun.common.node.OosZKNode;
import common.util.JsonUtils.UnJsonable;

/**
 * OOS管理平台REST API配置
 * 
 */
public class OOSMgrAPIConfig {
    private static final Log log = LogFactory.getLog(OOSMgrAPIConfig.class);
    public static int maxIdleTime = 30 * 1000;
    public static int minThreads = 3000;
    public static int maxThreads = 3000;
    public static int threadPoolQueue = 3000;
    public static int requestHeaderSize = 8192;
    public static int serverPort = 8090;
    public static String baseUri = "/oos/mgr/api";
    private static DValue value;
    private static DSyncService dsync;

    static {
        try {
            dsync = new DSyncService(GlobalHHZConfig.getQuorumServers(), GlobalHHZConfig.getSessionTimeout());
        } catch (IOException e) {
            throw new RuntimeException("Create DSyncService failed.", e);
        }
    }
    
    static {
        value = dsync.listen(OosZKNode.oosMgrAPIServerConfigPath, new DListener() {
            public void onChanged() {
                try {
                    synchronized (OOSMgrAPIConfig.class) {
                        byte[] data = value.get();
                        deserialize(OOSMgrAPIConfig.class, data);
                    }
                } catch (Throwable e) {
                    log.error(e.getMessage(), e);
                }
            }
        });
        try {
            synchronized (OOSMgrAPIConfig.class) {
                byte[] data = value.get();
                deserialize(OOSMgrAPIConfig.class, data);
            }
        } catch (Throwable e) {
            log.error(e.getMessage(), e);
        }
    }

    private static void deserialize(Class<?> clazz, byte[] data) {
        if (data == null)
            return;
        try {
            String v = new String(data, 0, data.length);
            JSONObject jo = new JSONObject(v);
            Field[] fields = clazz.getDeclaredFields();
            for (Field field : fields) {
                field.setAccessible(true);
                int m = field.getModifiers();
                if (Modifier.isFinal(m)
                        || field.isAnnotationPresent(UnJsonable.class))
                    continue;
                if (jo.has(field.getName())) {
                    field.set(clazz, jo.get(field.getName()));
                }
            }
            log.info("The data of OOSMgrAPIConfig has changed.");
            log.info("Now the data of OOSMgrAPIConfig is:"
                    + System.getProperty("line.separator") + jo.toString());
        } catch (Exception e) {
            throw new RuntimeException("Deserialize Error", e);
        }
    }
    
    public static void main(String[] args) throws JSONException {
        JSONObject jo = new JSONObject();
        jo.put("maxIdleTime", OOSMgrAPIConfig.maxIdleTime);
        jo.put("minThreads", OOSMgrAPIConfig.minThreads);
        jo.put("maxThreads", OOSMgrAPIConfig.maxIdleTime);
        jo.put("threadPoolQueue", OOSMgrAPIConfig.threadPoolQueue);
        jo.put("requestHeaderSize", OOSMgrAPIConfig.threadPoolQueue);
        jo.put("serverPort", OOSMgrAPIConfig.serverPort);
        jo.put("baseUri", OOSMgrAPIConfig.baseUri);
        System.out.println(jo);
    }
    
}