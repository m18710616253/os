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
import common.util.JsonUtils.UnJsonable;

/**
 * SynchroConfig
 * 用于配置资源池同步属性：
 *      synchroLogin:       是否开启所有资源池的全局免密切换功能； 
 *      synchroUserPackage： 是否开启本地资源池用户套餐对接；
 *      synchroOrderBill:   是否开启本地资源池用户话单对接；
 *      showRegisterButton: 是否显示注册按钮;
 *      acceptBssPermission:是否接受bss发送的用户权限修改；
 */
public class SynchroConfig {
    private static final Log log = LogFactory.getLog(SynchroConfig.class);
    private static DSyncService dsync;
    static {
        try {
            dsync = new DSyncService(GlobalHHZConfig.getQuorumServers(),
                    GlobalHHZConfig.getSessionTimeout());
        } catch (IOException e) {
            throw new RuntimeException("Create DSyncService failed.", e);
        }
    }
    public static HashMap<String,SynchroProperty> propertyPools;
    public static SynchroProperty propertyLocalPool = new SynchroProperty();
    
    @UnJsonable
    private static DValue value;
    static {
        value = dsync.listen(OosZKNode.synchroConfigPath, new DListener() {
            public void onChanged() {
                try {
                    synchronized (SynchroConfig.class) {
                        byte[] data = value.get();
                        deserialize(SynchroConfig.class, data);
                    }
                } catch (Throwable e) {
                    log.error(e.getMessage(), e);
                }
            }
        });
        try {
            synchronized (SynchroConfig.class) {
                byte[] data = value.get();
                deserialize(SynchroConfig.class, data);
            }
        } catch (Throwable e) {
            log.error(e.getMessage(), e);
        }
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
        if (clazz.isAssignableFrom(SynchroConfig.class))
            SynchroConfig.value.set(serialize(clazz));
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
                if (field.getName().equals("propertyPools") && jo.has("global")
                        && jo.getJSONObject("global").has("pools")) {
                    JSONArray array = jo.getJSONObject("global").getJSONArray("pools");
                    HashMap<String,SynchroProperty> tmp = new HashMap<String,SynchroProperty>();
                    for (int i = 0; i < array.length(); i++) {
                        SynchroProperty prop = new SynchroProperty();
                        prop.name = array.getJSONObject(i).getString("name").toLowerCase();
                        prop.synchroLogin = array.getJSONObject(i).getBoolean("synchroLogin");
                        prop.synchroUserPackage = array.getJSONObject(i).getBoolean("synchroUserPackage");
                        prop.synchroOrderBill = array.getJSONObject(i).getBoolean("synchroOrderBill");
                        prop.showRegisterButton = array.getJSONObject(i).getBoolean("showRegisterButton");
                        prop.acceptBssPermission = array.getJSONObject(i).getBoolean("acceptBssPermission");
                        
                        tmp.put(prop.name, prop);
                    }
                    field.set(clazz, tmp);
                    continue;
                }
                if (field.getName().equals("propertyLocalPool") && jo.has("local")) {
                    JSONObject jo2 = jo.getJSONObject("local");
                    SynchroProperty prop = new SynchroProperty();
                    prop.name = jo2.getString("name").toLowerCase();
                    prop.synchroLogin = jo2.getBoolean("synchroLogin");
                    prop.synchroUserPackage = jo2.getBoolean("synchroUserPackage");
                    prop.synchroOrderBill = jo2.getBoolean("synchroOrderBill");
                    prop.showRegisterButton = jo2.getBoolean("showRegisterButton");
                    prop.acceptBssPermission = jo2.getBoolean("acceptBssPermission");
                    field.set(clazz, prop);
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
    
    public static SynchroProperty getPropertyByName(String name) {
        SynchroProperty prop = propertyPools.get(name);
        if (prop != null) {
            return prop;
        }
        return null;
    }

    private static String toString(byte[] data, int offset, int size) {
        if (data == null || offset < 0 || size < 0 || data.length == 0)
            return null;
        return new String(data, offset, size, Consts.CS_UTF8);
    }
    
    public static class SynchroProperty {
        public String name;
        //true:同步登陆用户session，自门户可以免密切换；false：不同步登陆用户session，自门户不显示资源池名称；
        public boolean synchroLogin;
        //true:向bss对接用户套餐信息；false:不对接套餐信息；
        public boolean synchroUserPackage;
        //true:向bss对接用户话单信息；false:不对接话单信息；
        public boolean synchroOrderBill;
        //是否显示首页注册按钮
        public boolean showRegisterButton;
        //是否接受bss发送的用户权限修改
        public boolean acceptBssPermission;
    }
    
}
