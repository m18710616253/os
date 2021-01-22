package cn.ctyun.oos.server.conf;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;

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
import common.tuple.Pair;
import common.util.JsonUtils.UnJsonable;

/**
 * 查询容量工具的配置文件
 * 
 * @author CuiMeng
 * 
 */
public class QueryStorageConfig {
    public static ArrayList<Pair<String, String>> users = new ArrayList<Pair<String, String>>();
    public static String runtime = "1:5";
    public static String manageAPIIP;
    public static int manageAPIPort;
    private static final Log log = LogFactory.getLog(QueryStorageConfig.class);
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
        value = dsync.listen(OosZKNode.queryStorageConfigPath, new DListener() {
            public void onChanged() {
                try {
                    synchronized (QueryStorageConfig.class) {
                        byte[] data = value.get();
                        deserialize(QueryStorageConfig.class, data);
                    }
                } catch (Throwable e) {
                    log.error(e.getMessage(), e);
                }
            }
        });
        try {
            synchronized (QueryStorageConfig.class) {
                byte[] data = value.get();
                deserialize(QueryStorageConfig.class, data);
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
            Field[] fields = clazz.getDeclaredFields();
            for (Field field : fields) {
                field.setAccessible(true);
                int m = field.getModifiers();
                if (Modifier.isFinal(m) || field.isAnnotationPresent(UnJsonable.class))
                    continue;
                if (field.getName().equals("users")) {
                    JSONArray array = jo.getJSONArray("users");
                    ArrayList<Pair<String, String>> tmp = new ArrayList<Pair<String, String>>();
                    for (int i = 0; i < array.length(); i++) {
                        Pair<String, String> user = new Pair<String, String>();
                        user.first(array.getJSONObject(i).getString("ak"));
                        user.second(array.getJSONObject(i).getString("bucket"));
                        tmp.add(user);
                    }
                    field.set(clazz, tmp);
                    continue;
                }
                if (jo.has(field.getName())) {
                    if (field.getType().isArray()) {
                        String[] tmp = jo.get(field.getName()).toString().split(",");
                        field.set(clazz, tmp);
                    } else
                        field.set(clazz, jo.get(field.getName()));
                }
            }
            log.info("The data of QueryStorageConfig has changed.");
            log.info("Now the data of QueryStorageConfig is:" + System.getProperty("line.separator")
                    + jo.toString());
        } catch (Exception e) {
            throw new RuntimeException("Deserialize Error", e);
        }
    }
}
