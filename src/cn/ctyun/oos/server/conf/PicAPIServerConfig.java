package cn.ctyun.oos.server.conf;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;

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
 * 资源信息查询API的配置文件
 * 
 * @author WangJing
 * 
 */
public class PicAPIServerConfig {
    private static final Log log = LogFactory.getLog(PicAPIServerConfig.class);
    //全表扫描时间间隔限制
    private static int scanFullTableInterval = 30;
    private static DSyncService dsync;
    private static DValue value;
    static {
        try {
            dsync = new DSyncService(GlobalHHZConfig.getQuorumServers(),
                    GlobalHHZConfig.getSessionTimeout());
            value = dsync.listen(OosZKNode.picAPIServerConfigPath,
                    new DListener() {
                        @Override
                        public void onChanged() {
                            try {
                                synchronized (PicAPIServerConfig.class) {
                                    byte[] data = value.get();
                                    deserialize(PicAPIServerConfig.class, data);
                                }
                            } catch (Throwable e) {
                                log.error(e.getMessage(), e);
                            }
                        }
                    });
            try {
                synchronized (PicAPIServerConfig.class) {
                    byte[] data = value.get();
                    deserialize(PicAPIServerConfig.class, data);
                }
            } catch (Throwable e) {
                log.error(e.getMessage(), e);
            }
        } catch (IOException e) {
            throw new RuntimeException("Create DSyncService failed.", e);
        }
    }

    private static List<AKSK> aksks;

    public static List<AKSK> getReplicas() {
        return aksks;
    }
    
    /**
     * 获取SK
     * @param ak
     * @return
     */
    public static String getSecretKey(String ak){
        for (int i = 0; i < aksks.size(); i++) {
            if (aksks.get(i).ak.equalsIgnoreCase(ak)) {
                return aksks.get(i).getSecretKey();
            }
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
        if (clazz.isAssignableFrom(PicAPIServerConfig.class))
            PicAPIServerConfig.value.set(serialize(clazz));
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
            //处理普通成员变量
            Field[] fields = clazz.getDeclaredFields();
            for (Field field : fields) {
                if (field.getName().equals("aksks")) continue;
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
            //处理aksk内容
            if (jo.has("aksks")) {
                JSONArray array = (JSONArray) jo.get("aksks");
                ArrayList<AKSK> tmp = new ArrayList<AKSK>();
                for (int i = 0; i < array.length(); i++) {
                    AKSK aksk = new AKSK();
                    aksk.ak = array.getJSONObject(i).getString("ak");
                    aksk.sk = array.getJSONObject(i).getString("sk");
                    aksk.comment = array.getJSONObject(i).getString("comment");
                    tmp.add(aksk);
                }
                aksks = tmp;
            }
            log.info("The data of PicAPIServerConfig has changed.");
            log.info("Now the data of PicAPIServerConfig is:" + System.getProperty("line.separator")
                    + jo.toString());
        } catch (Exception e) {
            throw new RuntimeException("Deserialize Error", e);
        }
    }
    
    public static int getScanFullTableInterval() {
        return scanFullTableInterval;
    }

    private static String toString(byte[] data, int offset, int size) {
        if (data == null || offset < 0 || size < 0 || data.length == 0)
            return null;
        return new String(data, offset, size, Consts.CS_UTF8);
    }
    public static class AKSK {
        public String ak;
        public String sk;
        public String comment;
        
        public String getSecretKey() {
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

        public void setSecretKey(String sk) {
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
