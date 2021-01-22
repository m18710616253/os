package cn.ctyun.oos.server.conf;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONObject;

import cn.ctyun.common.Consts;
import cn.ctyun.common.conf.DataRegion;
import cn.ctyun.common.conf.RegionHHZConfig;
import cn.ctyun.common.dsync.DListener;
import cn.ctyun.common.dsync.DSyncService;
import cn.ctyun.common.dsync.DValue;
import cn.ctyun.common.node.OosZKNode;
import common.util.HexUtils;
import common.util.JsonUtils.UnJsonable;

/**
 * 内容安全的配置文件，包括图片鉴黄、文本反垃圾服务
 * @author WangJing
 *
 */
public class ContentSecurityConfig {
    /** 产品密钥ID，产品标识 */
    public static String secretIdNeteaseDun = "74a3f2ccf226c13b3bc10539c4b190a8";
    /** 产品私有密钥，服务端生成签名信息使用，请严格保管，避免泄露 */
    public static String secretKeyNeteaseDun = "3638653730323633356233396232623332663963353638303963653035626235";
    /** 业务ID，易盾根据产品业务特点分配 */
    public static String imageBusinessIdNeteaseDun = "c4a0f945121d2df6aa7ceba2c6443b8a";
    /** 易盾反垃圾云服务文本在线检测 业务ID */
    public static String textBusinessIdNeteaseDun = "f2e457b7a913c0de7788015dcabd409f";
    /** 易盾反垃圾云服务图片在线检测接口地址 */
    public static String imageApiUrlNeteaseDun = "https://as.dun.163yun.com/v3/image/check";
    /** 易盾反垃圾云服务文本在线检测接口地址 */
    public static String textApiUrlNeteaseDun = "https://as.dun.163yun.com/v3/text/check";
    public static int socketTimeout = 10000;
    public static int connectTimeout = 10000;
    public static int connectionRequestTimeout = 10000;
    /** 图片大小大于此值的进行鉴黄前先做压缩处理 */
    public static long imageResizeThreshold = 1L*1024*1024;
    /** 图片压缩处理参数 */
    public static String resizeParam = "256h_256w";

    private static final Log log = LogFactory.getLog(ContentSecurityConfig.class);
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
        value = dsync.listen(OosZKNode.getContentSecurityConfigPath(DataRegion.getRegion().getName()), new DListener() {
            public void onChanged() {
                try {
                    synchronized (ContentSecurityConfig.class) {
                        byte[] data = value.get();
                        deserialize(ContentSecurityConfig.class, data);
                    }
                } catch (Throwable e) {
                    log.error(e.getMessage(), e);
                }
            }
        });
        try {
            synchronized (ContentSecurityConfig.class) {
                byte[] data = value.get();
                deserialize(ContentSecurityConfig.class, data);
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
                if (jo.has(field.getName())) {
                    if (field.getType().isArray()) {
                        String[] tmp = jo.get(field.getName()).toString().split(",");
                        field.set(clazz, tmp);
                    } else
                        field.set(clazz, jo.get(field.getName()));
                }
            }
            log.info("The data of ContentSecurityConfig has changed.");
            log.info("Now the data of ContentSecurityConfig is:" + System.getProperty("line.separator")
                    + jo.toString());
        } catch (Exception e) {
            throw new RuntimeException("Deserialize Error", e);
        }
    }
    
    public static String getSecretKey() {
        if (secretKeyNeteaseDun == null)
            return null;
        byte[] buf = HexUtils.toByteArray(secretKeyNeteaseDun);
        for (int i = 0; i < buf.length / 2; i++) {
            byte tmp = buf[i];
            buf[i] = buf[buf.length - 1 - i];
            buf[buf.length - 1 - i] = tmp;
        }
        return new String(buf, Consts.CS_UTF8);
    }
}
