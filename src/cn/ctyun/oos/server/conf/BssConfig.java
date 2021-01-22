package cn.ctyun.oos.server.conf;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONObject;

import cn.ctyun.common.Consts;
import cn.ctyun.common.conf.GlobalHHZConfig;
import cn.ctyun.common.dsync.DListener;
import cn.ctyun.common.dsync.DSyncService;
import cn.ctyun.common.dsync.DValue;
import cn.ctyun.common.node.OosZKNode;
import common.util.JsonUtils.UnJsonable;

/**
 * bss sender和collector之间的各项配置
 * 
 */
public class BssConfig {
    public static String tempFilePath = "/home/appuser/tmp";
    public static String orderBillUploadType = "ftp";
    public static String userPackageUploadType = "ftp";
    
    //以下是话单文件对接参数
    public static String uploadHost = "";
    public static int    uploadPort = 80;
    public static String uploadBucket = "";
    public static String ak = "";
    public static String sk = "";
    
    public static String host = "192.168.13.201";
    public static int    port = 21;
    public static String userName = "ctyun_oos";
    public static String password = "ctyun_oos";
    public static String path = "/ftp_dir";
    
    //以下是套餐文件对接参数
    public static String up_uploadHost = "";
    public static int    up_uploadPort = 80;
    public static String up_uploadBucket = "";
    public static String up_ak = "";
    public static String up_sk = "";
    
    public static String up_host = "192.168.13.201";
    public static int    up_port = 21;
    public static String up_userName = "ctyun_oos";
    public static String up_password = "ctyun_oos";
    public static String up_path = "/ftp_dir";    
    
    public static String orderBillSenderStartTime = "0130";
    public static String orderBillCollectorStartTime = "0230";
    public static String userPackageSenderStartTime = "0130";
    public static String userPackageCollectorStartTime = "0230";
    
    //需要对接的region名称，多region时以逗号分隔；
    public static String orderBillRegions = "ShenYang,ChengDu,WuLuMuQi,LanZhou,QingDao,GuiYang,LaSa,WuHu";
    
    private static final Log log = LogFactory.getLog(BssConfig.class);
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
        value = dsync.listen(OosZKNode.bssConfigPath, new DListener() {
            public void onChanged() {
                try {
                    synchronized (BssConfig.class) {
                        byte[] data = value.get();
                        deserialize(BssConfig.class, data);
                    }
                } catch (Throwable e) {
                    log.error(e.getMessage(), e);
                }
            }
        });
        try {
            synchronized (BssConfig.class) {
                byte[] data = value.get();
                deserialize(BssConfig.class, data);
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
            log.info("The data of BssInterfaceConfig has changed.");
            log.info("Now the data of BssInterfaceConfig is:" + System.getProperty("line.separator")
                    + jo.toString());
        } catch (Exception e) {
            throw new RuntimeException("Deserialize Error", e);
        }
    }
}