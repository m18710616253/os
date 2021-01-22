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

public class CloudTrailConfig {
    /** 日志文件最大大小 */
    public static long maxTrailFileLength = 2 * 1024 * 1024;
    /** 当上传日志文件失败时，重试次数 */
    public static int trailUploadRetryTime = 3;
    /** 执行任务的线程数量 */
    public static int threadNum = 3;
    /** 每个跟踪每次取出的管理事件数量 */
    public static int manageEventListResultNum = 500;
    
    /** cloudTrailServer最大线程数 */
    private static int maxThreads = 10000;
    
    private static final Log log = LogFactory.getLog(CloudTrailConfig.class);
    private static DSyncService dsync;
    static {
        try {
            dsync = new DSyncService(GlobalHHZConfig.getQuorumServers(), GlobalHHZConfig.getSessionTimeout());
        } catch (IOException e) {
            throw new RuntimeException("Create DSyncService failed.", e);
        }
    }
    private static DValue value;
    static {
        value = dsync.listen(OosZKNode.cloudTrailConfigPath, new DListener() {
            public void onChanged() {
                try {
                    synchronized (CloudTrailConfig.class) {
                        byte[] data = value.get();
                        deserialize(CloudTrailConfig.class, data);
                    }
                } catch (Throwable e) {
                    log.error(e.getMessage(), e);
                }
            }
        });
        try {
            synchronized (CloudTrailConfig.class) {
                byte[] data = value.get();
                deserialize(CloudTrailConfig.class, data);
            }
        } catch (Throwable e) {
            log.error(e.getMessage(), e);
        }
    }

    private static void deserialize(Class<?> clazz, byte[] data) {
        if (data == null)
            return;
        try {
            JSONObject jo = new JSONObject(new String(data, Consts.CS_UTF8));
            Field[] fields = clazz.getDeclaredFields();
            for (Field field : fields) {
                field.setAccessible(true);
                int m = field.getModifiers();
                if (Modifier.isFinal(m) || field.isAnnotationPresent(UnJsonable.class))
                    continue;
                if (jo.has(field.getName())) {
                        field.set(clazz, jo.get(field.getName()));
                }
            }
            log.info("The data of CloudTrailConfig has changed.");
            log.info("Now the data of CloudTrailConfig is:" + System.getProperty("line.separator")
                    + jo.toString());
        } catch (Exception e) {
            throw new RuntimeException("Deserialize Error", e);
        }
    }
    
    public static int getMaxThreads() {
        return maxThreads;
    }
}
