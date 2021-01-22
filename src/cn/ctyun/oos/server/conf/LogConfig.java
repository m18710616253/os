package cn.ctyun.oos.server.conf;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.Consts;
import org.json.JSONArray;
import org.json.JSONObject;

import cn.ctyun.common.conf.DataRegion;
import cn.ctyun.common.conf.RegionHHZConfig;
import cn.ctyun.common.dsync.DListener;
import cn.ctyun.common.dsync.DSyncService;
import cn.ctyun.common.dsync.DValue;
import cn.ctyun.common.node.OosZKNode;
import common.util.JsonUtils.UnJsonable;

public class LogConfig {
    private static Log log = LogFactory.getLog(LogConfig.class);
    @UnJsonable
    private static DSyncService dsync;
    static {
        try {
            dsync = new DSyncService(RegionHHZConfig.getQuorumServers(),
                    RegionHHZConfig.getSessionTimeout());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static class AdapterEntry { // 变更了类属性怎么办？
        public String host = "";
        public int port;
        public String user = "";
        public String passwd = "";

        public AdapterEntry(String host, int port, String user, String passwd) {
            this.host = host;
            this.port = port;
            this.user = user;
            this.passwd = passwd;
        }

        public AdapterEntry() {
        }

        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("host=").append(host).append("\n").append("port=").append(port).append("\n")
                    .append("user=").append(user).append("\n").append("passwd=").append(passwd);
            return sb.toString();
        }
    }
    
    public static class LogServerNode {
        public int id;
        public String hostName = "";       
        public AdapterEntry[] adapterEntries;

        public LogServerNode(int id, String hostName, AdapterEntry[] adapterEntries) {
            this.id = id;
            this.hostName = hostName;
            this.adapterEntries = adapterEntries;
        }

        public LogServerNode() {
        }

        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("id=").append(id).append("\n").append("hostName=").append(hostName).append("\n")
                    .append("adapterEntries=").append(adapterEntries);
            return sb.toString();
        }
    }

    public static AdapterEntry[] getAdapterEntries(String hostName) {
        for (LogServerNode node : logServerNodes) {
            if (node.hostName.equals(hostName)) {
                return node.adapterEntries;
            }
        }
        return null;
    }
    
    public static int getId(String hostName) {
        for (LogServerNode node : logServerNodes) {
            if (node.hostName.equals(hostName)) {
                return node.id;
            }
        }
        return 0;
    }

    public static String getDstLocation() {
        return dstLocation;
    }

    public static String getPostProcessDir() {
        return postProcessDir;
    }

    public static String getArchiveDir() {
        return archiveDir;
    }

    public static int getFetchPriod() {
        return fetchPriod;
    }

    public static int getProcessPriod() {
        return processPriod;
    }

    public static int getProcessReSendPriod() {
        return processReSendPriod;
    }

    /** 单位秒 */
    public static int getArchivePriod() {
        return archivePriod;
    }
    
    public static int getArchiveBatchNum() {
        return archiveBatchNum;
    }
    
    public static int getArchiveCoreThreads() {
        return archiveCoreThreads;
    }
    
    public static int getArchiveMaxThreads() {
        return archiveMaxThreads;
    }
    
    public static int getArchiveQueueSize() {
        return archiveQueueSize;
    }
    
    public static int getArchiveAliveTime() {
        return archiveAliveTime;
    }

    public static int sendToGanglia() {
        return sendToGanglia;
    }

    public static int writeRespcodeToLog() {
        return writeRespcodeToLog;
    }

    public static int writeSlaToLog() {
        return writeSlaToLog;
    }

    public static int getPutMetaAlarmLimit() {
        return putMetaAlarmLimit;
    }

    public static int getGetMetaAlarmLimit() {
        return getMetaAlarmLimit;
    }

    /** 单位秒 */
    public static int getLogSLAInterval() {
        return oosLogSLAInterv;
    }

    public static String getPrivateKeyPath() {
        return privateKeyPath;
    }

    public static int getProcessCoreSize() {
        return processCoreSize;
    }
    public static int getProcessReSendCoreSize() {
        return processReSendCoreSize;
    }

    public static String[] getDomainSuffix() {
        return domainSuffix;
    }
    
    public static int getOosPort() {
        return oosPort;
    }

    public static int getProcessUserBucketObjCutSize() {
        return processUserBucketObjCutSize;
    }

    public static int getProcessReSendSizeLimit() {
        return processReSendSizeLimit;
    }


    public static int getNodesNum() {
        return logServerNodes.length;
    }

    private static void deserialize(Class<?> clazz, byte[] data) {
        if (data == null)
            return;
        try {
            JSONObject jo = new JSONObject(new String(data, Consts.UTF_8));
            if (jo.has("logServer2adapters")) {
                JSONArray logServer2adapters = (JSONArray) jo.get("logServer2adapters");
                logServerNodes = new LogServerNode[logServer2adapters.length()];
                for (int i = 0; i < logServer2adapters.length(); i++) {
                    LogServerNode node = new LogServerNode();
                    node.id = logServer2adapters.getJSONObject(i).getInt("id");
                    node.hostName = logServer2adapters.getJSONObject(i).getString("hostName");
                    JSONArray adapters = (JSONArray) logServer2adapters.getJSONObject(i).get("adapterEntries");
                    AdapterEntry[] adapterEntries = new AdapterEntry[adapters.length()];
                    for (int j = 0; j<adapters.length();j++){
                        AdapterEntry entry = new AdapterEntry();
                        entry.host = adapters.getJSONObject(j).getString("host");
                        entry.port = adapters.getJSONObject(j).getInt("port");
                        if (!adapters.getJSONObject(j).isNull("user")) {
                            entry.user = adapters.getJSONObject(j).getString("user");
                        }
                        if (!adapters.getJSONObject(j).isNull("passwd")) {
                            entry.passwd = adapters.getJSONObject(j).getString("passwd");
                        }
                        adapterEntries[j] = entry;
                    }
                    node.adapterEntries = adapterEntries;
                    logServerNodes[i] = node;
                }
            } else {
                log.error("processLogConfig has no logServer2adapters item. Please config it!");
            }
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
            log.info("The data of LogConfig has changed");
            log.info("Now the data of AdapterConfig is:" + System.getProperty("line.separator")
                    + jo.toString());
        } catch (Exception e) {
            log.error("deserialize ", e);
        }
    }

    @UnJsonable
    private static LogServerNode[] logServerNodes;
    private static String dstLocation; // 原始集中日志的目录
    private static String postProcessDir;
    private static String archiveDir;
    private static int fetchPriod; // unit second
    private static int processPriod; // unit second
    private static int processReSendPriod = 10*60; // unit second 10分钟
    private static int archivePriod;// 7day in seconds
    private static int archiveBatchNum = 2;// 一次读取几个5分钟的日志文件打成zip包
    //线程池参数
    public static int archiveCoreThreads = 10;
    public static int archiveMaxThreads = 40;
    public static int archiveQueueSize = 100;
    public static int archiveAliveTime = 5000;
    private static int sendToGanglia; // 1 send, 0 not send
    private static int writeRespcodeToLog = 0; // 1 write, 0 not write
    private static int writeSlaToLog = 0; // 1 write, 0 not write
    private static int putMetaAlarmLimit;// put meta告警的上限
    private static int getMetaAlarmLimit;// get meta告警的上限
    private static int oosLogSLAInterv;// oos log 统计sla的间隔，3小时
    private static String privateKeyPath;
    private static int processCoreSize = 10;
    private static int processReSendCoreSize = 5;
    private static String[] domainSuffix = { "oos-cn.ctyunapi.cn" };
    private static int oosPort = 80;
    private static int processUserBucketObjCutSize = 100;  // 单位 MB 默认100M 用户bucket上传对象分割大小
    private static int processReSendSizeLimit = 24*1024;  // 单位 MB 默认24GB ReSend目录大小，超过该大小OOSLogTool会暂停处理fetched文件
    @UnJsonable
    private static DValue value;
    static {
        value = dsync.listen(OosZKNode.getProcessLogConfigPath(DataRegion.getRegion().getName()),
                new DListener() {
                    public void onChanged() {
                        try {
                            synchronized (LogConfig.class) {
                                byte[] data = value.get();
                                deserialize(LogConfig.class, data);
                            }
                        } catch (Throwable e) {
                            log.error(e.getMessage(), e);
                        }
                    }
                });
        synchronized (LogConfig.class) {
            try {
                byte[] data = value.get();
                deserialize(LogConfig.class, data);
            } catch (Throwable e) {
                log.error(e.getMessage(), e);
            }
        }
    }
}