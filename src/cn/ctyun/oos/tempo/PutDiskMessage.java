package cn.ctyun.oos.tempo;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.Bytes;

import cn.ctyun.common.conf.RegionHHZConfig;
import cn.ctyun.common.dsync.DSyncService;
import cn.ctyun.common.node.OstorZKNode;
import cn.ctyun.oos.mq.MessageQueue.Status;
import cn.ctyun.oos.ostor.OstorID;
import cn.ctyun.oos.ostor.Route;
import cn.ctyun.oos.ostor.hbase.OstorMq;
import cn.ctyun.oos.ostor.hbase.OstorMq.KeyType;

public class PutDiskMessage {
    static {
        System.setProperty("log4j.log.app", "putdiskmessage");
    }
    private static Log log = LogFactory.getLog(PutDiskMessage.class);
    private static DSyncService dsync;
    private static OstorMq queue = new OstorMq( OstorID.getOstorID(), 
            RegionHHZConfig.getConfig());
    static{
        try {
            dsync = new DSyncService(RegionHHZConfig.getQuorumServers(),
                    RegionHHZConfig.getSessionTimeout());
        } catch (IOException e) {
            new RuntimeException("Initial RouteManager failed");
        }
    }
    
    private static void writeMsgQueue(KeyType type, long pdisk, long routeVersion)
            throws IOException {
        int num = 1;
        if(type == KeyType.DELETE_DISK){
            String rescuerProcessPath = OstorZKNode.getRescuerProcessPath(OstorID.getOstorID());
            List<String> rescuers = dsync.listenOnChildrenNode(rescuerProcessPath, null);
            int n = rescuers.size() * 3;
            num = (n < 3) ? 3 : (n > 100) ? 100 : n;
        }
        
        int intervalNum = Route.getTotalVDiskNumber() / num;
        for(int j = 0; j < num; j++){
            String key = null;
            if(j == num - 1) {
                if(type == KeyType.ADD_DISK)
                    key = type.buildAddDiskKey(routeVersion, pdisk, j * intervalNum, 
                            Route.getTotalVDiskNumber() - j * intervalNum);
            } else {
                if(type == KeyType.ADD_DISK)
                    key = type.buildAddDiskKey(routeVersion, pdisk, j * intervalNum, intervalNum);
            }    
            queue.checkAndPut(queue.mq_emergency, Bytes.toBytes(key), null,
                        Bytes.toBytes(Status.UNHANDLED.intValue()));
            log.info("put msg, " + type + ", pdisk: " + pdisk);
       }
    }
    
    public static void main(String[] args) throws IOException {
        
        if(null == args || args.length < 3){
            log.info("Usage: PutDiskMessage ADD_DISK pdisk");
            System.exit(1);
        }
        KeyType type = null;
        String opt = args[0];
        long pdisk = Long.valueOf(args[1]);
        long routeVersion = Long.valueOf(args[2]);
        if(opt.equals("ADD_DISK")){
            type = KeyType.ADD_DISK;
        }else{
            log.info("Usage: PutDiskMessage ADD_DISK pdisk");
            System.exit(1);
        }
        writeMsgQueue(type, pdisk, routeVersion);
    }
}
