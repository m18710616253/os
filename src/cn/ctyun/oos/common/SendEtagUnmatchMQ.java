package cn.ctyun.oos.common;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.json.JSONObject;

import cn.ctyun.common.Consts;
import cn.ctyun.common.conf.DataRegion;
import cn.ctyun.common.conf.OOSConfig;
import cn.ctyun.oos.mq.MessageQueue.Status;
import cn.ctyun.oos.mq.OOSMq;
import cn.ctyun.oos.mq.OOSMq.OOSMqType;
import cn.ctyun.oos.server.lifecycle.Lifecycle;

public class SendEtagUnmatchMQ {
    
    private static final Log log = LogFactory.getLog(Lifecycle.class);
    private static OOSMq mq = (OOSMq) OOSMq.getGlobalMQ();
    
    /**
            * 增加bucket  object 信息。 
      * @TODO  增加object key和bucket 信息在detail中。 
     * */
    public static void send(String etag, String originEtag, String ostorKey, String ostorId) {
        if(!Consts.ENABLE.equals(OOSConfig.getEnableEtagUnmatch())) {
            return;
        }
        try {
            String regionName = DataRegion.getRegion().getName();
            String k = OOSMqType.KeyType.ETAGUNMATCH.buildEtagUnmatchKey(ostorKey, ostorId, regionName);
            String details = new JSONObject().put("etag", etag).put("originEtag", originEtag).toString();
            mq.putWithRetries(OOSMqType.mq_etag_unmatch, Bytes.toBytes(k),
                    Status.UNHANDLED.bytesValue(), Bytes.toBytes(details), 3);
            log.info("put to etagUnmatch, key: " + k);
        } catch (Exception e) {
            log.error("send etagUnmatch mq error ",e);
        }
    }
    
    
    
}
