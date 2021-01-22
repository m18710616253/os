package cn.ctyun.oos.tempo;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.Bytes;

import cn.ctyun.common.ReplicaMode;
import cn.ctyun.oos.mq.MQDispatcher;
import cn.ctyun.oos.mq.MQDispatcher.ServiceName;
import cn.ctyun.oos.mq.MessageQueue.MessageType;
import cn.ctyun.oos.ostor.OstorID;
import cn.ctyun.oos.ostor.OstorProxy.OstorObjectMeta;
import cn.ctyun.oos.ostor.Route;
import cn.ctyun.oos.ostor.Route.RouteTable;
import cn.ctyun.oos.ostor.common.OstorConsts;
import cn.ctyun.oos.ostor.hbase.OstorHBaseClient;
import cn.ctyun.oos.ostor.hbase.OstorMq;
import cn.ctyun.oos.ostor.hbase.OstorMq.KeyType;
import cn.ctyun.oos.ostor.utils.SpeedController;
import common.tuple.Pair;
import common.util.BlockingExecutor;
import common.util.HexUtils;

public class RecoverIncurableMsg{

    /**
     * 获取到旧的（时间可配）OBJ类型的消息，从hbae中获取到size和副本模式
     * 检查对象中各个对象是否完整，如果有无法恢复的page，则将该对象标记为INCURABLE
     * @param table
     * @param key
     * @param type
     */
    static {
        System.setProperty("log4j.log.app", "recoverincurablemsg");
    }
    private static Log log = LogFactory.getLog(RecoverIncurableMsg.class);
    private static OstorMq queue = new OstorMq();
    private static RouteTable routeTable = new RouteTable();
    private static OstorHBaseClient client = new OstorHBaseClient();
    private static SpeedController controller = new SpeedController(100);
    private static long pageSize = 512 * 1024;
    private static BlockingExecutor pool = new BlockingExecutor(
            100, 100, 100, 30000, "rescuePage pool");
    private static AtomicLong total = new AtomicLong();
    private static AtomicLong failed = new AtomicLong();
    private static AtomicLong incurabal = new AtomicLong();
    private static MQDispatcher mqDispatcher = MQDispatcher.getOstorMQDispatcher(OstorID.getOstorID());
    
    public static void rescue(String key, int speed, MessageType type){
        
        try{
            String items[] = key.split("\\|");
            String msgType = items[0];
            if(null == msgType || items.length < 2 || 
                    !msgType.trim().equals(OstorConsts.MQ_INCURABLE_OBJ)){
                log.error("Unknown message: " + key);
                queue.delete(type, Bytes.toBytes(key));
                return;
            }
            String ostorKey = items[1];
            OstorObjectMeta meta = new OstorObjectMeta(ostorKey); 
            boolean exist = client.objectMetaGet(meta);
            if(!exist){
                log.info("Ostorkey not eixst. key:" + key);
                queue.delete(type, Bytes.toBytes(key));
                return;
            }
            
            ReplicaMode mode = meta.replica;
            long groupSize = (mode.ec_m == 0) ? pageSize : mode.ec_n * pageSize;
            long totalNum = meta.length / groupSize;
            totalNum = (meta.length % groupSize == 0) ? totalNum : totalNum + 1;
            RescuerResult result = new RescuerResult();
            
            boolean isIncurable = false;
            boolean isError = false;
            for(int i = 0; i < totalNum; i++){
                int tryCount = 5;
                while(tryCount-- > 0){
                    controller.setMaxSlotNum(speed);
                    controller.acquireSlot();
                    try {
                        result = rescuePage(mode, ostorKey, i);
                        if(result.finish){
                            break;
                        }else{
                            try {
                                Thread.sleep(1000);
                            } catch (Exception e) {}
                        }
                    } finally {
                        controller.releaseSlot();
                    }
                }
                
                if(!result.finish){
                    log.error("Failed object. ostorKey:" + ostorKey + ", mode:" 
                            + mode.toString() + ", pageNum:" + i);
                    isError = true;
                    failed.incrementAndGet();
                    break;
                }
                if(result.incurable){
                    log.error("Incurable object. ostorKey:" + ostorKey + ", mode:" 
                            + mode.toString() + ", pageNum:" + i);
                    isIncurable = true;
                    incurabal.incrementAndGet();                    
                    break;
                }
            }
            
            if(!isError && !isIncurable)
                queue.delete(type, Bytes.toBytes(key));
            
        }catch(Throwable t){
            log.error(t.getMessage(), t);
        }finally{
            total.incrementAndGet();
        }
    }
    public static Pair<Integer, String> getPageKey(String ostorKey, int n) {

        Pair<Integer, String> p = new Pair<Integer, String>();
        String pageKey = null;
        byte[] key = Route.getPageKey(ostorKey, n);
        pageKey = HexUtils.toHexString(key);
        int vdisk = Route.getVDisk(key);
        p.first(vdisk);
        p.second(pageKey);

        return p;
    }
    private static RescuerResult rescuePage(ReplicaMode mode, String ostorKey, int pageNum){
        
        RescuerResult result = new RescuerResult();
        try {
            Pair<Integer, String> pair = getPageKey(ostorKey, pageNum);
            int vdisk = pair.first();
            String pageKey = pair.second();
            String key = KeyType.PAGE_RESCUE.buildRescuePageKey(vdisk, mode.toString(), pageKey);
			// result = rescue(routeTable, key, ostorKey, queue.mq_todo, false);
        } catch (Throwable t) {
            log.error("Rescue page failed. ostorKey:" + ostorKey + ", mode:" + mode
                    + ", pageNum:" + pageNum, t);
        }
        return result;
    }
  
    public static void main(String[] args) {
        
        if(args.length < 1){
            log.info("Usage: IncurableRescuer speed");
            System.exit(1);
        }
        final int speed = Integer.parseInt(args[0]);
        final MessageType type = queue.mq_incurable;
        new Thread(){
            @Override
            public void run() {
                long sleepTime = 5 * 1000;
                long waitTime = 2 * 60 * 60 * 1000;
                while(true){
                    try {
                        List<byte[]> unhandledKeys = mqDispatcher.getMessages(ServiceName.TOOL, type, 
                                null, type.getFamilys()[0].first(), 
                                Bytes.toBytes("v"), null, 
                                50, 0, System.currentTimeMillis() - waitTime, false);
                        if(null == unhandledKeys || unhandledKeys.isEmpty()){
                            Thread.sleep(sleepTime);
                            continue;
                        }
                        
                        final CountDownLatch latch = new CountDownLatch(unhandledKeys.size());
                        for(final byte[] key : unhandledKeys){
                            pool.submit(new Runnable() {
                                @Override
                                public void run() {
                                    try {
                                        rescue(Bytes.toString(key), speed, type);
                                    } finally {
                                        latch.countDown();
                                    }
                                }
                            });
                        }
                        
                        while(true){
                            try {
                                latch.await();
                                break;
                            } catch (InterruptedException e) {}
                        }
                    } catch (Throwable t) {
                        log.error(t.getMessage(), t);
                    }
                }
            }
        }.start();
        
        new Thread(){
            public void run() {
                while(true){
                    try {
                        log.info("STATISTICS: total:" + total.get() + ", failed:" 
                                + failed.get() + ", incurable:" + incurabal.get()
                                + ", poolMaxPoolSize:" + pool.getMaximumPoolSize()
                                + ", poolActivePoolSize:" + pool.getActiveCount()
                                + ", poolQueueSize:" + pool.getQueue().size());
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {}
                }
            };
        }.start();
        
        while(true){
            try {
                Thread.sleep(Long.MAX_VALUE);
            } catch (InterruptedException e) {}
        }
    }
    /**
     * 恢复结果
     * @author jiazg
     *
     */
    public static class RescuerResult{
        /**
         * 该消息是否被处理完（重新放回队列返回false）
         */
        public boolean finish;
        /**
         * 该page是否需要放入INCURABLE队列
         */
        public boolean incurable;
    }
}

