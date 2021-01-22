package cn.ctyun.oos.server.storage;

import java.io.IOException;
import java.io.InputStream;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

import cn.ctyun.common.BaseException;
import cn.ctyun.common.Consts;
import cn.ctyun.common.ReplicaMode;
import cn.ctyun.common.conf.OOSConfig;
import cn.ctyun.common.conf.RegionHHZConfig;
import cn.ctyun.common.utils.LogUtils;
import cn.ctyun.oos.common.BandWidthControlStream;
import cn.ctyun.oos.common.BandWidthControlStream.ActualExpectedRatio;
import cn.ctyun.oos.common.ErrorMessage;
import cn.ctyun.oos.common.SignableInputStream;
import cn.ctyun.oos.hbase.HBaseConnectionManager;
import cn.ctyun.oos.hbase.HBaseUtil;
import cn.ctyun.oos.ostor.OstorClient;
import cn.ctyun.oos.ostor.OstorException;
import cn.ctyun.oos.ostor.OstorProxy;
import cn.ctyun.oos.ostor.OstorProxy.OstorObjectMeta;
import cn.ctyun.oos.ostor.Route;
import cn.ctyun.oos.ostor.Route.RouteTable;
import cn.ctyun.oos.ostor.hbase.HbasePdiskInfo;
import cn.ctyun.oos.ostor.hbase.PdiskInfo;
import cn.ctyun.oos.server.JettyServer;
import cn.ctyun.oos.server.conf.BackoffConfig;
import cn.ctyun.oos.server.formpost.FormPostHeaderNotice;
import cn.ctyun.oos.server.formpost.PolicyManager;
import cn.ctyun.oos.server.log.BucketLog;
import cn.ctyun.oos.server.signer.V4Signer;
import common.tuple.Pair;
import common.util.HexUtils;

public class Storage {
    private OstorProxy proxy;
    public static String product = "ostor";
    public Signer signer;
    public BucketLog bucketLog;
    private static final Log log = LogFactory.getLog(Storage.class);
    
    public Storage(BucketLog bucketLog, Signer signer, String ostorId) {
        proxy = OstorClient.getProxy(ostorId);
        this.signer = signer;
        this.bucketLog = bucketLog;
    }
    
    public Storage(BucketLog bucketLog, String ostorId) {
        this(bucketLog, null, ostorId);
    }
    
    public String getOstorId() {
        return proxy.getOstorID();
    }
    
    private long writeToOstor(InputStream input, OstorObjectMeta ostorObjMeta, int limitRate) throws BaseException {
        bucketLog.clientRequestFirstTime = System.currentTimeMillis();
        bucketLog.adapterRequestStartTime = System.currentTimeMillis();
        bucketLog.adapterRequestFirstTime = System.currentTimeMillis();
        SignableInputStream sin= new SignableInputStream(input, ostorObjMeta.length, signer);
        long writtenLen;
        BandWidthControlStream bwContent = null;
        try {
            //bucket白名单内的不限速。 如果环境配置不限速，但是客户请求配置限速，要限速。
            //如果环境带宽超过阈值，按环境配置限速（因为如果按照客户自己配置限速，环境带宽会溢出）
            if ((!BackoffConfig.isEnableStreamSlowDown() || BackoffConfig.getWhiteList().contains(ostorObjMeta.bucketName))
                    && limitRate == Consts.USER_NO_LIMIT_RATE) {
                writtenLen = proxy.put(sin, ostorObjMeta, OOSConfig.getMaxConcurrentPutNum());
            } else {
                int controlSize = ostorObjMeta.replica.ec_m > 0 ? ostorObjMeta.pageSize * ostorObjMeta.replica.ec_n : ostorObjMeta.pageSize;
                bwContent = new BandWidthControlStream(sin, controlSize, 
                //如果配置了个人限速，则不执行全局限速。 由于全局限速的带宽瓶颈很难达到，  所以暂时只会发生其中一种限速
                        BackoffConfig.getMaxStreamPauseTime(), new ActualExpectedRatio() {
                    @Override
                    public double getRatio() {
                        long globalMaxIncomingBytes = BackoffConfig.getMaxIncomingBytes();
                        long globalIncomingBytes = JettyServer.inComingBytes;
                        double globalRatio = 0f;
                        if (!((globalIncomingBytes <= 0) || (globalMaxIncomingBytes <= 0) || (globalIncomingBytes < globalMaxIncomingBytes))) {
                            globalRatio = (((double)globalIncomingBytes) / ((double)globalMaxIncomingBytes));
                        }
                        return globalRatio;
                    }

                    @Override
                    public int getSpeedLimit() {
                        return limitRate * Consts.KB;
                    }
                });
                writtenLen = proxy.put(bwContent, ostorObjMeta, OOSConfig.getMaxConcurrentPutNum());
            }
            try{
                V4Signer.checkContentSignatureV4(input);
            } catch (BaseException e) {
                log.error(e.getMessage(), e);
                try {
                    proxy.delete(ostorObjMeta, true);
                } catch (Exception e2) {
                    log.error(e2.getMessage(), e2);
                }
                throw e;
            }
        } catch (OstorException e) {
            log.error(e.getMessage() + " ostorId: " + proxy.getOstorID(), e);
            bucketLog.realRequestLength = sin.count;
            if (e.getErrorCode() == 400) {
                throw new BaseException(400, ErrorMessage.ERROR_CODE_INCOMPLETE_BODY, 
                        ErrorMessage.ERROR_MESSAGE_INCOMPLETE_BODY);
            } else if (e.getErrorCode() == 503) {
                throw new BaseException(503, ErrorMessage.ERROR_CODE_SLOW_DOWN, ErrorMessage.ERROR_MESSAGE_SLOW_DOWN);
            } else {
                throw new BaseException(500, "Upload Object Failed");
            }
        } finally {
            if(bwContent != null)
                try {
                    bwContent.close();
                } catch (IOException e) {
                }
        }
        bucketLog.clientRequestLastTime = System.currentTimeMillis();
        bucketLog.adapterRequestLastTime = System.currentTimeMillis();
        bucketLog.ostorResponseStartTime = System.currentTimeMillis();
        bucketLog.realRequestLength = sin.count;
        return writtenLen;
    }
    
    public String create(InputStream input, long contentLength, String userMeta, String objectName, String storageId, 
            int pageSize, ReplicaMode mode, String bucketName, int limitRate) throws BaseException {
        byte[] ostorKey;
        if (storageId == null) {
            ostorKey = OstorKeyGenerator.generateOstorKey(proxy, contentLength, pageSize, mode);
        } else {
            ostorKey = HexUtils.toByteArray(storageId);
        }
        OstorObjectMeta ostorObjMeta = new OstorObjectMeta(ostorKey, 0, contentLength, mode, pageSize, userMeta,
                bucketName, objectName, getRequestId());
        long writtenLen = writeToOstor(input, ostorObjMeta, limitRate);
        
        if (writtenLen != contentLength) {
            log.error("incomplete body, the real length from ostor is: " + writtenLen + ", the expected length is: " + 
                    contentLength + ", request id: " + bucketLog.requestId + " ostorId: " + proxy.getOstorID());
            try {
                proxy.delete(ostorObjMeta, true);
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
            throw new BaseException(400, ErrorMessage.ERROR_CODE_INCOMPLETE_BODY, 
                    ErrorMessage.ERROR_MESSAGE_INCOMPLETE_BODY);
        }
        LogUtils.log("objectName: " + objectName + ", put object success: " + HexUtils.toHexString(ostorKey) + 
                " mode: " + mode.toStringAll() + " length: " + contentLength + " ostorId: " + proxy.getOstorID());
        return HexUtils.toHexString(ostorKey);
    }
    
    public Pair<String, Long> create(InputStream input, long maxLength, String userMeta, String objectName, 
            int pageSize, ReplicaMode mode, String bucketName) throws BaseException {
        Pair<String, Long> result = new Pair<>();
        byte[] ostorKey = OstorKeyGenerator.generateOstorKey(proxy, maxLength, pageSize, mode);
        OstorObjectMeta ostorObjMeta = new OstorObjectMeta(ostorKey, 0, maxLength, mode, pageSize, userMeta, 
                bucketName, objectName, getRequestId());
        long writtenLen = writeToOstor(input, ostorObjMeta, Consts.USER_NO_LIMIT_RATE);
        result.first(HexUtils.toHexString(ostorKey));
        result.second(writtenLen);
        return result;
    }
    
    public String create(InputStream input, long contentLength, String userMeta, String objectName, int pageSize, 
            ReplicaMode mode, String bucketName, FormPostHeaderNotice formPostHeaderNotice) throws BaseException {
        long remainingLen = contentLength - formPostHeaderNotice.getHeaderTotalLength();
        byte[] ostorKey = OstorKeyGenerator.generateOstorKey(proxy, remainingLen, pageSize, mode);
        OstorObjectMeta ostorObjMeta = new OstorObjectMeta(ostorKey, 0, remainingLen, mode, pageSize, userMeta, 
                bucketName, objectName, getRequestId());
        long writtenLen = writeToOstor(input, ostorObjMeta, Consts.USER_NO_LIMIT_RATE);
        formPostHeaderNotice.fileLength = writtenLen;
        try {
            PolicyManager.checkFileLength(formPostHeaderNotice.policy, writtenLen);
            PolicyManager.checkFileFieldCount(formPostHeaderNotice);
        } catch(BaseException e) {
            try {
                proxy.delete(ostorObjMeta, true);
            } catch (Exception ex) {
                log.error(ex.getMessage(), ex);
            }
            throw e;
        }
        long totalHeaderLen = formPostHeaderNotice.getHeaderTotalLength();
        long bodyLength = writtenLen + totalHeaderLen;
        if (bodyLength != contentLength) {
            log.error("Form POST. incomplete body, the real length from ostor is: " + writtenLen + 
                    ", the body length is: " + bodyLength + ", contentLength is: " + contentLength + 
                    ", request id: " + bucketLog.requestId + ", ostorId: " + proxy.getOstorID());
            try {
                proxy.delete(ostorObjMeta, true);
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
            throw new BaseException(400, ErrorMessage.ERROR_CODE_INCOMPLETE_BODY, 
                    ErrorMessage.ERROR_MESSAGE_INCOMPLETE_BODY);
        }
        LogUtils.log("objectName: " + objectName + ", post object success: " + HexUtils.toHexString(ostorKey) + 
                " mode: " + mode.toStringAll() + " length: " + contentLength + " ostorId:" + proxy.getOstorID());
        return HexUtils.toHexString(ostorKey);
    }
    
    public InputStream read(String id, long offset, long length, String objectName, int pageSize,
            ReplicaMode mode) throws BaseException {
        InputStream input = null;
        if (bucketLog != null && bucketLog.adapterRequestStartTime == 0)
            bucketLog.adapterRequestStartTime = System.currentTimeMillis();
        try {
            OstorObjectMeta objMeta = new OstorObjectMeta(HexUtils.toByteArray(id), offset, length,
                    mode, pageSize, null, getRequestId());
            input = proxy.get(objMeta);
        } catch (OstorException e) {
            log.error(e.getMessage() + " ostorId:" + proxy.getOstorID(), e);
            if (e.getErrorCode() == 503)
                throw new BaseException(503, ErrorMessage.ERROR_CODE_SLOW_DOWN,
                        ErrorMessage.ERROR_MESSAGE_SLOW_DOWN);
            else
                throw new BaseException(500, ErrorMessage.GET_OBJECT_FAILED);
        }
        if (bucketLog != null && bucketLog.ostorResponseStartTime == 0)
            bucketLog.ostorResponseStartTime = System.currentTimeMillis();
        LogUtils.log("objectName:" + objectName + ", get object success:" + id + " offset:" + offset
                + " length:" + length + " mode:" + mode.toStringAll() + " ostorId:"
                + proxy.getOstorID());
        return input;
    }
    
    public void delete(String id, long length, String objectName, int pageSize, ReplicaMode mode,
            boolean isSync) throws BaseException {
        if (bucketLog != null && bucketLog.adapterRequestStartTime == 0)
            bucketLog.adapterRequestStartTime = System.currentTimeMillis();
        try {
            OstorObjectMeta objMeta = new OstorObjectMeta(HexUtils.toByteArray(id), 0, length,
                    mode, pageSize, null, getRequestId());
            proxy.delete(objMeta, isSync);
        } catch (OstorException e) {
            log.error(e.getMessage() + " ostorId:" + proxy.getOstorID(), e);
            if (e.getErrorCode() == 503)
                throw new BaseException(503, ErrorMessage.ERROR_CODE_SLOW_DOWN,
                        ErrorMessage.ERROR_MESSAGE_SLOW_DOWN);
            else
                throw new BaseException(500, "Delete Object Failed");
        }
        if (bucketLog != null && bucketLog.ostorResponseStartTime == 0)
            bucketLog.ostorResponseStartTime = System.currentTimeMillis();
        LogUtils.log("objectName:" + objectName + ", delete object success:" + id + " mode:"
                + mode.toStringAll() + " length:" + length + " ostorId:" + proxy.getOstorID());
    }

    public static String setStorageId(String storageId) {
        return product + "::" + OOSConfig.getOstorVersion() + "::" + storageId;
    }
    
    public static String getStorageId(String storageId) {
        return storageId.split("::")[2];
    }

    private String getRequestId() {
        if (bucketLog != null)
            return bucketLog.requestId;
        else
            return null;
    }
    public static String generateOstorKey(){
        return HexUtils.toHexString(OstorClient.getProxy().generateOstorKey());
    }
}
class OstorKeyGenerator{
    private static final Log log = LogFactory.getLog(OstorKeyGenerator.class);
    private static AtomicLong badOstorkeyNum = new AtomicLong(0);
    private static AtomicLong goodOstorkeyNum = new AtomicLong(0);
    private static AtomicLong totalTryNum = new AtomicLong(0);
    private static Map<String, CopyOnWriteArrayList<Long>> blacklistPdisks = new ConcurrentHashMap<String, CopyOnWriteArrayList<Long>>();
    
    static{
        new Thread(){
            @Override
            public void run(){
                for(;;){
                    log.info("Retry to get a good ostorkey. Good ostorkey num: " + goodOstorkeyNum.get() + ".Bad ostorkey num: " + badOstorkeyNum.get() + ".Total try:" + totalTryNum);
                    try { Thread.sleep(10000); } catch(InterruptedException e){}
                }
            }
        }.start();
        
        new Thread(){
            @Override
            public void run(){
                HConnection conn = null;
                try {
                    conn = HBaseConnectionManager.createConnection(RegionHHZConfig.getConfig());
                } catch (IOException e1) {
                    log.error(e1.getMessage(), e1);
                    System.exit(-1);
                }
                for(;;){
                    try {
                        Map<String, Map<Long, Long>> pdisks2space = getAllPdiskSpace(conn);
                        Map<String,List<Long>> topPdisks = getTopNPdisks(pdisks2space, OOSConfig.getTopNDisks());//当前策略Top n
                        Iterator<String> ostorIds = topPdisks.keySet().iterator();
                        while(ostorIds.hasNext()){
                            String ostorId = ostorIds.next();
                            if (blacklistPdisks.containsKey(ostorId)) {
                                CopyOnWriteArrayList<Long> tmp = blacklistPdisks.get(ostorId);
                                tmp.clear();
                                tmp.addAll(topPdisks.get(ostorId));
                            } else {
                                CopyOnWriteArrayList<Long> pdisks = new CopyOnWriteArrayList<>();
                                pdisks.addAll(topPdisks.get(ostorId));
                                blacklistPdisks.put(ostorId, pdisks);
                            }
                        }
                        log.info("now add top pdisks:" + topPdisks);
                    } catch (Throwable e) {
                        log.error(e.getMessage(), e);
                    }
                    try { Thread.sleep(300000); } catch(InterruptedException e){}
                }
            }
        }.start();
    }

    public static boolean checkOstorKeyOnVdisk(OstorProxy proxy, byte[] ostorKey, long objectSize, int pageSize, ReplicaMode mode, List<Long> pdisks){
        int groupSize;
        if(mode.ec_m == 0){
            groupSize = pageSize;
        }else{
            groupSize = mode.ec_n * pageSize;
        }
        
        int groupNum = (int) ((objectSize % groupSize == 0) ? (objectSize / groupSize) : (objectSize / groupSize + 1));
        for(int groupIdx=0; groupIdx<groupNum; groupIdx++){
            byte[] pageKey = Route.getPageKey(ostorKey, groupIdx);
            int vdisk = Route.getVDisk(pageKey);
            
            RouteTable routeTable = proxy.getRouteTable();
            List<Long> usedPdisks = routeTable.getVPTable().get(vdisk).subList(0, mode.ec_n+mode.ec_m);
            for(Long usedPdisk : usedPdisks){
                if(pdisks.contains(usedPdisk)){
                    return true;
                }
            }
        }
        return false;
    }
    
    public static byte[] generateOstorKey(OstorProxy proxy, long objectSize, int pageSize, ReplicaMode mode){
        byte[] key = proxy.generateOstorKey();
        
        boolean findOstorkey = false;
        String ostorId = proxy.getOstorID();
        List<Long> blackListPdisks = getBlacklistPdisks(ostorId);
        int tryCountConfig = OOSConfig.getOstorkeyTryCount();
        for(int tryCount=0; tryCount<tryCountConfig; tryCount++){//连同第一次，重试n次
            if (blackListPdisks == null || !checkOstorKeyOnVdisk(proxy, key, objectSize, pageSize, mode, blackListPdisks)) {
                findOstorkey = true;
                totalTryNum.addAndGet(tryCount+1);
                break;
            }else{
                key = proxy.generateOstorKey();
            }
        }
        
        if(tryCountConfig > 0 && ! findOstorkey){//开启了重试，但是没找到
            badOstorkeyNum.addAndGet(1);
            totalTryNum.addAndGet(tryCountConfig);
        }else if(tryCountConfig > 0 && findOstorkey){
            goodOstorkeyNum.addAndGet(1);
        }
        
        return key;
    }
    
    public static List<Long> getBlacklistPdisks(String ostorId){
        return blacklistPdisks.get(ostorId);
    }
    
    public static Map<String, Map<Long, Long>> getAllPdiskSpace(HConnection conn) throws IOException{
        Map<String, Map<Long, Long>> pdisks2space = new HashMap<String, Map<Long, Long>>();
        try(HTableInterface table = conn.getTable(HbasePdiskInfo.getTableName())){
            try(ResultScanner scanner = HBaseUtil.getScanner(table, null, null, null, HbasePdiskInfo.familyName, HbasePdiskInfo.columnNameSpace, 2000, 2000, 1)){
                Iterator<Result> iter = scanner.iterator();
                while(iter.hasNext()){
                    Result r = iter.next();
                    PdiskInfo pdiskInfo = new PdiskInfo();
                    pdiskInfo.readRow(r.getRow());
                    pdiskInfo.readSpaceFields(r.getValue(HbasePdiskInfo.familyName, HbasePdiskInfo.columnNameSpace));
                    long pdisk = Long.parseLong(pdiskInfo.pdiskId);
                    long usedSpace = pdiskInfo.space.usedSpace;
                    String ostorId = pdiskInfo.ostorId;
                    if (pdisks2space.containsKey(ostorId)) {
                        pdisks2space.get(ostorId).put(pdisk, usedSpace);
                    } else {
                        Map<Long, Long> map = new HashMap<Long,Long>();
                        map.put(pdisk, usedSpace);
                        pdisks2space.put(ostorId, map);
                    }
                }
            }
        }
        return pdisks2space;
    }
    
    public static Map<String,List<Long>> getTopNPdisks(Map<String, Map<Long, Long>> pdisk2space, int n){
        Map<String, List<Long>> ostorIds2TopNPdisks = new HashMap<String, List<Long>>();
        PriorityQueue<Pair<Long, Long>> pdiskSpaceDesend =  new PriorityQueue<>(1500, new Comparator<Pair<Long, Long>>() {
            @Override
            public int compare(Pair<Long, Long> o1, Pair<Long, Long> o2) {
                if(o1.second().longValue() > o2.second().longValue()){
                    return -1;
                }else if(o1.second().longValue() < o2.second().longValue()){
                    return 1;
                }else{
                    return 0;
                }
            }
        });
        Iterator<String> ostorIds = pdisk2space.keySet().iterator();
        while(ostorIds.hasNext()){
            pdiskSpaceDesend.clear();
            String ostorId = ostorIds.next();
            Map<Long, Long> map = pdisk2space.get(ostorId);
            Iterator<Long> pdisks = map.keySet().iterator();
            while(pdisks.hasNext()){
                long pdisk = pdisks.next();
                long space = map.get(pdisk);
                pdiskSpaceDesend.add(new Pair<Long, Long>(pdisk, space));
            }
            List<Long> ret = new LinkedList<>();
            int m = n;
            while(m-- > 0 && !pdiskSpaceDesend.isEmpty()){
                ret.add(pdiskSpaceDesend.poll().first());
            }
            ostorIds2TopNPdisks.put(ostorId, ret);
        }
        return ostorIds2TopNPdisks;
    }
    
    public static void main(String[] args) throws IOException, InterruptedException {
        OstorProxy proxy = OstorProxy.getProxy();
        int pagesize = 512 * 1024;
        int objectSize = 5*1024*1024;
        for(int i=0; i<1000;i++){
            generateOstorKey(proxy, objectSize, pagesize, new ReplicaMode(9, 3, 2));
        }
        
        System.out.println(OOSConfig.getTopNDisks());
        System.out.println(OOSConfig.getOstorkeyTryCount());
    }
}
