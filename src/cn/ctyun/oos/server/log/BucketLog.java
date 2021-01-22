package cn.ctyun.oos.server.log;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Date;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.s3.Headers;

import cn.ctyun.common.Consts;
import cn.ctyun.common.conf.DataRegion;
import cn.ctyun.common.conf.OOSConfig;
import cn.ctyun.common.utils.HostUtils;
import cn.ctyun.oos.common.OOSRequestId;
import cn.ctyun.oos.common.Utils;
import cn.ctyun.oos.common.WebsiteHeader;
import cn.ctyun.oos.hbase.MetaClient;
import cn.ctyun.oos.iam.accesscontroller.util.ARNUtils;
import cn.ctyun.oos.metadata.BucketMeta;
import cn.ctyun.oos.metadata.ObjectMeta;
import cn.ctyun.oos.metadata.OwnerMeta;
import cn.ctyun.oos.server.OpObject;
import cn.ctyun.oos.server.backoff.Backoff;
import cn.ctyun.oos.server.count.PinData;
import cn.ctyun.oos.server.util.Misc;
import common.util.JsonUtils;

/**
 * @author: Cui Meng
 */
public class BucketLog {
    // private static final Log bucketLog = LogFactory.getLog("bucketLog");
    static {
        System.setProperty("log4j.log.app", "bucketLog");
    }
    private static final Log log = LogFactory.getLog(BucketLog.class);
    public String targetBucketName;
    public String targetPrefix;
    public long ownerId;
    public String referer = "-";
    public String objectSize = "-";
    public String versionId = "-";
    public String errorCode = "-";
    public int status = 200;
    public String ipAddress = "-";
    public long endTime;
    // request
    // 开始时间
    public long startTime;// oos收到客户端请求的时间
    public String bucketOwnerName = "defalut";
    public String bucketName;
    public String requesterName = "Anonymous";
    public String objectName = "-";
    public String operation = "-";
    public String URI = "-";
    public String userAgent = "-";
    private static final String separator = System.getProperty("line.separator");
    public static AccessLog totalLog;
    // 接收第一个字节的时间
    public long clientRequestFirstTime;// 客户端发出请求体的第一个字节的时间
    // 接收最后一个字节的时间
    public long clientRequestLastTime; // 客户端发出请求体的最后一个字节的时间
    public long adapterRequestStartTime;// oos向ostor发出请求的开始时间
    public long adapterRequestFirstTime;// oos向ostor发出请求体的第一个字节的时间
    public long adapterRequestLastTime;// oos向ostor发出请求体的最后一个字节的时间
    // response
    public long ostorResponseStartTime;// ostor 响应的开始时间
    public long ostorResponseFirstTime;// ostor 响应第一个字节的时间
    public long ostorResponseLastTime;// ostor 响应最后一个字节的时间
    public long adapterResponseStartTime;// oos向客户端发出响应的时间
    public long adapterResponseFirstTime;// oos向客户端发出响应，第一个字节的时间
    public long adapterResponseLastTime;// oos向客户端发出响应，最后一个字节的时间
    public long realRequestLength = 0;
    public long realResponseLength = 0;
    public long preProcessTime;// 算签名的时间
    public long getMetaTime;// get metadata 时间
    public long putMetaTime;// put metadata 时间
    public long deleteMetaTime;// delete metadata时间
    public long pinTime;
    public String requestId = "-";
    public static long logUserId;
    public String contentLength = "-";
    public long prepareLogTime;
    public long writeBucketLogTime;
    public int exception = 0; /* 0：正常，1：get object过程中发生异常
                               * 2:分段下载未合并的object。3:分段上传object。4:删除分段上传的object;5:get object过程中，从ostor读数据异常
                               * 7：get object时返回客户端200响应码，但是在向客户端发送响应体过程中，从ostor中读数据时产生错误
                               * 8：get object时返回客户端200响应码，但是在向客户端发送响应体过程中，向客户端写数据错误
                               * 1，2，3，4，5在统计sla时过滤 */
    public String ostorId = "-";
    public String ostorKey = "-";
    public String metaRegion = "-";
    public String dataRegion = "-";
    public String etag = "-";
    public String replicaMode = "-";
    public String originRequestId = "-";
    public String akOwnerName = "";
    private static MetaClient client = MetaClient.getGlobalClient();
    static {
        totalLog = new AccessLog(System.getProperty("user.dir") + OOSConfig.getLogdir(), "", HostUtils
                .getHostName().replaceAll("-", "") + "-" + HostUtils.getPid() + ".log");
        OwnerMeta owner = new OwnerMeta(OOSConfig.getLogUser());
        try {
            if (!client.ownerSelect(owner)) {
                log.error("No such user:" + owner.getName());
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        logUserId = owner.getId();
    }
    
    public BucketLog(long startTime) {
        this.startTime = startTime;
    }
    
    public void getRequestInfo(HttpServletRequest req) {
        if (req.getHeader("Referer") != null)
            this.referer = req.getHeader("Referer");
        if (req.getHeader("User-Agent") != null)
            this.userAgent = req.getHeader("User-Agent");
        this.URI = req.getRequestURI();
        if (req.getHeader(WebsiteHeader.CLIENT_IP) != null)
            this.ipAddress = req.getHeader(WebsiteHeader.CLIENT_IP);
        else
            this.ipAddress = Utils.getIpAddr(req);
    }
    
    public void prepare(BucketMeta dbBucket, String key, OwnerMeta owner, HttpServletRequest req,
            int status, PinData pinData, String iamUserName) throws Exception {
        {
            if (dbBucket.logTargetBucket != 0) {
                BucketMeta targetBucket = new BucketMeta(dbBucket.logTargetBucket);
                if (!client.bucketSelect(targetBucket)) {
                    dbBucket.logTargetBucket = 0;
                    dbBucket.logTargetPrefix = "";
                    client.bucketUpdate(dbBucket);
                    return;
                }
                this.targetBucketName = targetBucket.name;
            }
            this.bucketName = dbBucket.name;
            OwnerMeta bucketOwner = pinData.jettyAttributes.bucketOwnerAttribute.getBucketOwnerFromAttribute(dbBucket.ownerId);
            this.bucketOwnerName = bucketOwner.getName();
            this.ownerId = bucketOwner.getId();
            if (dbBucket.logTargetPrefix != null && dbBucket.logTargetPrefix.trim().length() != 0)
                this.targetPrefix = dbBucket.logTargetPrefix;
            if (key != null && key.length() > 0) {
                this.objectName = key;
                this.operation = "REST." + req.getMethod() + ".OBJECT";
                if (pinData.jettyAttributes.objectAttribute.objectSize != 0)
                    this.objectSize = pinData.jettyAttributes.objectAttribute.objectSize + "";
                else {
                    ObjectMeta object = new ObjectMeta(key, dbBucket.name, dbBucket.metaLocation);
                    if(client.isBusy(object, OOSConfig.getMaxConcurrencyPerRegionServer()))
                        Backoff.backoff();
                    if (client.objectSelect(object))
                        this.objectSize = String.valueOf(OpObject.getObjectSize(object).first());
                }
            } else
                this.operation = "REST." + req.getMethod() + ".BUCKET";
        }
        if (owner == null) {
            this.requesterName = "Anonymous";
        } else {
            if (iamUserName != null) {
                // 子用户访问，记录子用户的ARN
                this.requesterName = ARNUtils.generateUserArn(owner.getAccountId(), iamUserName);
            } else {
                // 根用户访问，记录账户名
                this.requesterName = owner.getName();
            }
        }
           
        if (req.getParameter("versionId") != null)
            this.versionId = req.getParameter("versionId");
        this.status = status;
        if (req.getQueryString() != null)
            this.URI += "?" + req.getQueryString();
    }
    
    //copy
    public void internalApiPrepare(HttpServletRequest request) {
        try {
            this.prepareLogTime = System.currentTimeMillis();
            String bucket = request.getHeader(Consts.X_AMZ_BUCKET_NAME);
            if(StringUtils.isNotBlank(bucket))
                this.bucketName = bucket;
            
            String length = request.getHeader(Headers.CONTENT_LENGTH);
            if(StringUtils.isNotBlank(length))
                this.contentLength = length;
            
            String objectSize = request.getHeader(Consts.X_AMZ_OBJECT_SIZE);
            if(StringUtils.isNotBlank(objectSize))
                this.objectSize = objectSize;
            
            String ostorKey = request.getHeader(Consts.X_AMZ_OSTOR_KEY);
            if(StringUtils.isNotBlank(ostorKey))
                this.ostorKey = ostorKey;
            
            String ostorId = request.getHeader(Consts.X_AMZ_OSTOR_ID);
            if(StringUtils.isNotBlank(ostorId))
                this.ostorId = ostorId;
            
            String etag = request.getHeader(Headers.ETAG);
            if(StringUtils.isNotBlank(etag))
                this.etag = etag;
            
            String replicaMode = request.getHeader(Consts.X_AMZ_REPLICA_MODE);
            if(StringUtils.isNotBlank(replicaMode))
                this.replicaMode = replicaMode;
            
            String ip = Utils.getIpAddr(request);
            this.ipAddress = ip;
            
            this.URI = request.getRequestURI();
            if (request.getQueryString() != null)
                this.URI += "?" + request.getQueryString();
            
            String originRequestId = request.getHeader(Consts.X_CTYUN_ORIGIN_REQUEST_ID);
            this.originRequestId = originRequestId;
            this.requesterName = StringUtils.isNotBlank(originRequestId) ? 
                    OOSRequestId.parseRequestId(originRequestId) : "internal client";
            
            this.operation =  "REST." + request.getMethod() + ".OBJECT";
            
            String encodedObjectName = request.getHeader(Consts.X_AMZ_OBJECT_NAME);
            String object;
            if(StringUtils.isNotBlank(encodedObjectName)) {
                try {
                    object = URLDecoder.decode(encodedObjectName, Consts.STR_UTF8);
                    this.objectName = object;
                } catch (UnsupportedEncodingException e2) {
                    //do nothing
                }
            }
            //内部api都是访问本地data ，所以dataRegion只需获取本地region即可。 
            this.dataRegion = DataRegion.getRegion().getName();
        } catch (Throwable e) {
            log.error(e.getMessage(), e);
        }
    }
    
    
    public String writeLog(HttpServletRequest req, boolean writeUserAgent, PinData pinData) {
        StringBuilder sb = new StringBuilder();
        sb.append(this.bucketOwnerName).append(" ");
        sb.append(this.bucketName).append(" ");
        sb.append(" [").append(Misc.formatstrftime(new Date(this.startTime))).append("] ");
        sb.append(this.ipAddress).append(" ");
        sb.append(this.requesterName).append(" ");
        sb.append(this.requestId).append(" ");
        sb.append(this.operation).append(" ");
        sb.append(this.objectName).append(" ");
        sb.append("\"").append(req.getMethod()).append(" ").append(this.URI).append(" ")
                .append(req.getProtocol()).append("\" ");
        sb.append(this.status).append(" ");
        sb.append(StringUtils.replace(this.errorCode, " ", "")).append(" ");
        if (pinData.jettyAttributes.objectAttribute.length != 0)
            sb.append(pinData.jettyAttributes.objectAttribute.length).append(" ");
        else
            sb.append("-").append(" ");
        sb.append(this.objectSize).append(" ");
        if (this.endTime == 0)
            this.endTime = System.currentTimeMillis();
        sb.append(String.valueOf(this.endTime - this.startTime)).append(" ");
        sb.append("\"").append(this.referer).append("\" ");
        if (writeUserAgent)
            sb.append("\"").append(this.userAgent).append("\" ");
        else
            sb.append("\"").append("-").append("\" ");
        sb.append(this.versionId);
        return sb.toString();
    }

    public void writeLifecycleLog() {
        StringBuilder sb = new StringBuilder();
        sb.append(ownerId).append(" ");
        sb.append(this.bucketOwnerName).append(" ");
        sb.append(this.targetBucketName);
        if (this.targetPrefix != null && this.targetPrefix.length() != 0)
            sb.append(" ").append(this.targetPrefix);
        sb.append(separator);
        this.writeBucketLogTime = System.currentTimeMillis();
        sb.append(this.bucketOwnerName).append(" ");
        sb.append(this.bucketName).append(" ");
        sb.append(" [").append(Misc.formatstrftime(new Date(this.startTime))).append("] ");
        sb.append("-").append(" ");
        sb.append(this.requesterName).append(" ");
        sb.append(this.requestId).append(" ");
        sb.append(this.operation).append(" ");
        sb.append(this.objectName).append(" ");
        sb.append("\"").append("-").append("\" ");
        sb.append("-").append(" ");
        sb.append("-").append(" ");
        sb.append("-").append(" ");
        sb.append("0").append(" ");
        sb.append("-").append(" ");
        sb.append("\"").append("-").append("\" ");
        sb.append("\"").append("-").append("\" ");
        sb.append(this.versionId);
        this.writeBucketLogTime = System.currentTimeMillis() - this.writeBucketLogTime;
        totalLog.write(sb.toString(), 0);
    }
    
    public void reviseTime() {
        int maxTime = 100000;
        if (preProcessTime > maxTime)
            preProcessTime = -1;
        if (getMetaTime > maxTime)
            getMetaTime = -1;
        if (putMetaTime > maxTime)
            putMetaTime = -1;
        if (pinTime > maxTime)
            pinTime = -1;
        if (deleteMetaTime > maxTime)
            deleteMetaTime = -1;
        if (prepareLogTime > maxTime)
            prepareLogTime = -1;
        if (writeBucketLogTime > maxTime)
            writeBucketLogTime = -1;
    }
    
    public void write(HttpServletRequest req, String bucketName, String prefix,
            boolean writeUserAgent, PinData pinData) {
        reviseTime();
        StringBuilder sb = new StringBuilder();
        sb.append(ownerId).append(" ");
        sb.append(this.bucketOwnerName).append(" ");
        sb.append(bucketName);
        if (prefix != null && prefix.length() != 0)
            sb.append(" ").append(prefix);
        sb.append(separator);
        this.writeBucketLogTime = System.currentTimeMillis();
        totalLog.write(sb.toString() + writeLog(req, writeUserAgent, pinData), 0);
        this.writeBucketLogTime = System.currentTimeMillis() - this.writeBucketLogTime;
    }
    
    public void writeGlobalLog(HttpServletRequest req, PinData pinData) {
        reviseTime();
        if (pinData != null) {
            if (pinData.jettyAttributes.objectAttribute.length != 0)
                contentLength = pinData.jettyAttributes.objectAttribute.length + "";
        } else
            contentLength = this.objectSize;
        StringBuilder sb = new StringBuilder();
        sb.append(logUserId).append(" ");
        sb.append(OOSConfig.getLogUser()).append(" ");
        sb.append(OOSConfig.getLogBucket());
        sb.append(" ").append("log/");
        sb.append(separator);
        String jsonLog = JsonUtils.toJson(handleTime(), "targetBucketName", "targetPrefix");
        totalLog.write(sb.toString() + jsonLog, 0);
    }

    private BucketLog handleTime() {
        BucketLog bl = new BucketLog(this.startTime);      
        bl.endTime = this.endTime == 0 ? 0 : this.endTime - this.startTime;
        bl.clientRequestFirstTime = this.clientRequestFirstTime == 0 ? 0 : this.clientRequestFirstTime - this.startTime;
        bl.clientRequestLastTime = this.clientRequestLastTime == 0 ? 0 : this.clientRequestLastTime - this.startTime;
        bl.adapterRequestStartTime = this.adapterRequestStartTime == 0 ? 0 : this.adapterRequestStartTime - this.startTime;
        bl.adapterRequestFirstTime = this.adapterRequestFirstTime == 0 ? 0 : this.adapterRequestFirstTime - this.startTime;
        bl.adapterRequestLastTime = this.adapterRequestLastTime == 0 ? 0 : this.adapterRequestLastTime - this.startTime;
        bl.ostorResponseStartTime = this.ostorResponseStartTime == 0 ? 0 : this.ostorResponseStartTime - this.startTime;
        bl.ostorResponseFirstTime = this.ostorResponseFirstTime == 0 ? 0 : this.ostorResponseFirstTime - this.startTime;
        bl.ostorResponseLastTime = this.ostorResponseLastTime == 0 ? 0 : this.ostorResponseLastTime - this.startTime;
        bl.adapterResponseStartTime = this.adapterResponseStartTime == 0 ? 0 : this.adapterResponseStartTime - this.startTime;
        bl.adapterResponseFirstTime = this.adapterResponseFirstTime == 0 ? 0 : this.adapterResponseFirstTime - this.startTime;
        bl.adapterResponseLastTime = this.adapterResponseLastTime == 0 ? 0 : this.adapterResponseLastTime - this.startTime;
        bl.targetBucketName = this.targetBucketName;
        bl.targetPrefix = this.targetPrefix;
        bl.ownerId = this.ownerId;
        bl.referer = this.referer;
        bl.objectSize = this.objectSize;
        bl.versionId = this.versionId;
        bl.errorCode = StringUtils.replace(this.errorCode, " ", "");
        bl.status = this.status;
        bl.ipAddress = this.ipAddress;       
        bl.bucketOwnerName = this.bucketOwnerName;
        bl.bucketName = this.bucketName;
        bl.requesterName = this.requesterName;
        bl.objectName = this.objectName;
        bl.operation = this.operation;
        bl.URI = this.URI;
        bl.userAgent = this.userAgent;
        bl.realRequestLength = this.realRequestLength;
        bl.realResponseLength = this.realResponseLength;
        bl.preProcessTime = this.preProcessTime;
        bl.getMetaTime = this.getMetaTime;
        bl.putMetaTime = this.putMetaTime;
        bl.deleteMetaTime = this.deleteMetaTime;
        bl.pinTime = this.pinTime;
        bl.requestId = this.requestId;
        bl.contentLength = this.contentLength;
        bl.prepareLogTime = this.prepareLogTime;
        bl.writeBucketLogTime = this.writeBucketLogTime;
        bl.exception = this.exception;
        bl.ostorId = this.ostorId;
        bl.ostorKey = this.ostorKey;
        bl.metaRegion = this.metaRegion;
        bl.dataRegion = this.dataRegion;
        bl.etag = this.etag;
        bl.originRequestId = this.originRequestId;
        bl.replicaMode = this.replicaMode;
        return bl;
    }
}