package cn.ctyun.oos.server;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import javax.management.MBeanServer;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.http.HttpHeaders;
import org.apache.logging.log4j.ThreadContext;
import org.eclipse.jetty.io.Buffer;
import org.eclipse.jetty.io.NetworkTrafficListener;
import org.eclipse.jetty.jmx.MBeanContainer;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.server.nio.NetworkTrafficSelectChannelConnector;
import org.eclipse.jetty.server.ssl.SslSelectChannelConnector;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.json.JSONArray;
import org.json.JSONObject;

import com.amazonaws.AmazonClientException;
import com.amazonaws.HttpMethod;
import com.amazonaws.services.s3.Headers;
import com.amazonaws.services.s3.internal.XmlWriter;
import com.amazonaws.services.s3.model.CORSRule;
import com.amazonaws.services.s3.model.CORSRule.AllowedMethods;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.util.json.JSONException;

import cn.ctyun.common.BaseException;
import cn.ctyun.common.Consts;
import cn.ctyun.common.Program;
import cn.ctyun.common.conf.DataRegion;
import cn.ctyun.common.conf.GlobalHHZConfig;
import cn.ctyun.common.conf.OOSConfig;
import cn.ctyun.common.conf.RegionHHZConfig;
import cn.ctyun.common.ganglia.GangliaConsts;
import cn.ctyun.common.ganglia.GangliaPlugin;
import cn.ctyun.common.model.BucketLifecycleConfiguration.Rule;
import cn.ctyun.common.region.MetaRegionMapping;
import cn.ctyun.common.utils.LogUtils;
import cn.ctyun.oos.bssAdapter.server.BssAdapterClient;
import cn.ctyun.oos.common.AuthResult;
import cn.ctyun.oos.common.BandWidthControlInputStream;
import cn.ctyun.oos.common.ConcurrentConnectionsLimit;
import cn.ctyun.oos.common.ErrorMessage;
import cn.ctyun.oos.common.OOSActions;
import cn.ctyun.oos.common.OOSPermissions;
import cn.ctyun.oos.common.OOSRequestId;
import cn.ctyun.oos.common.UserBandWidthLimit;
import cn.ctyun.oos.common.UserConnectionsLimit;
import cn.ctyun.oos.common.Utils;
import cn.ctyun.oos.common.WebsiteHeader;
import cn.ctyun.oos.hbase.HBaseAccelerate;
import cn.ctyun.oos.hbase.HBaseAkSk;
import cn.ctyun.oos.hbase.HBaseBucket;
import cn.ctyun.oos.hbase.HBaseCdnVendor;
import cn.ctyun.oos.hbase.HBaseCloudTrail;
import cn.ctyun.oos.hbase.HBaseConnectionManager;
import cn.ctyun.oos.hbase.HBaseInitialUpload;
import cn.ctyun.oos.hbase.HBaseManageEvent;
import cn.ctyun.oos.hbase.HBaseManagerLog;
import cn.ctyun.oos.hbase.HBaseMinutesUsage;
import cn.ctyun.oos.hbase.HBaseNoninternetIP;
import cn.ctyun.oos.hbase.HBaseObject;
import cn.ctyun.oos.hbase.HBaseOwner;
import cn.ctyun.oos.hbase.HBaseRole;
import cn.ctyun.oos.hbase.HBaseTokenMeta;
import cn.ctyun.oos.hbase.HBaseUpload;
import cn.ctyun.oos.hbase.HBaseUserToRole;
import cn.ctyun.oos.hbase.HBaseUserToTag;
import cn.ctyun.oos.hbase.HBaseUserType;
import cn.ctyun.oos.hbase.HBaseVSNTag;
import cn.ctyun.oos.hbase.MetaClient;
import cn.ctyun.oos.iam.accesscontroller.AccessEffect;
import cn.ctyun.oos.metadata.BucketMeta;
import cn.ctyun.oos.metadata.CloudTrailEvent;
import cn.ctyun.oos.metadata.CloudTrailEvent.Resources;
import cn.ctyun.oos.metadata.CloudTrailManageEvent;
import cn.ctyun.oos.metadata.InitialUploadMeta;
import cn.ctyun.oos.metadata.ManageEventMeta;
import cn.ctyun.oos.metadata.ObjectMeta;
import cn.ctyun.oos.metadata.ObjectMeta.VersionType;
import cn.ctyun.oos.metadata.OwnerMeta;
import cn.ctyun.oos.metadata.UploadMeta;
import cn.ctyun.oos.mq.OOSMq.OOSMqType;
import cn.ctyun.oos.ostor.OstorProxy;
import cn.ctyun.oos.ostor.hbase.HbasePdiskInfo;
import cn.ctyun.oos.ostor.hbase.OperationLog;
import cn.ctyun.oos.server.MultiVersionsController.MultiVersionsOpt;
import cn.ctyun.oos.server.OpBucket.CORSRequestType;
import cn.ctyun.oos.server.backoff.Backoff;
import cn.ctyun.oos.server.conf.BackoffConfig;
import cn.ctyun.oos.server.conf.BssAdapterConfig;
import cn.ctyun.oos.server.count.Pin;
import cn.ctyun.oos.server.count.PinData;
import cn.ctyun.oos.server.formpost.FormPostHeaderNotice;
import cn.ctyun.oos.server.formpost.FormPostTool;
import cn.ctyun.oos.server.log.BucketLog;
import cn.ctyun.oos.server.signer.V4Signer;
import cn.ctyun.oos.server.storage.Storage;
import cn.ctyun.oos.server.usage.FiveMinuteUsage;
import cn.ctyun.oos.server.util.AccessControlUtils;
import cn.ctyun.oos.server.util.ObjectLockUtils;
import cn.ctyun.oos.server.util.ResourceAccessControlUtils;
import common.time.TimeUtils;
import common.tuple.Pair;
import common.util.BlockingExecutor;
import common.util.GetOpt;

class HttpHandler extends AbstractHandler {
    private static final Log log = LogFactory.getLog(HttpHandler.class);
    private int port;
    private int sslPort;
    private Pin pin;
    private static BlockingExecutor executor;
    private static MetaClient client = MetaClient.getGlobalClient();
    private static final InternalClient internalClient = InternalClient.getInstance();

    public HttpHandler(final Connector[] connectors, int port, int sslPort) throws Exception {
        this.port = port;
        this.sslPort = sslPort;
        this.pin = new Pin();
        HttpHandler.executor = new BlockingExecutor(OOSConfig.getSmallCoreThreadsNum(),
                OOSConfig.getSmallMaxThreadsNum(), OOSConfig.getSmallQueueSize(),
                OOSConfig.getSmallAliveTime(), "ResponseThread");
    }
    
    public static void writeResponseEntity(HttpServletResponse resp, String body,
            BucketLog bucketLog) {
        bucketLog.adapterResponseStartTime = System.currentTimeMillis();
        resp.setCharacterEncoding(Consts.STR_UTF8);
        resp.setHeader(Headers.CONTENT_TYPE, "application/xml");
        Utils.setCommonHeader(resp, new Date(bucketLog.startTime), bucketLog.requestId);
        if (resp.getHeader("Connection") == null)
            resp.setHeader("Connection", "close");
        if (body != null) {
            resp.setIntHeader(Headers.CONTENT_LENGTH, body.getBytes(Consts.CS_UTF8).length);
            bucketLog.adapterResponseFirstTime = System.currentTimeMillis();
            try {
                resp.getOutputStream().write(body.getBytes(Consts.STR_UTF8));
            } catch (Exception e) {
                log.error("catch write response error");
            }
            bucketLog.adapterResponseLastTime = System.currentTimeMillis();
            if(bucketLog.realResponseLength == 0)
                bucketLog.realResponseLength = body.length();
        } else
            resp.setIntHeader(Headers.CONTENT_LENGTH, 0);
    }

    void opService(HttpServletRequest req, HttpServletResponse resp, OwnerMeta owner,
            BucketLog bucketLog, PinData pinData, AuthResult authResult) throws IOException, BaseException, org.json.JSONException {
        String method = req.getMethod();
        try {
            if (owner != null && owner.auth != null && !owner.auth.getServicePermission().hasGet()) {
                throw new BaseException(403, "permissionIsNotAllow", owner.auth.getReason_ch());
            }
            if (!method.equalsIgnoreCase(HttpMethod.GET.toString()))
                throw new BaseException(405, ErrorMessage.ERROR_CODE_405,
                        ErrorMessage.ERROR_MESSAGE_405);
            String respMsg = "";
            if (req.getParameter("regions") != null) {
                pinData.jettyAttributes.action = OOSActions.Regions_Get;
                // 访问控制
                AccessControlUtils.auth(req, OOSActions.Regions_Get, authResult);
                Pair<Set<String>, Set<String>> res = client.getShowRegions(owner.getId());
                respMsg = OpService.getRegions(res);
                
                // 管理事件请求参数
                if (owner.ifRecordManageEvent) {
                    JSONObject jo = new JSONObject();
                    if (req.getHeader("Host") != null)
                        jo.put("host", req.getHeader("Host"));
                    pinData.jettyAttributes.manageEventAttribute.reqContent = jo;
                }
            } else {
                pinData.jettyAttributes.action = OOSActions.Service_Get;
                // 访问控制
                AccessControlUtils.auth(req, OOSActions.Service_Get, authResult);
                List<BucketMeta> list = client.bucketList(owner.getId());
                respMsg = OpService.listAllMyBucketsResult(owner, list);
                
                // 管理事件请求参数
                if (owner.ifRecordManageEvent) {
                    JSONObject jo = new JSONObject();
                    if (req.getHeader("Host") != null)
                        jo.put("host", req.getHeader("Host"));
                    pinData.jettyAttributes.manageEventAttribute.reqContent = jo;
                }
            }
            writeResponseEntity(resp, respMsg, bucketLog);
        } finally {
            String ipAddr = Utils.getIpAddr(req);
            boolean isInternetIP = Utils.isInternetIP(ipAddr);
            pinData.jettyAttributes.respHeadFlow = Utils.getRespHeaderLength(resp);
            Utils.pinTransfer(owner.getId(), pin, pinData, isInternetIP, null);
            pinData.setRequestInfo(owner.getId(), null, isInternetIP, method);
        }
    }

    void opBucket(HttpServletRequest req, HttpServletResponse resp, BucketMeta dbBucket,
            OwnerMeta owner, InputStream ip, boolean bucketExists, BucketLog bucketLog, PinData pinData, AuthResult authResult)
            throws Exception {
        String method = req.getMethod();
        String bucket = dbBucket.getName();
        pinData.jettyAttributes.manageEventAttribute.bucketName = bucket;
        if (method.equalsIgnoreCase(HttpMethod.PUT.toString())) {            
            try {
                if (owner != null && owner.auth != null && !owner.auth.getBucketPermission().hasPut()){
                    throw new BaseException(403, 
                            "permissionIsNotAllow", owner.auth.getReason_ch());
                }
                
                if (!req.getParameterMap().isEmpty()) {
                    if (bucketExists == false)
                        throw new BaseException(404, ErrorMessage.ERROR_CODE_NO_SUCH_BUCKET, ErrorMessage.ERROR_MESSAGE_404);
                    if (owner == null || owner.getId() != dbBucket.getOwnerId())
                        throw new BaseException(403, ErrorMessage.ERROR_CODE_403);
                }
                if (req.getParameterMap().containsKey("policy")) {
                    pinData.jettyAttributes.action = OOSActions.Bucket_Put_Policy;
                    // bucket访问控制
                    AccessControlUtils.auth(req, OOSActions.Bucket_Put_Policy, dbBucket, authResult);
                    OpBucket.putPolicy(dbBucket, ip, req.getContentLength());
                    
                    // 管理事件请求参数
                    if (owner.ifRecordManageEvent) {
                        JSONObject jo = new JSONObject();
                        if (dbBucket.policy != null) {
                            jo.put("bucketPolicy", dbBucket.policy);
                        }
                        pinData.jettyAttributes.manageEventAttribute.reqContent = jo;
                    }
                } else if (req.getParameterMap().containsKey("website")) {
                    pinData.jettyAttributes.action = OOSActions.Bucket_Put_WebSite;
                    // bucket访问控制
                    AccessControlUtils.auth(req, OOSActions.Bucket_Put_WebSite, dbBucket, authResult);
                    OpBucket.putWebSite(dbBucket, ip);
                    
                    // 管理事件请求参数
                    if (owner.ifRecordManageEvent) {
                        JSONObject jo = new JSONObject();
                        JSONObject jo2 = new JSONObject();
                        if (dbBucket.indexDocument != null)
                            jo2.put("Suffix", dbBucket.indexDocument);
                        if (dbBucket.errorDocument != null)
                            jo2.put("Key", dbBucket.errorDocument);
                        jo.put("websiteConfigurationRule", jo2);
                        pinData.jettyAttributes.manageEventAttribute.reqContent = jo;
                    }
                } else if (req.getParameterMap().containsKey("logging")) {
                    pinData.jettyAttributes.action = OOSActions.Bucket_Put_Logging;
                    // bucket访问控制
                    AccessControlUtils.auth(req, OOSActions.Bucket_Put_Logging, dbBucket, authResult);
                    OpBucket.putLogging(dbBucket, owner, ip);
                    
                    // 管理事件请求参数
                    if (owner.ifRecordManageEvent) {
                        JSONObject jo = new JSONObject();
                        JSONObject jo2 = new JSONObject();
                        jo2.put("TargetBucket", dbBucket.logTargetBucket);
                        if (dbBucket.logTargetPrefix != null)
                            jo2.put("TargetPrefix", dbBucket.logTargetPrefix);
                        jo.put("loggingRule", jo2);
                        pinData.jettyAttributes.manageEventAttribute.reqContent = jo;
                    }
                } else if (req.getParameterMap().containsKey("lifecycle")) {
                    pinData.jettyAttributes.action = OOSActions.Bucket_Put_Lifecycle;
                    // bucket访问控制
                    AccessControlUtils.auth(req, OOSActions.Bucket_Put_Lifecycle, dbBucket, authResult);
                    OpBucket.putBucketLifecycle(dbBucket, ip,
                            Long.parseLong(req.getHeader(Headers.CONTENT_LENGTH)),
                            req.getHeader(Headers.CONTENT_MD5));

                    // 管理事件请求参数
                    if (owner.ifRecordManageEvent) {
                        JSONObject jo = new JSONObject();
                        JSONObject jo2 = new JSONObject();
                        JSONArray ja = new JSONArray();
                        for (Rule rule : dbBucket.lifecycle.getRules()) {
                            JSONObject ruleJo = new JSONObject();
                            if (rule.getId() != null)
                                ruleJo.put("ID", rule.getId());
                            ruleJo.put("Status", rule.getStatus());
                            ruleJo.put("Prefix", rule.getPrefix());
                            if (rule.getExpirationInDays() != -1) {
                                ruleJo.put("Expiration", rule.getExpirationInDays());
                            } else {
                                if (rule.getExpirationDate() != null)
                                    ruleJo.put("Expiration", rule.getExpirationDate());
                            }
                            ja.put(ruleJo);
                        }
                        jo2.put("Rule", ja);
                        jo.put("lifecycleConfiguration", jo2);
                        pinData.jettyAttributes.manageEventAttribute.reqContent = jo;
                    }

                } else if (req.getParameterMap().containsKey("accelerate")) {
                    pinData.jettyAttributes.action = OOSActions.Bucket_Put_Accelerate;
                    // bucket访问控制
                    AccessControlUtils.auth(req, OOSActions.Bucket_Put_Accelerate, dbBucket, authResult);
                    OpBucket.putAccelerate(dbBucket, ip);
                } else if (req.getParameterMap().containsKey("cors")) {
                    pinData.jettyAttributes.action = OOSActions.Bucket_Put_Cors;
                    // bucket访问控制
                    AccessControlUtils.auth(req, OOSActions.Bucket_Put_Cors, dbBucket, authResult);
                    OpBucket.putBucketCors(dbBucket, ip, req.getContentLength());
                    
                    // 管理事件请求参数
                    if (owner.ifRecordManageEvent) {
                        JSONObject jo = new JSONObject();
                        JSONObject jo2 = new JSONObject();
                        JSONArray ja = new JSONArray();
                        for (CORSRule rule : dbBucket.cors.getRules()) {
                            JSONObject corsJo = new JSONObject();
                            corsJo.put("AllowedOrigin", rule.getAllowedOrigins());
                            if (rule.getAllowedHeaders() != null)
                                corsJo.put("AllowedHeader", rule.getAllowedHeaders());
                            JSONArray mJa = new JSONArray();
                            for (AllowedMethods m : rule.getAllowedMethods()) {
                                mJa.put(m.toString());
                            }
                            corsJo.put("AllowedMethod", mJa);
                            if (rule.getMaxAgeSeconds() > 0)
                                corsJo.put("MaxAgeSeconds", rule.getMaxAgeSeconds());
                            if (rule.getExposedHeaders() != null)
                                corsJo.put("ExposeHeader", rule.getExposedHeaders());
                            ja.put(corsJo);
                        }
                        jo2.put("Rule", ja);
                        jo.put("CORSConfiguration", jo2);
                        pinData.jettyAttributes.manageEventAttribute.reqContent = jo;
                    }
                } else if (req.getParameterMap().containsKey("versioning")) {
                    pinData.jettyAttributes.action = OOSActions.Bucket_Put_Versioning;
                    OpBucket.putBucketVersioning(dbBucket, ip, req.getContentLength());
                } else if (req.getParameterMap().containsKey("object-lock")) {
                    // 添加对象锁定配置
                    pinData.jettyAttributes.action = OOSActions.Bucket_Put_ObjectLockConfiguration;
                    // 访问控制
                    AccessControlUtils.auth(req, OOSActions.Bucket_Put_ObjectLockConfiguration, dbBucket, authResult);
                    OpBucket.putObjectLockConfiguration(dbBucket, ip, req, pinData, owner.ifRecordManageEvent);
                } else {
                    if (bucketExists && dbBucket.getOwnerId() != owner.getId())
                        throw new BaseException(409, "BucketAlreadyExists", "BucketAlreadyExists");
                    if (bucketExists && req.getHeader(Headers.S3_CANNED_ACL) != null) {
                        pinData.jettyAttributes.action = OOSActions.Bucket_Put_Acl;
                    } else {
                        pinData.jettyAttributes.action = OOSActions.Bucket_Put;
                    }
                    // bucket访问控制
                    AccessControlUtils.auth(req, OOSActions.Bucket_Put, dbBucket, authResult);
                    if (!bucketExists && !BssAdapterClient.containsRegion(owner.getId(), BssAdapterConfig.localPool.name)) {
                        throw new BaseException(403, "UnaccessibleRegion");
                    }
                    OpBucket.put(dbBucket, ip, bucketExists, owner,
                            req.getHeader(Headers.S3_CANNED_ACL), resp);

                    // 管理事件请求参数
                    if (owner != null && owner.ifRecordManageEvent) {
                        JSONObject config = new JSONObject();

                        JSONObject jo = new JSONObject();
                        JSONObject jo2 = new JSONObject();
                        jo2.put("Location", dbBucket.metaLocation);
                        jo.put("MetadataLocationConstraint", jo2);

                        JSONObject jo3 = new JSONObject();
                        jo3.put("Type", dbBucket.dataLocation.getType().toString());
                        jo3.put("LocationList", dbBucket.dataLocation.getDataRegions());
                        jo3.put("ScheduleStrategy", dbBucket.dataLocation.getStragegy().toString());
                        jo.put("DataLocationConstraint", jo3);

                        config.put("bucketConfiguration", jo);
                        pinData.jettyAttributes.manageEventAttribute.reqContent = config;
                    }
                }
                resp.setStatus(200);
                Utils.setCommonHeader(resp,new Date(bucketLog.startTime),bucketLog.requestId);
            } finally {
                String ipAddr = Utils.getIpAddr(req);
                boolean isInternetIP = Utils.isInternetIP(ipAddr);
                // 如果请求的bucket不存在，计量到请求发送者的请求次数
                if (bucketExists) {
                    Utils.pinUpload(dbBucket.getOwnerId(), pin, pinData, isInternetIP, dbBucket.name);
                    pinData.setRequestInfo(dbBucket.getOwnerId(), dbBucket.name, isInternetIP, method);
                } else if(owner != null){
                    Utils.pinUpload(owner.getId(), pin, pinData, isInternetIP, null);
                    pinData.setRequestInfo(owner.getId(), null, isInternetIP, method);
                }
            }
        } else if (method.equalsIgnoreCase(HttpMethod.GET.toString())) {
            try {
                if (owner != null && owner.auth != null
                        && !owner.auth.getBucketPermission().hasGet()) {
                    throw new BaseException(403, "permissionIsNotAllow", owner.auth.getReason_ch());
                }
                if (bucketExists == false)
                    throw new BaseException(404, ErrorMessage.ERROR_CODE_NO_SUCH_BUCKET, ErrorMessage.ERROR_MESSAGE_404);
                if (req.getParameterMap().containsKey("policy")) {
                    pinData.jettyAttributes.action = OOSActions.Bucket_Get_Policy;
                    if (owner == null || owner.getId() != dbBucket.getOwnerId())
                        throw new BaseException(403, ErrorMessage.ERROR_CODE_403);
                    // bucket访问控制
                    AccessControlUtils.auth(req, OOSActions.Bucket_Get_Policy, dbBucket, authResult);
                    if (dbBucket.policy == null || dbBucket.policy.trim().length() == 0)
                        throw new BaseException(404, "NotSuchBucketPolicy");
                    resp.setStatus(200);
                    writeResponseEntity(resp, dbBucket.policy, bucketLog);
                    
                } else if (req.getParameterMap().containsKey("website")) {
                    pinData.jettyAttributes.action = OOSActions.Bucket_Get_WebSite;
                    if (owner == null || owner.getId() != dbBucket.getOwnerId())
                        throw new BaseException(403, ErrorMessage.ERROR_CODE_403);
                    // bucket访问控制
                    AccessControlUtils.auth(req, OOSActions.Bucket_Get_WebSite, dbBucket, authResult);
                    resp.setStatus(200);
                    writeResponseEntity(resp, OpBucket.getBucketWebSite(dbBucket), bucketLog);                  
                    
                } else if (req.getParameterMap().containsKey("logging")) {
                    pinData.jettyAttributes.action = OOSActions.Bucket_Get_logging;
                    if (owner == null || owner.getId() != dbBucket.getOwnerId())
                        throw new BaseException(403, ErrorMessage.ERROR_CODE_403);
                    // bucket访问控制
                    AccessControlUtils.auth(req, OOSActions.Bucket_Get_logging, dbBucket, authResult);
                    resp.setStatus(200);
                    writeResponseEntity(resp, OpBucket.getLogging(dbBucket), bucketLog);                    
                } else if (req.getParameter("uploads") != null) {
                    pinData.jettyAttributes.action = OOSActions.Bucket_Get_MultipartUploads;
                    AccessEffect accessEffect = ResourceAccessControlUtils.checkBucketACLsAndPolicy(req, dbBucket, authResult, OOSPermissions.ListBucketMultipartUploads.toString());
                    // 传入基于资源的访问控制的结果，进行bucket访问控制
                    AccessControlUtils.auth(req, OOSActions.Bucket_Get_MultipartUploads, dbBucket, null, authResult, accessEffect);
                    int maxUploads = Consts.LIST_UPLOAD_PART_MAXNUM;
                    if (req.getParameter("max-uploads") != null)
                        maxUploads = Integer.parseInt(req.getParameter("max-uploads"));
                    if (maxUploads > Consts.LIST_UPLOAD_PART_MAXNUM || maxUploads < 1)
                        throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_ARGUMENT, "maxUploads should between 0~1000");
                    String delimiter = req.getParameter("delimiter");
                    if (delimiter != null && delimiter.length() != 1)
                        throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_ARGUMENT, "The length of delimiter must be 1.");
                    String keyMarker = req.getParameter("key-marker");
                    String prefix = "";
                    if (req.getParameter("prefix") != null) {
                        prefix = req.getParameter("prefix");
                    }
                    String uploadIdMarker = req.getParameter("upload-id-marker");
                    String body = OpBucket
                            .listMultipartUploads(dbBucket.metaLocation, dbBucket.name, maxUploads, delimiter, prefix,
                                    keyMarker, uploadIdMarker, bucketLog.requestId);// 查询upload表，统计时间
                    resp.setStatus(200);
                    writeResponseEntity(resp, body, bucketLog);
                   
                } else if (req.getParameterMap().containsKey("lifecycle")) {
                    pinData.jettyAttributes.action = OOSActions.Bucket_Get_Lifecycle;
                    if (owner == null || owner.getId() != dbBucket.getOwnerId())
                        throw new BaseException(403, ErrorMessage.ERROR_CODE_403);
                    // bucket访问控制
                    AccessControlUtils.auth(req, OOSActions.Bucket_Get_Lifecycle, dbBucket, authResult);
                    writeResponseEntity(resp, OpBucket.getBucketLifecycle(dbBucket), bucketLog);
                } else if (req.getParameterMap().containsKey("accelerate")) {
                    pinData.jettyAttributes.action = OOSActions.Bucket_Get_Accelerate;
                    if (owner == null || owner.getId() != dbBucket.getOwnerId())
                        throw new BaseException(403, ErrorMessage.ERROR_CODE_403);
                    // bucket访问控制
                    AccessControlUtils.auth(req, OOSActions.Bucket_Get_Accelerate, dbBucket, authResult);
                    resp.setStatus(200);
                    writeResponseEntity(resp, OpBucket.getAccelerate(dbBucket.name), bucketLog);
                } else if (req.getParameterMap().containsKey("location")) {
                    pinData.jettyAttributes.action = OOSActions.Bucket_Get_Location;
                    if (owner == null || owner.getId() != dbBucket.getOwnerId())
                        throw new BaseException(403, ErrorMessage.ERROR_CODE_403);
                    // bucket访问控制
                    AccessControlUtils.auth(req, OOSActions.Bucket_Get_Location, dbBucket, authResult);
                    writeResponseEntity(resp, OpBucket.getBucketLocation(dbBucket), bucketLog);
                } else if (req.getParameterMap().containsKey("cors")) {
                    pinData.jettyAttributes.action = OOSActions.Bucket_Get_Cors;
                    if (owner == null || owner.getId() != dbBucket.getOwnerId())
                        throw new BaseException(403, ErrorMessage.ERROR_CODE_403);
                    // bucket访问控制
                    AccessControlUtils.auth(req, OOSActions.Bucket_Get_Cors, dbBucket, authResult);
                    writeResponseEntity(resp, OpBucket.getBucketCors(dbBucket), bucketLog);
                } else if (req.getParameterMap().containsKey("versioning")) {
                    pinData.jettyAttributes.action = OOSActions.Bucket_Get_Versioning;
                    if (owner == null || owner.getId() != dbBucket.getOwnerId())
                        throw new BaseException(403, ErrorMessage.ERROR_CODE_403);
                    writeResponseEntity(resp, OpBucket.getBucketVersioning(dbBucket), bucketLog);
                } else if (req.getParameterMap().containsKey("subresource")) {
                    // 返回bucket多个属性，如query为/?subresource=acl,policy，返回bucket的acl和policy属性
                    pinData.jettyAttributes.action = OOSActions.Bucket_Get_Subresource;
                    if (owner == null || dbBucket.getOwnerId() != owner.getId())
                        throw new BaseException(403, "AccessDenied");
                    String respMsg = OpBucket.getBucketSubresource(authResult, dbBucket, req, resp);
                    resp.setStatus(200);
                    writeResponseEntity(resp, respMsg, bucketLog);
                } else if (req.getParameterMap().containsKey("object-lock")) {
                    // 获取对象锁定配置
                    pinData.jettyAttributes.action = OOSActions.Bucket_Get_ObjectLockConfiguration;
                    // 其他根用户不允许访问
                    if (owner == null || owner.getId() != dbBucket.getOwnerId()) {
                        throw new BaseException(403, ErrorMessage.ERROR_CODE_403);
                    }
                    // 访问控制
                    AccessControlUtils.auth(req, OOSActions.Bucket_Get_ObjectLockConfiguration, dbBucket, authResult);
                    String respMsg = OpBucket.getObjectLockConfiguration(dbBucket);
                    resp.setStatus(200);
                    writeResponseEntity(resp, respMsg, bucketLog);
                } else {
                    /*
                     * 1.if the bucket not exists, throw exception. 2.if the
                     * bucket exists,but the user can not read it,throw
                     * AccessDenied Exception. 3.then check whether it is a get
                     * ACL operation or listobjects operation
                     */
                    if (dbBucket.indexDocument != null
                            && dbBucket.indexDocument.trim().length() != 0
                            && (checkWebsiteHost(req.getHeader("Host"), dbBucket.getName()) || req
                                    .getHeader("Host").equals(dbBucket.getName()))) {
                        if (!ifCanRead(owner, dbBucket))
                            throw new BaseException(403, ErrorMessage.ERROR_CODE_403);
                        resp.sendRedirect(req.getRequestURL()
                                + URLEncoder.encode(dbBucket.indexDocument, Consts.STR_UTF8));
                    } else if (req.getParameterMap().containsKey("acl")) {
                        pinData.jettyAttributes.action = OOSActions.Bucket_Get_Acl;
                        if (!ifCanRead(owner, dbBucket))
                            throw new BaseException(403, ErrorMessage.ERROR_CODE_403);
                        if (dbBucket.getOwnerId() != owner.getId())
                            throw new BaseException(403, ErrorMessage.ERROR_CODE_403);
                        // bucket访问控制
                        AccessControlUtils.auth(req, OOSActions.Bucket_Get_Acl, dbBucket, authResult);
                        String respMsg = OpBucket.getBucketACLResult(owner, dbBucket);
                        resp.setStatus(200);
                        resp.setHeader(Headers.LAST_MODIFIED,
                                TimeUtils.toGMTFormat(new Date(dbBucket.createDate)));
                        writeResponseEntity(resp, respMsg, bucketLog);
                    } else {
                        AccessEffect accessEffect = ResourceAccessControlUtils.checkBucketACLsAndPolicy(req, dbBucket, authResult, OOSPermissions.ListBucket.toString());
                        pinData.jettyAttributes.action = OOSActions.Bucket_Get;
                        // 传入基于资源的访问控制的结果，进行bucket访问控制
                        AccessControlUtils.auth(req, OOSActions.Bucket_Get, dbBucket, null, authResult, accessEffect);
                        String prefix = req.getParameter("prefix");
                        if (req.getParameter("urlencode") != null
                                && req.getParameter("urlencode").trim().length() != 0)
                            prefix = URLDecoder.decode(prefix, Consts.STR_UTF8);
                        String marker = req.getParameter("marker");
                        int maxKeys;
                        if (req.getParameter("max-keys") != null)
                            maxKeys = Integer.parseInt(req.getParameter("max-keys"));
                        else
                            maxKeys = Consts.LIST_OBJECTS_MAX_KEY;
                        if ((maxKeys < 0) || (maxKeys > Consts.LIST_OBJECTS_MAX_KEY))
                            throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_ARGUMENT,
                                    "maxKeys should between 0~" + Consts.LIST_OBJECTS_MAX_KEY);
                        String delimiter = req.getParameter("delimiter");
                        if (delimiter != null && delimiter.length() != 1)
                            throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_ARGUMENT,
                                    "The length of delimiter must be 1.");
                        String respMsg;
                        if (MetaRegionMapping.needMapping(dbBucket.metaLocation)) {
                            String dataRegion = MetaRegionMapping.getMappedDataRegion(dbBucket.metaLocation);
                            respMsg = internalClient.listObjects(dataRegion, dbBucket.metaLocation, bucket, prefix, marker, maxKeys, delimiter,
                                    bucketLog.requestId);
                        } else {
                            ObjectListing ol = null;
                            LogUtils.log("Now listing objects from bucket:" + bucket + " prefix:" + prefix + " marker:"
                                    + marker + " delimiter:" + delimiter + " maxkeys:" + maxKeys);
                            long listMetaTime = System.currentTimeMillis();
                            ObjectMeta object = new ObjectMeta(prefix == null ? "" : prefix, bucket, dbBucket.metaLocation);
                            if (client.isBusy(object, OOSConfig.getMaxConcurrencyPerRegionServer()))
                                Backoff.backoff();
                            ol = client.objectList(dbBucket.metaLocation, bucket, prefix, marker, delimiter, maxKeys);
                            listMetaTime = System.currentTimeMillis() - listMetaTime;
                            LogUtils.log(
                                    "List objects from bucket:" + bucket + " prefix:" + prefix + " marker:" + marker + " delimiter:" + delimiter + " maxkeys:"
                                            + maxKeys + " success. Get metadata takes " + listMetaTime + "ms.");
                            respMsg = OpBucket.listObjectsResult(ol);
                        }
                        resp.setStatus(200);
                        writeResponseEntity(resp, respMsg, bucketLog);
                    }
                }
                client.bucketSelect(dbBucket);
            } finally {
                String ipAddr = Utils.getIpAddr(req);
                boolean isInternetIP = Utils.isInternetIP(ipAddr);
                pinData.jettyAttributes.respHeadFlow = Utils.getRespHeaderLength(resp);
                // 如果请求的bucket不存在，计量到请求发送者的请求次数
                if (bucketExists) {
                    Utils.pinTransfer(dbBucket.getOwnerId(), pin, pinData, isInternetIP, dbBucket.name);
                    pinData.setRequestInfo(dbBucket.getOwnerId(), dbBucket.name, isInternetIP, method);
                } else  if(owner != null) {
                    Utils.pinTransfer(owner.getId(), pin, pinData, isInternetIP, null);
                    pinData.setRequestInfo(owner.getId(), null, isInternetIP, method);
                }
            }
        } else if (method.equalsIgnoreCase(HttpMethod.DELETE.toString())) {
            try {
                if (owner != null && owner.auth != null && !owner.auth.getBucketPermission().hasDelete()){
                    throw new BaseException(403, 
                            "permissionIsNotAllow", owner.auth.getReason_ch());
                }
                
                if (bucketExists == false)
                    throw new BaseException(404, ErrorMessage.ERROR_CODE_NO_SUCH_BUCKET, ErrorMessage.ERROR_MESSAGE_404);
                if (owner == null || owner.getId() != dbBucket.getOwnerId())
                    throw new BaseException(403, ErrorMessage.ERROR_CODE_403);
                if (req.getParameterMap().containsKey("policy")) {
                    pinData.jettyAttributes.action = OOSActions.Bucket_Delete_Policy;
                    // bucket访问控制
                    AccessControlUtils.auth(req, OOSActions.Bucket_Delete_Policy, dbBucket, authResult);
                    if (dbBucket.policy == null || dbBucket.policy.trim().length() == 0)
                        resp.setStatus(204);
                    else {
                        dbBucket.policy = "";
                        client.bucketUpdate(dbBucket);
                        resp.setStatus(200);
                    }
                } else if (req.getParameterMap().containsKey("website")) {
                    pinData.jettyAttributes.action = OOSActions.Bucket_Delete_WebSite;
                    // bucket访问控制
                    AccessControlUtils.auth(req, OOSActions.Bucket_Delete_WebSite, dbBucket, authResult);
                    dbBucket.indexDocument = "";
                    dbBucket.errorDocument = "";
                    client.bucketUpdate(dbBucket);
                    resp.setStatus(200);
                } else if (req.getParameterMap().containsKey("lifecycle")) {
                    pinData.jettyAttributes.action = OOSActions.Bucket_Delete_Lifecycle;
                    // bucket访问控制
                    AccessControlUtils.auth(req, OOSActions.Bucket_Delete_Lifecycle, dbBucket, authResult);
                    dbBucket.lifecycle = null;
                    client.bucketUpdate(dbBucket);
                    resp.setStatus(204);
                } else if (req.getParameterMap().containsKey("cors")) {
                    pinData.jettyAttributes.action = OOSActions.Bucket_Delete_Cors;
                    // bucket访问控制
                    AccessControlUtils.auth(req, OOSActions.Bucket_Delete_Cors, dbBucket, authResult);
                    if (dbBucket.cors == null)
                        resp.setStatus(204);
                    else {
                        dbBucket.cors = null;
                        client.bucketUpdate(dbBucket);
                        resp.setStatus(200);
                    }
                } else if (req.getParameterMap().containsKey("object-lock")) {
                    // 删除对象锁定配置
                    pinData.jettyAttributes.action = OOSActions.Bucket_Delete_ObjectLockConfiguration;
                    //访问控制
                    AccessControlUtils.auth(req, OOSActions.Bucket_Delete_ObjectLockConfiguration, dbBucket, authResult);
                    OpBucket.deleteObjectLockConfiguration(dbBucket);
                    resp.setStatus(200);
                } else {
                    pinData.jettyAttributes.action = OOSActions.Bucket_Delete;
                    // bucket访问控制
                    AccessControlUtils.auth(req, OOSActions.Bucket_Delete, dbBucket, authResult);
                    /*
                     * 1. if the bucket not exists,throw NoSuchBucket exception.
                     * 2.if the user is not the bucket owner,throw AccessDenied
                     * exception. 3.if the bucket has objects, throw
                     * BucketNotEmpty exception.
                     */
                    if (!client.bucketIsEmpty(dbBucket.metaLocation, dbBucket.getName())) {
                        throw new BaseException(409, "BucketNotEmpty");
                    }
                    client.bucketDelete(dbBucket);   
                    BssAdapterClient.delete(dbBucket.name,
                            BssAdapterConfig.localPool.ak,
                            BssAdapterConfig.localPool.getSK());
                    resp.setStatus(204);
                }
                Utils.setCommonHeader(resp,new Date(bucketLog.startTime),bucketLog.requestId);
            } finally {
                String ipAddr = Utils.getIpAddr(req);
                boolean isInternetIP = Utils.isInternetIP(ipAddr);
                // 如果请求的bucket不存在，计量到请求发送者的请求次数
                if (bucketExists) {
                    Utils.pinUpload(dbBucket.getOwnerId(), pin, pinData, isInternetIP, dbBucket.name);
                    pinData.setRequestInfo(dbBucket.getOwnerId(), dbBucket.name, isInternetIP, method);
                } else  if(owner != null) {
                    Utils.pinUpload(owner.getId(), pin, pinData, isInternetIP, null);
                    pinData.setRequestInfo(owner.getId(), null, isInternetIP, method);
                }
            }
        } else if (method.equalsIgnoreCase(HttpMethod.HEAD.toString())) {
            try {
                if (owner != null && owner.auth != null && !owner.auth.getBucketPermission().hasGet()){
                    throw new BaseException(403, 
                            "permissionIsNotAllow", owner.auth.getReason_ch());
                }
                pinData.jettyAttributes.action = OOSActions.Bucket_Head;
                if (!bucketExists)
                    throw new BaseException(404, "NoSuchBucket");
                else {
                    AccessEffect accessEffect = ResourceAccessControlUtils.checkBucketACLsAndPolicy(req, dbBucket, authResult, OOSPermissions.ListBucket.toString());
                    // 传入基于资源的访问控制的结果，进行bucket访问控制
                    AccessControlUtils.auth(req, OOSActions.Bucket_Head, dbBucket, null, authResult, accessEffect);
                    resp.setStatus(200);
                }
                Utils.setCommonHeader(resp,new Date(bucketLog.startTime),bucketLog.requestId);
            } finally {
                String ipAddr = Utils.getIpAddr(req);
                boolean isInternetIP = Utils.isInternetIP(ipAddr);
                pinData.jettyAttributes.respHeadFlow = Utils.getRespHeaderLength(resp);
                // 如果请求的bucket不存在，计量到请求发送者的请求次数
                if (bucketExists) {
                    Utils.pinTransfer(dbBucket.getOwnerId(), pin, pinData, isInternetIP, dbBucket.name);
                    pinData.setRequestInfo(dbBucket.getOwnerId(), dbBucket.name, isInternetIP, method);
                } else  if(owner != null) {
                    Utils.pinTransfer(owner.getId(), pin, pinData, isInternetIP, null);
                    pinData.setRequestInfo(owner.getId(), null, isInternetIP, method);
                }
            }
        } else if (method.equalsIgnoreCase(HttpMethod.POST.toString())) {
            List<PinData> pinDataList = new ArrayList<>();
            try {
                if (req.getParameter("delete") != null) {
                    if (bucketExists == false)
                        throw new BaseException(404, ErrorMessage.ERROR_CODE_NO_SUCH_BUCKET, ErrorMessage.ERROR_MESSAGE_404);
                    /*
                     * 批量删除的功能放在了Bucket的POST操作中，所以此处添加对
                     * objcet的Delete权限验证的代码
                     */
                    if (owner != null && owner.auth != null && !owner.auth.getObjectPermission().hasDelete()){
                        throw new BaseException(403, 
                                "permissionIsNotAllow", owner.auth.getReason_ch());
                    }
                    pinData.jettyAttributes.action = OOSActions.Bucket_Delete_MultipleObjects;
                    // bucket访问控制
                    AccessControlUtils.auth(req, OOSActions.Bucket_Delete_MultipleObjects, dbBucket, authResult);
                    String body = OpBucket.deleteMultipleObjectsStatic(req, dbBucket, ip, owner,
                            bucketLog, pinDataList);
                    writeResponseEntity(resp, body, bucketLog);
                } else
                    throw new BaseException(405, ErrorMessage.ERROR_CODE_405);
            } finally {
                String ipAddr = Utils.getIpAddr(req);
                boolean isInternetIP = Utils.isInternetIP(ipAddr);
                Utils.pinUpload(dbBucket.getOwnerId(), pin, pinData, isInternetIP, dbBucket.name);
                pinData.setRequestInfo(dbBucket.ownerId, dbBucket.name, isInternetIP, method);
                for (PinData data : pinDataList) {
                    // 实际上只在成功删除了对象之后才会DecreStorage
                    Utils.pinDecreStorage(dbBucket.getOwnerId(), pin, data, dbBucket.name);
                    Utils.pinDeleteFlow(dbBucket.getOwnerId(), pin, data, isInternetIP, dbBucket.name);
                }
            }
        } else {
            throw new BaseException(405, ErrorMessage.ERROR_CODE_405,
                    ErrorMessage.ERROR_MESSAGE_405);
        }
        V4Signer.checkContentSignatureV4(ip);
    }

    private boolean checkWebsiteHost(String host, String bucketName) {
        for (String w : OOSConfig.getWebSiteEndpoint())
            if (host.equals(bucketName + "." + w))
                return true;
        return false;
    }
    
    void opKey(HttpServletRequest req, HttpServletResponse resp, BucketMeta dbBucket, String key,
            OwnerMeta owner, InputStream ip, BucketLog bucketLog, boolean isPostObject, FormPostHeaderNotice formPostHeaderNotice, PinData pinData, AuthResult authResult, AccessEffect accessEffect) throws Exception {
        String method = req.getMethod();
        int status = 200;
        if (method.equalsIgnoreCase(HttpMethod.GET.toString())) {
            try {
                if (owner != null && owner.auth != null && !owner.auth.getObjectPermission().hasGet()){
                    throw new BaseException(403, 
                            "permissionIsNotAllow", owner.auth.getReason_ch());
                }
                ObjectMeta object = new ObjectMeta(key, dbBucket.getName(), dbBucket.metaLocation);
                if(client.isBusy(object, OOSConfig.getMaxConcurrencyPerRegionServer()))
                    Backoff.backoff();
                boolean objectExists;
                //解析多版本信息
                if(MultiVersionsController.gwMultiVersionEnabled(req, dbBucket)) {
                    MultiVersionsController.parseVersionInfo(object, req);
                    MultiVersionsOpt mvOpt = MultiVersionsController.getMultiVersionsOpt(req);
                    if(mvOpt == MultiVersionsOpt.EQUAL) {
                        bucketLog.getMetaTime = System.currentTimeMillis();
                        try{
                            objectExists = client.objectSelect(object);
                        } finally {
                            bucketLog.getMetaTime = System.currentTimeMillis() - bucketLog.getMetaTime;
                        }
                    }
                    else if(mvOpt == MultiVersionsOpt.EQUAL_OR_LESS) {
                        bucketLog.getMetaTime = System.currentTimeMillis();
                        try{
                            objectExists = client.objectSelectVersionOrLess(object);
                        } finally {
                            bucketLog.getMetaTime = System.currentTimeMillis() - bucketLog.getMetaTime;
                        }
                    }
                    else
                        throw new BaseException(403, "Unsupported MultiVersionsOpt=" + mvOpt);
                } else {
                    bucketLog.getMetaTime = System.currentTimeMillis();
                    try{
                        objectExists = client.objectSelect(object);
                    } finally {
                        bucketLog.getMetaTime = System.currentTimeMillis() - bucketLog.getMetaTime;
                    }
                }
                if(objectExists) {
                    bucketLog.metaRegion = object.metaRegionName;
                    bucketLog.dataRegion = object.dataRegion;
                    bucketLog.ostorId = object.ostorId;
                }
                boolean isWebSite = false;
                if(dbBucket.indexDocument != null
                        && dbBucket.indexDocument.trim().length() != 0
                        && (checkWebsiteHost(req.getHeader("Host"), dbBucket.getName()) || req
                                .getHeader("Host").equals(dbBucket.getName()))){
                    isWebSite = true;
                }
                if (isWebSite && req.getRequestURL().toString().endsWith("/")) {
                    pinData.jettyAttributes.action = OOSActions.Object_Get;
                    resp.sendRedirect(req.getRequestURL()
                            + URLEncoder.encode(dbBucket.indexDocument, Consts.STR_UTF8));
                } else if (!objectExists){
                    throw new BaseException(404, ErrorMessage.ERROR_CODE_NO_SUCH_KEY, ErrorMessage.ERROR_MESSAGE_404);
                } else if(isWebSite && StringUtils.isNotEmpty(object.websiteRedirectLocation)){
                    //处理 bucket为website时，且对象已经设置redirect时的重定向
                    pinData.jettyAttributes.action = OOSActions.Object_Get;
                    resp.sendRedirect(object.websiteRedirectLocation);
                } else if (req.getParameter("uploadId") != null) {
                    pinData.jettyAttributes.action = OOSActions.Object_Get_ListParts;
                    int maxParts = Consts.LIST_UPLOAD_PART_MAXNUM;
                    if (req.getParameter("max-parts") != null) {
                        try {
                            maxParts = Integer.parseInt(req.getParameter("max-parts"));
                        } catch (NumberFormatException e) {
                            throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_ARGUMENT,
                                    "max-parts should between 0~" + Consts.LIST_UPLOAD_PART_MAXNUM);
                        }
                    }
                    if(maxParts < 0 || maxParts > Consts.LIST_UPLOAD_PART_MAXNUM) {
                        throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_ARGUMENT,
                                "max-parts should between 0~" + Consts.LIST_UPLOAD_PART_MAXNUM);
                    }
                    int partNumberMarker = 0;
                    if (req.getParameter("part-number-marker") != null) {
                        try {
                            partNumberMarker = Integer.parseInt(req.getParameter("part-number-marker"));
                        } catch (NumberFormatException e) {
                            throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_ARGUMENT,
                                    "part-number-marker Must be a positive integer" );
                        }
                    }
                    Utils.checkPartNumberMarker(req, partNumberMarker);

                    String body = OpObject
                                .listParts(dbBucket.metaLocation, dbBucket.name, object.name, req.getParameter("uploadId"), maxParts, partNumberMarker, bucketLog);
                    writeResponseEntity(resp, body, bucketLog);

                } else if (req.getParameter("acl") != null) {
                    pinData.jettyAttributes.action = OOSActions.Object_Get_ACL;
                    String body = OpObject.getObjectAcl(pinData, dbBucket);
                    writeResponseEntity(resp, body, bucketLog);
                } else {
                    pinData.jettyAttributes.action = OOSActions.Object_Get;
                    Pair<String, String> limitConf = ConcurrentConnectionsLimit.allow(req, dbBucket.name, key);
                    try {
                        OpObject.getObjectStatics(req, resp, dbBucket, object, bucketLog, pinData, pin);
                        setConnectionHeader(req, resp);
                    } finally {
                        ConcurrentConnectionsLimit.release(limitConf);
                    }
                }
                bucketLog.ostorId = object.ostorId;
                bucketLog.ostorKey = StringUtils.isEmpty(object.storageId) ? "-" : Storage.getStorageId(object.storageId);
            } catch (BaseException e) {
                status = e.status;
                throw e;
            } finally {
                String ipAddr = Utils.getIpAddr(req);
                boolean isInternetIP = Utils.isInternetIP(ipAddr);
                // 如果resp.getStatus()是200， 但status是500则表示客户端下载对象出错并且需要计费。
                pinData.setRequestInfo(dbBucket.ownerId, dbBucket.name, isInternetIP, method);
                if(null == resp.getHeader("Connection"))
                    resp.setHeader("Connection", "close");
                pinData.jettyAttributes.respHeadFlow = Utils.getRespHeaderLength(resp);                
                if (status != 403) {
                    //实际上只有Get object成功之后才统计流量
                    Utils.pinTransfer(dbBucket.getOwnerId(), pin, pinData, isInternetIP, dbBucket.name);
                    Utils.pinRestore(dbBucket.getOwnerId(), pin, pinData, dbBucket.name);
                }
            }
        } else if (method.equalsIgnoreCase(HttpMethod.PUT.toString())) {
            try {
                if (owner != null && owner.auth != null && !owner.auth.getObjectPermission().hasPut()){
                    throw new BaseException(403, 
                            "permissionIsNotAllow", owner.auth.getReason_ch());
                }
                
                ObjectMeta object = new ObjectMeta(key, dbBucket.getName(), dbBucket.metaLocation);
                //解析多版本信息
                if(MultiVersionsController.gwMultiVersionEnabled(req, dbBucket)) {
                    MultiVersionsController.parseVersionInfo(object, req);
                    object.versionType = VersionType.PUT;
                }
                boolean objectExists;
                bucketLog.getMetaTime = System.currentTimeMillis();
                try{
                    if(client.isBusy(object, OOSConfig.getMaxConcurrencyPerRegionServer()))
                        Backoff.backoff();
                    objectExists = client.objectSelect(object);
                } finally {
                    bucketLog.getMetaTime = System.currentTimeMillis() - bucketLog.getMetaTime;
                }
                if(objectExists) {
                    bucketLog.metaRegion = object.metaRegionName;
                    bucketLog.dataRegion = object.dataRegion;
                    bucketLog.ostorId = object.ostorId;
                }
                if (req.getParameter("partNumber") != null && req.getParameter("uploadId") != null) {
                    if(!objectExists) {
                        throw new BaseException(404, ErrorMessage.ERROR_CODE_NO_SUCH_KEY);
                    }
                    int partNum = 0;
                    try {
                        partNum = Integer.parseInt(req.getParameter("partNumber"));
                    } catch (Exception e) {
                        throw new BaseException();
                    }

                    //Upload Part和Copy Part都在此处判断
                    Utils.checkPartNumber(req, partNum);

                    InitialUploadMeta initialUpload = new InitialUploadMeta(dbBucket.metaLocation, dbBucket.getName(),
                            object.name, req.getParameter("uploadId"), false);
                    UploadMeta upload = new UploadMeta(dbBucket.metaLocation, initialUpload.uploadId, partNum);
                    boolean iuExists;
                    bucketLog.getMetaTime = System.currentTimeMillis();
                    try {
                        if (client.isBusy(initialUpload, OOSConfig.getMaxConcurrencyPerRegionServer()))
                            Backoff.backoff();
                        iuExists = client.initialUploadSelect(initialUpload);
                    } finally {
                        bucketLog.getMetaTime = System.currentTimeMillis() - bucketLog.getMetaTime;
                    }
                    if (iuExists) {
                        if (req.getHeader(Consts.X_AMZ_COPY_SOURCE) == null) {
                            pinData.jettyAttributes.action = OOSActions.Object_Put_UploadPart;
                            //只对上传分片做限制。 
                            Pair<String, String> limitConf = ConcurrentConnectionsLimit.allow(req, dbBucket.name, key);
                            try {
                                OpObject.uploadPartStatics(req, resp, dbBucket, object, upload, ip,
                                        bucketLog, initialUpload, pinData);
                                setConnectionHeader(req, resp);
                            } finally {
                                ConcurrentConnectionsLimit.release(limitConf);
                            }
                        } else {
                            pinData.jettyAttributes.action = OOSActions.Object_Put_CopyPart;
                            Pair<String, String> p = getSourceBucketObject(req, owner);
                            BucketMeta sourceBucket = new BucketMeta(p.first());
                            if (!client.bucketSelect(sourceBucket))
                                throw new BaseException(404, ErrorMessage.ERROR_CODE_NO_SUCH_BUCKET,
                                        ErrorMessage.ERROR_MESSAGE_404);
                            // resource object GetObject权限校验
                            AccessEffect sourceAccessEffect = ResourceAccessControlUtils.checkObjectACLsAndPolicy(req, sourceBucket, p.second(),
                                    authResult, OOSPermissions.GetObject.toString(), HttpMethod.GET.toString());
                            AccessControlUtils.auth(req, OOSActions.Object_Get, sourceBucket, p.second(), authResult, sourceAccessEffect);
                            ObjectMeta sourceObject = new ObjectMeta(p.second(), p.first(), sourceBucket.metaLocation);
                            bucketLog.getMetaTime = System.currentTimeMillis();
                            try {
                                if (client.isBusy(sourceObject, OOSConfig.getMaxConcurrencyPerRegionServer()))
                                    Backoff.backoff();
                                if (!client.objectSelect(sourceObject))
                                    throw new BaseException(404, ErrorMessage.ERROR_CODE_NO_SUCH_KEY,
                                            ErrorMessage.ERROR_MESSAGE_404);
                            } finally {
                                bucketLog.getMetaTime = System.currentTimeMillis() - bucketLog.getMetaTime;
                            }
                            bucketLog.metaRegion = "sourceObject:"+sourceObject.metaRegionName+"-object:"+object.metaRegionName;
                            bucketLog.dataRegion = "sourceObject:"+sourceObject.dataRegion+"-object:"+object.dataRegion;
                            OpObject.uploadPartCopy(req, resp, sourceBucket, sourceObject,
                                    dbBucket, object, upload, executor, bucketLog, initialUpload, pinData);
                        }
                    } else
                        throw new BaseException(404, ErrorMessage.ERROR_CODE_NO_SUCH_UPLOAD);
                    resp.setHeader("Connection", "keep-alive");
                } else if (req.getHeader(Consts.X_AMZ_COPY_SOURCE) == null) {
                    pinData.jettyAttributes.action = OOSActions.Object_Put;
                    if (req.getHeader(Headers.CONTENT_LENGTH) == null) {
                        throw new BaseException(411, ErrorMessage.ERROR_CODE_MISSING_CONTENT_LENGTH);
                    }
                    OpObject.putObjectStatics(req, resp, dbBucket, object, objectExists, bucketLog, ip, false, null, pinData);
                    resp.setHeader(Headers.CONTENT_LENGTH, "0");
                    setConnectionHeader(req, resp);
                } else if (req.getParameterMap().isEmpty()) {
                    pinData.jettyAttributes.action = OOSActions.Object_Put_Copy;
                    Pair<String, String> p = getSourceBucketObject(req, owner);
                    BucketMeta sourceBucket = new BucketMeta(p.first());
                    if (!client.bucketSelect(sourceBucket))
                        throw new BaseException(404, ErrorMessage.ERROR_CODE_NO_SUCH_BUCKET,
                                ErrorMessage.ERROR_MESSAGE_404);
                    // resource object GetObject权限校验
                    AccessEffect sourceAccessEffect = ResourceAccessControlUtils.checkObjectACLsAndPolicy(req, sourceBucket, p.second(),
                            authResult, OOSPermissions.GetObject.toString(), HttpMethod.GET.toString());
                    AccessControlUtils.auth(req, OOSActions.Object_Get, sourceBucket, p.second(), authResult, sourceAccessEffect);
                    ObjectMeta sourceObject = new ObjectMeta(p.second(), p.first(), sourceBucket.metaLocation);
                    bucketLog.getMetaTime = System.currentTimeMillis();
                    try {
                        if (client.isBusy(sourceObject, OOSConfig.getMaxConcurrencyPerRegionServer()))
                            Backoff.backoff();
                        if (!client.objectSelect(sourceObject))
                            throw new BaseException(404,
                                    ErrorMessage.ERROR_CODE_NO_SUCH_KEY,
                                    ErrorMessage.ERROR_MESSAGE_404);
                    } finally {
                        bucketLog.getMetaTime = System.currentTimeMillis() - bucketLog.getMetaTime;
                    }
                   //TODO 暂未支持多版本
                    OpObject.copyObject(req, resp, dbBucket, object, sourceBucket, sourceObject,
                            executor, bucketLog, objectExists, pinData);
                    resp.setHeader("Connection", "close");
                    bucketLog.ostorId = object.ostorId;
                } else
                    throw new BaseException(405, ErrorMessage.ERROR_CODE_405);
                Utils.setCommonHeader(resp,new Date(bucketLog.startTime),bucketLog.requestId);
            } catch (BaseException e) {
                status = e.status;
                throw e;
            } finally {
                String ipAddr = Utils.getIpAddr(req);
                boolean isInternetIP = Utils.isInternetIP(ipAddr);
                pinData.setRequestInfo(dbBucket.ownerId, dbBucket.name, isInternetIP, method);
                if (status != 403) {
                    //实际上只有真正put object成功之后才统计容量和流量
                    Utils.pinStorage(dbBucket.getOwnerId(), pin, pinData, dbBucket.name);
                    Utils.pinUpload(dbBucket.getOwnerId(), pin, pinData, isInternetIP, dbBucket.name);
                    Utils.pinRestore(dbBucket.getOwnerId(), pin, pinData, dbBucket.name);
                }
            }
        } else if (req.getMethod().equalsIgnoreCase(HttpMethod.DELETE.toString())) {
            List<PinData> pinDataList = new ArrayList<>();
            boolean deleteMultiVersionRangeObject = false;
            try {
                if (owner != null && owner.auth != null && !owner.auth.getObjectPermission().hasDelete()){
                    throw new BaseException(403, 
                            "permissionIsNotAllow", owner.auth.getReason_ch());
                }
                
                ObjectMeta object = new ObjectMeta(key, dbBucket.getName(), dbBucket.metaLocation);
                boolean objectExists;
                bucketLog.getMetaTime = System.currentTimeMillis();
                try {
                    if (client.isBusy(object, OOSConfig.getMaxConcurrencyPerRegionServer()))
                        Backoff.backoff();
                    objectExists = client.objectSelect(object);
                } finally {
                    bucketLog.getMetaTime = System.currentTimeMillis() - bucketLog.getMetaTime;
                }
                //TODO 是否需要判断对象存在。
                if (req.getParameter("uploadId") != null && objectExists) {
                    pinData.jettyAttributes.action = OOSActions.Object_Delete_AbortMultipartUpload;
                    bucketLog.metaRegion = object.metaRegionName;
                    bucketLog.dataRegion = object.dataRegion;
                    InitialUploadMeta initialUpload = new InitialUploadMeta(dbBucket.metaLocation, dbBucket.getName(),
                            object.name, req.getParameter("uploadId"), false);
                    UploadMeta upload = new UploadMeta(dbBucket.metaLocation, initialUpload.uploadId, true);
                    boolean ref;
                    bucketLog.getMetaTime = System.currentTimeMillis();
                    try {
                        if (client.isBusy(initialUpload, OOSConfig.getMaxConcurrencyPerRegionServer()))
                            Backoff.backoff();
                        ref = client.initialUploadSelect(initialUpload);
                    } finally {
                        bucketLog.getMetaTime = System.currentTimeMillis() - bucketLog.getMetaTime;
                    }
                    List<UploadMeta> uploads;
                    bucketLog.getMetaTime = System.currentTimeMillis();
                    try {
                        if (client.isBusy(upload, OOSConfig.getMaxConcurrencyPerRegionServer()))
                            Backoff.backoff();
                        uploads = client.uploadSelectByUploadId(upload);
                    } finally {
                        bucketLog.getMetaTime = System.currentTimeMillis() - bucketLog.getMetaTime;
                    }
                    if (ref)
                        OpObject.abortMultipartUpload(object, upload, uploads,
                                initialUpload, bucketLog, pinData);
                    else
                        throw new BaseException(404, ErrorMessage.ERROR_CODE_NO_SUCH_UPLOAD);
                }
                if (req.getParameter("uploadId") == null) {
                    if(MultiVersionsController.gwMultiVersionEnabled(req, dbBucket)) {
                        MultiVersionsController.parseVersionInfo(object, req);
                        MultiVersionsOpt mvOpt = MultiVersionsController.getMultiVersionsOpt(req);
                        if(mvOpt == MultiVersionsOpt.EQUAL) {
                            bucketLog.getMetaTime = System.currentTimeMillis();
                            try {
                                objectExists = client.objectSelect(object);
                            } finally {
                                bucketLog.getMetaTime = System.currentTimeMillis() - bucketLog.getMetaTime;
                            }
                            if(objectExists && object.versionType == VersionType.PUT) {
                                bucketLog.metaRegion = object.metaRegionName;
                                bucketLog.dataRegion = object.dataRegion;
                                bucketLog.ostorId = object.ostorId;
                                bucketLog.ostorKey = StringUtils.isEmpty(object.storageId) ? "-" : Storage.getStorageId(object.storageId);
                                bucketLog.etag = object.etag;
                                bucketLog.replicaMode = object.storageClass.toString();
                                if(object.version != null) {
                                    // 删除指定版本
                                    pinData.jettyAttributes.action = OOSActions.Object_Delete;
                                    OpObject.deleteObjectStatics(req, dbBucket, object, bucketLog, pinData);
                                } else {
                                    ObjectMeta deleteVersion = new ObjectMeta(key, dbBucket.getName(), dbBucket.metaLocation);
                                    Pair<Long, String> versionPair = MultiVersionsController.generateVersoin();
                                    deleteVersion.timestamp = versionPair.first();
                                    deleteVersion.version = versionPair.second();
                                    deleteVersion.versionType = VersionType.DELETE;
                                    bucketLog.putMetaTime = System.currentTimeMillis();
                                    try {
                                        if (client.isBusy(deleteVersion, OOSConfig.getMaxConcurrencyPerRegionServer()))
                                            Backoff.backoff();
                                        client.objectInsert(deleteVersion);
                                    } finally {
                                        bucketLog.putMetaTime = System.currentTimeMillis() - bucketLog.putMetaTime;
                                    }
                                }
                            }
                        } else if(mvOpt == MultiVersionsOpt.RANGE) {
                            if (object.version != null) {
                                // 删除指定版本范围的对象
                                String[] versionRange = object.version.split(",");
                                if (versionRange.length != 2)
                                    throw new BaseException(400, "Bad Request", "Bad Request");
                                List<ObjectMeta> objects = client.objectGetRangeVersions(object, Long.valueOf(versionRange[0]), Long.valueOf(versionRange[1]));
                                deleteMultiVersionRangeObject = true;
                                if(objects.size() >= 1) {
                                    StringBuilder meta = new StringBuilder();
                                    StringBuilder data = new StringBuilder();
                                    objects.stream().forEach(e -> {meta.append(e.metaRegionName).append(",");data.append(e.dataRegion).append(",");});
                                    bucketLog.metaRegion = (meta.deleteCharAt(meta.length()-1)).toString();
                                    bucketLog.dataRegion = (data.deleteCharAt(data.length()-1)).toString();
                                }
                                String body = OpBucket.deleteMultiVersionRangeObjectStatics(req, dbBucket, objects, owner, bucketLog, pinDataList, pinData);
                                writeResponseEntity(resp, body, bucketLog);
                            }
                        } else {
                            throw new BaseException(400, "Bad Request", "Bad Request");
                        }
                    } else {
                        bucketLog.getMetaTime = System.currentTimeMillis();
                        try {
                            objectExists = client.objectSelect(object);
                        } finally {
                            bucketLog.getMetaTime = System.currentTimeMillis() - bucketLog.getMetaTime;
                        }
                        if (objectExists) {
                            pinData.jettyAttributes.action = OOSActions.Object_Delete;
                            bucketLog.metaRegion = object.metaRegionName;
                            bucketLog.dataRegion = object.dataRegion;
                            OpObject.deleteObjectStatics(req, dbBucket, object, bucketLog, pinData);
                            setConnectionHeader(req, resp);
                        }
                    }
                }
                resp.setStatus(204);
                Utils.setCommonHeader(resp,new Date(bucketLog.startTime),bucketLog.requestId);
                bucketLog.ostorId = object.ostorId;
                bucketLog.ostorKey = StringUtils.isEmpty(object.storageId) ? "-" : Storage.getStorageId(object.storageId);
            } catch (BaseException e) {
                status = e.status;
                throw e;
            } finally {
                String ipAddr = Utils.getIpAddr(req);
                boolean isInternetIP = Utils.isInternetIP(ipAddr);
                pinData.setRequestInfo(dbBucket.ownerId, dbBucket.name, isInternetIP, method);
                if (status != 403) {
                    Utils.pinUpload(dbBucket.getOwnerId(), pin, pinData, isInternetIP, dbBucket.name);
                    if (!deleteMultiVersionRangeObject) {
                        //实际上只有真正delete object成功之后才减少容量
                        Utils.pinDecreStorage(dbBucket.getOwnerId(), pin, pinData, dbBucket.name);
                        Utils.pinDeleteFlow(dbBucket.getOwnerId(), pin, pinData, isInternetIP, dbBucket.name);
                    } else {
                        //多版本删除指定版本范围的对象
                        for (PinData data : pinDataList) {
                            Utils.pinDecreStorage(dbBucket.getOwnerId(), pin, data, dbBucket.name);
                            Utils.pinDeleteFlow(dbBucket.getOwnerId(), pin, data, isInternetIP, dbBucket.name);
                        }
                    }
                }
                if(null == resp.getHeader("Connection"))
                    resp.setHeader("Connection", "close");
            }
        } else if (req.getMethod().equalsIgnoreCase(HttpMethod.HEAD.toString())) {
            try {
                if (owner != null && owner.auth != null && !owner.auth.getObjectPermission().hasGet()){
                    throw new BaseException(403, 
                            "permissionIsNotAllow", owner.auth.getReason_ch());
                }
                pinData.jettyAttributes.action = OOSActions.Object_Head;
                ObjectMeta object = new ObjectMeta(key, dbBucket.getName(), dbBucket.metaLocation);
                boolean objectExists;
                bucketLog.getMetaTime = System.currentTimeMillis();
                try {
                    if (client.isBusy(object, OOSConfig.getMaxConcurrencyPerRegionServer()))
                        Backoff.backoff();
                    objectExists = client.objectSelect(object);
                    pinData.setStorageType(object.storageClassString);
                } finally {
                    bucketLog.getMetaTime = System.currentTimeMillis() - bucketLog.getMetaTime;
                }
                if (!objectExists)
                    throw new BaseException(404, ErrorMessage.ERROR_CODE_NO_SUCH_KEY, ErrorMessage.ERROR_MESSAGE_404);
                OpObject.setResponseHeaderFromObject(object, dbBucket, resp, object.size);
                OpObject.setExpirationHeader(resp,dbBucket,object);
                Utils.setCommonHeader(resp,new Date(bucketLog.startTime),bucketLog.requestId);
                bucketLog.metaRegion = object.metaRegionName;
                bucketLog.dataRegion = object.dataRegion;
                bucketLog.ostorId = object.ostorId;
                bucketLog.ostorKey =  StringUtils.isEmpty(object.storageId) ? "-" : Storage.getStorageId(object.storageId);
            } finally {
                String ipAddr = Utils.getIpAddr(req);
                boolean isInternetIP = Utils.isInternetIP(ipAddr);
                pinData.jettyAttributes.respHeadFlow = Utils.getRespHeaderLength(resp);
                pinData.setRequestInfo(dbBucket.ownerId, dbBucket.name, isInternetIP, method);
                Utils.pinTransfer(dbBucket.getOwnerId(), pin, pinData, isInternetIP, dbBucket.name);
                resp.setHeader("Connection", "close");
            }
        } else if (method.equalsIgnoreCase(HttpMethod.POST.toString())) {
            try {
                if (owner != null && owner.auth != null && !owner.auth.getObjectPermission().hasPut()){
                    throw new BaseException(403, 
                            "permissionIsNotAllow", owner.auth.getReason_ch());
                }
                
                ObjectMeta object = new ObjectMeta(key, dbBucket.getName(), dbBucket.metaLocation);
                boolean objectExists;
                bucketLog.getMetaTime = System.currentTimeMillis();
                try {
                    if (client.isBusy(object, OOSConfig.getMaxConcurrencyPerRegionServer()))
                        Backoff.backoff();
                    objectExists = client.objectSelect(object);
                } finally {
                    bucketLog.getMetaTime = System.currentTimeMillis() - bucketLog.getMetaTime;
                }
                if(objectExists) {
                    bucketLog.metaRegion = object.metaRegionName;
                    bucketLog.dataRegion = object.dataRegion;
                }
                if (req.getParameter("uploads") != null) {
                    pinData.jettyAttributes.action = OOSActions.Object_Post_InitiateMultipartUpload;
                    resp.setHeader("Connection", "keep-alive");
                    writeResponseEntity(resp, OpObject.initiateMultipartUpload(req, resp, dbBucket,
                            object, owner), bucketLog);
                    bucketLog.metaRegion = object.metaRegionName;
                    bucketLog.dataRegion = object.dataRegion;
                } else if (req.getParameter("uploadId") != null) {
                    pinData.jettyAttributes.action = OOSActions.Object_Post_CompleteMultipartUpload;
                    InitialUploadMeta initialUpload = new InitialUploadMeta(dbBucket.metaLocation, dbBucket.getName(),
                            object.name, req.getParameter("uploadId"), false);
                    boolean exists;
                    bucketLog.getMetaTime = System.currentTimeMillis();
                    try {
                        if (client.isBusy(initialUpload, OOSConfig.getMaxConcurrencyPerRegionServer()))
                            Backoff.backoff();
                        exists = client.initialUploadSelect(initialUpload);
                    } finally {
                        bucketLog.getMetaTime = System.currentTimeMillis() - bucketLog.getMetaTime;
                    }
                    if (exists)
                        OpObject.completeMultipartUpload_fake(req, resp, dbBucket, object,
                                initialUpload, bucketLog, ip, pinData);
                    else
                        throw new BaseException(404, ErrorMessage.ERROR_CODE_NO_SUCH_UPLOAD);
                    resp.setHeader("Connection", "close");
                } else if(isPostObject){
                    pinData.jettyAttributes.action = OOSActions.Object_Post;
                    if (req.getHeader(Headers.CONTENT_LENGTH) == null) {
                        throw new BaseException(411, ErrorMessage.ERROR_CODE_MISSING_CONTENT_LENGTH);
                    }
                    OpObject.putObjectStatics(req, resp, dbBucket, object,objectExists, bucketLog, ip, isPostObject,formPostHeaderNotice, pinData);
                    Utils.setCommonHeader(resp,new Date(bucketLog.startTime),bucketLog.requestId);
                    int statusCode = resp.getStatus();
                    if(statusCode != HttpServletResponse.SC_SEE_OTHER){
                        String reqUrl = req.getRequestURL().toString();
                        if(!reqUrl.endsWith("/")){
                            reqUrl +="/";
                        }
                        String location = reqUrl + URLEncoder.encode(object.name,Consts.STR_UTF8);
                        resp.setHeader("Location", location);
                        resp.setHeader("Connection", "close");
                        if(statusCode == 201){
                            XmlWriter xmlWriter = FormPostTool.getSuccessResponse(location, dbBucket.name, object.name, object.etag);
                            writeResponseEntity(resp, xmlWriter.toString(), bucketLog);
                        }
                    }
                } else
                    throw new BaseException(405, ErrorMessage.ERROR_CODE_405);
                bucketLog.ostorId = object.ostorId;
            } catch (BaseException e) {
                status = e.status;
                throw e;
            } finally {
                String ipAddr = Utils.getIpAddr(req);
                boolean isInternetIP = Utils.isInternetIP(ipAddr);
                pinData.setRequestInfo(dbBucket.ownerId, dbBucket.name, isInternetIP, method);
                if (status != 403) {
                    // 实际上只有真正put object成功之后才统计容量和流量
                    Utils.pinStorage(dbBucket.getOwnerId(), pin, pinData, dbBucket.name);
                    Utils.pinUpload(dbBucket.getOwnerId(), pin, pinData, isInternetIP, dbBucket.name);
                    Utils.pinRestore(dbBucket.getOwnerId(), pin, pinData, dbBucket.name);
                }
            }
        } else if (method.equalsIgnoreCase(HttpMethod.OPTIONS.toString())) {
            try {
                if (isPreflight(req)) {
                    pinData.jettyAttributes.action = OOSActions.Object_Options;
                    ObjectMeta object = new ObjectMeta(key, dbBucket.getName(), dbBucket.metaLocation);
                    Utils.setCommonHeader(resp, new Date(bucketLog.startTime), bucketLog.requestId);
                    bucketLog.ostorId = object.ostorId;
                } else {
                    // 非跨域请求，普通OPTIONS请求返回400
                    throw new BaseException(400, "Bad Request", "Bad Request");
                }     
            } finally {
                String ipAddr = Utils.getIpAddr(req);
                boolean isInternetIP = Utils.isInternetIP(ipAddr);
                pinData.jettyAttributes.respHeadFlow = Utils.getRespHeaderLength(resp);
                pinData.setRequestInfo(dbBucket.ownerId, dbBucket.name, isInternetIP, method);
                Utils.pinTransfer(dbBucket.getOwnerId(), pin, pinData, isInternetIP, dbBucket.name);
            }                   
        } else {
            throw new BaseException(405, ErrorMessage.ERROR_CODE_405,
                    ErrorMessage.ERROR_MESSAGE_405);
        }
    }
    
    /**
           * 如果定义了禁用客户端connection 请求头，则强制返回connection close，如果没有定义，
           * 客户端定义了建议返回，则按客户端返回。未定义仍然返回false
     * */
    private void setConnectionHeader(HttpServletRequest req,
            HttpServletResponse resp) {
        resp.setHeader("Connection", "close");
        if(req.getHeader("Connection") != null && OOSConfig.getClientDefineConnResp().equals(Consts.ENABLE)){
            resp.setHeader("Connection", req.getHeader("Connection"));
        }
    }
    
    /**
     * 是否为预检请求
     * @param req
     * @return
     */
    private boolean isPreflight(HttpServletRequest req) {
        String originHeader = req.getHeader(Consts.REQUEST_HEADER_ORIGIN);
        String methodHeader = req.getHeader(Consts.REQUEST_HEADER_ACCESS_CONTROL_REQUEST_METHOD);
        if (originHeader != null && !originHeader.isEmpty() && methodHeader != null && !methodHeader.isEmpty()) {
            return true;
        }
        return false;
    }

    /**
     * 根据请求方法及参数确定Policy对应的权限关键字，若请求为OPTIONS方法则返回null，表示跨域预检请求不需要验证ALCs和Policy权限
     * @param req
     * @return
     */
    private String getPermissionFromReq(HttpServletRequest req) {
        String permission = null;
        String method = req.getMethod();
        if (method.equalsIgnoreCase(HttpMethod.GET.toString())) {
            if (req.getParameter("uploadId") != null) {
                permission = OOSPermissions.ListMultipartUploadParts.toString();
            } else {
                permission = OOSPermissions.GetObject.toString();
            }
        } else if (method.equalsIgnoreCase(HttpMethod.PUT.toString())) {
            permission = OOSPermissions.PutObject.toString();
        } else if (method.equalsIgnoreCase(HttpMethod.DELETE.toString())) {
            if (req.getParameter("uploadId") != null) {
                permission = OOSPermissions.AbortMultipartUpload.toString();
            } else {
                permission = OOSPermissions.DeleteObject.toString();
            }
        } else if (method.equalsIgnoreCase(HttpMethod.HEAD.toString())) {
            permission = OOSPermissions.GetObject.toString();
        } else if (method.equalsIgnoreCase(HttpMethod.POST.toString())) {
            permission = OOSPermissions.PutObject.toString();
        }
        return permission;
    }
        
    private Pair<String, String> getSourceBucketObject(HttpServletRequest req, OwnerMeta owner)
            throws BaseException, IOException {
        Pair<String, String> p = new Pair<String, String>();
        String sourceBucketName = null;
        String sourceObjectName = null;
        try {
            String source = req.getHeader(Consts.X_AMZ_COPY_SOURCE);
            source = source.replaceAll("%20", "\\+");
            source = URLDecoder.decode(source, Consts.STR_UTF8);
            int slash2;
            if (source.startsWith("/")) {
                slash2 = source.indexOf('/', 1);
                sourceBucketName = source.substring(1, slash2);
            } else {
                slash2 = source.indexOf('/', 0);
                sourceBucketName = source.substring(0, slash2);
            }
            sourceObjectName = source.substring(slash2 + 1);
        } catch (Exception e) {
            throw new BaseException();
        }
        BucketMeta sourceBucket = new BucketMeta(sourceBucketName);
        if (client.bucketSelect(sourceBucket) == false)
            throw new BaseException(404, ErrorMessage.ERROR_CODE_NO_SUCH_BUCKET, "the request bucketname is "
                    + sourceBucketName);
        ObjectMeta sourceObject = new ObjectMeta(sourceObjectName, sourceBucketName, sourceBucket.metaLocation);
        if(client.isBusy(sourceObject, OOSConfig.getMaxConcurrencyPerRegionServer()))
            Backoff.backoff();
        boolean resObject = client.objectSelect(sourceObject);
        if (false == resObject)
            throw new BaseException(404, ErrorMessage.ERROR_CODE_NO_SUCH_KEY, "the request object is " + sourceObjectName);
        p.first(sourceBucketName);
        p.second(sourceObjectName);
        return p;
    }
    
   
    @Override
    public void handle(String target, Request basereq, final HttpServletRequest req,
            HttpServletResponse resp) throws ServletException, IOException {
        final BucketLog bucketLog = new BucketLog(System.currentTimeMillis());
        bucketLog.preProcessTime = System.currentTimeMillis();
        bucketLog.operation = "REST." + req.getMethod() + ".-";
        final PinData pinData = new PinData();
        pinData.jettyAttributes.manageEventAttribute.reqRecieveTime = bucketLog.startTime;
        
        //记录http请求头长度
        Enumeration<String> headerNames = req.getHeaderNames();
        int length = 0;
        while (null != headerNames && headerNames.hasMoreElements()){
            String key = (String)headerNames.nextElement();
            String value = req.getHeader(key);
            length += (key + "=" + value).length();
        }   
        pinData.jettyAttributes.reqHeadFlow = length;
        
        CloudTrailEvent cloudTrailEvent = new CloudTrailEvent(); 
        
        try {
            String reqid = OOSRequestId.generateRequestId();
            req.setAttribute(Headers.REQUEST_ID, reqid);
            // 为所有日志添加reqId属性
            ThreadContext.put("Request_ID", reqid);
            bucketLog.requestId = reqid;
            bucketLog.getRequestInfo(req);
            Utils.log(req, bucketLog.ipAddress);            
            if (InternalHandler.isForwarded(req)) {
                InternalHandler.handle(basereq, req, resp, bucketLog, pin);
            } else {
                handle2(target, basereq, req, resp, bucketLog, pinData);
            }
        } catch (BaseException e) {
            Utils.logError(e, bucketLog);
            resp.setStatus(e.status);
            if (e.responseHeaders != null && e.responseHeaders.size()>0) {
                for (String key : e.responseHeaders.keySet()) {
                    resp.setHeader(key, e.responseHeaders.get(key));
                }
            }
            e.reqId = bucketLog.requestId;
            e.resource = Utils.getResource(bucketLog);
            if (e.status == 503)
                resp.setIntHeader("Retry-After", OOSConfig.getRetryAfterTime());
            writeResponseEntity(resp, e.toXmlWriter().toString(), bucketLog);
            if (!(ErrorMessage.GET_OBJECT_FAILED.equals(e.code)
                    && bucketLog.ostorResponseFirstTime != 0)) {
                bucketLog.status = e.status;
                bucketLog.errorCode = e.code;
            }
            cloudTrailEvent.errorStatus = e.status;
            cloudTrailEvent.errorCode = e.code;
            cloudTrailEvent.errorMessage = e.message;
        } catch (JSONException | IllegalArgumentException | AmazonClientException e) {
            Utils.logError(e, bucketLog);
            BaseException be = new BaseException();
            be.status = 400;
            be.reqId = bucketLog.requestId;
            be.resource = Utils.getResource(bucketLog);
            be.message = "Invalid Argument";
            be.code = ErrorMessage.ERROR_CODE_INVALID_ARGUMENT;
            resp.setStatus(be.status);
            writeResponseEntity(resp, be.toXmlWriter().toString(), bucketLog);
            bucketLog.status = 400;
            bucketLog.errorCode = be.code;
            cloudTrailEvent.errorStatus = be.status;
            cloudTrailEvent.errorCode = be.code;
            cloudTrailEvent.errorMessage = be.message;
        } catch (Throwable e) {
            Utils.logError(e, bucketLog);
            BaseException be = new BaseException();
            be.status = 500;
            be.reqId = bucketLog.requestId;
            be.resource = Utils.getResource(bucketLog);
            be.message = ErrorMessage.ERROR_MESSAGE_500;
            be.code = ErrorMessage.ERROR_CODE_500;
            resp.setStatus(be.status);
            writeResponseEntity(resp, be.toXmlWriter().toString(), bucketLog);
            bucketLog.status = 500;
            bucketLog.errorCode = be.code;
            cloudTrailEvent.errorStatus = be.status;
            cloudTrailEvent.errorCode = be.code;
            cloudTrailEvent.errorMessage = be.message;
        } finally {
            basereq.setHandled(true);
            if (pinData.getMethodType() != null) {
                Utils.pinRequest(pin, pinData, resp.getStatus(), bucketLog);// 请求次数计量
            }
            try {
                // 如果用户开启了管理事件记录
                if(pinData.jettyAttributes.manageEventAttribute.ifRecordManageEvent)
                    manageCloudTrail(cloudTrailEvent, req, pinData, resp);
            } catch (Throwable e) {
                log.error("manage cloudtrail failed. " + e);
            }
            if (bucketLog.adapterResponseStartTime == 0) {
                bucketLog.adapterResponseStartTime = System.currentTimeMillis();
            }
            if (bucketLog.targetBucketName != null) {// 本地请求
                bucketLog.write(req, bucketLog.targetBucketName, bucketLog.targetPrefix, true, pinData);
            }
            bucketLog.endTime = System.currentTimeMillis();
            if (bucketLog.bucketOwnerName != null) {
                bucketLog.writeGlobalLog(req, pinData);
            }
        }
    }
    
    private boolean manageCloudTrail(CloudTrailEvent event, HttpServletRequest req, PinData pinData, HttpServletResponse resp) throws IOException, BaseException, org.json.JSONException {
        // 固定参数
        event.eventSource = "oos-cn.ctyunapi.cn";
        event.eventType = "ApiCall";
        event.serviceName = "OOS";
        event.managementEvent = true;        
        //请求相关参数
        event.eventTime = pinData.jettyAttributes.manageEventAttribute.reqRecieveTime;
        event.requestId = (String) req.getAttribute(Headers.REQUEST_ID);
        try {
            event.requestRegion = Utils.getRegionNameFromReq(req, "");
        } catch (BaseException e) {
            event.requestRegion = ""; // 如果解析不到记为空，如bucket形式
        }
        event.sourceIpAddress = Utils.getIpAddr(req);
        event.userAgent = req.getHeader(HttpHeaders.USER_AGENT);

        String protocol = req.getHeader("x-forwarded-proto");
        String requestURLString = req.getRequestURL().toString();
        int reqPort = req.getServerPort();
        requestURLString = requestURLString.replace(":" + reqPort, "");
        if (protocol != null && requestURLString.contains("https://")) {
            requestURLString = requestURLString.replace("https://", protocol + "://");
        } else if (protocol != null && requestURLString.contains("http://")) {
            requestURLString = requestURLString.replace("http://", protocol + "://");
        }
        event.requestURL = requestURLString;

        //用户参数
        event.eventOwnerId = pinData.jettyAttributes.manageEventAttribute.bucketOwnerId;
        event.userType = pinData.jettyAttributes.manageEventAttribute.userType;
        event.userName = pinData.jettyAttributes.manageEventAttribute.userName;
        event.principalId = pinData.jettyAttributes.manageEventAttribute.principleId;
        event.arn = pinData.jettyAttributes.manageEventAttribute.userArn;
        event.ownerId = pinData.jettyAttributes.manageEventAttribute.ownerId;
        event.accessKeyId = pinData.jettyAttributes.manageEventAttribute.accessKeyId;
        
        //操作相关参数
        OOSActions action = pinData.jettyAttributes.action;        
        // 判断oos action是否是管理事件，并获取对应的cloud trail事件名称与readonly
        Pair<String, Boolean> eventName = isManageEventAction(action);
        if(eventName == null) 
            return false;
        event.eventName = eventName.first();
        event.readOnly = eventName.second();
        JSONObject reqJo = pinData.jettyAttributes.manageEventAttribute.reqContent;
        String bucketName = pinData.jettyAttributes.manageEventAttribute.bucketName;
        if (reqJo != null) {
            if (bucketName != null)
                reqJo.put("bucketName", bucketName);
            event.requestParameters = reqJo.toString();
        } else {
            if (bucketName != null)
            {
                reqJo = new JSONObject();
                reqJo.put("bucketName", bucketName);
                event.requestParameters = reqJo.toString();
            }
        }
        event.responseElements = null;
        
        // 资源
        if (bucketName != null) {
            List<Resources> resourcesList = new ArrayList<Resources>();
            Resources r1 = new Resources();
            r1.resourceName = bucketName;
            String bucketAccountId = pinData.jettyAttributes.manageEventAttribute.bucketAccountId;
            r1.resourceARN = "arn:ctyun:oos::" + bucketAccountId + ":bucket/" + bucketName;
            r1.resourceType = "OOS Bucket";
            resourcesList.add(r1);
            event.resources = resourcesList;
        }

        if (req.getHeader("OOS-PROXY-HOST") != null && req.getHeader("OOS-PROXY-HOST").equals("oos-proxy")) {// 来自proxy转发的请求
            event.accessKeyId = null;
            event.eventSource = "oos-cn.ctyun.cn";
            event.userAgent =  "oos-cn.ctyun.cn";
            event.requestURL = req.getHeader("OOS-PROXY-URL");
            event.sourceIpAddress = req.getHeader(WebsiteHeader.CLIENT_IP);
        }
        
        ManageEventMeta manageEventMeta = new ManageEventMeta();
        manageEventMeta.setEvent(event);
        client.manageEventInsert(manageEventMeta);
        return true;
    }
    
    /**
     * 根据OOSAction 判断该操作是否属于管理事件操作
     * @param action
     * @return
     */
    private Pair<String, Boolean> isManageEventAction(OOSActions action) {
        if (action != null) {
            CloudTrailManageEvent event = null;
            try {
                event = CloudTrailManageEvent.valueOf(action.toString());
            } catch (Throwable e) {
            }
            if (event != null)
                return new Pair<String, Boolean>(event.getName(), event.getReadOnly());
        }
        return null;
    }

    private boolean isPostObject(HttpServletRequest req, String key) {
        if (req.getMethod().equalsIgnoreCase(HttpMethod.POST.toString())
                && (req.getHeader(HttpHeaders.CONTENT_TYPE) != null && req.getHeader(HttpHeaders.CONTENT_TYPE).startsWith("multipart/form-data"))
                && (req.getParameterMap() == null||req.getParameterMap().isEmpty()) && (key == null || key.length()==0))
            return true;
        else
            return false;
    }

    void handle2(String target, Request basereq, HttpServletRequest req, HttpServletResponse resp,
            BucketLog bucketLog, PinData pinData) throws Exception {
        String host = req.getHeader("Host");
        Pair<String, String> p = Utils.getBucketKey(req.getRequestURI(), host,
                OOSConfig.getDomainSuffix(), String.valueOf(port), String.valueOf(sslPort),
                OOSConfig.getWebSiteEndpoint(), "", OOSConfig.getInvalidBucketName(),
                OOSConfig.getInvalidObjectName(), false);
        String bucket = p.first();
        String key = p.second();
        if (bucket != null && bucket.trim().length() != 0)
            bucketLog.bucketName = bucket;
        if (key != null && key.trim().length() != 0)
            bucketLog.objectName = key;

        AuthResult authResult = null;
        OwnerMeta owner = null;
        BucketMeta dbBucket = null;
        boolean bucketExists = false;
        if (bucket != null && bucket.length() > 0) {
            dbBucket = new BucketMeta(bucket);
            bucketExists = client.bucketSelect(dbBucket);
        }
        InputStream ip = null;
        String ipAddr = Utils.getIpAddr(req);
        boolean isInternetIP = Utils.isInternetIP(ipAddr);
        try {
            //应该首先判断是否应该backoff。
            if (Backoff.checkHandlerBackoff() && !BackoffConfig.getWhiteList().contains(bucket))
                Backoff.backoff();

            try {
                ip = req.getInputStream();
            } catch (Exception e) {
                log.error(e.getMessage(), e);
                throw new BaseException(400, ErrorMessage.ERROR_CODE_INCOMPLETE_BODY,
                        ErrorMessage.ERROR_MESSAGE_INCOMPLETE_BODY);
            }
            //对请求时间的检查和back本应该是越早越好，但是为了记录bucketlog，只能将其放在Bucket数据库查询之后。
            //Java SDK 默认的content type是x-www-form-urlencoded，含义是表单上传，对put和post生效。 Request类里面的getParameterMap
            //方法会提取body里面的参数，所以如果本意不是表单上传，那么body就会被错误的提取到parameter，导致之后提取body里面的内容失败。
            //避免办法是先getInputStream,将 inputstate置为1，再getParameter就不会再提取body到parameter
            Utils.checkDate(req);
            FormPostHeaderNotice formPostHeaderNotice = null;
            boolean isPostObject = isPostObject(req,key);
            if(isPostObject){
                byte[] boundary = Utils.getBoundary(req.getHeader(HttpHeaders.CONTENT_TYPE));
                formPostHeaderNotice = new FormPostHeaderNotice(boundary.length);
                ip = FormPostTool.readFormFields(ip, formPostHeaderNotice, boundary);
                String fileName = FormPostTool.getFileNameFormField(formPostHeaderNotice);
                key = FormPostTool.getKeyFromField(formPostHeaderNotice,fileName);
                Utils.checkObjectName(key, OOSConfig.getInvalidObjectName());
                bucketLog.objectName = key;
            }

            //请求头Authorization 优先
            if (req.getHeader("Authorization") != null) {
                authResult = Utils.auth(basereq, req, bucket, key, false, true,V4Signer.S3_SERVICE_NAME);
                ip = authResult.inputStream;
            }else if(isPostObject){
                authResult = Utils.authObjectPost(basereq, req, bucket, key, false, formPostHeaderNotice);
            }else if (Utils.isPresignedUrl(req)) {
                // 非options方法才做签名校验
                if (!HttpMethod.OPTIONS.toString()
                        .equalsIgnoreCase(req.getMethod())) {
                    authResult = Utils.checkUrl(basereq, req, bucket, key);
                }
            }
            
            if(null != authResult) {
                owner = authResult.owner;
                if(null != owner)
                    bucketLog.akOwnerName = owner.name;
                // 管理事件拥有者
                OwnerMeta manageOwner = null;
                // 管理事件优先记录在bucket owner下
                // 获取不到bucket时记录在请求owner下
                if (dbBucket != null && dbBucket.getOwnerId() != 0) {
                    OwnerMeta OwnerMeta = new OwnerMeta(dbBucket.getOwnerId());
                    client.ownerSelectById(OwnerMeta);
                    manageOwner = OwnerMeta;
                } else
                    manageOwner = owner;
                // 如果该账户需要记录管理事件，记录请求用户信息
                pinData.jettyAttributes.manageEventAttribute.bucketAccountId = manageOwner.getAccountId();
                pinData.jettyAttributes.manageEventAttribute.bucketOwnerId = manageOwner.getId();
                if (manageOwner.ifRecordManageEvent) {
                    pinData.jettyAttributes.manageEventAttribute.ifRecordManageEvent = true;
                    if (authResult != null) {
                        if(authResult.isSts) {
                            pinData.jettyAttributes.manageEventAttribute.userType = "STSUser";
                            pinData.jettyAttributes.manageEventAttribute.accessKeyId = authResult.tokenMeta.stsAccessKey;
                        } else if (authResult.isRoot()) {
                            pinData.jettyAttributes.manageEventAttribute.userType = "Root";
                            pinData.jettyAttributes.manageEventAttribute.principleId = authResult.owner.getAccountId();
                            pinData.jettyAttributes.manageEventAttribute.userArn = authResult.getUserArn();
                            pinData.jettyAttributes.manageEventAttribute.accessKeyId = authResult.accessKey.accessKey;
                        } else {
                            pinData.jettyAttributes.manageEventAttribute.userType = "IAMUser";
                            pinData.jettyAttributes.manageEventAttribute.principleId = authResult.accessKey.userId;
                            pinData.jettyAttributes.manageEventAttribute.userName = authResult.accessKey.userName;
                            pinData.jettyAttributes.manageEventAttribute.userArn = authResult.getUserArn();
                            pinData.jettyAttributes.manageEventAttribute.accessKeyId = authResult.accessKey.accessKey;
                        }
                        pinData.jettyAttributes.manageEventAttribute.ownerId = owner.getId();
                    }
                }
            }
                
            // 如果用户没有分配元数据域或数据域，就拒绝请求
            if (owner != null) {
                Pair<Set<String>, Set<String>> regions = client.getRegions(owner.getId());
                if (regions.first().size() == 0 || regions.second().size() == 0) {
                    throw new BaseException(
                            "the user does not allocate the regions.", 403,
                            ErrorMessage.ERROR_CODE_INVALID_USER);
                }
                // 用户的数据域中不包含当前计算节点所在的数据域时，拒绝请求
                if (!regions.second().contains(DataRegion.getRegion().getName())
                        && !regions.second().contains(DataRegion.getRegion().getName().toLowerCase())) {
                    throw new BaseException(
                            "the user does not belong to the region.", 403,
                            ErrorMessage.ERROR_CODE_403);
                }
            }

            if (dbBucket != null && dbBucket.getOwnerId() != 0) {
                OwnerMeta OwnerMeta = new OwnerMeta(dbBucket.getOwnerId());
                client.ownerSelectById(OwnerMeta);
                pinData.jettyAttributes.bucketOwnerAttribute.setBucketOwner(OwnerMeta);
                //Utils.setBucketOwnerToReq(req, OwnerMeta);
                if(!isCreateBucket(p, isPostObject, basereq))
                    Utils.checkFrozenUser(OwnerMeta);
                Utils.checkBssBucketAuth(OwnerMeta);
                if (bucketExists) {
                    Utils.checkBssObjectAuth(OwnerMeta,key,req.getMethod());
                }
            }

            bucketLog.preProcessTime = System.currentTimeMillis() - bucketLog.preProcessTime;

            try {
                if (bucketExists) {
                    if (isInternetIP) {
                        // 用户级别连接数+1
                        FiveMinuteUsage.addAndGet(DataRegion.getRegion().getName(), dbBucket.getOwnerId(), null, "connections", 1, Consts.STORAGE_CLASS_STANDARD);
                        // bucket级别连接数+1
                        FiveMinuteUsage.addAndGet(DataRegion.getRegion().getName(), dbBucket.getOwnerId(), dbBucket.getName(), "connections", 1, Consts.STORAGE_CLASS_STANDARD);
                    } else {
                        // 用户级别连接数+1
                        FiveMinuteUsage.addAndGet(DataRegion.getRegion().getName(), dbBucket.getOwnerId(), null, "noNet_connections", 1, Consts.STORAGE_CLASS_STANDARD);
                        // bucket级别连接数+1
                        FiveMinuteUsage.addAndGet(DataRegion.getRegion().getName(), dbBucket.getOwnerId(), dbBucket.getName(), "noNet_connections", 1, Consts.STORAGE_CLASS_STANDARD);
                    }
                }
                if (bucket == null || bucket.length() == 0) {
                    if (owner == null)
                        throw new BaseException(403, ErrorMessage.ERROR_CODE_403, ErrorMessage.ERROR_MESSAGE_403);
                    opService(req, resp, owner, bucketLog, pinData, authResult);// listBuckets
                    resp.setHeader("Connection", "close");
                } else if ((bucket != null && bucket.length() > 0) // bucket exists
                        && (key == null || key.length() == 0) && !isPostObject) {// key NOT exist
                    // 匿名用户禁止发送DELETE、PUT请求，同时禁止请求GET Bucket ACL 接口
                    if (owner == null
                            && (HttpMethod.DELETE.toString().equals(req.getMethod())
                                || HttpMethod.PUT.toString().equals(req.getMethod())
                                || (HttpMethod.GET.toString().equals(req.getMethod()) && req.getParameterMap().containsKey("acl")))) {
                        throw new BaseException(403, ErrorMessage.ERROR_CODE_403, ErrorMessage.ERROR_MESSAGE_403);
                    }
                    // 在ACLs、policy之前处理bucket级别CORS
                    CORSRequestType reqType = OpBucket.checkRequestType(req);
                    if (reqType.equals(CORSRequestType.PRE_FLIGHT)) {
                        if (!bucketExists)
                            throw new BaseException(403, "Forbidden");
                        OpBucket.handlePreflightCORS(dbBucket, req, resp);
                        resp.setStatus(200);
                        Utils.setCommonHeader(resp, new Date(bucketLog.startTime), bucketLog.requestId);
                    } else {
                        if (reqType.equals(CORSRequestType.SIMPLE) || reqType.equals(CORSRequestType.ACTUAL)) {
                            OpBucket.handleSimpleAndActualCORS(req, resp, dbBucket);
                        }
                        opBucket(req, resp, dbBucket, owner, ip, bucketExists, bucketLog, pinData, authResult);
                        resp.setHeader("Connection", "close");
                    }
                } else if ((bucket != null && bucket.length() > 0) // bucket exists.
                        && ((key != null && key.length() > 0) || isPostObject)) {// key exists
                    if (bucketExists) {
                        // 在ACLs、policy之前处理object级别CORS
                        CORSRequestType reqType = OpBucket.checkRequestType(req);
                        if (reqType.equals(CORSRequestType.PRE_FLIGHT)) {
                            OpBucket.handlePreflightCORS(dbBucket, req, resp);
                            try {
                                opKey(req, resp, dbBucket, key, owner, ip, bucketLog, isPostObject, formPostHeaderNotice, pinData, authResult, null);
                            } catch (BaseException e) {
                                handleErrorDocument(e, resp, dbBucket, req.getHeader("Host"));
                                throw e;
                            }
                        } else {
                            if (reqType.equals(CORSRequestType.SIMPLE) || reqType.equals(CORSRequestType.ACTUAL)) {
                                OpBucket.handleSimpleAndActualCORS(req, resp, dbBucket);
                            }
                            boolean needLimitConnection = false;
                            try {
                                // 获取action名称
                                String action = getPermissionFromReq(req);
                                // 判断ACL及bucket policy权限
                                AccessEffect accessEffect = ResourceAccessControlUtils.checkObjectACLsAndPolicy(req, dbBucket, key, authResult, action, req.getMethod());
                                // options请求不做基于身份的权限校验
                                if (!req.getMethod().equalsIgnoreCase(HttpMethod.OPTIONS.toString())) {
                                	// 判断用户权限
                                    AccessControlUtils.auth(req, action, dbBucket, key, authResult, accessEffect);
                                }
                                // 对象操作合规保留逻辑校验
                                ObjectLockUtils.objectLockCheck(req, dbBucket, key);
                                if(null != owner) {
                                    needLimitConnection = UserConnectionsLimit.allow(owner.name, req.getMethod());
                                    if(UserBandWidthLimit.allow(owner.name, req.getMethod()))
                                        ip = new BandWidthControlInputStream(ip, owner.name, req.getMethod());
                                }
                                opKey(req, resp, dbBucket, key, owner, ip, bucketLog, isPostObject, formPostHeaderNotice, pinData, authResult, accessEffect);
                            } catch (BaseException e) {
                                handleErrorDocument(e, resp, dbBucket, req.getHeader("Host"));
                                throw e;
                            }finally {
                                if(needLimitConnection)
                                    UserConnectionsLimit.release(owner.name, req.getMethod());
                            }
                        }
                    } else
                        throw new BaseException(404, ErrorMessage.ERROR_CODE_NO_SUCH_BUCKET, ErrorMessage.ERROR_MESSAGE_404);
                } else
                    throw new BaseException();
            } catch (BaseException e) {
                //判断客户端错误优先抛签名异常。
                if(e.status < 500)
                    V4Signer.checkContentSignatureV4(ip);
                throw e;
            } finally {
                if (bucketExists) {
                    if (isInternetIP) {
                        // bucket级别连接数-1
                        FiveMinuteUsage.addAndGet(DataRegion.getRegion().getName(), dbBucket.getOwnerId(), dbBucket.getName(), "connections", -1, Consts.STORAGE_CLASS_STANDARD);
                        // 用户级别连接数-1
                        FiveMinuteUsage.addAndGet(DataRegion.getRegion().getName(), dbBucket.getOwnerId(), null, "connections", -1, Consts.STORAGE_CLASS_STANDARD);
                    } else {
                        // bucket级别连接数-1
                        FiveMinuteUsage.addAndGet(DataRegion.getRegion().getName(), dbBucket.getOwnerId(), dbBucket.getName(), "noNet_connections", -1, Consts.STORAGE_CLASS_STANDARD);
                        // 用户级别连接数-1
                        FiveMinuteUsage.addAndGet(DataRegion.getRegion().getName(), dbBucket.getOwnerId(), null, "noNet_connections", -1, Consts.STORAGE_CLASS_STANDARD);

                    }
                }
            }
        } finally {
            // 计量按返回码区分的请求次数
            if (pinData.getMethodType() == null) {
                // 如果请求的bucket存在，则计量到bucket owner上，否则如果能判断出发送请求的owner，计量到该owner上
                pinData.jettyAttributes.respHeadFlow = Utils.getRespHeaderLength(resp);
                if (bucketExists) {
                    if (req.getMethod().equalsIgnoreCase(HttpMethod.GET.toString()) || req.getMethod().equalsIgnoreCase(HttpMethod.HEAD.toString()) 
                            || req.getMethod().equalsIgnoreCase(HttpMethod.OPTIONS.toString())) {
                        Utils.pinTransfer(dbBucket.ownerId, pin, pinData, isInternetIP, dbBucket.name);
                    } else {
                        Utils.pinUpload(dbBucket.ownerId, pin, pinData, isInternetIP, dbBucket.name);
                    }
                    pinData.setRequestInfo(dbBucket.ownerId, dbBucket.name, isInternetIP, req.getMethod());
                } else if(owner != null) {
                    if (req.getMethod().equalsIgnoreCase(HttpMethod.GET.toString()) || req.getMethod().equalsIgnoreCase(HttpMethod.HEAD.toString()) 
                            || req.getMethod().equalsIgnoreCase(HttpMethod.OPTIONS.toString())) {
                        Utils.pinTransfer(owner.getId(), pin, pinData, isInternetIP, null);
                    } else {
                        Utils.pinUpload(owner.getId(), pin, pinData, isInternetIP, null);
                    }
                    pinData.setRequestInfo(owner.getId(), null, isInternetIP, req.getMethod());
                }
            }
            
            Backoff.releaseHandler();
            try{
                if (bucketExists && dbBucket.getOwnerId() != 0) {
                    bucketLog.prepareLogTime = System.currentTimeMillis();
                    String iamUserName = null;
                    // 如果是子用户，获取子用户名称
                    if (authResult != null && !authResult.isRoot() && authResult.accessKey != null) {
                        iamUserName = authResult.accessKey.userName;
                    }
                    bucketLog.prepare(dbBucket, key, owner, req, resp.getStatus(), pinData, iamUserName);
                    bucketLog.prepareLogTime = System.currentTimeMillis()
                            - bucketLog.prepareLogTime;
                }
            } catch (Throwable t){
                log.error(t.getMessage(), t);
            }
			try {
                if(ip != null)
                    ip.close();
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }
    }

    private boolean isCreateBucket(
            Pair<String, String> bucketAndObject, boolean isPost, Request req) {
        String bucket = bucketAndObject.first();
        String key = bucketAndObject.second();
        String method = req.getMethod();
        if((bucket != null && bucket.length() > 0)
                && (key == null || key.length() == 0) && !isPost) {
            if(method.equalsIgnoreCase(HttpMethod.PUT.toString()) 
                    && req.getParameterMap().size() == 0)
                return true;
        }
        return false;
    }
  
    private void handleErrorDocument(BaseException e, HttpServletResponse resp,
            BucketMeta dbBucket, String host) throws BaseException, IOException {
        if (e.status >= 400 && e.status < 500 && dbBucket.errorDocument != null
                && dbBucket.errorDocument.trim().length() != 0 && host != null
                && (checkWebsiteHost(host, dbBucket.getName()) || host.equals(dbBucket.getName()))) {
            log.error(e.getMessage(), e);
            if (dbBucket.permission != BucketMeta.PERM_PUBLIC_READ
                    && dbBucket.permission != BucketMeta.PERM_PUBLIC_READ_WRITE)
                throw new BaseException(403, ErrorMessage.ERROR_CODE_403,
                        "An Error Occurred While Attempting to Retrieve a Custom Error Document");
            else {
                ObjectMeta errorDocument = new ObjectMeta(dbBucket.errorDocument,
                        dbBucket.getName(), dbBucket.metaLocation);
                if(client.isBusy(errorDocument, OOSConfig.getMaxConcurrencyPerRegionServer()))
                    Backoff.backoff();
                if (!client.objectSelect(errorDocument))
                    throw new BaseException(403, ErrorMessage.ERROR_CODE_403,
                            "An Error Occurred While Attempting to Retrieve a Custom Error Document");
                resp.sendRedirect("http://" + host + "/"
                        + URLEncoder.encode(dbBucket.errorDocument, Consts.STR_UTF8));
            }
        }
    }
   
    public boolean ifCanRead(OwnerMeta owner, BucketMeta bucket) {
        if (bucket.permission == BucketMeta.PERM_PUBLIC_READ
                || bucket.permission == BucketMeta.PERM_PUBLIC_READ_WRITE)
            return true;
        if (owner == null)
            return false;
        if (bucket.getOwnerId() == owner.getId())
            return true;
        return false;
    }
}

public class JettyServer implements Program {
    static {
        System.setProperty("log4j.log.app", "oos");
    }
    private static Log log = LogFactory.getLog(JettyServer.class);  
    
    public static long inComingBytes = 0;
    public static long outgoingBytes = 0;
    public static long internalInComingBytes = 0;
    public static long internalOutGoingBytes = 0;
    public static long inComingBytesIpv4 = 0;
    public static long inComingBytesIpv6 = 0;
    public static long outgoingBytesIpv4 = 0;
    public static long outgoingBytesIpv6 = 0;
    public static long lastIncomingBytes = 0;
    public static long lastOutgoingBytes = 0;
    public static long internalLastIncomingBytes = 0;
    public static long internalLastOutgoingBytes = 0;
    // 互联网、非互联网出入流量累计值
    public static long lastNetIncomingBytes = 0;
    public static long lastNetOutgoingBytes = 0;
    public static long lastNoNetIncomingBytes = 0;
    public static long lastNoNetOutgoingBytes = 0;
    
    public static String ostorId;
    
    public static void main(String[] args) throws Exception {
        new JettyServer().exec(args);       
    }
  
    @Override
    public String usage() {
        return "Usage: \n";
    }
    public static enum ServerType{
        API,INTERNAL_API;
    }
    
    /**
     * 判断客户端请求ip是否为ipv4,是否为互联网ip
     * @param socket
     * @return
     */
    private static Pair<Boolean, Boolean> isIpv4AndIsInternetIP(Socket socket) {
        boolean isIpv4 = true;
        boolean isInternetIP = true;
        InetSocketAddress isa = (InetSocketAddress) socket.getRemoteSocketAddress();
        if (isa != null && isa.getAddress() != null) {
            String ip = isa.getAddress().getHostAddress();
            if (log.isDebugEnabled())
                log.debug("request ip is "+ip);
            isInternetIP = Utils.isInternetIP(ip);
            if (ip.contains(":")) {
                isIpv4 = false;
            }
        }
        return new Pair<Boolean, Boolean>(isIpv4, isInternetIP);
    }

    private NetworkTrafficSelectChannelConnector getConnector(int port, ServerType type) {
        QueuedThreadPool pool = new QueuedThreadPool();
        pool.setMaxQueued(OOSConfig.getMaxThreads());
        pool.setMaxThreads(OOSConfig.getMaxThreads());
        pool.setMinThreads(OOSConfig.getMinThreads());
        /* http://wiki.eclipse.org/Jetty/Tutorial/Embedding_Jetty */
        NetworkTrafficSelectChannelConnector httpConnector = new NetworkTrafficSelectChannelConnector();
        httpConnector.setAcceptQueueSize(OOSConfig.getAcceptQueueSize());// 增加backlog
        httpConnector.setThreadPool(pool);       
        httpConnector.setPort(port);
        httpConnector.setMaxIdleTime(OOSConfig.getSocketlIdleTimeout());
        httpConnector.setRequestHeaderSize(Consts.CONNECTOR_REQUEST_HEADER_SIZE);
        httpConnector.setStatsOn(true);
        httpConnector.setAcceptors(OOSConfig.getJettyServerAcceptorsNum());
        NetworkTrafficListener listener = new NetworkTrafficListener() {
            AtomicLong currentConnectNum = new AtomicLong(0);
            AtomicLong currentIncomingBytes = new AtomicLong(0);
            AtomicLong currentOutgoingBytes = new AtomicLong(0);
            
            AtomicLong currentConnectNumIpv4 = new AtomicLong(0);
            AtomicLong currentConnectNumIpv6 = new AtomicLong(0);
            AtomicLong currentIncomingBytesIpv4 = new AtomicLong(0);
            AtomicLong currentIncomingBytesIpv6 = new AtomicLong(0);
            AtomicLong currentOutgoingBytesIpv4 = new AtomicLong(0);
            AtomicLong currentOutgoingBytesIpv6 = new AtomicLong(0);
            // 区分互联网和非互联网流量，以便计算互联网和非互联网可用带宽
            AtomicLong currentNetIncomingBytes = new AtomicLong(0);
            AtomicLong currentNoNetIncomingBytes = new AtomicLong(0);
            AtomicLong currentNetOutgoingBytes = new AtomicLong(0);
            AtomicLong currentNoNetOutgoingBytes = new AtomicLong(0);
            long lastIncomingBytesIpv4 = 0;
            long lastIncomingBytesIpv6 = 0;
            long lastOutgoingBytesIpv4 = 0;
            long lastOutgoingBytesIpv6 = 0;

            @Override
            public void opened(Socket socket) {
                currentConnectNum.getAndIncrement();
                if (isIpv4AndIsInternetIP(socket).first()) {
                    currentConnectNumIpv4.getAndIncrement();
                    if (log.isDebugEnabled())
                        log.debug("currentConnectNumIpv4 +1");
                } else {
                    currentConnectNumIpv6.getAndIncrement();
                    if (log.isDebugEnabled())
                        log.debug("currentConnectNumIpv6 +1");
                }
                if (log.isDebugEnabled())
                    log.debug("open socket, remote ip: " + socket.getRemoteSocketAddress());
            }

            @Override
            public void closed(Socket socket) {
                currentConnectNum.getAndDecrement();
                String ip = socket.getInetAddress().toString();
                if (ip!= null && !ip.contains(":")) {
                    currentConnectNumIpv4.getAndDecrement();
                    if (log.isDebugEnabled())
                        log.debug("currentConnectNumIpv4 -1");
                } else {
                    currentConnectNumIpv6.getAndDecrement();
                    if (log.isDebugEnabled())
                        log.debug("currentConnectNumIpv6 -1");
                }
                if (log.isDebugEnabled())
                    log.debug("close socket, remote ip:" + socket.getInetAddress() + ":" + socket.getPort());
            }

            @Override
            public void incoming(Socket socket, Buffer bytes) {
                currentIncomingBytes.addAndGet(bytes.length());
                Pair<Boolean, Boolean> p = isIpv4AndIsInternetIP(socket);
                // 根据请求ip区分ipv4和ipv6
                if (p.first()) {
                    currentIncomingBytesIpv4.addAndGet(bytes.length());
                    if (log.isDebugEnabled())
                        log.debug("currentIncomingBytesIpv4:"+currentIncomingBytesIpv4.get());
                } else {
                    currentIncomingBytesIpv6.addAndGet(bytes.length());
                    if (log.isDebugEnabled())
                        log.debug("currentIncomingBytesIpv6:"+currentIncomingBytesIpv4.get());
                }
                // 根据请求ip区分互联网和非互联网
                if (p.second()) {
                    currentNetIncomingBytes.addAndGet(bytes.length());
                    if (log.isDebugEnabled())
                        log.debug("currentNetIncomingBytes:"+currentNetIncomingBytes.get());
                } else {
                    currentNoNetIncomingBytes.addAndGet(bytes.length());
                    if (log.isDebugEnabled())
                        log.debug("currentNoNetIncomingBytes:"+currentNoNetIncomingBytes.get());
                }
            }

            @Override
            public void outgoing(Socket socket, Buffer bytes) {
                currentOutgoingBytes.addAndGet(bytes.length());
                Pair<Boolean, Boolean> p = isIpv4AndIsInternetIP(socket);
                // 根据请求ip区分ipv4和ipv6
                if (p.first()) {
                    currentOutgoingBytesIpv4.addAndGet(bytes.length());
                    if (log.isDebugEnabled())
                        log.debug("currentOutgoingBytesIpv4:"+currentOutgoingBytesIpv4.get());
                } else {
                    currentOutgoingBytesIpv6.addAndGet(bytes.length());
                    if (log.isDebugEnabled())
                        log.debug("currentOutgoingBytesIpv6:"+currentOutgoingBytesIpv6.get());
                }
                // 根据请求ip区分互联网和非互联网
                if (p.second()) {
                    currentNetOutgoingBytes.addAndGet(bytes.length());
                    if (log.isDebugEnabled())
                        log.debug("currentNetOutgoingBytes:"+currentNetOutgoingBytes.get());
                } else {
                    currentNoNetOutgoingBytes.addAndGet(bytes.length());
                    if (log.isDebugEnabled())
                        log.debug("currentNoNetOutgoingBytes:"+currentNoNetOutgoingBytes.get());
                }
            }

            {
                new Thread() {
                    @Override
                    public void run() {
                        switch (type) {
                        case API:
                            while (true) {
                                try {
                                    Thread.sleep(OOSConfig.getGangliaInternalTime());
                                    // ipv4+ipv6,inComingBytes,outgoingBytes用于流量控制
                                    inComingBytes = (currentIncomingBytes.get() - lastIncomingBytes)
                                            / OOSConfig.getGangliaInternalTime() * 1000;
                                    outgoingBytes = (currentOutgoingBytes.get() - lastOutgoingBytes)
                                            / OOSConfig.getGangliaInternalTime() * 1000;
                                    lastIncomingBytes = currentIncomingBytes.get();
                                    lastOutgoingBytes = currentOutgoingBytes.get();
                                    // 互联网、非互联网出入流量累计值
                                    lastNetIncomingBytes = currentNetIncomingBytes.get();
                                    lastNetOutgoingBytes = currentNetOutgoingBytes.get();
                                    lastNoNetIncomingBytes = currentNoNetIncomingBytes.get();
                                    lastNoNetOutgoingBytes = currentNoNetOutgoingBytes.get();
                                    // ipv4
                                    inComingBytesIpv4 = (currentIncomingBytesIpv4.get() - lastIncomingBytesIpv4)
                                            / OOSConfig.getGangliaInternalTime() * 1000;
                                    outgoingBytesIpv4 = (currentOutgoingBytesIpv4.get() - lastOutgoingBytesIpv4)
                                            / OOSConfig.getGangliaInternalTime() * 1000;
                                    lastIncomingBytesIpv4 = currentIncomingBytesIpv4.get();
                                    lastOutgoingBytesIpv4 = currentOutgoingBytesIpv4.get();
                                    GangliaPlugin.sendToGanglia(
                                            GangliaConsts.API_CURRENT_CONNECT_NUM_IPV4,
                                            String.valueOf(currentConnectNumIpv4),
                                            GangliaConsts.YTYPE_INT32, GangliaConsts.YNAME_NUM,
                                            GangliaConsts.GROUP_NAME_API_SERVER_HTTP_CONNECTION,
                                            "\"current connection number from ipv4\"");
                                    GangliaPlugin.sendToGanglia(GangliaConsts.API_INCOMING_BYTES_IPV4,
                                            String.valueOf(inComingBytesIpv4),
                                            GangliaConsts.YTYPE_INT32,
                                            GangliaConsts.YNAME_BYTES_PER_SECOND,
                                            GangliaConsts.GROUP_NAME_API_SERVER_HTTP_CONNECTION,
                                            "\"OOS incoming bytes from ipv4\"");
                                    GangliaPlugin.sendToGanglia(GangliaConsts.API_OUTGOING_BYTES_IPV4,
                                            String.valueOf(outgoingBytesIpv4),
                                            GangliaConsts.YTYPE_INT32,
                                            GangliaConsts.YNAME_BYTES_PER_SECOND,
                                            GangliaConsts.GROUP_NAME_API_SERVER_HTTP_CONNECTION,
                                            "\"OOS outgoing bytes from ipv4\"");
                                    GangliaPlugin.sendToGanglia(
                                            GangliaConsts.API_LAST_INCOMING_BYTES_IPV4,
                                            String.valueOf(lastIncomingBytesIpv4),
                                            GangliaConsts.YTYPE_INT32, GangliaConsts.YNAME_BYTES,
                                            GangliaConsts.GROUP_NAME_API_SERVER_HTTP_CONNECTION,
                                            "\"OOS last second incoming total bytes from ipv4\"");
                                    GangliaPlugin.sendToGanglia(
                                            GangliaConsts.API_LAST_OUTGOING_BYTES_IPV4,
                                            String.valueOf(lastOutgoingBytesIpv4),
                                            GangliaConsts.YTYPE_INT32, GangliaConsts.YNAME_BYTES,
                                            GangliaConsts.GROUP_NAME_API_SERVER_HTTP_CONNECTION,
                                            "\"OOS last second outgoing total bytes from ipv4\"");
                                    // ipv6
                                    inComingBytesIpv6 = (currentIncomingBytesIpv6.get() - lastIncomingBytesIpv6)
                                            / OOSConfig.getGangliaInternalTime() * 1000;
                                    outgoingBytesIpv6 = (currentOutgoingBytesIpv6.get() - lastOutgoingBytesIpv6)
                                            / OOSConfig.getGangliaInternalTime() * 1000;
                                    lastIncomingBytesIpv6 = currentIncomingBytesIpv6.get();
                                    lastOutgoingBytesIpv6 = currentOutgoingBytesIpv6.get();
                                    GangliaPlugin.sendToGanglia(
                                            GangliaConsts.API_CURRENT_CONNECT_NUM_IPV6,
                                            String.valueOf(currentConnectNumIpv6),
                                            GangliaConsts.YTYPE_INT32, GangliaConsts.YNAME_NUM,
                                            GangliaConsts.GROUP_NAME_API_SERVER_HTTP_CONNECTION,
                                            "\"current connection number from ipv6\"");
                                    GangliaPlugin.sendToGanglia(GangliaConsts.API_INCOMING_BYTES_IPV6,
                                            String.valueOf(inComingBytesIpv6),
                                            GangliaConsts.YTYPE_INT32,
                                            GangliaConsts.YNAME_BYTES_PER_SECOND,
                                            GangliaConsts.GROUP_NAME_API_SERVER_HTTP_CONNECTION,
                                            "\"OOS incoming bytes from ipv6\"");
                                    GangliaPlugin.sendToGanglia(GangliaConsts.API_OUTGOING_BYTES_IPV6,
                                            String.valueOf(outgoingBytesIpv6),
                                            GangliaConsts.YTYPE_INT32,
                                            GangliaConsts.YNAME_BYTES_PER_SECOND,
                                            GangliaConsts.GROUP_NAME_API_SERVER_HTTP_CONNECTION,
                                            "\"OOS outgoing bytes from ipv6\"");
                                    GangliaPlugin.sendToGanglia(
                                            GangliaConsts.API_LAST_INCOMING_BYTES_IPV6,
                                            String.valueOf(lastIncomingBytesIpv6),
                                            GangliaConsts.YTYPE_INT32, GangliaConsts.YNAME_BYTES,
                                            GangliaConsts.GROUP_NAME_API_SERVER_HTTP_CONNECTION,
                                            "\"OOS last second incoming total bytes from ipv6\"");
                                    GangliaPlugin.sendToGanglia(
                                            GangliaConsts.API_LAST_OUTGOING_BYTES_IPV6,
                                            String.valueOf(lastOutgoingBytesIpv6),
                                            GangliaConsts.YTYPE_INT32, GangliaConsts.YNAME_BYTES,
                                            GangliaConsts.GROUP_NAME_API_SERVER_HTTP_CONNECTION,
                                            "\"OOS last second outgoing total bytes from ipv6\"");
                                } catch (Throwable e) {
                                    log.error(e.getMessage(), e);
                                }
                            }
                        case INTERNAL_API:
                            while (true) {
                                try {
                                    Thread.sleep(OOSConfig.getGangliaInternalTime());
                                    internalInComingBytes = (currentIncomingBytes.get()
                                            - internalLastIncomingBytes)
                                            / OOSConfig.getGangliaInternalTime() * 1000;
                                    internalOutGoingBytes = (currentOutgoingBytes.get()
                                            - internalLastOutgoingBytes)
                                            / OOSConfig.getGangliaInternalTime() * 1000;
                                    internalLastIncomingBytes = currentIncomingBytes.get();
                                    internalLastOutgoingBytes = currentOutgoingBytes.get();
                                    GangliaPlugin.sendToGanglia(
                                            GangliaConsts.INTERNAL_API_CURRENT_CONNECT_NUM,
                                            String.valueOf(currentConnectNum),
                                            GangliaConsts.YTYPE_INT32, GangliaConsts.YNAME_NUM,
                                            GangliaConsts.GROUP_NAME_INTERNAL_API_SERVER_HTTP_CONNECTION,
                                            "\"current connection number\"");
                                    GangliaPlugin.sendToGanglia(
                                            GangliaConsts.INTERNAL_API_INCOMING_BYTES,
                                            String.valueOf(internalInComingBytes),
                                            GangliaConsts.YTYPE_INT32,
                                            GangliaConsts.YNAME_BYTES_PER_SECOND,
                                            GangliaConsts.GROUP_NAME_INTERNAL_API_SERVER_HTTP_CONNECTION,
                                            "\"OOS incoming bytes\"");
                                    GangliaPlugin.sendToGanglia(
                                            GangliaConsts.INTERNAL_API_OUTGOING_BYTES,
                                            String.valueOf(internalOutGoingBytes),
                                            GangliaConsts.YTYPE_INT32,
                                            GangliaConsts.YNAME_BYTES_PER_SECOND,
                                            GangliaConsts.GROUP_NAME_INTERNAL_API_SERVER_HTTP_CONNECTION,
                                            "\"OOS outgoing bytes\"");
                                } catch (Throwable e) {
                                    log.error(e.getMessage(), e);
                                }
                            }
                        }
                    }
                }.start();
            }
        };
        httpConnector.addNetworkTrafficListener(listener);
        return httpConnector;
    }
    
    @Override
    public void exec(String[] args) throws Exception {
        GetOpt opts = new GetOpt("[p]:[sslp]:", args);
        int port = opts.getInt("p", 8080);
        int internalPort = opts.getInt("interp", 8081);
        int sslPort = opts.getInt("sslp", 8443);
        System.setProperty("bufferPool.maxSize", String.valueOf(OOSConfig.getBufferPoolMaxSize()));
        Properties properties = new Properties();
        InputStream is = null;
        try {
            is = new FileInputStream(System.getenv("OOS_HOME") + "/conf/oos.properties");
            properties.load(is);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            if (is != null)
                is.close();
        }
        ostorId = properties.getProperty("ostorId");
        if (ostorId == null)
            throw new RuntimeException("no ostorId");
        // 先初始化一下direct buffer
        OstorProxy.getProxy();
        // 如果表不存在，建表
        try {
            Configuration globalConf = GlobalHHZConfig.getConfig();
            HConnection globalConn = HBaseConnectionManager.createConnection(globalConf);
            HBaseAdmin globalHbaseAdmin = new HBaseAdmin(globalConn);
            Configuration regionConfig = RegionHHZConfig.getConfig();
            HConnection regionConn = HBaseConnectionManager.createConnection(regionConfig);
            HBaseAdmin regionHbaseAdmin = new HBaseAdmin(regionConn);
            
            HBaseAkSk.createTable(globalHbaseAdmin);
            HBaseInitialUpload.createTable(globalHbaseAdmin);
            HBaseObject.createTable(globalHbaseAdmin);
            HBaseUpload.createTable(globalHbaseAdmin);
            HBaseManagerLog.createTable(globalHbaseAdmin);
            OperationLog.createTable(globalHbaseAdmin);
            HBaseBucket.createTable(globalHbaseAdmin);
//            HBaseUsageCurrent.createTable(globalHbaseAdmin);
            HBaseOwner.createTable(globalHbaseAdmin);
            HbasePdiskInfo.createTable(regionHbaseAdmin);
//            OOSMqType.mq_lifecycle.createTable(globalHbaseAdmin);
            OOSMqType.mq_etag_unmatch.createTable(globalHbaseAdmin);
//            HBaseProgress.createTable(globalHbaseAdmin);
            HBaseAccelerate.createTable(globalHbaseAdmin);
            HBaseCdnVendor.createTable(globalHbaseAdmin);
            HBaseUserToTag.createTable(globalHbaseAdmin);
            HBaseVSNTag.createTable(globalHbaseAdmin);
            HBaseRole.createTable(globalHbaseAdmin);
            HBaseUserToRole.createTable(globalHbaseAdmin);
            HBaseNoninternetIP.createTable(globalHbaseAdmin);
            HBaseMinutesUsage.createTable(globalHbaseAdmin);
            HBaseTokenMeta.createTable(globalHbaseAdmin);
            HBaseUserType.createTable(globalHbaseAdmin);
            HBaseManageEvent.createTable(globalHbaseAdmin);
            HBaseCloudTrail.createTable(globalHbaseAdmin);
//          globalHbaseAdmin.close();
//          regionHbaseAdmin.close();
            log.info("CreateTable success.");
        } catch (Throwable t) {
            t.printStackTrace();
            System.exit(-1);
        }
        //预加载
        try {
            Class.forName("cn.ctyun.oos.ostor.OstorClient");
            log.info("Pre Load cn.ctyun.oos.ostor.OstorClient success.");
            Class.forName("cn.ctyun.oos.server.OpObject");
            log.info("Pre Load cn.ctyun.oos.server.OpObject success.");
            Class.forName("cn.ctyun.common.conf.StorageClassConfig");
            log.info("Pre Load cn.ctyun.common.conf.StorageClassConfig success.");
            Class.forName("cn.ctyun.oos.server.InternalHandler");
            log.info("Pre Load cn.ctyun.oos.server.InternalHandler success.");
            Class.forName("cn.ctyun.oos.server.storage.OstorKeyGenerator");
            log.info("Pre Load cn.ctyun.oos.server.storage.OstorKeyGenerator success.");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(-1);
        }
        // 外部api http connector
        NetworkTrafficSelectChannelConnector httpConnector = getConnector(port, ServerType.API);
        // 内部api http connector
        NetworkTrafficSelectChannelConnector internalConnector = getConnector(internalPort,ServerType.INTERNAL_API);
        // 外部api https connector
        QueuedThreadPool sslPool = new QueuedThreadPool();
        sslPool.setMaxQueued(OOSConfig.getMaxThreads());
        sslPool.setMaxThreads(OOSConfig.getMaxThreads());
        sslPool.setMinThreads(OOSConfig.getMinThreads());
        final SslSelectChannelConnector sslConnector = new SslSelectChannelConnector();
        sslConnector.setPort(sslPort);
        sslConnector.setMaxIdleTime(OOSConfig.getSocketlIdleTimeout());
        sslConnector.setRequestHeaderSize(Consts.CONNECTOR_REQUEST_HEADER_SIZE);
        sslConnector.setAcceptQueueSize(OOSConfig.getAcceptQueueSize());// 增加backlog
        sslConnector.setThreadPool(sslPool);
        sslConnector.setStatsOn(true);
        sslConnector.setAcceptors(OOSConfig.getJettyServerAcceptorsNum());
        SslContextFactory cf = sslConnector.getSslContextFactory();
        cf.setKeyStorePath(System.getenv("OOS_HOME") + OOSConfig.getOosKeyStore());
        cf.setKeyStorePassword(OOSConfig.getOosSslPasswd());
        final Server server = new Server();
        server.setThreadPool(new QueuedThreadPool(OOSConfig.getServerThreadPool())); // 增加最大线程数
        server.setSendServerVersion(true);
        server.setConnectors(new Connector[] { httpConnector, internalConnector });
        server.setHandler(new HttpHandler(server.getConnectors(), port, sslPort));
        MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
        MBeanContainer mBeanContainer = new MBeanContainer(mBeanServer);
        server.getContainer().addEventListener(mBeanContainer);
        server.setGracefulShutdown(OOSConfig.getGracefullShutdown());
        server.setStopAtShutdown(true);
        mBeanContainer.start();
        send2GangliaPutGetConnNum();
        new Thread() {
            @Override
            public void run() {
                Map<String, QueuedThreadPool> pools = new HashMap<String, QueuedThreadPool>();
                pools.put("api http", (QueuedThreadPool) httpConnector.getThreadPool());
                pools.put("api https", (QueuedThreadPool) sslConnector.getThreadPool());
                pools.put("internal http", (QueuedThreadPool) internalConnector.getThreadPool());
                for (;;) {
                    for (String p : pools.keySet()) {
                        QueuedThreadPool pool = pools.get(p);
                        String strStats = String.format(
                                p + ": IdleThread is:%d, MinThreads is:%d, MaxThreads is:%d, MaxQueued is %d, ThreadsNum is:%d, toString():%s, state is:%s",
                                pool.getIdleThreads(), pool.getMinThreads(), pool.getMaxThreads(),
                                pool.getMaxQueued(), pool.getThreads(), pool.toString(),
                                pool.getState());
                        log.info(strStats);
                    }
                    try {
                        Thread.sleep(10000);
                    } catch (InterruptedException e) {
                        log.error(e.getMessage(), e);
                    }
                }
            }
        }.start();
        
        //钩子，确保关闭时将缓存刷进数据库
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                FiveMinuteUsage.flushBeforeExit();
            }
        });
        
        try {
            server.start();
            LogUtils.startSuccess();
            server.join();
        } catch (Throwable e) {
            log.error(e.getMessage(), e);
            System.exit(-1);
        }
    }

    /**
     * 定期将put、get连接数发送至ganglia
     */
    private void send2GangliaPutGetConnNum() {
        new Thread() {
            @Override
            public void run() {
                while (true) {
                    try {
                        Thread.sleep(OOSConfig.getGangliaInternalTime());
                        GangliaPlugin.sendToGanglia(
                                GangliaConsts.API_PUT_CONNECT_NUM,
                                String.valueOf(Backoff.putConnections.get()),
                                GangliaConsts.YTYPE_INT32, GangliaConsts.YNAME_NUM,
                                GangliaConsts.GROUP_NAME_API_SERVER_HTTP_CONNECTION,
                                "\"put connection number\"");
                        GangliaPlugin.sendToGanglia(
                                GangliaConsts.API_GET_CONNECT_NUM,
                                String.valueOf(Backoff.getConnections.get()),
                                GangliaConsts.YTYPE_INT32, GangliaConsts.YNAME_NUM,
                                GangliaConsts.GROUP_NAME_API_SERVER_HTTP_CONNECTION,
                                "\"get connection number\"");
                    } catch (Throwable e) {
                        log.error(e.getMessage(), e);
                    }
                }
            }
        }.start();
    }
}