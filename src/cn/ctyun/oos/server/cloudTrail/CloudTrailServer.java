package cn.ctyun.oos.server.cloudTrail;

import java.io.IOException;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import javax.management.MBeanServer;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpHeaders;
import org.eclipse.jetty.jmx.MBeanContainer;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.server.nio.SelectChannelConnector;
import org.eclipse.jetty.server.ssl.SslSelectChannelConnector;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.amazonaws.HttpMethod;
import com.amazonaws.services.s3.Headers;

import cn.ctyun.common.BaseException;
import cn.ctyun.common.Consts;
import cn.ctyun.common.Program;
import cn.ctyun.common.conf.GlobalHHZConfig;
import cn.ctyun.common.conf.OOSConfig;
import cn.ctyun.common.dsync.DLock;
import cn.ctyun.common.dsync.DSyncService;
import cn.ctyun.common.node.OosZKNode;
import cn.ctyun.common.utils.LogUtils;
import cn.ctyun.oos.common.AuthResult;
import cn.ctyun.oos.common.ErrorMessage;
import cn.ctyun.oos.common.Utils;
import cn.ctyun.oos.common.WebsiteHeader;
import cn.ctyun.oos.hbase.MetaClient;
import cn.ctyun.oos.iam.accesscontroller.AccessController;
import cn.ctyun.oos.iam.accesscontroller.AccessEffect;
import cn.ctyun.oos.iam.accesscontroller.RequestInfo;
import cn.ctyun.oos.metadata.CloudTrailEvent;
import cn.ctyun.oos.metadata.CloudTrailManageEvent;
import cn.ctyun.oos.metadata.CloudTrailMeta;
import cn.ctyun.oos.metadata.ManageEventMeta;
import cn.ctyun.oos.metadata.OwnerMeta;
import cn.ctyun.oos.server.conf.CloudTrailConfig;
import cn.ctyun.oos.server.signer.V4Signer;
import common.util.GetOpt;

/**
 * 日志审计服务
 * TODO 加log与注释
 * @author wushuang
 *
 */
class CloudTrailHandler extends AbstractHandler {
    private static final Log log = LogFactory.getLog(CloudTrailHandler.class);
    private static MetaClient metaClient = MetaClient.getGlobalClient();
    /** 访问控制 */
    private static AccessController accessController = new AccessController();
    
    public CloudTrailHandler() throws Exception {
    }

    class ErrorResource {
        String resource = "/";
    }
    
    @Override
    public void handle(String target, Request basereq, HttpServletRequest req, HttpServletResponse resp)
            throws IOException, ServletException {
        InputStream inputStream = null;
        ManageEventMeta manageEvent = null;
        ErrorResource resource = new ErrorResource();
        try {
            try {
                inputStream = req.getInputStream();
            } catch (Exception e) {
                log.error(e.getMessage(), e);
                throw new BaseException(400, ErrorMessage.ERROR_CODE_INCOMPLETE_BODY, ErrorMessage.ERROR_MESSAGE_INCOMPLETE_BODY);
            }

            String reqid = Long.toHexString(UUID.randomUUID().getMostSignificantBits());
            req.setAttribute(Headers.REQUEST_ID, reqid);
            req.setAttribute(Headers.DATE, new Date().getTime());
            Utils.log(req, Utils.getIpAddr(req));
            Utils.checkDate(req);
            
            // 不支持V2签名
            if (isAuthV2(req)) {
                throw new BaseException(403, ErrorMessage.ERROR_CODE_SIGNATURE_DOES_NOT_MATCH, "The request signature does not conform to CloudTrail standards.");
            }
            // 签名认证            
            AuthResult authResult = Utils.auth(basereq, req, null, null, false, true, V4Signer.CLOUDTRAIL_SERVICE_NAME);
            inputStream = authResult.inputStream;
            OwnerMeta owner = authResult.owner;
            if (owner == null)
                throw new BaseException(403, ErrorMessage.ERROR_CODE_403);

            // 默认开启权限
            if (!owner.ifRecordManageEvent) {
                owner.ifRecordManageEvent = true;
                metaClient.ownerUpdate(owner);
            }
            // 记录管理事件
            manageEvent = new ManageEventMeta();
            CloudTrailEvent event = manageEvent.getEvent();
            // 固定参数
            event.eventSource = "oos-cn-cloudtrail.ctyunapi.cn";
            event.eventType = "ApiCall";
            event.serviceName = "CloudTrail";
            event.managementEvent = true;
            // 用户参数
            if (authResult.isSts) {
                event.userType = "STSUser";
                event.accessKeyId = authResult.tokenMeta.stsAccessKey;
            } else if (authResult.isRoot()) {
                event.userType = "Root";
                event.principalId = authResult.owner.getAccountId();
                event.arn = authResult.getUserArn();
                event.accessKeyId = authResult.accessKey.accessKey;
            } else {
                event.userType = "IAMUser";
                event.userName = authResult.accessKey.userName;
                event.principalId = authResult.accessKey.userId;
                event.arn = authResult.getUserArn();
                event.accessKeyId = authResult.accessKey.accessKey;
            }
            event.eventOwnerId = owner.getId();
            event.ownerId = owner.getId();
            // 请求的事件参数
            event.eventTime = (long) req.getAttribute(Headers.DATE);
            event.requestId = (String) req.getAttribute(Headers.REQUEST_ID);
            event.userAgent = req.getHeader(HttpHeaders.USER_AGENT);
            event.sourceIpAddress = Utils.getIpAddr(req);
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
            event.requestRegion = Utils.getRegionNameFromReq(req, V4Signer.CLOUDTRAIL_SERVICE_NAME);

            if (req.getHeader("OOS-PROXY-HOST") != null && req.getHeader("OOS-PROXY-HOST").equals("oos-proxy")) {// 来自proxy转发的请求
                event.accessKeyId = null;
                event.eventSource = "oos-cn.ctyun.cn";
                event.userAgent =  "oos-cn.ctyun.cn";
                event.requestURL = req.getParameter("proxyURL");
                event.sourceIpAddress = req.getHeader(WebsiteHeader.CLIENT_IP);
            }
            
            if (req.getMethod().equalsIgnoreCase(HttpMethod.POST.toString())) {
                String action = getAction(req);
                CloudTrailManageEvent eventAction = checkAndGetCloudTrailAction(action);
                event.eventName = eventAction.getName();
                event.readOnly = eventAction.getReadOnly();
                String inputString = IOUtils.toString(inputStream);
                event.requestParameters = inputString;

                String trailName = "*";
                try {
                    JSONObject jo = new JSONObject(inputString);
                    if (jo.has("Name")) {
                        trailName = jo.getString("Name");
                        resource.resource = trailName;
                    }
                    else if (jo.has("TrailName")) {
                        trailName = jo.getString("TrailName");
                        resource.resource = trailName;
                    }
                    else {
                        resource.resource = "/";
                    }
                } catch (JSONException e) {
                }
                CloudTrailMeta meta = new CloudTrailMeta(authResult.owner.getId(), trailName);
                
                // 对非根用户进行访问控制
                if (!authResult.isRoot()) {
                    // 访问控制
                    RequestInfo requestInfo = new RequestInfo("cloudtrail:" + action, meta.getARN(), authResult.owner.getAccountId(),
                            authResult.accessKey.userId, authResult.accessKey.userName, req);
                    // 进行访问控制操作
                    AccessEffect accessEffect = accessController.allow(requestInfo);
                    // 拒绝或隐式拒绝
                    if (accessEffect == AccessEffect.Deny || accessEffect == AccessEffect.ImplicitDeny) {
                        // 拒绝访问处理
                        throw new BaseException(403, "AccessDenied", requestInfo.getErrorMessage());
                    }
                }
                String actionContent = handleAction(owner, inputString, action, manageEvent);
                resp.setHeader(Headers.CONTENT_TYPE, "application/json");
                writeResponse(req, resp, actionContent);
            } else {
                throw new BaseException(405, "MethodNotAllowed", "Method is not allowed.");
            }
        } catch (BaseException e) {
            log.error(e.getMessage(), e);
            resp.setContentType("text/xml");
            resp.setStatus(e.status);
            e.reqId = (String) req.getAttribute(Headers.REQUEST_ID);
            try {
                resource.resource = URLEncoder.encode(resource.resource, Consts.STR_UTF8);
                e.resource = resource.resource;
            } catch (Exception e2) {
            }
            if(manageEvent != null) {
                manageEvent.getEvent().errorStatus = e.status;
                manageEvent.getEvent().errorCode = e.code;
                manageEvent.getEvent().errorMessage = e.message;
            }
            try {
                writeResponse(req, resp, e.toXmlWriter().toString());
            } catch (IOException e1) {
                log.error(e1.getMessage(), e1);
            }
        } catch (Throwable t) {
            log.error(t.getMessage(), t);
            BaseException be = new BaseException();
            be.status = 500;
            be.reqId = (String) req.getAttribute(Headers.REQUEST_ID);
            try {
                resource.resource = URLEncoder.encode(resource.resource, Consts.STR_UTF8);
                be.resource = resource.resource;
            } catch (Exception e2) {
            }            
            be.message = "We encountered an internal error. Please try again.";
            be.code = "InternalError";
            resp.setContentType("text/xml");
            resp.setStatus(be.status);
            if(manageEvent != null) {
                manageEvent.getEvent().errorStatus = be.status;
                manageEvent.getEvent().errorCode = be.code;
                manageEvent.getEvent().errorMessage = be.message;
            }
            try {
                writeResponse(req, resp, be.toXmlWriter().toString());
            } catch (IOException e1) {
                log.error(e1.getMessage(), e1);
            }        
        } finally {
            if (inputStream != null)
                try {
                    inputStream.close();
                } catch (Exception e) {
                }
            basereq.setHandled(true);
            // 当manageEvent不为空时，表示需要记录用户的管理事件
            if(manageEvent != null && manageEvent.getEvent().eventName != null && manageEvent.getEvent().eventName.length() != 0)
                try {
                    metaClient.manageEventInsert(manageEvent);
                } catch (Exception e) {
                    log.error("cloudTrailServer error: insert manageEvent error!" + manageEvent.getRowKey());
                }
        }
    }

    private static String handleAction(OwnerMeta owner, String input, String action, ManageEventMeta manageEvent)
            throws Exception {
        switch (action) {
        case "CreateTrail":
            return createTrail(owner, input, manageEvent);
        case "DeleteTrail":
            deleteTrail(owner, input, manageEvent);
            return null;
        case "DescribeTrails":
            return describeTrails(owner, input, manageEvent);
        case "GetTrailStatus":
            return getTrailStatus(owner, input, manageEvent);
        case "PutEventSelectors":
            return putEventSelectors(owner, input, manageEvent);
        case "GetEventSelectors":
            return getEventSelectors(owner, input, manageEvent);
        case "UpdateTrail":
            return updateTrail(owner, input, manageEvent);
        case "StartLogging":
            startLogging(owner, input, manageEvent);
            return null;
        case "StopLogging":
            stopLogging(owner, input, manageEvent);
            return null;
        case "LookupEvents":
            return lookupEvents(owner, input, manageEvent);
        default:
            return null;
        }
    }
    
    private static String createTrail(OwnerMeta owner, String input, ManageEventMeta manageEvent) throws Exception {
        String trailName = null;
        String targetBucket = null;
        String prefix = null;
        try {
            JSONObject jo = new JSONObject(input);
            trailName = jo.getString("Name");
            CloudTrailAction.checkTrailNameFormat(trailName);
            if (!jo.has("S3BucketName"))
                throw new BaseException(400, "InvalidS3BucketNameException", "Bucket name cannot be blank!");
            targetBucket = jo.getString("S3BucketName");
            prefix = null;
            if (jo.has("S3KeyPrefix")) {
                prefix = jo.getString("S3KeyPrefix");
                CloudTrailAction.checkTrailFilePrefix(prefix);
            }
        } catch (JSONException e) {
            // json格式错误也认为是审计名称为空
            log.error(e.getMessage(), e); 
            throw new BaseException(400, "InvalidTrailNameException", "Trail name cannot be blank!");
        }
        return CloudTrailAction.createTrailWithManageEvent(owner, trailName, targetBucket, prefix, manageEvent);
    }

    private static void deleteTrail(OwnerMeta owner, String input, ManageEventMeta manageEvent) throws Exception {
        String trailName = null;
        try {
            JSONObject jo = new JSONObject(input);
            trailName = jo.getString("Name");
        } catch (JSONException e) {
            log.error(e.getMessage(), e); 
            throw new BaseException(400, "InvalidTrailNameException", "Trail name cannot be blank!");
        }
        CloudTrailAction.deleteTrailWithManageEvent(owner, trailName, manageEvent);
    }

    private static String describeTrails(OwnerMeta owner, String input, ManageEventMeta manageEvent) throws JSONException, IOException, BaseException {
        List<String> trailNames = null;
        if (input != null) {
            try {
                JSONObject jo = new JSONObject(input);
                if(jo.has("TrailNameList")) {
                    JSONArray ja = jo.getJSONArray("TrailNameList");
                    trailNames = new ArrayList<String>();
                    for (int i = 0; i < ja.length(); i++) {
                        trailNames.add(ja.getString(i));
                    }
                }
            } catch (JSONException e) {
                log.error(e.getMessage(), e); 
                throw new BaseException(400, "InvalidTrailNameException", "Trail name cannot be blank!");
            }
        }
        return CloudTrailAction.describeTrailsWithManageEvent(owner, trailNames, manageEvent);
    }

    private static String getTrailStatus(OwnerMeta owner, String input, ManageEventMeta manageEvent) throws IOException, JSONException, BaseException {
        String trailName = null;
        try {
            JSONObject jo = new JSONObject(input);
            trailName = jo.getString("Name"); 
        } catch (JSONException e) {
            log.error(e.getMessage(), e);
            throw new BaseException(400, "InvalidTrailNameException", "Trail name cannot be blank!");
        }
        return CloudTrailAction.getTrailStatusWithManageEvent(owner, trailName, manageEvent);
    }

    private static String putEventSelectors(OwnerMeta owner, String input, ManageEventMeta manageEvent) throws JSONException, BaseException, IOException {
        String trailName = null;
        boolean includeManagementEvents = true;
        String readWriteType = null;
        try {
            JSONObject jo = new JSONObject(input);
            trailName = jo.getString("TrailName");
            JSONArray selectorJa = jo.getJSONArray("EventSelectors");
            // 本期一个事件追踪的选择器只能有一个
            if(selectorJa.length() > 1)
                throw new BaseException(400, "InvalidEventSelectorsException", "Specify a valid number of selectors  for your trail");
            // 本期只有管理事件，不加该参数
            if (selectorJa.length() > 0) {
                JSONObject selectorJo = selectorJa.getJSONObject(0);
                // boolean includeManagementEvents = selectorJo.getBoolean("IncludeManagementEvents");
                if (selectorJo.has("ReadWriteType"))
                    readWriteType = selectorJo.getString("ReadWriteType");
            }
        } catch (JSONException e) {
            log.error(e.getMessage(), e);
            throw new BaseException(400, "InvalidTrailNameException", "Trail name cannot be blank!");
        }
       
        return CloudTrailAction.putEventSelectorsWithManageEvent(owner, trailName, includeManagementEvents, readWriteType, manageEvent);
    }

    private static String getEventSelectors(OwnerMeta owner, String input, ManageEventMeta manageEvent) throws JSONException, IOException, BaseException {
        String trailName = null;
        try {
            JSONObject jo = new JSONObject(input);
            trailName = jo.getString("TrailName");
        } catch (JSONException e) {
            log.error(e.getMessage(), e);
            throw new BaseException(400, "InvalidTrailNameException", "Trail name cannot be blank!");
        }
        
        return CloudTrailAction.getEventSelectorsWithManageEvent(owner, trailName, manageEvent);
    }

    private static String updateTrail(OwnerMeta owner, String input, ManageEventMeta manageEvent) throws IOException, JSONException, BaseException {
        String trailName = null;
        String targetBucket = null;
        Object prefix = null;
        try {
            JSONObject jo = new JSONObject(input);
            trailName = jo.getString("Name");
            targetBucket = null;
            prefix = null;
            if (jo.has("S3BucketName"))
                targetBucket = jo.getString("S3BucketName");
            if (jo.has("S3KeyPrefix")) {   
                prefix = jo.get("S3KeyPrefix");
                CloudTrailAction.checkTrailFilePrefix(prefix.toString());
            }
        } catch (JSONException e) {
            log.error(e.getMessage(), e);
            throw new BaseException(400, "InvalidTrailNameException", "Trail name cannot be blank!");
        }
       
        return CloudTrailAction.updateTrailWithManageEvent(owner, trailName, targetBucket, prefix, manageEvent);
    }

    private static void startLogging(OwnerMeta owner, String input, ManageEventMeta manageEvent) throws IOException, JSONException, BaseException {
        String trailName = null;
        try {
            JSONObject jo = new JSONObject(input);
            trailName = jo.getString("Name");
        } catch (JSONException e) {
            log.error(e.getMessage(), e);
            throw new BaseException(400, "InvalidTrailNameException", "Trail name cannot be blank!");
        }
       
        CloudTrailAction.startLoggingWithManageEvent(owner, trailName, manageEvent);
    }

    private static void stopLogging(OwnerMeta owner, String input, ManageEventMeta manageEvent) throws IOException, JSONException, BaseException {
        String trailName = null;
        try {
            JSONObject jo = new JSONObject(input);
            trailName = jo.getString("Name");
        } catch (JSONException e) {
            log.error(e.getMessage(), e);
            throw new BaseException(400, "InvalidTrailNameException", "Trail name cannot be blank!");
        }
        
        CloudTrailAction.stopLoggingWithManageEvent(owner, trailName, manageEvent);
    }

    private static String lookupEvents(OwnerMeta owner, String input, ManageEventMeta manageEvent) throws JSONException, BaseException, IOException {
        long startTime = -1;
        long endTime = -1;
        int maxResult = 51;
        String nextToken = null;
        Map<String, String> attributes = new HashMap<String, String>();
        try {
            JSONObject jo = new JSONObject(input);
            if (jo.has("StartTime"))
                startTime = jo.getLong("StartTime");
            if (jo.has("EndTime"))
                endTime = jo.getLong("EndTime");
            if (jo.has("MaxResults")) {
                int reqMaxResult = jo.getInt("MaxResults");
                if (reqMaxResult > 50 || reqMaxResult < 1)
                    throw new BaseException(400, "InvalidMaxResultsException",
                            "A max results value of " + reqMaxResult + " is invalid. Specify max results between 1 and 50.");
                maxResult = Math.min(maxResult, reqMaxResult + 1);
            }
            if(jo.has("NextToken"))
                nextToken = jo.getString("NextToken");
            if (jo.has("LookupAttributes")) {
                JSONArray lookupAttributes = jo.getJSONArray("LookupAttributes");
                if(lookupAttributes.length() > 1)
                    throw new BaseException(400, "InvalidLookupAttributesException", "Lookup attribute key can not be multiple!");
                for (int i = 0; i < lookupAttributes.length(); i++) {
                    JSONObject atJo = lookupAttributes.getJSONObject(i);
                    String k = atJo.getString("AttributeKey");
                    String v = atJo.getString("AttributeValue");
                    attributes.put(k, v);
                }
            }
        } catch (JSONException e) {
            log.error(e.getMessage(), e);
            throw new BaseException(400, "InvalidLookupAttributesException", "Lookup Attributes cannot be blank!");
        }
      
        return CloudTrailAction.lookUpEventsWithManageEvent(owner, startTime, endTime, attributes, 1, maxResult, nextToken, manageEvent);      
    }

    /**
     * 判断签名是否是V2签名
     * @return
     */
    private boolean isAuthV2(HttpServletRequest req) {
        String auth = req.getHeader(V4Signer.AUTHORIZATION);
        if (auth == null || auth.length() == 0) {
            return false;
        }
        return auth.toUpperCase().startsWith("AWS ");
    }

    /**
     * 获取请求头中的action
     * @param req
     * @return
     * @throws BaseException 
     */
    private static String getAction(HttpServletRequest req) throws BaseException {
        String s = req.getHeader("X-Amz-Target");
        if (s == null || !s.contains("cn.ctyunapi.oos-cn-cloudtrail.v20131101.CloudTrail_20131101"))
            throw new BaseException(403, "AccessDenied", "Access Denied");
        String[] infos = s.split("\\.");
        return infos[infos.length - 1];
    }

    /**
     * 判断是否属于操作审计的action
     * @param action
     * @throws BaseException 
     */
    private static CloudTrailManageEvent checkAndGetCloudTrailAction(String action) throws BaseException {
        CloudTrailManageEvent[] eventArray = CloudTrailManageEvent.class.getEnumConstants();
        Optional<CloudTrailManageEvent> op = Arrays.stream(eventArray).filter(e -> e.getName().equals(action)).findAny();
        if(!op.isPresent())
            throw new BaseException(405, "MethodNotAllowed", "Method is not allowed.");
        CloudTrailManageEvent event = op.get();
        return event;
    }
    
    private static void writeResponse(HttpServletRequest req, HttpServletResponse resp, String body) {
        resp.setCharacterEncoding(Consts.STR_UTF8);
        Date date = new Date((long) req.getAttribute(Headers.DATE));
        Utils.setCommonHeader(resp, date, (String) req.getAttribute(Headers.REQUEST_ID));
        resp.setHeader(Headers.SERVER, "CTYUN");
        if (body != null) {
            resp.setIntHeader(Headers.CONTENT_LENGTH, body.getBytes(Consts.CS_UTF8).length);
            try {
                resp.getOutputStream().write(body.getBytes(Consts.STR_UTF8));
            } catch (Exception e) {
                log.error("catch write response error");
            }
        }
    }

}

public class CloudTrailServer implements Program {
    static {
        System.setProperty("log4j.log.app", "cloudTrail");
    }
    private static Log log = LogFactory.getLog(CloudTrailServer.class);

    public static void main(String[] args) throws Exception {
        new CloudTrailServer().exec(args);
    }

    @Override
    public String usage() {
        return "Usage: \n";
    }

    @Override
    public void exec(String[] args) throws Exception {
        GetOpt opts = new GetOpt("[p]:[sslp]:", args);
        int sslPort = opts.getInt("sslp", 9458);
        int port = opts.getInt("p", 9095);
        DSyncService dsync = new DSyncService(GlobalHHZConfig.getQuorumServers(), GlobalHHZConfig.getSessionTimeout());
        try {
            DLock lock = dsync.createLock(OosZKNode.cloudTrailServerLock, null);
            lock.lock();
            try {
                int maxThreads = CloudTrailConfig.getMaxThreads();
                QueuedThreadPool pool = new QueuedThreadPool();
                pool.setMaxQueued(maxThreads);
                pool.setMaxThreads(maxThreads);
                SelectChannelConnector connector = new SelectChannelConnector();
                connector.setAcceptQueueSize(OOSConfig.getAcceptQueueSize());// 增加backlog
                connector.setThreadPool(pool);
                connector.setPort(port);
                connector.setMaxIdleTime(OOSConfig.getSocketlIdleTimeout());
                connector.setRequestHeaderSize(Consts.CONNECTOR_REQUEST_HEADER_SIZE);

                QueuedThreadPool sslPool = new QueuedThreadPool();
                sslPool.setMaxQueued(maxThreads);
                sslPool.setMaxThreads(maxThreads);
                /* http://wiki.eclipse.org/Jetty/Tutorial/Embedding_Jetty */
                SslSelectChannelConnector ssl_connector = new SslSelectChannelConnector(); // 只支持https
                ssl_connector.setPort(sslPort);
                ssl_connector.setMaxIdleTime(OOSConfig.getSocketlIdleTimeout());
                ssl_connector.setRequestHeaderSize(Consts.CONNECTOR_REQUEST_HEADER_SIZE);
                ssl_connector.setAcceptQueueSize(OOSConfig.getAcceptQueueSize());// 增加backlog
                ssl_connector.setThreadPool(sslPool);
                SslContextFactory cf = ssl_connector.getSslContextFactory();
                cf.setKeyStorePath(System.getenv("OOS_HOME") + OOSConfig.getOosKeyStore());
                cf.setKeyStorePassword(OOSConfig.getOosSslPasswd());
                Server server = new Server();
                server.setThreadPool(new QueuedThreadPool(maxThreads)); // 增加最大线程数
                server.setConnectors(new Connector[] { connector, ssl_connector });
                server.setHandler(new CloudTrailHandler());
                MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
                MBeanContainer mBeanContainer = new MBeanContainer(mBeanServer);
                server.getContainer().addEventListener(mBeanContainer);
                mBeanContainer.start();
                try {
                    server.start();
                    LogUtils.startSuccess();
                    server.join();
                } catch (Throwable e) {
                    log.error(e.getMessage(), e);
                    System.exit(-1);
                }
            } finally {
                lock.unlock();
            }
        } finally {
            dsync.close();
        }
    }
}
