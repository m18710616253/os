package cn.ctyun.oos.website;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;

import javax.management.MBeanServer;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpEntity;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.BasicHttpEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.tsccm.ThreadSafeClientConnManager;
import org.apache.http.protocol.HTTP;
import org.apache.http.util.EntityUtils;
import org.eclipse.jetty.jmx.MBeanContainer;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.server.nio.SelectChannelConnector;
import org.eclipse.jetty.server.ssl.SslSelectChannelConnector;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.input.SAXBuilder;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.xml.sax.InputSource;

import com.amazonaws.HttpMethod;
import com.amazonaws.auth.SigningAlgorithm;
import com.amazonaws.http.HttpMethodName;
import com.amazonaws.services.s3.Headers;
import com.amazonaws.services.s3.internal.RestUtils;
import com.amazonaws.util.HttpUtils;

import cn.ctyun.common.BaseException;
import cn.ctyun.common.Consts;
import cn.ctyun.common.MultipartInputStream;
import cn.ctyun.common.Program;
import cn.ctyun.common.Session;
import cn.ctyun.common.conf.DataRegion;
import cn.ctyun.common.conf.GlobalHHZConfig;
import cn.ctyun.common.conf.OOSConfig;
import cn.ctyun.common.dsync.DLock;
import cn.ctyun.common.dsync.DSyncService;
import cn.ctyun.common.node.OosZKNode;
import cn.ctyun.common.region.DataRegions;
import cn.ctyun.common.utils.LogUtils;
import cn.ctyun.oos.bssAdapter.server.BssAdapterClient;
import cn.ctyun.oos.bssAdapter.server.SourceTYPE;
import cn.ctyun.oos.common.Email;
import cn.ctyun.oos.common.ErrorMessage;
import cn.ctyun.oos.common.OOSActions;
import cn.ctyun.oos.common.OOSRequest;
import cn.ctyun.oos.common.Parameters;
import cn.ctyun.oos.common.Utils;
import cn.ctyun.oos.common.WebsiteHeader;
import cn.ctyun.oos.hbase.MetaClient;
import cn.ctyun.oos.iam.accesscontroller.AccessController;
import cn.ctyun.oos.iam.accesscontroller.AccessEffect;
import cn.ctyun.oos.iam.accesscontroller.RequestInfo;
import cn.ctyun.oos.iam.accesscontroller.util.ARNUtils;
import cn.ctyun.oos.metadata.AkSkMeta;
import cn.ctyun.oos.metadata.BucketMeta;
import cn.ctyun.oos.metadata.BucketOwnerConsistencyMeta;
import cn.ctyun.oos.metadata.BucketOwnerConsistencyMeta.BucketOwnerConsistencyType;
import cn.ctyun.oos.metadata.CloudTrailEvent;
import cn.ctyun.oos.metadata.CloudTrailManageEvent;
import cn.ctyun.oos.metadata.CloudTrailMeta;
import cn.ctyun.oos.metadata.ManageEventMeta;
import cn.ctyun.oos.metadata.ObjectMeta;
import cn.ctyun.oos.metadata.OwnerMeta;
import cn.ctyun.oos.server.JettyServer;
import cn.ctyun.oos.server.Notify;
import cn.ctyun.oos.server.OpObject;
import cn.ctyun.oos.server.SinglepartInputStream;
import cn.ctyun.oos.server.cloudTrail.CloudTrailAction;
import cn.ctyun.oos.server.conf.BssAdapterConfig;
import cn.ctyun.oos.server.conf.SynchroConfig;
import cn.ctyun.oos.server.count.Pin;
import cn.ctyun.oos.server.count.PinData;
import cn.ctyun.oos.server.globalPortal.OpUser;
import cn.ctyun.oos.server.log.BucketLog;
import cn.ctyun.oos.server.management.Common;
import cn.ctyun.oos.server.signer.SignerUtils;
import cn.ctyun.oos.server.signer.V4Signer;
import cn.ctyun.oos.server.usage.FiveMinuteUsage;
import cn.ctyun.oos.server.usage.UsageResult;
import cn.ctyun.oos.server.util.AccessControlUtils;
import cn.ctyun.oos.server.util.CSVUtils.UsageType;
import cn.ctyun.oos.server.util.Misc;
import common.MimeType;
import common.io.ArrayOutputStream;
import common.io.FileCache;
import common.time.TimeUtils;
import common.tuple.Pair;
import common.tuple.Triple;
import common.util.ConfigUtils;
import common.util.GetOpt;
import common.util.HexUtils;

class HttpHandler extends AbstractHandler {
    private static final Log log = LogFactory.getLog(HttpHandler.class);
    private int port;
    private int sslPort;
    private File webdir;
    private int websitePort;
    private int websiteSslPort;
    private String portalDomain;
    private Session<String, String> session;
    private CompositeConfiguration config = null;
    private Pin pin;
    private String webpages;
    //iam 访问控制
    private AccessController accessController = new AccessController();
    private Pattern subAccountUrlPattern = null;
    private String subAccountUrlRelativePath = null;
    private String subAccountLoginPage = null;
    private int subAccountUrlIndex = -1;
    private long sessionRefreshInterval = -1;
    private HttpClient client = null;
    private int defaultCredit;
    private static MetaClient metaClient = MetaClient.getGlobalClient();
    
    public HttpHandler(CompositeConfiguration config) throws Exception {
        this.port = config.getInt("website.oosPort");
        this.sslPort = config.getInt("website.websiteSslPort");
        this.config = config;
        this.websitePort = config.getInt("website.websitePort");
        this.websiteSslPort = config.getInt("website.websiteSslPort");
        this.portalDomain = config.getString("website.portalDomain");
        this.webpages = config.getString("website.webpages");
        this.webdir = new File(System.getenv("OOS_HOME")).getAbsoluteFile();
        this.session = new Session<String, String>(Consts.MAX_SESSION_TIME, null);
        this.pin = new Pin();
        UDB.config = config;
        client = new DefaultHttpClient(new ThreadSafeClientConnManager());
        this.defaultCredit = config.getInt("website.defaultCredit");
        //for proxy iam subAccount
        this.subAccountUrlRelativePath = config.getString("website.subAccount.relativePath");
        this.subAccountLoginPage = config.getString("website.subAccount.loginPage");
        this.subAccountUrlPattern = Pattern.compile(String.format("^(/%s%s.+)$", this.webpages, this.subAccountUrlRelativePath));
        this.subAccountUrlIndex = webpages.length() + subAccountUrlRelativePath.length() + 1;
        long directIntervalTime = config.getInt("website.globalSessionRefresh.intervalTime", 10) * 60 * 1000;
        float sessionTimeoutRatio = config.getFloat("website.globalSessionRefresh.timeoutRatio", (float) 0.5);
        long sessionTimeoutRatioTime= (long) (Consts.MAX_SESSION_TIME*sessionTimeoutRatio);
        this.sessionRefreshInterval = directIntervalTime < sessionTimeoutRatioTime ? directIntervalTime : sessionRefreshInterval;
    }
    
    @Override
    public void handle(String target, Request basereq, HttpServletRequest request,
            HttpServletResponse response) {
        HttpRequestBase httpReq = null;
        ManageEventMeta manageEvent = new ManageEventMeta();
        try {
            HttpHost httpHost;
            String reqid = Long.toHexString(UUID.randomUUID().getMostSignificantBits());
            response.setHeader(WebsiteHeader.REQUEST_ID, reqid);
            request.setAttribute(WebsiteHeader.REQUEST_ID, reqid);
            request.setAttribute(Headers.DATE, new Date().getTime());
            if (handleWebPages(target, request, response))
                return;
            log(request);
            if (request.getParameter("op") == null
                    && handleWebsiteRequest(basereq, request, response, manageEvent))
                return;
            if (request.getParameter("op") != null && handleRequest(basereq, request, response))
                return;
            if (request.getHeader(WebsiteHeader.ACCESS_KEY) != null) {
                httpReq = buildIAMRequest(request);
                httpHost = new HttpHost(OOSConfig.getIAMDomainSuffix(),
                        config.getInt("website.iamPort"));
            }
            if (request.getHeader(WebsiteHeader.IAM_ACTION) != null) {
                String sessionId = Utils.getSessionId(request);
                if (handleIamOnlyProxy(basereq, request, response, manageEvent)) {
                    return;
                }
                //创建内部接口请求（直接将内部接口结果直接输出到响应）
                InternalIamRequest handInternalApiResult = handleIamInternalApi(basereq, request, sessionId);
                if (handInternalApiResult.newHandle) {
                    httpReq = buildIAMInternalApiRequest(handInternalApiResult.interfacePath, request,
                            handInternalApiResult.requestBody);
                } else {
                    httpReq = buildIAMV1Request(request);
                }
                //子账户当前内部接口与标准api接口都增加mfa登录参数
                if (Portal.isIamUser(session,sessionId)) {
                    processIamMfaLoginRequest(request,httpReq,sessionId);
                }
                httpHost = new HttpHost(OOSConfig.getIAMDomainSuffix(), config.getInt("website.iamPort"));
            } else if(request.getHeader(WebsiteHeader.BSS_PACKAGE) != null) {
                httpReq = buildPackageRequest(request, Utils.getSessionId(request));
                httpHost = new HttpHost(config.getString("portal.host"), config.getInt("portal.port"));
            } else {
                //创建OOS-API接口请求
                httpReq = buildRequest(request);
                httpReq.addHeader("Host", OOSConfig.getDomainSuffix()[0]);
                InetAddress ip = InetAddress.getByName(getAPIHostFromOwnerDataRegion(request));
                httpHost = new HttpHost(ip, port);
                log.info("proxy api HttpHost info:"+httpHost);
            }
            preProcess(httpReq);
            log.info("HttpClient.execute:" + httpHost);
            HttpResponse newResponse = client.execute(httpHost, httpReq);
            log.info("HttpClient.execute response:" + httpHost);
            Portal.refreshMFAStatus(session, request, newResponse.getStatusLine().getStatusCode());
            setResponse(response, newResponse);
        } catch (BaseException e) {
            log.error(e.getMessage(), e);
            response.setStatus(e.status);
            e.resource = request.getRequestURI();
            if(manageEvent.getEvent().eventName != null && manageEvent.getEvent().eventName.length() > 0) {
                manageEvent.getEvent().errorStatus = e.status;
                manageEvent.getEvent().errorCode = e.code;
                manageEvent.getEvent().errorMessage = e.message;
            }
            try {
                if (request.getHeader("Accept").equals("application/json")) {
                    response.getWriter().write(e.toJsonString());
                } else {
                response.getWriter().write(e.toXmlWriter().toString());
                }
            } catch (Exception e1) {
                log.error(e1.getMessage(), e1);
            }
        } catch (Throwable e) {
            log.error(e.getMessage(), e);
            BaseException be = new BaseException();
            be.status = 500;
            be.reqId = (String) request.getAttribute(Headers.REQUEST_ID);
            be.resource = request.getRequestURI();
            be.message = "Internal Error";
            be.code = "InternalError";
            response.setStatus(be.status);
            if(manageEvent.getEvent().eventName != null && manageEvent.getEvent().eventName.length() > 0) {
                manageEvent.getEvent().errorStatus = be.status;
                manageEvent.getEvent().errorCode = be.code;
                manageEvent.getEvent().errorMessage = be.message;
            }
            try {
                if (request.getHeader("Accept").equals("application/json")) {
                    response.getWriter().write(be.toJsonString());
                } else {
                response.getWriter().write(be.toXmlWriter().toString());
                }
            } catch (Exception e1) {
                log.error(e1.getMessage(), e1);
            }
        } finally {
            basereq.setHandled(true);
            if (httpReq != null)
                httpReq.abort();
            // 当manageEvent不为空时，表示需要记录用户的管理事件
            if(manageEvent.getEvent().eventName != null && manageEvent.getEvent().eventName.length() > 0)
                try {
                    metaClient.manageEventInsert(manageEvent);
                } catch (Exception e) {
                    log.error("cloudTrailServer error: insert manageEvent error!" + manageEvent.getRowKey());
                }
        }
    }
    
    private void preProcess(HttpRequest request) {
        if (request == null) {
            throw new IllegalArgumentException("HTTP request may not be null");
        }
        if (request instanceof HttpEntityEnclosingRequest) {
            if (request.containsHeader(HTTP.TRANSFER_ENCODING)) {
                request.removeHeaders(HTTP.TRANSFER_ENCODING);
            }
            if (request.containsHeader(HTTP.CONTENT_LEN)) {
                request.removeHeaders(HTTP.CONTENT_LEN);
            }
        }
    }
    /**
     * 该方法应该返回上次验证mfa code的时间间隔(注意:并非是验证时间点)
     */
    private String getIamMultiFactorAuthAge(String sessionId) {
        long interval = System.currentTimeMillis() - Long
                .parseLong(session.getAndUpdate(sessionId + "|iam|" + WebsiteHeader.IAM_MULTI_FACTOR_AUTH_AGE));
        return Long.toString(interval / 1000);
    }
    private void processIamMfaLoginRequest(HttpServletRequest request, HttpRequest httpReq, String sessionId)
            throws Exception {
        if (request == null) {
            throw new IllegalArgumentException("HTTP request may not be null");
        }
        if (Portal.isIamMultiFactorAuthPresent(session, sessionId)) {
            httpReq.setHeader(WebsiteHeader.IAM_MULTI_FACTOR_AUTH_PRESENT, "true");
            httpReq.setHeader(WebsiteHeader.IAM_MULTI_FACTOR_AUTH_AGE, getIamMultiFactorAuthAge(sessionId));
        } else {
            httpReq.setHeader(WebsiteHeader.IAM_MULTI_FACTOR_AUTH_PRESENT, "false");
            httpReq.setHeader(WebsiteHeader.IAM_MULTI_FACTOR_AUTH_AGE, "");
        }
    }
    private boolean handleWebPages(String target, HttpServletRequest request,
            HttpServletResponse response) throws IOException, BaseException {
        String protocal = request.getHeader("X-Forwarded-Proto");
        do {
            String sessionId = Utils.getSessionId(request);
            if (sessionId != null)
                break;
            String uri = request.getRequestURI();
            if (uri == null || !uri.equals("/"))
                break;
            if (request.getParameterNames().hasMoreElements())
                break;
            String loginHeader = request.getHeader("Access-Control-Request-Headers");
            if (loginHeader != null && loginHeader.equals(WebsiteHeader.LOGIN_USER))
                break;
            String newPassword = request.getHeader(WebsiteHeader.NEW_PASSWORD);
            if (newPassword != null)
                break;
            String redirectURL = ((protocal != null && protocal.trim().length() > 0) ? protocal
                    : request.getRequestURL().toString().split("://")[0]) + "://" + portalDomain
                            + "/" + webpages + "/product/introduce.html";
            // String redirectURL = config.getString("portal.indexPage");
            response.sendRedirect(redirectURL);
            return true;
        } while (false);
        if (!(target.startsWith("/" + webpages + "/") || target.startsWith("/crossdomain.xml")
                || target.equalsIgnoreCase("/favicon.icon") || target
                    .equalsIgnoreCase("/favicon.ico")) || request.getParameter("sessionId") != null)
            return false;
        if (!request.getMethod().toString().equals("GET"))
            if (request.getHeader("Referer") == null
                    || request.getHeader("Referer").startsWith("http://" + portalDomain)
                    || request.getHeader("Referer").startsWith("https://" + portalDomain))
                return false;
        
        //如果portalDomain后面跟的是 ch/v1/v2 则重定向到 ctyun
        String redirectURL = target;
        int dirLength = 0;
        if (target.startsWith("/" + webpages + "/padmin")) {
            dirLength = 6;
        } else if ( target.startsWith("/" + webpages + "/ch") ||
                    target.startsWith("/" + webpages + "/v1") ||
                    target.startsWith("/" + webpages + "/v2") ){
            dirLength = 2;
        }
        
        if (dirLength == 6 || dirLength == 2) {
            String baseAddr = ((protocal != null && protocal.trim().length() > 0) ? protocal
                    : request.getRequestURL().toString().split("://")[0]) + "://" + portalDomain
                            + "/" + webpages +"/";
            redirectURL = baseAddr + "ctyun" + 
                    target.substring(webpages.length()+dirLength+2);
            
            File redirectFile = new File(webdir, redirectURL); 
            
            if (redirectFile.isDirectory()) return true;
            
            if (!isLogin(request)) {
                redirectURL = redirectURL.replace(
                        target.substring(target.lastIndexOf("/")+1), "login.html");
            } else {
                if (!redirectFile.exists()) {
                    redirectURL = redirectURL.replace(
                            target.substring(target.lastIndexOf("/")+1), "consoleOverview.html");
                } 
            }
            response.sendRedirect(redirectURL);
        }  
        
        if (subAccountUrlPattern.matcher(target).matches()) {
            String accountId = target.substring(subAccountUrlIndex);
            response.setHeader("accountid", accountId);
            target = webpages + subAccountLoginPage;
        }
        File file = new File(webdir, target);
        if (file.isDirectory())
            return true;
        FileCache fc = FileCache.THIS;
        FileCache.FileAttr fa = fc.get(file, false);
        if (fa == null)
            return true;
        String mimeType = MimeType.getMimeType(file.getPath());
        ArrayOutputStream data = fa.data;
        response.setContentType(mimeType);
        response.setStatus(200);
        response.getOutputStream().write(data.data(), 0, data.size());
        return true;
    }
    
    private boolean isLogin(HttpServletRequest request) 
            throws IOException {
        Cookie[] cookies = request.getCookies();
        boolean isLogin = false;
        for (Cookie c : cookies)
            if (c.getName().equals(WebsiteHeader.LOGIN_SESSIONID)) {
                if (Portal.isValidSession(c.getValue(), session, "")) {
                    isLogin = true;
                    break;
                }
            }
        return isLogin;
    }
    
    private void checkYunPortal(HttpServletRequest req) throws BaseException {
        String yunPortalPassword = req.getParameter(Parameters.YUN_PORTAL_PASSWORD);
        if (yunPortalPassword == null
                || !yunPortalPassword.equals(config.getString("yunPortal.yunPortalPassword")))
            throw new BaseException(400, "Invalid YunPortalPassword");
        String ip = Utils.getIpAddr(req);
        log.info("yun portal ip is:" + ip);
        String[] ips = config.getStringArray("yunPortal.yunPortalIP");
        for (String i : ips)
            if (ip.equals(i))
                return;
        throw new BaseException(400, "Invalid YunPortalIp");
    }
    
    private void checkAdapter(HttpServletRequest req) throws BaseException {
        String ip = Utils.getIpAddr(req);
        log.info("portal ip is:" + ip);
        String[] ips = config.getStringArray("portal.adapterIp");
        for (String i : ips)
            if (ip.equals(i))
                return;
        throw new BaseException(400, "Invalid PortalIp");
    }
    
    private void checkAccountStatistcsSummaryAllow(HttpServletRequest req) throws Exception {
        String sessionId = Utils.getSessionId(req);
        if (Portal.isIamUser(session, sessionId)) {
            Portal.IamAccount iamAccount = Portal.getIamAccount(session, sessionId);
            AkSkMeta accessKey = new AkSkMeta(iamAccount.accessKeyId);
            metaClient.akskSelect(accessKey);
            String resource = "arn:ctyun:statistics::" + iamAccount.accountId + ":*";
            RequestInfo requestInfo = new RequestInfo("statistics:GetAccountStatistcsSummary", resource,
                    iamAccount.accountId, accessKey.userId, accessKey.userName, req);
            AccessEffect accessEffect = accessController.allow(requestInfo);
            if (accessEffect == AccessEffect.Deny || accessEffect == AccessEffect.ImplicitDeny) {
                throw new BaseException(403, "AccessDenied", requestInfo.getErrorMessage());
            }
        }
    }
    /**
     * 检查子账户关于PutObject的IAM权限
     * @param bucketName
     * @param objectName
     */
    private void checkAccountPutObjectAllow(HttpServletRequest req, BucketMeta bucket, String objectName)
            throws Exception {
        String sessionId = Utils.getSessionId(req);
        if (Portal.isIamUser(session, sessionId)) {
            Portal.IamAccount iamAccount = Portal.getIamAccount(session, sessionId);
            AkSkMeta accessKey = new AkSkMeta(iamAccount.accessKeyId);
            metaClient.akskSelect(accessKey);
            String resource = "arn:ctyun:oos::" + iamAccount.accountId + ":" + bucket.name + "/" + objectName;
            System.out.println("checkAccountPutObjectAllowTest[resource]:" + resource);
            RequestInfo requestInfo = new RequestInfo("oos:PutObject", resource, iamAccount.accountId, accessKey.userId,
                    accessKey.userName, req);
            AccessEffect accessEffect = accessController.allow(requestInfo);
            if (accessEffect == AccessEffect.Deny) {
                throw new BaseException(403, "AccessDenied", requestInfo.getErrorMessage());
            }
            // 如果是隐式拒绝且，bucket不是公有读写，拒绝访问
            if (accessEffect == AccessEffect.ImplicitDeny && bucket.permission != BucketMeta.PERM_PUBLIC_READ_WRITE) {
                throw new BaseException(403, "AccessDenied", requestInfo.getErrorMessage());
            }
        }
    }
    
    /**
     * 判断用户是有拥有移动对象的权限
     * @param req
     * @param sourceBucket 源bucket
     * @param sourceObjectName 源对象
     * @param destBucket 目标bucket
     * @param destObjectName 目标对象
     * @throws Exception
     */
    private void checkMoveObjectAllow(HttpServletRequest req, BucketMeta sourceBucket, String sourceObjectName,
            BucketMeta destBucket, String destObjectName) throws Exception {

        String sessionId = Utils.getSessionId(req);
        // 根用户不做限制
        if (!Portal.isIamUser(session, sessionId)) {
            return;
        }
        // 获取子用户信息
        Portal.IamAccount iamAccount = Portal.getIamAccount(session, sessionId);
        // 判断源对象的GET权限
        AccessControlUtils.checkObjectAllow(req, sourceBucket, sourceObjectName, OOSActions.Object_Get,
                HttpMethod.GET.toString(), iamAccount.accessKeyId);
        // 判断源对象的DELETE权限
        AccessControlUtils.checkObjectAllow(req, sourceBucket, sourceObjectName, OOSActions.Object_Delete,
                HttpMethod.DELETE.toString(), iamAccount.accessKeyId);
        // 判断目的对象的PUT权限
        AccessControlUtils.checkObjectAllow(req, destBucket, destObjectName, OOSActions.Object_Put,
                HttpMethod.PUT.toString(), iamAccount.accessKeyId);
    }

    /**
     * 检查用户iam权限的 cloudtrail权限
     * @param req
     * @param action
     * @throws Exception
     */
    private void checkAccountCloudTrailSummaryAllow(HttpServletRequest req, String action, CloudTrailMeta meta) throws Exception {
        String sessionId = Utils.getSessionId(req);
        if (Portal.isIamUser(session, sessionId)) {
            Portal.IamAccount iamAccount = Portal.getIamAccount(session, sessionId);
            AkSkMeta accessKey = new AkSkMeta(iamAccount.accessKeyId);
            metaClient.akskSelect(accessKey);
            RequestInfo requestInfo = new RequestInfo("cloudtrail:" + action, meta.getARN(),
                    iamAccount.accountId, accessKey.userId, accessKey.userName, req);
            AccessEffect accessEffect = accessController.allow(requestInfo);
            if (accessEffect == AccessEffect.Deny || accessEffect == AccessEffect.ImplicitDeny) {
                throw new BaseException(403, "AccessDenied", requestInfo.getErrorMessage());
            }
        }
    }
    

    boolean handleRequest(Request basereq, HttpServletRequest req, HttpServletResponse resp)
            throws Exception {
        // register user from yunPortal
        if (req.getParameter("op").equals(WebsiteHeader.ADD_USER)) {
            checkYunPortal(req);
            Email email = new Email(config);
            Email emailInner = new Email(config);
            setContentLength(resp, Portal.register(req, resp, null, email,
                    email.getRegisterBodyFromYunPortal(), Consts.REGISTER_FROM_YUNPORTAL,
                    emailInner, defaultCredit, config.getBoolean("website.isPublicCloud")));
            basereq.setHandled(true);
            return true;
        }
        
        // register from global portal,BSS系统工单注册oos用户入口
        /* register from global portal,BSS系统工单注册oos用户入口
         * 接入5.0大环境后，6.0资源池作为一个单独的池子应该不会使用到此接口，但是为了以防万一将6.0池子作为中心池使用
         * 特此增加了此部分内容；
         */
        if (req.getParameter("op").equals(WebsiteHeader.REGISTER_UESR_FROM_PORTAL)) {
            String sourceType = req.getParameter(Parameters.REGISTER_SOURCE_TYPE);
            
            if (sourceType == null) {
                OwnerMeta bssUser = createOwnerFromBSS(req);  
                
                //验证本地资源池用户是否已经存在
                OwnerMeta oosUser = new OwnerMeta(bssUser.email);
                if (metaClient.ownerSelect(oosUser)) {
                    throw new BaseException(400, "UserExistsInOOS");
                }
                //验证5.0全局用户是否已经存在
                BucketOwnerConsistencyMeta bucketOwnerConsistency = new BucketOwnerConsistencyMeta(
                        bssUser.email, BucketOwnerConsistencyType.OWNER);
                if (metaClient.bucketOwnerConsistencySelect(bucketOwnerConsistency)){
                    throw new BaseException(400, "UserExistsInOOS");
                }
                
                checkAdapter(req);
                //发送注册请求到中心节点进行注册；
                String accountId = req.getParameter("accountId");
                String userId = req.getParameter("userId");
                String orderId = req.getParameter("orderId");
                
                if (!BssAdapterClient.registerUserFromProxy(bssUser.name, BssAdapterConfig.localPool.ak , 
                        BssAdapterConfig.localPool.getSK(),bssUser,SourceTYPE.BSS,accountId,userId,orderId,
                        new String[]{BssAdapterConfig.localPool.name})) {
                    throw new BaseException(500, "RegisterUserFail");
                } else {
                    if (!BssAdapterConfig.localPool.name.equals(BssAdapterConfig.name)) {
                        OpUser.registerUser(req.getParameter("accountId"), req.getParameter("userId"),
                            req.getParameter("orderId"),bssUser);
                    }
                }
            } else if (SourceTYPE.valueOf(sourceType) == SourceTYPE.ADAPTERSERVER) {
                OwnerMeta bssUser = new OwnerMeta();
                InputStream is = req.getInputStream();
                if (is != null) {
                    bssUser.readFields(IOUtils.toByteArray(is));
                }
                bssUser.currentAKNum = 0;
                OpUser.registerUser(req.getParameter("accountId"), req.getParameter("userId"),
                        req.getParameter("orderId"),bssUser);
            }
            
            basereq.setHandled(true);
            return true;
        }
        return false;
    }
    /**
     * 截取仅在proxy端执行的iam接口
     */
    private boolean handleIamOnlyProxy(Request basereq, HttpServletRequest req, HttpServletResponse resp, ManageEventMeta manageEvent)
            throws Exception {
        if (req.getHeader(WebsiteHeader.IAM_ACCOUNTID) != null) {
            OwnerMeta owner = Portal.getOwnerFromSession(session, Utils.getSessionId(req));
            String accountId = owner.getAccountId();
            resp.setHeader("accountid", accountId);
            return true;
        }
        if (req.getHeader(WebsiteHeader.IAM_LOGININFO) != null) {
            Portal.getLastLoginInfo(req, resp, session);
            basereq.setHandled(true);
            return true;
        }
        if (req.getHeader(WebsiteHeader.IAM_CHECK_MFACODE) != null) {
            OwnerMeta owner = Portal.getOwnerFromSession(session, Utils.getSessionId(req));
            boolean success = true;
            try {
                Portal.checkMFACode(req, resp, session, config);
            } catch (Exception e) {
                success = false;
                throw e;
            } finally {
                manageCloudTrailEvent(owner, manageEvent, CloudTrailManageEvent.Consle_Check_MFA, "ConsoleSignin", null, null,
                        req, Utils.getSessionId(req));
                JSONObject jo = new JSONObject();
                if (success)
                    jo.put("checkMfa", "Success");
                else
                    jo.put("checkMfa", "Failure");
                manageEvent.getEvent().responseElements = jo.toString();
            }
            basereq.setHandled(true);
            return true;
        }
        return false;
    }
    public static class InternalIamRequest {
        public boolean newHandle = false;
        public String interfacePath;
        public String requestBody;
    }
    /**
     * 截取iam内部API接口
     */
    private InternalIamRequest handleIamInternalApi(Request basereq, HttpServletRequest req, String sessionId)
            throws Exception {
        InternalIamRequest result = new InternalIamRequest();
//        //iam内部接口例子
        if (req.getHeader("testMfa") != null) {
            result.newHandle = true;
            result.interfacePath = "/internal/testMfa";
//
            result.requestBody = Portal.buildIamCheckMFACodeBody(req, session, sessionId);
//            result.first(true);
//            result.second(requestBody);
            return result;
        }
        return result;
    }
    private OwnerMeta createOwnerFromBSS(HttpServletRequest req) 
            throws IOException, BaseException {
        String accountId = req.getParameter("accountId");
        if (accountId == null || accountId.trim().length() == 0)
            throw new BaseException(400, "AccountIdNotExists");
        
        OwnerMeta bssUser = OpUser.getOwnerInfo(accountId);
        if (bssUser == null)
            throw new BaseException(400, "CanNotGetUserInfo");
        //BSS对接用户信用默认最大,取消政企用户和普通用户的区分;
        bssUser.credit = -Long.MAX_VALUE;
        bssUser.maxAKNum = Consts.MAX_AK_NUM;
        bssUser.setPwd(UUID.randomUUID().toString());
        return bssUser;
    }
    
    private boolean handleWebsiteRequest(Request basereq, HttpServletRequest request,
            HttpServletResponse response, ManageEventMeta manageEvent) throws Exception {
        //  is login
        if (request.getHeader(WebsiteHeader.ISLOGIN) != null) {
            String sessionId = Utils.getSessionId(request);
            if (!Portal.isValidSession(sessionId, session, "")) {
                throw new BaseException(403, "NotLogin");
            }
            return true;
        }
        // udb
        if (request.getRequestURI().equals(config.getString("udb.udbLoginRedirect"))) {
            UDB.login(basereq, request, response, session);
            return true;
        }
        // logout from udb
        if (request.getRequestURI().equals("/logout")) {
            UDB.logoutFromUDB(request, response);
            return true;
        }
        // register user
        if (request.getHeader(WebsiteHeader.ADD_USER) != null) {
            String sourceType = request.getParameter(Parameters.REGISTER_SOURCE_TYPE);
            if (!Arrays.toString(SourceTYPE.values()).contains(sourceType)) {
                throw new BaseException(405, "InvalidSourceType");
            }
            
            if (SourceTYPE.valueOf(sourceType) == SourceTYPE.PROXY) {
              //修改了注册用户流程，所以先行校验验证码；
                String verifyCode = request.getParameter(Parameters.VERIFY_CODE);
                if (verifyCode == null
                        || !verifyCode.equalsIgnoreCase(session.getAndUpdate(request
                                .getParameter(Parameters.IMAGE_SESSIONID))))
                    throw new BaseException(400, "InvalidVerifyCode");
                OwnerMeta user = Portal.createOwnerFromRequest(request, Consts.REGISTER_FROM_OOS,defaultCredit);
                
                //发送注册请求到中心节点进行注册；
                if (!BssAdapterClient.registerUserFromProxy(user.name, BssAdapterConfig.localPool.ak , 
                        BssAdapterConfig.localPool.getSK(),user,SourceTYPE.valueOf(sourceType),null,
                        null,null,new String[]{BssAdapterConfig.localPool.name})) {
                    throw new BaseException(500, "RegisterUserFail");
                } else {
                    //如果发起注册的不是中心资源池
                    if (!BssAdapterConfig.localPool.name.equals(BssAdapterConfig.name)) {
                        Email email = new Email(config);
                        Email emailInner = new Email(config);
                        Portal.register(request, response, session, email, email.getRegisterBody(),
                                Consts.REGISTER_FROM_OOS, emailInner, defaultCredit,config);
                    } else {
                        //中心资源池发送注册邮件
                        Portal.sendUserRegistMail(request, response, Consts.REGISTER_FROM_OOS, 
                                user.name, config);
                    }
                }
                
                response.setHeader(WebsiteHeader.ADD_USER, "register success");
                log.info("user: " + user.name + " register success");
            } else if (SourceTYPE.valueOf(sourceType) == SourceTYPE.ADAPTERSERVER) {
              //由中心节点转发过来的异步请求,不发送邮件；
                Portal.register(request, response, session, null, null,
                        Consts.REGISTER_FROM_OOS, null, defaultCredit,config);
            }
            
            basereq.setHandled(true);
            return true;
        }
        // logout from protal
        if (request.getRequestURI().equals("/logoutRequest")) {
            Portal.logoutFromPortal(request, response, session);
            basereq.setHandled(true);
            return true;
        }
        
        // register user from udb
        if (request.getHeader(WebsiteHeader.ADD_USER_FROM_UDB) != null) {
            Email email = new Email(config);
            Email emailInner = new Email(config);
            Portal.register(request, response, session, email, email.getRegisterBodyFromUDB(),
                    Consts.REGISTER_FROM_UDB, emailInner, defaultCredit,
                    config.getBoolean("website.isPublicCloud"));
            basereq.setHandled(true);
            return true;
        }
        // user login
        if (request.getHeader(WebsiteHeader.LOGIN_USER) != null
                || (request.getHeader("Access-Control-Request-Headers") != null && request
                        .getHeader("Access-Control-Request-Headers").contains(
                                WebsiteHeader.LOGIN_USER))
                || request.getParameter(WebsiteHeader.LOGIN_USER) != null) {
            String logFlag = request.getHeader(WebsiteHeader.LOGIN_USER_GLOBAL);
            if (logFlag != null) {
                // 从bss广播来的登陆/登出
                if (logFlag.equalsIgnoreCase("logout")) {
                    // 从bss登出
                    String sessionId = request.getParameter(WebsiteHeader.LOGIN_SESSIONID);
                    OwnerMeta owner = Portal.getOwnerFromSession(session, sessionId);
                    boolean success = true;
                    try {
                        Portal.logoutFromBssAdapter(request, response, session);
                    } catch (Exception e) {
                        success = false;
                        throw e;
                    } finally {
                        // 记录管理事件
                        manageCloudTrailEvent(owner, manageEvent, CloudTrailManageEvent.Console_Logout, "ConsoleSignin", null,
                                null, request, sessionId);
                        JSONObject jo = new JSONObject();
                        if (success)
                            jo.put("logoutUser", "Success");
                        else
                            jo.put("logoutUser", "Failure");
                        manageEvent.getEvent().responseElements = jo.toString();
                    }
                } else if (logFlag.equalsIgnoreCase("login")) {
                    // 从bss登陆
                    OwnerMeta owner = new OwnerMeta();
                    boolean success = true;
                    String loginURL = request.getRequestURL().toString();
                    try {
                        Portal.loginFromBssAdapter(request, response, session, owner);
                    } catch (Exception e) {
                        success = false;
                        throw e;
                    } finally {
                        // 记录管理事件
                        String sessionId = response.getHeader(WebsiteHeader.LOGIN_SESSIONID);
                        manageCloudTrailEvent(owner, manageEvent, CloudTrailManageEvent.Console_Login, "ConsoleSignin", null,
                                null, request, sessionId);
                        JSONObject jo = new JSONObject();
                        if (success)
                            jo.put("consoleLogin", "Success");
                        else
                            jo.put("consoleLogin", "Failure");
                        manageEvent.getEvent().responseElements = jo.toString();
                        JSONObject additionalJo = new JSONObject();
                        additionalJo.put("MFAUsed", false);
                        additionalJo.put("loginTo", loginURL);
                        manageEvent.getEvent().additionalEventData = additionalJo.toString();
                    }
                }
            } else {
                // 普通登陆
                boolean success = true;
                String loginURL = request.getRequestURL().toString();
                OwnerMeta owner = new OwnerMeta();
                try {
                    Portal.login(request, response, session, owner);
                } catch (Exception e) {
                    success = false;
                    throw e;
                } finally {
                    // 记录管理事件
                    String sessionId = response.getHeader(WebsiteHeader.LOGIN_SESSIONID);
                    manageCloudTrailEvent(owner, manageEvent, CloudTrailManageEvent.Console_Login, "ConsoleSignin", null, null,
                            request, sessionId);
                    JSONObject jo = new JSONObject();
                    if (success)
                        jo.put("consoleLogin", "Success");
                    else
                        jo.put("consoleLogin", "Failure");
                    manageEvent.getEvent().responseElements = jo.toString();
                    JSONObject additionalJo = new JSONObject();
                    additionalJo.put("MFAUsed", false);
                    additionalJo.put("loginTo", loginURL);
                    manageEvent.getEvent().additionalEventData = additionalJo.toString();
                }
            }
            basereq.setHandled(true);
            return true;
        }
        if (request.getHeader(WebsiteHeader.LOGIN_IAM_USER) != null || (
                request.getHeader("Access-Control-Request-Headers") != null && request
                        .getHeader("Access-Control-Request-Headers").contains(WebsiteHeader.LOGIN_IAM_USER))
                || request.getParameter(WebsiteHeader.LOGIN_IAM_USER) != null) {

            OwnerMeta owner = new OwnerMeta();
            boolean success = true;
            String mfaCode = request.getParameter(Parameters.MFA_CODE);
            String loginURL = request.getRequestURL().toString();
            String respString = null;
            try {
                respString = Portal.loginIam(request, response, session, config, owner);
            } catch (Exception e) {
                success = false;
                throw e;
            } finally {
                // 记录管理事件
                String sessionId = response.getHeader(WebsiteHeader.LOGIN_SESSIONID);
                boolean needRecord = true;
                JSONObject json = null;
                try {
                    json = new JSONObject(respString);
                    if(json.has("mFACodeRequired") && json.getBoolean("mFACodeRequired")) {
                        needRecord = false;
                    }
                    if(response.getStatus() != 200 && sessionId == null && json != null) {                    
                        manageEvent.getEvent().errorStatus = json.getInt("status");
                        manageEvent.getEvent().errorCode = json.getString("code");
                        manageEvent.getEvent().errorMessage = json.getString("message");
                        success = false;
                    }
                } catch (Exception e2) {
                }                                
                //只有返回的有这个值，且这个值为true表示是需要mfa验证的第一次请求返回，此时不需要记录管理事件
                if (needRecord) {
                    manageCloudTrailEvent(owner, manageEvent, CloudTrailManageEvent.Console_Login, "ConsoleSignin", null, null,
                            request, sessionId);                    
                    JSONObject jo = new JSONObject();
                    if (success)
                        jo.put("consoleLogin", "Success");
                    else
                        jo.put("consoleLogin", "Failure");
                    manageEvent.getEvent().responseElements = jo.toString();
                    JSONObject additionalJo = new JSONObject();
                    if(!Portal.empty(mfaCode))
                        additionalJo.put("MFAUsed", true);
                    else
                        additionalJo.put("MFAUsed", false);
                    additionalJo.put("loginTo", loginURL);
                    manageEvent.getEvent().additionalEventData = additionalJo.toString();
                }                
            }
            basereq.setHandled(true);
            return true;
        }
        // cas ticket check
        if (request.getHeader(WebsiteHeader.CAS_TICKET_CHECK) != null) {
            
            @SuppressWarnings("unchecked")
            Pair<String, Object> result = Portal.checkTicket(request, response,config,webpages);
            
            if (result == null){
                throw new BaseException(403, "casLoginFailure");
            }
            if ("success".equals(result.first())){ 
                @SuppressWarnings("unchecked")
                Triple<String,String,String> t = (Triple<String, String, String>)result.second();
                Portal.loginFromGlobalPortalCAS(request, response,t,config,session);
            } else {
                String casMsg = (String) result.second();
                throw new BaseException(403, "casLoginFailure",casMsg);
            }
            
            basereq.setHandled(true);
            return true;
        }
        // user logout
        if (request.getHeader(WebsiteHeader.LOGOUT_USER) != null) {
            String sessionId = Utils.getSessionId(request);
            OwnerMeta owner = Portal.getOwnerFromSession(session, sessionId);
            boolean success = true;
            try {
                Portal.logout(request, response, session);
            } catch (Exception e) {
                success = false;
                throw e;
            } finally {
                // 记录管理事件
                manageCloudTrailEvent(owner, manageEvent, CloudTrailManageEvent.Console_Logout, "ConsoleSignin", null, null,
                        request, sessionId);
                JSONObject jo = new JSONObject();
                if (success)
                    jo.put("logoutUser", "Success");
                else
                    jo.put("logoutUser", "Failure");
                manageEvent.getEvent().responseElements = jo.toString();
            }
            basereq.setHandled(true);
            return true;
        }
        if (request.getHeader(WebsiteHeader.LOGOUT_IAM_USER) != null) {
            String sessionId = Utils.getSessionId(request);
            OwnerMeta owner = Portal.getOwnerFromSession(session, sessionId);
            boolean success = true;
            String sessionIam = session.get(sessionId + "|iam");
            try {
                Portal.logoutIam(request, response, session);
            } catch (Exception e) {
                success = false;
                throw e;
            } finally {
                // 记录管理事件
                manageCloudTrailEvent(owner, manageEvent, CloudTrailManageEvent.Console_Logout, "ConsoleSignin", null, null,
                        request, "iamUser", sessionIam);
                JSONObject jo = new JSONObject();
                if (success)
                    jo.put("logoutUser", "Success");
                else
                    jo.put("logoutUser", "Failure");
                manageEvent.getEvent().responseElements = jo.toString();
            }

            basereq.setHandled(true);
            return true;
        }
        // user permission
        if (request.getParameter(Parameters.PERMISSION) != null) {
            setContentLength(response,Portal.getUserPermission(request, response, session));
            basereq.setHandled(true);
            return true;
        }
        // verify email
        if (request.getParameter(Parameters.EMAIL) != null) {
            Portal.verifyEmail(request, response);
            basereq.setHandled(true);
            return true;
        }
        // verify register
        if (request.getParameter(Parameters.VERIFY_REGISTER) != null) {
            Portal.verifyRegister(request, response, webpages);
            return true;
        }
        // send verify email again
        if (request.getHeader(Parameters.SEND_VERIFY_EMAIL_AGAIN) != null) {
            Portal.sendEmailAgain(request, response, new Email(config));
            return true;
        }
        // get verify image
        if (request.getParameter(Parameters.GET_VERIFY_IMAGE) != null) {
            String sessionId = request.getParameter(Parameters.IMAGE_SESSIONID);
            Portal.generateImage(response, sessionId, session, webpages);
            basereq.setHandled(true);
            return true;
        }
        // get image sessionId
        if (request.getHeader(WebsiteHeader.VERIFY_IMAGE) != null) {
            String sessionId = UUID.randomUUID().toString();
            session.remove(sessionId);
            response.setHeader(Parameters.IMAGE_SESSIONID, sessionId);
            basereq.setHandled(true);
            return true;
        }
        // find password
        if (request.getParameter(Parameters.FIND_PASSWORD) != null) {
            Portal.findPassword(request, response, session, new Email(config), webpages,
                    config.getBoolean("website.isPublicCloud"));
            basereq.setHandled(true);
            return true;
        }
        // update password
        if (request.getHeader(WebsiteHeader.NEW_PASSWORD) != null) {
            Portal.updatePassword(request, response, session);
            basereq.setHandled(true);
            return true;
        }
        if (request.getParameter(WebsiteHeader.IAM_DOWNLOAD) != null) {
            Portal.iamDownloadFile(request, response, session);
            basereq.setHandled(true);
            return true;
        }
        // user access
        String sessionId = null;
        if ((sessionId = Utils.getSessionId(request)) != null) {
            String portalSessionId = null;
            Cookie[] cookies = request.getCookies();
            for (Cookie cookie : cookies) {
                if (cookie.getName().equals(WebsiteHeader.GLOBAL_PORTAL_SESSIONID)) {
                    portalSessionId = cookie.getValue();
                }
            }
            if (!Portal.isValidSession(sessionId, session, portalSessionId)) {
                //从中心资源池抓取已经登录的用户session
                BssAdapterClient.getGlobalUserSession(BssAdapterConfig.localPool.ak, BssAdapterConfig.localPool.getSK(),
                        sessionId,session,(code)->{
                            if (code != 200) {
                                throw new BaseException(403, "NotLogin");
                            }
                        });
            }
            else{
                //如果本地session验证成功，则定期刷新中心资源池session状态
                Portal.globalSessionRefresh(sessionId, session, sessionRefreshInterval);
            }
        } else
            throw new BaseException(403, "NotLogin");
        
        // get pools
        if (request.getHeader(WebsiteHeader.GET_USER_POOLS) != null) {
            //iam用户不适用可见资源池功能
            if(Portal.isIamUser(session,Utils.getSessionId(request))){
                response.setHeader("current", StringUtils.defaultIfEmpty(BssAdapterConfig.localPool.name, ""));
                response.setHeader("currentChName", URLEncoder.encode(StringUtils
                                .defaultIfEmpty(BssAdapterConfig.localPool.chName, BssAdapterConfig.localPool.name),
                        Consts.STR_UTF8));
                basereq.setHandled(true);
                return true;
            }
            response.setHeader("current", StringUtils.defaultIfEmpty(BssAdapterConfig.localPool.name, ""));
            response.setHeader("currentChName", URLEncoder.encode(StringUtils
                            .defaultIfEmpty(BssAdapterConfig.localPool.chName, BssAdapterConfig.localPool.name),
                    Consts.STR_UTF8));
            response.setHeader("showRegisterButton",SynchroConfig.propertyLocalPool==null?"false":
                SynchroConfig.propertyLocalPool.showRegisterButton+"");
            response.setHeader("synchroLogin", SynchroConfig.propertyLocalPool==null?"false":
                SynchroConfig.propertyLocalPool.synchroLogin+"");
            
            OwnerMeta owner = Portal.getOwnerFromSession(session, Utils.getSessionId(request));
            if (owner == null) {
                setContentLength(response,Portal.getAvailablePools(request, response, session));    
            } else {
                setContentLength(response,BssAdapterClient.getAvailablePoolsById(owner.getId(), 
                        BssAdapterConfig.localPool.ak, BssAdapterConfig.localPool.getSK()));
            }
            basereq.setHandled(true);
            return true;
        }
        
        // share url
        if (request.getHeader(WebsiteHeader.SHARE_URL) != null) {
            Pair<String, String> akskPair=  getAkSk(request);
            Portal.shareURL(request, response, akskPair, getAPIHostFromOwnerDataRegion(request));
            basereq.setHandled(true);
            return true;
        }
        // get cookie
        if (request.getHeader(WebsiteHeader.COOKIE) != null) {
            Portal.getCookie(request.getHeader(WebsiteHeader.COOKIE), response, session,
                    "oos.ctyun.cn");
            basereq.setHandled(true);
            return true;
        }
        // count
        if (request.getHeader(WebsiteHeader.COUNT) != null) {
            OwnerMeta owner = Portal.getOwnerFromSession(session, Utils.getSessionId(request));
            Portal.getUsage(owner, response);
            return true;
        }
        // monthcount 概览页面用户用量
        if (request.getHeader(WebsiteHeader.MONTHCOUNT) != null) {
            checkAccountStatistcsSummaryAllow(request);
            OwnerMeta owner = Portal.getOwnerFromSession(session, Utils.getSessionId(request));
            Portal.getUsageCurrentMonth(owner, response);
            return true;
        }
        // pay
        if (request.getHeader(WebsiteHeader.PAY) != null) {
            OwnerMeta owner = Portal.getOwnerFromSession(session, Utils.getSessionId(request));
            if (request.getParameter("amount") != null
                    && request.getParameter("amount").trim().length() != 0) {
                Portal.checkAmount(request.getParameter("amount"), 2, true);
                metaClient.ownerSelect(owner);
                if (request.getHeader(WebsiteHeader.PAY).equals("from-bestpay")) {
                    setContentLength(
                            response,
                            Portal.pay(owner.getId(),
                                    Double.parseDouble(request.getParameter("amount")),
                                    Utils.getIpAddr(request), null));
                } else {
                    String orderId = String.valueOf(owner.getId()) + generateOrderId();
                    Portal.payFromGlobalPortal(owner,
                            Double.parseDouble(request.getParameter("amount")),
                            Utils.getIpAddr(request), orderId, false,
                            Portal.getAccountIdFromCookie(request), "Pay");
                }
                basereq.setHandled(true);
                return true;
            } else
                throw new BaseException();
        }
        // status
        if (request.getHeader(WebsiteHeader.STATUS) != null) {
            OwnerMeta owner = Portal.getOwnerFromSession(session, Utils.getSessionId(request));
            setContentLength(response,
                    Portal.getStatus(owner, config.getInt("website.registerPackeageId")));
            basereq.setHandled(true);
            return true;
        }
        // replica mode
        // if (request.getHeader(WebsiteHeader.REPLICAMODE) != null) {
        // setContentLength(response, Portal.getReplicaMode());
        // basereq.setHandled(true);
        // return true;
        // }
        // recharge
        if (request.getHeader(WebsiteHeader.RECHARGE) != null) {
            OwnerMeta owner = Portal.getOwnerFromSession(session, Utils.getSessionId(request));
            String beginDate = request.getParameter("beginDate");
            String endDate = request.getParameter("endDate");
            if (beginDate != null && beginDate.trim().length() != 0 && endDate != null
                    && endDate.trim().length() != 0) {
                setContentLength(response,
                        Portal.getRecharge(owner, beginDate, endDate + " 23:59:59"));
                basereq.setHandled(true);
                return true;
            } else
                throw new BaseException();
        }
        // 使用量、已用带宽、可用带宽、连接数统计值统一查询接口
        if (request.getHeader(WebsiteHeader.USAGE) != null) {
            checkAccountStatistcsSummaryAllow(request);
            RequestParams p = prepareOrder(request);
            String usageStats = request.getParameter("usageStats");
            Utils.checkParameter(usageStats);
            // 如果是可用带宽，freq只能是五分钟
            if ((usageStats.startsWith("avail") && usageStats.endsWith("BW"))
                    || usageStats.toLowerCase().contains("connection")) {
                if (!p.freq.equals(Utils.BY_5_MIN)) {
                    throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_ARGUMENT, "Invalid freq.");
                }
            }
            String regionName = request.getParameter("regionName");
            Utils.checkParameter(regionName);
            // global只返回global數據
            Set<String> dataRegions = null;
            if (regionName.equals(Consts.GLOBAL_DATA_REGION)) {
                dataRegions = new HashSet<>(DataRegions.getAllRegions());
                dataRegions.add(Consts.GLOBAL_DATA_REGION);
            } else {
                dataRegions = new HashSet<>();
                dataRegions.add(regionName);
            }
            String storageType = Utils.checkAndGetStorageType(request.getParameter("storageClass"));
            setContentLength(response, UsageResult.getSingleUsageInJson(p.owner.getId(), p.beginDate, p.endDate, usageStats, p.bucketName, p.freq, dataRegions, storageType, true, false));
            JSONObject reqJo = new JSONObject();
            reqJo.put("beginDate", p.beginDate);
            reqJo.put("endDate", p.endDate);
            reqJo.put("bucket", p.bucketName);
            reqJo.put("region", regionName);
            reqJo.put("freq", p.freq);
            CloudTrailManageEvent event = getUsageManageEventType(usageStats);
            if (event != null) {
                manageCloudTrailEvent(p.owner, manageEvent, event, "ApiCall", "Management", reqJo.toString(), request, sessionId);
            }
            basereq.setHandled(true);
            return true;
        }
        // 统计数据下载，目前仅支持CSV格式
        if (request.getParameter("usageDownloadFormat") != null) {
            RequestParams p = prepareOrder(request);
            String format = request.getParameter("usageDownloadFormat");
            String file = request.getParameter("downloadFileName");
            String type = request.getParameter("downloadType");
            String regionName = request.getParameter("regionName");     
            Utils.checkParameter(regionName);
            Utils.checkParameter(file);
            Utils.checkParameter(type);
            if (type.equals("proxyAvailBandwidth") || type.equals("proxyConnections")) {
                if (!p.freq.equals(Utils.BY_5_MIN)) {
                    throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_ARGUMENT, "Invalid freq.");
                }
            }
            if (!format.equals(Utils.CSV))
                throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_ARGUMENT, "Invalid usageDownloadFormat.");
            UsageType usageType = Utils.checkAndGetUsageStatsTypeForDownload(type);
            Set<String> dataRegions = null;
            if (regionName.equals(Consts.GLOBAL_DATA_REGION)) {
                dataRegions = new HashSet<String>(DataRegions.getAllRegions());
                dataRegions.add(Consts.GLOBAL_DATA_REGION);
            } else {
                dataRegions = new HashSet<String>();
                dataRegions.add(regionName);
            }
            String storageType = Utils.checkAndGetStorageType(request.getParameter("storageClass"));
            setContentLength(response, UsageResult.getSingleUsageInCsv(p.owner.getId(), p.beginDate, p.endDate,
                    p.bucketName, p.freq, usageType, dataRegions, storageType, true, false), format, file);
            basereq.setHandled(true);
            return true;
        }
        
//        // order-detailConsume
//        if (request.getHeader(WebsiteHeader.ORDER_DETAILCONSUME) != null) {
//            RequestParams p = prepareOrder(request);
//            setContentLength(response,
//                    Portal.getOrderDetailConsume(p.ownerId, p.beginDate, p.endDate));
//            basereq.setHandled(true);
//            return true;
//        }
//        // order-consume
//        if (request.getHeader(WebsiteHeader.ORDER_CONSUME) != null) {
//            RequestParams p = prepareOrder(request);
//            setContentLength(response,
//                    Portal.getOrderTotalConsume(p.ownerId, p.beginDate, p.endDate));
//            basereq.setHandled(true);
//            return true;
//        }
//        

        // get object exists
        if (request.getHeader(WebsiteHeader.OBJECT_EXISTS) != null) {
            String bucketName = request.getParameter("bucketName");
            String objectName = URLDecoder.decode(request.getParameter("objectName"),
                    Consts.STR_UTF8);
            if (bucketName != null && bucketName.trim().length() != 0 && objectName != null
                    && objectName.trim().length() != 0) {
                setContentLength(response,
                        (Portal.getObjectExists(bucketName, objectName)?"1":"0"));
                basereq.setHandled(true);
                return true;
            } else
                throw new BaseException();
        }

        if (request.getHeader(WebsiteHeader.CHECK_OBJECT_MOVEALLOW) != null) {
            String sourceBucket = request.getParameter("sourceBucket");
            String sourceObject = URLDecoder.decode(request.getParameter("sourceObject"),
                    Consts.STR_UTF8);
            String destBucket = request.getParameter("destBucket");
            String destObject = URLDecoder.decode(request.getParameter("destObject"),
                    Consts.STR_UTF8);
            BucketMeta sourceBucketMeta = new BucketMeta(sourceBucket);
            BucketMeta destBucketMeta = new BucketMeta(destBucket);
            if (!metaClient.bucketSelect(sourceBucketMeta))
                throw new BaseException(400, ErrorMessage.ERROR_CODE_NO_SUCH_BUCKET);

            if (!metaClient.bucketSelect(destBucketMeta))
                throw new BaseException(400, ErrorMessage.ERROR_CODE_NO_SUCH_BUCKET);

            checkMoveObjectAllow(request,sourceBucketMeta,sourceObject,destBucketMeta,destObject);
            basereq.setHandled(true);
            return true;
        }

        // post object 前端上传对象已改为直接put api方式
        /*
        if (request.getParameter(Parameters.POST_OBEJCT) != null) {
            postObject(basereq, request, response);
            return true;
        }
         */
        // get package
        if (request.getHeader(WebsiteHeader.GET_PACKAGE) != null
                && request.getHeader(WebsiteHeader.GET_PACKAGE).equals("all")) {
            setContentLength(response, Portal.getPackage());
            basereq.setHandled(true);
            return true;
        }
        // get user package
        if (request.getHeader(WebsiteHeader.GET_PACKAGE) != null
                && !request.getHeader(WebsiteHeader.GET_PACKAGE).equals("all")) {
            OwnerMeta owner = Portal.getOwnerFromSession(session, Utils.getSessionId(request));
            setContentLength(response, Portal.getPackage(owner.getId()));
            basereq.setHandled(true);
            return true;
        }
        // order package
        if (request.getHeader(WebsiteHeader.ORDER_PACKAGE) != null) {
            OwnerMeta owner = Portal.getOwnerFromSession(session, Utils.getSessionId(request));
            Utils.checkParameter(request.getParameter("startTime"));
            Utils.checkParameter(request.getParameter("packageId"));
            Utils.checkParameter(request.getParameter("paytype"));
            setContentLength(response, Portal.orderPackage(owner,
                    request.getParameter("startTime"),
                    Integer.parseInt(request.getParameter("packageId")),
                    request.getParameter("paytype").equals("from-bestpay"),
                    Utils.getIpAddr(request), Portal.getAccountIdFromCookie(request)));
            basereq.setHandled(true);
            return true;
        }
        // close package
        if (request.getHeader(WebsiteHeader.CLOSE_PACKAGE) != null) {
            OwnerMeta owner = Portal.getOwnerFromSession(session, Utils.getSessionId(request));
            Utils.checkParameter(request.getParameter("orderId"));
            Portal.closePackage(owner.getId(), request.getParameter("orderId"));
            basereq.setHandled(true);
            return true;
        }
      
     // list bss package
        if (request.getHeader(WebsiteHeader.BSS_PACKAGE) != null) {
            response.setContentType("application/json;charset=utf-8");
            setContentLength(response, BssAdapterClient.listBssPackage(Portal.getAccountId(session, sessionId), 
                    BssAdapterConfig.localPool.ak,
                    BssAdapterConfig.localPool.getSK()));
            return true;
        }
      
        // modify user info
        if (request.getHeader(WebsiteHeader.MODIFY_USER_INFO) != null) {
            OwnerMeta owner = Portal.getOwnerFromSession(session, Utils.getSessionId(request));
            Utils.checkParameter(request.getParameter("companyName"));
            Utils.checkParameter(request.getParameter("companyAddress"));
            Utils.checkParameter(request.getParameter("displayName"));
            Utils.checkParameter(request.getParameter("mobilePhone"));
            Utils.checkParameter(request.getParameter("phone"));
            Utils.checkParameter(request.getParameter("email"));
            Portal.modifyUserInfo(owner, request.getParameter("companyName"),
                    request.getParameter("companyAddress"), request.getParameter("displayName"),
                    request.getParameter("mobilePhone"), request.getParameter("phone"),
                    request.getParameter("email"));
            basereq.setHandled(true);
            return true;
        }
        
        // get region Chinese name
        if (request.getHeader(WebsiteHeader.GET_REGION_MAP) != null) {
            setContentLength(response, Portal.getRgionMap());
            basereq.setHandled(true);
            return true;
        }
        
        // get owner bucket
        if (request.getHeader(WebsiteHeader.GET_BUCKETS_REGIONS) != null) {
            OwnerMeta owner = Portal.getOwnerFromSession(session, Utils.getSessionId(request));
            setContentLength(response, Portal.getOwnerBucketsAndRegions(owner));
            basereq.setHandled(true);
            return true;
        }
        
        // 管理事件
        if (request.getHeader(WebsiteHeader.MANAGE_EVENT) != null) {
            String userSessionId = Utils.getSessionId(request);
            OwnerMeta owner = Portal.getOwnerFromSession(session, userSessionId);
            String action = request.getHeader(WebsiteHeader.MANAGE_EVENT);
            if (action.equals("getEventNameList")) {
                JSONObject jo = new JSONObject();
                JSONArray ja = new JSONArray();
                for (CloudTrailManageEvent me : CloudTrailManageEvent.values()) {
                    ja.put(me.getName());
                }
                jo.put("eventNameList", ja);
                setContentLength(response, jo.toString());
            } else if (action.equals("lookupEvents")) {
                String attributeKey = request.getParameter("type");
                String attributeValue = request.getParameter("value");
                String option = request.getParameter("option");
                String beginDate = request.getParameter("beginDate");
                String endDate = request.getParameter("endDate");
                String beginTime = request.getParameter("beginTime");
                String endTime = request.getParameter("endTime");
                String maxNum = request.getParameter("maxNum");
                String nextToken = request.getParameter("nextToken");
                Map<String, String> attributes = new HashMap<String, String>();
                if (option.equals("read"))
                    attributes.put("ReadOnly", "true");
                else if (option.equals("write")) {
                    attributes.put("ReadOnly", "false");
                } else if (option.equals("all")) {
                } else
                    throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_ARGUMENT);

                if (attributeValue != null && attributeValue.length() > 0)
                    attributes.put(attributeKey, attributeValue);
                if (beginDate == null || beginDate.length() == 0)
                    beginDate = LocalDate.now().minusDays(180).toString();
                if (endDate == null || endDate.length() == 0)
                    endDate = LocalDate.now().toString();
                Date begin = Misc.formatyyyymmdd(beginDate);
                long startTime = begin.getTime();
                if (beginTime != null && beginTime.length() != 0)
                    startTime = Long.parseLong(beginTime);
                endDate = LocalDate.parse(endDate).plusDays(1).toString();
                Date end = Misc.formatyyyymmdd(endDate);
                long stopTime = end.getTime();
                if (endTime != null && endTime.length() != 0)
                    stopTime = Long.parseLong(endTime);
                JSONObject reqJo = new JSONObject();
                if (startTime > 0)
                    reqJo.put("StartTime", startTime);
                if (stopTime > 0)
                    reqJo.put("EndTime", stopTime);
                JSONArray reqJa = new JSONArray();
                for (Entry<String, String> entry : attributes.entrySet()) {
                    JSONObject reqJo2 = new JSONObject();
                    reqJo2.put("attributeKey", entry.getKey());
                    reqJo2.put("attributeValue", entry.getValue());
                    reqJa.put(reqJo2);
                }
                reqJo.put("lookupAttributes", reqJa);
                // 管理事件赋值
                manageCloudTrailEvent(owner, manageEvent, CloudTrailManageEvent.CloudTrail_Lookup_Events, "ApiCall", "CloudTrail",
                        reqJo.toString(), request, userSessionId);
                // 权限检验
                CloudTrailMeta meta = new CloudTrailMeta(owner.getId(), "*");
                checkAccountCloudTrailSummaryAllow(request, CloudTrailManageEvent.CloudTrail_Lookup_Events.getName(), meta);
                int max = -2;
                if (maxNum != null && maxNum.length() > 0)
                    max = Integer.valueOf(maxNum);
                String respString = CloudTrailAction.lookUpEventsWithManageEvent(owner, startTime, stopTime, attributes, Integer.MAX_VALUE, max + 1,
                        nextToken, manageEvent);                   

                JSONObject respJo = new JSONObject(respString);
                JSONArray respJa = respJo.getJSONArray("Events");
                for (int i = 0; i < respJa.length(); i++) {
                    JSONObject eventsJo = respJa.getJSONObject(i);
                    JSONObject cloudTrailJo = eventsJo.getJSONObject("CloudTrailEvent");
                    String eventTime = cloudTrailJo.getString("eventTime");
                    String ldt = LocalDateTime.ofInstant(Instant.parse(eventTime), ZoneId.of("+8")).format(Misc.format_uuuu_MM_dd_HH_mm_ss);
                    eventsJo.put("eventTime", ldt);
                    eventsJo.put("eventName", cloudTrailJo.get("eventName"));
                    eventsJo.put("requestId", cloudTrailJo.get("requestId"));
                    eventsJo.put("eventID", cloudTrailJo.get("eventId"));
                    eventsJo.put("readOnly", cloudTrailJo.get("readOnly"));
                    eventsJo.put("eventSource", cloudTrailJo.get("eventSource"));
                    eventsJo.put("sourceIp", cloudTrailJo.get("sourceIp"));
                    if(cloudTrailJo.has("errorCode"))
                        eventsJo.put("errorCode", cloudTrailJo.get("errorCode"));
                    JSONObject idJo = cloudTrailJo.getJSONObject("userIdentity");
                    String accessKeyId = "";
                    if (idJo.has("accessKeyId"))
                        accessKeyId = idJo.getString("accessKeyId");
                    eventsJo.put("accessKeyId", accessKeyId);
                    String userName = "";
                    if (idJo.has("userName"))
                        userName = idJo.getString("userName");
                    eventsJo.put("userName", userName);
                    String accountName = "";
                    String accountId = "";
                    if (idJo.has("accountId")) {
                        accountId = idJo.getString("accountId");
                        OwnerMeta ownerMeta = Portal.getOwnerByIamAccountId(accountId);
                        accountName = ownerMeta.name;
                    }
                    eventsJo.put("accountId", accountId);
                    eventsJo.put("accountName", accountName);
                    
                    JSONObject resourceJo = null;
                    List<String> resourceType = new ArrayList<String>();
                    List<String> resourceName = new ArrayList<String>();
                    if(cloudTrailJo.has("resource")) {
                        resourceJo = new JSONObject(cloudTrailJo.getString("resource"));
                        JSONArray resourceJa = resourceJo.getJSONArray("Resources");
                        for (int j = 0; j < resourceJa.length(); j++) {
                            JSONObject jo = resourceJa.getJSONObject(j);
                            resourceType.add(jo.getString("type"));
                            resourceName.add(jo.getString("name"));
                        }
                    }
                    eventsJo.put("resourceType", StringUtils.join(resourceType.toArray(), ","));
                    eventsJo.put("resourceName", StringUtils.join(resourceName.toArray(), ","));
                }
                setContentLength(response, respJo.toString());
            }
            basereq.setHandled(true);
            return true;
        }
        // 日志审计
        if(request.getHeader(WebsiteHeader.CLOUD_TRAIL) != null) {
            String userSessionId = Utils.getSessionId(request);
            OwnerMeta owner = Portal.getOwnerFromSession(session, userSessionId);
            String action = request.getHeader(WebsiteHeader.CLOUD_TRAIL);
            if(action.equals("checkAccess")) {
                String option = request.getParameter("option");
                String trailName = request.getParameter("name");
                String targetBucket = request.getParameter("targetBucket");
                String status = request.getParameter("isLogging");
                String readWriteType = request.getParameter("readWriteType");
                Common.checkParameter(option);
                Common.checkParameter(trailName);
                Common.checkParameter(targetBucket);
                Common.checkParameter(readWriteType);
                if (!readWriteType.equals("WriteOnly") && !readWriteType.equals("ReadOnly") && !readWriteType.equals("All"))
                    throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_ARGUMENT);
                CloudTrailMeta meta = new CloudTrailMeta(owner.getId(), trailName);
                if (option.equals("create")) {
                    checkAccountCloudTrailSummaryAllow(request, CloudTrailManageEvent.CloudTrail_Create_Trail.getName(), meta);
                    if (status != null && status.equals("true")) {
                        checkAccountCloudTrailSummaryAllow(request, CloudTrailManageEvent.CloudTrail_Start_Logging.getName(), meta);
                    }
                    checkAccountCloudTrailSummaryAllow(request, CloudTrailManageEvent.CloudTrail_Put_EventSelectors.getName(), meta);
                } else if (option.equals("update")) {
                    checkAccountCloudTrailSummaryAllow(request, CloudTrailManageEvent.CloudTrail_Update_Trail.getName(), meta);
                    checkAccountCloudTrailSummaryAllow(request, CloudTrailManageEvent.CloudTrail_Put_EventSelectors.getName(), meta);
                } else {
                    throw new BaseException(400, "InvalidOptionNameException", "option name can be create/update");
                }
            } else if (action.equals("createTrail")) {
                String trailName = request.getParameter("name");
                String targetBucket = request.getParameter("targetBucket");
                String prefix = request.getParameter("prefix");
                Common.checkParameter(trailName);
                Common.checkParameter(targetBucket);
                // 请求参数
                JSONObject reqJo = new JSONObject();
                reqJo.put("name", trailName);
                reqJo.put("s3BucketName", targetBucket);
                if (prefix != null && prefix.length() > 0)
                    reqJo.put("s3KeyPrefix", prefix);
                // 管理事件赋值
                manageCloudTrailEvent(owner, manageEvent, CloudTrailManageEvent.CloudTrail_Create_Trail, "ApiCall", "CloudTrail", reqJo.toString(), request, userSessionId);
                // 权限检验
                CloudTrailMeta meta = new CloudTrailMeta(owner.getId(), trailName);
                checkAccountCloudTrailSummaryAllow(request, CloudTrailManageEvent.CloudTrail_Create_Trail.getName(), meta);
                CloudTrailAction.checkTrailNameFormat(trailName);
                if (targetBucket == null || targetBucket.length() == 0)
                    throw new BaseException(400, "InvalidS3BucketNameException", "Bucket name cannot be blank!");
                if (prefix != null)
                    CloudTrailAction.checkTrailFilePrefix(prefix);
                CloudTrailAction.createTrailWithManageEvent(owner, trailName.trim(), targetBucket, prefix, manageEvent);
            }
            else if (action.equals("updateTrail")) {
                String trailName = request.getParameter("name");
                String targetBucket = request.getParameter("targetBucket");
                String prefix = request.getParameter("prefix");
                // 请求参数
                JSONObject reqJo = new JSONObject();
                reqJo.put("name", trailName);
                reqJo.put("s3BucketName", targetBucket);
                if (prefix != null && prefix.length() > 0)
                    reqJo.put("s3KeyPrefix", prefix);
                // 管理事件赋值
                manageCloudTrailEvent(owner, manageEvent, CloudTrailManageEvent.CloudTrail_Update_Trail, "ApiCall", "CloudTrail", reqJo.toString(), request, userSessionId);
                // 权限检验
                CloudTrailMeta meta = new CloudTrailMeta(owner.getId(), trailName);
                checkAccountCloudTrailSummaryAllow(request, CloudTrailManageEvent.CloudTrail_Update_Trail.getName(), meta);
                if (targetBucket == null || targetBucket.length() == 0)
                    throw new BaseException(400, "InvalidS3BucketNameException", "Bucket name cannot be blank!");
                if (prefix != null)
                    CloudTrailAction.checkTrailFilePrefix(prefix);
                CloudTrailAction.updateTrailWithManageEvent(owner, trailName, targetBucket, prefix, manageEvent);
            }
            else if (action.equals("describeTrails")) {
                String trailName = request.getParameter("name");
                List<String> trailNames = new ArrayList<String>();
                // 请求参数
                JSONObject reqJo = new JSONObject();
                JSONArray reqJa = new JSONArray();
                if (trailName != null && trailName.length() != 0) {
                    trailNames.add(trailName);
                    reqJa.put(trailName);
                }
                reqJo.put("TrailNameList", reqJa);
                HttpResponse newResponse = buildCloudTrailRequest(request, reqJo.toString(),
                        CloudTrailManageEvent.CloudTrail_Describe_Trails.getName());
                HttpEntity respEntity = newResponse.getEntity();
                InputStream respInputStream = respEntity.getContent();
                String respFromCloudTrailServer = IOUtils.toString(respInputStream, Consts.STR_UTF8);
                try {
                    JSONObject respJo = new JSONObject(respFromCloudTrailServer);
                    JSONArray respJa = respJo.getJSONArray("trailList");
                    for (int i = 0; i < respJa.length(); i++) {
                        JSONObject jo = respJa.getJSONObject(i);
                        String name = jo.getString("Name");
                        JSONObject reqJo2 = new JSONObject();
                        reqJo2.put("Name", name);
                        HttpResponse statusResponse = buildCloudTrailRequest(request, reqJo2.toString(),
                                CloudTrailManageEvent.CloudTrail_Get_Trail_Status.getName());
                        HttpEntity statusEntity = statusResponse.getEntity();
                        InputStream statusInputStream = statusEntity.getContent();
                        String respStatus = IOUtils.toString(statusInputStream, Consts.STR_UTF8);
                        try {
                            JSONObject respJo2 = new JSONObject(respStatus);
                            jo.put("isLogging", respJo2.getBoolean("IsLogging"));
                        } catch (JSONException e) {
                            InputSource is = new InputSource(new StringReader(respStatus));
                            Document doc = (new SAXBuilder()).build(is);
                            Element root = doc.getRootElement();
                            String message = root.getChildText("Code");
                            StatusLine status = statusResponse.getStatusLine();
                            throw new BaseException(status.getStatusCode(), message); //转发至cloudTrailServer后返回无权限报错
                        }
                    }
                    setContentLength(response, respJo.toString());
                } catch (JSONException e) {
                    InputSource is = new InputSource(new StringReader(respFromCloudTrailServer));
                    Document doc = (new SAXBuilder()).build(is);
                    Element root = doc.getRootElement();
                    Element code = root.getChild("Code");
                    String message = code.getText();
                    StatusLine status = newResponse.getStatusLine();
                    throw new BaseException(status.getStatusCode(), message); //转发至cloudTrailServer后返回无权限报错
                }
            }
            else if (action.equals("deleteTrail")) {
                String trailName = request.getParameter("name");
                Common.checkParameter(trailName);
                // 请求参数
                JSONObject reqJo = new JSONObject();
                reqJo.put("name", trailName);
                // 管理事件赋值
                manageCloudTrailEvent(owner, manageEvent, CloudTrailManageEvent.CloudTrail_Delete_Trail, "ApiCall", "CloudTrail", reqJo.toString(), request, userSessionId);
                // 权限检验
                CloudTrailMeta meta = new CloudTrailMeta(owner.getId(), trailName);
                checkAccountCloudTrailSummaryAllow(request, CloudTrailManageEvent.CloudTrail_Delete_Trail.getName(), meta);
                CloudTrailAction.deleteTrailWithManageEvent(owner, trailName, manageEvent);
            }
            else if (action.equals("setStatus")) {
                String trailName = request.getParameter("name");
                String status = request.getParameter("isLogging");
                Common.checkParameter(trailName);
                Common.checkParameter(status);
                // 请求参数
                JSONObject reqJo = new JSONObject();
                reqJo.put("name", trailName);
                CloudTrailMeta meta = new CloudTrailMeta(owner.getId(), trailName);
                if (status.equals("true")) {
                    // 管理事件赋值
                    manageCloudTrailEvent(owner, manageEvent, CloudTrailManageEvent.CloudTrail_Start_Logging, "ApiCall", "CloudTrail", reqJo.toString(), request, userSessionId);
                    // 权限检验
                    checkAccountCloudTrailSummaryAllow(request, CloudTrailManageEvent.CloudTrail_Start_Logging.getName(), meta);
                    CloudTrailAction.startLoggingWithManageEvent(owner, trailName, manageEvent);
                } else if (status.equals("false")) {
                    // 管理事件赋值
                    manageCloudTrailEvent(owner, manageEvent, CloudTrailManageEvent.CloudTrail_Stop_Logging, "ApiCall", "CloudTrail", reqJo.toString(), request, userSessionId);
                    // 权限检验
                    checkAccountCloudTrailSummaryAllow(request, CloudTrailManageEvent.CloudTrail_Stop_Logging.getName(), meta);
                    CloudTrailAction.stopLoggingWithManageEvent(owner, trailName, manageEvent);
                } else
                    throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_ARGUMENT);
            }
            else if (action.equals("getStatus")) {
                String trailName = request.getParameter("name");
                Common.checkParameter(trailName);
                // 请求参数
                JSONObject reqJo = new JSONObject();
                reqJo.put("name", trailName);
                // 管理事件赋值
                manageCloudTrailEvent(owner, manageEvent, CloudTrailManageEvent.CloudTrail_Get_Trail_Status, "ApiCall",
                        "CloudTrail", reqJo.toString(), request, userSessionId);
                // 权限检验
                CloudTrailMeta meta = new CloudTrailMeta(owner.getId(), trailName);
                checkAccountCloudTrailSummaryAllow(request, CloudTrailManageEvent.CloudTrail_Get_Trail_Status.getName(), meta);
                String respString = CloudTrailAction.getTrailStatusWithManageEvent(owner, trailName, manageEvent);
                JSONObject respJo = new JSONObject(respString);
                JSONObject jo = new JSONObject();
                jo.put("isLogging", respJo.get("IsLogging"));
                String lastDeliveryTime = "";
                if (respJo.has("LatestDeliveryTime")) {
                    lastDeliveryTime = LocalDateTime
                            .ofInstant(Instant.ofEpochMilli(respJo.getLong("LatestDeliveryTime")), ZoneId.of("+8")).format(Misc.format_uuuu_MM_dd_HH_mm_ss);
                    lastDeliveryTime = lastDeliveryTime + " UTC+0800";
                }                    
                jo.put("latestDeliveryTime", lastDeliveryTime);
                setContentLength(response, jo.toString());
            }
            else if (action.equals("putEventSelectors")) {
                String trailName = request.getParameter("name");
                String readWriteType = request.getParameter("readWriteType");
                Common.checkParameter(trailName);
                Common.checkParameter(readWriteType);
                if (!readWriteType.equals("WriteOnly") && !readWriteType.equals("ReadOnly") && !readWriteType.equals("All"))
                    throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_ARGUMENT);
                // 请求参数
                JSONObject reqJo = new JSONObject();
                reqJo.put("name", trailName);
                JSONArray reqJa = new JSONArray();
                JSONObject reqJo2 = new JSONObject();
                reqJo2.put("readWriteType", readWriteType);
                reqJa.put(reqJo2);
                reqJo.put("eventSelectors", reqJa);
                // 管理事件赋值
                manageCloudTrailEvent(owner, manageEvent, CloudTrailManageEvent.CloudTrail_Put_EventSelectors, "ApiCall",
                        "CloudTrail", reqJo.toString(), request, userSessionId);
                // 权限检验
                CloudTrailMeta meta = new CloudTrailMeta(owner.getId(), trailName);
                checkAccountCloudTrailSummaryAllow(request, CloudTrailManageEvent.CloudTrail_Put_EventSelectors.getName(), meta);
                CloudTrailAction.putEventSelectorsWithManageEvent(owner, trailName, true, readWriteType, manageEvent);
            }
            else if (action.equals("getEventSelectors")) {
                String trailName = request.getParameter("name");
                Common.checkParameter(trailName);
                // 请求参数
                JSONObject reqJo = new JSONObject();
                reqJo.put("name", trailName);
                // 管理事件赋值
                manageCloudTrailEvent(owner, manageEvent, CloudTrailManageEvent.CloudTrail_Get_EventSelectors, "ApiCall", "CloudTrail", reqJo.toString(), request, userSessionId);
                // 权限检验
                CloudTrailMeta meta = new CloudTrailMeta(owner.getId(), trailName);
                checkAccountCloudTrailSummaryAllow(request, CloudTrailManageEvent.CloudTrail_Get_EventSelectors.getName(), meta);
                String respString = CloudTrailAction.getEventSelectorsWithManageEvent(owner, trailName, manageEvent);
                JSONObject jo = new JSONObject(respString);
                JSONArray ja = jo.getJSONArray("EventSelectors");
                String type = "All";
                if(ja.length() > 0) {
                    JSONObject jo2 = (JSONObject)ja.get(0);
                    type = jo2.getString("ReadWriteType");
                }
                JSONObject respJo = new JSONObject();
                respJo.put("readWriteType", type);
                setContentLength(response, respJo.toString());
            }
            else if(action.equals("setOwnerPermission")) {
                owner.ifRecordManageEvent = true;
                metaClient.ownerUpdate(owner);
            }
            else if(action.equals("getOwnerPermission")) {
                JSONObject jo = new JSONObject();
                jo.put("needPermit", !owner.ifRecordManageEvent);
                setContentLength(response, jo.toString());
            }
            basereq.setHandled(true);
            return true;
        }
        
        return false;
    }
    
    /**
     * 内部转发请求到cloudTrailServer
     * @param request
     * @param reqJson
     * @param action
     * @return
     * @throws Exception
     */
    private HttpResponse buildCloudTrailRequest(HttpServletRequest request, String reqJson, String action) throws Exception {
        String method = "POST";
        String hostName = OOSConfig.getCloudTrailDomainSuffix();
        int port = config.getInt("website.cloudTrailPort");
        String uri = "/?proxyURL=" + request.getRequestURL().toString();
        HttpRequestBase httpReq = new HttpPost(uri);
        @SuppressWarnings("rawtypes")
        OOSRequest cloudTrailRequest = new OOSRequest(httpReq);
        cloudTrailRequest.setHttpMethod(HttpMethodName.POST);
        cloudTrailRequest.addParameter("proxyURL", request.getRequestURL().toString());
        
        String dateTimeStamp =  SignerUtils.formatTimestamp(new Date().getTime());
        httpReq.addHeader("Date", dateTimeStamp);
        httpReq.addHeader("Host", hostName + ":" + port);
        httpReq.addHeader("x-amz-content-sha256", V4Signer.UNSIGNED_PAYLOAD);
        httpReq.addHeader("OOS-PROXY-HOST", "oos-proxy");
        httpReq.addHeader("Content-Type", "application/json");
        httpReq.addHeader("X-Amz-Target", "cn.ctyunapi.oos-cn-cloudtrail.v20131101.CloudTrail_20131101." + action);
        
        cloudTrailRequest.addHeader("Date", dateTimeStamp);
        cloudTrailRequest.addHeader("Host", hostName + ":" + port);
        cloudTrailRequest.addHeader("x-amz-content-sha256", V4Signer.UNSIGNED_PAYLOAD);
        cloudTrailRequest.addHeader("OOS-PROXY-HOST", "oos-proxy");
        cloudTrailRequest.addHeader("Content-Type", "application/json");
        cloudTrailRequest.addHeader("X-Amz-Target", "cn.ctyunapi.oos-cn-cloudtrail.v20131101.CloudTrail_20131101." + action);
        
        BasicHttpEntity entity = new BasicHttpEntity();
        InputStream inputStream = new ByteArrayInputStream(reqJson.getBytes());
        entity.setContent(inputStream);
        ((HttpPost) httpReq).setEntity(entity);

        Pair<String, String> akskPair=  getAkSk(request);
        String ak = akskPair.first();
        String sk =  akskPair.second();
        String cloudTrailURL = "http://" + hostName + ":" + port;
        URL url = new URL(cloudTrailURL);
        String authorization = Signer.signV4Iam(cloudTrailRequest, ak, sk, url, method, dateTimeStamp, V4Signer.CLOUDTRAIL_SERVICE_NAME);
        if (authorization != null)
            httpReq.addHeader("Authorization", authorization);
        
        httpReq.addHeader(WebsiteHeader.CLIENT_IP, Utils.getIpAddr(request));
        preProcess(httpReq);
        HttpHost httpHost = new HttpHost(hostName,port);
        HttpResponse newResponse = client.execute(httpHost, httpReq);
        return newResponse;
    }
    
    private void manageCloudTrailEvent(OwnerMeta owner, ManageEventMeta manageEvent, CloudTrailManageEvent action,
            String eventType, String serviceName,String reqString, HttpServletRequest req, String sessionId) throws Exception {
        String userType = "";
        String sessionIam = null;
        if (sessionId != null && Portal.isIamUser(session, sessionId)) {
            userType = "iamUser";
            sessionIam = session.getAndUpdate(sessionId + "|iam");
        //如果子用户登陆失败，仍需记录子用户相关信息
        } else if (req.getParameter(Parameters.USER_IAM_ACCOUNTID) != null) {
            userType = "iamAccount";
        }
        manageCloudTrailEvent(owner, manageEvent, action, eventType, serviceName, reqString, req, userType, sessionIam);
    }

    private void manageCloudTrailEvent(OwnerMeta owner, ManageEventMeta manageEvent, CloudTrailManageEvent action,
            String eventType, String serviceName,String reqString, HttpServletRequest req, String userType, String sessionIam) throws Exception {
        if (owner.ifRecordManageEvent) {
            CloudTrailEvent event = manageEvent.getEvent();
            // 固定参数
            event.eventSource = "oos-cn.ctyun.cn";
            event.eventType = eventType;
            event.managementEvent = true;
            event.serviceName = serviceName;

            // 用户参数
            if (userType != null && userType.equals("iamUser")) {
                // 子用户
                Portal.IamAccount iamAccount = new Portal.IamAccount(new JSONObject(sessionIam));
                AkSkMeta accessKey = new AkSkMeta(iamAccount.accessKeyId);
                metaClient.akskSelect(accessKey);
                event.userType = "IAMUser";
                event.userName = iamAccount.userName;
                event.principalId = accessKey.userId;
                event.arn = ARNUtils.generateUserArn(owner.getAccountId(), accessKey.userName);
                event.eventOwnerId = owner.getId();
                event.ownerId = owner.getId();
            //如果子用户登陆失败，仍需记录子用户相关信息
            } else if (userType != null && userType.equals("iamAccount")) {
                String userName = req.getParameter(Parameters.USER_NAME);
                event.userType = "IAMUser";
                event.userName = userName;
                event.principalId = "-";
                event.arn = ARNUtils.generateArn(owner.getAccountId(), userName);
                event.eventOwnerId = owner.getId();
                event.ownerId = owner.getId();
            } else {
                if(owner.getId() == 0)
                    return;
                // 根账户
                event.userType = "Root";
                event.principalId = owner.getAccountId();
                event.arn = ARNUtils.generateArn(owner.getAccountId(), "root");
                event.eventOwnerId = owner.getId();
                event.ownerId = owner.getId();
                event.accessKeyId = null;
            }
            // 请求的事件参数
            event.eventTime = (long) req.getAttribute(Headers.DATE);
            event.requestId = (String) req.getAttribute(WebsiteHeader.REQUEST_ID);
            event.userAgent = "oos-cn.ctyun.cn";
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
            event.requestRegion = "cn";

            event.eventName = action.getName();
            event.readOnly = action.getReadOnly();
            event.requestParameters = reqString;
        }
    }
    
    /**
     * 获取使用量统计请求query参数，并校验参数合法性
     * @param request
     * @return
     * @throws Exception
     */
    public RequestParams prepareOrder(HttpServletRequest request) throws Exception {
        OwnerMeta owner = Portal.getOwnerFromSession(session, Utils.getSessionId(request));
        String beginDate = request.getParameter("beginDate");
        String endDate = request.getParameter("endDate");
        String bucketName = request.getParameter("bucketName");
        String freq = request.getParameter("freq");
        Utils.checkUsageReqParamsValid(beginDate, endDate, freq);
        RequestParams p = new RequestParams(owner, beginDate, endDate, bucketName, freq);
        return p;
    }
    
    /**
     * 查询用量时根据查询数据类型返回管理事件操作类型，由于页面中根据不同类型数据查询，因此简单判断请求参数
     * @param usageStat
     * @return
     */
    public CloudTrailManageEvent getUsageManageEventType(String usageStat) {
        if (usageStat.contains("Size"))
            return CloudTrailManageEvent.Manage_Get_Capacity;
        if (usageStat.contains("Req"))
            return CloudTrailManageEvent.Manage_Get_Requests;
        if (usageStat.contains("avail") && usageStat.contains("BW"))
            return CloudTrailManageEvent.Manage_Get_AvailableBandwidth;
        if (usageStat.contains("BW"))
            return CloudTrailManageEvent.Manage_Get_Bandwidth;
        if (usageStat.contains("connection") || usageStat.contains("Connection"))
            return CloudTrailManageEvent.Manage_Get_ConcurrentConnection;
        if (usageStat.contains("200") || usageStat.contains("204") || usageStat.contains("206") || usageStat.contains("2_n")
                || usageStat.contains("403") || usageStat.contains("404") || usageStat.contains("4_n"))
            return CloudTrailManageEvent.Manage_Get_ReturnCode;
        if (usageStat.contains("upload") || usageStat.contains("Upload") || usageStat.contains("download")
                || usageStat.contains("Download"))
            return CloudTrailManageEvent.Manage_Get_Traffics;
        return null;
    }
    
    static String generateOrderId() {
        byte[] buf = new byte[3];
        new Random().nextBytes(buf);
        return HexUtils.toHexString(buf);
    }

    /**
     * 前端上传对象已改为直接put api方式，本方法已作废
     */
    @Deprecated
    private void postObject(Request basereq, HttpServletRequest request,
            HttpServletResponse response) throws Exception {
        BucketLog bucketLog = new BucketLog(System.currentTimeMillis());
        bucketLog.operation = "REST." + request.getMethod() + ".-";
        bucketLog.getRequestInfo(request);
        String contentType = request.getContentType();
        byte[] boundary = Utils.getBoundary(contentType);
        InputStream input = request.getInputStream();
        String bucketName, objectName, position, sessionId2;
        objectName = request.getParameter("filename");
        Notify notify = new Notify();
        bucketName = request.getParameter("bucketname");
        position = URLDecoder.decode(request.getParameter("position"), Consts.STR_UTF8);
        objectName = position.trim() + objectName;
        sessionId2 = request.getParameter("sessionId");
        OwnerMeta owner = Portal.getOwnerFromSession(session, sessionId2);
        BucketMeta bucket = new BucketMeta(bucketName);
        if (!metaClient.bucketSelect(bucket))
            throw new BaseException(400, ErrorMessage.ERROR_CODE_NO_SUCH_BUCKET);
        ObjectMeta object = new ObjectMeta(objectName, bucketName, bucket.metaLocation);
        // 验证用户PutObject权限
        checkAccountPutObjectAllow(request,bucket,objectName);
        if (!ifCanWrite(owner, bucket))
            throw new BaseException(403, "AccessDenied");
        boolean objectExists = false;
        PinData pinData=new PinData();
        try {
            SinglepartInputStream ms = new SinglepartInputStream(new MultipartInputStream(input,
                    boundary, notify), notify);
            objectExists = metaClient.objectSelect(object);
            OpObject.postObject(request, response, bucket, object, ms, bucketLog, objectExists, pinData);
        } finally {
            input.close();
        }
        String ipAddr = Utils.getIpAddr(request);
        boolean isInternetIP = Utils.isInternetIP(ipAddr);
        pinData.setRequestInfo(bucket.getOwnerId(), bucket.name, isInternetIP, "post");
        if (response.getStatus() != 403) {
            //实际上只有真正post object成功之后才统计容量和流量
            Utils.pinStorage(bucket.getOwnerId(), pin, pinData, bucket.name);
            Utils.pinUpload(bucket.getOwnerId(), pin, pinData, isInternetIP, bucket.name);
//            Utils.pinPut(response.getStatus(), bucketLog, bucket.getOwnerId(), pin, pinData,isInternetIP, bucket.name);//TODO 改成内部api的region
        }
        basereq.setHandled(true);
        Utils.pinRequest(pin, pinData, response.getStatus(), bucketLog);

        // 子用户名称
        String iamUserName = null;
        String sessionId = Utils.getSessionId(request);
        // 如果是子用户，获取子用户名称
        if (Portal.isIamUser(session, sessionId)) {
            Portal.IamAccount iamAccount = Portal.getIamAccount(session, sessionId);
            iamUserName = iamAccount.userName;
        }
        bucketLog.prepare(bucket, object.name, owner, request, response.getStatus(), pinData, iamUserName);
        bucketLog.ostorId = object.ostorId;
        if (bucket.logTargetBucket != 0) {
            bucketLog.write(request, bucketLog.targetBucketName, bucketLog.targetPrefix, true, pinData);
        }
        if (bucketLog.bucketOwnerName != null)
            bucketLog.writeGlobalLog(request, pinData);
    }
    
    public boolean ifCanWrite(OwnerMeta owner, BucketMeta bucket) throws SQLException {
        if (bucket.permission == BucketMeta.PERM_PUBLIC_READ_WRITE)
            return true;
        if (owner == null)
            return false;
        if (bucket.getOwnerId() == owner.getId())
            return true;
        return false;
    }
    
    public static void setContentLength(HttpServletResponse resp, String body) throws IOException {
        resp.setCharacterEncoding(Consts.STR_UTF8);
        if (body != null) {
            resp.setIntHeader(Headers.CONTENT_LENGTH, body.getBytes(Consts.CS_UTF8).length);
            resp.getWriter().write(body);
        } else
            resp.setIntHeader(Headers.CONTENT_LENGTH, 0);
    }
    
    public static void setContentLength(HttpServletResponse resp, String body, String format, String file) throws IOException {
        resp.setCharacterEncoding(Consts.STR_UTF8);
        if (format!= null && format.equals(Utils.CSV)) {
            resp.setHeader("Content-type", "application/octet-stream");
            resp.setHeader("Content-Disposition","attachment;filename="+file);
        }
        if (body != null) {
            resp.setIntHeader(Headers.CONTENT_LENGTH, body.getBytes(Consts.CS_UTF8).length);
            resp.getWriter().write(body);
        } else
            resp.setIntHeader(Headers.CONTENT_LENGTH, 0);
    }
    
    private void setResponse(HttpServletResponse resp, HttpResponse response) throws IOException {
        org.apache.http.Header[] headers = response.getAllHeaders();
        for (org.apache.http.Header header : headers) {
            resp.addHeader(header.getName(), header.getValue());
        }
        StatusLine status = response.getStatusLine();
        resp.setStatus(status.getStatusCode());
        HttpEntity entity = response.getEntity();
        if (entity != null) {
            ServletOutputStream outstream = null;
            try {
                outstream = resp.getOutputStream();
                entity.writeTo(outstream);
                outstream.flush();
                outstream.close();
            } catch (IOException e) {
                log.error(e.getMessage(), e);
            } finally {
                if (outstream != null)
                    try {
                        outstream.close();
                    } catch (Throwable e) {
                        log.error(e.getMessage(), e);
                    }
                EntityUtils.consume(entity);
            }
        }
    }
    
    private HttpRequestBase buildPackageRequest(HttpServletRequest request, String sessionId) throws SQLException, URISyntaxException {
        HttpRequestBase httpRequest = null;
        String accountId = Portal.getAccountId(session, sessionId);
        String method = request.getMethod();
        String path = request.getPathInfo();
        httpRequest = new HttpGet(path+"?accoundId="+accountId);
        return httpRequest;
    }

    private HttpRequestBase buildRequest(HttpServletRequest request) throws Exception {
        Pair<String, String> p;
        String host = null;
        if (request.getHeader(WebsiteHeader.HOST) != null) {
            host = request.getHeader(WebsiteHeader.HOST);
            p = Utils.getBucketKey(request.getRequestURI(), host, OOSConfig.getDomainSuffix(),
                    String.valueOf(port), String.valueOf(sslPort), OOSConfig.getWebSiteEndpoint(),
                    portalDomain, OOSConfig.getInvalidBucketName(),
                    OOSConfig.getInvalidObjectName(), true);
        } else {
            host = request.getHeader("Host");
            p = Utils.getBucketKey(request.getRequestURI(), host, new String[] { portalDomain },
                    String.valueOf(websitePort), String.valueOf(websiteSslPort),
                    OOSConfig.getWebSiteEndpoint(), portalDomain,
                    config.getStringArray("oos.invalidBucketName"),
                    config.getStringArray("oos.invalidObjectName"), true);
        }
        String bucket = p.first();
        String key = p.second();
        String keyOri = key;
        if (key != null && key.trim().length() != 0) {
            key = URLEncoder.encode(key, Consts.STR_UTF8);
            key = key.replaceAll("\\+", "%20");
        }
        String method = request.getMethod();
        if (method == null)
            throw new BaseException(405, "Method Not Allowed");
        @SuppressWarnings("rawtypes")
        OOSRequest oosRequest = new OOSRequest(request);
        String parameters = HttpUtils.encodeParameters(oosRequest);
        String uri;
        StringBuilder sb = new StringBuilder();
        sb.append("/");
        if (bucket != null && bucket.trim().length() != 0)
            sb.append(bucket).append("/");
        if (key != null && key.trim().length() != 0)
            sb.append(key);
        if (parameters != null && parameters.trim().length() != 0)
            sb.append("?").append(parameters);
        uri = sb.toString();
        HttpRequestBase httpRequest = null;
        if (method.equals("GET")) {
            httpRequest = new HttpGet(uri);
        } else if (method.equals("PUT")) {
            httpRequest = new HttpPut(uri);
        } else if (method.equals("DELETE")) {
            httpRequest = new HttpDelete(uri);
        } else if (method.equals("HEAD")) {
            httpRequest = new HttpHead(uri);
        } else if (method.equals("POST")) {
            httpRequest = new HttpPost(uri);
        }
        Enumeration<?> e = request.getHeaderNames();
        String header = null;
        String value = null;
        String bucketAction = null;
        while (e.hasMoreElements()) {
            header = (String) e.nextElement();
            value = request.getHeader(header);
            if (header.contains("x-ctyun"))
                continue;
            if (header.equalsIgnoreCase("host")) {
                continue;
            }
            if (header.equalsIgnoreCase("date")) {
                continue;
            }
            if (header.equalsIgnoreCase("Referer")) {
                continue;
            }
            if (header.equalsIgnoreCase("bucketRegion")) {
                bucketAction = value;
                continue;
            }
            //清理上传对象时传递的冗余Content-Disposition头（否则会被保存到对象属性当中，下载时被浏览器识别判断为下载连接）
            if (header.equalsIgnoreCase("Content-Disposition")) {
                continue;
            }
            httpRequest.addHeader(header, value);
        }
        httpRequest.addHeader("Referer", "http://" + portalDomain + "/");
        httpRequest.addHeader(WebsiteHeader.CLIENT_IP, Utils.getIpAddr(request));
        httpRequest.addHeader("OOS-PROXY-HOST", "oos-proxy");
        httpRequest.addHeader("OOS-PROXY-URL", request.getRequestURL().toString());
        String date = TimeUtils.toGMTFormat(new Date());
        httpRequest.addHeader("Date", date);
        oosRequest.addHeader("Date", date);
        if (((oosRequest.getParameters().containsKey("website")
                || oosRequest.getParameters().containsKey("policy")
                || oosRequest.getParameters().containsKey("cname")
                || oosRequest.getParameters().containsKey("logging") 
                || oosRequest.getParameters().containsKey("lifecycle")
                || oosRequest.getParameters().containsKey("accelerate")
                || oosRequest.getParameters().containsKey("cors")
                || oosRequest.getParameters().containsKey("object-lock")
                ) && method.equals("PUT")
            )
                || (bucketAction != null && (bucketAction.equalsIgnoreCase("put") ||  //新增bucket 或 修改bucketregion属性
                bucketAction.equalsIgnoreCase("update"))) || request.getHeader("postObject") != null) {
            BasicHttpEntity entity = new BasicHttpEntity();
            String ctLenStr = request.getHeader(Headers.CONTENT_LENGTH);
            log.info("The client specifies the request length:" + ctLenStr);
            //存储客户端指定长度时，采用客户端指定值
            if (StringUtils.isNotEmpty(ctLenStr)) {
                entity.setContentLength(Long.parseLong(ctLenStr));
            } else {
                entity.setContentLength(request.getContentLength());
            }
            entity.setContent(request.getInputStream());
            ((HttpPut) httpRequest).setEntity(entity);
        }
        /*
         * log.info(bucket+"|"+key+"|");
         * httpRequest.addHeader(Header.BUCKET_KEY, bucket + ":" + key);
         */
        String authorization = getAuthorizationIam(oosRequest, request, method, bucket, keyOri);
        if (authorization != null)
            httpRequest.addHeader("Authorization", authorization);
        return httpRequest;
    }
    
    private HttpRequestBase buildIAMRequest(HttpServletRequest request) throws Exception {
        String method = request.getMethod();
        if (method == null)
            throw new BaseException(405, "Method Not Allowed");
        @SuppressWarnings("rawtypes")
        OOSRequest iamRequest = new OOSRequest(request);
        String parameters = HttpUtils.encodeParameters(iamRequest);
        String uri;
        StringBuilder sb = new StringBuilder();
        sb.append("/");
        if (parameters != null && parameters.trim().length() != 0)
            sb.append("?").append(parameters);
        uri = sb.toString();
        HttpRequestBase httpRequest = null;
        if (method.equals("GET")) {
            httpRequest = new HttpGet(uri);
        } else if (method.equals("PUT")) {
            httpRequest = new HttpPut(uri);
        } else if (method.equals("DELETE")) {
            httpRequest = new HttpDelete(uri);
        } else if (method.equals("HEAD")) {
            httpRequest = new HttpHead(uri);
        } else if (method.equals("POST")) {
            httpRequest = new HttpPost(uri);
        }
        Enumeration<?> e = request.getHeaderNames();
        String header = null;
        String value = null;
        while (e.hasMoreElements()) {
            header = (String) e.nextElement();
            value = request.getHeader(header);
            if (header.contains("x-ctyun"))
                continue;
            if (header.equalsIgnoreCase("host")) {
                continue;
            }
            if (header.equalsIgnoreCase("date")) {
                continue;
            }
            if (header.equalsIgnoreCase("Referer")) {
                continue;
            }
            httpRequest.addHeader(header, value);
        }
        httpRequest.addHeader("Referer", "http://" + portalDomain + "/");
        String date = TimeUtils.toGMTFormat(new Date());
        httpRequest.addHeader("Date", date);
        iamRequest.addHeader("Date", date);
        if (method.equals("POST")) {
            BasicHttpEntity entity = new BasicHttpEntity();
            entity.setContent(request.getInputStream());
            ((HttpPost) httpRequest).setEntity(entity);
        }
        String authorization = getAuthorization(iamRequest, request, method, null, null);
        if (authorization != null)
            httpRequest.addHeader("Authorization", authorization);
        return httpRequest;
    }
    private HttpRequestBase buildIAMV1Request(HttpServletRequest request) throws Exception {
        String method = request.getMethod();
        if (method == null)
            throw new BaseException(405, "Method Not Allowed");
        @SuppressWarnings("rawtypes")
        OOSRequest iamRequest = new OOSRequest(request);
        iamRequest.addParameter("proxyURL", request.getRequestURL().toString());
        String parameters = HttpUtils.encodeParameters(iamRequest);
        String uri;
        StringBuilder sb = new StringBuilder();
        sb.append("/");
        if (parameters != null && parameters.trim().length() != 0)
            sb.append("?").append(parameters);
        uri = sb.toString();
        HttpRequestBase httpRequest = null;
        if (method.equals("GET")) {
            httpRequest = new HttpGet(uri);
        } else if (method.equals("PUT")) {
            httpRequest = new HttpPut(uri);
        } else if (method.equals("DELETE")) {
            httpRequest = new HttpDelete(uri);
        } else if (method.equals("HEAD")) {
            httpRequest = new HttpHead(uri);
        } else if (method.equals("POST")) {
            httpRequest = new HttpPost(uri);
        }
        Enumeration<?> e = request.getHeaderNames();
        String header = null;
        String value = null;
        while (e.hasMoreElements()) {
            header = (String) e.nextElement();
            value = request.getHeader(header);
            if (header.contains("x-ctyun")) {
                iamRequest.getHeaders().remove(header);
                continue;
            }
            if (header.equalsIgnoreCase("host")) {
                continue;
            }
            if (header.equalsIgnoreCase("date")) {
                continue;
            }
//            if (header.equalsIgnoreCase("Referer")) {
//                continue;
//            }
            httpRequest.addHeader(header, value);
        }
//        httpRequest.addHeader("Referer", "http://" + "oos-cn.ctyun.cn" + "/");
        String dateTimeStamp =  SignerUtils.formatTimestamp(new Date().getTime());
        httpRequest.addHeader("Date", dateTimeStamp);
        iamRequest.addHeader("Date", dateTimeStamp);
        httpRequest.addHeader("Host", OOSConfig.getIAMDomainSuffix() + ":" + config.getInt("website.iamPort"));
        iamRequest.addHeader("Host", OOSConfig.getIAMDomainSuffix() + ":" + config.getInt("website.iamPort"));
        httpRequest.addHeader("x-amz-content-sha256", V4Signer.UNSIGNED_PAYLOAD);
        iamRequest.addHeader("x-amz-content-sha256", V4Signer.UNSIGNED_PAYLOAD);
        httpRequest.addHeader("OOS-PROXY-HOST", "oos-proxy");
        if (method.equals("POST")) {
            BasicHttpEntity entity = new BasicHttpEntity();
            entity.setContent(request.getInputStream());
            ((HttpPost) httpRequest).setEntity(entity);
        }
        String authorization = getAuthorizationIamSignV4(iamRequest, request, method, null, null, dateTimeStamp);
        if (authorization != null)
            httpRequest.addHeader("Authorization", authorization);
        
        httpRequest.addHeader(WebsiteHeader.CLIENT_IP, Utils.getIpAddr(request));
        return httpRequest;
    }
    private HttpRequestBase buildIAMInternalApiRequest(String interfacePath,HttpServletRequest request, String requestBody)
            throws Exception {
        String method = request.getMethod();
        if (method == null)
            throw new BaseException(405, "Method Not Allowed");
        @SuppressWarnings("rawtypes") OOSRequest iamRequest = new OOSRequest(request);
        String parameters = HttpUtils.encodeParameters(iamRequest);
        String uri;
        StringBuilder sb = new StringBuilder();
        sb.append("/internal/login");
        if (parameters != null && parameters.trim().length() != 0)
            sb.append("?").append(parameters);
        uri = sb.toString();
        HttpRequestBase httpRequest = null;
        if (method.equals("GET")) {
            httpRequest = new HttpGet(uri);
        } else if (method.equals("PUT")) {
            httpRequest = new org.apache.http.client.methods.HttpPut(uri);
        } else if (method.equals("DELETE")) {
            httpRequest = new HttpDelete(uri);
        } else if (method.equals("HEAD")) {
            httpRequest = new HttpHead(uri);
        } else if (method.equals("POST")) {
            httpRequest = new HttpPost(uri);
        }
        /*暂时注释共用签名，待IAM接口支持后对接
        Enumeration<?> e = request.getHeaderNames();
        String header = null;
        String value = null;
        while (e.hasMoreElements()) {
            header = (String) e.nextElement();
            value = request.getHeader(header);
            if (header.contains("x-ctyun"))
                continue;
            if (header.equalsIgnoreCase("host")) {
                continue;
            }
            if (header.equalsIgnoreCase("date")) {
                continue;
            }
            if (header.equalsIgnoreCase("Referer")) {
                continue;
            }
            httpRequest.addHeader(header, value);
        }
        httpRequest.addHeader("Referer", "http://" + portalDomain + "/");
        String date = TimeUtils.toGMTFormat(new Date());
        httpRequest.addHeader("Date", date);
        iamRequest.addHeader("Date", date);
         */
        if (method.equals("POST")) {
            byte[] paramBytes = requestBody.getBytes("utf-8");
            BasicHttpEntity entity = new BasicHttpEntity();
            entity.setContent(new ByteArrayInputStream(paramBytes));
            ((HttpPost) httpRequest).setEntity(entity);
        }
       /* 暂时注释共用签名，待IAM接口支持后对接
        String authorization = getAuthorization(iamRequest, request, method, null, null);
        if (authorization != null)
            httpRequest.addHeader("Authorization", authorization);
        */
        return httpRequest;
    }
    private OwnerMeta getOwner(HttpServletRequest req) throws Exception {
        String sessionId = Utils.getSessionId(req);
        String userName = session.getAndUpdate(sessionId);
        if (userName == null)
            return null;
        OwnerMeta owner = new OwnerMeta(userName);
        if (!metaClient.ownerSelect(owner))
            throw new BaseException(403, "InvalidAccessKeyId");
        return owner;
    }

    private String getAuthorization(com.amazonaws.Request<?> oosRequest, HttpServletRequest req,
            String method, String bucket, String key) throws Exception {
        OwnerMeta owner = getOwner(req);
        AkSkMeta asKey = new AkSkMeta(owner.getId());
        metaClient.akskSelectPrimaryKeyByOwnerId(asKey);
        String secret = asKey.getSecretKey();
        String canonicalString = RestUtils.makeS3CanonicalString(method,
                Utils.toResourcePath(bucket, key, true), oosRequest, null);
        String signature = Utils.sign(canonicalString, secret, SigningAlgorithm.HmacSHA1);
        String authorization = "AWS " + asKey.accessKey + ":" + signature;
        return authorization;
    }
   
    private String getAuthorizationIam(com.amazonaws.Request<?> oosRequest, HttpServletRequest req, String method,
            String bucket, String key) throws Exception {
        Pair<String, String> akSkPair = getAkSk(req);
        String ak = akSkPair.first();
            //根账户
        String sk = akSkPair.second();
        String canonicalString = RestUtils
                .makeS3CanonicalString(method, Utils.toResourcePath(bucket, key, true), oosRequest, null);
        String signature = Utils.sign(canonicalString, sk, SigningAlgorithm.HmacSHA1);
        String authorization = "AWS " + ak + ":" + signature;
        return authorization;
    }

    /**
     * 从HttpServletRequest请求当中找出当前用户有效的aksk(根账户优先查找内建ak、sk，子账户返回iam登录是的ak、sk)
     * @return Pair 1为ak，2为sk
     * @throws Exception
     */
    private Pair<String, String> getAkSk(HttpServletRequest req) throws Exception {
        Pair<String, String> aksk = new Pair();
        String sessionId = Utils.getSessionId(req);
        if (Portal.isIamUser(session, sessionId)) {
            Portal.IamAccount iamAccount = Portal.getIamAccount(session, sessionId);
            aksk.first(iamAccount.accessKeyId);
            aksk.second(iamAccount.secretAccessKey);
        } else {
            //根账户
            AkSkMeta asKey = Utils.getAvailableAk(getOwner(req));
            aksk.first(asKey.accessKey);
            aksk.second(asKey.getSecretKey());
        }
        return aksk;
    }
    private String getAuthorizationIamSignV4(com.amazonaws.Request<?> oosRequest,HttpServletRequest req, String method,
            String bucket, String key, String dateTimeStamp) throws Exception {
        Pair<String, String> akSkPair=  getAkSk(req);
        String ak = akSkPair.first();
        String sk = akSkPair.second();
        String iamEndpointUrl = "http://" + OOSConfig.getIAMDomainSuffix() + ":" + config.getInt("website.iamPort");
        URL url = new URL(iamEndpointUrl);
        String authorization = Signer.signV4Iam(oosRequest, ak, sk, url, method, dateTimeStamp, V4Signer.STS_SERVICE_NAME);
        return authorization;
    }

    private String getAPIHostFromOwnerDataRegion(HttpServletRequest req) throws Exception {
        OwnerMeta owner = getOwner(req);
        Pair<Set<String>, Set<String>> regions = metaClient.getRegions(owner.getId());
        if (regions.first().size() == 0 || regions.second().size() == 0)
            throw new BaseException("the user does not allocate the regions.", 403,
                    ErrorMessage.ERROR_CODE_INVALID_USER);
        String proxyRegion = DataRegion.getRegion().getName();
        Set<String> ownerDataRegions = regions.second();
        if (ownerDataRegions.contains(proxyRegion)) {
            return DataRegions.getRegionDataInfo(proxyRegion).getHost();
        } else {
            return DataRegions.getRegionDataInfo((String) ownerDataRegions.toArray()[0]).getHost();
        }
    }
    
    private void log(HttpServletRequest req) {
        log.info("request id:" + req.getAttribute(WebsiteHeader.REQUEST_ID) + " Http Method:"
                + req.getMethod() + " User-Agent:" + req.getHeader("User-Agent") + " RequestURI:"
                + req.getRequestURI() + " ip:" + Utils.getIpAddr(req));
        String k = null;
        String v = null;
        StringBuilder sb = new StringBuilder();
        sb.append("Headers:");
        Enumeration<?> e = req.getHeaderNames();
        while (e.hasMoreElements()) {
            k = (String) e.nextElement();
            v = req.getHeader(k);
            sb.append(k).append("=").append(v).append(" ");
        }
        log.info(sb.toString());
        sb.setLength(0);
        sb.append("Parameters:");
        e = req.getParameterNames();
        while (e.hasMoreElements()) {
            k = (String) e.nextElement();
            v = req.getParameter(k);
            sb.append(k).append("=").append(v).append(" ");
        }
        log.info(sb.toString());
    }
    
    public static class RequestParams {
        OwnerMeta owner;
        String beginDate;
        String endDate;
        String bucketName;
        String freq;

        public RequestParams(OwnerMeta owner, String beginDate, String endDate, String bucketName, String freq) {
            this.owner = owner;
            this.beginDate = beginDate;
            this.endDate = endDate;
            this.bucketName = bucketName;
            this.freq = freq;
        }
    }
}

public class Proxy implements Program {
    static {
        System.setProperty("log4j.log.app", "proxy");
    }
    private static final Log log = LogFactory.getLog(Proxy.class);
    
    public static void main(String[] args) throws Exception {
        new Proxy().exec(args);
    }
    
    @Override
    public String usage() {
        return "Usage: \n";
    }
    
    @Override
    public void exec(String[] args) throws Exception {
        System.setProperty("bufferPool.maxSize", String.valueOf(OOSConfig.getProxyBufferSize()));
        GetOpt opts = new GetOpt("[p]:[sslp]:", args);
        int port = opts.getInt("p", 9098);
        int sslPort = opts.getInt("sslp", 9461);
        CompositeConfiguration config = null;
        File[] xmlConfs = { new File(System.getenv("OOS_HOME") + "/conf/oos-online.xml"),
                new File(System.getenv("OOS_HOME") + "/conf/oos.xml") };
        config = (CompositeConfiguration) ConfigUtils.loadXmlConfig(xmlConfs);
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
        JettyServer.ostorId = properties.getProperty("ostorId");
        if (JettyServer.ostorId == null)
            throw new RuntimeException("no ostorId");
        DSyncService dsync = new DSyncService(GlobalHHZConfig.getQuorumServers(),
                GlobalHHZConfig.getSessionTimeout());
        try {
            DLock lock = dsync.createLock(OosZKNode.proxyLock, null);
            lock.lock();
            try {
                QueuedThreadPool pool = new QueuedThreadPool();
                int maxThreads = OOSConfig.getMaxThreads();
                pool.setMaxQueued(maxThreads);
                pool.setMaxThreads(maxThreads);
                QueuedThreadPool sslPool = new QueuedThreadPool();
                sslPool.setMaxQueued(maxThreads);
                sslPool.setMaxThreads(maxThreads);
                SelectChannelConnector connector0 = new SelectChannelConnector();
                connector0.setAcceptQueueSize(OOSConfig.getAcceptQueueSize());// 增加backlog
                connector0.setThreadPool(pool);
                connector0.setPort(port);
                connector0.setMaxIdleTime(OOSConfig.getSocketlIdleTimeout());
                connector0.setRequestHeaderSize(Consts.CONNECTOR_REQUEST_HEADER_SIZE);
                SslSelectChannelConnector ssl_connector = new SslSelectChannelConnector();
                ssl_connector.setPort(sslPort);
                ssl_connector.setMaxIdleTime(OOSConfig.getSocketlIdleTimeout());
                ssl_connector.setRequestHeaderSize(Consts.CONNECTOR_REQUEST_HEADER_SIZE);
                ssl_connector.setAcceptQueueSize(OOSConfig.getAcceptQueueSize());// 增加backlog
                ssl_connector.setThreadPool(sslPool);
                SslContextFactory cf = ssl_connector.getSslContextFactory();
                cf.setKeyStorePath(System.getenv("OOS_HOME")
                        + config.getString("website.websiteKeyStore"));
                cf.setKeyStorePassword(config.getString("website.websiteSslPasswd"));
                Server server = new Server();
                server.setThreadPool(new QueuedThreadPool(OOSConfig.getServerThreadPool())); // 增加最大线程数
                server.setConnectors(new Connector[] { connector0, ssl_connector });
                server.setHandler(new HttpHandler(config));
                MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
                MBeanContainer mBeanContainer = new MBeanContainer(mBeanServer);
                server.getContainer().addEventListener(mBeanContainer);
                mBeanContainer.start();

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
            } finally {
                lock.unlock();
            }
        } finally {
            dsync.close();
        }
    }
}
