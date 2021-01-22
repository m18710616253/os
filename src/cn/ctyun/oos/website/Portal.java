package cn.ctyun.oos.website;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.sql.SQLException;
import java.text.ParseException;
import java.time.LocalDate;
import java.time.temporal.TemporalAdjusters;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import javax.imageio.ImageIO;
import javax.imageio.stream.ImageOutputStream;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.client.ClientProtocolException;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.amazonaws.HttpMethod;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.Headers;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.internal.ServiceUtils;
import com.amazonaws.services.s3.internal.XmlWriter;
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest;
import com.google.common.collect.Lists;

import cn.ctyun.common.BaseException;
import cn.ctyun.common.Consts;
import cn.ctyun.common.Session;
import cn.ctyun.common.conf.OOSConfig;
import cn.ctyun.common.region.DataRegions;
import cn.ctyun.common.utils.XSSUtils;
import cn.ctyun.oos.bssAdapter.server.BssAdapterClient;
import cn.ctyun.oos.bssAdapter.server.SourceTYPE;
import cn.ctyun.oos.bssAdapter.server.UpdateType;
import cn.ctyun.oos.common.Email;
import cn.ctyun.oos.common.ErrorMessage;
import cn.ctyun.oos.common.Parameters;
import cn.ctyun.oos.common.SocketEmail;
import cn.ctyun.oos.common.Utils;
import cn.ctyun.oos.common.WebsiteHeader;
import cn.ctyun.oos.hbase.MetaClient;
import cn.ctyun.oos.metadata.BucketMeta;
import cn.ctyun.oos.metadata.MinutesUsageMeta;
import cn.ctyun.oos.metadata.MinutesUsageMeta.UsageMetaType;
import cn.ctyun.oos.metadata.ObjectMeta;
import cn.ctyun.oos.metadata.OwnerMeta;
import cn.ctyun.oos.server.conf.BssAdapterConfig;
import cn.ctyun.oos.server.conf.BssAdapterConfig.Pool;
import cn.ctyun.oos.server.conf.PeriodUsageStatsConfig;
import cn.ctyun.oos.server.conf.SynchroConfig;
import cn.ctyun.oos.server.db.DB;
import cn.ctyun.oos.server.db.DbInitialAccount;
import cn.ctyun.oos.server.db.dbpay.DBPay;
import cn.ctyun.oos.server.db.dbpay.DbAccount;
import cn.ctyun.oos.server.db.dbpay.DbBill;
import cn.ctyun.oos.server.db.dbprice.DBPrice;
import cn.ctyun.oos.server.db.dbprice.DbPackage;
import cn.ctyun.oos.server.db.dbprice.DbUserPackage;
import cn.ctyun.oos.server.globalPortal.OpUser;
import cn.ctyun.oos.server.pay.BestPay;
import cn.ctyun.oos.server.pay.Bill;
import cn.ctyun.oos.server.usage.UsageResult;
import cn.ctyun.oos.server.util.DateParser;
import cn.ctyun.oos.server.util.Misc;
import common.time.TimeUtils;
import common.tuple.Pair;
import common.tuple.Triple;
import common.util.VerifyImage;

/**
 * @author: Cui Meng
 */
public class Portal {
    private static final Log log = LogFactory.getLog(Portal.class);
    private static MetaClient client = MetaClient.getGlobalClient();
    private static final String REGION_MAP_FILEPATH = System.getenv("OOS_HOME") + "/conf/regionmap/regionMap.conf";
    private static ConcurrentHashMap<String, AtomicInteger> sendEmailTimes = new ConcurrentHashMap<String, AtomicInteger>();
    private static ConcurrentHashMap<String, ReentrantLock> sessionRefreshLock = new ConcurrentHashMap<String, ReentrantLock>();
    private static final String SESSION_REFRESH_KEY_PREFIX = "session/refresh/";
    private static final ExecutorService simpleCachedThreadPool = Executors.newCachedThreadPool();

    public static void writeXmlResponseEntity(HttpServletResponse resp, String body)
            throws IOException {
        resp.setCharacterEncoding(Consts.STR_UTF8);
        if (body != null) {
            resp.setIntHeader(Headers.CONTENT_LENGTH,
                    body.getBytes(Consts.CS_UTF8).length);
            resp.setHeader(Headers.CONTENT_TYPE, "application/xml");
            try {
                resp.getOutputStream().write(body.getBytes(Consts.STR_UTF8));
            } catch (Exception e) {
                log.error("catch write response error");
            }
        } else
            resp.setIntHeader(Headers.CONTENT_LENGTH, 0);
    }
    public static void writeJsonResponseEntity(HttpServletResponse resp, String body)
            throws IOException {
        resp.setCharacterEncoding(Consts.STR_UTF8);
        if (body != null) {
            resp.setIntHeader(Headers.CONTENT_LENGTH,
                    body.getBytes(Consts.CS_UTF8).length);
            resp.setHeader(Headers.CONTENT_TYPE, "application/json");
            try {
                resp.getOutputStream().write(body.getBytes(Consts.STR_UTF8));
            } catch (Exception e) {
                log.error("catch write response error");
            }
        } else
            resp.setIntHeader(Headers.CONTENT_LENGTH, 0);
    }
    public static void verifyEmail(HttpServletRequest req, HttpServletResponse resp)
            throws Exception {
        OwnerMeta owner = new OwnerMeta(req.getParameter(Parameters.EMAIL));
        
        if (client.ownerSelect(owner))
            throw new BaseException(400, "UserAlreadyExist");
        //非中心资源池向全局进行确认
        if (!BssAdapterConfig.localPool.name.equals(BssAdapterConfig.name)) {
            if (BssAdapterClient.ownerSelect(owner, BssAdapterConfig.localPool.ak , 
                    BssAdapterConfig.localPool.getSK(),null)) {
                throw new BaseException(400, "UserAlreadyExist");
            }
        }
    }

    public static boolean isValidSession(String sessionId, Session<String, String> session,
            String portalSessionId) throws ClientProtocolException, IOException {
        String userName = session.getAndUpdate(sessionId);
        if (userName == null)
            return false;
//        session.update(sessionId, userName);
        // if (loginFromGlobalPortal)
        // OpUser.updateSession(portalSessionId);
        return true;
    }
    
    
    public static void login(HttpServletRequest req, HttpServletResponse resp,
            Session<String, String> session, OwnerMeta owner) throws Exception {
        if (!req.getMethod().equalsIgnoreCase(HttpMethod.POST.toString())) {
            throw new BaseException(403, "InvalidLoginRequest");
        }

        String email = req.getParameter(Parameters.EMAIL);
        String password = req.getParameter(Parameters.PASSWORD);
        String userName = email;
        if (userName == null || password == null)
            throw new BaseException();
        owner.name = userName;
        owner.setId(owner.getIdByName(userName));
        if (!client.ownerSelect(owner)) {
            //尝试从中心资源池拉取用户信息注册到本地
            downloadOwnerFromBssAdapter(owner);
        }
        if (!BssAdapterClient.containsRegion(owner.getId(), BssAdapterConfig.localPool.name)) {
            throw new BaseException(403, "UnaccessibleRegion");
        }
        String verifyCode = req.getParameter(Parameters.VERIFY_CODE);
        if (verifyCode == null || verifyCode.trim().length() == 0
                || !verifyCode.equalsIgnoreCase(session.getAndUpdate(req.getParameter(Parameters.IMAGE_SESSIONID))))
            throw new BaseException(400, "InvalidVerifyCode");
        if (!Misc.getMd5(owner.getPwd()).equals(password))
            throw new BaseException(403, "InvalidPassword");
        if (owner.verify != null)
            throw new BaseException(403, "NotVerifyEmail");
        String sessionId = generateSession(req, resp, session, userName, owner.displayName);
        // 删除image sessionId，避免客户端重复使用同一个sessionid
        session.remove(req.getParameter(Parameters.IMAGE_SESSIONID));
        saveLastLoginInfo(session,sessionId);
        client.getOrCreateBuiltInAK(owner);
        String requestIpAddr = Utils.getIpAddr(req);
        updateUserInfo(owner, requestIpAddr, System.currentTimeMillis());
        // 私有资源池不同步登录信息
        if (!SynchroConfig.propertyLocalPool.synchroLogin)
            return;

        // 资源池单点登录后，广播此用户session到其他资源池以便免密登录；
        BssAdapterClient.regsterUserSessionId(userName, password, sessionId, requestIpAddr,BssAdapterConfig.localPool.ak,
                BssAdapterConfig.localPool.getSK());
    }
    
    public static String loginIam(HttpServletRequest req, HttpServletResponse resp, Session<String, String> session,
            CompositeConfiguration config, OwnerMeta owner) throws Exception {
        if (!req.getMethod().equalsIgnoreCase(HttpMethod.POST.toString())) {
            throw new BaseException(403, "InvalidLoginRequest");
        }

        String accountId = req.getParameter(Parameters.USER_IAM_ACCOUNTID);
        String userName = req.getParameter(Parameters.USER_NAME);
        String passwordMd5 = req.getParameter(Parameters.PASSWORD);
        String mfaCode = req.getParameter(Parameters.MFA_CODE);
        if (accountId == null || userName == null || passwordMd5 == null)
            throw new BaseException(403, "InvalidLoginRequest");
        owner.copy(getOwnerByIamAccountId(accountId));
        owner.setId(owner.getIdByName(owner.name));
        if (empty(mfaCode)) {//首次采用账户密码登陆时需要校验验证码
            String verifyCode = req.getParameter(Parameters.VERIFY_CODE);
            if (verifyCode == null || !verifyCode
                    .equalsIgnoreCase(session.getAndUpdate(req.getParameter(Parameters.IMAGE_SESSIONID))))
                throw new BaseException(400, "InvalidVerifyCode");
            session.remove(req.getParameter(Parameters.IMAGE_SESSIONID));
        }
        String requestIpAddr = Utils.getIpAddr(req);
        JSONObject param = new JSONObject();
        param.put("accountId", accountId);
        param.put("userName", userName);
        param.put("passwordMd5", passwordMd5);
        if (!empty(mfaCode))
            param.put("mFACode", mfaCode);
        param.put("loginIp", requestIpAddr);     

        URL url = new URL("http", OOSConfig.getIAMDomainSuffix(), config.getInt("website.iamPort"), "/internal/login");
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        String date = TimeUtils.toGMTFormat(new Date());
        //        String resourcePath = Utils.toResourcePath(null, null, false);
        conn.setRequestMethod("POST");
        conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
        conn.setRequestProperty("Date", date);
        conn.setRequestProperty("Host",
                config.getString("website.portalDomain") + ":" + config.getInt("website.iamPort"));
        conn.setRequestProperty("Connection", "close");
        conn.setRequestProperty("x-forwarded-for", requestIpAddr);
        //        @SuppressWarnings("unchecked")
        //        String canonicalString = RestUtils.makeS3CanonicalString(
        //                conn.getRequestMethod(), resourcePath, new OOSRequest(conn),null);
        //        String signature = Utils.sign(canonicalString, sk,
        //                SigningAlgorithm.HmacSHA1);
        //        String authorization = "AWS " + ak + ":" + signature;
        //        conn.setRequestProperty("Authorization", authorization);
        conn.setUseCaches(false);
        conn.setDoInput(true);
        conn.setDoOutput(true);
        conn.connect();
        try (OutputStream out = conn.getOutputStream()) {
            out.write(param.toString().getBytes("utf-8"));
            out.flush();
        }
        int code = conn.getResponseCode();
        InputStream ip = null;
        try {
            resp.setStatus(code);
            if (code == 200) {
                ip = conn.getInputStream();
                String respStr = IOUtils.toString(ip, Consts.CS_UTF8);
                System.out.println(respStr);
                Portal.generateIamUserSession(req, resp, session, accountId, userName, respStr, owner);
                // 删除image sessionId，避免客户端重复使用同一个sessionid
                return respStr;
            } else {
                ip = conn.getErrorStream();
                String respStr = IOUtils.toString(ip, Consts.CS_UTF8);
                writeJsonResponseEntity(resp, respStr);
                return respStr;
            }
        } finally {
            if (ip != null) {
                ip.close();
            }
        }
    }
    public static void checkMFACode(HttpServletRequest req, HttpServletResponse resp, Session<String, String> session,
            CompositeConfiguration config) throws Exception {
        if (!req.getMethod().equalsIgnoreCase(HttpMethod.POST.toString())) {
            throw new BaseException(403, "InvalidLoginRequest");
        }
        String sessionId = Utils.getSessionId(req);
        IamAccount iamAccount = getIamAccount(session, sessionId);
        String accountId = iamAccount.accountId;
        String userName = iamAccount.userName;
        String mfaCode = req.getParameter(Parameters.MFA_CODE);
        if (mfaCode == null)
            throw new BaseException(400, "InvalidArgument");

        JSONObject param = new JSONObject();
        param.put("accountId", accountId);
        param.put("userName", userName);
        param.put("mFACode", mfaCode);
        URL url = new URL("http", OOSConfig.getIAMDomainSuffix(), config.getInt("website.iamPort"),
                "/internal/checkCode");
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        String date = TimeUtils.toGMTFormat(new Date());
        //        String resourcePath = Utils.toResourcePath(null, null, false);
        conn.setRequestMethod("POST");
        conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
        conn.setRequestProperty("Date", date);
        conn.setRequestProperty("Host",
                config.getString("website.portalDomain") + ":" + config.getInt("website.iamPort"));
        conn.setRequestProperty("Connection", "close");
        //        @SuppressWarnings("unchecked")
        //        String canonicalString = RestUtils.makeS3CanonicalString(
        //                conn.getRequestMethod(), resourcePath, new OOSRequest(conn),null);
        //        String signature = Utils.sign(canonicalString, sk,
        //                SigningAlgorithm.HmacSHA1);
        //        String authorization = "AWS " + ak + ":" + signature;
        //        conn.setRequestProperty("Authorization", authorization);
        conn.setUseCaches(false);
        conn.setDoInput(true);
        conn.setDoOutput(true);
        conn.connect();
        try (OutputStream out = conn.getOutputStream()) {
            out.write(param.toString().getBytes("utf-8"));
            out.flush();
        }
        int code = conn.getResponseCode();
        InputStream ip = null;
        try {
            resp.setStatus(code);
            if (code == 200) {
                ip = conn.getInputStream();
                String respStr = IOUtils.toString(ip, Consts.CS_UTF8);
                JSONObject respJson=new JSONObject(respStr);
                if (respJson.getBoolean("multiFactorAuthPresent")) {
                refreshIamLoginTime(session, sessionId);
                }
                writeJsonResponseEntity(resp, respStr);
            } else {
                ip = conn.getErrorStream();
                String respStr = IOUtils.toString(ip, Consts.CS_UTF8);
                writeJsonResponseEntity(resp, respStr);
            }
        } finally {
            if (ip != null) {
                ip.close();
            }
        }

    }
    /* 此方法用于原先电商用户登录时候的登录过程
     * BSS单点登录后，对接过来的用户如果OOS没有注册，需要返回bss登录界面进行工单注册处理，所以此函数
     * 暂时不用；
     */
    public static void loginFromGlobalPortal(HttpServletRequest req, HttpServletResponse resp,
            Session<String, String> session) throws Exception {
        String accountId = req.getParameter(WebsiteHeader.ACCOUNTID);
        String portalSessionId = req.getParameter(WebsiteHeader.GLOBAL_PORTAL_SESSIONID);
        String userId = req.getParameter(WebsiteHeader.USER_ID);
        if (accountId == null || portalSessionId == null || userId == null)
            throw new BaseException();
        OwnerMeta owner = OpUser.getOwnerInfo(accountId);
        if (owner == null)
            throw new BaseException(400, "NoSuchUserInPortal");
        DB db = DB.getInstance();
        if (!client.ownerSelect(owner)) {
            owner = OpUser.getOwnerInfo(accountId);
            if (owner.type == OwnerMeta.TYPE_ZHQ) {
                owner.verify = null;
                owner.credit = -Long.MAX_VALUE;
                owner.maxAKNum = Consts.MAX_AK_NUM;
                DbInitialAccount initialAccount = new DbInitialAccount(accountId);
                initialAccount.userId = userId;
                initialAccount.orderId = "";
                initialAccount.ownerId = owner.getId();
                client.ownerInsert(owner);
                db.initialAccountInsert(initialAccount);
            } else
                throw new BaseException(403, "InvalidAccessKeyId");
        }
        OpUser.querySession(portalSessionId, accountId);
        generateSession(req, resp, session, owner.getName(), owner.displayName);
    }
    
    public static String generateSession(HttpServletRequest req, HttpServletResponse resp,
            Session<String, String> session, String userName, String displayName) throws BaseException {
        String sessionId = UUID.randomUUID().toString();
        session.update(sessionId, userName);
        try {
            resp.getWriter().write("<SessionId>" + sessionId + "</SessionId>");
            resp.setHeader(WebsiteHeader.LOGIN_SESSIONID, sessionId);
            resp.setHeader(WebsiteHeader.LOGIN_USER, userName);
        } catch (IOException e) {
            log.error(e.getMessage(), e);
            throw new BaseException(400,"SetSessionFailed");
        }
        resp.setStatus(200);
        log.info("user: " + userName + " login success, sessionId is:" + sessionId);
        //Cookie cookie = new Cookie(WebsiteHeader.PORTAL_COOKIE, sessionId);
        resp.addCookie(new Cookie(WebsiteHeader.LOGIN_SESSIONID, sessionId));
        resp.addCookie(new Cookie(WebsiteHeader.LOGIN_USER, displayName));
        //resp.addCookie(cookie);
        return sessionId;
    }
    
    public static String generatePortalUserSession(HttpServletRequest req, HttpServletResponse resp,
            Session<String, String> session, String userName, String ticket) throws BaseException {
        String sessionId = UUID.randomUUID().toString();
        try {
            session.update(sessionId, userName);
            session.update(ticket, sessionId);
            
            resp.getWriter().write("<SessionId>" + sessionId + "</SessionId>");
            resp.setHeader(WebsiteHeader.LOGIN_SESSIONID, sessionId);
            resp.setHeader(WebsiteHeader.LOGIN_USER, userName);
        } catch (IOException e) {
            log.error(e.getMessage(), e);
            throw new BaseException(400,"SetSessionFailed");
        }
        resp.setStatus(200);
        log.info("portal user: " + userName + " login success, sessionId is:" + sessionId + ",ticket is:"+ticket);
        Cookie cookie = new Cookie(WebsiteHeader.LOGIN_SESSIONID, sessionId);
        resp.addCookie(cookie);
        return sessionId;
    }

    /**
     * 刷新中心池的session过期时间(保证在同一个用户并发多次获取session请求当中，在一个刷新周期内只有一个能够调用刷新中心池任务)
     */
    public static void globalSessionRefresh(String sessionId, Session<String, String> session, long sessionRefreshInterval) throws IOException, BaseException {
        long lastRefresh = Portal.getLastGlobalSessionRefreshTime(session, sessionId);
        if ((lastRefresh > sessionRefreshInterval) || lastRefresh == -1) {//到期需要刷新中心池Session
            String lockKey = SESSION_REFRESH_KEY_PREFIX + sessionId;
            //保证session锁已创建
            synchronized (lockKey.intern()) {
                if (!sessionRefreshLock.containsKey(lockKey)) {
                    sessionRefreshLock.put(lockKey, new ReentrantLock());
                }
            }
            //已有相同sessionId用户创建锁
            ReentrantLock lock = sessionRefreshLock.get(lockKey);
            if (lock != null && lock.tryLock()) {
                boolean result = false;//同步session状态
                try {
                    Callable<Boolean> refreshCall = () -> BssAdapterClient.refreshUserSessionExpTime(sessionId, BssAdapterConfig.localPool.ak, BssAdapterConfig.localPool.getSK());
                    Future<Boolean> future = simpleCachedThreadPool.submit(refreshCall);
                    result = future.get(BssAdapterConfig.connectTimeout, TimeUnit.SECONDS);
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                } finally {
                    Portal.setGlobalSessionRefreshTime(session, sessionId);
                    //不解锁,直接删除lock
                    sessionRefreshLock.remove(lockKey);
                }

            }
        }
    }
    /**
     * 设置上次刷新中心池session的时间
     */
    public static void setGlobalSessionRefreshTime(Session<String, String> session, String sessionId) {
        session.update(sessionId + "|" + WebsiteHeader.Last_Global_Session_Refresh,
                Long.toString(System.currentTimeMillis()));
    }

    /**
     * 获取上次刷新中心池session的间隔时间, 返回-1表明从未同步过
     */
    public static long getLastGlobalSessionRefreshTime(Session<String, String> session, String sessionId) {
        String lastRefresh=session.get(sessionId + "|" + WebsiteHeader.Last_Global_Session_Refresh);
        return StringUtils.isNotEmpty(lastRefresh) ? (System.currentTimeMillis() - Long.parseLong(lastRefresh)) : -1;
    }

    /**
     * 刷新iam账户登录时间
     */
    public static void refreshIamLoginTime(Session<String, String> session, String sessionId) {
        session.update(sessionId + "|iam|" + WebsiteHeader.IAM_MULTI_FACTOR_AUTH_AGE,
                Long.toString(System.currentTimeMillis()));
    }
    public static String generateIamUserSession(HttpServletRequest req, HttpServletResponse resp,
            Session<String, String> session, String accountId, String userName, String respStr, OwnerMeta owner)
            throws Exception {
        //接口格式{"passwordExpired":false,"mFACodeRequired":false,"passwordResetRequired":false,"hardExpiry":false,"multiFactorAuthPresent":false,"accountId":"","accessKeyId":"","secretAccessKey":""}
        String sessionId = UUID.randomUUID().toString();
        try {
            JSONObject iamUserJsn = new JSONObject(respStr);
            //根据iam mfa接口逻辑，返回ak表示登录成功，否则只返回账户非敏感信息不做登录逻辑
            if (iamUserJsn.has("accessKeyId")) {
                session.update(sessionId, owner.name);
                iamUserJsn.put("userName", userName);
                session.update(sessionId + "|iam", iamUserJsn.toString());
                refreshIamLoginTime(session, sessionId);
                long passwordLastUsed = -1;
                String iPLastUsed = "";
                if (iamUserJsn.has("passwordLastUsed")) {
                    passwordLastUsed = iamUserJsn.getLong("passwordLastUsed");
                }
                if (iamUserJsn.has("iPLastUsed")) {
                    iPLastUsed = iamUserJsn.getString("iPLastUsed");
                }
                saveLastLoginInfo(session, sessionId, passwordLastUsed, iPLastUsed);
                //必须清理敏感信息
                iamUserJsn.remove("accessKeyId");
                iamUserJsn.remove("secretAccessKey");
                resp.setHeader(WebsiteHeader.LOGIN_SESSIONID, sessionId);
                resp.setHeader(WebsiteHeader.LOGIN_USER, owner.name);
                resp.setHeader(WebsiteHeader.LOGIN_IAM_USER_NAME, userName);
                resp.setHeader(WebsiteHeader.LOGIN_USER_TYPE, "IAM");
                resp.getWriter().write(iamUserJsn.toString());
            }else {
                resp.getWriter().write(iamUserJsn.toString());
            }

        } catch (IOException e) {
            log.error(e.getMessage(), e);
            throw new BaseException(400, "SetSessionFailed");
        } resp.setStatus(200);
        //        log.info("portal user: " + userName + " login success, sessionId is:" + sessionId + ",ticket is:"+ticket);
        Cookie cookie = new Cookie(WebsiteHeader.LOGIN_SESSIONID, sessionId);
        resp.addCookie(cookie);
        return sessionId;
    }
    public static String getIamAccountId(long ownerId) {
        String accountId = Long.toUnsignedString(ownerId, 36);
        accountId = String.format("%13s", accountId).replaceAll(" ", "0");
        return accountId;
    }
    public static class IamAccount {
        public String accountId;
        public String userName;
        public String accessKeyId;
        public String secretAccessKey;
        public boolean mFACodeRequired = false;
        public boolean multiFactorAuthPresent = false;
        public IamAccount(JSONObject iamAccount) throws JSONException {
            this.accountId = iamAccount.getString("accountId");
            this.userName = iamAccount.getString("userName");
            this.accessKeyId = iamAccount.getString("accessKeyId");
            this.secretAccessKey = iamAccount.getString("secretAccessKey");
            this.mFACodeRequired = iamAccount.getBoolean("mFACodeRequired");
            this.multiFactorAuthPresent = iamAccount.getBoolean("multiFactorAuthPresent");
        }
    }
    public static IamAccount getIamAccount(Session<String, String> session, String sessionId) throws Exception {
        try {
            return new IamAccount(new JSONObject(session.getAndUpdate(sessionId + "|iam")));
        } catch (JSONException e) {
            log.error(e.getMessage(), e);
            throw new BaseException(500, "Invalid Iam Session");
        }
    }
    public static long getOwnerId(String iamAccountId) {
        return Long.parseUnsignedLong(iamAccountId, 36);
    }
    public static OwnerMeta getOwnerByIamAccountId(String iamAccountId) throws Exception {
        OwnerMeta owner = new OwnerMeta(getOwnerId(iamAccountId));
        if (!client.ownerSelectById(owner)) {
            throw new BaseException(400, "UserNotExist");
        }
        return owner;
    }
    public static boolean isIamUser(Session<String, String> session, String sessionId) {
        if (session.getAndUpdate(sessionId + "|iam") != null) {
            return true;
        } else {
            return false;
        }
    }
    
    public static boolean isIamMultiFactorAuthPresent(Session<String, String> session, String sessionId)
            throws Exception {
        IamAccount iamAccount = getIamAccount(session, sessionId);
        return iamAccount.multiFactorAuthPresent;
    }
    /**
     * 如果iam子账户绑定成功或者用户解绑(解绑分为解绑设备和删除设备2次操作，将删除设备作为解绑成功的标志)mfa设备时，则刷新当前登录账户session内的mfa状态
     */
    public static void refreshMFAStatus(Session<String, String> session, HttpServletRequest request, int statusCode)
            throws Exception {
        if (statusCode == 200 && request.getHeader(WebsiteHeader.IAM_ENABLE_MFA_DEVICE) != null) {
            String sessionId = Utils.getSessionId(request);
            JSONObject iamAccountJson = new JSONObject(session.getAndUpdate(sessionId + "|iam"));
            iamAccountJson.put("multiFactorAuthPresent", true);
            session.update(sessionId + "|iam", iamAccountJson.toString());
            refreshIamLoginTime(session, sessionId);
        }
        if (statusCode == 200 && request.getHeader(WebsiteHeader.IAM_DELETE_VIRTUAL_MFA_DEVICE) != null) {
            String sessionId = Utils.getSessionId(request);
            JSONObject iamAccountJson = new JSONObject(session.getAndUpdate(sessionId + "|iam"));
            iamAccountJson.put("multiFactorAuthPresent", false);
            session.update(sessionId + "|iam", iamAccountJson.toString());
        }
    }
    public static String buildIamCheckMFACodeBody(HttpServletRequest req, Session<String, String> session, String sessionId)
            throws Exception {
        String mFACode = req.getParameter("mFACode ");
        IamAccount iam = getIamAccount(session, sessionId);
        JSONObject jsnData = new JSONObject();
        jsnData.put("accountId",iam.accountId);
        jsnData.put("userName",iam.userName);
        jsnData.put("mFACode",mFACode);
        return jsnData.toString();
    }
    public static boolean empty(String val) {
        if (val == null || val.length() == 0) {
            return true;
        } else {
            return false;
        }
    }
    public static void logout(HttpServletRequest req, HttpServletResponse resp,
            Session<String, String> session) throws BaseException, SQLException,
            ClientProtocolException, IOException {
        String sessionId = Utils.getSessionId(req);
        log.info("user: " + session.getAndUpdate(sessionId) + " logout success, sessionId is:" + sessionId);
        session.remove(sessionId);
		session.remove(sessionId + "|loginInfo");
        BssAdapterClient.removeUserSessionId(sessionId,BssAdapterConfig.localPool.ak,
                BssAdapterConfig.localPool.getSK(),Utils.getIpAddr(req));
        resp.setStatus(200);
    }
    public static void logoutIam(HttpServletRequest req, HttpServletResponse resp,
            Session<String, String> session) throws BaseException, SQLException,
            ClientProtocolException, IOException {
        String sessionId = Utils.getSessionId(req);
        log.info("user: " + session.getAndUpdate(sessionId) + " logout success, sessionId is:" + sessionId);
        session.remove(sessionId);
        session.remove(sessionId + "|loginInfo");     
        session.remove(sessionId + "|iam");
        session.remove(sessionId + "|iam|" + WebsiteHeader.IAM_MULTI_FACTOR_AUTH_AGE);
        resp.setStatus(200);
    }
    
    public static boolean isLengthRight(String para, int minLength, int maxLength) {
        if (para == null)
            return false;
        if (para.length() == 0 || para.length() > maxLength || para.length() < minLength)
            return false;
        return true;
    }
    
    public static boolean isLengthRight(String para, int maxLength)
            throws UnsupportedEncodingException {
        if (para == null)
            return true;
        if (para.equals(""))
            return true;
        if (para.length() > maxLength)
            return false;
        return true;
    }

    /**
     * 尝试从中心资源池拉取用户信息注册到本地,如果抓取失败则抛出 403 InvalidAccessKeyId异常
     * @param owner
     * @throws Exception
     */
    private static void downloadOwnerFromBssAdapter(OwnerMeta owner) throws Exception {
        DbInitialAccount dbInitialAccount = new DbInitialAccount();
        if (BssAdapterClient.ownerSelect(owner, BssAdapterConfig.localPool.ak ,
                BssAdapterConfig.localPool.getSK(),dbInitialAccount)) {
            if (dbInitialAccount.accountId != null && !dbInitialAccount.accountId.equals("")) { //BSS用户
                OpUser.registerUser(dbInitialAccount.accountId,dbInitialAccount.userId,dbInitialAccount.orderId, owner);
            } else {//普通用户
                client.ownerInsert(owner);
            }
        } else {
            throw new BaseException(403, "InvalidAccessKeyId");
        }
    }

    public static OwnerMeta createOwnerFromRequest(HttpServletRequest req, int registerType
            ,int defaultCredit) throws Exception {
        String email = URLDecoder.decode(req.getParameter(Parameters.EMAIL),
                Consts.STR_UTF8);
        String userName = email;
        String password = req.getParameter(Parameters.PASSWORD);
        OwnerMeta user = new OwnerMeta(userName);
        
        String displayName = "";
        if (req.getParameter(Parameters.DISPLAY_NAME) != null)
        displayName = URLDecoder.decode(req.getParameter(Parameters.DISPLAY_NAME),
                Consts.STR_UTF8);
        String companyName = "";
        if (req.getParameter(Parameters.COMPANY_NAME) != null)
        companyName = URLDecoder.decode(req.getParameter(Parameters.COMPANY_NAME),
                Consts.STR_UTF8);
        String companyAddress = "";
        if (req.getParameter(Parameters.COMPANY_ADDRESS) != null)
        companyAddress = URLDecoder.decode(req.getParameter(Parameters.COMPANY_ADDRESS),
                Consts.STR_UTF8);
        String mobilePhone = "";
        if (req.getParameter(Parameters.MOBILE_PHONE) != null)
        mobilePhone = req.getParameter(Parameters.MOBILE_PHONE);
        String phone = "";
        if (req.getParameter(Parameters.PHONE) != null)
        phone = URLDecoder.decode(req.getParameter(Parameters.PHONE), Consts.STR_UTF8);
        if (!Utils.isValidPassword(password)) {
            throw new BaseException(400, "InvalidPassword");
        } 
        
        if (!Utils.isValidEmail(email)) {
            throw new BaseException(400, "InvalidUserEmail");
        }
        
        if (!Portal.isLengthRight(email, Consts.EMAIL_MIN_LENGTH, Consts.EMAIL_MAX_LENGTH)
            || !Portal.isLengthRight(userName, Consts.EMAIL_MIN_LENGTH, Consts.EMAIL_MAX_LENGTH)
            || !Portal.isLengthRight(password, Consts.PASSWORD_MIN_LENGTH,Consts.PASSWORD_MAX_LENGTH))
        throw new BaseException(400, "InvalidLength");
        
        user.displayName = XSSUtils.clearXss(displayName);
        user.setPwd(password);
        user.email = XSSUtils.clearXss(email);
        user.companyName = XSSUtils.clearXss(companyName);
        user.companyAddress = XSSUtils.clearXss(companyAddress);
        user.mobilePhone = mobilePhone;
        user.phone = phone;
        user.maxAKNum = Consts.MAX_AK_NUM;
        checkUserInfo(user);
        if (registerType == Consts.REGISTER_FROM_OOS) {
            user.verify = UUID.randomUUID().toString();
            user.credit = defaultCredit;
            user.type = OwnerMeta.TYPE_OOS;
        }
        
        return user;
    }
    
    public static String register(HttpServletRequest req, HttpServletResponse resp,
            Session<String, String> session, Email emailContent, String body, int registerType,
            Email emailInner, int defaultCredit, boolean isPublicCloud) throws Exception {
        String password = null;
        String userName = null;
        String email = null;
        OwnerMeta owner = null;
        if (registerType == Consts.REGISTER_FROM_UDB
                || registerType == Consts.REGISTER_FROM_YUNPORTAL) {
            if (req.getParameter(Parameters.USER_NAME) == null
                    || req.getParameter(Parameters.EMAIL) == null)
                throw new BaseException();
            password = RandomStringUtils.randomAlphanumeric(6);
            userName = req.getParameter(Parameters.USER_NAME);
            email = URLDecoder.decode(req.getParameter(Parameters.EMAIL), Consts.STR_UTF8);
            owner = new OwnerMeta(userName);
            if (registerType == Consts.REGISTER_FROM_YUNPORTAL && client.ownerSelect(owner)) {
                owner.frozenDate = "";
                owner.setPwd(password);
                client.ownerUpdate(owner);
                replaceContent(emailContent, owner, body);
                emailContent.setSubject(emailContent.getRegisterSubjectFromYunPortal());
                emailContent.setNick(emailContent.getEmailNick());
                SocketEmail.sendEmail(emailContent);
                log.info("user: " + userName + " open success");
                return "open user success";
            }
        } else {
            password = req.getParameter(Parameters.PASSWORD);
            email = URLDecoder.decode(req.getParameter(Parameters.EMAIL), Consts.STR_UTF8);
            userName = email;
            owner = new OwnerMeta(userName);
        }
        String displayName = "";
        if (req.getParameter(Parameters.DISPLAY_NAME) != null)
            displayName = URLDecoder.decode(req.getParameter(Parameters.DISPLAY_NAME),
                    Consts.STR_UTF8);
        String companyName = "";
        if (req.getParameter(Parameters.COMPANY_NAME) != null)
            companyName = URLDecoder.decode(req.getParameter(Parameters.COMPANY_NAME),
                    Consts.STR_UTF8);
        String companyAddress = "";
        if (req.getParameter(Parameters.COMPANY_ADDRESS) != null)
            companyAddress = URLDecoder.decode(req.getParameter(Parameters.COMPANY_ADDRESS),
                    Consts.STR_UTF8);
        String mobilePhone = "";
        if (req.getParameter(Parameters.MOBILE_PHONE) != null)
            mobilePhone = req.getParameter(Parameters.MOBILE_PHONE);
        String phone = "";
        if (req.getParameter(Parameters.PHONE) != null)
            phone = URLDecoder.decode(req.getParameter(Parameters.PHONE), Consts.STR_UTF8);
        if (!StringUtils.isAlphanumeric(password))
            throw new BaseException(400, "InvalidPassword");
        if (!Portal.isLengthRight(email, Consts.EMAIL_MIN_LENGTH, Consts.EMAIL_MAX_LENGTH)
                || !Portal.isLengthRight(userName, Consts.EMAIL_MIN_LENGTH, Consts.EMAIL_MAX_LENGTH)
                || !Portal.isLengthRight(password, Consts.PASSWORD_MIN_LENGTH,Consts.PASSWORD_MAX_LENGTH))
            throw new BaseException(400, "InvalidLength");
        if (!Utils.isAlphaAndNumeric(password))
            throw new BaseException(400, "InvalidPassword");
        owner.displayName = XSSUtils.clearXss(displayName);
        owner.setPwd(password);
        owner.email = XSSUtils.clearXss(email);
        owner.companyName = XSSUtils.clearXss(companyName);
        owner.companyAddress = XSSUtils.clearXss(companyAddress);
        owner.mobilePhone = mobilePhone;
        owner.phone = phone;
        checkUserInfo(owner);
        if (registerType != Consts.REGISTER_FROM_YUNPORTAL && client.ownerSelect(owner))
            throw new BaseException(400, "UserAlreadyExist");
        if (registerType == Consts.REGISTER_FROM_YUNPORTAL) {
            owner.verify = null;
            owner.credit = -Long.MAX_VALUE;
            owner.type = OwnerMeta.TYPE_CRM;
        } else if (registerType == Consts.REGISTER_FROM_OOS) {
            owner.verify = UUID.randomUUID().toString();
            owner.credit = defaultCredit;
            owner.type = OwnerMeta.TYPE_OOS;
        } else {
            owner.verify = null;
            owner.credit = defaultCredit;
            owner.type = OwnerMeta.TYPE_UDB;
        }
        owner.maxAKNum = Consts.MAX_AK_NUM;
        client.ownerInsert(owner);
        if (registerType != Consts.REGISTER_FROM_OOS) {
            replaceContent(emailContent, owner, body);
            emailContent.setSubject(emailContent.getRegisterSubjectFromYunPortal());
            emailContent.setNick(emailContent.getEmailNick());
        } else {
            if (emailContent.isSendEmail()) {
                replaceContent(emailContent, owner);
                resp.setHeader(WebsiteHeader.SEND_EMAIL_TO_MARKET, "true");
                emailContent.setNick(emailContent.getEmailNick());
                emailContent.setSubject(emailContent.getRegisterSubject());
            } else {
                if (req.getParameter(Parameters.ISENGLISH) != null
                        && req.getParameter(Parameters.ISENGLISH).equals("true")) {
                    replaceContent2(emailContent, owner, emailContent.registerBodyEnglish,
                            true, req.getParameter(Parameters.URL_TYPE));
                    emailContent.setSubject(emailContent.registerSubjectEnglish);
                    emailContent.setNick(emailContent.registerNickEnglish);
                } else {
                    replaceContent2(emailContent, owner, emailContent.getRegisterBody(),
                            false, req.getParameter(Parameters.URL_TYPE));
                    resp.setHeader(WebsiteHeader.SEND_EMAIL_TO_MARKET, "false");
                    emailContent.setNick(emailContent.getEmailNick());
                    emailContent.setSubject(emailContent.getRegisterSubject());
                }
            }
        }
        if (isPublicCloud)// 公有云发邮件，私有云不发
        {
            SocketEmail.sendEmail(emailContent);
            // 发邮件给内部人
            emailInner.setInnerRegisterEmail();
            replaceContent(emailInner, owner);
            emailInner.setSubject(emailContent.getRegisterSubject());
            emailInner.setNick(emailContent.getEmailNick());
            SocketEmail.sendEmail(emailInner);
        }
        if (registerType == Consts.REGISTER_FROM_YUNPORTAL) {
            log.info("user: " + userName + " register success");
        } else {
            resp.setHeader(WebsiteHeader.ADD_USER, "register success");
            resp.setStatus(200);
            log.info("user: " + userName + " register success");
        }
        // 删除image sessionId，避免客户端重复使用同一个sessionid
        session.remove(req.getParameter(Parameters.IMAGE_SESSIONID));
        return "register success";
    }
    
    public static String register(HttpServletRequest req, HttpServletResponse resp,
            Session<String, String> session, Email emailContent, String body, int registerType,
            Email emailInner, int defaultCredit,CompositeConfiguration config) throws Exception {
        String password = null;
        String userName = null;
        String email = null;
        OwnerMeta owner = null;
        if (registerType == Consts.REGISTER_FROM_UDB
                || registerType == Consts.REGISTER_FROM_YUNPORTAL) {
            if (req.getParameter(Parameters.USER_NAME) == null
                    || req.getParameter(Parameters.EMAIL) == null)
                throw new BaseException();
            password = RandomStringUtils.randomAlphanumeric(6);
            userName = req.getParameter(Parameters.USER_NAME);
            email = URLDecoder.decode(req.getParameter(Parameters.EMAIL), Consts.STR_UTF8);
            owner = new OwnerMeta(userName);
            if (registerType == Consts.REGISTER_FROM_YUNPORTAL && client.ownerSelect(owner)) {
                owner.frozenDate = "";
                owner.setPwd(password);
                client.ownerUpdate(owner);
                if (emailContent != null) {
                    replaceContent(emailContent, owner, body);
                    emailContent.setSubject(emailContent.getRegisterSubjectFromYunPortal());
                    emailContent.setNick(emailContent.getEmailNick());
                    SocketEmail.sendEmail(emailContent);
                }
                
                log.info("user: " + userName + " open success");
                return "open user success";
            }
        } else {
            password = req.getParameter(Parameters.PASSWORD);
            email = URLDecoder.decode(req.getParameter(Parameters.EMAIL), Consts.STR_UTF8);
            userName = email;
            owner = new OwnerMeta(userName);
        }
        String displayName = "";
        if (req.getParameter(Parameters.DISPLAY_NAME) != null)
            displayName = URLDecoder.decode(req.getParameter(Parameters.DISPLAY_NAME),
                    Consts.STR_UTF8);
        String companyName = "";
        if (req.getParameter(Parameters.COMPANY_NAME) != null)
            companyName = URLDecoder.decode(req.getParameter(Parameters.COMPANY_NAME),
                    Consts.STR_UTF8);
        String companyAddress = "";
        if (req.getParameter(Parameters.COMPANY_ADDRESS) != null)
            companyAddress = URLDecoder.decode(req.getParameter(Parameters.COMPANY_ADDRESS),
                    Consts.STR_UTF8);
        String mobilePhone = "";
        if (req.getParameter(Parameters.MOBILE_PHONE) != null)
            mobilePhone = req.getParameter(Parameters.MOBILE_PHONE);
        String phone = "";
        if (req.getParameter(Parameters.PHONE) != null)
            phone = URLDecoder.decode(req.getParameter(Parameters.PHONE), Consts.STR_UTF8);

        if (!Utils.isValidPassword(password)) {
            throw new BaseException(400, "InvalidPassword");
        } 
        
        if (!Utils.isValidEmail(email)) {
            throw new BaseException(400, "InvalidUserEmail");
        }
        
        if (!Portal.isLengthRight(email, Consts.EMAIL_MIN_LENGTH, Consts.EMAIL_MAX_LENGTH)
                || !Portal.isLengthRight(userName, Consts.EMAIL_MIN_LENGTH, Consts.EMAIL_MAX_LENGTH)
                || !Portal.isLengthRight(password, Consts.PASSWORD_MIN_LENGTH,Consts.PASSWORD_MAX_LENGTH))
            throw new BaseException(400, "InvalidLength");
        owner.displayName = XSSUtils.clearXss(displayName);
        owner.setPwd(password);
        owner.email = XSSUtils.clearXss(email);
        owner.companyName = XSSUtils.clearXss(companyName);
        owner.companyAddress = XSSUtils.clearXss(companyAddress);
        owner.mobilePhone = mobilePhone;
        owner.phone = phone;
        checkUserInfo(owner);
        if (registerType != Consts.REGISTER_FROM_YUNPORTAL && client.ownerSelect(owner))
            throw new BaseException(400, "UserAlreadyExist");
        if (registerType == Consts.REGISTER_FROM_YUNPORTAL) {
            owner.verify = null;
            owner.credit = -Long.MAX_VALUE;
            owner.type = OwnerMeta.TYPE_CRM;
        } else if (registerType == Consts.REGISTER_FROM_OOS) {
            owner.verify = UUID.randomUUID().toString();
            owner.credit = defaultCredit;
            owner.type = OwnerMeta.TYPE_OOS;
        } else {
            owner.verify = null;
            owner.credit = defaultCredit;
            owner.type = OwnerMeta.TYPE_UDB;
        }
        owner.maxAKNum = Consts.MAX_AK_NUM;
        
        client.ownerInsert(owner);
        
        if (emailContent != null) {
            sendUserRegistMail(req,resp,registerType,userName,config);
        }
        
        if (registerType == Consts.REGISTER_FROM_YUNPORTAL) {
            log.info("user: " + userName + " register success");
        } else {
            resp.setHeader(WebsiteHeader.ADD_USER, "register success");
            resp.setStatus(200);
            log.info("user: " + userName + " register success");
        }
        return "register success";
    }
    
    public static void sendUserRegistMail(HttpServletRequest req, HttpServletResponse resp,
            int registerType, String userName,CompositeConfiguration config) throws BaseException, Exception {
        Email emailContent = new Email(config);
        Email emailInner = new Email(config);
        OwnerMeta owner = new OwnerMeta(userName);
        if (!client.ownerSelect(owner)) {
            throw new BaseException(400, "UserNotExist");
        }
        
        if (registerType != Consts.REGISTER_FROM_OOS) {
            replaceContent(emailContent, owner, emailContent.getRegisterBody());
            emailContent.setSubject(emailContent.getRegisterSubjectFromYunPortal());
            emailContent.setNick(emailContent.getEmailNick());
        } else {
            if (emailContent.isSendEmail()) {
                replaceContent(emailContent, owner);
                resp.setHeader(WebsiteHeader.SEND_EMAIL_TO_MARKET, "true");
                emailContent.setNick(emailContent.getEmailNick());
                emailContent.setSubject(emailContent.getRegisterSubject());
            } else {
                if (req.getParameter(Parameters.ISENGLISH) != null
                        && req.getParameter(Parameters.ISENGLISH).equals("true")) {
                    replaceContent2(emailContent, owner, emailContent.registerBodyEnglish,
                            true, req.getParameter(Parameters.URL_TYPE));
                    emailContent.setSubject(emailContent.registerSubjectEnglish);
                    emailContent.setNick(emailContent.registerNickEnglish);
                } else {
                    replaceContent2(emailContent, owner, emailContent.getRegisterBody(),
                            false, req.getParameter(Parameters.URL_TYPE));
                    resp.setHeader(WebsiteHeader.SEND_EMAIL_TO_MARKET, "false");
                    emailContent.setNick(emailContent.getEmailNick());
                    emailContent.setSubject(emailContent.getRegisterSubject());
                }
            }
        }
        
        if (config.getBoolean("website.isPublicCloud"))// 公有云发邮件，私有云不发
        {
            SocketEmail.sendEmail(emailContent);
            // 发邮件给内部人
            emailInner.setInnerRegisterEmail();
            replaceContent(emailInner, owner);
            emailInner.setSubject(emailContent.getRegisterSubject());
            emailInner.setNick(emailContent.getEmailNick());
            SocketEmail.sendEmail(emailInner);
        }
    }
    
    private static void checkUserInfo(OwnerMeta owner) throws UnsupportedEncodingException,
            BaseException {
        if (!Portal.isLengthRight(owner.email, Consts.EMAIL_MIN_LENGTH, Consts.EMAIL_MAX_LENGTH))
            throw new BaseException(400, "InvalidLength");
        if (!Portal.isLengthRight(owner.displayName, Consts.DISPLAYNAME_MAX_LENGTH))
            throw new BaseException(400, "InvalidDisplayNameLength");
        if (!Portal.isLengthRight(owner.companyName, Consts.COMPANYNAME_MAX_LENGTH))
            throw new BaseException(400, "InvalidCompanyNameLength");
        if (!Portal.isLengthRight(owner.companyAddress, Consts.COMPANYADDRESS_MAX_LENGTH))
            throw new BaseException(400, "InvalidCompanyAddressLength");
        if (owner.mobilePhone != null
                && owner.mobilePhone.trim().length() != 0
                && (owner.mobilePhone.length() > Consts.MOBILEPHONE_MAX_LENGTH || !StringUtils
                        .isNumeric(owner.mobilePhone)))
            throw new BaseException(400, "InvalidMobilePhone");
        if (!Portal.isLengthRight(owner.phone, Consts.PHONE_MAX_LENGTH))
            throw new BaseException(400, "InvalidPhoneLength");
        if (owner.phone != null && owner.phone.trim().length() == 0 && owner.mobilePhone != null
                && owner.mobilePhone.trim().length() == 0)
            throw new BaseException();
    }
    
    public static void replaceContent(Email emailContent, OwnerMeta owner, String body) {
        body = body.replace("USERNAME", StringEscapeUtils.escapeHtml4(owner.getName()).toString());
        body = body.replace("PASSWORD", owner.getPwd());
        body = body.replace("DATE", Misc.formatYearMonthDate(new Date()));
        emailContent.setBody(body);
        emailContent.setTo(owner.email);
        emailContent.setFrom(emailContent.getFrom189());
        emailContent.setSmtp(emailContent.getSmtp189());
    }
    
    public static String getAccountId(Session<String, String> session, String sessionId) throws SQLException {
        OwnerMeta owner = new OwnerMeta(session.get(sessionId));
        DbInitialAccount dbInitialAccount = new DbInitialAccount();
        dbInitialAccount.ownerId = owner.getId();
        DB db = DB.getInstance();
        if (db.initialAccountSelectByOwnerId(dbInitialAccount)) {
            return dbInitialAccount.accountId;
        }
        return null;
    }
    
    public static String getAccountIdFromCookie(HttpServletRequest req) {
        String accountId = null;
        Cookie[] cookies = req.getCookies();
        for (Cookie cookie : cookies) {
            if (cookie.getName().equals("x-ctyun-accountId")) {
                accountId = cookie.getValue();
                break;
            }
        }
        return accountId;
    }
    
    private static void replaceContent(Email emailContent, OwnerMeta owner) {
        String body = emailContent.getRegisterBody();
        if (owner.companyName != null && owner.companyName.trim().length() != 0) {
            body = body.replace("COMPANY_NAME", StringEscapeUtils.escapeHtml4(owner.companyName));
        } else
            body = body.replace("COMPANY_NAME", "未填");
        if (owner.companyAddress != null && owner.companyAddress.trim().length() != 0) {
            body = body.replace("COMPANY_ADDRESS",
                    StringEscapeUtils.escapeHtml4(owner.companyAddress).toString());
        } else
            body = body.replace("COMPANY_ADDRESS", "未填");
        if (owner.displayName != null && owner.displayName.trim().length() != 0) {
            body = body.replace("DISPLAY_NAME", StringEscapeUtils.escapeHtml4(owner.displayName)
                    .toString());
        } else
            body = body.replace("DISPLAY_NAME", "未填");
        if (owner.mobilePhone != null && owner.mobilePhone.trim().length() != 0)
            body = body.replace("MOBILE_PHONE", owner.mobilePhone);
        else
            body = body.replace("MOBILE_PHONE", "未填");
        if (owner.phone != null && owner.phone.trim().length() != 0) {
            body = body.replace("PHONE", StringEscapeUtils.escapeHtml4(owner.phone).toString());
        } else
            body = body.replace("PHONE", "未填");
        body = body.replace("EMAIL_LINK", "https://" + emailContent.getDomainSuffix()
                + "/?verifyRegister=" + owner.verify + "&market=1" + "&verifyEmail=" + owner.email);
        body = body.replace("EMAIL", owner.email);
        body = body.replace("DATE", Misc.formatYearMonthDate(new Date()));
        emailContent.setBody(body);
        emailContent.setFrom(emailContent.getFrom189());
        emailContent.setSmtp(emailContent.getSmtp189());
    }
    
    private static void replaceContent2(Email emailContent, OwnerMeta owner,
            String body, boolean isEnglish, String type) {
        if (owner.displayName != null && owner.displayName.trim().length() != 0) {
            body = body.replace("USERNAME", StringEscapeUtils.escapeHtml4(owner.displayName)
                    .toString());
        } else if (isEnglish)
            body = body.replace("USERNAME", "");
        else
            body = body.replace("USERNAME", "客户");
        if (isEnglish) {
            body = body.replace("EMAIL_LINK", "https://" + emailContent.getDomainSuffix()
                    + "/?verifyRegister=" + owner.verify + "&market=0&isEnglish=true"
                    + "&verifyEmail=" + owner.email);
            body = body.replace("DATE", Misc.formatyyyymmdd(new Date()));
        } else {
            if (type != null && type.equals("ctyun"))
                body = body.replace("EMAIL_LINK", "https://" + emailContent.getDomainSuffix()
                        + "/?verifyRegister=" + owner.verify + "&market=0&type=ctyun"
                        + "&verifyEmail=" + owner.email);
            else
                body = body.replace("EMAIL_LINK", "https://" + emailContent.getDomainSuffix()
                        + "/?verifyRegister=" + owner.verify + "&market=0" + "&verifyEmail="
                        + owner.email);
            body = body.replace("DATE", Misc.formatYearMonthDate(new Date()));
        }
        emailContent.setBody(body);
        emailContent.setTo(owner.email);
        emailContent.setFrom(emailContent.getFrom189());
        emailContent.setSmtp(emailContent.getSmtp189());
    }
    
    public static void findPassword(HttpServletRequest req, HttpServletResponse resp,
            Session<String, String> session, Email emailContent, String webpages,
            boolean isPublicCloud) throws Exception {
        String email = req.getParameter(Parameters.FIND_PASSWORD);
        String verifyCode = req.getParameter(Parameters.VERIFY_CODE);
        String sessionId = req.getParameter(Parameters.IMAGE_SESSIONID);
        if (verifyCode == null || !verifyCode.equalsIgnoreCase(session.remove(sessionId)))
            throw new BaseException(400, "InvalidVerifyCode");
        OwnerMeta owner = new OwnerMeta(email);
        if (!client.ownerSelect(owner))
            throw new BaseException(400, "AccountInformationCouldNotBeVerified");
        session.update(sessionId, email);
        String findPasswordId = UUID.randomUUID().toString();
        session.update(findPasswordId, sessionId);
        if (req.getParameter(Parameters.ISENGLISH) != null
                && req.getParameter(Parameters.ISENGLISH).equals("true")) {
            String body = emailContent.findPasswordBodyEnglish;
            if (owner.displayName != null && owner.displayName.trim().length() != 0) {
                body = body.replace("USERNAME", StringEscapeUtils.escapeHtml4(owner.displayName)
                        .toString());
            } else
                body = body.replace("USERNAME", "");
            body = body.replace("EMAIL_LINK", "https://" + emailContent.getDomainSuffix() + "/"
                    + webpages + "/en/users/findpassword_1.html?updatePwdSessionId=" + findPasswordId);
            body = body.replace("DATE", Misc.formatyyyymmdd(new Date()));
            emailContent.setBody(body);
            emailContent.setSubject(emailContent.findPasswordSubjectEnglish);
            emailContent.setNick(emailContent.registerNickEnglish);
        } else {
            String body = emailContent.getFindPasswordBody();
            if (owner.displayName != null && owner.displayName.trim().length() != 0) {
                body = body.replace("USERNAME", StringEscapeUtils.escapeHtml4(owner.displayName)
                        .toString());
            } else
                body = body.replace("USERNAME", "客户");
            if (req.getParameter(Parameters.URL_TYPE) != null
                    && req.getParameter(Parameters.URL_TYPE).equals("ctyun"))
                body = body.replace("EMAIL_LINK", "https://" + emailContent.getDomainSuffix() + "/"
                        + webpages + "/ctyun/users/findpassword_1.html?updatePwdSessionId="
                        + findPasswordId);
            else
                body = body.replace("EMAIL_LINK", "https://" + emailContent.getDomainSuffix() + "/"
                        + webpages + "/v1/findpassword_1.html?updatePwdSessionId=" + findPasswordId);
            body = body.replace("DATE", Misc.formatYearMonthDate(new Date()));
            emailContent.setBody(body);
            emailContent.setSubject(emailContent.getFindPasswordSubject());
            emailContent.setNick(emailContent.getFindPasswordNick());
        }
        emailContent.setTo(owner.email);
        emailContent.setFrom(emailContent.getFrom189());
        emailContent.setSmtp(emailContent.getSmtp189());
        SocketEmail.sendEmail(emailContent);
        resp.setStatus(200);
        log.info("user: " + email + " find password email send success");
    }
    
    public static void updatePassword(HttpServletRequest req, HttpServletResponse resp,
            Session<String, String> session) throws Exception {
        String sourceType = req.getParameter(Parameters.REGISTER_SOURCE_TYPE);
        if (!Arrays.toString(SourceTYPE.values()).contains(sourceType)) {
            throw new BaseException(405, "InvalidSourceType");
        }
        
        if (SourceTYPE.valueOf(sourceType) == SourceTYPE.PROXY) {
            String newPwd = req.getHeader(WebsiteHeader.NEW_PASSWORD);
            String sessionId = session.getAndUpdate(req.getHeader(WebsiteHeader.UPDATE_PASSWORD_SESSIONID));
            String email = session.getAndUpdate(sessionId);

            if (email == null)
                throw new BaseException(400, "OutOfDate");
            if (!isLengthRight(newPwd, Consts.PASSWORD_MIN_LENGTH, Consts.PASSWORD_MAX_LENGTH))
                throw new BaseException(400, "InvalidLength");
            if (!Utils.isValidPassword(newPwd))
                throw new BaseException(400, "InvalidPassword");
            
            //先更新本地密码；
            OwnerMeta owner = new OwnerMeta(email);
            if (!client.ownerSelect(owner))
                throw new BaseException(400, "OutOfDate");
            owner.setPwd(newPwd);
            client.ownerUpdate(owner);
            
            if (!BssAdapterClient.modifyUser(BssAdapterConfig.localPool.ak , 
                    BssAdapterConfig.localPool.getSK(),owner,SourceTYPE.valueOf(sourceType),
                    UpdateType.PASSWORD,new String[]{BssAdapterConfig.localPool.name})) {
                throw new BaseException(500, "UpdatePWDUserFail");
            }
            
        } else if (SourceTYPE.valueOf(sourceType) == SourceTYPE.ADAPTERSERVER) {
            String newPwd = req.getHeader(WebsiteHeader.NEW_PASSWORD);
            String username = req.getHeader(Parameters.USER_NAME);
            
            OwnerMeta owner = new OwnerMeta(username);
            if (!client.ownerSelect(owner))
                throw new BaseException(400, "OutOfDate");
            owner.setPwd(newPwd);
            client.ownerUpdate(owner);
            log.info("user: " + username + " update password success");
        }
        
        resp.setStatus(200);
        session.remove(req.getHeader(WebsiteHeader.UPDATE_PASSWORD_SESSIONID));
    }
    
    public static void updateUserInfo(OwnerMeta owner, String lastLoginIp, long lastLoginTime)
            throws Exception {
        if (client.ownerSelect(owner)) {
            owner.proxyLastLoginIp = lastLoginIp;
            owner.proxyLastLoginTime = lastLoginTime;
            client.ownerUpdate(owner);
        }
    }
    /**
     * 从session里获取上一次用户的登录信息，如果不存在，再从用户表里获取
     */
    public static void getLastLoginInfo(HttpServletRequest req, HttpServletResponse resp,
            Session<String, String> session) throws Exception {
        String sessionId = Utils.getSessionId(req);
        String loginInfo = session.getAndUpdate(sessionId + "|loginInfo");
        if (loginInfo == null) {
            JSONObject restJson = null;
            OwnerMeta owner = Portal.getOwnerFromSession(session, Utils.getSessionId(req));
            restJson = new JSONObject();
            restJson.put("proxyLastLoginIp", owner.proxyLastLoginIp);
            if (owner.proxyLastLoginTime != -1) {
                restJson.put("proxyLastLoginTime", owner.proxyLastLoginTime);
            } else {
                restJson.put("proxyLastLoginTime", "");
            }
            resp.getWriter().write(restJson.toString());
        } else {
            resp.getWriter().write(loginInfo);
        }
        resp.setStatus(200);
    }

    /**
     * 在session里保存上一次用户的登录信息
     */
    public static void saveLastLoginInfo(Session<String, String> session, String sessionId, long lastLoginTime,
            String lastLoginIp) throws Exception {
        JSONObject restJson = new JSONObject();
        restJson.put("proxyLastLoginIp", lastLoginIp);
        if (lastLoginTime != -1) {
            restJson.put("proxyLastLoginTime", lastLoginTime);
        } else {
            restJson.put("proxyLastLoginTime", "");
        }
        session.update(sessionId + "|loginInfo", restJson.toString());
    }

    public static void saveLastLoginInfo(Session<String, String> session, String sessionId) throws Exception {
        OwnerMeta owner = Portal.getOwnerFromSession(session, sessionId);
        saveLastLoginInfo(session, sessionId, owner.proxyLastLoginTime, owner.proxyLastLoginIp);
    }

    public static void writeDownloadResponseEntity(HttpServletResponse resp, String body, String filename) throws IOException {
        resp.setCharacterEncoding(Consts.STR_UTF8);
        resp.setHeader("Content-type", "application/octet-stream");
        resp.setHeader("Content-Disposition", "attachment;filename=" + filename);
        if (body != null) {
            resp.setIntHeader(Headers.CONTENT_LENGTH, body.getBytes(Consts.CS_UTF8).length);
        }
        resp.getWriter().write(body);
    }
    public static void writeDownloadResponseEntityWithUtf8bom(HttpServletResponse resp, String body, String filename) throws IOException {
        resp.setCharacterEncoding(Consts.STR_UTF8);
        resp.setHeader("Content-type", "application/octet-stream");
        resp.setHeader("Content-Disposition", "attachment;filename=" + filename);
        byte[] bodyBytes = body.getBytes(Consts.CS_UTF8);
        byte[] uft8bom = { (byte) 0xef, (byte) 0xbb, (byte) 0xbf };
        if (body != null) {
            resp.setIntHeader(Headers.CONTENT_LENGTH, body.getBytes(Consts.CS_UTF8).length + uft8bom.length);
        }
        OutputStream out = resp.getOutputStream();
        out.write(uft8bom);
        out.write(bodyBytes);
    }
    public static void iamDownloadFile(HttpServletRequest req, HttpServletResponse resp, Session<String, String> session)
            throws Exception {
        String file = req.getParameter("fileName");
        String body = req.getParameter("data");
        if (body != null) {
            writeDownloadResponseEntityWithUtf8bom(resp, URLDecoder.decode(body, Consts.STR_UTF8), file);
        }
        resp.setStatus(200);
    }
    public static void verifyRegister(HttpServletRequest req, HttpServletResponse resp,
            String webpages) throws Exception {
        OwnerMeta owner;
        if (req.getParameter("verifyEmail") != null)
            owner = new OwnerMeta(req.getParameter("verifyEmail"));
        else
            throw new BaseException();
        String isEnglish = req.getParameter(Parameters.ISENGLISH);
        String type = req.getParameter(Parameters.URL_TYPE);
        if (client.ownerSelect(owner) && owner.verify != null
                && owner.verify.equals(req.getParameter(Parameters.VERIFY_REGISTER))) {
            owner.verify = null;
            //广播此用户的激活操作；使用各个资源池的业务管理平台服务激活用户；
            if (!BssAdapterClient.modifyUser(BssAdapterConfig.localPool.ak, BssAdapterConfig.localPool.getSK(), 
                    owner, SourceTYPE.MANAGEMENT, UpdateType.ACTIVE,new String[]{BssAdapterConfig.localPool.name})){
                throw new BaseException(500, "UpdateUserFail");
            } else {
                if (!BssAdapterConfig.localPool.name.equals(BssAdapterConfig.name)) {
                    client.ownerDeleteColumn(owner, "verify");    
                }
                
                log.info("user: " + owner.getName() + " verify success");
                if (req.getParameter("market").equals("1"))
                    resp.sendRedirect(webpages + "/activatesuccess.html");
                else {
                    resp.sendRedirect(webpages + "/ctyun/users/regsuccess.html?email=" + owner.email);
                }
            }
        } else {
            resp.sendRedirect(webpages + "/ctyun/users/error.html?msg="
                    + URLEncoder.encode("该用户不存在，或者已经被激活", Consts.STR_UTF8));
        }
    }
    
    public static void sendEmailAgain(HttpServletRequest req, HttpServletResponse resp,
            Email emailContent) throws Exception {
        String email = req.getParameter(Parameters.SEND_VERIFY_EMAIL_AGAIN);
        OwnerMeta owner = new OwnerMeta(email);
        boolean ref = client.ownerSelect(owner);
        if (ref == false)
            throw new BaseException(400, "InvalidAccessKeyId");
        AtomicInteger v = sendEmailTimes.get(email);
        if (v == null) {
            sendEmailTimes.putIfAbsent(email, new AtomicInteger(0));
            AtomicInteger ov = sendEmailTimes.get(email);
            if (ov != null)
                v = ov;
        }
        v.incrementAndGet();
        if (v.get() > 3)
            throw new BaseException(400, "SendEmailToManyTimes");
        if (emailContent.isSendEmail()) {
            replaceContent(emailContent, owner);
            resp.setHeader(WebsiteHeader.SEND_EMAIL_TO_MARKET, "true");
        } else {
            if (req.getParameter(Parameters.ISENGLISH) != null
                    && req.getParameter(Parameters.ISENGLISH).equals("true")) {
                replaceContent2(emailContent, owner, emailContent.registerBodyEnglish, true,
                        req.getParameter(Parameters.URL_TYPE));
                resp.setHeader(WebsiteHeader.SEND_EMAIL_TO_MARKET, "false");
                emailContent.setSubject(emailContent.registerSubjectEnglish);
                emailContent.setNick(emailContent.registerNickEnglish);
            } else {
                replaceContent2(emailContent, owner, emailContent.getRegisterBody(), false,
                        req.getParameter(Parameters.URL_TYPE));
                resp.setHeader(WebsiteHeader.SEND_EMAIL_TO_MARKET, "false");
                emailContent.setSubject(emailContent.getRegisterSubject());
                emailContent.setNick(emailContent.getEmailNick());
            }
        }
        SocketEmail.sendEmail(emailContent);
        log.info("user: " + email + " send email again success");
    }
    
    public static void getUsage(OwnerMeta owner, HttpServletResponse resp)
            throws Exception {
        boolean ref = client.ownerSelect(owner);
        if (ref == false)
            throw new BaseException(400, "InvalidAccessKeyId");
//        String count = pin.select(owner.getId(), Consts.GLOBAL_DATA_REGION);
//        String count;
//        
//        if (count != null)
//            resp.getWriter().write(count);
////        sb.append(dbCurrent.originalTotalSize).append(" ").append(dbCurrent.originalPeakSize).append(" ")
////        .append(dbCurrent.upload).append(" ").append(dbCurrent.transfer).append(" ")
////        .append(dbCurrent.ghRequest).append(" ").append(dbCurrent.otherRequest).append(" ")
////        .append(dbCurrent.roamFlow).append(" ").append(dbCurrent.roamUpload).append(" ")
////        .append(dbCurrent.noNetUpload).append(" ").append(dbCurrent.noNetTransfer).append(" ")
////        .append(dbCurrent.noNetGHReq).append(" ").append(dbCurrent.noNetOtherReq).append(" ")
////        .append(dbCurrent.noNetRoamFlow).append(" ").append(dbCurrent.noNetRoamUpload).append(" ")
////        .append(dbCurrent.spamRequest).append(" ").append(dbCurrent.pornReviewFalse+dbCurrent.pornReviewTrue);
    }
    
    public static void shareURL(HttpServletRequest req, HttpServletResponse resp,Pair<String, String> akskPair,
            String domainSuffix) throws Exception {
        String ak = akskPair.first();
        String sk = akskPair.second();
        String sessionId = Utils.getSessionId(req);
        String bucketName = req.getParameter(Parameters.BUCKET_NAME);
        String objectName = req.getParameter(Parameters.OBJECT_NAME);
        String amzLimit = req.getParameter(Consts.X_AMZ_LIMIT);
        String date = req.getHeader(WebsiteHeader.DATE);
        Date expireDate = null;
        if (date != null)
            try {
                expireDate = TimeUtils.fromGMTFormat(req.getHeader(WebsiteHeader.DATE));
            } catch (ParseException e) {
                throw new BaseException();
            }
        BucketMeta dbBucket = new BucketMeta(bucketName);
        boolean res = client.bucketSelect(dbBucket);
        if (false == res) {
            throw new BaseException(404, ErrorMessage.ERROR_CODE_NO_SUCH_BUCKET, ErrorMessage.ERROR_MESSAGE_404);
        }
        ObjectMeta object = new ObjectMeta(objectName, bucketName, dbBucket.metaLocation);
        res = client.objectSelect(object);
        if (false == res)
            throw new BaseException(404, ErrorMessage.ERROR_CODE_NO_SUCH_KEY, ErrorMessage.ERROR_MESSAGE_404);
        if (amzLimit != null) {
            validAmzLimit(amzLimit);
        }
        BasicAWSCredentials credentials = new BasicAWSCredentials(ak, sk);
        AmazonS3Client s3Client = new AmazonS3Client(credentials);
        s3Client.setEndpoint("http://" + domainSuffix);
        S3ClientOptions clientOptions = new S3ClientOptions();
        clientOptions.setPathStyleAccess(true);
        s3Client.setS3ClientOptions(clientOptions);
        GeneratePresignedUrlRequest request = new GeneratePresignedUrlRequest(bucketName, objectName);
        request.setExpiration(expireDate);
        if (amzLimit != null) {
            request.addRequestParameter(Consts.X_AMZ_LIMIT, amzLimit);
        }
        String url = s3Client.generatePresignedUrl(request).toString();
        resp.getWriter().write(url);
    }

    public static void validAmzLimit(String amzLimit) throws BaseException {
        try {
            String[] limitParms = amzLimit.split(",");
            boolean concurrencyExist = false;
            boolean rateExist = false;
            for (int i = 0; i < limitParms.length; i++) {
                String[] aPram = limitParms[i].split("=");
                if (aPram.length != 2) {
                    throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_ARGUMENT,
                            ErrorMessage.ERROR_MESSAGE_INVALID_X_AMZ_LIMIT);
                }
                switch (aPram[0].trim()) {
                case Consts.X_AMZ_LIMIT_CONCURRENCY_INDEX: {
                    if (concurrencyExist) {
                        throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_ARGUMENT,
                                ErrorMessage.ERROR_MESSAGE_INVALID_X_AMZ_LIMIT);
                    }

                    int concurrency = Integer.parseInt(aPram[1]);
                    if (concurrency < 1) {
                        throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_ARGUMENT,
                                ErrorMessage.ERROR_MESSAGE_INVALID_X_AMZ_CONCURRENCY);
                    }
                    concurrencyExist = true;
                    break;
                }
                case Consts.X_AMZ_LIMIT_RATE_INDEX: {
                    if (rateExist) {
                        throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_ARGUMENT,
                                ErrorMessage.ERROR_MESSAGE_INVALID_X_AMZ_LIMIT);
                    }
                    int rate = Integer.parseInt(aPram[1]);
                    if (rate < 1) {
                        throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_ARGUMENT,
                                ErrorMessage.ERROR_MESSAGE_INVALID_X_AMZ_LIMITRATE);
                    }
                    rateExist = true;
                    break;
                }
                default: {
                    throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_ARGUMENT,
                            ErrorMessage.ERROR_MESSAGE_INVALID_X_AMZ_LIMIT);
                }
                }

            }
        } catch (NumberFormatException e) {
            throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_ARGUMENT,
                    ErrorMessage.ERROR_MESSAGE_INVALID_X_AMZ_LIMIT);
        }
    }
    
    public static void generateImage(HttpServletResponse resp, String sessionId,
            Session<String, String> session, String webpages) throws IOException {
        String randomStr = RandomStringUtils.randomAlphanumeric(4);
        resp.setHeader("Cache-Control", "no-cache");
        resp.setDateHeader("Expires", 0);
        resp.setContentType("image/png");
        resp.setHeader(Parameters.IMAGE_SESSIONID, sessionId);
        ImageOutputStream stream = null;
        try {
            stream = ImageIO.createImageOutputStream(resp.getOutputStream());
            ImageIO.write(VerifyImage.create(Consts.VERIFY_IMAGE_WIDTH, Consts.VERIFY_IMAGE_HEIGHT,
                    randomStr), "PNG", stream);
        } finally {
            if (stream != null)
                stream.close();
        }
        session.update(sessionId, randomStr);
    }
    
    public static void getCookie(String cookie, HttpServletResponse resp,
            Session<String, String> session, String domainSuffix) throws BaseException {
        String userName = session.getAndUpdate(cookie);
        if (userName == null)
            throw new BaseException(403, "InvalidAccessKeyId");
        Cookie sessionCookie = new Cookie("x-ctyun-cookie", cookie);
        sessionCookie.setDomain(domainSuffix);
        sessionCookie.setMaxAge(Consts.COOKIE_MAXAGE);
        sessionCookie.setPath("/");
        resp.addCookie(sessionCookie);
    }
    
    public static String pay(Long ownerId, double amount, String ip, String orderSeq)
            throws Exception {
        StringBuffer str = new StringBuffer(BestPay.URL);
        DbBill bill = new DbBill();
        bill = Bill.generalBill(ownerId, amount, ip, orderSeq);
        if (amount > 0) {
            str.append("?MERCHANTID=" + bill.getMERCHANTID());
            str.append("&SUBMERCHANTID=" + bill.SUBMERCHANTID);
            str.append("&ORDERSEQ=" + bill.getORDERSEQ());
            str.append("&ORDERREQTRANSEQ=" + bill.getORDERREQTRANSEQ());
            str.append("&ORDERDATE=" + bill.getORDERDATE());
            str.append("&ORDERAMOUNT=" + bill.getORDERAMOUNT());
            str.append("&PRODUCTAMOUNT=" + bill.getPRODUCTAMOUNT());
            str.append("&ATTACHAMOUNT=" + bill.getATTACHAMOUNT());
            str.append("&CURTYPE=" + bill.getCURTYPE());
            str.append("&ENCODETYPE=" + bill.getENCODETYPE());
            str.append("&MERCHANTURL=" + bill.getMERCHANTURL());
            str.append("&BACKMERCHANTURL=" + bill.getBACKMERCHANTURL());
            str.append("&ATTACH=" + URLEncoder.encode(bill.ATTACH, "GBK"));
            str.append("&BUSICODE=" + bill.getBUSICODE());
            str.append("&PRODUCTID=" + bill.getPRODUCTID());
            str.append("&TMNUM=" + bill.getTMNUM());
            str.append("&CUSTOMERID=" + bill.getCUSTOMERID());
            str.append("&PRODUCTDESC=" + URLEncoder.encode(bill.getPRODUCTDESC(), "GBK"));
            str.append("&MAC=" + bill.getMAC());
            str.append("&CLIENTIP=" + bill.getCLIENTIP());
            return str.toString();
        } else {
            DbUserPackage up = new DbUserPackage(orderSeq);
            DBPrice dbPrice = DBPrice.getInstance();
            if (dbPrice.userPackageSelectByOrderId(up))
                dbPrice.userPackageBillUpdate(up);
            return "PaySuccess";
        }
    }
    /**
     * @param ownerId
     * @return 返回用户数据欠费删除状态 -1（正常）1（准备删除，在缓冲期内）1（删除中）2(已删除)
     */
    public static int getDataStatus(long ownerId) throws IOException, BaseException, JSONException {
        int dataStatus = -1;
        try {
            String deleteUserInfo = BssAdapterClient
                    .getDeleteUserInfoV2(String.valueOf(ownerId), BssAdapterConfig.localPool.ak,
                            BssAdapterConfig.localPool.getSK(),"Proxy");
            if (StringUtils.isNotEmpty(deleteUserInfo)) {
                JSONObject userStatus = new JSONObject(deleteUserInfo);
                //HANDLED//删除成功    UNHANDLE//未开始删除(准备删除)    HANDLING//删除中       STOP//用户回退删除操作
                String missionStatus = null;
                if (userStatus.has("missionStatus")) {
                    missionStatus = userStatus.getString("missionStatus");
                    switch (missionStatus) {
                    case "STOP":
                        dataStatus = -1;
                        break;
                    case "UNHANDLE":
                        dataStatus = 0;
                        break;
                    case "HANDLING":
                        dataStatus = 1;
                        break;
                    case "HANDLED":
                        dataStatus = 2;
                        break;
                    }
                }
            }
        } catch (Exception e) {
            log.error("Load DeleteUserInfo From BssAdapter Fail!");
            log.error(e.getMessage(), e);
        } finally {
            return dataStatus;
        }
    }
    public static String getStatus(OwnerMeta owner, int defaultPackageId) throws Exception {
        double balance = 0;
        DBPay dbPay = DBPay.getInstance();
        DbAccount dbAccount = new DbAccount(owner.getId());
        dbPay.accountSelect(dbAccount);
        balance = dbAccount.balance;
        String msg;
        if (Utils.isFrozen(owner)) {
            msg = setStatus(Consts.OWNER_STATUS_FROZEN, balance, owner, defaultPackageId);
        } else if (balance < 0 && owner.credit != -Long.MAX_VALUE) {
            msg = setStatus(Consts.OWNER_STATUS_PAY, balance, owner, defaultPackageId);
        } else
            msg = setStatus(Consts.OWNER_STATUS_RIGHT, balance, owner, defaultPackageId);
        return msg;
    }
    
    private static int getPackageId(long ownerId, int defaultPackageId) throws SQLException {
        DBPrice dbPrice = DBPrice.getInstance();
        DbUserPackage dup = new DbUserPackage(ownerId);
        dup.isPaid = 1;
        dup.date = TimeUtils.toYYYY_MM_dd(new Date());
        if (dbPrice.userPackageSelect(dup))
            if (dup.packageId == defaultPackageId)
                return dup.packageId;
            else
                return 0;
        else
            return 0;
    }
    
    private static String setStatus(int i, double balance, OwnerMeta owner, int defaultPackageId)
            throws SQLException {
        XmlWriter xml = new XmlWriter();
        xml.start("UserInfo");
        xml.start("Status").value(String.valueOf(i)).end();
        xml.start("Balance").value(String.valueOf(balance)).end();
        xml.start("Email").value(owner.email).end();
        if (owner.type != OwnerMeta.TYPE_UDB)
            xml.start("Name").value(owner.getName()).end();
        else
            xml.start("Name").value("").end();
        xml.start("MobilePhone").value(owner.mobilePhone).end();
        xml.start("Phone").value(owner.phone).end();
        xml.start("CompanyName").value(owner.companyName).end();
        xml.start("CompanyAddress").value(owner.companyAddress).end();
        xml.start("DisplayName").value(owner.displayName).end();
        xml.start("UserName").value(owner.getName()).end();
        xml.start("Pid").value(String.valueOf(getPackageId(owner.getId(), defaultPackageId))).end();
        if (owner.credit == -Long.MAX_VALUE)
            xml.start("UserCredit").value("不限").end();
        else
            xml.start("UserCredit").value(String.valueOf(-owner.credit)).end();
        xml.start("Type").value(String.valueOf(owner.type)).end();
        xml.start("isBandwidthUser").value("false").end();
        xml.end();
        return xml.toString();
    }
    
    public static OwnerMeta getOwnerFromSession(Session<String, String> session, String sessionId)
            throws Exception {
        String userName = session.getAndUpdate(sessionId);
        if (userName == null)
            throw new BaseException(403, "Invalid SessionId");
        OwnerMeta owner = new OwnerMeta(userName);
        if (!client.ownerSelect(owner))
            throw new BaseException(403, "Invalid OwnerInfo");
        return owner;
    }
    
    public static String getRecharge(OwnerMeta owner, String beginDate, String endDate)
            throws BaseException, SQLException, IOException {
        DBPay dbPay = DBPay.getInstance();
        DbBill bill = new DbBill();
        bill.setCUSTOMERID(String.valueOf(owner.getId()));
        bill.dateBegin = beginDate;
        bill.dateEnd = endDate;
        dbPay.billSelectByRange(bill);
        XmlWriter xml = new XmlWriter();
        xml.start("Recharge");
        String[] fields = null;
        for (String record : bill.records) {
            xml.start("Bill");
            fields = record.split(" ");
            xml.start("OrderSeq").value(fields[0]).end();
            xml.start("OrderAmount").value(String.valueOf(Double.parseDouble(fields[1]) / 100))
                    .end();
            xml.start("OrderDate").value(fields[2] + " " + fields[3]).end();
            xml.start("Tag").value(fields[4]).end();
            xml.end();
        }
        xml.end();
        return xml.toString();
    }
    
    public static void checkAmount(String amount, int numAfterPoint, boolean mustPositive)
            throws BaseException {
        if (amount.split("\\.").length > 1 && amount.split("\\.")[1].length() > numAfterPoint)
            throw new BaseException();
        if (mustPositive && Double.parseDouble(amount) < 0)
            throw new BaseException();
    }

    public static boolean getObjectExists(String bucketName, String objectName) throws IOException,
            SQLException {
        BucketMeta bucket=new BucketMeta(bucketName);
        if(!client.bucketSelect(bucket))
            return false;
        ObjectMeta object = new ObjectMeta(objectName, bucketName, bucket.metaLocation);
        return client.objectSelect(object);
    }
    
    public static String getPackage() throws SQLException {
        DBPrice dbPrice = DBPrice.getInstance();
        DbPackage dbPackage = new DbPackage();
        dbPrice.packageSelectAll(dbPackage, false);
        XmlWriter xml = new XmlWriter();
        xml.start("Packages");
        for (DbPackage p : dbPackage.packages) {
            xml.start("Package");
            xml.start("ID").value(String.valueOf(p.packageId)).end();
            xml.start("Name").value(p.name).end();
            xml.start("Storage").value(String.valueOf(p.storage)).end();
            xml.start("Flow").value(String.valueOf(p.flow)).end();
            if (p.ghRequest == Long.MAX_VALUE)
                xml.start("GHRequest").value(String.valueOf("不限")).end();
            else
                xml.start("GHRequest").value(String.valueOf(p.ghRequest / 10)).end();
            if (p.otherRequest == Long.MAX_VALUE)
                xml.start("OtherRequest").value(String.valueOf("不限")).end();
            else
                xml.start("OtherRequest").value(String.valueOf(p.otherRequest)).end();
            xml.start("RoamFlow").value(String.valueOf(p.roamFlow)).end();
            xml.start("RoamUpload").value(String.valueOf(p.roamUpload)).end();
            xml.start("NoNetFlow").value(String.valueOf(p.noNetFlow)).end();
            xml.start("NoNetRoamFlow").value(String.valueOf(p.noNetRoamFlow)).end();
            xml.start("NoNetRoamUpload").value(String.valueOf(p.noNetRoamUpload)).end();
            if (p.noNetGHReq == Long.MAX_VALUE)
                xml.start("NoNetGHReq").value(String.valueOf("不限")).end();
            else
                xml.start("NoNetGHReq").value(String.valueOf(p.noNetGHReq / 10)).end();
            if (p.noNetOtherReq == Long.MAX_VALUE)
                xml.start("NoNetOtherReq").value(String.valueOf("不限")).end();
            else
                xml.start("NoNetOtherReq").value(String.valueOf(p.noNetOtherReq)).end();
            xml.start("Duration").value(String.valueOf(p.duration)).end();
            if (p.costPrice == 0)
                xml.start("CostPrice").value("免费").end();
            else
                xml.start("CostPrice").value(String.valueOf(p.costPrice)).end();
            if (p.packagePrice == 0)
                xml.start("PackagePrice").value("免费").end();
            else
                xml.start("PackagePrice").value(String.valueOf(p.packagePrice)).end();
            if (p.spamRequest == Long.MAX_VALUE)
                xml.start("SpamRequest").value(String.valueOf("不限")).end();
            else
                xml.start("SpamRequest").value(String.valueOf(p.spamRequest)).end();
            if (p.porn == Long.MAX_VALUE)
                xml.start("Porn").value(String.valueOf("不限")).end();
            else
                xml.start("Porn").value(String.valueOf(p.porn)).end();
            xml.end();
        }
        xml.end();
        return xml.toString();
    }
    
    public static void payFromGlobalPortal(OwnerMeta owner, double amount, String ip,
            String orderSeq, boolean isPackage, String accountId, String description)
            throws Exception {
        DBPay dbPay = DBPay.getInstance();
        DBPrice dbPrice = DBPrice.getInstance();
        pay(owner.getId(), amount, ip, orderSeq);
        OpUser.deductionAll(accountId, amount, orderSeq, description);// 单位元
        if (isPackage) {
            DbUserPackage up = new DbUserPackage(orderSeq);
            dbPrice.userPackageSelectByOrderId(up);
            dbPrice.userPackageBillUpdate(up);
        } else {
            DbAccount dbAccount = new DbAccount(owner.getId());
            dbPay.accountInsert(dbAccount, owner, amount, orderSeq);
        }
        log.info("deposit to oos success , the owner name is:" + owner.getName()
                + " the amount is :" + amount);
    }
    
    public static String orderPackage(OwnerMeta owner, String startTime, int packageId,
            boolean fromBestPay, String ip, String accountId) throws Exception {
        DBPrice dbPrice = DBPrice.getInstance();
        String res = "";
        String orderId = String.valueOf(owner.getId()) + System.nanoTime();
        DbPackage dbPackage = new DbPackage(packageId);
        if (!dbPrice.packageSelect(dbPackage))
            throw new BaseException(400, "InvalidPackageId");
        if (dbPackage.isValid != 1)
            throw new BaseException(400, "InvalidPackageId");
        DbUserPackage dbUserPackage = new DbUserPackage(owner.getId());
        dbUserPackage.isPaid = 1;
        dbUserPackage.date = startTime;
        if (dbPrice.userPackageSelect(dbUserPackage))
            throw new BaseException(400, "PackageAlreadyExists");
        String endDate = TimeUtils.toYYYY_MM_dd(DateUtils.addMonths(
                DateParser.parseDate(startTime), (int) dbPackage.duration));
        dbUserPackage.date = endDate;
        if (dbPrice.userPackageSelect(dbUserPackage))
            throw new BaseException(400, "PackageAlreadyExists");
        dbUserPackage.startDate = startTime;
        dbUserPackage.endDate = endDate;
        if (dbPrice.userPackageSelectInRange(dbUserPackage))
            throw new BaseException(400, "PackageAlreadyExists");
        dbUserPackage.isPaid = 0;
        dbUserPackage.packageId = packageId;
        dbUserPackage.packageDiscount = 100;
        dbUserPackage.packageStart = startTime;
        dbUserPackage.orderId = orderId;
        dbUserPackage.packageEnd = endDate;
        dbUserPackage.storage = dbPackage.storage;
        dbUserPackage.flow = dbPackage.flow;
        dbUserPackage.ghRequest = dbPackage.ghRequest;
        dbUserPackage.otherRequest = dbPackage.otherRequest;
        dbUserPackage.roamFlow = dbPackage.roamFlow;
        dbUserPackage.roamUpload = dbPackage.roamUpload;
        dbUserPackage.isClose = 0;
        try {
            dbPrice.userPackageInsert(dbUserPackage);
        } catch (SQLException e) {
            log.error(e.getMessage(), e);
            throw new BaseException(500, "OrderPackageError");
        }
        if (fromBestPay) {
            res = pay(owner.getId(), dbPackage.packagePrice, ip, orderId);
        } else {
            payFromGlobalPortal(owner, dbPackage.packagePrice, ip, orderId, true, accountId,
                    "OrderPackage");
        }
        return res;
    }
    
    public static String getPackage(long ownerId) throws SQLException {
        DBPrice dbPrice = DBPrice.getInstance();
        DbUserPackage up = new DbUserPackage(ownerId);
        dbPrice.userPackageSelectByOwnerId(up);
        XmlWriter xml = new XmlWriter();
        xml.start("Packages");
        for (DbUserPackage p : up.ups) {
            DbPackage dbPackage = new DbPackage(p.packageId);
            dbPrice.packageSelect(dbPackage);
            xml.start("Package");
            xml.start("ID").value(String.valueOf(p.packageId)).end();
            xml.start("Name").value(dbPackage.name).end();
            xml.start("Storage").value(String.valueOf(dbPackage.storage)).end();
            xml.start("Flow").value(String.valueOf(dbPackage.flow)).end();
            if (dbPackage.ghRequest == Long.MAX_VALUE)
                xml.start("GHRequest").value(String.valueOf("不限")).end();
            else
                xml.start("GHRequest").value(String.valueOf(dbPackage.ghRequest / 10)).end();
            if (dbPackage.otherRequest == Long.MAX_VALUE)
                xml.start("OtherRequest").value(String.valueOf("不限")).end();
            else
                xml.start("OtherRequest").value(String.valueOf(dbPackage.otherRequest)).end();
            xml.start("Duration").value(String.valueOf(dbPackage.duration)).end();
            if (dbPackage.costPrice == 0)
                xml.start("CostPrice").value("免费").end();
            else
                xml.start("CostPrice").value(String.valueOf(dbPackage.costPrice)).end();
            if (dbPackage.packagePrice == 0)
                xml.start("PackagePrice").value("免费").end();
            else
                xml.start("PackagePrice").value(String.valueOf(dbPackage.packagePrice)).end();
            xml.start("PackageDiscount").value(String.valueOf(p.packageDiscount)).end();
            xml.start("PackageStart").value(p.packageStart).end();
            xml.start("PackageEnd").value(p.packageEnd).end();
            xml.start("IsPaid").value(p.isPaid == 1 ? "Paid" : "NotPaid").end();
            xml.start("IsClose").value(p.isClose == 1 ? "Close" : "Open").end();
            xml.start("OrderId").value(p.orderId).end();
            xml.end();
        }
        xml.end();
        return xml.toString();
    }
    
    // public static String getAccessSecretKey(long id) throws SQLException {
    // DB db = DB.getInstance();
    // DbAccessSecretKey asKey = new DbAccessSecretKey(id);
    // LinkedHashSet<DbAccessSecretKey> keys = db
    // .accessSecretKeySelectAll(asKey);
    // XmlWriter xml = new XmlWriter();
    // xml.start("Owner");
    // for (DbAccessSecretKey key : keys) {
    // xml.start("AccessKey").value(key.accessKey).end();
    // xml.start("SecretKey").value(key.getSecretKey()).end();
    // }
    // xml.end();
    // return xml.toString();
    // }
    public static void modifyUserInfo(OwnerMeta owner, String companyName, String companyAddress,
            String displayName, String mobilePhone, String phone, String email) throws Exception {
        if (!client.ownerSelect(owner))
            throw new BaseException(400, "NoSuchUser");
        owner.mobilePhone = mobilePhone;
        owner.phone = phone;
        owner.companyName = companyName;
        owner.companyAddress = companyAddress;
        owner.displayName = displayName;
        owner.email = email;
        checkUserInfo(owner);
        client.ownerUpdate(owner);
    }
    
    public static void closePackage(long ownerId, String orderId) throws SQLException,
            BaseException {
        DBPrice dbPrice = DBPrice.getInstance();
        DbUserPackage dbUserPackage = new DbUserPackage(orderId);
        if (!dbPrice.userPackageSelectByOrderId(dbUserPackage))
            throw new BaseException(400, "PackageNotExists");
        if (dbUserPackage.usrId != ownerId)
            throw new BaseException(400, "PackageNotExists");
        if (dbUserPackage.isClose == 1)
            throw new BaseException(400, "PackageAlreadyClosed");
        dbUserPackage.isClose = 1;
        try {
            dbPrice.userPackageUpdate(dbUserPackage);
        } catch (SQLException e) {
            log.error(e.getMessage(), e);
            throw new BaseException(500, "CloseUserPackageError");
        }
    }
    
    public static String getRgionMap() throws SQLException, 
            IOException {
        XmlWriter xml = new XmlWriter();
        xml.start("RegionMaps");
        
        //data region
        for (Iterator<String> iterator = DataRegions.getAllRegions().listIterator(); iterator
                .hasNext();) {
            String enName = iterator.next();
            xml.start("RegionMap");
            xml.start("RegionCode").value(enName).end();
            xml.start("RegionName").value(DataRegions.
                    getRegionDataInfo(enName).getCHName()).end();
            xml.end();
        }
        
        //meta region
        File fFile = new File(REGION_MAP_FILEPATH);
        if (fFile.exists()) {
            InputStreamReader reader = null;
            FileInputStream file = null;
            Properties p = new Properties();
            
            try{
                file = new FileInputStream(fFile);
                reader = new InputStreamReader(file, "utf-8");
                p.load(reader);
                for (Iterator<Entry<Object, Object>> iterator = p.entrySet().iterator(); iterator
                        .hasNext();) {
                    Entry<Object, Object> en = (Entry<Object, Object>) iterator.next();
                    xml.start("metaRegionMap");
                    xml.start("RegionCode").value((String) en.getKey()).end();
                    xml.start("RegionName").value((String) en.getValue()).end();
                    xml.end();
                }
            } finally {
                reader.close();
                file.close();
            }   
        }
        
        xml.end(); 
        return xml.toString();
    }
    
    public static String getOwnerBucketsAndRegions(OwnerMeta owner) throws Exception {
        // 所有数据域
        Set<String> allRegions = new HashSet<String>(DataRegions.getAllRegions());
        // 有标签的
        Set<String> availableRegions = client.getDataRegions(owner.getId());
        // 有可用带宽权限的
        List<String> permRegions = Utils.getABWPermissionRegions(owner.getId());
        // 有数据的
        List<String> hasDataRegions = new ArrayList<String>();
        String today = LocalDate.now().format(Misc.format_uuuu_MM_dd);
        Map<String, List<MinutesUsageMeta>> data = UsageResult.getSingleCommonUsageStatsQuery(owner.getId(), today, today, null,
                Utils.BY_DAY, allRegions, UsageMetaType.DAY_OWNER, Consts.ALL, true);
        for (String k : data.keySet()) {
            if (data.get(k).size() > 0)
                hasDataRegions.add(k);
        }

        JSONObject resultItem = new JSONObject();
        JSONArray regions = new JSONArray();
        JSONArray abwRegions = new JSONArray();
        for(String region : allRegions) {           
            //如果有数据或有标签，并集
            if (hasDataRegions.contains(region) || availableRegions.contains(region)) {
                JSONObject jo = new JSONObject();
                jo.put("regionCode", region);
                jo.put("regionName", DataRegions.getRegionDataInfo(region).getCHName());
                regions.put(jo);
            }
            //有权限且有标签和有权限且有数据的并集
            if ((permRegions.contains(region) && availableRegions.contains(region)) || 
                    (hasDataRegions.contains(region) && permRegions.contains(region))) {
                JSONObject jo = new JSONObject();
                jo.put("regionCode", region);
                jo.put("regionName", DataRegions.getRegionDataInfo(region).getCHName());
                abwRegions.put(jo);
            }
        }
        resultItem.put("regions", regions);
        resultItem.put("ABWregions", abwRegions);
        
        JSONObject buckets = new JSONObject();
        List<BucketMeta> bucketList = client.bucketList(owner.getId());
        if (bucketList != null && bucketList.size() > 0) {
            buckets.put("bucketNum", String.valueOf(bucketList.size()));
            // 创建时间晚于统计功能上线时间的bucket
            List<String> newBucketsNames = new LinkedList<String>();
            // 所有bucket
            List<String> allBucketsNames = new LinkedList<String>();
            for (BucketMeta bucket : bucketList) {
                allBucketsNames.add(bucket.name);
                if (bucket.createDate > Misc.formatyyyymmddhhmm(PeriodUsageStatsConfig.deployDate + " 23:59").getTime()) {
                    newBucketsNames.add(bucket.name);
                }
            }
            JSONArray ja = new JSONArray();
            for (String name : allBucketsNames) {
                JSONObject jo = new JSONObject();
                jo.put("bucketName", name);
                ja.put(jo);
            }
            buckets.put("allBucketsNames", ja);
            // 过滤掉线上已有的bucket，bucket级别的容量只支持统计功能上线后新建的bucket
            JSONArray ja2 = new JSONArray();
            for (String name : newBucketsNames) {
                JSONObject jo = new JSONObject();
                jo.put("bucketName", name);
                ja2.put(jo);
            }
            buckets.put("newBucketsNames", ja2);
        }
        resultItem.put("buckets", buckets);
        return resultItem.toString();
    }
    
    static String replicaMode;
    
    @SuppressWarnings("rawtypes")
    public static Pair checkTicket(HttpServletRequest req,HttpServletResponse resp,
            CompositeConfiguration config,String domain) throws BaseException, 
            MalformedURLException,IOException {
        String service = req.getParameter("service");
        String ticket = req.getParameter("ticket");
        String ticketCheckURL = req.getParameter("ticketCheckURL");
        
        if (service == null || service.equals("") || ticket == null || ticket.equals("") ||
                ticketCheckURL == null || ticketCheckURL.equals("")){
            throw new BaseException(400, "PortalTicketCheckError","Ticket check error.");
        }
             
        if (ticketCheckURL.charAt(ticketCheckURL.length()-1) == '/'){
            ticketCheckURL = ticketCheckURL.substring(0,ticketCheckURL.length()-1);
        }
        
        String host = ticketCheckURL.substring(ticketCheckURL.indexOf("//")+2);
        String path = "/serviceValidate?"+"service="+service+"&"+"ticket="+ticket;
        int port = config.getInt("portal.bssTicketCheckPort",80);
        String protocol = config.getString("portal.bssTicketCheckProtocol","http");
        URL url = new URL(protocol, host, port, path);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        
        conn.setRequestProperty("host", domain);
        conn.setRequestMethod("GET");
        conn.setUseCaches(false);
        conn.setDoInput(true);
        conn.connect();      
        
        int code = conn.getResponseCode(); 
        Pair result = null;
        if (code == 200){
            InputStream in = conn.getInputStream();
            try{
                String xmlDate = IOUtils.toString(in);
                String status = getXMLValue(xmlDate,"status");
                if ("fail".equals(status)){
                    result = new Pair<String,String>("fail",getXMLValue(xmlDate,"cas:authenticationFailure")); 
                } else {
                    String userName = getXMLValue(xmlDate,"cas:user");
                    String accountId = getXMLValue(xmlDate,"domainId");
                    //String userId = getXMLValue(xmlDate,"userId");
                    result = new Pair<String,Triple>("success",new Triple<String,String,String>(userName, accountId, ticket));
                }
            }finally{
                in.close();
            }
        }
        return result;
    }
    
    private static String getXMLValue(String xmlData,String key){
        String returnVal = "";
        
        if (key.equals("status")){
            return (xmlData.indexOf("Success")>-1?"success":"fail"); 
        }
        
        if (xmlData.indexOf(key)==-1) return "";
        
        int begin = -1;
        int end = -1;
        if (xmlData.indexOf("Success")>-1){
            begin = xmlData.indexOf(key)+key.length()+1;
            end = xmlData.indexOf("<", begin);
             
        } else if (xmlData.indexOf("SessionIndex")>-1){
            begin = xmlData.indexOf(key)+key.length()+1;
            end = xmlData.indexOf("<", begin);
             
        } else {
            begin = xmlData.indexOf(">", xmlData.indexOf("code="))+1;
            end   = xmlData.indexOf("<",begin);          
        }
               
        if (begin>-1 && end>-1){
            returnVal = xmlData.substring(begin, end).trim();
        } else {
            returnVal = "";
        }   
        
        return returnVal;
    }
    
    public static void loginFromGlobalPortalCAS(HttpServletRequest req, HttpServletResponse resp,
            Triple<String,String,String> userInfo,CompositeConfiguration config, Session<String, String> session) throws Exception {
        String userName = userInfo.first();
        String accountId = userInfo.second();
        String ticket = userInfo.third();
        
        OwnerMeta owner = OpUser.getOwnerInfo(accountId);
        if (owner == null)
            throw new BaseException(400, "NoSuchUserInPortal");
        
        //跳转上海的用户
        String shPortalDomain = config.getString("portal.shhaiOosDomain","oos.ctyun.cn");
        int shPortalPort = config.getInt("portal.shhaiOosPort",80); 
        if (userName != null && checkUserExists(owner.email,shPortalDomain,shPortalPort)) {
            throw new BaseException(403, "UserExistsInOldShHai");
        }
        
        //没有注册oos本地用户的情况将返回此错误代码 前台进行跳转
        if (!client.ownerSelect(owner)) 
            throw new BaseException(400, "PortalUserNotExistInOOS");
        //验证Portal登录的用户在initialAcocunt表中是否已经注册；
        OpUser.checkInitialAccountInfo(accountId);
        
        //私有资源池不同步登录信息
        if (!SynchroConfig.propertyLocalPool.synchroLogin) return;
        
        String sessionId = generatePortalUserSession(req, resp, session, owner.getName(),ticket);
        //资源池单点登录后，广播此用户session到其他资源池以便免密登录；
        BssAdapterClient.regsterUserSessionId(owner.name,Misc.getMd5(owner.getPwd()),
                sessionId,Utils.getIpAddr(req),BssAdapterConfig.localPool.ak,BssAdapterConfig.localPool.getSK());
        
        //获取用户允许登录的资源池list
        List<Pair<String,Pool>> userPools = BssAdapterClient.getAvailablePoolsAsListById(owner.getId(),
                BssAdapterConfig.localPool.ak,BssAdapterConfig.localPool.getSK());
        
        //用户可见数据域不包含中心资源池时，跳转到可见数据域中的第一个
        if (userPools!=null && userPools.stream().filter(p -> p.first().equals(
                BssAdapterConfig.name)).collect(Collectors.toList()).size()==0) {
            Pool aPool = userPools.get(0).second();
            
            String webPges = config.getString("website.webpages","oos");
            StringBuilder urlBuilder = new StringBuilder();
            urlBuilder.append(aPool.url).append("/").append(webPges)
                      .append("/ctyun/loginPortal.html")
                      .append("?relogin=1&userName=").append(owner.name)
                      .append("&userSession=").append(sessionId);
            log.info("Redirect login user ["+owner.name+"] to pool:" + aPool.name);
            resp.setHeader("LoginSuccToURL", ServiceUtils.urlEncode(urlBuilder.toString()));
            throw new BaseException(400, "LoginSuccToURL");
        }
        log.info("user login success:" + owner.name + ",sessionId="+sessionId);
    }

    private static boolean checkUserExists(String userName, String shPortalDomain, int shPortalPort) throws IOException {
        String date = TimeUtils.toGMTFormat(new Date());
        
        URL url = new URL("http://" + shPortalDomain + ":" + shPortalPort 
                + "/?email="+userName);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setUseCaches(false);
        conn.setRequestMethod("GET");
        conn.setRequestProperty("Host", shPortalDomain + ":" + shPortalPort);
        conn.setRequestProperty("Date", date);
        conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded; charset=UTF-8");
        conn.setRequestProperty("x-ctyun-verify-email", userName);
        //conn.setDoInput(true);
        int code = conn.getResponseCode();
        
        //无此用户
        if (code == 200) return false;
        
        //电商老用户
        if (code == 400) {
            String xml = IOUtils.toString(conn.getErrorStream());
            if (StringUtils.containsIgnoreCase(xml, "UserAlreadyExist")) {
                return true;
            }
        }
        
        return false; 
    }

    public static void logoutFromPortal(HttpServletRequest req,
            HttpServletResponse resp, Session<String, String> session) throws IOException, BaseException {
        InputStream in = req.getInputStream();
        try{
            String xmlDate = IOUtils.toString(in);
            String ticket = getXMLValue(xmlDate,"SessionIndex");
             
            if (ticket!=null && !"".equals(ticket)){
                String protalUserSession = session.getAndUpdate(ticket);
                log.info("Portal user: " + session.getAndUpdate(protalUserSession) + " logout success, sessionId is:" + protalUserSession);
                session.remove(protalUserSession);
                session.remove(ticket);
                BssAdapterClient.removeUserSessionId(protalUserSession,BssAdapterConfig.localPool.ak,
                        BssAdapterConfig.localPool.getSK(),Utils.getIpAddr(req));
            }
            resp.setStatus(200); 
        }finally{
            in.close();
        }
    }

    /**
     * 获取概览页面的当月、当日、昨日使用量
     * @param owner
     * @param resp
     * @throws Exception 
     */
    public static void getUsageCurrentMonth(OwnerMeta owner, HttpServletResponse resp) throws Exception {
        boolean ref = client.ownerSelect(owner);
        if (!ref)
            throw new BaseException(400, "InvalidAccessKeyId");
        //用户有标签数据域
        List<String> permittedRegions = new ArrayList<>(client.getDataRegions(owner.getId()));
        log.info("------proxy ownerid is " + owner.getId() + " permittedRegions: " + permittedRegions);
        Set<String> allRegions = new HashSet<String>(DataRegions.getAllRegions());
        log.info("------proxy ownerid is " + owner.getId() + " all regions: " + allRegions);
        allRegions.add(Consts.GLOBAL_DATA_REGION);
        //本月第一天
        String beginDate = LocalDate.now().with(TemporalAdjusters.firstDayOfMonth()).minusDays(1).toString();
        //当天
        String endDate = LocalDate.now().toString();
        //昨日
        String yesterday = LocalDate.now().minusDays(1).toString();
        // 获取时间范围内所有数据域的按天统计的用量
        Pair<Map<String, List<MinutesUsageMeta>>, Map<String, List<MinutesUsageMeta>>> usage = UsageResult
                .getSingleUsageStatsQuery(owner.getId(), beginDate, endDate, null, Utils.BY_DAY, allRegions, Consts.ALL, UsageMetaType.DAY_OWNER, false, true);
        Map<String, List<MinutesUsageMeta>> regionUsages = usage.first();
        //当日用量，包括全部数据域的用量和global的用量
        List<MinutesUsageMeta> todayUsage = new ArrayList<MinutesUsageMeta>();
        //昨日用量，包括全部数据域用量
        List<MinutesUsageMeta> yesterdayUsage = new ArrayList<MinutesUsageMeta>();
        //当月用量，包括全部数据域用量
        List<MinutesUsageMeta> monthUsage = new ArrayList<MinutesUsageMeta>();

        //todo 兼容前端旧版本逻辑 6.8升级完成后可删除
        boolean showStandardIa = OOSConfig.getFrontEndVersion() > 6_7;

        for (Entry<String, List<MinutesUsageMeta>> entry : regionUsages.entrySet()) {
            String region = entry.getKey();
            List<MinutesUsageMeta> v = entry.getValue();
            if (region.equals(Consts.GLOBAL_DATA_REGION)) {
                if (v.size() == 0) {
                    //如果所有数据域都没有数据
                    MinutesUsageMeta todayMeta = new MinutesUsageMeta(UsageMetaType.DAY_OWNER, region, owner.getId(), endDate, Consts.STORAGE_CLASS_STANDARD);
                    todayMeta.initStatsExceptConnection();
                    todayUsage.add(todayMeta);
                }
                for (MinutesUsageMeta m : v) {
                    //当日用量只需要global，其他不需要
                    if (m.time.equals(endDate)) {
                        if (Consts.STORAGE_CLASS_STANDARD.equals(m.storageType) || (Consts.STORAGE_CLASS_STANDARD_IA.equals(m.storageType) && showStandardIa)) {
                            todayUsage.add(m);
                        }
                    }
                }
                continue;
            }
            if (!permittedRegions.contains(region) && v.size() == 0) {
                log.info("-----proxy ownerid is " + owner.getId() + " no permitted and no data region is " + region);
                // 没有权限且没有数据
                continue;
            } else if (permittedRegions.contains(region) && v.size() == 0) {
                log.info("-----proxy ownerid is " + owner.getId() + " permitted but no data region is " + region);
                // 有权限，但是没有数据的数据域
                MinutesUsageMeta yesterdayMeta = new MinutesUsageMeta(UsageMetaType.DAY_OWNER, region, owner.getId(), yesterday, Consts.STORAGE_CLASS_STANDARD);
                yesterdayMeta.initStatsExceptConnection();
                yesterdayUsage.add(yesterdayMeta);
            }
            List<String> storageType2process = Utils.STORAGE_TYPES;
            if (!showStandardIa) {
                storageType2process = Lists.newArrayList(Consts.STORAGE_CLASS_STANDARD);
            }
            for (String st : storageType2process) {
                MinutesUsageMeta month = new MinutesUsageMeta(UsageMetaType.DAY_OWNER, region, owner.getId(), endDate, st);
                month.initStatsExceptConnection();
                //当前region本月所有按天统计数据，遍历叠加得到数据域当月总用量
                for (MinutesUsageMeta m : v) {
                    if (!st.equals(m.storageType)) {
                        continue;
                    }
                    if (m.time.equals(yesterday)) {
                        yesterdayUsage.add(m);
                    }
                    // 确认该数据是否属于当月
                    if (!m.time.contains(endDate.substring(0, endDate.lastIndexOf("-")))) {
                        continue;
                    }
                    month.sizeStats.size_peak = Math.max(month.sizeStats.size_peak, m.sizeStats.size_peak);
                    // 添加月度数据取回量统计
                    month.sizeStats.size_restore += m.sizeStats.size_restore;
                    if (m.flowStats != null) {
                        month.flowStats.sumAll(m.flowStats);
                    }
                    if (m.requestStats != null) {
                        month.requestStats.sumAll(m.requestStats);
                    }
                    if (m.codeRequestStats != null) {
                        month.codeRequestStats.sumAll(m.codeRequestStats);
                    }
                    if (m.time.equals(endDate)) {
                        //当月容量为当天的容量
                        month.sizeStats.size_total = m.sizeStats.size_total;
                    }
                }
                monthUsage.add(month);
            }
        }
        String count = buildUsageReq(permittedRegions, todayUsage, yesterdayUsage, monthUsage);
        if (count != null)
            resp.getWriter().write(count);
    }
    
    private static String buildUsageReq(List<String> permittedRegions, List<MinutesUsageMeta> todayUsage, List<MinutesUsageMeta> yesterdayUsage, List<MinutesUsageMeta> monthUsage) throws Exception {
        JSONObject usageJo = new JSONObject();
        JSONArray todayRegionJo = new JSONArray();
        JSONArray yesterdayRegionJo = new JSONArray();
        JSONArray monthRegionJo = new JSONArray();
        for (MinutesUsageMeta m : todayUsage) {
            JSONObject jo = new JSONObject();
            jo.put("currentGlobalTotalSize", m.sizeStats.size_total);
            jo.put("currentGlobalPeakSize", m.sizeStats.size_peak);
            jo.put("currentGlobalDownload", m.flowStats.flow_download + m.flowStats.flow_roamDownload);
            jo.put("currentGlobalUpload", m.flowStats.flow_upload + m.flowStats.flow_roamUpload);
            jo.put("currentGlobalGHReq", m.requestStats.req_get + m.requestStats.req_noNetGet + m.requestStats.req_head
                    + m.requestStats.req_noNetHead);
            jo.put("currentGlobalPPReq", m.requestStats.req_put + m.requestStats.req_noNetPut + m.requestStats.req_post + m.requestStats.req_noNetPost);
            jo.put("storageType", m.storageType);
            jo.put("restoreUsage", m.sizeStats.size_restore);
            todayRegionJo.put(jo);

            //todo 兼容旧前端代码，6.8上线完成后删除
            usageJo.put("currentGlobalTotalSize", m.sizeStats.size_total);
            usageJo.put("currentGlobalDownload", m.flowStats.flow_download + m.flowStats.flow_roamDownload);
            usageJo.put("currentGlobalUpload", m.flowStats.flow_upload + m.flowStats.flow_roamUpload);
            usageJo.put("currentGlobalGHReq", m.requestStats.req_get + m.requestStats.req_noNetGet + m.requestStats.req_head
                    + m.requestStats.req_noNetHead);
            usageJo.put("currentGlobalPPReq", m.requestStats.req_put + m.requestStats.req_noNetPut + m.requestStats.req_post + m.requestStats.req_noNetPost);
            //todo 兼容旧前端代码，6.8上线完成后删除
        }
        for (MinutesUsageMeta m : yesterdayUsage) {
            JSONObject jo = new JSONObject();
            jo.put("regionName", m.regionName);
            jo.put("peakSize", m.sizeStats.size_peak);
            jo.put("download", m.flowStats.flow_download + m.flowStats.flow_roamDownload);
            jo.put("upload", m.flowStats.flow_upload + m.flowStats.flow_roamUpload);
            jo.put("ghReq", m.requestStats.req_get + m.requestStats.req_noNetGet + m.requestStats.req_head + m.requestStats.req_noNetHead);
            jo.put("ppReq", m.requestStats.req_put + m.requestStats.req_noNetPut + m.requestStats.req_post + m.requestStats.req_noNetPost);
            jo.put("storageType", m.storageType);
            jo.put("restoreUsage", m.sizeStats.size_restore);
            yesterdayRegionJo.put(jo);
        }
        for (MinutesUsageMeta m : monthUsage) {
            JSONObject jo = new JSONObject();
            jo.put("regionName", m.regionName);
            jo.put("avgTotalSize", m.sizeStats.size_avgTotal);
            jo.put("peakSize", m.sizeStats.size_peak);
            jo.put("download", m.flowStats.flow_download + m.flowStats.flow_roamDownload);
            jo.put("upload", m.flowStats.flow_upload + m.flowStats.flow_roamUpload);
            jo.put("ghReq", m.requestStats.req_get + m.requestStats.req_noNetGet + m.requestStats.req_head + m.requestStats.req_noNetHead);
            jo.put("ppReq", m.requestStats.req_put + m.requestStats.req_noNetPut + m.requestStats.req_post + m.requestStats.req_noNetPost);
            jo.put("storageType", m.storageType);
            jo.put("restoreUsage", m.sizeStats.size_restore);
            monthRegionJo.put(jo);
        }
        usageJo.put("todayRegionUsage", todayRegionJo);
        usageJo.put("yesterdayRegionUsage", yesterdayRegionJo);
        usageJo.put("monthRegionUsage", monthRegionJo);
        usageJo.put("time", UsageResult.getStandardLastPeriodTime());
        return usageJo.toString();
    }
    
//    /** 用户月用量cache */
//    private static LoadingCache<String, OwnerUsage> ownerMouthUsageCache = CacheBuilder.newBuilder()
//            .expireAfterWrite(1, TimeUnit.HOURS)
//            .build(
//                new CacheLoader<String, OwnerUsage>() {
//                    @Override
//                    public OwnerUsage load(String key) throws Exception {
//                        String[] params = key.split(",");
//                        // 获取用户用量数据
//                        return getOwnerUsage(Long.valueOf(params[0]), params[1], params[2]);
//                    }
//                });
    
    /**
     * 获取用户指定时间段的用量
     * @param ownerId
     * @param dateBegin 
     * @param dateEnd
     * @return
     * @throws Exception
     */
    public static OwnerUsage getOwnerUsage(long ownerId, String dateBegin, String dateEnd) throws Exception {
        long start = System.currentTimeMillis();
        dateEnd = dateEnd + Character.MAX_VALUE;
        // 获取所有的region
        List<String> allRegions = DataRegions.getAllRegions();
        // 保存每个region的月用量，用于最后计算全局的月用量
        Map<String, OwnerUsage> regionUsageMap = new HashMap<>();
        // 计算用户每个region的当月用量
        for (String region : allRegions) {
            // 获取region下当月每天的使用量
            List<MinutesUsageMeta> metas = client.minutesUsageCommonStatsList(region, ownerId, null, UsageMetaType.DAY_OWNER, dateBegin, dateEnd, "");
            if (metas == null || metas.size() == 0) {
                continue;
            }
            // 对region下当月用量进行统计
            OwnerUsage regionMonthUsage = new OwnerUsage();
            for (MinutesUsageMeta meta : metas) {
                regionMonthUsage.avgTotalSize += meta.sizeStats.size_avgTotal;
                regionMonthUsage.transfer += meta.flowStats.flow_download;
                regionMonthUsage.roamFlow += meta.flowStats.flow_roamDownload;
                regionMonthUsage.ghRequest += meta.requestStats.req_get + meta.requestStats.req_head;
                regionMonthUsage.ppRequest += meta.requestStats.req_put + meta.requestStats.req_post;
            }
            // 对平均值进行计算，除以有数据天数
            regionMonthUsage.avgTotalSize = regionMonthUsage.avgTotalSize / metas.size();
            regionUsageMap.put(region, regionMonthUsage);
        }
        // 循环累加所有region的月用量
        OwnerUsage ownerMonthUsage = new OwnerUsage();
        for (OwnerUsage regionUsage : regionUsageMap.values()) {
            ownerMonthUsage.avgTotalSize += regionUsage.avgTotalSize;
            ownerMonthUsage.transfer += regionUsage.transfer;
            ownerMonthUsage.roamFlow += regionUsage.roamFlow;
            ownerMonthUsage.ghRequest += regionUsage.ghRequest;
            ownerMonthUsage.ppRequest += regionUsage.ppRequest;
        }
        log.info("getMouthUsage ownerId: " + ownerId + ", use time: " + (System.currentTimeMillis() - start) + "ms");
        return ownerMonthUsage;
    }
    
    /**
     * 用户用量
     */
    public static class OwnerUsage {
        public long totalSize;
        public long avgTotalSize;
        public long transfer;
        public long roamFlow;
        /** get类请求，包括get、head请求 */
        public long ghRequest;
        /** put类请求，包括put、post请求 */
        public long ppRequest;
    }
    
    public static String getAvailablePools(HttpServletRequest req, HttpServletResponse resp,
            Session<String, String> session) throws BaseException, IOException {
        String poolsXML = BssAdapterClient.getAvailablePools(BssAdapterConfig.localPool.ak , 
                BssAdapterConfig.localPool.getSK());
        
        if (poolsXML == null) 
            poolsXML = "";
        return poolsXML;
    }

    public static String getUserPermission(HttpServletRequest req,
            HttpServletResponse resp, Session<String, String> session) throws Exception {
        OwnerMeta owner = Portal.getOwnerFromSession(session, Utils.getSessionId(req));
        int dataStatus = getDataStatus(owner.getId());
        resp.setHeader("userDataStatus", String.valueOf(dataStatus));
        return owner.auth.getJsonStringWithOutUserId();
    }

    /**
     * 本地资源池响应中心BssAdapterServer广播登录
     */
    public static void loginFromBssAdapter(HttpServletRequest req,
            HttpServletResponse resp, Session<String, String> session, OwnerMeta owner) throws Exception {
        String email = req.getParameter(Parameters.EMAIL);
        String password = req.getParameter(Parameters.PASSWORD);
        String userAvaiablePools = req.getHeader(WebsiteHeader.POOL_OF_USERTOROLE);
        String userName = email;
        if (userName == null || password == null)
            throw new BaseException();
        owner.name = userName;
        owner.setId(owner.getIdByName(userName));
        //表明账户有效池列表中是否包含当前池
        boolean  isAvailablePool = isAvailablePool(userAvaiablePools);
        //如果账户在本地不存在，且当前池不是该账户有效池
        if (!client.ownerSelect(owner) && !isAvailablePool) {
            throw new BaseException(403, "InvalidAccessKeyId");
        } else {
            //当前池是账户的有效池，从中心获取账户，并保存到本地
            downloadOwnerFromBssAdapter(owner);
        }
        if (owner.verify != null)
            throw new BaseException(403, "NotVerifyEmail");

        String sessionId = req.getParameter(WebsiteHeader.LOGIN_SESSIONID);
        if (sessionId == null || "".equals(sessionId.trim())) {
            throw new BaseException(403, "InvalidSessionId");
        }
        log.debug("user " + owner.name + " loginFromBssAdapter");
        session.update(sessionId,userName);
        saveLastLoginInfo(session,sessionId);
        client.getOrCreateBuiltInAK(owner);
        String requestIpAddr = Utils.getIpAddr(req);
        updateUserInfo(owner, requestIpAddr, System.currentTimeMillis());
        resp.addCookie(new Cookie(WebsiteHeader.LOGIN_SESSIONID, sessionId));
        resp.addCookie(new Cookie(WebsiteHeader.LOGIN_USER, owner.displayName));
        resp.addHeader(WebsiteHeader.LOGIN_SESSIONID, sessionId);
        resp.addHeader(WebsiteHeader.LOGIN_USER, userName);
    }
    /**
     * @param userAvaiablePools 中心池账户可用池name组成的列表,格式:name1,name2,name3
     * @return 返回当前池是否在可用池列表当中
     */
    private static boolean isAvailablePool(String userAvaiablePools) {
        log.info("userAvaiablePools: " + userAvaiablePools);
        log.info("bssAdapterConfig.localPool.name: " + BssAdapterConfig.localPool.name);
        boolean localAvaiablePool = false;
        if (StringUtils.isNotEmpty(userAvaiablePools)
                && Arrays.asList(userAvaiablePools.split(",")).contains(BssAdapterConfig.localPool.name)) {
            localAvaiablePool = true;
        }
        return localAvaiablePool;
    }
    /**
     * 本地资源池响应中心BssAdapterServer广播注销
     */

    public static void logoutFromBssAdapter(HttpServletRequest req,
            HttpServletResponse resp, Session<String, String> session) {
        String sessionId = req.getParameter(WebsiteHeader.LOGIN_SESSIONID);
        if (sessionId != null) {
            session.remove(sessionId);
        }
    }
    
}