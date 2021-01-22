package cn.ctyun.oos.bssAdapter.server;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLSession;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.input.SAXBuilder;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.XML;
import org.xml.sax.InputSource;

import com.amazonaws.auth.SigningAlgorithm;
import com.amazonaws.services.s3.Headers;
import com.amazonaws.services.s3.internal.RestUtils;
import com.amazonaws.services.s3.internal.XmlWriter;

import cn.ctyun.common.BaseException;
import cn.ctyun.common.Consts;
import cn.ctyun.common.Session;
import cn.ctyun.common.cache.Cache;
import cn.ctyun.common.cache.LocalCache;
import cn.ctyun.oos.common.ErrorMessage;
import cn.ctyun.oos.common.OOSRequest;
import cn.ctyun.oos.common.Parameters;
import cn.ctyun.oos.common.Utils;
import cn.ctyun.oos.common.WebsiteHeader;
import cn.ctyun.oos.metadata.BucketOwnerConsistencyMeta.BucketOwnerConsistencyType;
import cn.ctyun.oos.metadata.OwnerMeta;
import cn.ctyun.oos.metadata.RoleMeta;
import cn.ctyun.oos.metadata.UserToRoleMeta;
import cn.ctyun.oos.server.conf.BssAdapterConfig;
import cn.ctyun.oos.server.conf.BssAdapterConfig.Pool;
import cn.ctyun.oos.server.conf.SynchroConfig;
import cn.ctyun.oos.server.conf.SynchroConfig.SynchroProperty;
import cn.ctyun.oos.server.db.DbInitialAccount;
import common.time.TimeUtils;
import common.tuple.Pair;
import common.util.BlockingExecutor;

/**
 * 中心节点的一个客户端类，方便各个资源池的自门户服务和业务管理服务中调用相应的中心节点的服务； 
 * 函数：
 * 1)create:在中心资源池中创建用户或者bucket的源数据; 2)delete:删除bucket
 * 3)registerUserFromProxy:由自门户页面发起的用户注册的服务
 * 4)modifyUser：修改用户信息（密码，是否激活的状态，修改密码等） 5)modifyUserPermission：用以修改电商用户权限的接口
 * 6)getAvailablePools:获取目前用户可以用于资源池切换的资源 7)ownerSelect：从个中心节点 资源池获取用户基本数据
 * 8)regsterUserSessionId：向各个资源池注册用户在一个资源池登录的session，方便切换资源池；
 */
public class BssAdapterClient {
    private static Log log = LogFactory.getLog(BssAdapterClient.class);
    private static String globalPoolsXML = null;
    private static ScheduledThreadPoolExecutor scheduleThreadPool = 
            new ScheduledThreadPoolExecutor(1);
    
    private static ScheduledThreadPoolExecutor scheduleCacheThread = 
            new ScheduledThreadPoolExecutor(1);
    private static BlockingExecutor exec = new BlockingExecutor(BssAdapterConfig.coreThreadsNum,
            BssAdapterConfig.maxThreadsNum, BssAdapterConfig.queueSize,
            BssAdapterConfig.aliveTime, "cache-ownerRegions");
    private static LocalCache ownerRegionsCache = new LocalCache() {
        public void init() {
            scheduleCacheThread.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    try {
                        Set<Entry<Object, Cache>> kvs = cacheMap.entrySet();
                        final AtomicInteger count = new AtomicInteger(0);
                        for (Entry<Object, Cache> kv : kvs) {
                            final Object k = kv.getKey();
                            final Cache v = kv.getValue();
                            final long st = v.lastUpdate.get();
                            if (System.currentTimeMillis() - st < BssAdapterConfig.userRegionCacheTimeout)
                                continue;
                            count.incrementAndGet();
                            exec.execute(new Thread() {
                                public void run() {
                                    try {
                                        List<String> value = null;
                                        List<String> userRegion = Utils.getAvailableDataRegions((long)k);
                                        if (userRegion!=null)
                                            value = userRegion;
                                        if (value != null) {
                                            if (v.lastUpdate.compareAndSet(st,
                                                    System.currentTimeMillis()))
                                                v.value = value;
                                        } else
                                            invalidate(String.valueOf(k));
                                    } catch (Throwable t) {
                                        log.error(t.getMessage(), t);
                                    } finally {
                                        count.decrementAndGet();
                                    }
                                }
                            });
                        }
                        while (count.get() != 0)
                            try {
                                Thread.sleep(100);
                            } catch (Throwable t) {
                                log.error(t.getMessage(), t);
                            }
                    } catch (Throwable t) {
                        log.error(t.getMessage(), t);
                    }
                }
                
            }, 0, BssAdapterConfig.userRegionCacheTimeout, TimeUnit.MILLISECONDS);
        }
    };
    
    static {
        HttpsURLConnection.setDefaultHostnameVerifier(new HostnameVerifier() {
            @Override
            public boolean verify(String hostname, SSLSession session) {
                return true;
            }
        });
        
        scheduleThreadPool.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    getAvailablePools(BssAdapterConfig.localPool.ak,
                            BssAdapterConfig.localPool.getSK());
                } catch (Exception e) {
                    log.error("load available pool error:"+e.getMessage(), e);
                }
            }
        }, 0, 5, TimeUnit.MINUTES);
    }
    
    public static String getGlobalPoolsXML(String ak,String sk) throws IOException, BaseException {
        if (globalPoolsXML == null || globalPoolsXML.isEmpty()) {
            getAvailablePools(ak,sk);
        }

        return globalPoolsXML;
    } 

    public static boolean create(String name, String ak, String sk,
            BucketOwnerConsistencyType type) throws IOException, BaseException {
        String action = null;
        switch (type) {
        case BUCKET:
            action = BssAdapterConsts.ACTION_CREATE_BUCKET;
            break;
        case OWNER:
            action = BssAdapterConsts.ACTION_CREATE_USER;
            break;
        }
        URL url = new URL(BssAdapterConfig.protocol, BssAdapterConfig.ip,
                BssAdapterConfig.port, "/?Action=" + action);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        String date = TimeUtils.toGMTFormat(new Date());
        String resourcePath = Utils.toResourcePath(null, null, false);
        conn.setRequestProperty("Host",
                BssAdapterConfig.ip + ":" + BssAdapterConfig.port);
        switch (type) {
        case OWNER:
            conn.setRequestProperty(BssAdapterConsts.HEADER_OWNER, name);
        case BUCKET:
            conn.setRequestProperty(BssAdapterConsts.HEADER_BUCKET, name);
        }
        conn.setRequestMethod("PUT");
        conn.setRequestProperty("Date", date);
        conn.setRequestProperty("Connection", "close");
        @SuppressWarnings("unchecked")
        String canonicalString = RestUtils.makeS3CanonicalString(
                conn.getRequestMethod(), resourcePath, new OOSRequest(conn),null);
        String signature = Utils.sign(canonicalString, sk,
                SigningAlgorithm.HmacSHA1);
        String authorization = "AWS " + ak + ":" + signature;
        conn.setRequestProperty("Authorization", authorization);
        conn.setConnectTimeout(BssAdapterConfig.connectTimeout);
        conn.setReadTimeout(BssAdapterConfig.readTimeout);
        conn.connect();
        int code = conn.getResponseCode();
        if (code != 200) {
            log.error("code:" + code + " name:" + name + " type:" + type);
            if (code == 409) {
                String pool = conn.getHeaderField(BssAdapterConsts.HEADER_POOL);
                if (pool != null
                        && pool.equals(BssAdapterConfig.localPool.name)) {
                    log.info("bucket|owner " + name + " is already in pool "
                            + pool);
                    return true;
                }
                throw new BaseException(409, "BucketOwnerAlreadyExists",
                        "bucket|owner already exists in pool " + pool);
            } else {
                log.error("create " + type + " failed, code: " + code);
                throw new BaseException(500, ErrorMessage.ERROR_CODE_500);
            }
        }
        return true;
    }

    public static void delete(String bucketName, String ak, String sk)
            throws IOException, BaseException {
        URL url = new URL(BssAdapterConfig.protocol, BssAdapterConfig.ip,
                BssAdapterConfig.port,"/?Action=" + BssAdapterConsts.ACTION_DELETE_BUCKET);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        String date = TimeUtils.toGMTFormat(new Date());
        String resourcePath = Utils.toResourcePath(null, null, false);
        conn.setRequestMethod("DELETE");
        conn.setRequestProperty("Date", date);
        conn.setRequestProperty("Host", BssAdapterConfig.ip + ":" + BssAdapterConfig.port);
        conn.setRequestProperty(BssAdapterConsts.HEADER_BUCKET, bucketName);
        conn.setRequestProperty("Connection", "close");
        @SuppressWarnings("unchecked")
        String canonicalString = RestUtils.makeS3CanonicalString(
                conn.getRequestMethod(), resourcePath, new OOSRequest(conn),
                null);
        String signature = Utils.sign(canonicalString, sk,
                SigningAlgorithm.HmacSHA1);
        String authorization = "AWS " + ak + ":" + signature;
        conn.setRequestProperty("Authorization", authorization);
        conn.setDoOutput(true);
        conn.setConnectTimeout(BssAdapterConfig.connectTimeout);
        conn.setReadTimeout(BssAdapterConfig.readTimeout);
        conn.connect();
        int code = conn.getResponseCode();
        if (code != 200) {
            log.error("code:" + code + " name:" + bucketName);
            throw new BaseException(500, "DeleteFailed");
        }
    }

    public static boolean registerUserFromProxy(String name, String ak,
            String sk, OwnerMeta user, SourceTYPE registerSource,
            String accountId, String userId, String orderId,
            String[] excludePoolNames) throws IOException, BaseException {
        String action = BssAdapterConsts.ACTION_REGISTER_USER_PROXY;// 来自自服务门户的注册请求
        if (registerSource != null && registerSource == SourceTYPE.BSS) {
            action = BssAdapterConsts.ACTION_REGISTER_USER_BSS;// 来自BSS平台的注册请求
            action = action.concat("&accountId=" + accountId + "&userId="
                    + userId + "&orderId=" + orderId);
        }

        URL url = new URL(BssAdapterConfig.protocol, BssAdapterConfig.ip,
                BssAdapterConfig.port, "/?Action=" + action);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        String date = TimeUtils.toGMTFormat(new Date());
        String resourcePath = Utils.toResourcePath(null, null, false);
        conn.setRequestProperty("x-portaloos-add-user", "1");
        conn.setRequestProperty("x-portaloos-exclude-pools",String.join(",", excludePoolNames));
        conn.setRequestProperty("Host",
                BssAdapterConfig.ip + ":" + BssAdapterConfig.port);
        conn.setRequestProperty(BssAdapterConsts.HEADER_OWNER, name);
        conn.setRequestMethod("PUT");
        conn.setRequestProperty("Date", date);
        conn.setRequestProperty("Connection", "close");
        @SuppressWarnings("unchecked")
        String canonicalString = RestUtils.makeS3CanonicalString(
                conn.getRequestMethod(), resourcePath, new OOSRequest(conn),
                null);
        String signature = Utils.sign(canonicalString, sk,
                SigningAlgorithm.HmacSHA1);
        String authorization = "AWS " + ak + ":" + signature;
        conn.setRequestProperty("Authorization", authorization);
        conn.setDoOutput(true);
        conn.setConnectTimeout(BssAdapterConfig.connectTimeout);
        conn.setReadTimeout(BssAdapterConfig.readTimeout);
        conn.connect();
        try (OutputStream out = conn.getOutputStream()) {
            out.write(user.getJsonRow());
            out.flush();
        }
        int code = conn.getResponseCode();
        if (code != 200) {
            log.error("code:" + code + " name:" + name + " type:"
                    + BucketOwnerConsistencyType.OWNER);
            if (code == 409) {
                log.info("owner " + name + " is already exists.");
            } else {
                log.error("create owner failed, code: " + code);
                throw new BaseException(500, ErrorMessage.ERROR_CODE_500);
            }
            return false;
        }
        return true;
    }

    public static boolean modifyUser(String ak, String sk, OwnerMeta user,
            SourceTYPE sourceType, UpdateType updateType,String[] excludePoolNames) throws IOException, BaseException {
        String action = BssAdapterConsts.ACTION_UPDATE_USER;// 来自自服务门户的注册请求

        URL url = new URL(BssAdapterConfig.protocol, BssAdapterConfig.ip,
                BssAdapterConfig.port, "/?Action=" + action);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        String date = TimeUtils.toGMTFormat(new Date());
        String resourcePath = Utils.toResourcePath(null, null, false);
        conn.setRequestProperty("x-portaloos-modify-user",updateType.toString());
        conn.setRequestProperty("x-portaloos-exclude-pools",String.join(",", excludePoolNames));
        conn.setRequestProperty(Parameters.REGISTER_SOURCE_TYPE,
                sourceType.toString());
        conn.setRequestProperty("Host",
                BssAdapterConfig.ip + ":" + BssAdapterConfig.port);
        conn.setRequestProperty(BssAdapterConsts.HEADER_OWNER, user.name);
        conn.setRequestMethod("PUT");
        conn.setRequestProperty("Date", date);
        conn.setRequestProperty("Connection", "close");
        @SuppressWarnings("unchecked")
        String canonicalString = RestUtils.makeS3CanonicalString(
                conn.getRequestMethod(), resourcePath, new OOSRequest(conn),null);
        String signature = Utils.sign(canonicalString, sk,
                SigningAlgorithm.HmacSHA1);
        String authorization = "AWS " + ak + ":" + signature;
        conn.setRequestProperty("Authorization", authorization);
        conn.setDoOutput(true);
        conn.setConnectTimeout(BssAdapterConfig.connectTimeout);
        conn.setReadTimeout(BssAdapterConfig.readTimeout);
        conn.connect();
        try (OutputStream out = conn.getOutputStream()) {
            out.write(user.getJsonRow());
            out.flush();
        }
        int code = conn.getResponseCode();
        if (code != 200) {
            log.error("Fail to update user's password:" + user.name);
            return false;
        }
        return true;
    }

    public static boolean modifyUserPermission(String ak, String sk,
            OwnerMeta user, SourceTYPE sourceType, UpdateType updateType)
            throws IOException, BaseException {
        String action = BssAdapterConsts.ACTION_PERMISSION_REQUEST;

        URL url = new URL(BssAdapterConfig.protocol, BssAdapterConfig.ip,
                BssAdapterConfig.port, "/?Action=" + action);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        String date = TimeUtils.toGMTFormat(new Date());
        String resourcePath = Utils.toResourcePath(null, null, false);
        conn.setRequestProperty("x-portaloos-modify-user",updateType.toString());
        conn.setRequestProperty(Parameters.REGISTER_SOURCE_TYPE,sourceType.toString());
        conn.setRequestProperty("Host",BssAdapterConfig.ip + ":" + BssAdapterConfig.port);
        conn.setRequestMethod("PUT");
        conn.setRequestProperty("Date", date);
        conn.setRequestProperty("Connection", "close");

        @SuppressWarnings("unchecked")
        String canonicalString = RestUtils.makeS3CanonicalString(
                conn.getRequestMethod(), resourcePath, new OOSRequest(conn),null);
        String signature = Utils.sign(canonicalString, sk,
                SigningAlgorithm.HmacSHA1);
        String authorization = "AWS " + ak + ":" + signature;
        conn.setRequestProperty("Authorization", authorization);
        conn.setDoOutput(true);
        conn.setConnectTimeout(BssAdapterConfig.connectTimeout);
        conn.setReadTimeout(BssAdapterConfig.readTimeout);
        conn.connect();
        try (OutputStream out = conn.getOutputStream()) {
            out.write(user.getJsonRow());
            out.flush();
        }
        int code = conn.getResponseCode();
        if (code != 200) {
            log.error("Fail to update user's permission:" + user.name);
            return false;
        }
        return true;
    }

    public static List<Pair<String,Pool>> getAvailablePoolsAsList(String ak, String sk) 
            throws BaseException, IOException, JDOMException {
        return createPoolList(getGlobalPoolsXML(ak,sk));
    }
    
    public static String getAvailablePools(String ak, String sk) 
            throws IOException, BaseException{
        URL url = new URL(BssAdapterConfig.protocol, BssAdapterConfig.ip,
                BssAdapterConfig.port,"/?Action=" + BssAdapterConsts.ACTION_GET_POOLS);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        String date = TimeUtils.toGMTFormat(new Date());
        String resourcePath = Utils.toResourcePath(null, null, false);
        conn.setRequestMethod("GET");
        conn.setRequestProperty("Date", date);
        conn.setRequestProperty("Host", BssAdapterConfig.ip + ":" + BssAdapterConfig.port);
        conn.setRequestProperty("Connection", "close");
        @SuppressWarnings("unchecked")
        String canonicalString = RestUtils.makeS3CanonicalString(
                conn.getRequestMethod(), resourcePath, new OOSRequest(conn),null);
        String signature = Utils.sign(canonicalString, sk,
                SigningAlgorithm.HmacSHA1);
        String authorization = "AWS " + ak + ":" + signature;
        conn.setRequestProperty("Authorization", authorization);
        conn.setUseCaches(false);
        conn.setDoInput(true);
        conn.setConnectTimeout(BssAdapterConfig.connectTimeout);
        conn.setReadTimeout(BssAdapterConfig.readTimeout);
        conn.connect();
        int code = conn.getResponseCode();
        InputStream ip = null;

        if (code == 200) {
            try {
                ip = conn.getInputStream();
                globalPoolsXML = IOUtils.toString(ip, Consts.CS_UTF8);
                return globalPoolsXML;
            } finally {
                if (ip != null) {
                    ip.close();
                }
            }
        } else {
            log.error("GetPoolsFailed code:" + code);
            
            XmlWriter xml = new XmlWriter();
            xml.start("pools");
            xml.start("pool");
            xml.start("Name").value(BssAdapterConfig.localPool.name).end();
            xml.start("chName").value(BssAdapterConfig.localPool.chName).end();
            xml.start("url").value(BssAdapterConfig.localPool.url).end();
            xml.start("proxyPort").value(BssAdapterConfig.localPool.proxyPort + "").end();
            SynchroProperty prop = SynchroConfig.getPropertyByName(BssAdapterConfig.localPool.name);
            if (prop != null) {
                xml.start("synchroLogin").value(prop.synchroLogin + "").end();                
            }
            xml.end();
            xml.end();
            globalPoolsXML = Consts.XML_HEADER + xml.toString();
            
            return globalPoolsXML;
        }
    } 

    public static boolean ownerSelect(OwnerMeta user, String ak, String sk,
            DbInitialAccount dbInitialAccount)
            throws BaseException, IOException {
        URL url = new URL(BssAdapterConfig.protocol, BssAdapterConfig.ip,
                BssAdapterConfig.port,"/?Action=" + BssAdapterConsts.ACTION_GET_USER);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        String date = TimeUtils.toGMTFormat(new Date());
        String resourcePath = Utils.toResourcePath(null, null, false);
        conn.setRequestMethod("POST");
        conn.setRequestProperty("Content-Type",
                "application/x-www-form-urlencoded");
        conn.setRequestProperty("Date", date);
        conn.setRequestProperty("Host", BssAdapterConfig.ip + ":" + BssAdapterConfig.port);
        conn.setRequestProperty("Connection", "close");
        @SuppressWarnings("unchecked")
        String canonicalString = RestUtils.makeS3CanonicalString(
                conn.getRequestMethod(), resourcePath, new OOSRequest(conn),null);
        String signature = Utils.sign(canonicalString, sk,
                SigningAlgorithm.HmacSHA1);
        String authorization = "AWS " + ak + ":" + signature;
        conn.setRequestProperty("Authorization", authorization);
        conn.setUseCaches(false);
        conn.setDoInput(true);
        conn.setDoOutput(true);
        conn.setConnectTimeout(BssAdapterConfig.connectTimeout);
        conn.setReadTimeout(BssAdapterConfig.readTimeout);
        conn.connect();
        try (OutputStream out = conn.getOutputStream()) {
            //todo 未指定编码，在客户端和服务端默认编码不同时可能出现乱码
            out.write("name=".concat(user.name).getBytes());
            out.flush();
        }

        int code = conn.getResponseCode();
        InputStream ip = null;

        if (code == 200) {
            try {
                ip = conn.getInputStream();
                user.readFields(IOUtils.toByteArray(ip));
    
                // 判断并获取BSS用户的mysql注册信息
                if (dbInitialAccount != null && conn.getHeaderField("accountId") != null) {
                    dbInitialAccount.accountId = StringUtils.defaultIfEmpty(conn.getHeaderField("accountId"), "");
                    dbInitialAccount.userId = StringUtils.defaultIfEmpty(conn.getHeaderField("userId"), "");
                    dbInitialAccount.orderId = StringUtils.defaultIfEmpty(conn.getHeaderField("orderId"), "");
                } else {
                    dbInitialAccount = null;
                }
                return true;
            } finally {
                if (ip != null) {
                    ip.close();
                }
            } 
        } else {
            log.error("code:" + code);
            return false;
        }
    }

    public static boolean regsterUserSessionId(String userName, String passwd,
            String sessionId,String requestIpAddr , String ak, String sk)
            throws JSONException, IOException, BaseException {
        String action = BssAdapterConsts.ACTION_PUT_USER_SESSION;

        URL url = new URL(BssAdapterConfig.protocol, BssAdapterConfig.ip,
                BssAdapterConfig.port, "/?Action=" + action);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        String date = TimeUtils.toGMTFormat(new Date());
        String resourcePath = Utils.toResourcePath(null, null, false);
        conn.setRequestProperty(WebsiteHeader.LOGIN_USER_GLOBAL, "login");
        conn.setRequestProperty("Host",BssAdapterConfig.ip + ":" + BssAdapterConfig.port);
        conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
        conn.setRequestMethod("POST");
        conn.setRequestProperty("Date", date);
        conn.setRequestProperty("x-forwarded-for", requestIpAddr);
        conn.setRequestProperty("Connection", "close");

        @SuppressWarnings("unchecked")
        String canonicalString = RestUtils.makeS3CanonicalString(
                conn.getRequestMethod(), resourcePath, new OOSRequest(conn),
                null);
        String signature = Utils.sign(canonicalString, sk,
                SigningAlgorithm.HmacSHA1);
        String authorization = "AWS " + ak + ":" + signature;
        conn.setRequestProperty("Authorization", authorization);
        conn.setRequestProperty("RequestPoolName", BssAdapterConfig.localPool.name);
        conn.setDoOutput(true);
        conn.setConnectTimeout(BssAdapterConfig.connectTimeout);
        conn.setReadTimeout(BssAdapterConfig.readTimeout);
        conn.connect();

        StringBuffer parameters = new StringBuffer();
        parameters.append(Parameters.EMAIL + "=" + userName + "&")
                .append(Parameters.PASSWORD + "=" + passwd + "&")
                .append(WebsiteHeader.LOGIN_SESSIONID + "=" + sessionId);

        try (OutputStream out = conn.getOutputStream()) {
            out.write(Bytes.toBytes(parameters.toString()));
            out.flush();
        }
        int code = conn.getResponseCode();
        if (code != 200) {
            log.error("Fail to register user's sessionid:" + sessionId
                    + " for user:" + userName);
            return false;
        }
        return true;
    }

    public static boolean removeUserSessionId(String sessionId, String ak,
            String sk , String requestIpAddr) throws IOException, BaseException {
        String action = BssAdapterConsts.ACTION_PUT_USER_SESSION;

        URL url = new URL(BssAdapterConfig.protocol, BssAdapterConfig.ip,
                BssAdapterConfig.port, "/?Action=" + action);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        String date = TimeUtils.toGMTFormat(new Date());
        String resourcePath = Utils.toResourcePath(null, null, false);
        conn.setRequestProperty(WebsiteHeader.LOGIN_USER_GLOBAL, "logout");
        conn.setRequestProperty("Host", BssAdapterConfig.ip + ":" + BssAdapterConfig.port);
        conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
        conn.setRequestMethod("POST");
        conn.setRequestProperty("Date", date);
        conn.setRequestProperty("x-forwarded-for", requestIpAddr);
        conn.setRequestProperty("RequestPoolName", BssAdapterConfig.localPool.name);
        conn.setRequestProperty("Connection", "close");

        @SuppressWarnings("unchecked")
        String canonicalString = RestUtils.makeS3CanonicalString(
                conn.getRequestMethod(), resourcePath, new OOSRequest(conn),null);
        String signature = Utils.sign(canonicalString, sk,
                SigningAlgorithm.HmacSHA1);
        String authorization = "AWS " + ak + ":" + signature;
        conn.setRequestProperty("Authorization", authorization);
        conn.setDoOutput(true);
        conn.setConnectTimeout(BssAdapterConfig.connectTimeout);
        conn.setReadTimeout(BssAdapterConfig.readTimeout);
        conn.connect();

        StringBuilder parameters = new StringBuilder();
        parameters.append(WebsiteHeader.LOGIN_SESSIONID).append("=").append(sessionId);

        try (OutputStream out = conn.getOutputStream()) {
            out.write(Bytes.toBytes(parameters.toString()));
            out.flush();
        }
        int code = conn.getResponseCode();
        if (code != 200) {
            log.error("Fail to remove user's sessionid:" + sessionId);
            return false;
        }
        return true;
    }

    public static boolean refreshUserSessionExpTime(String sessionId, String ak,
                                                    String sk) throws IOException, BaseException {
        String action = BssAdapterConsts.ACTION_REFRESH_USER_SESSION_EXPTIME;

        URL url = new URL(BssAdapterConfig.protocol, BssAdapterConfig.ip,
                BssAdapterConfig.port, "/?Action=" + action);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        String date = TimeUtils.toGMTFormat(new Date());
        String resourcePath = Utils.toResourcePath(null, null, false);
        conn.setRequestProperty("Host",BssAdapterConfig.ip + ":" + BssAdapterConfig.port);
        conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
        conn.setRequestMethod("POST");
        conn.setRequestProperty("Date", date);
        conn.setRequestProperty("Connection", "close");

        @SuppressWarnings("unchecked")
        String canonicalString = RestUtils.makeS3CanonicalString(
                conn.getRequestMethod(), resourcePath, new OOSRequest(conn),
                null);
        String signature = Utils.sign(canonicalString, sk,
                SigningAlgorithm.HmacSHA1);
        String authorization = "AWS " + ak + ":" + signature;
        conn.setRequestProperty("Authorization", authorization);
        conn.setRequestProperty("RequestPoolName", BssAdapterConfig.localPool.name);
        conn.setDoOutput(true);
        conn.setConnectTimeout(BssAdapterConfig.connectTimeout);
        conn.setReadTimeout(BssAdapterConfig.readTimeout);
        conn.connect();

        StringBuilder parameters = new StringBuilder();
        parameters.append(WebsiteHeader.LOGIN_SESSIONID).append("=").append(sessionId);

        try (OutputStream out = conn.getOutputStream()) {
            out.write(Bytes.toBytes(parameters.toString()));
            out.flush();
        }
        int code = conn.getResponseCode();
        if (code != 200) {
            log.error("Fail to refresh global pool user session expTime, user's sessionid:" + sessionId);
            return false;
        }
        return true;
    }

    public static void modifyRole(String ak,String sk,TaskType type, String roleId,
            RoleMeta roleMeta,String[] excludePoolNames, CallbackListener listener) throws JSONException, IOException,BaseException {
        String action = BssAdapterConsts.ACTION_PUT_ROLE;

        URL url = new URL(BssAdapterConfig.protocol, BssAdapterConfig.ip,
                BssAdapterConfig.port, "/?Action=" + action+"&roleId="+roleId + "&taskType="+type);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        String date = TimeUtils.toGMTFormat(new Date());
        String resourcePath = Utils.toResourcePath(null, null, false);
        conn.setRequestProperty("x-portaloos-exclude-pools",String.join(",", excludePoolNames));
        conn.setRequestProperty("Host", BssAdapterConfig.ip + ":" + BssAdapterConfig.port);
        //conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
        conn.setRequestProperty("Content-Type","application/json");
        conn.setRequestMethod("POST");
        conn.setRequestProperty("Date", date);
        conn.setRequestProperty("Connection", "close");

        @SuppressWarnings("unchecked")
        String canonicalString = RestUtils.makeS3CanonicalString(
                conn.getRequestMethod(), resourcePath, new OOSRequest(conn),null);
        String signature = Utils.sign(canonicalString, sk,
                SigningAlgorithm.HmacSHA1);
        String authorization = "AWS " + ak + ":" + signature;
        conn.setRequestProperty("Authorization", authorization);
        conn.setDoOutput(true);
        conn.setConnectTimeout(BssAdapterConfig.connectTimeout);
        conn.setReadTimeout(BssAdapterConfig.readTimeout);
        conn.connect();

        try (OutputStream out = conn.getOutputStream()) {
            out.write(roleMeta.write());
            out.flush();
        }
        int code = conn.getResponseCode();
        if (code != 200) {
            log.error("Fail to insert/update role:" + roleMeta.getName());
        }
        if (listener != null) listener.onComplete(code);
    }
    
    public static void modifyUserToRole(String ak,String sk,UserToRoleMeta userToRole, 
            TaskType type,String[] excludePoolNames, CallbackListener listener) throws JSONException,IOException, BaseException {
        String action = BssAdapterConsts.ACTION_PUT_USERTOROLE;
        StringBuffer parameters = new StringBuffer();
        parameters.append("&taskType=").append(type);

        URL url = new URL(BssAdapterConfig.protocol, BssAdapterConfig.ip,
                BssAdapterConfig.port, "/?Action=" + action + parameters.toString());
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        String date = TimeUtils.toGMTFormat(new Date());
        String resourcePath = Utils.toResourcePath(null, null, false);
        conn.setRequestProperty("x-portaloos-exclude-pools",String.join(",", excludePoolNames));
        conn.setRequestProperty("Host", BssAdapterConfig.ip + ":" + BssAdapterConfig.port);
        conn.setRequestProperty("Content-Type","application/json");
        conn.setRequestMethod("POST");
        conn.setDoOutput(true);
        conn.setRequestProperty("Date", date);
        conn.setRequestProperty("Connection", "close");

        @SuppressWarnings("unchecked")
        String canonicalString = RestUtils.makeS3CanonicalString(
                conn.getRequestMethod(), resourcePath, new OOSRequest(conn),null);
        String signature = Utils.sign(canonicalString, sk,
                SigningAlgorithm.HmacSHA1);
        String authorization = "AWS " + ak + ":" + signature;
        conn.setRequestProperty("Authorization", authorization);
        conn.setConnectTimeout(BssAdapterConfig.connectTimeout);
        conn.setReadTimeout(BssAdapterConfig.readTimeout);
        conn.connect();

        try (OutputStream out = conn.getOutputStream()) {
            out.write(userToRole.writeAll());
            out.flush();
        }
        int code = conn.getResponseCode();
        if (code != 200) {
            log.error("Fail to insert/update userRole:" + userToRole.getOwnerId());
        }
        if (listener != null) listener.onComplete(code);
    }
    
    public static void getGlobalUserSession(String ak,String sk,String sessionId,Session session,
            CallbackListener listener) throws JSONException,IOException, BaseException {
        String action = BssAdapterConsts.ACTION_GET_USER_SESSION;

        URL url = new URL(BssAdapterConfig.protocol, BssAdapterConfig.ip,
                BssAdapterConfig.port, "/?Action=" + action + "&sessionId="+sessionId);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        String date = TimeUtils.toGMTFormat(new Date());
        String resourcePath = Utils.toResourcePath(null, null, false);
        conn.setRequestProperty("Host", BssAdapterConfig.ip + ":" + BssAdapterConfig.port);
        conn.setRequestProperty("Content-Type","application/json");
        conn.setRequestMethod("GET");
        conn.setRequestProperty("Date", date);
        conn.setRequestProperty("Connection", "close");
        conn.setDoInput(true);

        @SuppressWarnings("unchecked")
        String canonicalString = RestUtils.makeS3CanonicalString(
                conn.getRequestMethod(), resourcePath, new OOSRequest(conn),null);
        String signature = Utils.sign(canonicalString, sk,
                SigningAlgorithm.HmacSHA1);
        String authorization = "AWS " + ak + ":" + signature;
        conn.setRequestProperty("Authorization", authorization);
        conn.setConnectTimeout(BssAdapterConfig.connectTimeout);
        conn.setReadTimeout(BssAdapterConfig.readTimeout);
        conn.connect(); 
        int code = conn.getResponseCode();
        
        InputStream ip = null;
        JSONObject user = null;
        if (code != 200) {
            log.error("Could not find the user logined.");
        } else {
            try {
                ip = conn.getInputStream();
                if (ip != null) {
                    user = new JSONObject(IOUtils.toString(ip, Consts.CS_UTF8));
                    if (user.has("sessionId") && user.has("userName")) {
                        session.update(user.getString("sessionId"),user.getString("userName"));
                    }
                } else 
                    throw new BaseException(403, "NotLogin");
            } finally {
                if (ip != null) {
                    ip.close();
                }
            }
        }
        if (listener != null) listener.onComplete(code);
    }
    
    private static List<Pair<String,Pool>> createPoolList(String globalPoolsXML) 
            throws IOException, JDOMException {
        List<Pair<String,Pool>> globalPoolsList = new ArrayList();
        
        InputSource is = new InputSource(new StringReader(globalPoolsXML));
        Document doc = (new SAXBuilder()).build(is);
        Element root = doc.getRootElement();
        List<Element> pools = root.getChildren("pool");
        for(Element el:pools) {
            Pool p = new Pool();
            p.name = el.getChildText("Name");
            p.chName = el.getChildText("chName");
            p.url = el.getChildText("url");
            p.proxyPort = Integer.parseInt(el.getChildText("proxyPort"));
            
            globalPoolsList.add(new Pair(el.getChildText("Name"),p));
        }
        
        return globalPoolsList;
    }
    
    /**
     * 根据用户ID获取该用户在可配置数据域中配置的数据域列表,如果没有为该用户配置可见数据域，则默认看到全部资源池；
     * @param ownerId
     * @param ak
     * @param sk
     * @return
     * @throws Exception 
     */
    public static String getAvailablePoolsById(long ownerId,String ak,String sk) 
            throws Exception {
        JSONObject pools = XML.toJSONObject(getGlobalPoolsXML(ak,sk));
        if (!pools.has("pools")) return null; 
        pools = pools.getJSONObject("pools");
        
        JSONArray systemPools = null;
        if (JSONObject.class.isInstance(pools.get("pool"))) {
            systemPools = new JSONArray();
            systemPools.put(pools.getJSONObject("pool"));
        } else if (JSONArray.class.isInstance(pools.get("pool"))) {
            systemPools = pools.getJSONArray("pool");
        }
        JSONArray arrUsers = new JSONArray();
        
        Cache content = ownerRegionsCache.getContent(ownerId);
        List<String> userPools = null;
        if (content != null && content.value != null) {
            userPools = (ArrayList<String>)content.value;
        } else {
            userPools = Utils.getAvailableDataRegions(ownerId);
            if (userPools != null && userPools.size()>0)
                ownerRegionsCache.putCache(ownerId, new Cache(ownerId,userPools));
        }
         
        //配置
        if (userPools != null && userPools.size()>0) {
            for (int i=0;i<systemPools.length();i++) {
                JSONObject pool = systemPools.getJSONObject(i);               
                if (pool.has("Name") && userPools.contains(pool.getString("Name"))) {
                    arrUsers.put(pool);
                }
            }
        } else {  //未配置
            //私有池用户
            if (SynchroConfig.propertyLocalPool != null && !SynchroConfig.propertyLocalPool.synchroLogin) {
                for (int i=0;i<systemPools.length();i++) {
                    JSONObject pool = systemPools.getJSONObject(i);               
                    if (pool.has("Name") && pool.get("Name").equals(BssAdapterConfig.localPool.name)) {
                        arrUsers.put(pool);
                        break;
                    }
                }
            } else {  //非私有池用户看到的list中排除私有池name
                for (int i=0;i<systemPools.length();i++) {
                    JSONObject pool = systemPools.getJSONObject(i);               
                    if (pool.has("synchroLogin") && !pool.getBoolean("synchroLogin")) continue;
                    arrUsers.put(pool);
                }                
            }
        }
        
        JSONObject userAvailablePools = new JSONObject();
        userAvailablePools.put("pool", arrUsers);
        
        return XML.toString(new JSONObject().put("pools", userAvailablePools));
    }
    
    /**
     * 根据OwnerId查询该用户可见数据域配置的资源池列表
     * @param ownerId 
     * @param ak
     * @param sk
     * @return key：资源池name，value：Pool
     * @throws JDOMException 
     * @throws IOException 
     * @throws BaseException 
     * @throws JSONException 
     * @throws Exception 
     */
    public static List<Pair<String,Pool>> getAvailablePoolsAsListById(long ownerId,String ak,String sk) 
            throws BaseException, IOException, JDOMException, JSONException{
        List<Pair<String,Pool>> pools = getAvailablePoolsAsList(ak , sk);
        if (pools == null || pools.size() == 0) return null;
        
        Cache content = ownerRegionsCache.getContent(ownerId);
        List<String> userRegion = null;
        if (content != null && content.value != null) {
            userRegion = (ArrayList<String>)content.value;
        } else {
            userRegion = Utils.getAvailableDataRegions(ownerId);
            ownerRegionsCache.putCache(ownerId, new Cache(ownerId,userRegion));
        }        
        
        List<Pair<String,Pool>> poolFilter = null;
        //已配置
        if (userRegion != null && userRegion.size()>0) {
            final List<String> temp = userRegion;
            poolFilter = pools.stream().filter(item->temp.contains(item.first()))
                    .collect(Collectors.toList());
        } else { //未配置
            //私有池用户
            if (SynchroConfig.propertyLocalPool != null && !SynchroConfig.propertyLocalPool.synchroLogin) {
                poolFilter = pools.stream().filter(item->item.first().equals(BssAdapterConfig.localPool.name))
                        .collect(Collectors.toList());
                return poolFilter;
            }
            
            //非私有池用户排除私有池名称
            poolFilter = pools;
            JSONObject jsonPools = XML.toJSONObject(getGlobalPoolsXML(ak,sk));
            if (jsonPools != null && jsonPools.has("pools")) {
                jsonPools = jsonPools.getJSONObject("pools");
                
                JSONArray systemPools = null;
                if (JSONObject.class.isInstance(jsonPools.get("pool"))) {
                    systemPools = new JSONArray();
                    systemPools.put(jsonPools.getJSONObject("pool"));
                } else if (JSONArray.class.isInstance(jsonPools.get("pool"))) {
                    systemPools = jsonPools.getJSONArray("pool");
                }
                
                for (int i=0;i<systemPools.length();i++) {
                    JSONObject aPool = systemPools.getJSONObject(i);
                    if (aPool.has("synchroLogin") && !aPool.getBoolean("synchroLogin")) {
                        //poolFilter.remove(poolFilter.indexOf(aPool.getString("Name")));
                        poolFilter = poolFilter.parallelStream().
                                filter(item->{
                                    try {
                                        return !item.first().equals(aPool.getString("Name"));
                                    } catch (JSONException e) {
                                        return false;
                                    }
                                }).collect(Collectors.toList());
                    }
                }
            } 
        }
        return poolFilter;
    }
    
    /**
     * 判断用户是否有权登录指定的资源池名称；
     * @param ownerId
     * @param regionName
     * @return
     * @throws Exception 
     */
    public static boolean containsRegion(long ownerId,String regionName) 
            throws Exception {
        if (regionName == null || regionName.isEmpty()) return false;
        
        List<Pair<String,Pool>> userList = getAvailablePoolsAsListById(ownerId,BssAdapterConfig.localPool.ak
                ,BssAdapterConfig.localPool.getSK());
        return userList.stream().filter(item->item.first().equals(regionName)).collect(Collectors.toList()).size()>0;
    }
    
    public static Pair<Boolean, String> getDeleteUserInfo(String ownerId, String ak, String sk) 
            throws JSONException,IOException, BaseException {
        URL url = new URL(BssAdapterConfig.protocol, BssAdapterConfig.ip,
                BssAdapterConfig.port,"/?Action=" + BssAdapterConsts.ACTION_GET_DELETE_USERINFO);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        String date = TimeUtils.toGMTFormat(new Date());
        String resourcePath = Utils.toResourcePath(null, null, false);
        conn.setRequestMethod("GET");
        conn.setRequestProperty("Date", date);
        conn.setRequestProperty("Host", BssAdapterConfig.ip + ":" + BssAdapterConfig.port);
        conn.setRequestProperty("Connection", "close");
        conn.setRequestProperty("x-amz-ownerId", ownerId);
        
        String canonicalString = RestUtils.makeS3CanonicalString(
                conn.getRequestMethod(), resourcePath, new OOSRequest<Object>(conn),null);
        String signature = Utils.sign(canonicalString, sk,
                SigningAlgorithm.HmacSHA1);
        String authorization = "AWS " + ak + ":" + signature;
        conn.setRequestProperty("Authorization", authorization);
        conn.setConnectTimeout(BssAdapterConfig.connectTimeout);
        conn.setReadTimeout(BssAdapterConfig.readTimeout);
        conn.connect();
        int code = conn.getResponseCode();

        InputStream ip = null;
        if (code != 200) {
            log.error("Could not find the user status.");
        } else {
            try {
                ip = conn.getInputStream();
                if (ip != null) {
                    JSONObject userStatus = null;
                    Pair<Boolean,String> pair = new Pair<Boolean, String>();
                    userStatus = new JSONObject(IOUtils.toString(ip, Consts.CS_UTF8));
                    String missionStatus = null;
                    if(userStatus.has("missionStatus")) {
                        missionStatus = userStatus.getString("missionStatus");
                    }
                    String isCallUserPermission = null;
                    if(userStatus.has("isCallUserPermission")) {
                        isCallUserPermission = userStatus.getString("isCallUserPermission");
                    }
                    if(StringUtils.isBlank(isCallUserPermission) && StringUtils.isBlank(missionStatus))
                        return null;
                    if(StringUtils.isBlank(missionStatus))
                        return null;
                    if(StringUtils.isNotBlank(isCallUserPermission)) {
                        pair.first("true".equals(isCallUserPermission) ? true : false);  
                    }
                    if(StringUtils.isNotBlank(missionStatus))
                        pair.second(missionStatus);
                    return pair;
                } else 
                    return null;
            } finally {
                if (ip != null) {
                    try {
                        ip.close();
                    } catch (Exception e) {
                        log.error(e.getMessage(), e);
                    }
                }
            }
        }
        return null;
    }

    //删除数据之提供proxy 与 manager 查询调用
    public static String getDeleteUserInfoV2(String ownerId, String ak, String sk, String source)
            throws IOException, BaseException {
        URL url = new URL(BssAdapterConfig.protocol, BssAdapterConfig.ip, BssAdapterConfig.port,
                "/?Action=" + BssAdapterConsts.ACTION_GET_DELETE_USERINFO);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        String date = TimeUtils.toGMTFormat(new Date());
        String resourcePath = Utils.toResourcePath(null, null, false);
        conn.setRequestMethod("GET");
        conn.setRequestProperty("Date", date);
        conn.setRequestProperty("Host", BssAdapterConfig.ip + ":" + BssAdapterConfig.port);
        if (source != null) {//source不为null 则拼proxy请求参数。否则拼不带"source"字段的API请求
            conn.setRequestProperty("source", "Proxy");
        }
        conn.setRequestProperty("x-amz-ownerId", ownerId);
        conn.setRequestProperty("Connection", "close");
        String canonicalString = RestUtils
                .makeS3CanonicalString(conn.getRequestMethod(), resourcePath, new OOSRequest<Object>(conn), null);
        String signature = Utils.sign(canonicalString, sk, SigningAlgorithm.HmacSHA1);
        String authorization = "AWS " + ak + ":" + signature;
        conn.setRequestProperty("Authorization", authorization);
        conn.setDoOutput(true);
        conn.setConnectTimeout(BssAdapterConfig.connectTimeout);
        conn.setReadTimeout(BssAdapterConfig.readTimeout);
        conn.connect();
        int code = conn.getResponseCode();
        if (code != 200) {
            log.error("Could not find the user status.");
        }
        try (InputStream input = conn.getInputStream()) {
            if (input != null) {
                // proxy的查表请求  查询到用户则返回带字段的json格式数据，否则返回为空字段的额json格式数据
                return IOUtils.toString(input, Consts.CS_UTF8);
            }
        }
        return null;//若Server返回为非200或者读取输入失败 则返回null
    }
    
    public static String listBssPackage(String accountId, String ak, String sk) throws IOException, BaseException {
        URL url = new URL(BssAdapterConfig.protocol, BssAdapterConfig.ip, BssAdapterConfig.port,
                "/?Action=" + BssAdapterConsts.ACTION_BSSPACKAGE_LIST + "&x-amz-accountId=" + accountId);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        String date = TimeUtils.toGMTFormat(new Date());
        String resourcePath = Utils.toResourcePath(null, null, false);
        conn.setRequestMethod("GET");
        conn.setRequestProperty("Date", date);
        conn.setRequestProperty("Host", BssAdapterConfig.ip + ":" + BssAdapterConfig.port);
        conn.setRequestProperty("Connection", "close");
        String canonicalString = RestUtils
                .makeS3CanonicalString(conn.getRequestMethod(), resourcePath, new OOSRequest<Object>(conn), null);
        String signature = Utils.sign(canonicalString, sk, SigningAlgorithm.HmacSHA1);
        String authorization = "AWS " + ak + ":" + signature;
        conn.setRequestProperty("Authorization", authorization);
        conn.setConnectTimeout(BssAdapterConfig.connectTimeout);
        conn.setReadTimeout(BssAdapterConfig.readTimeout);
        conn.connect();
        String bssPackage = IOUtils.toString(conn.getInputStream(), Consts.CS_UTF8);
        log.info(conn.getHeaderField(Headers.REQUEST_ID) + ":" + bssPackage);
        return bssPackage;
    }
}
