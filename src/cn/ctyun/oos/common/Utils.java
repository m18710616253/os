package cn.ctyun.oos.common;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.URLDecoder;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.text.ParseException;
import java.time.LocalDate;
import java.time.YearMonth;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAdjusters;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.net.util.SubnetUtils;
import org.eclipse.jetty.server.Request;
import org.jdom.JDOMException;

import com.amazonaws.AmazonClientException;
import com.amazonaws.HttpMethod;
import com.amazonaws.auth.SigningAlgorithm;
import com.amazonaws.services.s3.Headers;
import com.amazonaws.services.s3.internal.Constants;
import com.amazonaws.services.s3.internal.RestUtils;
import com.amazonaws.services.s3.internal.ServiceUtils;
import com.amazonaws.util.BinaryUtils;
import com.amazonaws.util.json.JSONArray;
import com.amazonaws.util.json.JSONException;
import com.amazonaws.util.json.JSONObject;
import com.google.common.collect.Lists;

import cn.ctyun.common.BaseException;
import cn.ctyun.common.Consts;
import cn.ctyun.common.ReplicaMode;
import cn.ctyun.common.conf.DataRegion;
import cn.ctyun.common.conf.OOSConfig;
import cn.ctyun.common.utils.LogUtils;
import cn.ctyun.oos.bssAdapter.server.BssAdapterClient;
import cn.ctyun.oos.hbase.MetaClient;
import cn.ctyun.oos.metadata.AkSkMeta;
import cn.ctyun.oos.metadata.BucketMeta;
import cn.ctyun.oos.metadata.ObjectMeta;
import cn.ctyun.oos.metadata.OwnerMeta;
import cn.ctyun.oos.metadata.RoleMeta;
import cn.ctyun.oos.metadata.RoleMeta.RolePermission;
import cn.ctyun.oos.metadata.TokenMeta;
import cn.ctyun.oos.metadata.UserToRoleMeta;
import cn.ctyun.oos.server.MultiVersionsController;
import cn.ctyun.oos.server.conf.BackoffConfig;
import cn.ctyun.oos.server.conf.BssAdapterConfig;
import cn.ctyun.oos.server.conf.BssAdapterConfig.Pool;
import cn.ctyun.oos.server.conf.PeriodUsageStatsConfig;
import cn.ctyun.oos.server.count.Pin;
import cn.ctyun.oos.server.count.PinData;
import cn.ctyun.oos.server.db.DB;
import cn.ctyun.oos.server.db.DbBucketWebsite;
import cn.ctyun.oos.server.formpost.FormField;
import cn.ctyun.oos.server.formpost.FormPostHeaderNotice;
import cn.ctyun.oos.server.formpost.PolicyManager;
import cn.ctyun.oos.server.formpost.exceptions.InvalidArgumentException;
import cn.ctyun.oos.server.formpost.exceptions.SignatureDoesNotMatchException;
import cn.ctyun.oos.server.log.BucketLog;
import cn.ctyun.oos.server.signer.SignerUtils;
import cn.ctyun.oos.server.signer.V4Signer;
import cn.ctyun.oos.server.signer.V4Signer.AuthorizationItem;
import cn.ctyun.oos.server.signer.V4Signer.CredentialItem;
import cn.ctyun.oos.server.util.CSVUtils.UsageType;
import cn.ctyun.oos.server.util.Misc;
import common.tuple.Pair;
import common.url.URLUtil;
import common.util.HexUtils;

public class Utils {
    private static final Log log = LogFactory.getLog(Utils.class);
    protected final static DateTimeFormatter formatyyyy_mm = DateTimeFormatter.ofPattern("yyyy-MM");
    public static final String OOS_IMAGE = "@oosImage";
    public static final String OOS_TEXT = "@oosText";
    public static final String PORN = "porn";
    public static final String SPAM = "spam";
    public static final String AT_SYMBOL = "@";
    public static final String TYPE_IMAGE_PROCESS = "imageProcess";
    public static final String TYPE_TEXT_PROCESS = "textProcess";
    public static final String TYPE_PORN = "porn";
    public static final String TYPE_SPAM = "spam";
    public static final String TYPE_NORMAL = "normal";
    public static final String KEYWORD_REVIEW = "review"; // 图片鉴黄请求结果review字段
    public static final String BY_DAY = "byDay"; // 使用量查询粒度，按天
    public static final String BY_5_MIN = "by5min";// 使用量查询粒度，5分钟
    public static final String BY_HOUR = "byHour";// 使用量查询粒度，5分钟

    //仅可用于查询
    public static final List<String> STORAGE_TYPES = Lists.newArrayList(Consts.STORAGE_CLASS_STANDARD, Consts.STORAGE_CLASS_STANDARD_IA); // 存储类型
    public static final List<String> STORAGE_TYPES_RESTORE = Lists.newArrayList(Consts.STORAGE_CLASS_STANDARD_IA); // 需要收取数据取回的存储类型，低频和归档

    public static final String CSV = "csv";//统计数据下载格式
    public static final String JSON = "json";//统计概览页面返回格式
    private static MetaClient client = MetaClient.getGlobalClient();
    
    // 符合IPV4格式的正则表达式
    private static String ipRegExp = "^(1\\d{2}|2[0-4]\\d|25[0-5]|[1-9]\\d|[1-9])\\."  
            +"(00?\\d|1\\d{2}|2[0-4]\\d|25[0-5]|[1-9]\\d|\\d)\\."  
            +"(00?\\d|1\\d{2}|2[0-4]\\d|25[0-5]|[1-9]\\d|\\d)\\."  
            +"(00?\\d|1\\d{2}|2[0-4]\\d|25[0-5]|[1-9]\\d|\\d)$";
    
    // 符合CIDR格式的IPv4地址段表达式
    private static String cidrStrictRegExp = "^(1\\d{2}|2[0-4]\\d|25[0-5]|[1-9]\\d|[1-9])\\."  
                +"(00?\\d|1\\d{2}|2[0-4]\\d|25[0-5]|[1-9]\\d|\\d)\\."  
                +"(00?\\d|1\\d{2}|2[0-4]\\d|25[0-5]|[1-9]\\d|\\d)\\."  
                +"(00?\\d|1\\d{2}|2[0-4]\\d|25[0-5]|[1-9]\\d|\\d)/(\\d|[1-2]\\d|3[0-2])$";
    
    //验证用户密码的正则表达式
    private static String pwRegEx = 
            "^(?=.*\\d)(?=.*[a-zA-Z])(?=.*[\\_\\.~!@#$%^&*])[\\da-zA-Z\\_\\.~!@#$%^&*]{8,}$";
    
    //验证用户名的正则表达式
    private static String emailRegEx = 
            "^([A-Za-z0-9_\\-\\.])+\\@([A-Za-z0-9_\\-\\.])+\\.([A-Za-z]{2,4})$";
    
    private static Pattern getPatternCompile(String strRegexp){
        return Pattern.compile(strRegexp);
    }
    
    public static String sign(String data, String key, SigningAlgorithm algorithm) {
        // refer to com.amazonaws.services.s3.internal.S3Signer
        try {
            Mac mac = Mac.getInstance(algorithm.toString());
            mac.init(new SecretKeySpec(key.getBytes(Consts.STR_UTF8), algorithm.toString()));
            byte[] bs = mac.doFinal(data.getBytes(Consts.STR_UTF8));
            return new String(Base64.encodeBase64(bs), Consts.STR_UTF8);
        } catch (UnsupportedEncodingException e) {
            throw new AmazonClientException("Unable to calculate a request signature: "
                    + e.getMessage(), e);
        } catch (Exception e) {
            throw new AmazonClientException("Unable to calculate a request signature: "
                    + e.getMessage(), e);
        }
    }

    public static String toResourcePath(String bucket, String key, boolean endWithSlash) {
        // refer to com.amazonaws.services.s3.AmazonS3Client.createSigner
        // 增加对斜杠的判断
        String resourcePath;
        if (endWithSlash)
            resourcePath = "/" + ((bucket != null && !bucket.equals("")) ? bucket + "/" : "")
                    + ((key != null) ? ServiceUtils.urlEncode(key) : "");
        else
            resourcePath = "/" + ((bucket != null && !bucket.equals("")) ? bucket : "")
                    + ((key != null) ? "/" + ServiceUtils.urlEncode(key) : "");
        return resourcePath;
    }

    public static String toResourcePathNotEncode(String bucket, String key, boolean endWithSlash) {
        String resourcePath;
        if (endWithSlash)
            resourcePath = "/" + ((bucket != null && !bucket.equals("")) ? bucket + "/" : "")
                    + ((key != null) ? key : "");
        else
            resourcePath = "/" + ((bucket != null && !bucket.equals("")) ? bucket : "")
                    + ((key != null) ? "/" + key : "");
        return resourcePath;
    }

    /**
     * return bucket and key as a pair ,from HTTP request path and host .if
     * bucket not exists, return empty string . if key not exists, return null
     * or empty string.
     *
     * @throws SQLException
     * @throws IOException
     *
     *
     *
     */
    public static Pair<String, String> getBucketKey(String path, String host, String[] domainName,
            String port, String sslPort, String[] webSiteEndpoint, String websiteDomain,
            String[] invalidBucketNames, String[] invalidObjectNames, boolean isFromProxy)
            throws BaseException, SQLException, IOException {
        if (host == null || host.length() == 0)
            throw new BaseException("invalid host", 400, ErrorMessage.ERROR_CODE_INVALID_ARGUMENT);
        if (path == null || path.length() == 0 || path.charAt(0) != '/')
            throw new BaseException(400, "InvalidURI");
        // Web browsers do not always handle '+' characters well, use the
        // well-supported '%20' instead.
        path = path.replaceAll("%20", "\\+");
        try {
            path = URLDecoder.decode(path, Constants.DEFAULT_ENCODING);
        } catch (UnsupportedEncodingException e) {
            throw new BaseException(400, "InvalidURI", "the uri is:" + path);
        }
        Pair<String, String> p = new Pair<String, String>();
        host = host.trim().toLowerCase();
        int idx;
        if (path.contains(OOS_IMAGE) || path.contains(OOS_TEXT)) {
            path = path.split(AT_SYMBOL)[0];
        }
        if (checkHosts(host.split(":")[0], domainName, websiteDomain)) {
            /*
             * bucket is in http request path. GET /bucket/key
             */
            path = path.substring(1);
            if ((idx = path.indexOf('/')) < 0) {
                p.first(path);
                p.second(null);
            } else {
                p.first(path.substring(0, idx));
                p.second(path.substring(idx + 1));
            }
        } else {
            idx = getHostIndex(host.split(":")[0], domainName, webSiteEndpoint);
            if (idx < 0) {
                /*
                 * bucket is the host. GET /key
                 */
                p.first(host);
                int par = path.length();
                p.second(path.substring(1, par));
            } else {
                /*
                 * bucket is in front of the host. GET /key
                 */
                p.first(host.substring(0, idx));
                int par = path.length();
                p.second(path.substring(1, par));
            }
        }
        assert (p.first() != null);
        if (p.first().length() > 0) {
            if (OOSConfig.getCheckBucketName() == 1) {
                try {
                    Misc.validateBucketName(p.first(), invalidBucketNames);
                } catch (IllegalArgumentException e) {
                    log.error(e.getMessage(), e);
                    throw new BaseException(400, "InvalidBucketName", "the bucket name is:"
                            + p.first());
                }
            }
        }
        checkObjectName(p.second(), invalidObjectNames);
        MetaClient client = MetaClient.getGlobalClient();
        BucketMeta dbBucket = new BucketMeta(p.first());
        //1 非proxy 请求。 
        //2 bucketName 即为host  String host = req.getHeader("Host");  即CNAME请求。
        //3 host 等于 http://host:port形式获取的 host或者 host 等于 www.host:port 形式获取的host
        //4 bucket 存在于数据库中。 
        if (!isFromProxy
                && p.first().equals(host)
                && (host.endsWith(URLUtil.getDomainName("http://" + host)) || host.equals("www."
                        + URLUtil.getDomainName("http://" + host)))
                && client.bucketSelect(dbBucket)) {
            DbBucketWebsite bucketWebsite = new DbBucketWebsite(dbBucket.getId());
            DB db = DB.getInstance();
            if (!db.bucketWebsiteSelect(bucketWebsite))
                throw new BaseException("bucket is not in website white list", 403, "AccessDenied");
        }
        return p;
    }
    /**
     * 校验Key是否合法
     * @param objectName
     * @param invalidObjectName
     * @throws BaseException
     */
    public static void checkObjectName(String objectName, String[] invalidObjectNames) throws BaseException {
        if(objectName == null){
            return;
        }
        if (objectName.length() > Consts.MAX_OBJECT_NAME_LENGTH) {
            throw new BaseException(400, "InvalidObjectName", "the object name is:" + objectName);
        }
        if (invalidObjectNames != null) {
            for (String i : invalidObjectNames)
                if (i != null && i.trim().length() != 0 && objectName.equals(i))
                    throw new BaseException("the object name is in black list", 400, ErrorMessage.ERROR_CODE_INVALID_ARGUMENT);
        }
    }

    private static boolean checkHosts(String host, String[] domainName, String websiteDomain) {
        for (String d : domainName) {
            if (host.equals(d))
                return true;
        }
        if (host.equals(websiteDomain) || host.equals(Consts.S3_ENDPOINT))
            return true;
        else
            return false;
    }

    private static int getHostIndex(String host, String[] domainName, String[] webSiteEndpoint) {
        int idx = -1;
        for (String w : webSiteEndpoint) {
            if (idx < 0)
                idx = host.lastIndexOf("." + w);
        }
        for (String d : domainName) {
            if (idx < 0)
                idx = host.lastIndexOf("." + d);
        }
        if (idx < 0)
            idx = host.lastIndexOf("." + Consts.S3_ENDPOINT);
        return idx;
    }

    public static String getSessionId(HttpServletRequest request) {
        String sessionId = null;
        sessionId = request.getHeader(WebsiteHeader.LOGIN_SESSIONID);
        if (sessionId == null)
            sessionId = request.getParameter("sessionId");
        
        if (sessionId == null)
            sessionId = (String) request.getAttribute("sessionId");
        
        return sessionId;
    }

    public static String getIpAddr(HttpServletRequest req) {
        String ipAddress = null;
        ipAddress = req.getHeader("x-forwarded-for");
        if (ipAddress == null || ipAddress.length() == 0 || "unknown".equalsIgnoreCase(ipAddress)) {
            ipAddress = req.getHeader("Proxy-Client-IP");
        }
        if (ipAddress == null || ipAddress.length() == 0 || "unknown".equalsIgnoreCase(ipAddress)) {
            ipAddress = req.getHeader("WL-Proxy-Client-IP");
        }
        if (ipAddress == null || ipAddress.length() == 0 || "unknown".equalsIgnoreCase(ipAddress)) {
            ipAddress = req.getHeader("HTTP_CLIENT_IP");
        }
        if (ipAddress == null || ipAddress.length() == 0 || "unknown".equalsIgnoreCase(ipAddress)) {
            ipAddress = req.getHeader("HTTP_X_FORWARDED_FOR");
        }
        if (ipAddress == null || ipAddress.length() == 0 || "unknown".equalsIgnoreCase(ipAddress)) {
            ipAddress = req.getRemoteAddr();
        }
        // 对于通过多个代理的情况，第一个IP为客户端真实IP,多个IP按照','分割
        if (ipAddress != null && ipAddress.length() > 15) {
            if (ipAddress.indexOf(",") > 0) {
                ipAddress = ipAddress.substring(0, ipAddress.indexOf(","));
            }
        }
        return ipAddress;
    }

    /**
     * 判断请求ip是否为互联网ip
     * @param ipAddr
     * @return
     */
    public static boolean isInternetIP(String ipAddr) {
        try {
            if (ipAddr == null || ipAddr.equals("") || !isValidIpAddr(ipAddr)) {
                // 非法请求ip归为互联网ip请求
                return true;
            }
            List<String> ipSegmentList = client.noNetIPSegmentList();
            if (ipSegmentList != null && ipSegmentList.size() != 0) {
                for (String ipSegment : ipSegmentList) {
                    if (!isValidIpOrCidr(ipSegment)) {
                        log.error("ipSegment is not valid. ipSegment:" + ipSegment);
                        continue;
                    }
                    if (ipExistsInRange(ipAddr, ipSegment)) {
                        return false;
                    }
                }
                return true;
            }
            return true;
        } catch (Exception e) {
            log.error("check isInternetIP error. " + e.getMessage());
            return true;
        }
    }
    
    /**
     * 判断ip是否处于ip段内
     * @param ip
     * @param ipRegion
     * @return
     * @throws UnknownHostException 
     */
    public static boolean ipExistsInRange(String ip, String ipRegion) throws UnknownHostException{
        if (!ip.contains(":")) {
            // 请求ip为ipv4
            if (!ipRegion.contains(":")) {
                if (ipRegion.contains("/")) {
                    // ip段为ipv4 cidr
                    SubnetUtils subnetUtils = new SubnetUtils(ipRegion);
                    subnetUtils.setInclusiveHostCount(true);
                    if (subnetUtils.getInfo().isInRange(ip)) {
                        return true;
                    } else {
                        return false;
                    }
                } else {
                    // ip段为ipv4
                    if (ipRegion.equals(ip)) {
                        return true;
                    } else {
                        return false;
                    }
                }
            } else {
                return false;
            }

        } else {
            // 请求ip为ipv6
            if (ipRegion.contains(":")) {
                // ip段为ipv6或ipv6 cidr
                if (isInIpv6Range(ip, ipRegion)) {
                    return true;
                } else {
                    return false;
                }
            } else {
                return false;
            }
        }
    }

    public static boolean isFrozen(OwnerMeta owner) throws ParseException {
        return owner.frozenDate != null
                && owner.frozenDate.trim().length() != 0
                && DateUtils.addHours(ServiceUtils.parseIso8601Date(owner.frozenDate),
                        Consts.FROZEN_VALIDATION_HOURS).before(new Date());
    }

    public static boolean ifCanWrite(OwnerMeta owner, BucketMeta bucket) {
        if (bucket.permission == BucketMeta.PERM_PUBLIC_READ_WRITE)
            return true;
        if (owner == null)
            return false;
        if (bucket.ownerId == owner.getId())
            return true;
        return false;
    }

    public static void checkParameter(String parameterName) throws BaseException {
        if (parameterName == null || parameterName.trim().length() == 0)
            throw new BaseException();
    }
    
    /**TODO 该校验方法存在严重问题，很不严谨
     * 校验日期是否符合yyyy-MM-dd格式
     * @param parameterName
     * @return
     * @throws BaseException
     */
    public static boolean dayFormatIsValid(String parameterName) {
        // yyyy-MM-dd格式
        String regex = "[0-9]{4}-[0-9]{2}-[0-9]{2}";
        Pattern pattern = Pattern.compile(regex);
        Matcher endMatch = pattern.matcher(parameterName);
        if (endMatch.matches()) {
            return true;
        } else {
            return false;
        }
    }
    
    /**
     * 校验按月日期是否符合yyyy-MM格式
     * @param parameterName
     * @return
     */
    public static boolean monthFormatIsValid(String parameterName){
        // yyyy-MM格式
        String regex = "^[0-9]{4}-(0[1-9]|1[012])$";
        Pattern pattern = Pattern.compile(regex);
        Matcher endMatch = pattern.matcher(parameterName);
        if (endMatch.matches()) {
            return true;
        } else {
            return false;
        }
    }
    
    public static UsageType checkAndGetUsageStatsTypeForDownload(String parameterName) throws BaseException {
        UsageType usageType = null;
        if (parameterName != null) {
            /* 自服务csv下载类型 */
            if (parameterName.equalsIgnoreCase("proxyCapacity")) {
                usageType = UsageType.PROXY_CAPACITY;
            } else if (parameterName.equalsIgnoreCase("proxyBilledStorage")) {
                usageType = UsageType.PROXY_BILLED_STORAGE;
            } else if (parameterName.equalsIgnoreCase("proxyRestoreStorage")) {
                usageType = UsageType.PROXY_RESTORE_STORAGE;
            } else if (parameterName.equalsIgnoreCase("proxyDeleteStorage")) {
                usageType = UsageType.PROXY_DELETE_STORAGE;
            } else if (parameterName.equalsIgnoreCase("proxyFlow")) {
                usageType = UsageType.PROXY_FLOW;
            } else if (parameterName.equalsIgnoreCase("proxyRequestAll")) {
                usageType = UsageType.PROXY_REQUEST_ALL;
            } else if (parameterName.equalsIgnoreCase("proxyRequestGet")) {
                usageType = UsageType.PROXY_REQUEST_GET;
            } else if (parameterName.equalsIgnoreCase("proxyRequestHead")) {
                usageType = UsageType.PROXY_REQUEST_HEAD;
            } else if (parameterName.equalsIgnoreCase("proxyRequestPut")) {
                usageType = UsageType.PROXY_REQUEST_PUT;
            } else if (parameterName.equalsIgnoreCase("proxyRequestPost")) {
                usageType = UsageType.PROXY_REQUEST_POST;
            } else if (parameterName.equalsIgnoreCase("proxyRequestDelete")) {
                usageType = UsageType.PROXY_REQUEST_DELETE;
            } else if (parameterName.equalsIgnoreCase("proxyRequestOther")) {
                usageType = UsageType.PROXY_REQUEST_OTHER;
            } else if (parameterName.equalsIgnoreCase("proxyAvailBandwidth")) {
                usageType = UsageType.PROXY_AVAIL_BANDWIDTH;
            } else if (parameterName.equalsIgnoreCase("proxyAvailableRate")) {
                usageType = UsageType.PROXY_AVAILABLE_RATE;
            } else if (parameterName.equalsIgnoreCase("proxyEffectiveRate")) {
                usageType = UsageType.PROXY_EFFECTIVE_RATE;
            } else if (parameterName.equalsIgnoreCase("proxyForbiddenRate")) {
                usageType = UsageType.PROXY_FORBIDDEN_RATE;
            } else if (parameterName.equalsIgnoreCase("proxyNotfoundRate")) {
                usageType = UsageType.PROXY_NOTFOUND_RATE;
            } else if (parameterName.equalsIgnoreCase("proxyOtherserrorRate")) {
                usageType = UsageType.PROXY_OTHERSERROR_RATE;
            } else if (parameterName.equalsIgnoreCase("proxyConnections")) {
                usageType = UsageType.PROXY_CONNECTIONS;
            }
            /* **** */
            else if (parameterName.equalsIgnoreCase("connection")) {
                usageType = UsageType.CONNECTIONS;
            } else if (parameterName.equalsIgnoreCase("flow")) {
                usageType = UsageType.FLOW;
            } else if (parameterName.equalsIgnoreCase("bandwidth")) {
                usageType = UsageType.BANDWIDTH;
            } else if (parameterName.equalsIgnoreCase("availBandwidth")) {
                usageType = UsageType.AVAIL_BANDWIDTH;
            } else if (parameterName.equalsIgnoreCase("request")) {
                usageType = UsageType.REQUEST;
            }
            /** 容量采样值 */
            else if (parameterName.equalsIgnoreCase("capacity")) { //容量采样值totalSize
                usageType = UsageType.CAPACITY;
            } else if (parameterName.equalsIgnoreCase("redundantSize")) { //冗余容量采样值redundantSize
                usageType = UsageType.REDUNDANTSIZE;
            } else if (parameterName.equalsIgnoreCase("alignSize")) { //裸容量采样值alignSize
                usageType = UsageType.ALIGNSIZE;
            } else if (parameterName.equalsIgnoreCase("originalTotalSize")) { //应属地容量采样值originalTotalSize
                usageType = UsageType.ORIGINALTOTALSIZE;
            } else if (parameterName.equalsIgnoreCase("billedStorage")) { //计费容量
                usageType = UsageType.BILLED_STORAGE;
            } else if (parameterName.equalsIgnoreCase("restoreStorage")) { // 数据取回量
                usageType = UsageType.RESTORE_STORAGE;
            } else if (parameterName.equalsIgnoreCase("deleteStorage")) { // 数据删除量
                usageType = UsageType.DELETE_STORAGE;
            }
            /** 容量峰值 */
            else if (parameterName.equalsIgnoreCase("maxcapacity")) { //容量峰值peakSize
                usageType = UsageType.MAX_CAPACITY;
            } else if (parameterName.equalsIgnoreCase("originalPeakSize")) { //应属地容量峰值originalTotalSize
                usageType = UsageType.MAX_ORIGINALPEAKSIZE;
            }
            /** 容量平均值 */
            else if (parameterName.equalsIgnoreCase("avgcapacity")) { //容量平均值avgTotalSize
                usageType = UsageType.AVG_CAPACITY;
            } else if (parameterName.equalsIgnoreCase("avgRedundantSize")) { //冗余容量采样值avgRedundantSize
                usageType = UsageType.AVG_REDUNDANTSIZE;
            } else if (parameterName.equalsIgnoreCase("avgAlignSize")) { //裸容量采样值avgAlignSize
                usageType = UsageType.AVG_ALIGNSIZE;
            } else if (parameterName.equalsIgnoreCase("avgOriginalTotalSize")) { //应属地容量采样值avgOriginalTotalSize
                usageType = UsageType.AVG_ORIGINALTOTALSIZE;
            }
            /** 带宽95峰值 */
            else if (parameterName.equalsIgnoreCase("bandwidth95")) {
                usageType = UsageType.PEAK_BANDWIDTH_95;
            } else {
                throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_ARGUMENT, "Invalid downloadType.");
            }
        }
        return usageType;
    }

    /**
     * 获取boundary
     * 例如： multipart/form-data;boundary=qbyJJIF2IBhBK5P-_qUcGjFubXon23meY46mO94J; charset=UTF-8
     *
     * 注意：返回的boundary前面增加了 "\r\n--"
     * @param contentType
     * @return
     * @throws IOException
     */
    public static byte[] getBoundary(String contentType) throws BaseException, IOException {
        //byte[] boundary = ("\r\n--" + contentType.split("boundary=")[1]).split(";")[0].getBytes(Consts.STR_UTF8);
        if(contentType == null){
            throw new BaseException(412, "precondition failed", "Bucket Post must be of the enclosure-type multipart/form-data");
        }
        String[] bds= contentType.split("boundary=");
        if(bds.length!=2){
            throw new BaseException(400, "MalformedPOSTRequest", "The body of your POST request is not well-formed multipart/form-data.");
        }
        String afterBoundary = bds[1];
        String[] bdsCharset = afterBoundary.split(";");
        String boundaryStr = ("\r\n--" + bdsCharset[0]);
        return boundaryStr.getBytes(Consts.STR_UTF8);
    }

    public static Date getDate(HttpServletRequest req) throws BaseException {
        if (req.getHeader(Headers.S3_ALTERNATE_DATE) != null) {
            try {
                return Misc.parseDateFormat(req.getHeader(Headers.S3_ALTERNATE_DATE));
            } catch (Exception e) {
                throw new BaseException("invalid date.", 400, ErrorMessage.ERROR_CODE_INVALID_ARGUMENT, ErrorMessage.ERROR_MESSAGE_INVALID_DATE_HEADER);
            }
        } else if (req.getHeader("Date") != null) {
            try {
                return Misc.parseDateFormat(req.getHeader("Date"));
            } catch (Exception e) {
                throw new BaseException("invalid date.", 400, ErrorMessage.ERROR_CODE_INVALID_ARGUMENT, ErrorMessage.ERROR_MESSAGE_INVALID_DATE_HEADER);
            }
        } else if (req.getHeader("Authorization") != null)
            throw new BaseException("invalid date.", 400, ErrorMessage.ERROR_CODE_INVALID_ARGUMENT, ErrorMessage.ERROR_MESSAGE_INVALID_DATE_HEADER);
        return null;
    }

    /**
     * 签名验证，兼容V2及V4，请求头Authorization方式
     * @param basereq
     * @param req
     * @param bucket
     * @param key
     * @param isMustBePrimaryKey
     * @return
     * @throws Exception
     */
    public static AuthResult auth(Request basereq, HttpServletRequest req, String bucket,
            String key, boolean isMustBePrimaryKey, boolean contentSha256HeaderIsRequired, String serviceName) throws Exception {
        String auth = req.getHeader(V4Signer.AUTHORIZATION);
        if (auth == null || auth.length() == 0) {
            return new AuthResult();
        }
        AuthResult authResult = null;
        if (auth.toUpperCase().startsWith("AWS ")) {
            authResult = authV2(basereq, req, bucket, key, isMustBePrimaryKey);
            authResult.inputStream = req.getInputStream();
        } else if (auth.toUpperCase().startsWith(V4Signer.AWS4_SIGNING_ALGORITHM)) {
            authResult = authV4(basereq, req, bucket, key, isMustBePrimaryKey, contentSha256HeaderIsRequired, serviceName);
        } else {
            throw new BaseException(403, ErrorMessage.ERROR_CODE_SIGNATURE_DOES_NOT_MATCH);
        }
        checkFrozenUser(authResult.owner);
        return authResult;
    }
    
    /**
     * V2签名验证，请求头Authorization方式
     * @param basereq
     * @param req
     * @param bucket
     * @param key
     * @param isMustBePrimaryKey
     * @return
     * @throws Exception
     */
    public static AuthResult authV2(Request basereq, HttpServletRequest req, String bucket,
            String key, boolean isMustBePrimaryKey) throws Exception {
        String auth = req.getHeader("Authorization");
        String origId = null;
        String origSign = null;
        try {
            origId = Misc.getUserIdFromAuthentication(auth);
            origSign = auth.substring(auth.indexOf(':') + 1);
        } catch (Exception e) {
            throw new BaseException();
        }
        String token = req.getHeader("X-Amz-Security-Token");
        AuthResult authResult = new AuthResult();
        String sk;
        if (token== null || token.length() == 0) {
            AkSkMeta asKey = getSecretKeyFromAccessKey(origId);
            authResult.owner = checkAkSk(asKey, isMustBePrimaryKey, req, bucket, key);
            authResult.accessKey = asKey;
            sk = asKey.getSecretKey();
        } else {
            // sts临时授权访问，aksk信息从stsToken表获取
            Pair<TokenMeta, OwnerMeta> p = checkAndGetTokenSkAndOwner(token, origId);
            sk = p.first().getSecretKey();
            authResult.owner = p.second();
            authResult.tokenMeta = p.first();
            authResult.isSts = true;
        }
        checkAuth(origSign, sk, bucket, key, basereq, req, null);
        return authResult;
    }
    
    /**
     * 从token表获取aksk信息
     * @param token
     * @return
     * @throws BaseException
     * @throws IOException
     */
    public static TokenMeta getStsTokenFromToken(String token) throws BaseException, IOException {
        MetaClient client = MetaClient.getGlobalClient();
        TokenMeta stsToken = new TokenMeta(token);
        if (!client.stsTokenSelect(stsToken))
            throw new BaseException(403, "InvalidToken", "InvalidToken:" + token);
        return stsToken;
    }
    
    /**
     * 校验stsToken的有效性，并从token表中获取aksk信息，返回sk及ownerMeta
     * @param token
     * @return
     * @throws BaseException
     * @throws IOException
     * @throws ParseException
     * @throws Exception
     */
    public static Pair<TokenMeta, OwnerMeta> checkAndGetTokenSkAndOwner(
            String token,String clientAk) throws BaseException, IOException, ParseException, Exception {
        TokenMeta stsToken = getStsTokenFromToken(token);
        String expiration = stsToken.expiration;
        Date expiredate = Misc.formatIso8601time(expiration);
        if (expiredate.compareTo(new Date()) < 0)
            throw new BaseException(403, "AccessDenied", "the secret token is expired. expiration:"+expiration);
        if(StringUtils.isBlank(clientAk) || !clientAk.equals(stsToken.stsAccessKey))
            throw new BaseException(403, "InvalidAccessKeyId", "The specified accessKey: "+ clientAk +" does not exist");
        OwnerMeta owner = new OwnerMeta(stsToken.ownerId);
        MetaClient client = MetaClient.getGlobalClient();
        client.ownerSelectById(owner);
        return new Pair<TokenMeta,OwnerMeta>(stsToken,owner);
    }

    /**
     * V4签名验证，请求头Authorization方式
     * @param basereq
     * @param req
     * @param bucket
     * @param key
     * @param isMustBePrimaryKey
     * @return
     * @throws Exception
     */
    public static AuthResult authV4(Request basereq, HttpServletRequest req, String bucket,
            String key, boolean isMustBePrimaryKey, boolean contentSha256HeaderIsRequired,String serviceName) throws Exception {
        // 验证V4签名请求头
        V4Signer.validAuthV4Headers(req, contentSha256HeaderIsRequired);
        // 解析并验证Authorization标头合法性
        AuthorizationItem authItem = V4Signer.parseAndCheckAuthorizationHeader(req.getHeader("Authorization"));
        // 解析并验证Authorization标头中Credential部分合法性
        String regionName = getRegionNameFromReqHost(req.getHeader(V4Signer.HOST_CAPITAL), serviceName);
        CredentialItem credential = V4Signer.parseAndCheckCredential(authItem.credential, regionName, serviceName);
        // 验证Authorization标头中SignedHeaders部分合法性
        V4Signer.checkSignedHeadersValid(new OOSRequest(req), authItem.signedHeaders, false,contentSha256HeaderIsRequired);
        // 验证aksk
        String ak = credential.ak;
        String sk;
        String token = req.getHeader("X-Amz-Security-Token");
        // 签名认证返回信息
        AuthResult authResult = new AuthResult();
        if (token== null || token.length() == 0) {
            AkSkMeta asKey = getSecretKeyFromAccessKey(ak);
            authResult.owner = checkAkSk(asKey, isMustBePrimaryKey, req, bucket, key);
            authResult.accessKey = asKey;
            sk = asKey.getSecretKey();
        } else {
            // sts临时授权访问，aksk信息从stsToken表获取
            Pair<TokenMeta, OwnerMeta> pair = checkAndGetTokenSkAndOwner(token, ak);
            sk = pair.first().getSecretKey();
            authResult.tokenMeta = pair.first();
            authResult.owner = pair.second();
            authResult.isSts = true;
        }
        // 验证签名是否一致
        String uri = req.getRequestURI();
        try {
            uri = URLDecoder.decode(uri, Constants.DEFAULT_ENCODING);
        } catch (UnsupportedEncodingException e) {
            throw new BaseException(400, "InvalidURI", "the uri is:" + uri);
        }
        String resourcePath = V4Signer.getCanonicalizedResourcePath(uri);
        Pair<String, InputStream> pair = V4Signer.sign(new OOSRequest(req), ak, sk, regionName, serviceName, resourcePath,contentSha256HeaderIsRequired);
        String expectedSign = pair.first();
        String origSign = authItem.signature;
        if (!expectedSign.equals(origSign)) {
            throw new BaseException(403, ErrorMessage.ERROR_CODE_SIGNATURE_DOES_NOT_MATCH, ErrorMessage.ERROR_MESSAGE_SIGNATURE_DOES_NOT_MATCH);
        }
        authResult.inputStream = pair.second();
        return authResult;
    }

    /**
     * 签名验证，兼容V2及V4，POST表单上传方式
     * @param basereq
     * @param req
     * @param bucket
     * @param key
     * @param isMustBePrimaryKey
     * @param formPostHeaderNotice
     * @return
     * @throws Exception
     */
    public static AuthResult authObjectPost(Request basereq,
            HttpServletRequest req, String bucket, String key, boolean isMustBePrimaryKey,
            FormPostHeaderNotice formPostHeaderNotice) throws Exception {
        Map<String,FormField> fields = formPostHeaderNotice.getFieldMap();
        AuthResult authResult = null;
        if (isPostByV4(fields)) {
            authResult = authObjectPostV4(basereq, req, bucket, key, isMustBePrimaryKey, formPostHeaderNotice);
        } else if (isPostByV2(fields)) {
            authResult = authObjectPostV2(basereq, req, bucket, key, isMustBePrimaryKey, formPostHeaderNotice);
        } else {
            return null;
        }
        if(null != authResult.owner) {
            checkFrozenUser(authResult.owner);
        }
        return authResult;
    }

    private static boolean isPostByV2(Map<String, FormField> fields) {
        String[] v2Parameters = { "awsaccesskeyid", "signature" };
        for (String p : v2Parameters) {
            if (null!=fields.get(p)) {
                return true;
            }
        }
        return false;
    }
    private static boolean isPostByV4(Map<String, FormField> fields) {
        String[] v4Parameters = { "x-amz-algorithm", "x-amz-credential", "x-amz-date", "x-amz-signature" };
        for (String p : v4Parameters) {
            if (null!=fields.get(p)) {
                return true;
            }
        }
        return false;
    }

    /**
     * V4签名验证，POST表单上传方式
     * @param basereq
     * @param req
     * @param bucket
     * @param key
     * @param isMustBePrimaryKey
     * @param formPostHeaderNotice
     * @return
     * @throws Exception 
     */
    public static AuthResult authObjectPostV4(Request basereq,
            HttpServletRequest req, String bucket, String key, boolean isMustBePrimaryKey,
            FormPostHeaderNotice formPostHeaderNotice) throws Exception {
        // 验证签名相关表单项合法性
        Map<String,FormField> fields = formPostHeaderNotice.getFieldMap();
        FormField policy = fields.get("policy");
        FormField algorithm = fields.get(V4Signer.X_AMZ_ALGORITHM);
        FormField credential = fields.get(V4Signer.X_AMZ_CREDENTIAL);
        FormField date = fields.get(V4Signer.X_AMZ_DATE);
        FormField origSign = fields.get(V4Signer.X_AMZ_SIGNATURE);
        if (checkFormFieldIsEmpty(policy) || checkFormFieldIsEmpty(algorithm)
                || checkFormFieldIsEmpty(credential)
                || checkFormFieldIsEmpty(date)
                || checkFormFieldIsEmpty(origSign)) {
            throw new BaseException(400, ErrorMessage.ERROR_CODE_AUTH_POST_ERROR, ErrorMessage.ERROR_MESSAGE_MISSING_AUTH_POST_FORM);
        }
        if (!algorithm.getValue().equals(V4Signer.AWS4_SIGNING_ALGORITHM))
            throw new BaseException(400, ErrorMessage.ERROR_CODE_AUTH_POST_ERROR, ErrorMessage.ERROR_MESSAGE_INVALID_X_AMZ_ALGORITHM_FORM);
        String regionName = getRegionNameFromReq(req, V4Signer.S3_SERVICE_NAME);
        CredentialItem credentialItem = V4Signer.parseAndCheckCredential(credential.getValue(), regionName, V4Signer.S3_SERVICE_NAME);
        try {
            SignerUtils.formatTimestamp(date.getValue());
        } catch (ParseException e) {
            throw new BaseException(400, ErrorMessage.ERROR_CODE_AUTH_POST_ERROR, ErrorMessage.ERROR_MESSAGE_INVALID_X_AMZ_DATE_POST_FORM);
        }
        // 验证x-amz-date不能早于7天以前
        Date now = new Date();
        long dayTimeBefore7 = now.getTime() - 7 * 24 * 60 *60 * 1000;
        Date reqDate = SignerUtils.formatTimestamp(date.getValue());
        if (reqDate.getTime() < dayTimeBefore7)
            throw new BaseException(403, ErrorMessage.ERROR_CODE_403, ErrorMessage.ERROR_MESSAGE_INVALID_POLICY);
        String ak = credentialItem.ak;
        String sk;
        FormField tokenField = fields.get("x-amz-security-token");
        // 签名认证返回信息
        AuthResult authResult = new AuthResult();
        if(tokenField == null || tokenField.getValue()==null || tokenField.getValue().trim().length()==0){
            AkSkMeta asKey = getSecretKeyFromAccessKey(ak);
            authResult.owner = checkAkSk(asKey, isMustBePrimaryKey, req, bucket, key);
            authResult.accessKey = asKey;
            sk = asKey.getSecretKey();
        } else {
            // STS临时访问凭证，aksk从token表获取
           Pair<TokenMeta, OwnerMeta> p = checkAndGetTokenSkAndOwner(tokenField.getValue(), ak);
           sk = p.first().getSecretKey();
           authResult.tokenMeta = p.first();
           authResult.owner = p.second();
           authResult.isSts = true;
        }
        // 验证签名是否一致
        String signature = V4Signer.postSign(new OOSRequest(req), ak, sk, regionName, policy.getValue(), credentialItem.dateStamp);
        if (!signature.equals(origSign.getValue()))
            throw new BaseException(403, ErrorMessage.ERROR_CODE_SIGNATURE_DOES_NOT_MATCH);
        formPostHeaderNotice.policy = PolicyManager.readPolicy(policy.getValue());
        // 验证policy
        PolicyManager.checkPolicyV4(formPostHeaderNotice.policy, fields, bucket);
        return authResult;
    }
    
    public static String getRegionNameFromReq(HttpServletRequest req, String serviceName) throws BaseException {
        StringBuffer url = req.getRequestURL();
        String uri = req.getRequestURI();
        int end = url.length() - uri.length();
        String domain = url.substring(0, end);
        String scheme = req.getScheme();
        String host = domain.replaceFirst(scheme+"://", "");
        String regionName = getRegionNameFromReqHost(host, serviceName);
        return regionName;
    }
    
    public static boolean checkFormFieldIsEmpty(FormField formField) {
        if (formField == null || formField.getValue().length() == 0 || formField.getValue().trim().length() == 0)
            return true;
        return false;
    }

    /**
     * V2签名验证，POST表单上传方式
     * @param basereq
     * @param req
     * @param bucket
     * @param key
     * @param isMustBePrimaryKey
     * @param formPostHeaderNotice
     * @return
     * @throws Exception
     */
    public static AuthResult authObjectPostV2(Request basereq,
            HttpServletRequest req, String bucket, String key, boolean isMustBePrimaryKey,
            FormPostHeaderNotice formPostHeaderNotice) throws Exception {
        Map<String,FormField> fields = formPostHeaderNotice.getFieldMap();
        FormField awsAccessKeyIdField = fields.get("awsaccesskeyid");//"AWSAccessKeyId".toLowerCase()
        if(awsAccessKeyIdField == null || awsAccessKeyIdField.getValue()==null || awsAccessKeyIdField.getValue().trim().length()==0){
            throw new InvalidArgumentException(400,
                    "Bucket POST must contain a field named 'AWSAccessKeyId'. If it is specified, please check the order of the fields.",
                    "policy", "");
        }
        String origId = awsAccessKeyIdField.getValue();
        FormField tokenField = fields.get("x-amz-security-token");
        String sk;
        AuthResult authResult = new AuthResult();
        if(tokenField == null || tokenField.getValue()==null || tokenField.getValue().trim().length()==0){
            AkSkMeta asKey = getSecretKeyFromAccessKey(origId);
            authResult.accessKey = asKey;
            authResult.owner = checkAkSk(asKey, isMustBePrimaryKey, req, bucket, key);
            sk = asKey.getSecretKey();
        } else {
            // STS临时访问凭证，aksk从token表获取
            Pair<TokenMeta, OwnerMeta> p = checkAndGetTokenSkAndOwner(tokenField.getValue(), origId);
            sk = p.first().getSecretKey();
            authResult.owner = p.second();
            authResult.tokenMeta = p.first();
            authResult.isSts = true;
        }
        checkAuthObjectPost(origId, sk, bucket, key, formPostHeaderNotice);
        return authResult;
    }

    public static void checkAuthObjectPost(String awsAccessKeyId, String secret, String bucket,
            String key, FormPostHeaderNotice formPostHeaderNotice) throws Exception {
        Map<String,FormField> fields = formPostHeaderNotice.getFieldMap();
        FormField policyField = fields.get("policy");
        if (policyField == null) {
            throw new InvalidArgumentException(400,
                    "Bucket POST must contain a field named 'policy'. If it is specified, please check the order of the fields.",
                    "policy", "");
        }
        FormField signatureField = fields.get("signature");
        if (signatureField == null) {
            throw new InvalidArgumentException(400,
                    "Bucket POST must contain a field named 'signature'. If it is specified, please check the order of the fields.",
                    "signature", "");
        }
        String origSign = signatureField.getValue();
        String origPolicy = policyField.getValue();
        String hmacBts64Text = Utils.sign(origPolicy, secret, SigningAlgorithm.HmacSHA1);
        String stringToSignBytes = HexUtils
                .toHexString(origPolicy.getBytes(Consts.STR_UTF8));
        if (!hmacBts64Text.equals(origSign)) {
            throw new SignatureDoesNotMatchException(403,
                    "The request signature we calculated does not match the signature you provided. Check your key and signing method.",
                    "", "", awsAccessKeyId, origPolicy, origSign,
                    stringToSignBytes);
        }
        formPostHeaderNotice.policy = PolicyManager.readPolicy(origPolicy);
        PolicyManager.checkPolicy(formPostHeaderNotice.policy, fields, bucket);
    }

    public static void checkNotPrimaryKey( String bucket, String key,
            HttpServletRequest req,String accessKey) throws BaseException, Exception {
        if ((bucket != null && bucket.length() > 0) // bucket
                && (key == null || key.length() == 0)) {
            if (req.getMethod().equals(HttpMethod.GET.toString())
                    && (req.getParameterMap().size() == 0 || req.getParameter("prefix") != null
                            || req.getParameter("delimiter") != null
                            || req.getParameter("max-keys") != null || req.getParameter("marker") != null)) {// list
                // objects操作
                if (req.getParameter("prefix") == null
                        || !req.getParameter("prefix").startsWith(accessKey))
                    throw new BaseException("non-primary key can not list without prefix or prefix is not start with access key.", 403, "AccessDenied", "Access Denied");
            } else
                throw new BaseException("non-primary key can not operate bucket.", 403, "AccessDenied", "Access Denied");
        } else if ((bucket != null && bucket.length() > 0) // key
                && (key != null && key.length() > 0)) {
            if (!key.startsWith(accessKey))
                throw new BaseException("non-primary key can not operate object that not start with access key.", 403, "AccessDenied", "Access Denied");
            else {
                if ((req.getHeader(Consts.X_AMZ_COPY_SOURCE) != null) && req.getMethod().equalsIgnoreCase("PUT")) {

                    //String sourceBucketName = null;
                    String sourceObjectName = null;
                    String source = req.getHeader(Consts.X_AMZ_COPY_SOURCE);
                    source = source.replaceAll("%20", "\\+");
                    source = URLDecoder.decode(source, Consts.STR_UTF8);
                    int slash2;
                    if (source.startsWith("/")) {
                        slash2 = source.indexOf('/', 1);
                        //sourceBucketName = source.substring(1, slash2);
                    } else {
                        slash2 = source.indexOf('/', 0);
                        //sourceBucketName = source.substring(0, slash2);
                    }
                    sourceObjectName = source.substring(slash2 + 1);
                    if (!sourceObjectName.startsWith(accessKey)) {
                        throw new BaseException("non-primary key can not operate object that not start with access key.", 403, "AccessDenied", "Access Denied");
                    }

                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    public static boolean checkAuth(String origSign, String secret, String bucket, String key,
            Request basereq, HttpServletRequest req, String expirationInSeconds)
            throws BaseException {
        String uri = req.getRequestURI();
        uri = uri.split("\\?")[0];
        // 内容安全服务的签名,调整key为原object文件名称+内容安全服务关键字+图片或文本请求参数
        if (uri.contains(Utils.OOS_IMAGE) || uri.contains(Utils.OOS_TEXT)) {
            try {
                uri = URLDecoder.decode(uri, Constants.DEFAULT_ENCODING);
            } catch (UnsupportedEncodingException e) {
                throw new BaseException(400, "InvalidURI", "the uri is:" + uri);
            }
            int idx = uri.indexOf(Utils.AT_SYMBOL);
            key = key + uri.substring(idx);
        }
        String resourcePath = Utils.toResourcePath(bucket, key, uri.endsWith("/"));
        String method = req.getMethod();
        String canonicalString = RestUtils.makeS3CanonicalString(method, resourcePath,
                new OOSRequest(basereq), expirationInSeconds);
        String signature = Utils.sign(canonicalString, secret, SigningAlgorithm.HmacSHA1);
        if (!signature.equals(origSign)) {
            // 先用urlencode方式计算签名，如果不匹配，再用非encode方式计算一遍
            if (key != null && key.length() != 0) {
                if (!uri.startsWith("/" + bucket + "/"))
                    key = uri.substring(1);
                else
                    key = uri.substring(bucket.length() + 2);
            }
            resourcePath = Utils.toResourcePathNotEncode(bucket, key, uri.endsWith("/"));
            canonicalString = RestUtils.makeS3CanonicalString(method, resourcePath, new OOSRequest(
                    basereq), expirationInSeconds);
            signature = Utils.sign(canonicalString, secret, SigningAlgorithm.HmacSHA1);
            if (!signature.equals(origSign))
                throw new BaseException(403, ErrorMessage.ERROR_CODE_SIGNATURE_DOES_NOT_MATCH);
        }
        return true;
    }

    //amazon s3的签名中不支持action，fromDate，toDate参数，需要加上
    @SuppressWarnings("unchecked")
    public static boolean checkAuth2(String origSign, String secret, String bucket, String key,
            Request basereq, HttpServletRequest req, String expirationInSeconds)
            throws BaseException {
        String query = req.getQueryString();
        String uri = req.getRequestURI();
        uri = uri.split("\\?")[0];
        String resourcePath = Utils.toResourcePath(bucket, key, uri.endsWith("/"));
        String method = req.getMethod();
        String canonicalString = RestUtils.makeS3CanonicalString(method, resourcePath,
                new OOSRequest(basereq), expirationInSeconds);
        //query参数加上
        canonicalString = canonicalString + "?" + query;
        // log.info(canonicalString);
        String signature = Utils.sign(canonicalString, secret, SigningAlgorithm.HmacSHA1);
        if (!signature.equals(origSign)) {
            // 先用urlencode方式计算签名，如果不匹配，再用非encode方式计算一遍
            if (key != null && key.length() != 0) {
                if (!uri.startsWith("/" + bucket + "/"))
                    key = uri.substring(1);
                else
                    key = uri.substring(bucket.length() + 2);
            }
            resourcePath = Utils.toResourcePathNotEncode(bucket, key, uri.endsWith("/"));           
            canonicalString = RestUtils.makeS3CanonicalString(method, resourcePath, new OOSRequest(
                    basereq), expirationInSeconds);
            //query参数加上
            canonicalString = canonicalString + "?" + query;
            signature = Utils.sign(canonicalString, secret, SigningAlgorithm.HmacSHA1);
            if (!signature.equals(origSign))
                throw new BaseException(403, ErrorMessage.ERROR_CODE_SIGNATURE_DOES_NOT_MATCH);
        }
        return true;
    }

    /**
     * 签名验证，兼容V2及V4，URL query方式
     * @param basereq
     * @param req
     * @param bucket
     * @param key
     * @return
     * @throws Exception
     */
    public static AuthResult checkUrl(Request basereq, HttpServletRequest req, String bucket,
            String key) throws Exception {
        AuthResult authResult = null;
        if (req.getParameter(V4Signer.X_AMZ_SIGNATURE_CAPITAL) != null) {
            authResult = checkUrlV4(basereq, req, bucket, key);
        } else {
            authResult = checkUrlV2(basereq, req, bucket, key);
        }
        return authResult;
    }
    
    /**
     * V2签名验证，URL query方式
     * @param basereq
     * @param req
     * @param bucket
     * @param key
     * @return
     * @throws Exception
     */
    public static AuthResult checkUrlV2(Request basereq, HttpServletRequest req, String bucket,
            String key) throws Exception {
        String origSign = req.getParameter("Signature");
        String expirationInSeconds = req.getParameter("Expires");
        String accessKeyId = req.getParameter("AWSAccessKeyId");
        if (origSign == null || expirationInSeconds == null || accessKeyId == null) {
            throw new BaseException("the request parameters not contain: Signature,Expires,AWSAccessKeyId", 403, "AccessDenied", "Access Denied");
        }
        String method = req.getMethod();
        if (!method.equalsIgnoreCase(HttpMethod.GET.toString()) && !method.equalsIgnoreCase(HttpMethod.POST.toString()))
            throw new BaseException();
        Date expire = new Date(Long.parseLong(expirationInSeconds) * 1000);
        Date now = new Date();
        if (expire.compareTo(now) == -1)
            throw new BaseException("the url is expired", 403, "AccessDenied", "Access Denied");
        // V2签名为小写x-amz-security-token
        String token = req.getParameter("x-amz-security-token");
        String sk;
        AuthResult authResult = new AuthResult();
        if (token== null || token.length() == 0) {
            AkSkMeta asKey = getSecretKeyFromAccessKey(accessKeyId);
            authResult.accessKey = asKey;
            authResult.owner = checkAkSk(asKey, false, req, bucket, key);
            sk = asKey.getSecretKey();
        } else {
            // sts临时授权访问，aksk信息从stsToken表获取
            Pair<TokenMeta, OwnerMeta> pair = checkAndGetTokenSkAndOwner(token, accessKeyId);
            sk = pair.first().getSecretKey();
            authResult.owner = pair.second();
            authResult.tokenMeta = pair.first();
            authResult.isSts = true;
        }
        checkAuth(origSign, sk, bucket, key, basereq, req, expirationInSeconds);
        return authResult;
    }
    
    /**
     * V4签名验证，URL query方式
     * @param basereq
     * @param req
     * @param bucket
     * @param key
     * @return
     * @throws Exception
     */
    public static AuthResult checkUrlV4(Request basereq, HttpServletRequest req, String bucket,
            String key) throws Exception {
        // 验证签名相关query参数合法性
        String algorithm = req.getParameter(V4Signer.X_AMZ_ALGORITHM_CAPITAL);
        String credential = req.getParameter(V4Signer.X_AMZ_CREDENTIAL_CAPITAL);
        String signedHeaders = req.getParameter(V4Signer.X_AMZ_SIGNED_HEADER_CAPITAL);
        String origSign = req.getParameter(V4Signer.X_AMZ_SIGNATURE_CAPITAL);
        String expirationInSeconds = req.getParameter(V4Signer.X_AMZ_EXPIRES_CAPITAL);
        String date = req.getParameter(V4Signer.X_AMZ_DATE_CAPITAL);
        if (algorithm == null || credential == null || signedHeaders == null || origSign == null || expirationInSeconds == null || date == null) {
            throw new BaseException(400, ErrorMessage.ERROR_CODE_AUTH_QUERY_ERROR, ErrorMessage.ERROR_MESSAGE_MISSING_AUTH_QUERY);
        }
        if (!algorithm.equals(V4Signer.AWS4_SIGNING_ALGORITHM)) {
            throw new BaseException(400, ErrorMessage.ERROR_CODE_AUTH_QUERY_ERROR, ErrorMessage.ERROR_MESSAGE_INVALID_X_AMZ_ALGORITHM);
        }
        String host = req.getHeader(V4Signer.HOST_CAPITAL);
        if (host == null || host.isEmpty())
            throw new BaseException(403, ErrorMessage.ERROR_CODE_403, ErrorMessage.ERROR_MESSAGE_INVALID_HOST);
        String regionName = getRegionNameFromReqHost(host, V4Signer.S3_SERVICE_NAME);
        CredentialItem credentialItem = V4Signer.parseAndCheckCredential(credential, regionName, V4Signer.S3_SERVICE_NAME);
        // 验证X-Amz-SignedHeaders合法性
        V4Signer.checkSignedHeadersValid(new OOSRequest(req), signedHeaders, true, true);
        // 过期时间最大为7天
        int expireInSeconds = Integer.parseInt(expirationInSeconds);
        if (expireInSeconds > Consts.MAX_EXPIRE_NUMBER) {
            throw new BaseException(400, ErrorMessage.ERROR_CODE_AUTH_QUERY_ERROR, ErrorMessage.ERROR_MESSAGE_INVALID_X_AMZ_EXPIRES_TOO_LARGE);
        }
        if (expireInSeconds < 0) {
            throw new BaseException(400, ErrorMessage.ERROR_CODE_AUTH_QUERY_ERROR, ErrorMessage.ERROR_MESSAGE_INVALID_X_AMZ_EXPIRES);
        }
        Date reqDate;
        try {
            reqDate = SignerUtils.formatTimestamp(date);
        } catch (ParseException e) {
            throw new BaseException(400, ErrorMessage.ERROR_CODE_AUTH_QUERY_ERROR, ErrorMessage.ERROR_MESSAGE_INVALID_X_AMZ_DATE);
        }
        // credential date与X-Amz-Date的日期需要一致
        if (date != null && !date.startsWith(credentialItem.dateStamp))
            throw new BaseException(400,
                    ErrorMessage.ERROR_CODE_AUTH_QUERY_ERROR,
                    "Invalid credential date \"" + credentialItem.dateStamp
                            + "\". This date is not the same as X-Amz-Date: \"" + date.split("T")[0] + "\".");
        // 预签名是否过期
        Date expire = DateUtils.addSeconds(reqDate, expireInSeconds);
        Date now = new Date();
        if (expire.compareTo(now) == -1)
            throw new BaseException(403, ErrorMessage.ERROR_CODE_403, ErrorMessage.ERROR_MESSAGE_REQUEST_HAS_EXPIRED);
        String sk;
        String ak = credentialItem.ak;
        // V4签名为大写X-Amz-Security-Token
        String token = req.getParameter("X-Amz-Security-Token");
        AuthResult authResult = new AuthResult();
        if (token== null || token.length() == 0) {
            AkSkMeta asKey = getSecretKeyFromAccessKey(ak);
            authResult.accessKey = asKey;
            authResult.owner = checkAkSk(asKey, false, req, bucket, key);
            sk = asKey.getSecretKey();
        } else {
            // sts临时授权访问，aksk信息从stsToken表获取
            Pair<TokenMeta, OwnerMeta> pair = checkAndGetTokenSkAndOwner(token, ak);
            sk = pair.first().getSecretKey();
            authResult.tokenMeta = pair.first();
            authResult.owner = pair.second();
            authResult.isSts = true;
        }
        // 签名是否一致
        String uri = req.getRequestURI();
        try {
            uri = URLDecoder.decode(uri, Constants.DEFAULT_ENCODING);
        } catch (UnsupportedEncodingException e) {
            throw new BaseException(400, "InvalidURI", "the uri is:" + uri);
        }
        String resourcePath = V4Signer.getCanonicalizedResourcePath(uri);
        String signature = V4Signer.preSign(new OOSRequest(req), ak, sk, regionName, expirationInSeconds, resourcePath);
        if (!signature.equals(origSign))
            throw new BaseException(403, ErrorMessage.ERROR_CODE_SIGNATURE_DOES_NOT_MATCH, ErrorMessage.ERROR_MESSAGE_SIGNATURE_DOES_NOT_MATCH);
        return authResult;
    }

    /**
     * 从请求头Host获取regionName，用于验证V4签名scope，如host=oos-hz.ctyuanapi.cn，则regionName=hz
     * @param host
     * @param serviceName
     * @return
     * @throws BaseException
     */
    public static String getRegionNameFromReqHost(String host, String serviceName) throws BaseException {
        String pattern;
        if (serviceName.equals(V4Signer.STS_SERVICE_NAME)) {
            pattern = "oos-([\\w-]*)-iam.ctyunapi.cn(:\\d*)?$";
        } else if (serviceName.equals(V4Signer.CLOUDTRAIL_SERVICE_NAME)) {
            pattern = "oos-([\\w-]*)-cloudtrail.ctyunapi.cn(:\\d*)?$";
        } else if (serviceName.equals(V4Signer.MANAGE_API_SERVICE_NAME)) {
            pattern = "oos-([\\w-]*)-mg.ctyunapi.cn(:\\d*)?$";
        } else if(serviceName.equals(V4Signer.MAIN_SERVICE_NAME)){
            pattern = "oos-([\\w-]*).ctyun.cn(:\\d*)?$";
        }else {            
            pattern = "oos-([\\w-]*).ctyunapi.cn(:\\d*)?$";
        }
        Pattern r = Pattern.compile(pattern);
        Matcher m = r.matcher(host);
        if (m.find( )) {
            return m.group(1).toLowerCase();
        } else {
            throw new BaseException("can not get regionName from request host header", 403, "AccessDenied", "Access Denied");
        }
    }

    public static AkSkMeta getSecretKeyFromAccessKey(String accessKey)
            throws BaseException, IOException {
        MetaClient client = MetaClient.getGlobalClient();
        AkSkMeta asKey = new AkSkMeta(accessKey);
        if (!client.akskSelect(asKey))
            throw new BaseException(403, "InvalidAccessKeyId", "InvalidAccessKeyId");
        return asKey;
    }

    public static OwnerMeta checkAkSk(AkSkMeta asKey, boolean isMustBePrimaryKey, HttpServletRequest req, String bucket, String key)
            throws Exception {
        OwnerMeta owner = new OwnerMeta(asKey.ownerId);
        if (asKey.status == 0)
            throw new BaseException("the access key is disabled.", 403, "AccessDenied", "AccessDenied");
        MetaClient client = MetaClient.getGlobalClient();
        if (!client.ownerSelectById(owner))
            throw new BaseException("the owner is not exists.", 403, "InvalidAccessKeyId");
        if (owner.verify != null)
            throw new BaseException("the owner is not verify.", 403, "NotVerify", "The user is not verify");
        if (isMustBePrimaryKey && asKey.isPrimary == 0)
            throw new BaseException("the access key is not primary key.", 403, "AccessDenied", "please use primary access key");
        if (asKey.isPrimary == 0){
            checkNotPrimaryKey( bucket, key,req, asKey.accessKey);
        }
        return owner;
    }

    public static void checkFrozenUser(OwnerMeta owner) throws BaseException, ParseException {
        if(owner.auth.allOperationFrozen()) {
            try {
                Pair<Boolean,String> status = BssAdapterClient.getDeleteUserInfo(
                        owner.getId()+"",BssAdapterConfig.localPool.ak,BssAdapterConfig.localPool.getSK());
                if(null != status) {
                    getFrozenStatusException(status);
                }
            } catch (org.json.JSONException | IOException  e) {
                //do nothing
            }
            throw new BaseException("the user has been frozen.", 403, "AccessDenied", ErrorMessage.ERROR_FROZEN_OVERDUE_NORM);
        }
        if (Utils.isFrozen(owner)) {
            throw new BaseException("the user has been frozen.", 403, "AccessDenied", "the user's balance is not enough");
        }
    }
    /**
     * 首先从内建ak里获取数据，否则使用用户ak数据
     */
    public static AkSkMeta getAvailableAk(OwnerMeta owner) throws Exception {
        AkSkMeta asKey = new AkSkMeta(owner.proxyBuiltInAccessKey);
        if (StringUtils.isEmpty(owner.proxyBuiltInAccessKey) || !client.akskSelect(asKey)) {
            //获取内建AK失败时继续使用原有用户AK逻辑
            asKey = new AkSkMeta(owner.getId());
            client.akskSelectPrimaryKeyByOwnerId(asKey);
        }
        return asKey;
    }

//    public static AkSkMeta createBuiltInAKIfNotExists(OwnerMeta owner) throws Exception {
//
//        if (StringUtils.isEmpty(owner.proxyBuiltInAccessKey)) {
//            AkSkMeta asKey = new AkSkMeta(owner.getId());
//            client.akskBuiltInInsert(asKey);
//            return asKey;
//        } else {
//            AkSkMeta asKey = new AkSkMeta(owner.proxyBuiltInAccessKey);
//            client.akskSelect(asKey);
//            return asKey;
//        }
//    }



    public static void checkBssBucketAuth(OwnerMeta owner) throws BaseException{
        if (!owner.auth.getBucketPermission().hasGet()) {
            throw new BaseException(403, "AccessDenied", "the user's permission is not enough");
        }
    }
    
    public static void checkBssObjectAuth(OwnerMeta owner, String key, String methodName) throws BaseException {
        //获取bucket的object list
        if ((key == null || key.length() == 0) && (methodName.equals(HttpMethod.GET.toString()))){
           if (!owner.auth.getBucketPermission().hasGet()) {
               throw new BaseException(403, "AccessDenied", "the user's permission is not enough");
           }         
        }
        
        
        //获取object
        if ((key != null && key.length() > 0) && 
                (methodName.equals(HttpMethod.GET.toString()) || methodName.equals(HttpMethod.HEAD.toString()))){
           if (!owner.auth.getObjectPermission().hasGet()) {
               throw new BaseException(403, "AccessDenied", "the user's permission is not enough");
           }         
        }
        
        //上传object
        if (methodName.equals(HttpMethod.PUT.toString())||
                methodName.equals(HttpMethod.POST.toString())){
           if (!owner.auth.getObjectPermission().hasPut()) {
               throw new BaseException(403, "AccessDenied", "the user's permission is not enough");
           }         
        }
        
        //删除object
        if (methodName.equals(HttpMethod.DELETE.toString())){
            if (!owner.auth.getObjectPermission().hasDelete()) {
                throw new BaseException(403, "AccessDenied", "the user's permission is not enough");
            }         
         }
    }

    public static long getObjectSizeFromMeta(String meta) {
        if (meta != null) {
            meta = meta.trim();
            int pos = meta.indexOf("size=");
            if (pos >= 0) {
                String value;
                int comma = meta.indexOf(',', pos);
                if (comma == -1) { // size=是末尾的k-v对
                    value = meta.substring(meta.lastIndexOf('=') + 1);
                } else
                    value = meta.substring(meta.lastIndexOf('=', comma) + 1, comma);
                return Long.parseLong(value);
            }
        }
        return 0;
    }

    public static boolean isPublicIP(String ip) {
        if (!ip.contains(":")) {
            // ipv4
            try {
                SubnetUtils subnetUtils = new SubnetUtils(BackoffConfig.getPrivateIp());
                subnetUtils.setInclusiveHostCount(true);
                if (subnetUtils.getInfo().isInRange(ip))
                    return false;
                else
                    return true;
            } catch (Exception e) {
                log.error(e.getMessage());
                return true;
            }
        } else {
            // ipv6都是public ip
            return true;
        }
    }

    // 计算冗余容量
    public static long getRedundantSize(long totalSize, ReplicaMode mode) {
        //EC_N_M
        int m = mode.ec_m;
        int n = mode.ec_n;
        return m != 0 ? totalSize * (n + m) / n : totalSize * n;
    }

    // 计算对齐后容量
    public static long getAlinSize(long totalSize, int pageSize, ReplicaMode mode) {
        int m = mode.ec_m;
        int n = mode.ec_n;
        if (m != 0) {
            //EC_N_M模式
            long pageGroup = totalSize / pageSize / n;
            pageGroup = (totalSize % ( pageSize * n ) == 0) ? pageGroup : pageGroup + 1;
            long size = pageGroup * (pageSize * (n + m));
            return size;
        } else {
            //简单多副本模式
            long size = totalSize * n;
            return size;
        }
    }

    private static long nanoSecond_ = System.currentTimeMillis() * 1000000;
    private static long nanoSecond = System.nanoTime();

    public static long getNanoTime() {
        long deltNano = System.nanoTime() - nanoSecond;
        return nanoSecond_ + deltNano;
    }

   /*public static void pinPut(int status, BucketLog bucketLog, long ownerId, Pin pin,
            PinData pinData, boolean isInternetIP, String bucketName) {
        if (isValidStatus(status)) {
            bucketLog.pinTime = System.currentTimeMillis();
            pinOtherRequest(status, ownerId, pin, pinData, isInternetIP, bucketName);
            pinStorage(ownerId, pin, pinData, bucketName);
            pinUpload(ownerId, pin, pinData, isInternetIP, bucketName);
            bucketLog.pinTime = System.currentTimeMillis() - bucketLog.pinTime;
        }
    }*/

   /* public static void pinGet(int status, BucketLog bucketLog, long ownerId, Pin pin,
            PinData pinData, boolean isInternetIP, String bucketName) {
        if (Utils.isValidStatus(status)) {
            bucketLog.pinTime = System.currentTimeMillis();
            pinGetHeadRequest(status, ownerId, pin, pinData, isInternetIP, bucketName);
            pinTransfer(ownerId, pin, pinData, isInternetIP, bucketName);
            bucketLog.pinTime = System.currentTimeMillis() - bucketLog.pinTime;
        }
    }
*/
    /**
     * GetHead请求次数统计，用于计费
     * @param ownerId
     * @param pin
     * @param pinData
     * @param isInternetIP
     * @param bucketName
     */
//    public static void pinGetHeadRequest(long ownerId, Pin pin, PinData pinData,
//            boolean isInternetIP, String bucketName) {
//        if (isInternetIP) {
//            pin.increGHRequest(ownerId, getPinRegion(pinData), bucketName);
//        } else {
//            pin.increNoNetGHReq(ownerId, getPinRegion(pinData), bucketName);
//        }
//    }
    
    /**
     * Other请求次数统计，用于计费
     * @param ownerId
     * @param pin
     * @param pinData
     * @param isInternetIP
     * @param bucketName
     */
//    public static void pinOtherRequest(long ownerId, Pin pin, PinData pinData,
//            boolean isInternetIP, String bucketName) {
//        if (isInternetIP) {
//            pin.increOtherRequest(ownerId, getPinRegion(pinData), bucketName);
//        } else {
//            pin.increNoNetOtherReq(ownerId, getPinRegion(pinData), bucketName);
//        }
//    }
    
    public static String getPinRegion(PinData pinData){
        return (pinData.getDirectRegion() != null ? pinData.getDirectRegion()
                : (pinData.getRoamRegion() != null ? pinData.getRoamRegion()
                        : DataRegion.getRegion().getName()));
    }
    
    /**TODO bucketlog
     * 根据请求方法及返回码增加请求次数
     * @param pin
     * @param pinData
     * @param status
     */
    public static void pinRequest(Pin pin, PinData pinData, int status, BucketLog bucketLog) {
        bucketLog.pinTime = System.currentTimeMillis();
        String method = pinData.getMethodType();
        if (method.equalsIgnoreCase(HttpMethod.PUT.toString())) {
            pinPutMethodRequest(pinData.getOwnerId(), pin, pinData, pinData.getIsInternetIp(), pinData.getBucketName(), status, pinData.getStorageType());
        } else if (method.equalsIgnoreCase(HttpMethod.GET.toString())) {
            pinGetMethodRequest(pinData.getOwnerId(), pin, pinData, pinData.getIsInternetIp(), pinData.getBucketName(), status, pinData.getStorageType());
        } else if (method.equalsIgnoreCase(HttpMethod.HEAD.toString())) {
            pinHeadMethodRequest(pinData.getOwnerId(), pin, pinData, pinData.getIsInternetIp(), pinData.getBucketName(), status, pinData.getStorageType());
        } else if (method.equalsIgnoreCase(HttpMethod.DELETE.toString())) {
            pinDeleteMethodRequest(pinData.getOwnerId(), pin, pinData, pinData.getIsInternetIp(), pinData.getBucketName(), status, pinData.getStorageType());
        } else if (method.equalsIgnoreCase(HttpMethod.POST.toString())) {
            pinPostMethodRequest(pinData.getOwnerId(), pin, pinData, pinData.getIsInternetIp(), pinData.getBucketName(), status, pinData.getStorageType());
        } else {
            pinOtherMethodRequest(pinData.getOwnerId(), pin, pinData, pinData.getIsInternetIp(), pinData.getBucketName(), status, pinData.getStorageType());
        }
        bucketLog.pinTime = System.currentTimeMillis() - bucketLog.pinTime;
    }
    
    /**
     * HTTP Get Method请求次数统计，用于计量
     * @param ownerId
     * @param pin
     * @param pinData
     * @param isInternetIP
     * @param bucketName
     */
    private static void pinGetMethodRequest(long ownerId, Pin pin, PinData pinData,
            boolean isInternetIP, String bucketName, int status, String storageType) {
        if (isInternetIP) {
            pin.increGetMethodRequest(ownerId, getPinRegion(pinData), bucketName, status, storageType);
        } else {
            pin.increNoNetGetMethodReq(ownerId, getPinRegion(pinData), bucketName, status, storageType);
        }
    }
    
    /**
     * HTTP Head Method请求次数统计，用于计量
     * @param ownerId
     * @param pin
     * @param pinData
     * @param isInternetIP
     * @param bucketName
     */
    private static void pinHeadMethodRequest(long ownerId, Pin pin, PinData pinData,
            boolean isInternetIP, String bucketName, int status, String storageType) {
        if (isInternetIP) {
            pin.increHeadMethodRequest(ownerId, getPinRegion(pinData), bucketName, status, storageType);
        } else {
            pin.increNoNetHeadMethodReq(ownerId, getPinRegion(pinData), bucketName, status, storageType);
        }
    }
    
    /**
     * HTTP Put Method请求次数统计，用于计量
     * @param ownerId
     * @param pin
     * @param pinData
     * @param isInternetIP
     * @param bucketName
     */
    private static void pinPutMethodRequest(long ownerId, Pin pin,
            PinData pinData, boolean isInternetIP, String bucketName, int status, String storageType) {
      if (isInternetIP) {
          pin.increPutMethodRequest(ownerId, getPinRegion(pinData), bucketName, status, storageType);
      } else {
          pin.increNoNetPutMethodReq(ownerId, getPinRegion(pinData), bucketName, status, storageType);
      }
   }
    
    /**
     * HTTP Post Method请求次数统计，用于计量
     * @param ownerId
     * @param pin
     * @param pinData
     * @param isInternetIP
     * @param bucketName
     */
    private static void pinPostMethodRequest(long ownerId, Pin pin,
            PinData pinData, boolean isInternetIP, String bucketName, int status, String storageType) {
      if (isInternetIP) {
          pin.increPostMethodRequest(ownerId, getPinRegion(pinData), bucketName, status, storageType);
      } else {
          pin.increNoNetPostMethodReq(ownerId, getPinRegion(pinData), bucketName, status, storageType);
      }
   }
    
    /**
     * HTTP Delete Method请求次数统计，用于计量
     * @param ownerId
     * @param pin
     * @param pinData
     * @param isInternetIP
     * @param bucketName
     */
    private static void pinDeleteMethodRequest(long ownerId, Pin pin,
            PinData pinData, boolean isInternetIP, String bucketName, int status, String storageType) {
      if (isInternetIP) {
          pin.increDeleteMethodRequest(ownerId, getPinRegion(pinData), bucketName, status, storageType);
      } else {
          pin.increNoNetDeleteMethodReq(ownerId, getPinRegion(pinData), bucketName, status, storageType);
      }
   }
    
    /**
     * 其它HTTP Method(Options)请求次数统计，用于计量
     * @param ownerId
     * @param pin
     * @param pinData
     * @param isInternetIP
     * @param bucketName
     */
    private static void pinOtherMethodRequest(long ownerId, Pin pin,
            PinData pinData, boolean isInternetIP, String bucketName, int status, String storageType) {
      if (isInternetIP) {
          pin.increOtherMethodRequest(ownerId, getPinRegion(pinData), bucketName, status, storageType);
      } else {
          pin.increNoNetOtherMethodReq(ownerId, getPinRegion(pinData), bucketName, status, storageType);
      }
   }
   
   public static void pinDeleteFlow(long ownerId, Pin pin, PinData pinData,
           boolean isInternetIP, String bucketName) {
      if (isInternetIP) {
          pin.deleteFlow(ownerId, pinData.getTotalSize(), getPinRegion(pinData), bucketName, pinData.getStorageType());
      } else {
          pin.noNetDeleteFlow(ownerId, pinData.getTotalSize(), getPinRegion(pinData), bucketName, pinData.getStorageType());
      }
   }

    public static void pinTransfer(long ownerId, Pin pin, PinData pinData, boolean isInternetIP, String bucketName) {
        // transfer、roamFlow由原来的直接下载流量、漫游下载流量字段改为互联网的直接下载流量、漫游下载流量字段，以避免字段太多。
        if (pinData.getRoamRegion() != null){
            if (isInternetIP) {
                pin.increRoamFlow(ownerId, pinData.getRoamFlow() + pinData.jettyAttributes.respHeadFlow, pinData.getRoamRegion(), bucketName, pinData.getStorageType());
            } else {
                pin.increNoNetRoamFlow(ownerId, pinData.getRoamFlow() + pinData.jettyAttributes.respHeadFlow, pinData.getRoamRegion(), bucketName, pinData.getStorageType());
            }
        } else {
            if (isInternetIP) {
                pin.increTransfer(ownerId, pinData.getFlow() + pinData.jettyAttributes.respHeadFlow, getPinRegion(pinData), bucketName, pinData.getStorageType());
            } else {
                pin.increNoNetTransfer(ownerId, pinData.getFlow() + pinData.jettyAttributes.respHeadFlow, getPinRegion(pinData), bucketName, pinData.getStorageType());
            }
        }
    }
    
    public static void pinRestore(long ownerId, Pin pin, PinData pinData, String bucketName) {
        if (!StringUtils.isEmpty(pinData.getSourceDataRegion())) {
            pin.increSizeRestore(ownerId, pinData.getRestore(), pinData.getSourceDataRegion(), pinData.getSourceBucket(), pinData.getSourceStorageType());
            return;
        }
        if (pinData.needRestoreAndCompelete()) {
            if (pinData.getRoamRegion() != null){
                pin.increSizeRestore(ownerId, pinData.getRoamFlow(), pinData.getRoamRegion(), bucketName, pinData.getStorageType());
            } else {
                pin.increSizeRestore(ownerId, pinData.getFlow(), getPinRegion(pinData), bucketName, pinData.getStorageType());
            }
        }
    }

    public static void pinUpload(long ownerId, Pin pin, PinData pinData, boolean isInternetIP, String bucketName) {
        // upload、roamUpload由原来的直接上传流量、漫游上传流量字段改为互联网的直接上传流量、漫游上传流量字段，以避免字段太多。
        if(pinData.getRoamRegion() != null) {
            if (isInternetIP) {
                pin.increRoamUpload(ownerId, pinData.getRoamUpload() + pinData.jettyAttributes.reqHeadFlow, pinData.getRoamRegion(), bucketName, pinData.getStorageType());
            } else {
                pin.increNoNetRoamUpload(ownerId, pinData.getRoamUpload() + pinData.jettyAttributes.reqHeadFlow, pinData.getRoamRegion(), bucketName, pinData.getStorageType());
            }
        } else {
            if (isInternetIP) {
                pin.increUpload(ownerId, pinData.getUpload() + pinData.jettyAttributes.reqHeadFlow, getPinRegion(pinData), bucketName, pinData.getStorageType());
            } else {
                pin.increNoNetUpload(ownerId, pinData.getUpload() + pinData.jettyAttributes.reqHeadFlow, getPinRegion(pinData), bucketName, pinData.getStorageType());
            }
        }
    }

    public static void pinStorage(long ownerId, Pin pin, PinData pinData, String bucketName) {
        //覆盖上传
        if(pinData.sourceObject != null) {
            if(pinData.getStorageType().equals(pinData.sourceObject.getStorageType()) && pinData.getDataRegion().equals(pinData.sourceObject.getDataRegion())) {
                pin.increStorage(ownerId, pinData.getTotalSize() - pinData.sourceObject.getSourceSize(), pinData.getDataRegion(), bucketName, pinData.getStorageType());
                pin.increOriginalStorage(ownerId, pinData.getOriginalTotalSize() - pinData.sourceObject.getSourceSize(), pinData.getOriginalRegion(), bucketName, pinData.getStorageType());
                pin.increRedundantStorage(ownerId, pinData.getRedundantSize() - pinData.sourceObject.getSourceRedundantSize(), pinData.getDataRegion(), bucketName, pinData.getStorageType());
                pin.increAlignStorage(ownerId, pinData.getAlinSize() - pinData.sourceObject.getSourceAlinSize(), pinData.getDataRegion(), bucketName, pinData.getStorageType());
            } else {
                //删除减少的存储类型的容量
                pin.decreStorage(ownerId, pinData.sourceObject.getSourceSize(), pinData.sourceObject.getDataRegion(), bucketName, pinData.sourceObject.getStorageType());
                pin.decreOriginalStorage(ownerId, pinData.sourceObject.getSourceSize(), pinData.sourceObject.getOriginalRegion(), bucketName, pinData.sourceObject.getStorageType());
                pin.decreRedundantStorage(ownerId, pinData.sourceObject.getSourceRedundantSize(), pinData.sourceObject.getDataRegion(), bucketName, pinData.sourceObject.getStorageType());
                pin.decreAlignStorage(ownerId, pinData.sourceObject.getSourceAlinSize(), pinData.sourceObject.getDataRegion(), bucketName, pinData.sourceObject.getStorageType());
                
                //增加新增的
                pin.increStorage(ownerId, pinData.getTotalSize(), pinData.getDataRegion(), bucketName, pinData.getStorageType());
                pin.increOriginalStorage(ownerId, pinData.getOriginalTotalSize(), pinData.getOriginalRegion(), bucketName, pinData.getStorageType());
                pin.increRedundantStorage(ownerId, pinData.getRedundantSize(), pinData.getDataRegion(), bucketName, pinData.getStorageType());
                pin.increAlignStorage(ownerId, pinData.getAlinSize(), pinData.getDataRegion(), bucketName, pinData.getStorageType());
            }
            //减去原object的补齐容量
            if(pinData.sourceObject.needRestoreAndCompelete() && pinData.sourceObject.getSourceSize() < Misc.MIN_STORAGE_IA  && !pinData.sourceObject.getIsMultipartUpload()) {
                pin.increSizeCompelete(ownerId, pinData.sourceObject.getSourceSize() - Misc.MIN_STORAGE_IA, pinData.sourceObject.getDataRegion(), bucketName, pinData.sourceObject.getStorageType());
            }
            if(pinData.needRestoreAndCompelete() || pinData.sourceObject.needRestoreAndCompelete()) {
                pinChangeSize(ownerId, pin, pinData, bucketName); 
            }
        } else {
            pin.increStorage(ownerId, pinData.getTotalSize(), pinData.getDataRegion(), bucketName, pinData.getStorageType());
            pin.increOriginalStorage(ownerId, pinData.getOriginalTotalSize(), pinData.getOriginalRegion(), bucketName, pinData.getStorageType());
            pin.increRedundantStorage(ownerId, pinData.getRedundantSize(), pinData.getDataRegion(), bucketName, pinData.getStorageType());
            pin.increAlignStorage(ownerId, pinData.getAlinSize(), pinData.getDataRegion(), bucketName, pinData.getStorageType());
        }
        //大小补齐
        if(pinData.needRestoreAndCompelete() && pinData.getTotalSize() < Misc.MIN_STORAGE_IA  && !pinData.getIsMultipartUpload()) {
            pin.increSizeCompelete(ownerId, Misc.MIN_STORAGE_IA - pinData.getTotalSize(), pinData.getDataRegion(), bucketName, pinData.getStorageType());
        }
    }
    
    private static void pinChangeSize(long ownerId, Pin pin, PinData pinData, String bucketName) {
        pin.increChangeSize(ownerId, pinData.sourceObject.getSourceSize(), pinData.sourceObject.getDataRegion(), bucketName, pinData.sourceObject.getStorageType());
        long days = pinData.getLastModify() - pinData.sourceObject.getLastModify();
        if (pinData.sourceObject.needRestoreAndCompelete() && days < Misc.THIRTY_DAY_MILLSECONDS) {
            pin.increPreChangeSize(ownerId, pinData.sourceObject.getSourceSize(), pinData.sourceObject.getDataRegion(), bucketName, pinData.sourceObject.getStorageType());
            pin.increPreChangeCompelete(ownerId, computeComplete(pinData.sourceObject.getSourceSize(), days), pinData.sourceObject.getDataRegion(), bucketName, pinData.sourceObject.getStorageType());
        }
    }
    
    private static long computeComplete(long size, long day) {        
        long dayRemainder = (Misc.THIRTY_DAY_MILLSECONDS - day) / Misc.DAY_MILLSECONDS;
        if (size < Misc.MIN_STORAGE_IA) {
            return Misc.MIN_STORAGE_IA * dayRemainder;
        } else 
            return size * dayRemainder;
    }
    
    public static void pinDecreStorage(long ownerId, Pin pin, PinData pinData, String bucketName) {
        pin.decreStorage(ownerId, pinData.getTotalSize(), pinData.getDataRegion(), bucketName, pinData.getStorageType());
        pin.decreOriginalStorage(ownerId, pinData.getOriginalTotalSize(), pinData.getOriginalRegion(), bucketName, pinData.getStorageType());
        pin.decreRedundantStorage(ownerId, pinData.getRedundantSize(), pinData.getDataRegion(), bucketName, pinData.getStorageType());
        pin.decreAlignStorage(ownerId, pinData.getAlinSize(), pinData.getDataRegion(), bucketName, pinData.getStorageType());
        long days = System.currentTimeMillis() - pinData.getLastModify();
        if(pinData.needRestoreAndCompelete()) {
            if (days < Misc.THIRTY_DAY_MILLSECONDS) {
                pin.increPreDelete(ownerId, pinData.getTotalSize(), pinData.getDataRegion(), bucketName, pinData.getStorageType());
                pin.increPreDeleteCompelete(ownerId, computeComplete(pinData.getTotalSize(), days), pinData.getDataRegion(), bucketName, pinData.getStorageType());
            }
            if (pinData.getTotalSize() < Misc.MIN_STORAGE_IA && !pinData.getIsMultipartUpload()) {
                pin.increSizeCompelete(ownerId, pinData.getTotalSize() - Misc.MIN_STORAGE_IA, pinData.getDataRegion(), bucketName, pinData.getStorageType());
            }
        }
    }

   /* public static boolean isValidStatus(int status) {
        if (status != 403)
            return true;
        else
            return false;
    }
*/
    /*public static void pinDelete(int status, BucketLog bucketLog, long ownerId, Pin pin,
            PinData pinData, boolean isInternetIP, String bucketName) {
        if (Utils.isValidStatus(status)) {
            pinDecreStorage(ownerId, pin, pinData, bucketName);
            pinOtherRequest(status, ownerId, pin, pinData, isInternetIP, bucketName);
        }
    }*/
    
    /**
     * 文本反垃圾请求计量
     * @param code
     * @param ownerId
     * @param pin
     * @param pinData
     */
    public static void pinSpam(int code, long ownerId, Pin pin, PinData pinData, String bucketName) {
        if (code == 200) {
            pin.increSpamRequest(ownerId, getPinRegion(pinData), bucketName, pinData.getStorageType());
        }
    }
    
    /**
     * 图片鉴黄张数计量，算法确定、不确定部分计费标准不同，分开计量
     * @param code
     * @param respResultArray
     * @param ownerId
     * @param pin
     * @param pinData
     * @throws JSONException 
     */
    public static void pinPorn(int code, JSONArray respResultArray,
            long ownerId, Pin pin, PinData pinData, String bucketName)
            throws JSONException {
        if (code == 200 && respResultArray != null) {
            boolean review = false;
            for (int i = 0; i < respResultArray.length(); i++) {
                JSONObject jObject = respResultArray.getJSONObject(i);
                boolean rev = jObject.getBoolean(KEYWORD_REVIEW);
                if (rev) {
                    review = true;
                    break;
                }
            }
            if (!review) {
                // 算法确定部分
                pin.increPornReviewFalseRequset(ownerId, getPinRegion(pinData), bucketName, pinData.getStorageType());
            } else {
                // 待用户确定部分
                pin.increPornReviewTrueRequset(ownerId, getPinRegion(pinData), bucketName, pinData.getStorageType());
            }
        }
    }

    public static void logError(Throwable e, BucketLog bucketLog) {
        StringBuilder sb = new StringBuilder();
        if (e instanceof BaseException) {
            if (((BaseException) e).status >= 500)
                sb.append("Error ").append(bucketLog != null ? bucketLog.URI : "").append(" ")
                        .append(bucketLog != null ? bucketLog.requestId : "");
        }
        if (e.getMessage() != null)
            sb.append(e.getMessage());
        sb.append(" ").append(bucketLog.requestId);
        log.error(sb.toString(), e);
    }

    public static long getTimeStamp() throws BaseException {
        return System.currentTimeMillis();
    }

    public static void setCommonHeader(HttpServletResponse resp, Date date, String requestId) {
        resp.setHeader(Headers.DATE, ServiceUtils.formatRfc822Date(date));
        resp.setHeader(Headers.REQUEST_ID, requestId);
    }
    
    
    public static long getRespHeaderLength(HttpServletResponse resp) {
        Collection<String> headerNames = resp.getHeaderNames();
        long length = 0;
        for (String name : headerNames) {
            String key = name;
            String value = resp.getHeader(name);
            length += (key + "=" + value).length();
        } 
        return length;
    }
    
    /**
     * 多版本响应头增加版本号ID,网关多版本只有get对象时返回版本号
     * @param request
     * @param response
     * @param bucket
     * @param object
     * @param returnGwVersionId
     */
    public static void setVersionIdHeaderIfVersioning(
            HttpServletRequest request, HttpServletResponse response,
            BucketMeta bucket, ObjectMeta object, boolean returnGwVersionId) {
        if(MultiVersionsController.gwMultiVersionEnabled(request, bucket)) {
            if (object.version != null && object.version.trim().length() != 0) {
                String gwVersion = request.getHeader("x-ctyun-gw-version");
                if (gwVersion != null && returnGwVersionId) {
                    response.setHeader("x-ctyun-gw-version", object.version);
                } else {
                    response.setHeader("x-amz-version-id", object.version);
                }
            }
        }
    }

    public static void checkDate(HttpServletRequest req) throws BaseException {
        //预签名不校验请求头。
        if(isPresignedUrl(req))
            return;
        Date clientDate = Utils.getDate(req);
        Date serverDateMax = DateUtils.addMinutes(new Date(), OOSConfig.getTimeDifference());
        Date serverDateMin = DateUtils.addMinutes(new Date(), 0 - OOSConfig.getTimeDifference());
        if (clientDate != null) {
            if (serverDateMax.compareTo(clientDate) == -1
                    || serverDateMin.compareTo(clientDate) == 1) {
                throw new BaseException("The time difference between the server and the client is over 15 minutes.", 403, "RequestTimeTooSkewed"
                        ,"The difference between the request time and the server's time is too large.");
            }
        }
    }

    public static void log(HttpServletRequest req, String ipAddress) {
        final StringBuilder sb = new StringBuilder();
        sb.append(ipAddress).append(" ");
        sb.append(req.getMethod()).append(" ");
        sb.append("\"").append(req.getRequestURI()).append(" ").append("\" ");
        sb.append("requestId:").append(req.getAttribute(Headers.REQUEST_ID)).append(" ");
        String k = null;
        String v = null;
        sb.append("Headers: ");
        Enumeration<?> e = req.getHeaderNames();
        while (e.hasMoreElements()) {
            k = (String) e.nextElement();
            v = req.getHeader(k);
            sb.append(k).append("=").append(v).append(" ");
        }
        sb.append("queryString:").append(req.getQueryString());
        LogUtils.log(sb.toString());
    }

    public static void writeResponseEntity(HttpServletResponse resp, String body,
            HttpServletRequest req) throws IOException {
        resp.setCharacterEncoding(Consts.STR_UTF8);
        Utils.setCommonHeader(resp, (Date) req.getAttribute(Headers.DATE),
                (String) req.getAttribute(Headers.REQUEST_ID));
        if (body != null) {
            resp.setIntHeader(Headers.CONTENT_LENGTH, body.getBytes(Consts.CS_UTF8).length);
            try {
                resp.getOutputStream().write(body.getBytes(Consts.STR_UTF8));
            } catch (Exception e) {
                log.error("catch write response error");
            }
        } else
            resp.setIntHeader(Headers.CONTENT_LENGTH, 0);
    }

    public static void checkMd5(String contentMD5, String etag) throws BaseException {
        if (!Arrays.equals(BinaryUtils.fromBase64(contentMD5), BinaryUtils.fromHex(etag)))
            throw new BaseException(400, ErrorMessage.ERROR_CODE_BAD_DIGEST,
                    "The Content-MD5 you specified did not match what we received.");
    }

    public static boolean isAlphaAndNumeric(CharSequence cs) {
        if (cs == null || cs.length() == 0) {
            return false;
        }
        int sz = cs.length();
        boolean isAlpha = false;
        boolean isNumeric = false;
        for (int i = 0; i < sz; i++) {
            if (!isAlpha && Character.isLetter(cs.charAt(i))) {
                isAlpha = true;
            }
            if (!isNumeric && Character.isDigit(cs.charAt(i))) {
                isNumeric = true;
            }
            if (isAlpha && isNumeric)
                return true;
        }
        return false;
    }
    
    /**
     * 获取上个月第一天和上个月最后一天
     * @return
     */
    public static Pair<String,String> getLastMonthRegionDays() {
        LocalDate date = LocalDate.now();
        //获取上个月第一天和上个月最后一天
        LocalDate firstDay = date.minusMonths(1).with(TemporalAdjusters.firstDayOfMonth());
        LocalDate lastDay = date.minusMonths(1).with(TemporalAdjusters.lastDayOfMonth());
        Pair<String,String> p = new Pair<String,String>();
        p.first(firstDay.toString());
        p.second(lastDay.toString());
        return p;
    }

    //获取上个月最后一天
    public static String getLastMonthLastDay() {
        LocalDate date = LocalDate.now();
        LocalDate lastDay = date.minusMonths(1).with(TemporalAdjusters.lastDayOfMonth());
        return lastDay.toString();
    }

    
    /**
     * 获取起始日期中间的日期列表，包括起始日期
     * @param begin
     * @param end
     * @return
     */
    public static List<String> getDatesBetweenTwoDate(String begin, String end) {
        List<String> lDate = new ArrayList<String>();
        lDate.add(begin);
        LocalDate beginDate = LocalDate.parse(begin);
        LocalDate endDate = LocalDate.parse(end);
        int num = (int) ChronoUnit.DAYS.between(beginDate, endDate);
        LocalDate tmp = beginDate;
        for (int i = 0; i < num; i++) {
            tmp = tmp.plusDays(1);
            lDate.add(tmp.toString());
        }
        return lDate;
    }

    
    /**
     * 获取起始月份中间的月份列表，包括起始月份
     * @param begin
     * @param end
     * @return
     */
    public static List<String> getMonsBetweenTwoMon(String begin, String end) {
        List<String> lDate = new ArrayList<String>();
        lDate.add(begin);
        YearMonth beginDate = YearMonth.parse(begin, formatyyyy_mm);
        YearMonth endDate = YearMonth.parse(end, formatyyyy_mm);
        int num = (int) ChronoUnit.MONTHS.between(beginDate, endDate);
        YearMonth tmp = beginDate;
        for (int i = 0; i < num; i++) {
            tmp = tmp.plusMonths(1);
            lDate.add(tmp.toString());
        }
        return lDate;
    }

    //获取上个月月份，返回字符串格式"yyyy-MM"
    public static String getLastMonth() {
        LocalDate date = LocalDate.now();
        String lastMonth = date.minusMonths(1).format(formatyyyy_mm);
        return lastMonth;
    }

    // 返回错误响应信息中的resource，格式/bucket/object
    public static String getResource(BucketLog bucketLog) {
        String resource = "";
        if (bucketLog.bucketName != null && bucketLog.bucketName.trim().length() != 0)
            resource = "/" + bucketLog.bucketName;
        if (bucketLog.objectName != null && bucketLog.objectName.trim().length() != 0)
            resource += "/" + bucketLog.objectName;
        return resource;
    }
    
    /**
     * 判断是否为合法ipv4
     * @param ip
     * @return
     */
    public static boolean isValidIPv4Addr(String ip){
        return getPatternCompile(ipRegExp).matcher(ip).matches();
    }
    
    /**
     * 判断是否为合法ipv4 cidr
     * @param ip
     * @return
     */
    public static boolean isValidIPv4CidrAddr(String ip){
        return getPatternCompile(cidrStrictRegExp).matcher(ip).matches();
    }

    /**
     * 判断是否为合法ipv6
     * @param ip
     * @return
     */
    public static boolean isValidIpv6Addr(String ip) {
        InetAddress addressIPv6;
        try {
            addressIPv6 = InetAddress.getByName(ip);
            if (addressIPv6 instanceof Inet6Address) {
                return true;
            } else {
                return false;
            }
        } catch (UnknownHostException e) {
            return false;
        }
    }
    
    /**
     * 判断是否为合法ipv6 cidr
     * @param ip
     * @return
     */
    public static boolean isValidIpv6Cidr(String ip) {
        if (ip.contains(":")) {
            try {
                if (ip.contains("/")) {
                    int index = ip.indexOf("/");
                    String addressPart = ip.substring(0, index);
                    String networkPart = ip.substring(index + 1);
                    int prefixLength = Integer.parseInt(networkPart);
                    if (prefixLength < 0 || prefixLength > 128) {
                        return false;
                    }
                    return isValidIpv6Addr(addressPart);
                } else {
                    return false;
                }                       
            } catch (Exception e) {
                return false;
            }
        } else {
            return false;
        }
    }
    
    /**
     * 判断是否为合法ip，包括ipv4、ipv6格式
     * @param ip
     * @return
     */
    public static boolean isValidIpAddr(String ip) {
        if (!isValidIPv4Addr(ip) && !isValidIpv6Addr(ip))
            return false;
        return true;
    }
    
    /**
     * 判断是否为合法ip，包括ipv4、ipv6及其CIDR格式
     * @param ip
     * @throws
     */
    public static boolean isValidIpOrCidr(String ip) {
        if (!isValidIPv4Addr(ip) && !isValidIPv4CidrAddr(ip) && !isValidIpv6Addr(ip) && !isValidIpv6Cidr(ip))
            return false;
        return true;
    }
    
    /**
     * 返回ipv4，ipv6列表
     * @param sourceIp
     * @return
     * @throws JSONException
     */
    public static Pair<List<String>,List<String>> getSourceIpSeparated(JSONArray sourceIp) throws JSONException {
        Pair<List<String>,List<String>> p = new Pair<List<String>,List<String>>();
        List<String> ipv4 = new ArrayList<String>();
        List<String> ipv6 = new ArrayList<String>();
        for (int i = 0; i < sourceIp.length(); i++) {
            String sip = (String) sourceIp.get(i);
            if (!sip.contains(":")) {
                ipv4.add(sip);
            } else {
                ipv6.add(sip);
            }            
        }
        p.first(ipv4);
        p.second(ipv6);
        return p;       
    }
    
    /**
     * 判断ip(单个ip)是否属于sourceIp(ipv6或ipv6 cidr)范围内
     * @param ip
     * @param sourceIp
     * @return
     * @throws UnknownHostException
     */
    public static boolean isInIpv6Range(String ip, String sourceIp) throws UnknownHostException {
        if (sourceIp.contains("/")) {
            // souceIp为ipv6 cidr
            InetAddress address = InetAddress.getByName(ip);
            Pair<InetAddress, InetAddress> p = calculate(sourceIp);
            BigInteger start = new BigInteger(1, p.first().getAddress());
            BigInteger end = new BigInteger(1, p.second().getAddress());
            BigInteger target = new BigInteger(1, address.getAddress());
            int st = start.compareTo(target);
            int te = target.compareTo(end);
            return (st == -1 || st == 0) && (te == -1 || te == 0);
        } else {
            // souceIp为ipv6
            return twoIpv6IsSame(ip, sourceIp);
        }        
    }
    
    /**
     * 判断两个ipv6是否相等
     * @param ip1
     * @param ip2
     * @return
     * @throws UnknownHostException
     */
    public static boolean twoIpv6IsSame(String ip1, String ip2) throws UnknownHostException {
        InetAddress ipAddress = InetAddress.getByName(ip1);
        InetAddress sourceIpAddress = InetAddress.getByName(ip2);           
        if (ipAddress.getHostAddress().equals(sourceIpAddress.getHostAddress())) {
            return true;
        } else {
            return false;
        }
    }
    
    
    /**
     * 计算ipv6地址段的首末地址
     * @param cidr
     * @return
     * @throws UnknownHostException
     */
    public static Pair<InetAddress,InetAddress> calculate(String cidr) throws UnknownHostException {
        Pair<InetAddress,InetAddress> p = new Pair<InetAddress,InetAddress>();
        int prefixLength;
        if (cidr.contains("/")) {
            int index = cidr.indexOf("/");
            String addressPart = cidr.substring(0, index);
            String networkPart = cidr.substring(index + 1);
            InetAddress inetAddress = InetAddress.getByName(addressPart);
            prefixLength = Integer.parseInt(networkPart);
            ByteBuffer maskBuffer;
            int targetSize;
            if (inetAddress.getAddress().length == 4) {
                maskBuffer = ByteBuffer.allocate(4).putInt(-1);
                targetSize = 4;
            } else {
                maskBuffer = ByteBuffer.allocate(16).putLong(-1L).putLong(-1L);
                targetSize = 16;
            }
            BigInteger mask = (new BigInteger(1, maskBuffer.array())).not().shiftRight(prefixLength);
            ByteBuffer buffer = ByteBuffer.wrap(inetAddress.getAddress());
            BigInteger ipVal = new BigInteger(1, buffer.array());
            BigInteger startIp = ipVal.and(mask);
            BigInteger endIp = startIp.add(mask.not());
            byte[] startIpArr = toBytes(startIp.toByteArray(), targetSize);
            byte[] endIpArr = toBytes(endIp.toByteArray(), targetSize);
            InetAddress startAddress = InetAddress.getByAddress(startIpArr);
            InetAddress endAddress = InetAddress.getByAddress(endIpArr);
            p.first(startAddress);
            p.second(endAddress);
            return p;
        } else {
            InetAddress inetAddress1 = InetAddress.getByName(cidr);
            p.first(inetAddress1);
            p.second(inetAddress1);
            return p;
        }
    }
    
    private static byte[] toBytes(byte[] array, int targetSize) {
        int counter = 0;
        List<Byte> newArr = new ArrayList<Byte>();
        while (counter < targetSize && (array.length - 1 - counter >= 0)) {
            newArr.add(0, array[array.length - 1 - counter]);
            counter++;
        }
        int size = newArr.size();
        for (int i = 0; i < (targetSize - size); i++) {
            newArr.add(0, (byte) 0);
        }
        byte[] ret = new byte[newArr.size()];
        for (int i = 0; i < newArr.size(); i++) {
            ret[i] = newArr.get(i);
        }
        return ret;
    }
    
    /**
     * 判断是否为合法的用户密码
     * @param password
     * @return
     */
    public static boolean isValidPassword(String password){
        if (password.isEmpty()) return false;
        return getPatternCompile(pwRegEx).matcher(password).matches();
    }
    
    /**
     * 判断是否为合法的用户email
     * @param email
     * @return
     */
    public static boolean isValidEmail(String email){
        if (email.isEmpty()) return false;
        return getPatternCompile(emailRegEx).matcher(email).matches();
    }

    /**
     * 用于获取某个ownerID的可用带宽权限数据域列表
     * @param ownerID
     * @return
     * @throws Exception 
     */
    public static List<String> getABWPermissionRegions(long ownerID)
            throws Exception {
        List<String> usrRegions = new ArrayList<>();
        UserToRoleMeta userToRole = new UserToRoleMeta(ownerID);
        if (client.userToRoleSelect(userToRole)) {
            List<Long> roleIDs = userToRole.getRoleID();
            List<String> region1 = new ArrayList<>();
            List<String> region2 = new ArrayList<>();
            RoleMeta roleMeta;
            for (Long roleID : roleIDs) {
                roleMeta = new RoleMeta(roleID);
                client.roleSelect(roleMeta);
                for(Entry<RoleMeta.RolePermission, List<String>> permission : roleMeta.getRegions().entrySet()) {
                    if (permission.getKey().equals(RoleMeta.RolePermission.PERMISSION_AVAIL_BW)) {
                        region2 = new ArrayList<String>(permission.getValue());
                        break;
                    }
                }
                region2.removeAll(region1);
                region1.addAll(region2);
            }
            return region1;
        }
        return usrRegions;
    }
    
    
    /**
     * 用于获取某个ownerID的有可用带宽权限且有标签的数据域列表
     * @param ownerID
     * @return
     * @throws Exception 
     */
    public static List<String> getABWPermissionAndTagRegions(long ownerID)
            throws Exception {
        List<String> region1 = getABWPermissionRegions(ownerID);
        List<String> usrRegions = new ArrayList<>();
        for (String region : region1) {
            if (client.getDataRegions(ownerID).contains(region)) {
                usrRegions.add(region);
            }
        }
        return usrRegions;
    }
    
    public static List<String> getAvailableDataRegions(long ownerID) 
            throws IOException, BaseException, JDOMException {
        UserToRoleMeta userToRole = new UserToRoleMeta(ownerID);
        client.userToRoleSelect(userToRole);
        List<Long> roleIDs = userToRole.getRoleID();
        List<String> regionAll = new ArrayList<>();
        RoleMeta roleMeta;
        for (Long roleID : roleIDs) {
            roleMeta = new RoleMeta(roleID);
            client.roleSelect(roleMeta);
            for(Entry<RoleMeta.RolePermission, List<String>> permission : roleMeta.getPools().entrySet()) {
                if (permission.getKey().equals(RolePermission.PERMISSION_AVAIL_DATAREGION)) {
                    regionAll.addAll(permission.getValue());
                    break;
                }
            }
        }
        //去重
        List<String> regionDistinct = regionAll.stream().distinct().collect(Collectors.toList());
        List<String> regionSystem   = getScope();
        
        //取交集
        List<String> ownerABWPRegions = regionSystem.stream()
                .filter(a -> regionDistinct.contains(a)).collect(Collectors.toList());
        
        return ownerABWPRegions;
    }
    
    /**
* 从配置文件中获取需要计算带宽95峰值的用户及其regions
     * @return
     * @throws Exception
     */
    public static Map<Long, List<String>> get95PeakBWPermittedOwnerRegions() throws Exception {
        Map<Long, List<String>> ownerRegions = new HashMap<>();
        for (Entry<String, List<String>> ownerRegion : PeriodUsageStatsConfig.peakBWPermitOwnerWithRegions.entrySet()) {
            String ownerName = ownerRegion.getKey();
            OwnerMeta owner = new OwnerMeta(ownerName);
            if(!client.ownerSelect(owner)) //用户不存在
            {
                log.error("95peakBandWidth no such owner " + ownerName);
                continue;
            }
            long ownerId = owner.getId();
            Set<String> regions = new HashSet<String>(ownerRegion.getValue()); //去重
            if (regions.size() != 0) {
                //没有检查owner的region，如果region不正确则查询不到数据
                ownerRegions.put(ownerId, new ArrayList<String>(regions));
            } else {
                log.info("95peakBandWidth owner " + ownerId + " has no permitted regions!");
            }
        }
        return ownerRegions;
    }
    
    /**
     * 此方法用于获取5.0版本的资源池列表信息
     * 根据权限类型来判断可选择范围
     * @param scope
     * 
     * @return
     * @throws IOException 
     * @throws BaseException 
     * @throws JDOMException 
     */
    public static List<String> getScope() throws BaseException, IOException, JDOMException { 
        List<Pair<String, Pool>> poolList = BssAdapterClient.getAvailablePoolsAsList(
                BssAdapterConfig.localPool.ak,BssAdapterConfig.localPool.getSK());
        
        return poolList.parallelStream().map(new Function<Pair<String, Pool>, String>() {
            public String apply(Pair<String, Pool> t) {
                return t.first();
            }
        }).collect(Collectors.toList());
    }
    
    public static void timingLog(String method, String param, long startTime) {
        long time = System.currentTimeMillis() - startTime;
        log.info("Timing Method:" + method + " Time:" + time + " Params:" + param);
    }

    
    public static boolean isPresignedUrl(HttpServletRequest req) {
        return (req.getParameter("Expires") != null || req.getParameter("Signature") != null
                || req.getParameter("AWSAccessKeyId") != null
                || req.getParameter(V4Signer.X_AMZ_SIGNATURE_CAPITAL) != null);
    }

    /**
     * 检查分段的partNum值是否合法
     *
     * @param partNum 范围是[1,10000]
     */
    public static void checkPartNumber(HttpServletRequest req, int partNum) throws BaseException {
        if (partNum > Consts.UPLOAD_PART_MAXNUM || partNum < 1) {
            throw new BaseException(400, "InvalidPartNumber");
        }
    }

    /**
     * 检查listPart时，分段的partNum值是否合法
     *
     * @param partNumberMarker 范围是>=-1(list结果不包含partNumberMarker的分段所以要返回0片段则需要传递-1)
     */
    public static void checkPartNumberMarker(HttpServletRequest req, int partNumberMarker) throws BaseException {
        if (partNumberMarker < -1) {
            throw new BaseException(400, "InvalidPartNumberMarker");
        }
    }    /**
     * 检查统计项查询请求参数合法性
     * @param beginDate
     * @param endDate
     * @throws BaseException
     */
    public static void checkUsageReqParamsValid(String beginDate, String endDate, String freq) throws BaseException {
        // beginDate、endDate参数为必填项
        if (beginDate != null && beginDate.trim().length() != 0
                && endDate != null && endDate.trim().length() != 0)
            ;
        else
            throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_ARGUMENT);
        // beginDate、endDate格式为yyyy-MM-dd
        if (Utils.dayFormatIsValid(endDate) && Utils.dayFormatIsValid(beginDate)) {
            Date tmpEndDate;
            Date tmpBeginDate;
            try {
                tmpEndDate = Misc.formatyyyymmdd(endDate);
                tmpBeginDate = Misc.formatyyyymmdd(beginDate);
                if (tmpEndDate.before(tmpBeginDate))
                    throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_ARGUMENT);
            } catch (ParseException e) {
                throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_ARGUMENT);
            }
        } else {
            throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_ARGUMENT);
        }
        // 若有freq参数，则必须为byDay或by5min或byHour
        if (freq != null && freq.trim().length() != 0 && !freq.equals(Utils.BY_DAY) && !freq.equals(Utils.BY_HOUR) && !freq.equals(Utils.BY_5_MIN))
            throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_ARGUMENT);
    }

    /**
     * 检测前端传递的storageClass参数是否合法，并且返回对应的后端统一常量,默认返回标准存储类型
     *
     * @param storageClass
     * @return
     * @throws BaseException storageClass参数不合法 抛出异常
     */
    public static String checkAndGetStorageType(String storageClass) throws BaseException {
        return checkAndGetStorageType(storageClass, Consts.STORAGE_CLASS_STANDARD);
    }

    /**
     * 检测前端传递的storageClass参数是否合法，并且返回对应的后端统一常量。
     * 可设置默认返回结果
     *
     * @param storageClass
     * @return
     * @throws BaseException storageClass参数不合法 抛出异常
     */
    public static String checkAndGetStorageType(String storageClass, String defaultStorageType) throws BaseException {
        // 前端可选的合法传递参数
        final List<String> validStorageClass = Lists.newArrayList(Consts.ALL, Consts.STORAGE_CLASS_STANDARD, Consts.STORAGE_CLASS_STANDARD_IA);
        // storageType为可选参数，如果不存在则默认值为标准存储
        if (StringUtils.isBlank(storageClass)) {
            return StringUtils.defaultIfBlank(defaultStorageType, Consts.STORAGE_CLASS_STANDARD);
        } else if (validStorageClass.contains(storageClass)) {
            return storageClass;
        }
        throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_ARGUMENT, "Invalid StorageClass.");
    }

    public static List<String> getStorageTypeProcessList(String storageType) {
        List<String> storageTypeProcessList;
        if (Consts.ALL.equals(storageType)) {
            storageTypeProcessList = Lists.newArrayList(Consts.STORAGE_CLASS_STANDARD, Consts.STORAGE_CLASS_STANDARD_IA);
        }else {
            storageTypeProcessList = Lists.newArrayList(storageType);
        }
        return storageTypeProcessList;
    }
    
    public static void getFrozenStatusException(Pair<Boolean,String> pair) throws BaseException{
        String missionStatus = pair.second();
        Boolean isCallUserPermission = pair.first();
        String frozen = "the user has been frozen.";
        switch (missionStatus) {
        case "HANDLED":
            if(null != isCallUserPermission && !isCallUserPermission)
                throw new BaseException(frozen, 403, "AccessDenied", ErrorMessage.ERROR_FROZEN_OVERDUE_HANDLED);
            break;
        case "UNHANDLE":
            throw new BaseException(frozen, 403, "AccessDenied",ErrorMessage.ERROR_DELETE_OBJECT_HANDLING);
        case "HANDLING":
            if(null != isCallUserPermission && isCallUserPermission)
                throw new BaseException(frozen, 403, "AccessDenied", ErrorMessage.ERROR_FROZEN_OVERDUE_NEED_UNFREEZON);
            else
                throw new BaseException(frozen, 403, "AccessDenied",ErrorMessage.ERROR_DELETE_OBJECT_HANDLING);
        case "NORM":
            throw new BaseException(frozen, 403, "AccessDenied", ErrorMessage.ERROR_FROZEN_OVERDUE_NORM);

        default:
            //do nothing
        }
    }

    public static void streamClose(InputStream... iss) {
        for(InputStream is : iss) {
            try {
                if(is != null)
                    is.close();
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }
    }

    public static void streamClose(OutputStream... oss) {
        for(OutputStream os : oss) {
            try {
                if(os != null)
                    os.close();
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }
    }
    
}
