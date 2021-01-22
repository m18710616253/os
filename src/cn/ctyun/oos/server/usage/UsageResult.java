package cn.ctyun.oos.server.usage;

import java.io.IOException;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.text.ParseException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jdom.JDOMException;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.amazonaws.services.s3.internal.XmlWriter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import cn.ctyun.common.BaseException;
import cn.ctyun.common.Consts;
import cn.ctyun.common.conf.DataRegion;
import cn.ctyun.common.region.DataRegions;
import cn.ctyun.oos.common.AuthResult;
import cn.ctyun.oos.common.ErrorMessage;
import cn.ctyun.oos.common.Utils;
import cn.ctyun.oos.hbase.MetaClient;
import cn.ctyun.oos.metadata.BucketMeta;
import cn.ctyun.oos.metadata.MinutesUsageMeta;
import cn.ctyun.oos.metadata.MinutesUsageMeta.BandwidthMeta;
import cn.ctyun.oos.metadata.MinutesUsageMeta.CodeRequestStats;
import cn.ctyun.oos.metadata.MinutesUsageMeta.Connection;
import cn.ctyun.oos.metadata.MinutesUsageMeta.PeakBandWidth;
import cn.ctyun.oos.metadata.MinutesUsageMeta.SizeStats;
import cn.ctyun.oos.metadata.MinutesUsageMeta.UsageMetaType;
import cn.ctyun.oos.metadata.OwnerMeta;
import cn.ctyun.oos.server.conf.PeriodUsageStatsConfig;
import cn.ctyun.oos.server.management.Common;
import cn.ctyun.oos.server.util.CSVUtils;
import cn.ctyun.oos.server.util.CSVUtils.UsageType;
import cn.ctyun.oos.server.util.Misc;
import common.tuple.Pair;

public class UsageResult {
    private static Log log = LogFactory.getLog(UsageResult.class);
    private static MetaClient client = MetaClient.getGlobalClient();

    public static String getCapacityInXML(AuthResult authResult, String beginDate, String endDate, HttpServletRequest req)
            throws Exception {
        OwnerMeta owner = authResult.owner;
        // 解析请求参数
        List<String> regionList = new ArrayList<String>();
        String bucket = req.getParameter("Bucket");
        String region = req.getParameter("Region");
        String freq = req.getParameter("Freq");
        String storageClassInput = req.getParameter("StorageClass");
        String storageType = Utils.checkAndGetStorageType(storageClassInput);
        // Freq默认为byHour
        if (freq == null || freq.length() == 0) {
            freq = Utils.BY_HOUR;
        } else {
            if (!freq.equals(Utils.BY_5_MIN) && !freq.equals(Utils.BY_HOUR) && !freq.equals(Utils.BY_DAY)) {
                throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_ARGUMENT, "Invalid Freq.");
            }
        }
        checkBucketExist(owner.getId(), bucket);
           
        // 获取数据域
        getRegionList(owner.getId(), regionList, region);
        // 检查时间是否符合要求
        checkValidDateByDay(beginDate, endDate, freq);
        // 获取用户用量
        // Map<time, Map<storageType, MinutesUsageMeta>> 按time,storageType分类,方便后续生成xml
        Map<String, Map<String, MinutesUsageMeta>> bucketRegionUsageByTime = makeUserBucketUsageByTime(owner.getId(), beginDate, endDate, bucket, regionList, freq, storageType);

        // 构建xml
        XmlWriter xml = new XmlWriter();
        xml.start("GetCapacityResponse");
        xml.start("Account").value(owner.getName()).end();
        if (authResult.isRoot()) {
            xml.start("UserName").value("root").end();
        } else {
            xml.start("UserName").value(authResult.accessKey.userName).end();
        }
        xml.start("StorageClass").value(StringUtils.defaultIfBlank(storageClassInput, Consts.STORAGE_CLASS_STANDARD)).end();
        xml.start("TimeZone").value("UTC +0800").end();
        xml.start("Freq").value(freq).end();
        xml.start("BucketName").value(bucket).end();
        xml.start("RegionName").value(region).end();
        buildCapacityXML(xml, bucketRegionUsageByTime, region, freq);
        xml.end();
        String str = Consts.XML_HEADER + xml.toString();
        str = str.replaceAll("&quote;", "&quot;");
        return str;
    }
    
    public static String getDeleteCapacityInXML(AuthResult authResult, String beginDate, String endDate, HttpServletRequest req)
            throws Exception {
        OwnerMeta owner = authResult.owner;
        // 解析请求参数
        List<String> regionList = new ArrayList<String>();
        String bucket = req.getParameter("Bucket");
        String region = req.getParameter("Region");
        String freq = req.getParameter("Freq");
        String storageClassInput = req.getParameter("StorageClass");
        String storageType = Utils.checkAndGetStorageType(storageClassInput);
        // Freq默认为byHour
        if (freq == null || freq.length() == 0) {
            freq = Utils.BY_HOUR;
        } else {
            if (!freq.equals(Utils.BY_5_MIN) && !freq.equals(Utils.BY_HOUR) && !freq.equals(Utils.BY_DAY)) {
                throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_ARGUMENT, "Invalid Freq.");
            }
        }
        checkBucketExist(owner.getId(), bucket);
        // 获取数据域
        getRegionList(owner.getId(), regionList, region);
        // 检查时间是否符合要求
        checkValidDateByDay(beginDate, endDate, freq);
        // 获取用户用量
        // Map<time, Map<storageType, MinutesUsageMeta>> 按time,storageType分类,方便后续生成xml
        Map<String, Map<String, MinutesUsageMeta>> bucketRegionUsageByTime = makeUserBucketUsageByTime(owner.getId(), beginDate, endDate, bucket, regionList, freq, storageType);

        // 构建xml
        XmlWriter xml = new XmlWriter();
        xml.start("GetDeleteCapacityResponse");
        xml.start("Account").value(owner.getName()).end();
        if (authResult.isRoot()) {
            xml.start("UserName").value("root").end();
        } else {
            xml.start("UserName").value(authResult.accessKey.userName).end();
        }
        xml.start("StorageClass").value(StringUtils.defaultString(storageClassInput, Consts.STORAGE_CLASS_STANDARD)).end();
        xml.start("TimeZone").value("UTC +0800").end();
        xml.start("Freq").value(freq).end();
        xml.start("BucketName").value(bucket).end();
        xml.start("RegionName").value(region).end();
        buildDeleteCapacityXML(xml, bucketRegionUsageByTime);
        xml.end();
        String str = Consts.XML_HEADER + xml.toString();
        str = str.replaceAll("&quote;", "&quot;");
        return str;
    }
    
    public static String getTrafficsInXML(AuthResult authResult, String beginDate, String endDate, HttpServletRequest req)
            throws Exception {
        OwnerMeta owner = authResult.owner;
        // 解析请求参数
        List<String> regionList = new ArrayList<String>();
        String bucket = req.getParameter("Bucket");
        String region = req.getParameter("Region");
        String freq = req.getParameter("Freq");
        String storageClassInput = req.getParameter("StorageClass");
        String storageType = Utils.checkAndGetStorageType(storageClassInput);
        // 默认为按小时粒度
        if (freq == null || freq.length() == 0) {
            freq = Utils.BY_HOUR;
        } else {
            if (!freq.equals(Utils.BY_5_MIN) && !freq.equals(Utils.BY_HOUR) && !freq.equals(Utils.BY_DAY)) {
                throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_ARGUMENT, "Invalid Freq.");
            }
        }
        checkBucketExist(owner.getId(), bucket);
        getRegionList(owner.getId(), regionList, region);
        checkValidDateByDay(beginDate, endDate, freq);
        
        String inOutType = req.getParameter("InOutType");
        String internetType = req.getParameter("InternetType");
        String trafficsType = req.getParameter("TrafficsType");
        if (inOutType != null && inOutType.length() != 0) {
            if (!inOutType.equals("all") && !inOutType.equals("inbound") && !inOutType.equals("outbound"))
                throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_ARGUMENT, "Invalid InOutType.");
        } else {
            inOutType = "all";
        }
        if (internetType != null && internetType.length() != 0) {
            if (!internetType.equals("all") && !internetType.equals("internet") && !internetType.equals("noninternet"))
                throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_ARGUMENT, "Invalid InternetType.");
        } else {
            internetType = "all";
        }
        if (trafficsType != null && trafficsType.length() != 0) {
            if (!trafficsType.equals("all") && !trafficsType.equals("direct") && !trafficsType.equals("roam"))
                throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_ARGUMENT, "Invalid TrafficsType.");
        } else {
            trafficsType = "all";
        }
        // 获取用户用量
        // Map<time, Map<storageType, MinutesUsageMeta>> 按time,storageType分类,方便后续生成xml
        Map<String, Map<String, MinutesUsageMeta>> bucketRegionUsageByTime = makeUserBucketUsageByTime(owner.getId(), beginDate, endDate, bucket, regionList, freq, storageType);

        // 构建xml
        XmlWriter xml = new XmlWriter();
        xml.start("GetTrafficsResponse");
        xml.start("Account").value(owner.getName()).end();
        if (authResult.isRoot()) {
            xml.start("UserName").value("root").end();
        } else {
            xml.start("UserName").value(authResult.accessKey.userName).end();
        }
        xml.start("StorageClass").value(StringUtils.defaultString(storageClassInput, Consts.STORAGE_CLASS_STANDARD)).end();
        xml.start("TimeZone").value("UTC +0800").end();
        xml.start("InOutType").value(inOutType).end();
        xml.start("InternetType").value(internetType).end();
        xml.start("TrafficsType").value(trafficsType).end();
        xml.start("Freq").value(freq).end();
        xml.start("BucketName").value(bucket).end();
        xml.start("RegionName").value(region).end();
        buildTrafficsXML(xml, bucketRegionUsageByTime, inOutType, internetType, trafficsType);
        xml.end();
        String str = Consts.XML_HEADER + xml.toString();
        str = str.replaceAll("&quote;", "&quot;");
        return str;
    }

    public static String getRequestInXML(AuthResult authResult, String beginDate, String endDate, HttpServletRequest req) throws Exception {
        OwnerMeta owner = authResult.owner;
        //解析请求参数
        List<String> regionList = new ArrayList<String>();
        String bucket = req.getParameter("Bucket");
        String region = req.getParameter("Region");
        String storageClassInput = req.getParameter("StorageClass");
        String storageType = Utils.checkAndGetStorageType(storageClassInput);
        String freq = req.getParameter("Freq");
        if (freq == null || freq.length() == 0) {
            freq = Utils.BY_HOUR;
        } else {
            if (!freq.equals(Utils.BY_5_MIN) && !freq.equals(Utils.BY_HOUR) && !freq.equals(Utils.BY_DAY)) {
                throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_ARGUMENT, "Invalid Freq.");
            }
        }
        checkBucketExist(owner.getId(), bucket);
        getRegionList(owner.getId(), regionList, region);
        checkValidDateByDay(beginDate, endDate, freq);
        
        String internetType = req.getParameter("InternetType");
        String requestsType = req.getParameter("RequestsType");
        if (internetType == null || internetType.length() == 0) {
            internetType = "all";
        } else {
            if (!internetType.equals("all") && !internetType.equals("internet") && !internetType.equals("noninternet")) {
                throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_ARGUMENT, "Invalid InternetType.");
            }
        }
        if (requestsType == null || requestsType.length() == 0) {
            requestsType = "all";
        } else {
            if (!requestsType.equals("all") && !requestsType.equals("get") && !requestsType.equals("head") && !requestsType.equals("put") 
                    && !requestsType.equals("post") && !requestsType.equals("delete") && !requestsType.equals("others")) {
                throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_ARGUMENT, "Invalid RequestsType.");
            }
        }
        // 获取用户用量
        // Map<time, Map<storageType, MinutesUsageMeta>> 按time,storageType分类,方便后续生成xml
        Map<String, Map<String, MinutesUsageMeta>> bucketRegionUsageByTime = makeUserBucketUsageByTime(owner.getId(), beginDate, endDate, bucket, regionList, freq, storageType);

        //构建xml
        XmlWriter xml = new XmlWriter();
        xml.start("GetRequestsResponse");
        xml.start("Account").value(owner.getName()).end();
        if (authResult.isRoot()) {
            xml.start("UserName").value("root").end();
        } else {
            xml.start("UserName").value(authResult.accessKey.userName).end();
        }
        xml.start("StorageClass").value(StringUtils.defaultString(storageClassInput, Consts.STORAGE_CLASS_STANDARD)).end();
        xml.start("TimeZone").value("UTC +0800").end();
        xml.start("InternetType").value(internetType).end();
        xml.start("RequestsType").value(requestsType).end();
        xml.start("Freq").value(freq).end();
        xml.start("BucketName").value(bucket).end();
        xml.start("RegionName").value(region).end();
        buildRequestsXML(xml, bucketRegionUsageByTime, internetType, requestsType);
        xml.end();
        String str = Consts.XML_HEADER + xml.toString();
        str = str.replaceAll("&quote;", "&quot;");
        return str;
    }

    public static String getReturnCodeInXML(AuthResult authResult, String beginDate, String endDate, HttpServletRequest req) throws Exception {
        OwnerMeta owner = authResult.owner;
        //解析请求参数
        List<String> regionList = new ArrayList<String>();
        String bucket = req.getParameter("Bucket");
        String region = req.getParameter("Region");
        String storageClassInput = req.getParameter("StorageClass");
        String storageType = Utils.checkAndGetStorageType(storageClassInput);
        String freq = req.getParameter("Freq");
        if (freq == null || freq.length() == 0) {
            freq = Utils.BY_HOUR;
        } else {
            if (!freq.equals(Utils.BY_5_MIN) && !freq.equals(Utils.BY_HOUR) && !freq.equals(Utils.BY_DAY)) {
                throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_ARGUMENT, "Invalid Freq.");
            }
        }
        checkBucketExist(owner.getId(), bucket);
        getRegionList(owner.getId(), regionList, region);
        checkValidDateByDay(beginDate, endDate, freq);
        
        String internetType = req.getParameter("InternetType");
        String requestsType = req.getParameter("RequestsType");
        String responseType = req.getParameter("ResponseType");
        
        if (internetType == null || internetType.length() == 0) {
            internetType = "all";
        } else {
            if (!internetType.equals("all") && !internetType.equals("internet") && !internetType.equals("noninternet")) {
                throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_ARGUMENT, "Invalid InternetType.");
            }
        }
        if (requestsType == null || requestsType.length() == 0) {
            requestsType = "all";
        }
        if (responseType == null || responseType.length() == 0) {
            responseType = "all";
        }
        
        List<String> requestsList = new ArrayList<String>();
        if (requestsType.equals("all")) {
            requestsList.add("get");
            requestsList.add("head");
            requestsList.add("put");
            requestsList.add("post");
            requestsList.add("delete");
            requestsList.add("other");
        } else {
            for (String resType : requestsType.split("%2B|\\+")) {
                if(resType.equals("get")||resType.equals("head")
                        ||resType.equals("put")||resType.equals("delete")
                        ||resType.equals("post")||resType.equals("others")) {
                    //要求返回是others
                    if (resType.equals("others")) {
                        requestsList.add("other");
                    } else {
                        requestsList.add(resType);
                    }
                } else {
                    throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_ARGUMENT, "Invalid RequestsType.");
                }
            }
        }
        
        List<String> responseTypes = new ArrayList<String>();
        if (responseType.equals("all")) {
            responseTypes.add("Response200");
            responseTypes.add("Response204");
            responseTypes.add("Response206");
            responseTypes.add("Response403");
            responseTypes.add("Response404");
            responseTypes.add("Response4xx");
            responseTypes.add("Response500");
            responseTypes.add("Response503");
        } else {
            for (String resType : responseType.split("%2B|\\+")) {
                if (resType.equals("Response200") || resType.equals("Response204") || resType.equals("Response206") 
                        || resType.equals("Response403") || resType.equals("Response404") || resType.equals("Response4xx")
                        || resType.equals("Response500") || resType.equals("Response503")) {
                    responseTypes.add(resType);
                } else {
                    throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_ARGUMENT, "Invalid ResponseType.");
                }
            }  
        }
        // 获取用户用量
        // Map<time, Map<storageType, MinutesUsageMeta>> 按time,storageType分类,方便后续生成xml
        Map<String, Map<String, MinutesUsageMeta>> bucketRegionUsageByTime = makeUserBucketUsageByTime(owner.getId(), beginDate, endDate, bucket, regionList, freq, storageType);

        //构建xml
        XmlWriter xml = new XmlWriter();
        xml.start("GetReturnCodeResponse");
        xml.start("Account").value(owner.getName()).end();
        if (authResult.isRoot()) {
            xml.start("UserName").value("root").end();
        } else {
            xml.start("UserName").value(authResult.accessKey.userName).end();
        }
        xml.start("StorageClass").value(StringUtils.defaultString(storageClassInput, Consts.STORAGE_CLASS_STANDARD)).end();
        xml.start("TimeZone").value("UTC +0800").end();
        xml.start("InternetType").value(internetType).end();
        xml.start("RequestsType").value(requestsType.replace("%2B", "+")).end();
        xml.start("ResponseType").value(responseType.replace("%2B", "+")).end();
        xml.start("Freq").value(freq).end();
        xml.start("BucketName").value(bucket).end();
        xml.start("RegionName").value(region).end();
        buildReturnCodeXML(xml, bucketRegionUsageByTime, internetType, requestsList, responseTypes);
        xml.end();
        String str = Consts.XML_HEADER + xml.toString();
        str = str.replaceAll("&quote;", "&quot;");
        return str;
    }

    public static String getConcurrentConnectionInXML(AuthResult authResult, String beginDate, String endDate, HttpServletRequest req) throws Exception {
        OwnerMeta owner = authResult.owner;
        //解析请求参数
        List<String> regionList = new ArrayList<String>();
        String bucket = req.getParameter("Bucket");
        String region = req.getParameter("Region");
        String freq = req.getParameter("Freq");
        if (freq == null) {
            freq = Utils.BY_5_MIN;
        } else if (!freq.equals(Utils.BY_5_MIN)) {
            throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_ARGUMENT, "Invalid Freq.");
        }
        checkBucketExist(owner.getId(), bucket);
        getRegionList(owner.getId(), regionList, region);
        checkValidDateByDay(beginDate, endDate, freq);
        
        String internetType = req.getParameter("InternetType");
        if (internetType == null || internetType.length() == 0) {
            internetType = "all";
        } else {
            if (!internetType.equals("all") && !internetType.equals("internet") && !internetType.equals("noninternet")) {
                throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_ARGUMENT, "Invalid InternetType.");
            }
        }
        //获取用户用量
        Map<String, MinutesUsageMeta> bucketRegionUsage = makeUserBucketUsageByTime(owner.getId(), beginDate, endDate, bucket, regionList, freq);

        //构建xml
        XmlWriter xml = new XmlWriter();
        xml.start("GetConcurrentConnectionResponse");
        xml.start("Account").value(owner.getName()).end();
        if (authResult.isRoot()) {
            xml.start("UserName").value("root").end();
        } else {
            xml.start("UserName").value(authResult.accessKey.userName).end();
        }  
        xml.start("TimeZone").value("UTC +0800").end();
        xml.start("InternetType").value(internetType).end();
        xml.start("Freq").value(freq).end();
        xml.start("BucketName").value(bucket).end();
        xml.start("RegionName").value(region).end();
        buildConcurrentConnectionXML(xml, bucketRegionUsage, internetType);
        xml.end();
        String str = Consts.XML_HEADER + xml.toString();
        str = str.replaceAll("&quote;", "&quot;");
        return str;
    }

    public static String getAvailableBandwidthInXML(AuthResult authResult, String beginDate, String endDate, HttpServletRequest req) throws Exception {
        OwnerMeta owner = authResult.owner;
        //解析请求参数
        List<String> regionList = new ArrayList<String>();
        String region = req.getParameter("Region");
        String freq = req.getParameter("Freq");
        if (freq == null) {
            freq = Utils.BY_5_MIN;
        } else if (!freq.equals(Utils.BY_5_MIN)) {
            throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_ARGUMENT, "Invalid Freq.");
        }
        
        //获取用户可用带宽权限分配且用户有标签的region
        List<String> dataRegions = Utils.getABWPermissionAndTagRegions(owner.getId());
        if (region == null || region.length() == 0) {
            regionList.addAll(dataRegions);
        } else {
            if (region.contains("%2B") || region.contains("+")) {
                throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_ARGUMENT, "Invalid Region.");
            } else if (dataRegions.contains(region)) {
                regionList.add(region);
            } else {
                throw new BaseException(403, ErrorMessage.ERROR_CODE_403);
            } 
        }
        
        checkValidDateByDay(beginDate, endDate, freq);
        
        String inOutType = req.getParameter("InOutType");
        String internetType = req.getParameter("InternetType");
        if (inOutType == null || inOutType.length() == 0) {
            inOutType = "all";
        } else {
            if (!inOutType.equals("all") && !inOutType.equals("inbound") && !inOutType.equals("outbound")) {
                throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_ARGUMENT, "Invalid InOutType.");
            }
        }
        if (internetType == null || internetType.length() == 0) {
            internetType = "all";
        } else {
            if (!internetType.equals("all") && !internetType.equals("internet") && !internetType.equals("noninternet")) {
                throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_ARGUMENT, "Invalid InternetType.");
            }
        }
        //获取可用带宽用量
        Map<String, List<MinutesUsageMeta>> availBandwidthUsage = makeAvailableBandwidthByTime(owner.getId(), beginDate, endDate, regionList);
        //构建xml
        XmlWriter xml = new XmlWriter();
        xml.start("GetAvailableBandwidthResponse");
        xml.start("Account").value(owner.getName()).end();
        if (authResult.isRoot()) {
            xml.start("UserName").value("root").end();
        } else {
            xml.start("UserName").value(authResult.accessKey.userName).end();
        }  
        xml.start("TimeZone").value("UTC +0800").end();
        xml.start("InOutType").value(inOutType).end();
        xml.start("InternetType").value(internetType).end();
        xml.start("Freq").value(freq).end();
        buildAvailableBandwidthXML(xml, availBandwidthUsage, inOutType, internetType, region);
        xml.end();
        String str = Consts.XML_HEADER + xml.toString();
        str = str.replaceAll("&quote;", "&quot;");
        return str;
    }

    public static String getBilledStorageUsageInXml(AuthResult authResult, String beginDate, String endDate, HttpServletRequest req) throws Exception {
        OwnerMeta owner = authResult.owner;
        //解析请求参数
        List<String> regionList = new ArrayList<>();
        String bucket = req.getParameter("Bucket");
        String region = req.getParameter("Region");
        String storageClassInput = req.getParameter("StorageClass");
        String storageType = Utils.checkAndGetStorageType(storageClassInput, Consts.ALL);
        String freq = req.getParameter("Freq");
        if (StringUtils.isBlank(freq)) {
            freq = Utils.BY_HOUR;
        } else if (!Utils.BY_HOUR.equals(freq) && !Utils.BY_DAY.equals(freq)) {
            throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_ARGUMENT, "Invalid Freq.");
        }
        checkBucketExist(owner.getId(), bucket);
        getRegionList(owner.getId(), regionList, region);
        checkValidDateByDay(beginDate, endDate, freq);

        // 获取用户用量
        // Map<time, Map<storageType, MinutesUsageMeta>> 按time,storageType分类,方便后续生成xml
        Map<String, Map<String, MinutesUsageMeta>> bucketRegionUsageByTime = makeUserBucketUsageByTime(owner.getId(), beginDate, endDate, bucket, regionList, freq, storageType);

        //构建xml
        XmlWriter xml = new XmlWriter();
        xml.start("GetBilledStorageUsageResponse");
        xml.start("Account").value(owner.getName()).end();
        if (authResult.isRoot()) {
            xml.start("UserName").value("root").end();
        } else {
            xml.start("UserName").value(authResult.accessKey.userName).end();
        }
        xml.start("StorageClass").value(StringUtils.defaultString(storageClassInput, Consts.ALL)).end();
        xml.start("TimeZone").value("UTC +0800").end();
        xml.start("Freq").value(freq).end();
        xml.start("BucketName").value(bucket).end();
        xml.start("RegionName").value(region).end();
        buildBilledStorageUsageXML(xml, bucketRegionUsageByTime);
        xml.end();
        String str = Consts.XML_HEADER + xml.toString();
        str = str.replaceAll("&quote;", "&quot;");
        return str;
    }

    public static String getRestoreCapacityInXml(AuthResult authResult, String beginDate, String endDate, HttpServletRequest req) throws Exception{
        OwnerMeta owner = authResult.owner;
        //解析请求参数
        List<String> regionList = new ArrayList<>();
        String bucket = req.getParameter("Bucket");
        String region = req.getParameter("Region");
        String storageClassInput = req.getParameter("StorageClass");
        String storageType = Utils.checkAndGetStorageType(storageClassInput, Consts.STORAGE_CLASS_STANDARD_IA);
        if (Consts.STORAGE_CLASS_STANDARD.equals(storageType)) {
            throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_ARGUMENT, "Invalid StorageClass.");
        }
        String freq = req.getParameter("Freq");
        if (StringUtils.isBlank(freq)) {
            freq = Utils.BY_HOUR;
        } else if (!Utils.BY_5_MIN.equals(freq) && !Utils.BY_HOUR.equals(freq) && !Utils.BY_DAY.equals(freq)) {
            throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_ARGUMENT, "Invalid Freq.");
        }
        checkBucketExist(owner.getId(), bucket);
        getRegionList(owner.getId(), regionList, region);
        checkValidDateByDay(beginDate, endDate, freq);

        // 获取用户用量
        // Map<time, Map<storageType, MinutesUsageMeta>> 按time,storageType分类,方便后续生成xml
        Map<String, Map<String, MinutesUsageMeta>> bucketRegionUsageByTime = makeUserBucketUsageByTime(owner.getId(), beginDate, endDate, bucket, regionList, freq, storageType);

        //构建xml
        XmlWriter xml = new XmlWriter();
        xml.start("GetRestoreCapacityResponse");
        xml.start("Account").value(owner.getName()).end();
        if (authResult.isRoot()) {
            xml.start("UserName").value("root").end();
        } else {
            xml.start("UserName").value(authResult.accessKey.userName).end();
        }
        xml.start("StorageClass").value(StringUtils.defaultString(storageClassInput, Consts.ALL)).end();
        xml.start("TimeZone").value("UTC +0800").end();
        xml.start("Freq").value(freq).end();
        xml.start("BucketName").value(bucket).end();
        xml.start("RegionName").value(region).end();
        buildRestoreCapacityXml(xml, bucketRegionUsageByTime);
        xml.end();
        String str = Consts.XML_HEADER + xml.toString();
        str = str.replaceAll("&quote;", "&quot;");
        return str;
    }

    private static void buildCapacityXML(XmlWriter xml, Map<String, Map<String, MinutesUsageMeta>> bucketRegionUsageByTime, String regionInReq, String freq) {
        for (Entry<String, Map<String, MinutesUsageMeta>> entry : bucketRegionUsageByTime.entrySet()) {
            xml.start("Statistics");
            xml.start("Date").value(entry.getKey()).end();
            // storageType, MinutesUsageMeta
            Map<String, MinutesUsageMeta> metaMap = entry.getValue();
            metaMap.forEach((storageType, meta) -> {
                xml.start(convertStorageTypeXmlLabel(storageType));
                if (freq.equals(Utils.BY_5_MIN)) {
                    xml.start("MaxCapacity").value("").end();
                    xml.start("AverageCapacity").value("").end();
                } else {
                    // 当按天或按小时查询时，如果是global级别，不要峰值容量，因为各地域的峰值不能直接叠加，如以后有了global级别的统计数据，可修改
                    if ((freq.equals(Utils.BY_DAY) || freq.equals(Utils.BY_HOUR)) && (regionInReq == null || regionInReq.length() == 0)) {
                        xml.start("MaxCapacity").value("").end();
                    } else {
                        xml.start("MaxCapacity").value(String.valueOf(meta.sizeStats.size_peak)).end();
                    }
                    xml.start("AverageCapacity").value(String.valueOf(meta.sizeStats.size_avgTotal)).end();
                }
                xml.start("SampleCapacity").value(String.valueOf(meta.sizeStats.size_total)).end();
                xml.end();
            });
            xml.end();
        }
    }
    
    private static void buildDeleteCapacityXML(XmlWriter xml, Map<String, Map<String, MinutesUsageMeta>> bucketRegionUsageByTime) {
        for (Entry<String, Map<String, MinutesUsageMeta>> entry : bucketRegionUsageByTime.entrySet()) {
            xml.start("Statistics");
            xml.start("Date").value(entry.getKey()).end();
            Map<String, MinutesUsageMeta> metaMap = entry.getValue();
            metaMap.forEach((storageType, meta) -> {
                xml.start(convertStorageTypeXmlLabel(storageType));
                xml.start("DeleteCapacity").value(String.valueOf(meta.flowStats.flow_delete + meta.flowStats.flow_noNetDelete)).end();
                xml.end();
            });
            xml.end();
        }
    }

    private static void buildTrafficsXML(XmlWriter xml, Map<String, Map<String, MinutesUsageMeta>> bucketRegionUsageByTime, String inOutType, String internetType, String trafficsType) {
        for (Entry<String, Map<String, MinutesUsageMeta>> entry : bucketRegionUsageByTime.entrySet()) {
            Map<String, MinutesUsageMeta> metaMap = entry.getValue();
            xml.start("Statistics");
            xml.start("Date").value(entry.getKey()).end();
            metaMap.forEach((storageType, meta) -> {
                xml.start(convertStorageTypeXmlLabel(storageType));
                if (!(inOutType.equalsIgnoreCase("outbound") || internetType.equalsIgnoreCase("noninternet") || trafficsType.equalsIgnoreCase("roam"))) {
                    xml.start("InternetDirectInbound").value(String.valueOf(meta.flowStats.flow_upload)).end();
                }
                if (!(inOutType.equalsIgnoreCase("outbound") || internetType.equalsIgnoreCase("noninternet") || trafficsType.equalsIgnoreCase("direct"))) {
                    xml.start("InternetRoamInbound").value(String.valueOf(meta.flowStats.flow_roamUpload)).end();
                }
                if (!(inOutType.equalsIgnoreCase("outbound") || internetType.equalsIgnoreCase("internet") || trafficsType.equalsIgnoreCase("roam"))) {
                    xml.start("NonInternetDirectInbound").value(String.valueOf(meta.flowStats.flow_noNetUpload)).end();
                }
                if (!(inOutType.equalsIgnoreCase("outbound") || internetType.equalsIgnoreCase("internet") || trafficsType.equalsIgnoreCase("direct"))) {
                    xml.start("NonInternetRoamInbound").value(String.valueOf(meta.flowStats.flow_noNetRoamUpload)).end();
                }
                if (!(inOutType.equalsIgnoreCase("inbound") || internetType.equalsIgnoreCase("noninternet") || trafficsType.equalsIgnoreCase("roam"))) {
                    xml.start("InternetDirectOutbound").value(String.valueOf(meta.flowStats.flow_download)).end();
                }
                if (!(inOutType.equalsIgnoreCase("inbound") || internetType.equalsIgnoreCase("noninternet") || trafficsType.equalsIgnoreCase("direct"))) {
                    xml.start("InternetRoamOutbound").value(String.valueOf(meta.flowStats.flow_roamDownload)).end();
                }
                if (!(inOutType.equalsIgnoreCase("inbound") || internetType.equalsIgnoreCase("internet") || trafficsType.equalsIgnoreCase("roam"))) {
                    xml.start("NonInternetDirectOutbound").value(String.valueOf(meta.flowStats.flow_noNetDownload)).end();
                }
                if (!(inOutType.equalsIgnoreCase("inbound") || internetType.equalsIgnoreCase("internet") || trafficsType.equalsIgnoreCase("direct"))) {
                    xml.start("NonInternetRoamOutbound").value(String.valueOf(meta.flowStats.flow_noNetRoamDownload)).end();
                }
                xml.end();
            });
            xml.end();
        }
    }

    private static void buildRequestsXML(XmlWriter xml, Map<String, Map<String, MinutesUsageMeta>> bucketRegionUsageByTime, String internetType, String requestsType) {
        for (Entry<String, Map<String, MinutesUsageMeta>> entry : bucketRegionUsageByTime.entrySet()) {
            Map<String, MinutesUsageMeta> metaMap = entry.getValue();
            xml.start("Statistics");
            xml.start("Date").value(entry.getKey()).end();
            BigDecimal[] requests = new BigDecimal[]{BigDecimal.ZERO};
            metaMap.forEach((storageType, meta) -> {
                xml.start(convertStorageTypeXmlLabel(storageType));
                if (!internetType.equalsIgnoreCase("noninternet")) {
                    xml.start("Internet");
                    if (requestsType.equalsIgnoreCase("all") || requestsType.equalsIgnoreCase("get")) {
                        xml.start("GetRequest").value(String.valueOf(meta.requestStats.req_get)).end();
                        requests[0] = requests[0].add(BigDecimal.valueOf(meta.requestStats.req_get));
                    }
                    if (requestsType.equalsIgnoreCase("all") || requestsType.equalsIgnoreCase("head")) {
                        xml.start("HeadRequest").value(String.valueOf(meta.requestStats.req_head)).end();
                        requests[0] = requests[0].add(BigDecimal.valueOf( meta.requestStats.req_head));

                    }
                    if (requestsType.equalsIgnoreCase("all") || requestsType.equalsIgnoreCase("put")) {
                        xml.start("PutRequest").value(String.valueOf(meta.requestStats.req_put)).end();
                        requests[0] = requests[0].add(BigDecimal.valueOf( meta.requestStats.req_put));
                    }
                    if (requestsType.equalsIgnoreCase("all") || requestsType.equalsIgnoreCase("post")) {
                        xml.start("PostRequest").value(String.valueOf(meta.requestStats.req_post)).end();
                        requests[0] = requests[0].add(BigDecimal.valueOf( meta.requestStats.req_post));
                    }
                    if (requestsType.equalsIgnoreCase("all") || requestsType.equalsIgnoreCase("delete")) {
                        xml.start("DeleteRequest").value(String.valueOf(meta.requestStats.req_delete)).end();
                        requests[0] = requests[0].add(BigDecimal.valueOf( meta.requestStats.req_delete));
                    }
                    if (requestsType.equalsIgnoreCase("all") || requestsType.equalsIgnoreCase("others")) {
                        xml.start("OthersRequest").value(String.valueOf(meta.requestStats.req_other)).end();
                        requests[0] = requests[0].add(BigDecimal.valueOf( meta.requestStats.req_other));
                    }
                    xml.end();
                }
                if (!internetType.equalsIgnoreCase("internet")) {
                    xml.start("NonInternet");
                    if (requestsType.equalsIgnoreCase("all") || requestsType.equalsIgnoreCase("get")) {
                        xml.start("GetRequest").value(String.valueOf(meta.requestStats.req_noNetGet)).end();
                        requests[0] = requests[0].add(BigDecimal.valueOf( meta.requestStats.req_noNetGet));
                    }
                    if (requestsType.equalsIgnoreCase("all") || requestsType.equalsIgnoreCase("head")) {
                        xml.start("HeadRequest").value(String.valueOf(meta.requestStats.req_noNetHead)).end();
                        requests[0] = requests[0].add(BigDecimal.valueOf( meta.requestStats.req_noNetHead));
                    }
                    if (requestsType.equalsIgnoreCase("all") || requestsType.equalsIgnoreCase("put")) {
                        xml.start("PutRequest").value(String.valueOf(meta.requestStats.req_noNetPut)).end();
                        requests[0] = requests[0].add(BigDecimal.valueOf( meta.requestStats.req_noNetPut));
                    }
                    if (requestsType.equalsIgnoreCase("all") || requestsType.equalsIgnoreCase("post")) {
                        xml.start("PostRequest").value(String.valueOf(meta.requestStats.req_noNetPost)).end();
                        requests[0] = requests[0].add(BigDecimal.valueOf( meta.requestStats.req_noNetPost));
                    }
                    if (requestsType.equalsIgnoreCase("all") || requestsType.equalsIgnoreCase("delete")) {
                        xml.start("DeleteRequest").value(String.valueOf(meta.requestStats.req_noNetDelete)).end();
                        requests[0] = requests[0].add(BigDecimal.valueOf( meta.requestStats.req_noNetDelete));
                    }
                    if (requestsType.equalsIgnoreCase("all") || requestsType.equalsIgnoreCase("others")) {
                        xml.start("OthersRequest").value(String.valueOf(meta.requestStats.req_noNetOther)).end();
                        requests[0] = requests[0].add(BigDecimal.valueOf( meta.requestStats.req_noNetOther));
                    }
                    xml.end();
                }
                xml.end();
            });
            xml.start("Requests").value(String.valueOf(requests[0])).end();
            xml.end();
        }
    }
    

    private static void buildReturnCodeXML(XmlWriter xml, Map<String, Map<String, MinutesUsageMeta>> bucketRegionUsageByTime, String internetType, List<String> requestsTypes, List<String> responsTypes) throws Exception {
        for (Entry<String, Map<String, MinutesUsageMeta>> entry : bucketRegionUsageByTime.entrySet()) {
            Map<String, MinutesUsageMeta> metaMap = entry.getValue();
            xml.start("Statistics");
            xml.start("Date").value(entry.getKey()).end();
            for (Entry<String, MinutesUsageMeta> e : metaMap.entrySet()) {
                String storageType = e.getKey();
                MinutesUsageMeta meta = e.getValue();
                xml.start(convertStorageTypeXmlLabel(storageType));
                Field field = meta.getClass().getDeclaredField("codeRequestStats");
                CodeRequestStats codeRequestStats = (CodeRequestStats) field.get(meta);
                if (!internetType.equalsIgnoreCase("noninternet")) {
                    xml.start("Internet");
                    for (String requestsType : requestsTypes) {
                        //要求返回是others
                        if (requestsType.equals("other")) {
                            xml.start("Others");
                        } else {
                            xml.start(requestsType.substring(0, 1).toUpperCase() + requestsType.substring(1));
                        }
                        for (String responseType : responsTypes) {
                            String response = responseType.replace("4xx", "4_n");
                            String code = response.substring(response.length() - 3, response.length());
                            if (code.equals("204") && !requestsType.equalsIgnoreCase("delete"))
                                continue;
                            if (code.equals("206") && !(requestsType.equalsIgnoreCase("get") || requestsType.equalsIgnoreCase("head"))) {
                                continue;
                            }
                            Field value = codeRequestStats.getClass().getDeclaredField("code_" + requestsType + code);
                            xml.start(responseType).value(String.valueOf(value.getLong(codeRequestStats))).end();
                        }
                        xml.end();
                    }
                    xml.end();
                }
                if (!internetType.equalsIgnoreCase("internet")) {
                    xml.start("NonInternet");
                    for (String requestsType : requestsTypes) {
                        String upperRequest = requestsType.substring(0, 1).toUpperCase() + requestsType.substring(1);
                        if (requestsType.equals("other")) {
                            xml.start("Others");
                        } else {
                            xml.start(upperRequest);
                        }
                        for (String responseType : responsTypes) {
                            String response = responseType.replace("4xx", "4_n");
                            String code = response.substring(response.length() - 3, response.length());
                            if (code.equals("204") && !requestsType.equalsIgnoreCase("delete"))
                                continue;
                            if (code.equals("206") && !(requestsType.equalsIgnoreCase("get") || requestsType.equalsIgnoreCase("head")))
                                continue;
                            Field value = codeRequestStats.getClass().getDeclaredField("code_noNet" + upperRequest + code);
                            xml.start(responseType).value(String.valueOf(value.getLong(codeRequestStats))).end();
                        }
                        xml.end();
                    }
                    xml.end();
                }
                xml.end();
            }
            xml.end();
        }
    }

    private static void buildConcurrentConnectionXML(XmlWriter xml, Map<String, MinutesUsageMeta> bucketRegionUsage, String internetType) {
        for (Entry<String, MinutesUsageMeta> entry : bucketRegionUsage.entrySet()) {
            xml.start("Statistics");
            xml.start("Date").value(entry.getKey()).end();
            MinutesUsageMeta meta = entry.getValue();
            xml.start("Data");
            //          xml.start("Connection").value(String.valueOf(meta.connections + meta.noNet_connections)).end();
            if (!internetType.equalsIgnoreCase("noninternet")) {
                xml.start("InternetConnection").value(String.valueOf(meta.connection.connections)).end();
            }
            if (!internetType.equalsIgnoreCase("internet")) {
                xml.start("NonInternetConnection").value(String.valueOf(meta.connection.noNet_connections)).end();
            }
            xml.end();
            xml.end();
        }
    }

    private static void buildAvailableBandwidthXML(XmlWriter xml, Map<String, List<MinutesUsageMeta>> availBandwidthUsage,
            String inOutType, String internetType, String region) {
        for (Entry<String, List<MinutesUsageMeta>> usage : availBandwidthUsage.entrySet()) {
            xml.start("Statistics");
            xml.start("Date").value(usage.getKey()).end();
            for (MinutesUsageMeta meta : usage.getValue()) {
                xml.start("Data");
                xml.start("RegionName").value(meta.regionName).end();
                if (!(internetType.equalsIgnoreCase("noninternet") || inOutType.equalsIgnoreCase("outbound"))) {
                    xml.start("InternetInbound").value(String.valueOf(meta.availableBandwidth.pubInBW)).end();
                }
                if (!(internetType.equalsIgnoreCase("internet") || inOutType.equalsIgnoreCase("outbound"))) {
                    xml.start("NonInternetInbound").value(String.valueOf(meta.availableBandwidth.priInBW)).end();
                }
                if (!(internetType.equalsIgnoreCase("noninternet") || inOutType.equalsIgnoreCase("inbound"))) {
                    xml.start("InternetOutbound").value(String.valueOf(meta.availableBandwidth.pubOutBW)).end();
                }                    
                if (!(internetType.equalsIgnoreCase("internet") || inOutType.equalsIgnoreCase("inbound"))) {
                    xml.start("NonInternetOutbound").value(String.valueOf(meta.availableBandwidth.priOutBW)).end();
                }
                xml.end();
            }
            xml.end();
        }
    }

    private static void buildBilledStorageUsageXML(XmlWriter xml, Map<String, Map<String, MinutesUsageMeta>> bucketRegionUsageByTime) {
        for (Entry<String, Map<String, MinutesUsageMeta>> entry : bucketRegionUsageByTime.entrySet()) {
            xml.start("Statistics")
                    .start("Date").value(entry.getKey()).end();
            Map<String, MinutesUsageMeta> metaMap = entry.getValue();
            metaMap.forEach((storageType, meta) -> {
                SizeStats sizeStats = meta.sizeStats;
                xml.start(convertStorageTypeXmlLabelV2(storageType));
                // BilledStorageUsage：收费容量  RemainderChargeOfDuration：时长补齐 RemainderChargeOfSize：大小补齐
                xml.start("BilledStorage")
                        .start("BilledStorageUsage").value(String.valueOf(sizeStats.size_peak + sizeStats.size_preChangeComplete + sizeStats.size_preDeleteComplete + sizeStats.size_completePeak)).end()
                        .start("RemainderChargeStorageUsage").value(String.valueOf(sizeStats.size_preChangeComplete + sizeStats.size_preDeleteComplete + sizeStats.size_completePeak)).end()
                        .start("RemainderChargeOfDuration").value(String.valueOf(sizeStats.size_preChangeComplete + sizeStats.size_preDeleteComplete)).end()
                        .start("RemainderChargeOfSize").value(String.valueOf(sizeStats.size_completePeak)).end()
                        .end();
                xml.end();
            });
            xml.end();
        }
    }

    private static void buildRestoreCapacityXml(XmlWriter xml, Map<String, Map<String, MinutesUsageMeta>> bucketRegionUsageByTime) {
        for (Entry<String, Map<String, MinutesUsageMeta>> entry : bucketRegionUsageByTime.entrySet()) {
            xml.start("Statistics")
                    .start("Date").value(entry.getKey()).end();
            Map<String, MinutesUsageMeta> metaMap = entry.getValue();
            metaMap.forEach((storageType, meta) -> {
                if (Consts.STORAGE_CLASS_STANDARD.equals(storageType)) {
                    return;
                }
                xml.start(convertStorageTypeXmlLabel(storageType))
                        // RestoreCapacity：数据取回量
                        .start("RestoreCapacity").value(String.valueOf(meta.sizeStats.size_restore)).end()
                        .end();
            });
            xml.end();
        }
    }

    /**
     * 只适用于6.8版本之前的管理api接口
     */
    @Deprecated
    private static String convertStorageTypeXmlLabel(String storageType) {
        if (Objects.equals(Consts.STORAGE_CLASS_STANDARD_IA, storageType)) {
            return "Standard_ia";
        }
        //todo zyq 后续添加归档存储类型后做相应处理
        return "Data";
    }

    /**
     * 6.8版本之后的接口进行标签转换使用这个方法
     */
    private static String convertStorageTypeXmlLabelV2(String storageType) {
        if (Objects.equals(Consts.STORAGE_CLASS_STANDARD_IA, storageType)) {
            return "Standard_ia";
        }
        //todo zyq 后续添加归档存储类型后做相应处理
        return "Standard";
    }

    /**
     * 将统计数据按照time、bucket整理，包含一般统计项和连接数，不包含可用带宽
     *
     * zyq 目前该方法只提供给getConcurrentConnectionInXML（获取当前连接数）方法使用
     * 其余用量由于需要区分存储类型，已经提供新的方法
     * @see UsageResult#makeUserBucketUsageByTime(long, java.lang.String, java.lang.String, java.lang.String, java.util.List, java.lang.String, java.lang.String)
     * 此处调用新方法进行处理
     *
     * @param ownerId
     * @param beginDate
     * @param endDate
     * @param bucket
     * @param regionList
     * @param freq
     * @return
     * @throws Exception
     */
    private static Map<String, MinutesUsageMeta> makeUserBucketUsageByTime(long ownerId, String beginDate, String endDate, String bucket, List<String> regionList, String freq) throws Exception{
        //获取按time、bucket、region区分的数据
        Map<String, MinutesUsageMeta> resultMap = new TreeMap<>();
        Map<String, Map<String, MinutesUsageMeta>> map = makeUserBucketUsageByTime(ownerId, beginDate, endDate, bucket, regionList, freq, Consts.STORAGE_CLASS_STANDARD);
        map.forEach((time,mapBySt)->{
            if (mapBySt.get(Consts.STORAGE_CLASS_STANDARD) != null) {
                resultMap.put(time, mapBySt.get(Consts.STORAGE_CLASS_STANDARD));
            }
        });
        return resultMap;
    }

    /**
     * 将统计数据按照time、bucket整理，包含一般统计项和连接数，不包含可用带宽
     *
     * @param ownerId
     * @param beginDate
     * @param endDate
     * @param bucket
     * @param regionList
     * @param freq
     * @param storageType 存储类型 全部， 标准，低频，归档
     * @return Map<time, Map<storageType, minutesUsageMeta>>
     * @throws Exception
     */
    private static Map<String, Map<String, MinutesUsageMeta>> makeUserBucketUsageByTime(long ownerId, String beginDate, String endDate, String bucket, List<String> regionList, String freq, String storageType) throws Exception {

        bucket = StringUtils.defaultIfBlank(bucket, null);
        List<String> storageTypeList = Utils.getStorageTypeProcessList(storageType);
        UsageMetaType type = getUsageStatsQueryType(bucket, freq);

        // Map<region, List<MinutesUsageMeta>>
        Map<String, List<MinutesUsageMeta>> region2usage = getSingleCommonUsageStatsQuery(ownerId, beginDate, endDate, bucket,
                freq, new HashSet<>(regionList), type, storageType, false);
        // Map<storageType, Map<time, MinutesUsageMeta>>
        Map<String, Map<String, MinutesUsageMeta>> mapByStorageType = Maps.newHashMapWithExpectedSize(3);
        for (String st : storageTypeList) {
            //获取按time、bucket、region区分的数据
            Map<String, MinutesUsageMeta> resultMap = new TreeMap<>();
            // 整理数据格式，用于构建xml
            if (region2usage.containsKey(Consts.GLOBAL_DATA_REGION)) {
                for (MinutesUsageMeta meta : region2usage.get(Consts.GLOBAL_DATA_REGION)) {
                    if (Objects.equals(st, meta.storageType)) {
                        resultMap.put(meta.time, meta);
                    }
                }
            } else {
                for (Entry<String, List<MinutesUsageMeta>> entry : region2usage.entrySet()) {
                    for (MinutesUsageMeta meta : entry.getValue()) {
                        if (Objects.equals(st, meta.storageType)) {
                            resultMap.put(meta.time, meta);
                        }
                    }
                }
            }
            mapByStorageType.put(st, resultMap);
        }

        // Map<time, Map<storageType, MinutesUsageMeta>> 按time,storageType分类,方便后续生成xml
        Map<String, Map<String, MinutesUsageMeta>> result = Maps.newTreeMap();
        mapByStorageType.forEach((st,map)->{
            map.forEach((time,minutesUsageMeta)->{
                result.computeIfAbsent(time, k -> Maps.newHashMap()).put(st, minutesUsageMeta);
            });
        });
        return result;
    }

    /**
     * 将可用带宽数据按照time整理，只包含可用带宽数据
     * @param ownerId
     * @param beginDate
     * @param endDate
     * @param regionList
     * @return
     * @throws Exception
     */
    private static Map<String, List<MinutesUsageMeta>> makeAvailableBandwidthByTime(long ownerId, String beginDate, String endDate, List<String> regionList) throws Exception{
        Map<String, List<MinutesUsageMeta>> resultMap = new TreeMap<String, List<MinutesUsageMeta>>();
        Map<String, List<MinutesUsageMeta>> regionAvailableBandwidthUsage = getSingleAvailBandwidthUsageStatsQuery(ownerId, beginDate, endDate, new HashSet<String>(regionList), true);
        for (Entry<String, List<MinutesUsageMeta>> entry : regionAvailableBandwidthUsage.entrySet()) {
            for (MinutesUsageMeta meta : entry.getValue()) {
                List<MinutesUsageMeta> list = resultMap.get(meta.time);
                if (list == null)
                    list = new ArrayList<MinutesUsageMeta>();
                list.add(meta);
                resultMap.put(meta.time, list);
            }
        }
        return resultMap;
    }

    public static String getRegionUsageInJson(String beginDate,
                                              String endDate, String usageStats, String freq, String storageType) throws Exception {
        return getTotalUsersUsageStats(beginDate, endDate, usageStats, freq, storageType, null, Utils.JSON, true);
    }

    public static String getRegionUsageInCsv(String beginDate,
                                             String endDate, UsageType usageType, String freq, String storageType, boolean needAllRegionUsage) throws Exception {
        return getTotalUsersUsageStats(beginDate, endDate, null, freq, storageType, usageType, Utils.CSV, needAllRegionUsage);
    }

    /**
     * 解析所有用户的统计信息的查询,（region级别）
     *
     * @param beginDate
     * @param endDate
     * @param usageStats
     * @param freq
     * @return
     * @throws Exception
     */
    private static String getTotalUsersUsageStats(String beginDate,
                                                  String endDate, String usageStats, String freq, String storageType, UsageType usageType, String resultForm, boolean ifNeedAllRegionsUsage)
            throws Exception {
        List<String> storageTypeNeed2Process = Utils.getStorageTypeProcessList(storageType);
        UsageMetaType usageMetaType = getUsageStatsQueryType(null, freq);
        // 不分统计项进行查询
        Pair<Map<String, List<MinutesUsageMeta>>, Map<String, List<MinutesUsageMeta>>> p = getTotalUsersUsageStatsQuery(beginDate, endDate, freq, storageType);

        Map<String, List<MinutesUsageMeta>> region2usage = p.first();
        Map<String, List<MinutesUsageMeta>> region2AvailBandwidthUsage = p.second();

        if (Utils.JSON.equals(resultForm)) {
            JSONObject jsonResult = new JSONObject();
            for (String st : storageTypeNeed2Process) {
                // 根据存储类型 划分返回结果
                Map<String, List<MinutesUsageMeta>> regionUsageBySt = Maps.newHashMapWithExpectedSize(region2usage.size());
                Map<String, List<MinutesUsageMeta>> regionAvailBandwidthUsageBySt = Maps.newHashMapWithExpectedSize(region2AvailBandwidthUsage.size());

                region2usage.forEach((region, usageList) ->
                        regionUsageBySt.put(region, usageList.stream().filter(u -> Objects.equals(st, u.storageType)).collect(Collectors.toList())));
                region2AvailBandwidthUsage.forEach((region, usageList) ->
                        regionAvailBandwidthUsageBySt.put(region, usageList.stream().filter(u -> Objects.equals(st, u.storageType)).collect(Collectors.toList())));

                //todo 兼容旧版本接口数据 6.8上线后删除该数据
                if (Consts.STORAGE_CLASS_STANDARD.equals(storageType)) {
                    // 这里一定优先处理标准存储类型，所以重新初始化没有问题
                    jsonResult = new JSONObject(buildUsageStatsJson(usageStats, regionUsageBySt, regionAvailBandwidthUsageBySt, ifNeedAllRegionsUsage));
                }
                //todo

                jsonResult.put(st, buildUsageStatsJson(usageStats, regionUsageBySt, regionAvailBandwidthUsageBySt, ifNeedAllRegionsUsage));
            }
            if (Consts.ALL.equals(storageType)) {
                jsonResult.put(Consts.ALL, buildUsageStatsJson(usageStats, region2usage, region2AvailBandwidthUsage, ifNeedAllRegionsUsage));
            }
            jsonResult.put("time", getStandardLastPeriodTime());
            return jsonResult.toString();
        } else if (Utils.CSV.equals(resultForm)) {
            // csv格式无法将多个存储类型的数据统一返回，所以如果是csv格式直接返回结果即可
            return buildUsageStatsCsv(usageType, region2usage, region2AvailBandwidthUsage, usageMetaType, freq, ifNeedAllRegionsUsage, beginDate, endDate);
        }

        return null;
    }

    /**
     * 不分统计项进行查询，region级别
     *
     * @param beginDate
     * @param endDate
     * @param freq
     * @param storageType 存储类型 全部，标准，低频
     * @return Map<regionName, List<MinutesUsageMeta>>
     * @throws Exception
     */
    public static Pair<Map<String, List<MinutesUsageMeta>>, Map<String, List<MinutesUsageMeta>>> getTotalUsersUsageStatsQuery(
            String beginDate, String endDate, String freq, String storageType) throws Exception {
        UsageMetaType type = getUsageStatsQueryType(null, freq);
        List<String> regions = Common.getAllRegionsExceptGlobalRegion();
        Map<String, List<MinutesUsageMeta>> regionUsage = new HashMap<>();
        Map<String, List<MinutesUsageMeta>> regionAvailBandwidthUsage = new HashMap<>();
        // 计算5分钟粒度的查询endtime
        String today = Misc.formatyyyymmdd(new Date());
        boolean isToday = endDate.startsWith(today);
        long ownerId = -1;
        switch (type) {
            case DAY_OWNER:
                List<MinutesUsageMeta> allRegionUsages = UsageStatsQuery.listAllRegionUsagePerDay(beginDate, endDate, storageType);
                if (CollectionUtils.isNotEmpty(allRegionUsages)) {
                    for (MinutesUsageMeta usage : allRegionUsages) {
                        regionUsage.computeIfAbsent(usage.regionName, k -> new LinkedList<>()).add(usage);
                    }
                }
                break;
            case MINUTES_OWNER:
                String endTime;
                if (isToday) {
                    endTime = Misc.formatyyyymmddhhmm(Misc.getCurrentFiveMinuteMinus5MinDateCeil());
                } else {
//                endTime = endDate.concat(" 24:00") + Character.MAX_VALUE;
                    endTime = LocalDate.parse(endDate, Misc.format_uuuu_MM_dd).plusDays(1).toString().concat(" 00:00");
                }
                List<MinutesUsageMeta> allUsagesPer5Minutes = UsageStatsQuery.listAllRegionUsagePer5Minutes(beginDate.concat(" 00:05"), endTime, storageType);
                if (CollectionUtils.isNotEmpty(allUsagesPer5Minutes)) {
                    for (MinutesUsageMeta usage : allUsagesPer5Minutes) {
                        regionUsage.computeIfAbsent(usage.regionName, k -> new LinkedList<>()).add(usage);
                    }
                }
                for (String dataRegion : regions) {
                    // 查询可用带宽(低频存储类型，归档存储类型 不存在带宽数据)
                    List<MinutesUsageMeta> abwRes = UsageStatsQuery.listAvailableBandwidth(dataRegion, beginDate.concat(" 00:05"), endTime);
                    if (abwRes != null && abwRes.size() > 0) {
                        regionAvailBandwidthUsage.put(dataRegion, abwRes);
                    }
                }
                break;
            default:
                break;
        }
        if (MapUtils.isNotEmpty(regionUsage)) {
            getGlobalRegionUsage(ownerId, type, regionUsage);
        }
        return new Pair<>(regionUsage, regionAvailBandwidthUsage);
    }

    public static String getSingleUsageInJson(Long ownerId, String beginDate, String endDate, String usageStats, String bucketName, String freq, Set<String> regions, String storageType, boolean abwPermissionCheckEnabled, boolean ifNeedAllRegionsUsage) throws Exception {
        return getSingleUsageStats(ownerId, beginDate, endDate, usageStats, bucketName, freq, null, regions, storageType, abwPermissionCheckEnabled, Utils.JSON, ifNeedAllRegionsUsage);
    }

    public static String getSingleUsageInCsv(Long ownerId, String beginDate,
                                             String endDate, String bucketName, String freq, UsageType type,
                                             Set<String> regions, String storageType, boolean abwPermissionCheckEnabled, boolean ifNeedAllRegionsUsage) throws Exception {
        return getSingleUsageStats(ownerId, beginDate, endDate, null, bucketName, freq, type, regions, storageType, abwPermissionCheckEnabled, Utils.CSV, ifNeedAllRegionsUsage);
    }

    /**
     * 解析统计信息的查询， Usage_Stats
     *
     * @param ownerId
     * @param beginDate
     * @param endDate
     * @param usageStats
     * @param bucketName
     * @param freq
     * @param regions
     * @param abwPermissionCheckEnabled true表示需要检查用户的可用带宽可以查看的数据域权限，用于自服务门户查看；false表示不检查权限，用于业务管理平台查看
     * @param ifNeedAllRegionsUsage,    自服务门户为false 业务管理平台为true
     * @return
     * @throws Exception
     */
    private static String getSingleUsageStats(Long ownerId, String beginDate, String endDate, String usageStats, String bucketName, String freq, UsageType type, Set<String> regions, String storageType, boolean abwPermissionCheckEnabled, String resultForm, boolean ifNeedAllRegionsUsage) throws Exception {
        UsageMetaType usageMetaType = getUsageStatsQueryType(bucketName, freq);
        List<String> storageTypeNeed2Process = Utils.getStorageTypeProcessList(storageType);

        // 不分统计项进行查询
        Pair<Map<String, List<MinutesUsageMeta>>, Map<String, List<MinutesUsageMeta>>> p = getSingleUsageStatsQuery(ownerId, beginDate, endDate, bucketName, freq, regions, storageType, usageMetaType, abwPermissionCheckEnabled, ifNeedAllRegionsUsage);
        Map<String, List<MinutesUsageMeta>> regionUsage = p.first();
        Map<String, List<MinutesUsageMeta>> regionAvailBandwidthUsage = p.second();

        if (Utils.JSON.equals(resultForm)) {
            JSONObject jsonResult = new JSONObject();
            for (String st : storageTypeNeed2Process) {
                // 根据存储类型 划分返回结果
                Map<String, List<MinutesUsageMeta>> regionUsageBySt = Maps.newHashMapWithExpectedSize(regionUsage.size());
                Map<String, List<MinutesUsageMeta>> regionAvailBandwidthUsageBySt = Maps.newHashMapWithExpectedSize(regionAvailBandwidthUsage.size());

                regionUsage.forEach((region, usageList) ->
                        regionUsageBySt.put(region, usageList.stream().filter(u -> Objects.equals(st, u.storageType)).collect(Collectors.toList())));
                regionAvailBandwidthUsage.forEach((region, usageList) ->
                        regionAvailBandwidthUsageBySt.put(region, usageList.stream().filter(u -> Objects.equals(st, u.storageType)).collect(Collectors.toList())));

                //todo 兼容旧版本接口数据 6.8上线后删除该数据
                if (Consts.STORAGE_CLASS_STANDARD.equals(storageType)) {
                    // 这里一定优先处理标准存储类型，所以重新初始化没有问题
                    jsonResult = new JSONObject(buildUsageStatsJson(usageStats, regionUsageBySt, regionAvailBandwidthUsageBySt, ifNeedAllRegionsUsage));
                }
                //todo
                jsonResult.put(st, buildUsageStatsJson(usageStats, regionUsageBySt, regionAvailBandwidthUsageBySt, ifNeedAllRegionsUsage));
            }
            if (Consts.ALL.equals(storageType)) {
                jsonResult.put(Consts.ALL, buildUsageStatsJson(usageStats, regionUsage, regionAvailBandwidthUsage, ifNeedAllRegionsUsage));
            }
            jsonResult.put("time", getStandardLastPeriodTime());
            return jsonResult.toString();
        } else if (Utils.CSV.equals(resultForm)) {
            return buildUsageStatsCsv(type, regionUsage, regionAvailBandwidthUsage, usageMetaType, freq, ifNeedAllRegionsUsage, beginDate, endDate);
        }
        return null;
    }

    /**
     * 不分统计项进行查询，owner及bucket级别，包括一般统计项、连接数、可用带宽
     *
     * @param ownerId
     * @param beginDate
     * @param endDate
     * @param bucketName
     * @param freq
     * @param regions
     * @param type
     * @param abwPermissionCheckEnabled
     * @param ifDontNeedFill            没有数据的region是否需要补全0，不需要补全为true，需要补全为false
     * @return
     * @throws Exception
     */
    public static Pair<Map<String, List<MinutesUsageMeta>>, Map<String, List<MinutesUsageMeta>>> getSingleUsageStatsQuery(
            Long ownerId, String beginDate, String endDate, String bucketName,
            String freq, Set<String> regions, String storageType, UsageMetaType type, boolean abwPermissionCheckEnabled, boolean ifDontNeedFill) throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(20);
        Map<String, Future<List<MinutesUsageMeta>>> regionUsage = new HashMap<>();
        Map<String, Future<List<MinutesUsageMeta>>> regionAvailBandwidthUsage = new HashMap<>();
        // 获取用户可以查看的可用带宽数据域列表
        List<String> permRegions = Utils.getABWPermissionRegions(ownerId);
        for (String dataRegion : regions) {
            // 多个线程取数据
            SelectUsageTask task = new SelectUsageTask(type, ownerId, dataRegion, bucketName, beginDate, endDate, storageType, ifDontNeedFill);
            Future<List<MinutesUsageMeta>> res = executor.submit(task);
            regionUsage.put(dataRegion, res);
            if (type.equals(UsageMetaType.MINUTES_OWNER)) {
                if (abwPermissionCheckEnabled) {
                    if (permRegions.contains(dataRegion)) {
                        SelectUsageTask taskAb = new SelectUsageTask(UsageMetaType.MINUTES_AVAIL_BW, -1, dataRegion, null, beginDate, endDate, ifDontNeedFill);
                        Future<List<MinutesUsageMeta>> resAb = executor.submit(taskAb);
                        regionAvailBandwidthUsage.put(dataRegion, resAb);
                    }
                } else {
                    SelectUsageTask taskAb = new SelectUsageTask(UsageMetaType.MINUTES_AVAIL_BW, -1, dataRegion, null, beginDate, endDate, ifDontNeedFill);
                    Future<List<MinutesUsageMeta>> resAb = executor.submit(taskAb);
                    regionAvailBandwidthUsage.put(dataRegion, resAb);
                }
            }
        }
        Map<String, List<MinutesUsageMeta>> reqRegionUsage = new HashMap<String, List<MinutesUsageMeta>>();
        Map<String, List<MinutesUsageMeta>> reqRegionAvailBandwidthUsage = new HashMap<String, List<MinutesUsageMeta>>();
        for (String k : regionUsage.keySet()) {
            // if (regionUsage.get(k).get().size() > 0) {
            reqRegionUsage.put(k, regionUsage.get(k).get());
        }
        for (String k : regionAvailBandwidthUsage.keySet())
            if (regionAvailBandwidthUsage.get(k).get().size() > 0) {
                reqRegionAvailBandwidthUsage.put(k, regionAvailBandwidthUsage.get(k).get());
            }
        executor.shutdown();
        if (regions.contains(Consts.GLOBAL_DATA_REGION)) {
            getGlobalRegionUsage(ownerId, type, reqRegionUsage);
        }
        return new Pair<>(reqRegionUsage, reqRegionAvailBandwidthUsage);
    }

    /**
     * 只查询一般统计数据，owner级别及bucket级别
     *
     * @param ownerId
     * @param beginDate
     * @param endDate
     * @param bucketName
     * @param freq
     * @param regions
     * @param type
     * @param storageType 存储类型，全部，标准，低频，归档。
     * @param ifDontNeedFill 没有数据的region是否需要补全0，不需要补全为true，需要补全为false
     * @return
     * @throws Exception
     */
    public static Map<String, List<MinutesUsageMeta>> getSingleCommonUsageStatsQuery(
            Long ownerId, String beginDate, String endDate, String bucketName,
            String freq, Set<String> regions, UsageMetaType type, String storageType, boolean ifDontNeedFill) throws Exception {
        //todo zyq 修改线程池 提出来共用
        ExecutorService executor = Executors.newFixedThreadPool(20);
        Map<String, Future<List<MinutesUsageMeta>>> regionUsage = new HashMap<>();
        for (String dataRegion : regions) {
            // 多个线程取数据
            SelectUsageTask task = new SelectUsageTask(type, ownerId, dataRegion, bucketName, beginDate, endDate, storageType, ifDontNeedFill);
            Future<List<MinutesUsageMeta>> res = executor.submit(task);
            regionUsage.put(dataRegion, res);
        }
        Map<String, List<MinutesUsageMeta>> reqRegionUsage = new HashMap<String, List<MinutesUsageMeta>>();
        for (String k : regionUsage.keySet()) {
//            if (regionUsage.get(k).get().size() > 0) {
            reqRegionUsage.put(k, regionUsage.get(k).get());
        }
        executor.shutdown();
        if (regions.contains(Consts.GLOBAL_DATA_REGION)) {
            getGlobalRegionUsage(ownerId, type, reqRegionUsage);
        }
        return reqRegionUsage;
    }
    
    /**
     * 只查询可用带宽数据，owner及bucket级别
     * @param ownerId
     * @param beginDate
     * @param endDate
     * @param regions
     * @param abwPermissionCheckEnabled
     * @return
     * @throws Exception
     */
    public static Map<String, List<MinutesUsageMeta>> getSingleAvailBandwidthUsageStatsQuery(Long ownerId,
            String beginDate, String endDate, Set<String> regions, boolean abwPermissionCheckEnabled) throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(20);
        Map<String, Future<List<MinutesUsageMeta>>> regionAvailBandwidthUsage = new HashMap<>();
        // 获取用户可以查看的可用带宽数据域列表
        List<String> permRegions = Utils.getABWPermissionRegions(ownerId);
        for (String dataRegion : regions) {
            // 多个线程取数据
            if (abwPermissionCheckEnabled) {
                if (permRegions.contains(dataRegion)) {
                    SelectUsageTask taskAb = new SelectUsageTask(UsageMetaType.MINUTES_AVAIL_BW, -1, dataRegion, null,
                            beginDate, endDate);
                    Future<List<MinutesUsageMeta>> resAb = executor.submit(taskAb);
                    regionAvailBandwidthUsage.put(dataRegion, resAb);
                }
            } else {
                SelectUsageTask taskAb = new SelectUsageTask(UsageMetaType.MINUTES_AVAIL_BW, -1, dataRegion, null,
                        beginDate, endDate);
                Future<List<MinutesUsageMeta>> resAb = executor.submit(taskAb);
                regionAvailBandwidthUsage.put(dataRegion, resAb);
            }

        }
        Map<String, List<MinutesUsageMeta>> reqRegionAvailBandwidthUsage = new HashMap<String, List<MinutesUsageMeta>>();
        for (String k : regionAvailBandwidthUsage.keySet())
            if (regionAvailBandwidthUsage.get(k).get().size() > 0) {
                reqRegionAvailBandwidthUsage.put(k, regionAvailBandwidthUsage.get(k).get());
            }        executor.shutdown();
        return reqRegionAvailBandwidthUsage;
    }

    /**
     * 只查询连接数数据，owner级别及bucket级别，补充到s粒度
     * @param ownerId
     * @param beginDate
     * @param endDate
     * @param bucketName
     * @param regions
     * @param type
     * @return
     * @throws Exception
     */
    public static Map<String, List<MinutesUsageMeta>> getSingleConnectionUsageStatsQuery(
            Long ownerId, String beginDate, String endDate, String bucketName,
            Set<String> regions, UsageMetaType type) throws Exception {      
        boolean isAccountLevel = isAccountLevel(bucketName);
        
        Map<String, List<MinutesUsageMeta>> region2usage = new HashMap<>();
        for (String dataRegion : regions) {
            List<MinutesUsageMeta> res = null;
            if (isAccountLevel) {
                res = UsageStatsQuery.listOwnerConnectionPer5Minutes(dataRegion, ownerId, beginDate, endDate + Character.MAX_VALUE);
            } else {
                res = UsageStatsQuery.listBucketConnectionPer5Minutes(dataRegion, ownerId, bucketName, beginDate, endDate + Character.MAX_VALUE);
            }
            if (res != null && res.size() > 0) {
                region2usage.put(dataRegion, res);
            }
        }
        if (regions.contains(Consts.GLOBAL_DATA_REGION)) {
            getGlobalRegionUsage(ownerId, type, region2usage);
        }
        
        return region2usage;
    }
    
    /**
     * 根据各个region统计值计算global的统计值
     * @param ownerId
     * @param type
     * @param region2usage
     * @return
     * @throws ExecutionException 
     * @throws InterruptedException 
     */
    private static void getGlobalRegionUsage(Long ownerId,  UsageMetaType type,
            Map<String, List<MinutesUsageMeta>> region2usage) throws InterruptedException, ExecutionException {
        TreeMap<String, MinutesUsageMeta> globalUsages = new TreeMap<>();
        Map<String, List<MinutesUsageMeta>> reqGlobalUsage = new HashMap<String, List<MinutesUsageMeta>>();
        for (String keyInfo : region2usage.keySet()) {
            List<MinutesUsageMeta> regionUsages = region2usage.get(keyInfo);
            for (MinutesUsageMeta usage : regionUsages) {
                String k = usage.bucketName == null ? usage.time + usage.storageType : usage.time + usage.bucketName + usage.storageType;
                MinutesUsageMeta globalUsage = globalUsages.get(k);
                if (globalUsage == null) {
                    if (UsageMetaType.DAY_OWNER.equals(type) || UsageMetaType.HOUR_OWNER.equals(type) || UsageMetaType.MINUTES_OWNER.equals(type)) {
                        globalUsage = new MinutesUsageMeta(type, Consts.GLOBAL_DATA_REGION, ownerId, usage.time, usage.storageType);
                    } else {
                        globalUsage = new MinutesUsageMeta(type, Consts.GLOBAL_DATA_REGION, ownerId, usage.bucketName, usage.time, usage.storageType);
                    }
                    globalUsage.initCommonStats();
                    globalUsage.timeVersion = usage.timeVersion;
                    if (usage.bandwidthMeta != null)
                        globalUsage.bandwidthMeta = new BandwidthMeta();
                    globalUsages.put(k, globalUsage);
                }
                if (usage.sizeStats != null) {
                    globalUsage.sizeStats.sumExceptPeakAndAvg(usage.sizeStats);
                    // 计算平均容量
                    globalUsage.sizeStats.size_avgTotal += usage.sizeStats.size_avgTotal;
                    globalUsage.sizeStats.size_avgRedundant += usage.sizeStats.size_avgRedundant;
                    globalUsage.sizeStats.size_avgAlin += usage.sizeStats.size_avgAlin;
                    globalUsage.sizeStats.size_avgOriginalTotal += usage.sizeStats.size_avgOriginalTotal;
                    // 峰值容量
                    globalUsage.sizeStats.size_peak += usage.sizeStats.size_peak;
                    globalUsage.sizeStats.size_originalPeak += usage.sizeStats.size_originalPeak;
                    globalUsage.sizeStats.size_completePeak += usage.sizeStats.size_completePeak;
                }
                if (usage.flowStats != null)
                    globalUsage.flowStats.sumAll(usage.flowStats);
                if (usage.requestStats != null)
                    globalUsage.requestStats.sumAll(usage.requestStats);
                if (usage.codeRequestStats != null)
                    globalUsage.codeRequestStats.sumAll(usage.codeRequestStats);
                if (usage.bandwidthMeta != null) {
                    if (globalUsage.bandwidthMeta == null)
                        globalUsage.bandwidthMeta = new BandwidthMeta();
                    globalUsage.bandwidthMeta.sum(usage.bandwidthMeta);
                }
                if (usage.connection != null) {
                    globalUsage.connection.connections += usage.connection.connections;
                    globalUsage.connection.noNet_connections += usage.connection.noNet_connections;
                }                
            }
        }
        reqGlobalUsage.put(Consts.GLOBAL_DATA_REGION, new ArrayList<>(globalUsages.values()));
        region2usage.putAll(reqGlobalUsage);
    }
    
    /**
     * 按统计项返回JSON格式的结果
     * @param usageStats
     * @param regionUsage
     * @param region2AvailBandwidthUsage
     * @param ifNeedAllRegionsUsage
     * @return
     * @throws Exception 
     */
    private static String buildUsageStatsJson(String usageStats,
            Map<String, List<MinutesUsageMeta>> regionUsage,
            Map<String, List<MinutesUsageMeta>> region2AvailBandwidthUsage,
            boolean ifNeedAllRegionsUsage) throws Exception {
        //获取请求query参数，多个统计项以逗号分隔
        String[] usageArr = usageStats.split(",");
        JSONObject usageItem = new JSONObject();

        //自服务门户如果包含global，则只取global，ifNeedAllRegionsUsage为false
        //如果是业务管理平台则同时需要global和所有region的数据，ifNeedAllRegionsUsage为true
        if (regionUsage.containsKey(Consts.GLOBAL_DATA_REGION) && !ifNeedAllRegionsUsage) {
            Map<String, List<MinutesUsageMeta>> tempMap = new HashMap<String, List<MinutesUsageMeta>>();
            tempMap.put(Consts.GLOBAL_DATA_REGION, regionUsage.get(Consts.GLOBAL_DATA_REGION));
            regionUsage = tempMap;
        }

        for (int i = 0; i < usageArr.length; i++) {
            String usageRequest = usageArr[i];
            // 连接数
            buildSingleCommonUsageJson("connection",regionUsage, usageItem, usageRequest, "connection", "connections", "noNet_connections");
            buildSingleCommonUsageJson("connection", regionUsage, usageItem, usageRequest, "netConnection", "connections");
            buildSingleCommonUsageJson("connection", regionUsage, usageItem, usageRequest, "nonetConnection", "noNet_connections");
            // 容量，自服务门户显示归属地容量totalSize，业务管理平台还包括冗余容量redundantSize、对齐后容量alignSize
            // 按天查询，容量包含实时值、平均值、峰值；按5min查询，容量包含实时值
            buildSingleCommonUsageJson("sizeStats", regionUsage, usageItem, usageRequest, "totalSize", "size_total");
            buildSingleCommonUsageJson("sizeStats", regionUsage, usageItem, usageRequest, "redundantSize", "size_redundant");
            buildSingleCommonUsageJson("sizeStats", regionUsage, usageItem, usageRequest, "alignSize", "size_alin");
            buildSingleCommonUsageJson("sizeStats", regionUsage, usageItem, usageRequest, "originalTotalSize", "size_originalTotal");
            buildSingleCommonUsageJson("sizeStats", regionUsage, usageItem, usageRequest, "avgTotalSize", "size_avgTotal");
            buildSingleCommonUsageJson("sizeStats", regionUsage, usageItem, usageRequest, "avgRedundantSize", "size_avgRedundant");
            buildSingleCommonUsageJson("sizeStats", regionUsage, usageItem, usageRequest, "avgAlignSize", "size_avgAlin");
            buildSingleCommonUsageJson("sizeStats", regionUsage, usageItem, usageRequest, "avgOriginalTotalSize", "size_avgOriginalTotal");
            buildSingleCommonUsageJson("sizeStats", regionUsage, usageItem, usageRequest, "peakSize", "size_peak");
            buildSingleCommonUsageJson("sizeStats", regionUsage, usageItem, usageRequest, "originalPeakSize", "size_originalPeak");
            // 低频存储相关字段
            buildSingleCommonUsageJson("sizeStats", regionUsage, usageItem, usageRequest, "restoreSize", "size_restore");
            buildSingleCommonUsageJson("sizeStats", regionUsage, usageItem, usageRequest, "changeSize", "size_changeSize");
            buildSingleCommonUsageJson("sizeStats", regionUsage, usageItem, usageRequest, "preChangeSize", "size_preChangeSize");
            buildSingleCommonUsageJson("sizeStats", regionUsage, usageItem, usageRequest, "preDeleteSize", "size_preDeleteSize");
            buildSingleCommonUsageJson("sizeStats", regionUsage, usageItem, usageRequest, "preDeleteComplete", "size_preDeleteComplete");
            buildSingleCommonUsageJson("sizeStats", regionUsage, usageItem, usageRequest, "preChangeComplete", "size_preChangeComplete");
            buildSingleCommonUsageJson("sizeStats", regionUsage, usageItem, usageRequest, "completeSize", "size_completeSize");
            buildSingleCommonUsageJson("sizeStats", regionUsage, usageItem, usageRequest, "completePeakSize", "size_completePeak");
            // 计费容量
            buildSingleCommonUsageJson("sizeStats", regionUsage, usageItem, usageRequest, "billSize", "size_completePeak","size_preChangeComplete","size_preDeleteComplete","size_peak");
            // 计费容量峰值
            buildSingleCommonUsageJson("sizeStats", regionUsage, usageItem, usageRequest, "billPeakSize", "size_completePeak","size_preChangeComplete","size_preDeleteComplete","size_peak");

            // 以http method分类的请求次数,包括互联网、非互联网的get、head、put、post、delete、other请求次数
            buildSingleCommonUsageJson("requestStats", regionUsage, usageItem, usageRequest, "allReq",
                    "req_get", "req_head", "req_put", "req_post", "req_delete", "req_other",
                    "req_noNetGet", "req_noNetHead", "req_noNetPut", "req_noNetPost", "req_noNetDelete", "req_noNetOther");

            buildSingleCommonUsageJson("requestStats", regionUsage, usageItem, usageRequest, "getReq", "req_get", "req_noNetGet");
            buildSingleCommonUsageJson("requestStats", regionUsage, usageItem, usageRequest, "headReq", "req_head", "req_noNetHead");
            buildSingleCommonUsageJson("requestStats", regionUsage, usageItem, usageRequest, "putReq", "req_put", "req_noNetPut");
            buildSingleCommonUsageJson("requestStats", regionUsage, usageItem, usageRequest, "postReq", "req_post", "req_noNetPost");
            buildSingleCommonUsageJson("requestStats", regionUsage, usageItem, usageRequest, "deleteReq", "req_delete", "req_noNetDelete");
            buildSingleCommonUsageJson("requestStats", regionUsage, usageItem, usageRequest, "otherReq", "req_other", "req_noNetOther");

            buildSingleCommonUsageJson("requestStats", regionUsage, usageItem, usageRequest, "netGetReq", "req_get");
            buildSingleCommonUsageJson("requestStats", regionUsage, usageItem, usageRequest, "netHeadReq", "req_head");
            buildSingleCommonUsageJson("requestStats", regionUsage, usageItem, usageRequest, "netPutReq", "req_put");
            buildSingleCommonUsageJson("requestStats", regionUsage, usageItem, usageRequest, "netPostReq", "req_post");
            buildSingleCommonUsageJson("requestStats", regionUsage, usageItem, usageRequest, "netDeleteReq", "req_delete");
            buildSingleCommonUsageJson("requestStats", regionUsage, usageItem, usageRequest, "netOtherReq", "req_other");
            buildSingleCommonUsageJson("requestStats", regionUsage, usageItem, usageRequest, "noNetGetReq", "req_noNetGet");
            buildSingleCommonUsageJson("requestStats", regionUsage, usageItem, usageRequest, "noNetHeadReq", "req_noNetHead");
            buildSingleCommonUsageJson("requestStats", regionUsage, usageItem, usageRequest, "noNetPutReq", "req_noNetPut");
            buildSingleCommonUsageJson("requestStats", regionUsage, usageItem, usageRequest, "noNetPostReq", "req_noNetPost");
            buildSingleCommonUsageJson("requestStats", regionUsage, usageItem, usageRequest, "noNetDeleteReq", "req_noNetDelete");
            buildSingleCommonUsageJson("requestStats", regionUsage, usageItem, usageRequest, "noNetOtherReq", "req_noNetOther");

            // 文本反垃圾、图片鉴黄调用量
            buildSingleCommonUsageJson("requestStats", regionUsage, usageItem, usageRequest, "spamReq", "req_spam");
            buildSingleCommonUsageJson("requestStats", regionUsage, usageItem, usageRequest, "pornReviewFalse", "req_pornReviewFalse");
            buildSingleCommonUsageJson("requestStats", regionUsage, usageItem, usageRequest, "pornReviewTrue", "req_pornReviewTrue");
            buildSingleCommonUsageJson("requestStats", regionUsage, usageItem, usageRequest, "porn", "req_pornReviewFalse", "req_pornReviewTrue");

            // ALL 所有上下行流量之和
            buildSingleCommonUsageJson("flowStats", regionUsage, usageItem, usageRequest, "allUploadTotal", "flow_upload",
                    "flow_noNetUpload", "flow_roamUpload", "flow_noNetRoamUpload");
            buildSingleCommonUsageJson("flowStats", regionUsage, usageItem, usageRequest, "allDownloadTotal", "flow_download",
                    "flow_noNetDownload", "flow_roamDownload", "flow_noNetRoamDownload");
            //互联网上下行流量之和
            buildSingleCommonUsageJson("flowStats", regionUsage, usageItem, usageRequest, "netUploadTotal", "flow_upload", "flow_roamUpload");
            buildSingleCommonUsageJson("flowStats", regionUsage, usageItem, usageRequest, "netDownloadTotal", "flow_download", "flow_roamDownload");

            //非互联网上下行流量之和
            buildSingleCommonUsageJson("flowStats", regionUsage, usageItem, usageRequest, "noNetUploadTotal", "flow_noNetUpload", "flow_noNetRoamUpload");
            buildSingleCommonUsageJson("flowStats", regionUsage, usageItem, usageRequest, "noNetDownloadTotal", "flow_noNetDownload", "flow_noNetRoamDownload");

            //直接上下行流量之和
            buildSingleCommonUsageJson("flowStats", regionUsage, usageItem, usageRequest, "uploadTotal", "flow_upload", "flow_noNetUpload");
            buildSingleCommonUsageJson("flowStats", regionUsage, usageItem, usageRequest, "downloadTotal", "flow_download", "flow_noNetDownload");
            //漫游上下行流量之和
            buildSingleCommonUsageJson("flowStats", regionUsage, usageItem, usageRequest, "roamUploadTotal", "flow_roamUpload", "flow_noNetRoamUpload");
            buildSingleCommonUsageJson("flowStats", regionUsage, usageItem, usageRequest, "roamDownloadTotal", "flow_roamDownload", "flow_noNetRoamDownload");

            // 直接流量
            buildSingleCommonUsageJson("flowStats", regionUsage, usageItem, usageRequest, "netUpload", "flow_upload");
            buildSingleCommonUsageJson("flowStats", regionUsage, usageItem, usageRequest, "netDownload", "flow_download");
            buildSingleCommonUsageJson("flowStats", regionUsage, usageItem, usageRequest, "noNetUpload", "flow_noNetUpload");
            buildSingleCommonUsageJson("flowStats", regionUsage, usageItem, usageRequest, "noNetDownload", "flow_noNetDownload");
            // 漫游流量
            buildSingleCommonUsageJson("flowStats", regionUsage, usageItem, usageRequest, "netRoamUpload", "flow_roamUpload");
            buildSingleCommonUsageJson("flowStats", regionUsage, usageItem, usageRequest, "netRoamDownload", "flow_roamDownload");
            buildSingleCommonUsageJson("flowStats", regionUsage, usageItem, usageRequest, "noNetRoamUpload", "flow_noNetRoamUpload");
            buildSingleCommonUsageJson("flowStats", regionUsage, usageItem, usageRequest, "noNetRoamDownload", "flow_noNetRoamDownload");

            //删除流量
            buildSingleCommonUsageJson("flowStats", regionUsage, usageItem, usageRequest, "deleteFlow", "flow_delete");
            buildSingleCommonUsageJson("flowStats", regionUsage, usageItem, usageRequest, "noNetDeleteFlow", "flow_noNetDelete");

            // ALL 上下行可用带宽之和
            buildSingleCommonUsageJson("availableBandwidth", region2AvailBandwidthUsage, usageItem, usageRequest,
                    "availTotalUploadBW", "pubInBW", "priInBW");
            buildSingleCommonUsageJson("availableBandwidth", region2AvailBandwidthUsage, usageItem, usageRequest,
                    "availTotalDownloadBW", "pubOutBW", "priOutBW");
            // 可用带宽
            buildSingleCommonUsageJson("availableBandwidth", region2AvailBandwidthUsage, usageItem, usageRequest, "availPubUploadBW", "pubInBW");
            buildSingleCommonUsageJson("availableBandwidth", region2AvailBandwidthUsage, usageItem, usageRequest, "availPubDownloadBW", "pubOutBW");
            buildSingleCommonUsageJson("availableBandwidth", region2AvailBandwidthUsage, usageItem, usageRequest, "availPriUploadBW", "priInBW");
            buildSingleCommonUsageJson("availableBandwidth", region2AvailBandwidthUsage, usageItem, usageRequest, "availPriDownloadBW", "priOutBW");

            // 已用带宽
            buildSingleCommonUsageJson("bandwidthMeta", regionUsage, usageItem, usageRequest, "netUploadBW", "upload");
            buildSingleCommonUsageJson("bandwidthMeta", regionUsage, usageItem, usageRequest, "netDownloadBW", "transfer");
            buildSingleCommonUsageJson("bandwidthMeta", regionUsage, usageItem, usageRequest, "netRoamUploadBW", "roamUpload");
            buildSingleCommonUsageJson("bandwidthMeta", regionUsage, usageItem, usageRequest, "netRoamDownloadBW", "roamFlow");
            buildSingleCommonUsageJson("bandwidthMeta", regionUsage, usageItem, usageRequest, "noNetUploadBW", "noNetUpload");
            buildSingleCommonUsageJson("bandwidthMeta", regionUsage, usageItem, usageRequest, "noNetDownloadBW", "noNetTransfer");
            buildSingleCommonUsageJson("bandwidthMeta", regionUsage, usageItem, usageRequest, "noNetRoamUploadBW", "noNetRoamUpload");
            buildSingleCommonUsageJson("bandwidthMeta", regionUsage, usageItem, usageRequest, "noNetRoamDownloadBW", "noNetRoamFlow");
            // 外网上行已用带宽（外网直接上行和漫游上行之和）
            buildSingleCommonUsageJson("bandwidthMeta", regionUsage, usageItem, usageRequest, "netUploadTotalBW", "upload", "roamUpload");
            // 外网下行已用带宽（外网直接下行和漫游下行之和）
            buildSingleCommonUsageJson("bandwidthMeta", regionUsage, usageItem, usageRequest, "netDownloadTotalBW", "transfer", "roamFlow");
            // 内网上行已用带宽（内网直接上行和漫游上行之和）
            buildSingleCommonUsageJson("bandwidthMeta", regionUsage, usageItem, usageRequest, "noNetUploadTotalBW", "noNetUpload", "noNetRoamUpload");
            // 内网下行已用带宽（内网直接下行和漫游下行之和）
            buildSingleCommonUsageJson("bandwidthMeta", regionUsage, usageItem, usageRequest, "noNetDownloadTotalBW", "noNetTransfer", "noNetRoamFlow");


            //各个返回码所有类型的请求次数总和
            buildSingleCommonUsageJson("codeRequestStats", regionUsage, usageItem, usageRequest, "totalResponse200",
                    "code_get200", "code_noNetGet200", "code_put200", "code_noNetPut200", "code_head200", "code_noNetHead200",
                    "code_delete200", "code_noNetDelete200", "code_post200", "code_noNetPost200", "code_other200", "code_noNetOther200");
            buildSingleCommonUsageJson("codeRequestStats", regionUsage, usageItem, usageRequest, "totalResponse204",
                    "code_get204", "code_noNetGet204", "code_put204", "code_noNetPut204", "code_head204", "code_noNetHead204",
                    "code_delete204", "code_noNetDelete204", "code_post204", "code_noNetPost204", "code_other204", "code_noNetOther204");
            buildSingleCommonUsageJson("codeRequestStats", regionUsage, usageItem, usageRequest, "totalResponse206",
                    "code_get206", "code_noNetGet206", "code_put206", "code_noNetPut206", "code_head206", "code_noNetHead206",
                    "code_delete206", "code_noNetDelete206", "code_post206", "code_noNetPost206", "code_other206", "code_noNetOther206");
            buildSingleCommonUsageJson("codeRequestStats", regionUsage, usageItem, usageRequest, "totalResponse2_n",
                    "code_get2_n", "code_noNetGet2_n", "code_put2_n", "code_noNetPut2_n", "code_head2_n", "code_noNetHead2_n",
                    "code_delete2_n", "code_noNetDelete2_n", "code_post2_n", "code_noNetPost2_n", "code_other2_n", "code_noNetOther2_n");
            buildSingleCommonUsageJson("codeRequestStats", regionUsage, usageItem, usageRequest, "totalResponse403",
                    "code_get403", "code_noNetGet403", "code_put403", "code_noNetPut403", "code_head403", "code_noNetHead403",
                    "code_delete403", "code_noNetDelete403", "code_post403", "code_noNetPost403", "code_other403", "code_noNetOther403");
            buildSingleCommonUsageJson("codeRequestStats", regionUsage, usageItem, usageRequest, "totalResponse404",
                    "code_get404", "code_noNetGet404", "code_put404", "code_noNetPut404", "code_head404", "code_noNetHead404",
                    "code_delete404", "code_noNetDelete404", "code_post404", "code_noNetPost404", "code_other404", "code_noNetOther404");
            buildSingleCommonUsageJson("codeRequestStats", regionUsage, usageItem, usageRequest, "totalResponse4_n",
                    "code_get4_n", "code_noNetGet4_n", "code_put4_n", "code_noNetPut4_n", "code_head4_n", "code_noNetHead4_n",
                    "code_delete4_n", "code_noNetDelete4_n", "code_post4_n", "code_noNetPost4_n", "code_other4_n", "code_noNetOther4_n");

            // 区分返回码的请求次数
            buildSingleCommonUsageJson("codeRequestStats", regionUsage, usageItem, usageRequest, "totalGet200", "code_get200", "code_noNetGet200");
            buildSingleCommonUsageJson("codeRequestStats", regionUsage, usageItem, usageRequest, "totalGet204", "code_get204", "code_noNetGet204");
            buildSingleCommonUsageJson("codeRequestStats", regionUsage, usageItem, usageRequest, "totalGet206", "code_get206", "code_noNetGet206");
            buildSingleCommonUsageJson("codeRequestStats", regionUsage, usageItem, usageRequest, "totalGet2_n", "code_get2_n", "code_noNetGet2_n");
            buildSingleCommonUsageJson("codeRequestStats", regionUsage, usageItem, usageRequest, "totalGet403", "code_get403", "code_noNetGet403");
            buildSingleCommonUsageJson("codeRequestStats", regionUsage, usageItem, usageRequest, "totalGet404", "code_get404", "code_noNetGet404");
            buildSingleCommonUsageJson("codeRequestStats", regionUsage, usageItem, usageRequest, "totalGet4_n", "code_get4_n", "code_noNetGet4_n");

            buildSingleCommonUsageJson("codeRequestStats", regionUsage, usageItem, usageRequest, "totalPut200", "code_put200", "code_noNetPut200");
            buildSingleCommonUsageJson("codeRequestStats", regionUsage, usageItem, usageRequest, "totalPut204", "code_put204", "code_noNetPut204");
            buildSingleCommonUsageJson("codeRequestStats", regionUsage, usageItem, usageRequest, "totalPut206", "code_put206", "code_noNetPut206");
            buildSingleCommonUsageJson("codeRequestStats", regionUsage, usageItem, usageRequest, "totalPut2_n", "code_put2_n", "code_noNetPut2_n");
            buildSingleCommonUsageJson("codeRequestStats", regionUsage, usageItem, usageRequest, "totalPut403", "code_put403", "code_noNetPut403");
            buildSingleCommonUsageJson("codeRequestStats", regionUsage, usageItem, usageRequest, "totalPut404", "code_put404", "code_noNetPut404");
            buildSingleCommonUsageJson("codeRequestStats", regionUsage, usageItem, usageRequest, "totalPut4_n", "code_put4_n", "code_noNetPut4_n");

            buildSingleCommonUsageJson("codeRequestStats", regionUsage, usageItem, usageRequest, "totalHead200", "code_head200", "code_noNetHead200");
            buildSingleCommonUsageJson("codeRequestStats", regionUsage, usageItem, usageRequest, "totalHead204", "code_head204", "code_noNetHead204");
            buildSingleCommonUsageJson("codeRequestStats", regionUsage, usageItem, usageRequest, "totalHead206", "code_head206", "code_noNetHead206");
            buildSingleCommonUsageJson("codeRequestStats", regionUsage, usageItem, usageRequest, "totalHead2_n", "code_head2_n", "code_noNetHead2_n");
            buildSingleCommonUsageJson("codeRequestStats", regionUsage, usageItem, usageRequest, "totalHead403", "code_head403", "code_noNetHead403");
            buildSingleCommonUsageJson("codeRequestStats", regionUsage, usageItem, usageRequest, "totalHead404", "code_head404", "code_noNetHead404");
            buildSingleCommonUsageJson("codeRequestStats", regionUsage, usageItem, usageRequest, "totalHead4_n", "code_head4_n", "code_noNetHead4_n");

            buildSingleCommonUsageJson("codeRequestStats", regionUsage, usageItem, usageRequest, "totalDelete200", "code_delete200", "code_noNetDelete200");
            buildSingleCommonUsageJson("codeRequestStats", regionUsage, usageItem, usageRequest, "totalDelete204", "code_delete204", "code_noNetDelete204");
            buildSingleCommonUsageJson("codeRequestStats", regionUsage, usageItem, usageRequest, "totalDelete206", "code_delete206", "code_noNetDelete206");
            buildSingleCommonUsageJson("codeRequestStats", regionUsage, usageItem, usageRequest, "totalDelete2_n", "code_delete2_n", "code_noNetDelete2_n");
            buildSingleCommonUsageJson("codeRequestStats", regionUsage, usageItem, usageRequest, "totalDelete403", "code_delete403", "code_noNetDelete403");
            buildSingleCommonUsageJson("codeRequestStats", regionUsage, usageItem, usageRequest, "totalDelete404", "code_delete404", "code_noNetDelete404");
            buildSingleCommonUsageJson("codeRequestStats", regionUsage, usageItem, usageRequest, "totalDelete4_n", "code_delete4_n", "code_noNetDelete4_n");

            buildSingleCommonUsageJson("codeRequestStats", regionUsage, usageItem, usageRequest, "totalPost200", "code_post200", "code_noNetPost200");
            buildSingleCommonUsageJson("codeRequestStats", regionUsage, usageItem, usageRequest, "totalPost204", "code_post204", "code_noNetPost204");
            buildSingleCommonUsageJson("codeRequestStats", regionUsage, usageItem, usageRequest, "totalPost206", "code_post206", "code_noNetPost206");
            buildSingleCommonUsageJson("codeRequestStats", regionUsage, usageItem, usageRequest, "totalPost2_n", "code_post2_n", "code_noNetPost2_n");
            buildSingleCommonUsageJson("codeRequestStats", regionUsage, usageItem, usageRequest, "totalPost403", "code_post403", "code_noNetPost403");
            buildSingleCommonUsageJson("codeRequestStats", regionUsage, usageItem, usageRequest, "totalPost404", "code_post404", "code_noNetPost404");
            buildSingleCommonUsageJson("codeRequestStats", regionUsage, usageItem, usageRequest, "totalPost4_n", "code_post4_n", "code_noNetPost4_n");

            buildSingleCommonUsageJson("codeRequestStats", regionUsage, usageItem, usageRequest, "totalOther200", "code_other200", "code_noNetOther200");
            buildSingleCommonUsageJson("codeRequestStats", regionUsage, usageItem, usageRequest, "totalOther204", "code_other204", "code_noNetOther204");
            buildSingleCommonUsageJson("codeRequestStats", regionUsage, usageItem, usageRequest, "totalOther206", "code_other206", "code_noNetOther206");
            buildSingleCommonUsageJson("codeRequestStats", regionUsage, usageItem, usageRequest, "totalOther2_n", "code_other2_n", "code_noNetOther2_n");
            buildSingleCommonUsageJson("codeRequestStats", regionUsage, usageItem, usageRequest, "totalOther403", "code_other403", "code_noNetOther403");
            buildSingleCommonUsageJson("codeRequestStats", regionUsage, usageItem, usageRequest, "totalOther404", "code_other404", "code_noNetOther404");
            buildSingleCommonUsageJson("codeRequestStats", regionUsage, usageItem, usageRequest, "totalOther4_n", "code_other4_n", "code_noNetOther4_n");

            // 性能分析 本期不上
//            buildSinglePerformanceAnalysisJson(regionUsage, usageItem, usageRequest, "availableRate");
//            buildSinglePerformanceAnalysisJson(regionUsage, usageItem, usageRequest, "effectiveRate");
//            buildSinglePerformanceAnalysisJson(regionUsage, usageItem, usageRequest, "effectiveReqNum");
//            buildSinglePerformanceAnalysisJson(regionUsage, usageItem, usageRequest, "forbiddenRate");
//            buildSinglePerformanceAnalysisJson(regionUsage, usageItem, usageRequest, "forbiddenReqNum");
//            buildSinglePerformanceAnalysisJson(regionUsage, usageItem, usageRequest, "notFoundRate");
//            buildSinglePerformanceAnalysisJson(regionUsage, usageItem, usageRequest, "notFoundReqNum");
//            buildSinglePerformanceAnalysisJson(regionUsage, usageItem, usageRequest, "othersErrorRate");
//            buildSinglePerformanceAnalysisJson(regionUsage, usageItem, usageRequest, "othersErrorReqNum");
        }
        return usageItem.toString();
    }
    
    //返回标准的最后统计运行时间
    public static String getStandardLastPeriodTime() throws Exception {
        long lastTime = UsageResult.getLastPeriodTime();
//        LocalDateTime ldt = LocalDateTime.ofEpochSecond(lastTime, 0, ZoneOffset.ofHours(8));
        Instant instant = Instant.ofEpochMilli(lastTime);
        LocalDateTime ldt = LocalDateTime.ofInstant(instant, ZoneId.systemDefault());
        DateTimeFormatter dTF = DateTimeFormatter.ofPattern("yyyy.MM.dd HH:mm");
        return ldt.format(dTF) + " UTC+0800";
    }

    /**
     * 构建CVS格式统计项
     *
     * @param type                       统计项分类
     * @param region2usage
     * @param region2AvailBandwidthUsage
     * @param usageMetaType              rowkey type
     * @param freq
     * @return
     * @throws JSONException
     * @throws BaseException
     */
    private static String buildUsageStatsCsv(UsageType type,
                                             Map<String, List<MinutesUsageMeta>> region2usage,
                                             Map<String, List<MinutesUsageMeta>> region2AvailBandwidthUsage,
                                             UsageMetaType usageMetaType,
                                             String freq,
                                             boolean ifNeedAllRegionsUsage, String beginDate, String endDate) throws JSONException, BaseException {
        if ((type.equals(UsageType.AVAIL_BANDWIDTH) || type.equals(UsageType.BANDWIDTH) || type.equals(UsageType.CONNECTIONS))
                && (usageMetaType.equals(UsageMetaType.DAY_BUCKET) || usageMetaType.equals(UsageMetaType.DAY_OWNER) ||
                usageMetaType.equals(UsageMetaType.HOUR_OWNER) || usageMetaType.equals(UsageMetaType.HOUR_BUCKET))) {
            throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_ARGUMENT, ErrorMessage.ERROR_CODE_INVALID_ARGUMENT);
        }
        return CSVUtils.createCSV(region2usage, region2AvailBandwidthUsage, type, usageMetaType, freq, ifNeedAllRegionsUsage, beginDate, endDate);
    }
    
    /**
     * 构建单个JSON格式性能分析统计项
     * @param regionUsage
     * @param jsonItem
     * @param usageRequest
     * @param jsonUsageItem
     * @throws JSONException 
     */
    private static void buildSinglePerformanceAnalysisJson(Map<String, List<MinutesUsageMeta>> regionUsage,
            JSONObject jsonItem, String usageRequest, String jsonUsageItem) {
        try {
            if (usageRequest.equals(jsonUsageItem)) {
                JSONArray regionsUsageJson = new JSONArray();
                for (String k : regionUsage.keySet()) {
                    JSONObject regionJo = new JSONObject();
                    JSONArray dataJa = new JSONArray();
                    for (MinutesUsageMeta m : regionUsage.get(k)) {
                        if (m.codeRequestStats != null && m.requestStats != null) {
                            JSONObject dataJo = new JSONObject();
                            dataJo.put("date", m.time);
                            BigDecimal total5xx;
                            BigDecimal total2xx;
                            BigDecimal total403;
                            BigDecimal total404;
                            BigDecimal total4_n;
                            BigDecimal totalReq;
                            BigDecimal ratio;
                            switch (jsonUsageItem) {
                            case "availableRate":
                                // 服务可用性：1-服务端错误请求（返回状态码为5xx）占总请求的百分比
                                total5xx = new BigDecimal(m.codeRequestStats.getTotal5xxRequestNum());
                                totalReq = new BigDecimal(m.requestStats.getTotalRequest());
                                ratio = new BigDecimal(1).subtract(total5xx.divide(totalReq, 2, BigDecimal.ROUND_HALF_UP));
                                dataJo.put("value", ratio.doubleValue());
                                break;
                            case "effectiveRate":
                                // 有效请求率：有效请求数占总请求数的百分比
                                total2xx = new BigDecimal(m.codeRequestStats.getTotal2xxRequestNum());
                                totalReq = new BigDecimal(m.requestStats.getTotalRequest());
                                ratio = new BigDecimal(1).subtract(total2xx.divide(totalReq, 2, BigDecimal.ROUND_HALF_UP));
                                dataJo.put("value", ratio.doubleValue());
                                break;
                            case "effectiveReqNum":
                                // 有效请求：返回状态码为2xx的请求数量
                                total2xx = new BigDecimal(m.codeRequestStats.getTotal2xxRequestNum());
                                dataJo.put("value", total2xx.longValue());
                                break;
                            case "forbiddenRate":
                                // 客户端授权错误率：客户端授权错误数占总请求数的百分比
                                total403 = new BigDecimal(m.codeRequestStats.getTotal403RequestNum());
                                totalReq = new BigDecimal(m.requestStats.getTotalRequest());
                                ratio = new BigDecimal(1).subtract(total403.divide(totalReq, 2, BigDecimal.ROUND_HALF_UP));
                                dataJo.put("value", ratio.doubleValue());
                                break;
                            case "forbiddenReqNum":
                                // 客户端授权错误数量：返回状态码为403的请求数量
                                total403 = new BigDecimal(m.codeRequestStats.getTotal403RequestNum());
                                dataJo.put("value", total403.longValue());
                                break;
                            case "notFoundRate":
                                // 客户端资源不存在错误率：客户端资源不存在数占总请求数的百分比
                                total404 = new BigDecimal(m.codeRequestStats.getTotal404RequestNum());
                                totalReq = new BigDecimal(m.requestStats.getTotalRequest());
                                ratio = new BigDecimal(1).subtract(total404.divide(totalReq, 2, BigDecimal.ROUND_HALF_UP));
                                dataJo.put("value", ratio.doubleValue());
                                break;
                            case "notFoundReqNum":
                                // 客户端资源不存在数量：返回状态码为404的请求数量
                                total404 = new BigDecimal(m.codeRequestStats.getTotal404RequestNum());
                                dataJo.put("value", total404.longValue());
                                break;
                            case "othersErrorRate":
                                // 客户端其他错误错误率：客户端其他错误数占总请求数的百分比
                                total4_n = new BigDecimal(m.codeRequestStats.getTotal4_nRequestNum());
                                totalReq = new BigDecimal(m.requestStats.getTotalRequest());
                                ratio = new BigDecimal(1).subtract(total4_n.divide(totalReq, 2, BigDecimal.ROUND_HALF_UP));
                                dataJo.put("value", ratio.doubleValue());
                                break;
                            case "othersErrorReqNum":
                                // 客户端其他错误数量：返回状态码为除403、404之外的其他4xx的请求数量
                                total4_n = new BigDecimal(m.codeRequestStats.getTotal4_nRequestNum());
                                dataJo.put("value", total4_n.longValue());
                                break;
                            default:
                                break;
                            }
                            dataJa.put(dataJo);
                        }
                    }
                    regionJo.put("regionName", k);
                    regionJo.put("order", dataJa);
                    regionsUsageJson.put(regionJo);
                }
                jsonItem.put(usageRequest, regionsUsageJson);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    private static void buildSingleCommonUsageJson(String cName, Map<String, List<MinutesUsageMeta>> regionUsage,
            JSONObject jsonItem, String usageRequest, String jsonUsageItem,
            String ... fieldNames) {
        Field f = null;
        try {
            if (usageRequest.equals(jsonUsageItem)) {
                JSONArray regionsUsageJson = new JSONArray();
                for (String k : regionUsage.keySet()) {
                    JSONObject regionJo = new JSONObject();
                    JSONArray dataJa = new JSONArray();
                    // 用于记录时间参数对应的数组下标，用于合并多个相同时间下的数据
                    Map<String, Integer> timeIndexMap = Maps.newHashMapWithExpectedSize(regionUsage.size());
                    for (MinutesUsageMeta m : regionUsage.get(k)) {
                        Field f0 = m.getClass().getDeclaredField(cName);
                        if (f0.get(m) != null) {
                            int putIndex = timeIndexMap.getOrDefault(m.time, dataJa.length());
                            JSONObject dataJo;
                            long oldValue;
                            if (timeIndexMap.containsKey(m.time)) {
                                dataJo = dataJa.getJSONObject(putIndex);
                                oldValue = dataJo.optLong("value", 0L);
                            } else {
                                dataJo = new JSONObject();
                                oldValue = 0L;
                            }
                            dataJo.put("date", m.time);
                            long value = oldValue;
                            for (String fieldName : fieldNames) {
                                f = f0.get(m).getClass().getDeclaredField(fieldName);
                                if (f != null) {
                                    value += f.getLong(f0.get(m));
                                }
                            }
                            dataJo.put("value", value);
                            dataJa.put(putIndex, dataJo);
                            timeIndexMap.put(m.time, putIndex);
                        }
                    }
                    regionJo.put("regionName", k);
                    regionJo.put("order", dataJa);
                    regionsUsageJson.put(regionJo);
                }             
                jsonItem.put(usageRequest, regionsUsageJson);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }
    
    @Deprecated
    private static void buildSingleConnectionsJson(Map<String, List<MinutesUsageMeta>> regionUsage,
            JSONObject jsonItem, String usageRequest, String jsonUsageItem, String fieldName1, String fieldName2) {
        Field f1 = null, f2 = null;
        try {
            f1 = MinutesUsageMeta.Connection.class.getDeclaredField(fieldName1);
            if (fieldName2 != null) {
                f2 = MinutesUsageMeta.Connection.class.getDeclaredField(fieldName2);
            }
            if (usageRequest.equals(jsonUsageItem)) {
                JSONArray regionsUsage = new JSONArray();
                for (String region : regionUsage.keySet()) {
                    JSONObject regionJo = new JSONObject();
                    JSONArray dataJa = new JSONArray();
                    // 用于记录时间参数对应的数组下标，用于合并多个相同时间下的数据
                    Map<String, Integer> timeIndexMap = Maps.newHashMapWithExpectedSize(regionUsage.size());
                    for (MinutesUsageMeta m : regionUsage.get(region)) {
                        int putIndex = timeIndexMap.getOrDefault(m.time, dataJa.length());
                        JSONObject dataJo;
                        long oldValue;
                        if (timeIndexMap.containsKey(m.time)) {
                            dataJo = dataJa.getJSONObject(putIndex);
                            oldValue = dataJo.optLong("value", 0);
                        } else {
                            dataJo = new JSONObject();
                            oldValue = 0L;
                        }
                        dataJo.put("date", m.time);
                        if (f1 != null) {
                            if (f2 != null) {
                                dataJo.put("value", oldValue + f1.getLong(m.connection) + f2.getLong(m.connection));
                            } else {
                                dataJo.put("value", oldValue + f1.getLong(m.connection));
                            }
                        }
                        dataJa.put(putIndex, dataJo);
                        timeIndexMap.put(m.time, putIndex);
                    }
                    regionJo.put("regionName", region);
                    regionJo.put("order", dataJa);
                    regionsUsage.put(regionJo);
                }
                jsonItem.put(usageRequest, regionsUsage);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }
    
    /**
     * 解析带宽95峰值统计信息的查询，获取统计项json
     * @param ownerId
     * @param beginDate
     * @param endDate
     * @param peakBWStats
     * @param bucketName
     * @return
     * @throws Exception 
     */
    public static String get95PeakBWStatsInJson(Long ownerId, String beginDate, String endDate, String peakBWStats, String bucketName, String storageType) throws Exception {
        return get95PeakBWStats(ownerId, beginDate, endDate, peakBWStats, bucketName, "json", storageType);
    }
    
    /**
     * 获取带宽95峰值统计Csv下载项
     * @param ownerId
     * @param beginDate
     * @param endDate
     * @param peakBWStats
     * @param bucketName
     * @return
     * @throws Exception
     */
    public static String get95PeakBWStatsInCsv(Long ownerId, String beginDate, String endDate, UsageType usageType, String bucketName, String storageType) throws Exception {
        return get95PeakBWStats(ownerId, beginDate, endDate, null, bucketName, "csv", storageType);
    }

    private static String get95PeakBWStats(Long ownerId, String beginDate, String endDate, String peakBWStats, String bucketName, String resultForm, String storageType) throws Exception {
        Map<Long,List<String>> peakBWOwnerRegions = Utils.get95PeakBWPermittedOwnerRegions(); //配置文件中所有用户带宽95数据域
        List<String> regions = new ArrayList<String>();//peakBWOwnerRegions.get(ownerId);
        regions.add("Zw");
        regions.add("ShenZhen");
        Set<String> userRegions = client.getDataRegions(ownerId);
        Map<String, List<MinutesUsageMeta>> region2PeakBW = new HashMap<>();
        UsageMetaType type;
        if (isAccountLevel(bucketName)) { //用户级别
            type = UsageMetaType.MONTH_OWNER;
            bucketName = null;
        }
        else { //bucket级别
            type = UsageMetaType.MONTH_BUCKET;
        }
        if (regions != null && regions.size() > 0) {
            for (String region : regions) {
                if (userRegions.contains(region)) {
                    List<MinutesUsageMeta> peakBWMetas = UsageStatsQuery.list95PeakBW(region, ownerId, bucketName, beginDate, endDate, storageType);
                    if (peakBWMetas != null && peakBWMetas.size() > 0) {
                        region2PeakBW.put(region, peakBWMetas);
                    }
                }
            }
        } else {
            return null;
        }
        if(resultForm.equals("json"))
            return build95PeakBWStatsJson(peakBWStats, region2PeakBW, beginDate, endDate, type); 
        else if(resultForm.equals("csv"))
            return build95PeakBWStatsCsv(UsageType.PEAK_BANDWIDTH_95, region2PeakBW, type, beginDate, endDate);
        return null;
    }
    
    /**
     * 构建带宽95峰值统计项json
     * @param usageStats
     * @param region2usage
     * @param type
     * @return
     * @throws JSONException
     * @throws ParseException 
     */
    private static String build95PeakBWStatsJson(String usageStats,
            Map<String, List<MinutesUsageMeta>> region2usage,
            String startTime, String endTime,
            UsageMetaType type) throws JSONException, ParseException {
        //获取请求query参数，多个统计项以逗号分隔
        String[] usageArr = usageStats.split(",");
        JSONObject usageItem = new JSONObject();
        for (int i = 0; i < usageArr.length; i++) {
            buildSingle95PeakBWJson(region2usage, usageArr, usageItem, i, "uploadPeakBW", "pubPeakUpBW", startTime, endTime);
            buildSingle95PeakBWJson(region2usage, usageArr, usageItem, i, "transferPeakBW", "pubPeakTfBW", startTime ,endTime);
        }
        return usageItem.toString();
    }

    /**
     * 构建带宽95峰值统计项csv
     *
     * @param type
     * @param region2usage
     * @param usageMetaType
     * @return
     * @throws JSONException
     * @throws BaseException
     */
    private static String build95PeakBWStatsCsv(UsageType type,
                                                Map<String, List<MinutesUsageMeta>> region2usage,
                                                UsageMetaType usageMetaType, String beginDate, String endDate) throws JSONException, BaseException {
        if ((type.equals(UsageType.PEAK_BANDWIDTH_95))
                && (!usageMetaType.equals(UsageMetaType.MONTH_OWNER)
                && !usageMetaType.equals(UsageMetaType.MONTH_BUCKET))) {
            throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_ARGUMENT, ErrorMessage.ERROR_CODE_INVALID_ARGUMENT);
        }
        return CSVUtils.createCSV(region2usage, null, type, usageMetaType, null, true, beginDate, endDate);
    }
    
    /**
     * 带宽95权限json
     * @param region2usage
     * @param usageArr
     * @param usageItem
     * @param i
     * @param jsonUsageItem
     * @param fieldName1
     * @throws ParseException 
     */
    private static void buildSingle95PeakBWJson( 
            Map<String, List<MinutesUsageMeta>> region2usage, String[] usageArr,
            JSONObject usageItem, int i, String jsonUsageItem,
            String fieldName1, String beginDate, String endDate) throws ParseException {
        Field f1 = null;
        Date startTime = Misc.formatyyyymm(beginDate);
        Date endTime = Misc.formatyyyymm(endDate);
        Calendar calendarStart = Calendar.getInstance();
        Calendar calendarEnd = Calendar.getInstance();
        
        try {
            f1 = PeakBandWidth.class.getDeclaredField(fieldName1);
            if (usageArr[i].equals(jsonUsageItem)) {
                JSONArray regionsUsage = new JSONArray();
                for (String region : region2usage.keySet()) {
                    calendarStart.setTime(startTime);
                    calendarEnd.setTime(endTime);
                    JSONObject regionJo = new JSONObject();
                    JSONArray dataJa = new JSONArray();
                    Map<String, Double> values = new HashMap<>();
                    for (MinutesUsageMeta m : region2usage.get(region)) {
                        if (m.peakBandWidth != null) {
                            values.put(m.time, Double.valueOf(f1.get(m.peakBandWidth).toString()));
                        }
                    }
                    while (calendarStart.compareTo(calendarEnd) <= 0) {
                        String time = Misc.formatyyyymm(calendarStart.getTime());
                        JSONObject dataJo = new JSONObject();
                        dataJo.put("date", time);
                        if (values.get(time) != null)
                            dataJo.put("value", values.get(time));
                        else
                            dataJo.put("value", ""); //没有数据的时间返回空
                        dataJa.put(dataJo);
                        calendarStart.add(Calendar.MONTH, 1);
                    }
                    regionJo.put("regionName", region);
                    regionJo.put("order", dataJa);
                    regionsUsage.put(regionJo);
                }
                usageItem.put(usageArr[i], regionsUsage);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }
    
  
    
    /**
     * 返回查询粒度及查询级别类型
     * @param bucket
     * @param freq
     * @return DAY_OWNER 按天的用户统计，DAY_BUCKET 按天的bucket统计，MINUTES_OWNER 按5分钟的用户统计，MINUTES_BUCKET 按5分钟的bucket统计
     */
    public static UsageMetaType getUsageStatsQueryType(String bucket, String freq) {
        UsageMetaType type = UsageMetaType.DAY_OWNER;
        if (isAccountLevel(bucket)) { // owner级别
            switch (freq) {
            case Utils.BY_DAY:
                type = UsageMetaType.DAY_OWNER;
                break;
            case Utils.BY_5_MIN:
                type = UsageMetaType.MINUTES_OWNER;
                break;
            case Utils.BY_HOUR:
                type = UsageMetaType.HOUR_OWNER;
                break;
            default:
                break;
            }
        } else { // bucket级别
            switch (freq) {
            case Utils.BY_DAY:
                type = UsageMetaType.DAY_BUCKET;
                break;
            case Utils.BY_5_MIN:
                type = UsageMetaType.MINUTES_BUCKET;
                break;
            case Utils.BY_HOUR:
                type = UsageMetaType.HOUR_BUCKET;
                break;
            default:
                break;
            }
        }
        return type;
    }
    
    public static boolean isAccountLevel(String bucketName) {
        if (bucketName == null || bucketName.length() == 0) {
            return true;
        }
        return false;
    }

    /**
     * 检查时间是否符合要求，需要yyyy-mm-dd格式，且begin与end之间不能超过maxQueryNum天
     * @param beginDate
     * @param endDate
     * @param maxQueryNum
     * @throws Exception
     */
    private static void checkValidDateByDay(String beginDate, String endDate, String freq) throws Exception {
        //TODO 是否可以删除
        if(!Utils.dayFormatIsValid(beginDate)) {
            throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_ARGUMENT, "Invalid BeginDate.");
        }
        if(!Utils.dayFormatIsValid(endDate)) {
            throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_ARGUMENT, "Invalid EndDate.");
        }        
        Date begin = null;
        Date end = null;
        //这种判断方式并不能筛选出2020-08-07格式的数据，即使是2020-08-07 10:00格式的数据也不会抛出异常
        try {
            begin = Misc.formatyyyymmdd(beginDate);
        } catch (Exception e) {
            throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_ARGUMENT, "Invalid BeginDate.");
        }
        try {
            end = Misc.formatyyyymmdd(endDate);
        } catch (Exception e) {
            throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_ARGUMENT, "Invalid EndDate.");
        }
        // beginDate早于endDate
        if (begin.after(end))
            throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_ARGUMENT, "Invalid BeginDate and EndDate.");
        int maxQueryNum = 7;
        switch (freq) {
        case Utils.BY_5_MIN:
            maxQueryNum = 1;
            break;
        case Utils.BY_HOUR:
            maxQueryNum = 7;
            break;
        case Utils.BY_DAY:
            maxQueryNum = 30;
            break;
        }
        // 最多查询天数
        Date dateMax = DateUtils.addDays(begin, maxQueryNum);
        if (!dateMax.after(end)) {
            throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_ARGUMENT, "Invalid BeginDate and EndDate.");
        }
    }

    /**
     * timeZone默认为+8
     * @param timeZone
     * @return
     */
//    private static String getTimeZone(String timeZone) {
//        if (timeZone == null || timeZone.length() == 0)
//            timeZone = "utc+8:00";
//        return timeZone;
//    }

    /**
     * 解析时区信息，获得查询开始时间与截止时间
     * @param ownerId
     * @param buckets
     * @param bucketList
     * @param regions
     * @param regionList
     * @param freq
     * @param timeZone
     * @throws Exception
     */
//    private static Pair<String, String> parseDateByTimeZone(String beginDate, String endDate, String timeZone) throws Exception {
//        //解析时区信息，将beginDate、endDate转换为服务器本地时间
//        String offset = timeZone.split("utc")[1];
//        int hour = Integer.valueOf(offset.substring(1).split(":")[0]);
//        int minutes = Integer.valueOf(offset.substring(1).split(":")[1]);
//        LocalTime lt = LocalTime.of(hour, minutes);
//        offset = offset.substring(0, 1) + lt.toString();
//        ZoneOffset zf = ZoneOffset.of(offset);
//        LocalDate beginLDT = LocalDate.parse(beginDate);
//        OffsetDateTime offDT = beginLDT.atStartOfDay().atOffset(zf);
//        beginDate = offDT.atZoneSameInstant(ZoneOffset.systemDefault()).toLocalDate().toString();
//        LocalDate endLDT = LocalDate.parse(endDate);
//        offDT = endLDT.atStartOfDay().atOffset(zf);
//        endDate = offDT.atZoneSameInstant(ZoneOffset.systemDefault()).toLocalDate().toString();
//        return new Pair<String, String>(beginDate, endDate);
//    }

    /**
     * 如果region为空，则查询global的数据
     * @param ownerId
     * @param regionList
     * @param region
     * @throws IOException
     * @throws BaseException
     * @throws JDOMException
     */
    private static void getRegionList(long ownerId, List<String> regionList, String region) throws BaseException {
        if (region == null || region.length() == 0) {
            regionList.addAll(DataRegions.getAllRegions());
            regionList.add(Consts.GLOBAL_DATA_REGION);
        } else {
            if (DataRegions.getAllRegions().contains(region)) {
                regionList.add(region);
            } else {
                throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_ARGUMENT, "Invalid regionName");
            } 
        }
    }
    
    public static void checkBucketExist(long ownerId, String bucketName) throws BaseException, IOException {
        if (bucketName != null && bucketName.trim().length() != 0) {
            BucketMeta dbBucket = new BucketMeta(bucketName);
            if (!client.bucketSelect(dbBucket))
                throw new BaseException(404, ErrorMessage.ERROR_CODE_NO_SUCH_BUCKET, ErrorMessage.ERROR_CODE_NO_SUCH_BUCKET);
            if(dbBucket.ownerId != ownerId)
                throw new BaseException(403, ErrorMessage.ERROR_CODE_403, ErrorMessage.ERROR_CODE_403);
        }
    }

    public static String getLastFillToDate(String region) throws Exception {
        return client.minutesUsageSelectLastFillDate(region);
    }

    public static long getLastPeriodTime() throws Exception {
        return client.minutesUsageSelectLastPeriodTime();
    }
    
    public static void putLastFillToDate(String region, String date) throws Exception {
        client.minutesUsageInsertLastFillDate(region ,date);
    }
    
    public static void putLastPeriodTime(long time) throws Exception {
        client.minutesUsageInsertLastPeriodTime(time);
    }
    
    
    public static boolean deleteOwnerUsage(long ownerid, String regionName, int cacheSize, long sleepTime) throws Exception {
        List<UsageMetaType> usageMetaTypes = Lists.newArrayList(UsageMetaType.DAY_OWNER, UsageMetaType.DAY_BUCKET, UsageMetaType.MINUTES_OWNER, UsageMetaType.MINUTES_BUCKET,
                UsageMetaType.HOUR_OWNER, UsageMetaType.HOUR_BUCKET, UsageMetaType.MONTH_BUCKET, UsageMetaType.MONTH_OWNER, UsageMetaType.CURRENT_BUCKET, UsageMetaType.CURRENT_OWNER);
        for (UsageMetaType usageMetaType : usageMetaTypes) {
            String startRow = regionName + "|" + usageMetaType.toString() + "|" + ownerid + "|" + Character.MIN_VALUE;
            String stopRow = regionName + "|" + usageMetaType.toString() + "|" + ownerid + "|" + Character.MAX_VALUE;
            client.deleteAllOwnerUsageMetas(startRow, stopRow, cacheSize, sleepTime);
        }
        return true;
    }
    
    public static MinutesUsageMeta lastMinutesUsageCommonStats(String regionName, long userId, String bucketName, UsageMetaType type, String endTime, String storageType) throws Exception {
        LocalDate lastFillToDate = LocalDate.parse(PeriodUsageStatsConfig.updateDate, Misc.format_uuuu_MM_dd).minusDays(2);
        LocalDate endDate = LocalDate.parse(endTime, Misc.format_uuuu_MM_dd);
        int intervalDays  = PeriodUsageStatsConfig.intervalDays;
        LocalDate startDate = endDate.minusDays(intervalDays);        
        List<MinutesUsageMeta> usages = null;
        while (endDate.isAfter(lastFillToDate)) {
            String startTime = startDate.format(Misc.format_uuuu_MM_dd);
            String endTimeTemp = endDate.format(Misc.format_uuuu_MM_dd) + Character.MAX_VALUE;
            usages = client.minutesUsageCommonStatsList(regionName, userId, bucketName, type, startTime, endTimeTemp, storageType);
            if (usages != null && usages.size() != 0) {
                break;
            }
            endDate = endDate.minusDays(intervalDays);
            startDate = startDate.minusDays(intervalDays);
        }
        if (usages != null && usages.size() > 0) {
            return usages.get(usages.size()-1);
        }
        return null;
    }
    
    static class SelectUsageTask implements Callable<List<MinutesUsageMeta>>{
        UsageMetaType type;
        long ownerId;
        String region;
        String bucketName;
        String beginDate;
        String endDate;
        String storageType = Consts.STORAGE_CLASS_STANDARD; // 默认使用标准存储作为查询参数
        boolean ifDontNeedFill = false;
        
        public SelectUsageTask(UsageMetaType type, long ownerId, String region, String bucketName, String beginDate, String endDate) {
            this.type = type;
            this.ownerId = ownerId;
            this.region = region;
            this.bucketName = bucketName;
            this.beginDate = beginDate;
            this.endDate = endDate;
        }
        
        public SelectUsageTask(UsageMetaType type, long ownerId, String region, String bucketName, String beginDate, String endDate, boolean ifDontNeedFill) {
            this.type = type;
            this.ownerId = ownerId;
            this.region = region;
            this.bucketName = bucketName;
            this.beginDate = beginDate;
            this.endDate = endDate;
            this.ifDontNeedFill = ifDontNeedFill;
        }

        public SelectUsageTask(UsageMetaType type, long ownerId, String region, String bucketName, String beginDate, String endDate, String storageType, boolean ifDontNeedFill) {
            this.type = type;
            this.ownerId = ownerId;
            this.region = region;
            this.bucketName = bucketName;
            this.beginDate = beginDate;
            this.endDate = endDate;
            this.storageType = storageType;
            this.ifDontNeedFill = ifDontNeedFill;
        }

        @Override
        public List<MinutesUsageMeta> call() throws Exception {
            List<MinutesUsageMeta> list = null;
            String today = Misc.formatyyyymmdd(new Date());
            boolean isToday = endDate.startsWith(today);
            String beginTime = null;
            String endTime = null;
//            String endTimeForABW = null;
            //传入的查询时间为按天粒度时增加到分钟粒度，传入的时间为分钟粒度时不需要增加
            if (endDate.length() == 10) {
                if (isToday) {
                    Instant instant = Instant.ofEpochMilli(getLastPeriodTime());
                    LocalDateTime ldt = LocalDateTime.ofInstant(instant, ZoneId.systemDefault());
                    endTime = ldt.format(Misc.format_uuuu_MM_dd_HH_mm);
                } else {
                    //endTime改为第二天00:00 为了兼容旧统计数据有00:00的，新版改为24:00
                    endTime = LocalDate.parse(endDate, Misc.format_uuuu_MM_dd).plusDays(1).toString().concat(" 00:00");
//                    endTimeForABW = endDate.concat(" 24:00");
                }
            } else {
                endTime = endDate;
            }
            if (beginDate.length() == 10) {
                beginTime = beginDate.concat(" 00:00");
            } else {
                beginTime = beginDate;
            }
            switch (type) {
                case DAY_OWNER:
                beginTime = beginDate;
                endTime = endDate;
                list = UsageStatsQuery.listOwerUsagePerDay(region, ownerId, beginTime, endTime + Character.MAX_VALUE, storageType);
                return fillInavtiveBySt(UsageMetaType.DAY_OWNER, Utils.BY_DAY, beginTime, endTime, list, storageType);
                case DAY_BUCKET:
                    beginTime = beginDate;
                    endTime = endDate;
                    list = UsageStatsQuery.listBucketUsagePerDay(region, ownerId, bucketName, beginTime, endTime + Character.MAX_VALUE, storageType);
                    return fillInavtiveBySt(UsageMetaType.DAY_BUCKET, Utils.BY_DAY, beginTime, endTime, list, storageType);
                case HOUR_OWNER:
                    list = UsageStatsQuery.listOwerUsagePerHour(region, ownerId, beginTime, endTime + Character.MAX_VALUE, storageType);
                    return fillInavtiveBySt(UsageMetaType.DAY_OWNER, Utils.BY_HOUR, beginTime, endTime, list, storageType);
                case HOUR_BUCKET:
                    list = UsageStatsQuery.listBucketUsagePerHour(region, ownerId, bucketName, beginTime, endTime + Character.MAX_VALUE, storageType);
                    return fillInavtiveBySt(UsageMetaType.DAY_BUCKET, Utils.BY_HOUR, beginTime, endTime, list, storageType);
                case MINUTES_OWNER:
                    list = UsageStatsQuery.listOwerUsagePer5Minutes(region, ownerId, beginTime, endTime + Character.MAX_VALUE, storageType);
                    return fillInavtiveBySt(UsageMetaType.DAY_OWNER, Utils.BY_5_MIN, beginTime, endTime, list, storageType);
                case MINUTES_BUCKET:
                    list = UsageStatsQuery.listBucketUsagePer5Minutes(region, ownerId, bucketName, beginTime, endTime + Character.MAX_VALUE, storageType);
                    return fillInavtiveBySt(UsageMetaType.DAY_BUCKET, Utils.BY_5_MIN, beginTime, endTime, list, storageType);
                case MINUTES_AVAIL_BW:
                    list = UsageStatsQuery.listAvailableBandwidth(region, beginTime, endTime + Character.MAX_VALUE);
                return list;
                default:
                    return null;
            }
        }

        /**
         * 补充查询的日期间隔中缺少的非活跃数据
         *
         * @param lastMetaType
         * @param freq
         * @param beginTime
         * @param endTime
         * @param metas
         * @return
         * @throws Exception
         */
        private List<MinutesUsageMeta> fillInavtive(UsageMetaType lastMetaType, String freq, String beginTime, String endTime, List<MinutesUsageMeta> metas, String storageType) throws Exception {
            Map<String, MinutesUsageMeta> usageMap = new TreeMap<>();
            List<MinutesUsageMeta> fullMetas = new ArrayList<>();
            for (MinutesUsageMeta meta : metas) {
                if (freq.equals(Utils.BY_5_MIN) || freq.equals(Utils.BY_HOUR)) {
                    String t = LocalDateTime.parse(meta.time, Misc.format_uuuu_MM_dd_HH_mm).format(Misc.format_uuuu_MM_dd_HH_mm);
                    MinutesUsageMeta oldMeta = usageMap.get(t);
                    if (oldMeta != null) {
                        oldMeta.checkAndinitCommonStats();
                        if (meta.sizeStats != null) {
                            oldMeta.sizeStats.sumAll(meta.sizeStats);
                        }
                        if (meta.requestStats != null) {
                            oldMeta.requestStats.sumAll(meta.requestStats);
                        }
                        if (meta.flowStats != null) {
                            oldMeta.flowStats.sumAll(meta.flowStats);
                        }
                        if (meta.codeRequestStats != null) {
                            oldMeta.codeRequestStats.sumAll(meta.codeRequestStats);
                        }
                        if(oldMeta.connection == null)
                            oldMeta.connection = new Connection();
                        if(meta.connection != null) {                            
                            oldMeta.connection.sum(meta.connection);
                        }                        
                    } else {
                        usageMap.put(t, meta);
                    }
                } else {
                    usageMap.put(meta.time, meta);
                }
            }
            LocalDateTime begin;
            LocalDateTime end;
            if (beginTime.length() == 10) {
                begin = LocalDate.parse(beginTime).atTime(0, 0, 0, 0);
            } else {
                begin = LocalDateTime.parse(beginTime, Misc.format_uuuu_MM_dd_HH_mm);
            }
            if (endTime.length() == 10) {
                end = LocalDate.parse(endTime).atTime(0, 0, 0, 0);
            } else {
                end = LocalDateTime.parse(endTime, Misc.format_uuuu_MM_dd_HH_mm);
            }
            // 如果结束时间大于当前时间，只补充到上次统计运行时间
            if (end.isAfter(LocalDateTime.now())) {
                Instant instant = Instant.ofEpochMilli(getLastPeriodTime());
                end = LocalDateTime.ofInstant(instant, ZoneId.systemDefault());
            }
            MinutesUsageMeta lastMeta = usageMap.get(beginTime);
            if (lastMeta == null) {
                // 如果第一个点没有数据，则获取第一个点以前的最近的值
                lastMeta = lastMinutesUsageCommonStats(region, ownerId, bucketName, lastMetaType, begin.minusDays(1).toLocalDate().format(Misc.format_uuuu_MM_dd), storageType);
                if (lastMeta == null) {
                    if (metas.size() == 0 && ifDontNeedFill) {
                        return fullMetas;
                    }
                    lastMeta = new MinutesUsageMeta(lastMetaType, region, ownerId, bucketName, begin.toLocalDate().format(Misc.format_uuuu_MM_dd), storageType);
                    lastMeta.initStatsExceptConnection();
                } else {
                    lastMeta.clearCommonStatsExceptSize();
                }
            }
            //五分钟类型的数据，可能会存在api插入了有连接数但是没有容量的情况
            if (lastMeta.sizeStats == null)
                lastMeta.sizeStats = new SizeStats();

            if (freq.equals(Utils.BY_5_MIN)) {
                //当天不再有00:00的数据，所以从下一个数据开始
                if (begin.getHour() == 0 && begin.getMinute() == 0) {
                    begin = begin.plusMinutes(5);
                }
                while (!begin.isAfter(end)) {
                    String beginTemp = begin.format(Misc.format_uuuu_MM_dd_HH_mm);
                    MinutesUsageMeta m = usageMap.get(beginTemp);
                    if (m == null) {
                        //00:00改为24:00
                        if (begin.getHour() == 0 && begin.getMinute() == 0) {
                            beginTemp = begin.minusDays(1).format(Misc.format_uuuu_MM_dd_HH_mm).replace(" 00:00", " 24:00");
                        }
                        m = new MinutesUsageMeta(type, region, ownerId, bucketName, beginTemp, storageType);
                    } else {
                        if (m.time.contains("00:00")) {
                            m.time = LocalDateTime.parse(m.time, Misc.format_uuuu_MM_dd_HH_mm).minusDays(1).
                                    format(Misc.format_uuuu_MM_dd_HH_mm).replace(" 00:00", " 24:00");
                        }
                    }
                    // 五分钟类型的数据，可能会存在api插入了有连接数但是没有容量的情况
                    if (m.sizeStats == null) {
                        m.initCommonStats();
                        m.sizeStats.sumExceptPeakAndAvgAndRestore(lastMeta.sizeStats);
                        m.sizeStats.size_peak = m.sizeStats.size_avgTotal = m.sizeStats.size_total;
                        m.sizeStats.size_originalPeak = m.sizeStats.size_avgOriginalTotal = m.sizeStats.size_originalTotal;
                        m.sizeStats.size_avgAlin = m.sizeStats.size_alin;
                        m.sizeStats.size_avgRedundant = m.sizeStats.size_redundant;
                        m.sizeStats.size_completePeak = m.sizeStats.size_completeSize;
                    }
                    fullMetas.add(m);
                    lastMeta = m;
                    begin = begin.plusMinutes(5);
                }
            } else if (freq.equals(Utils.BY_HOUR)) {
                if (begin.getHour() == 0 && begin.getMinute() == 0) {
                    begin = begin.plusHours(1);
                }
                while (!begin.isAfter(end)) {
                    String beginTemp = begin.format(Misc.format_uuuu_MM_dd_HH_mm);
                    MinutesUsageMeta m = usageMap.get(beginTemp);
                    if (m == null) {
                        if (begin.getHour() == 0 && begin.getMinute() == 0) {
                            beginTemp = begin.minusDays(1).format(Misc.format_uuuu_MM_dd_HH_mm).replace(" 00:00", " 24:00");
                        }
                        m = new MinutesUsageMeta(type, region, ownerId, bucketName, beginTemp, storageType);
                        m.initCommonStats();
                        m.sizeStats.sumExceptPeakAndAvgAndRestore(lastMeta.sizeStats);
                        m.sizeStats.size_peak = m.sizeStats.size_avgTotal = m.sizeStats.size_total;
                        m.sizeStats.size_originalPeak = m.sizeStats.size_avgOriginalTotal = m.sizeStats.size_originalTotal;
                        m.sizeStats.size_avgAlin = m.sizeStats.size_alin;
                        m.sizeStats.size_avgRedundant = m.sizeStats.size_redundant;
                        m.sizeStats.size_completePeak = m.sizeStats.size_completeSize;
                    }
                    fullMetas.add(m);
                    lastMeta = m;
                    begin = begin.plusHours(1);
                }
            } else if (freq.equals(Utils.BY_DAY)) {
                while (!begin.isAfter(end)) {
                    MinutesUsageMeta m = usageMap.get(begin.format(Misc.format_uuuu_MM_dd_HH_mm).split(" ")[0]);
                    if (m == null) {
                        m = new MinutesUsageMeta(type, region, ownerId, bucketName, begin.toLocalDate().toString(), storageType);
                        m.timeVersion = begin.toEpochSecond(ZoneOffset.of("+8"));
                        m.initCommonStats();
                        m.sizeStats.sumExceptPeakAndAvgAndRestore(lastMeta.sizeStats);
                        m.sizeStats.size_peak = m.sizeStats.size_avgTotal = m.sizeStats.size_total;
                        m.sizeStats.size_originalPeak = m.sizeStats.size_avgOriginalTotal = m.sizeStats.size_originalTotal;
                        m.sizeStats.size_avgAlin = m.sizeStats.size_alin;
                        m.sizeStats.size_avgRedundant = m.sizeStats.size_redundant;
                        m.sizeStats.size_completePeak = m.sizeStats.size_completeSize;
                    }
                    fullMetas.add(m);
                    lastMeta = m;
                    begin = begin.plusDays(1);
                }
            }
            return fullMetas;
        }

        private List<MinutesUsageMeta> fillInavtiveBySt(UsageMetaType lastMetaType, String freq, String beginTime, String endTime, List<MinutesUsageMeta> metas, String storageType) throws Exception {
            if (Consts.ALL.equals(storageType)) {
                List<MinutesUsageMeta> standard = fillInavtive(lastMetaType, freq, beginTime, endTime, metas.stream().filter(m -> Consts.STORAGE_CLASS_STANDARD.equals(m.storageType)).collect(Collectors.toList()), Consts.STORAGE_CLASS_STANDARD);
                List<MinutesUsageMeta> standard_ia = fillInavtive(lastMetaType, freq, beginTime, endTime, metas.stream().filter(m -> Consts.STORAGE_CLASS_STANDARD_IA.equals(m.storageType)).collect(Collectors.toList()), Consts.STORAGE_CLASS_STANDARD_IA);
                standard.addAll(standard_ia);
                return standard;
            }
            return fillInavtive(lastMetaType, freq, beginTime, endTime, metas.stream().filter(m -> storageType.equals(m.storageType)).collect(Collectors.toList()), storageType);
        }
    }
}
