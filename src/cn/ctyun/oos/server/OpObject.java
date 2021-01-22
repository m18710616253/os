package cn.ctyun.oos.server;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.logging.log4j.ThreadContext;

import com.amazonaws.AmazonClientException;
import com.amazonaws.HttpMethod;
import com.amazonaws.http.HttpMethodName;
import com.amazonaws.services.s3.Headers;
import com.amazonaws.services.s3.internal.Constants;
import com.amazonaws.services.s3.internal.ServiceUtils;
import com.amazonaws.services.s3.internal.XmlWriter;
import com.amazonaws.services.s3.model.CtyunBucketDataLocation;
import com.amazonaws.services.s3.model.CtyunBucketDataLocation.CtyunBucketDataScheduleStrategy;
import com.amazonaws.services.s3.model.CtyunBucketDataLocation.CtyunBucketDataType;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.ResponseHeaderOverrides;
import com.amazonaws.util.json.JSONArray;
import com.amazonaws.util.json.JSONObject;
import com.emc.esu.api.Permission;

import cn.ctyun.common.BaseException;
import cn.ctyun.common.Consts;
import cn.ctyun.common.Misc;
import cn.ctyun.common.ReplicaMode;
import cn.ctyun.common.conf.DataRegion;
import cn.ctyun.common.conf.OOSConfig;
import cn.ctyun.common.conf.StorageClassConfig;
import cn.ctyun.common.model.BucketLifecycleConfiguration;
import cn.ctyun.common.model.BucketLifecycleConfiguration.Rule;
import cn.ctyun.common.region.MetaRegionMapping;
import cn.ctyun.oos.common.BandWidthControlInputStream;
import cn.ctyun.oos.common.BandWidthControlStream;
import cn.ctyun.oos.common.BandWidthControlStream.ActualExpectedRatio;
import cn.ctyun.oos.common.ErrorMessage;
import cn.ctyun.oos.common.SendEtagUnmatchMQ;
import cn.ctyun.oos.common.SignableInputStream;
import cn.ctyun.oos.common.UserBandWidthLimit;
import cn.ctyun.oos.common.Utils;
import cn.ctyun.oos.common.XmlResponseSaxParser;
import cn.ctyun.oos.hbase.MetaClient;
import cn.ctyun.oos.metadata.BucketMeta;
import cn.ctyun.oos.metadata.InitialUploadMeta;
import cn.ctyun.oos.metadata.ObjectMeta;
import cn.ctyun.oos.metadata.ObjectMeta.VersionType;
import cn.ctyun.oos.metadata.OwnerMeta;
import cn.ctyun.oos.metadata.UploadMeta;
import cn.ctyun.oos.metadata.UserOstorWeightMeta;
import cn.ctyun.oos.ostor.OstorClient;
import cn.ctyun.oos.ostor.OstorException;
import cn.ctyun.oos.ostor.OstorProxy;
import cn.ctyun.oos.ostor.OstorProxy.OstorObjectMeta;
import cn.ctyun.oos.server.InternalClient.WriteResult;
import cn.ctyun.oos.server.backoff.Backoff;
import cn.ctyun.oos.server.backoff.BandwidthBackoff;
import cn.ctyun.oos.server.conf.BackoffConfig;
import cn.ctyun.oos.server.conf.ContentSecurityConfig;
import cn.ctyun.oos.server.count.Pin;
import cn.ctyun.oos.server.count.PinData;
import cn.ctyun.oos.server.formpost.FormField;
import cn.ctyun.oos.server.formpost.FormPostHeaderNotice;
import cn.ctyun.oos.server.formpost.FormPostTool;
import cn.ctyun.oos.server.formpost.PolicyManager;
import cn.ctyun.oos.server.log.BucketLog;
import cn.ctyun.oos.server.processor.Processor.ProcessorException;
import cn.ctyun.oos.server.processor.TextProcessor;
import cn.ctyun.oos.server.processor.image.ImageEvent.ImageParams;
import cn.ctyun.oos.server.processor.image.ImageProcessor;
import cn.ctyun.oos.server.storage.EtagMaker;
import cn.ctyun.oos.server.storage.Storage;
import cn.ctyun.oos.server.util.HttpLimitParser;
import cn.ctyun.oos.server.util.HttpUtils;
import common.MimeType;
import common.threadlocal.ThreadLocalBytes;
import common.time.TimeUtils;
import common.tuple.Pair;
import common.tuple.Triple;
import common.util.BlockingExecutor;
import common.util.HexUtils;

/**
 * @author: Cui Meng
 */
public class OpObject {
    private static final String LOCAL_REGION_NAME = DataRegion.getRegion().getName();
    private static final Log log = LogFactory.getLog(OpObject.class);
    protected static final com.amazonaws.util.DateUtils dateUtils = new com.amazonaws.util.DateUtils();
    private static BandwidthBackoff bandwidthBackoff = new BandwidthBackoff();
    private static MetaClient client = MetaClient.getGlobalClient();
    private static InternalClient internalClient = InternalClient.getInstance();
    private static Pattern dataLocationPattern = Pattern.compile("^type=(Local|Specified)," +
                                                                 "(location=(([^,\\s]+,)*[^,\\s]+),){0,1}" + 
                                                                 "scheduleStrategy=(Allowed|NotAllowed)$");
    
    //将队列数置为0，当新任务到达时，不加队列，直接开启新线程进入执行状态。
    static BlockingExecutor continuousInputStreamPool = new BlockingExecutor(OOSConfig.getMinThreads(),
            OOSConfig.getMaxThreads(), OOSConfig.getSmallQueueSize(), OOSConfig.getSmallAliveTime(),
            "ContinuousInputStreamThread");
    static {
        // 启个线程监控带宽的使用量
        new Thread() {
            @Override
            public void run() {
                for (;;) {
                    if (BackoffConfig.getBandwidthBackoff() == 1)
                        bandwidthBackoff = Backoff.learnBandwidth();
                    try {
                        Thread.sleep(BackoffConfig.getCheckInterval());
                    } catch (InterruptedException e) {
                        log.error(e.getMessage(), e);
                    }
                }
            }
        }.start();
    }
    
    public static Triple<Long, Long, Long> getObjectSize(ObjectMeta object)
            throws BaseException, IOException {
        if (object.storageId == null || object.storageId.trim().length() == 0) {// 分段上传未合并
            InitialUploadMeta initialUpload = new InitialUploadMeta(object.metaRegionName, object.bucketName, object.name,
                    null, false);
            if(client.isBusy(initialUpload, OOSConfig.getMaxConcurrencyPerRegionServer()))
                Backoff.backoff();
            client.initialSelectByObjectId(initialUpload);
            if (initialUpload.uploadId == null)
                return new Triple<Long, Long, Long>(0L, 0L, 0L);
            UploadMeta upload = new UploadMeta(object.metaRegionName, initialUpload.uploadId, 0);
            if(client.isBusy(upload, OOSConfig.getMaxConcurrencyPerRegionServer()))
                Backoff.backoff();
            List<UploadMeta> uploads = client.uploadListAllParts(upload, -1,
                    Consts.UPLOAD_PART_MAXNUM);
            long wholeSize = 0;
            long redundantSize = 0;
            long alinSize = 0;
            for (UploadMeta u : uploads) {
                wholeSize += u.size;
                redundantSize += Utils.getRedundantSize(u.size, u.replicaMode);
                alinSize += Utils.getAlinSize(u.size, u.pageSize, u.replicaMode);
            }
            return new Triple<Long, Long, Long>(wholeSize, redundantSize, alinSize);
        } else if (object.ostorId == null || object.ostorId.trim().length() == 0) {// 分段上传已合并
            long wholeSize = 0;
            long redundantSize = 0;
            long alinSize = 0;
            boolean first = true;
            for (Integer i : object.partNum) {
                UploadMeta u = new UploadMeta(object.metaRegionName, object.initialUploadId, i);
                if(first && client.isBusy(u, OOSConfig.getMaxConcurrencyPerRegionServer()))
                    Backoff.backoff();
                first = false;
                client.uploadSelect(u);
                wholeSize += u.size;
                redundantSize += Utils.getRedundantSize(u.size, u.replicaMode);
                alinSize += Utils.getAlinSize(u.size, u.pageSize, u.replicaMode);
            }
            return new Triple<Long, Long, Long>(wholeSize, redundantSize, alinSize);
        } else {// 普通对象
            long size = object.size;
            long redundantSize = Utils.getRedundantSize(size, object.storageClass);
            long alinSize = Utils.getAlinSize(size, object.ostorPageSize, object.storageClass);
            return new Triple<Long, Long, Long>(size, redundantSize, alinSize);
        }
    }

    public static void getObject(HttpServletRequest request, HttpServletResponse response,
            BucketMeta bucket, ObjectMeta object, BucketLog bucketLog, PinData pinData, Pin pin)
            throws BaseException, IOException {
        if (object.versionType != null && object.versionType == VersionType.DELETE) {
            throw new BaseException(404, ErrorMessage.ERROR_CODE_NO_SUCH_KEY, "the request object is " + object.name);
        }
        checkIfCanOperate(request, bucket, object);
        long objectSize = getObjectSize(object).first();
        pinData.jettyAttributes.objectAttribute.objectSize = objectSize;
        String range = request.getHeader(Headers.RANGE);
        Pair<Long, Long> pair = HttpUtils.getRange(range, objectSize);
        long offset = pair.first(), endRange = pair.second();
        if (range != null)
            response.setStatus(206);
        else
            response.setStatus(200);
        setExpirationHeader(response,bucket,object);
        // 多版本的话，响应头增加版本号ID
        Utils.setVersionIdHeaderIfVersioning(request, response, bucket, object, true);
        Utils.setCommonHeader(response,new Date(bucketLog.startTime),bucketLog.requestId);
        if (range != null) {
            if (endRange == -1)
                response.setHeader("Content-Range", "bytes " + offset + "-0" + "/" + objectSize);
            else
                response.setHeader("Content-Range", "bytes " + offset + "-" + endRange + "/"
                        + objectSize);
            response.setHeader("Accept-Ranges", "bytes");
        }
        setResponseHeaderFromObject(object, bucket, response, endRange - offset + 1);
        setResponseHeaderFromRequest(request, response);
        long count = 0;
        boolean hasLimitRate = false;
        int limitRate = Consts.USER_NO_LIMIT_RATE;
        String rate = HttpLimitParser.getLimit(request, Consts.X_AMZ_LIMIT_RATE_INDEX);
        //如果是以x-amz-limit：rate=10形式传递限速
        if(StringUtils.isNotBlank(rate)) {
            hasLimitRate = true;
            limitRate = validLimitRate(rate.replace("rate=", ""));
        }else {
            //如果是以x-amz-limitrate: rate=10传递限速
            if (preSignAndLimitRate(request)) {
                rate = request.getParameter(Consts.X_AMZ_LIMITRATE).replace("rate=", "");
            } else if (StringUtils.isNotBlank(request.getHeader(Consts.X_AMZ_LIMITRATE))) {
                rate = request.getHeader(Consts.X_AMZ_LIMITRATE).replace("rate=", "");
            }
            if(StringUtils.isNotBlank(rate)) {
                hasLimitRate = true;
                limitRate = validLimitRate(rate);
            }
        }
            
        try {
             count = getObject(response, bucket.getName(), object, offset, endRange, range, 
                    bucketLog, request.getRequestURI(), bucket.ownerId, pin, pinData,hasLimitRate,limitRate);
        } finally {
            //如果客戶端下载object出错，记录已下载的文件长度在bucketLog.realResponseLength 中，用来计费
            if(count == 0 && bucketLog.realResponseLength != 0) 
               count = bucketLog.realResponseLength;
            if (object.dataRegion.equals(LOCAL_REGION_NAME)) {
                pinData.setDirectFlow(count, LOCAL_REGION_NAME, object.storageClassString);
            } else {
                pinData.setRoamFlow(count, object.dataRegion, object.storageClassString);
            }
            pinData.jettyAttributes.objectAttribute.length = count;
        }
    }
    
    public static int validLimitRate(String limitRate) throws BaseException {
        int rate = 0;
        try {
            rate = Integer.parseInt(limitRate);
            if (rate < 1) {
                throw new BaseException(400,
                        ErrorMessage.ERROR_CODE_INVALID_ARGUMENT,
                        ErrorMessage.ERROR_MESSAGE_INVALID_X_AMZ_LIMITRATE);
            }
            if(rate < OOSConfig.getMinLimitRate())
                return OOSConfig.getMinLimitRate();
        } catch (NumberFormatException e) {
            throw new BaseException(400,
                    ErrorMessage.ERROR_CODE_INVALID_ARGUMENT,
                    ErrorMessage.ERROR_MESSAGE_INVALID_X_AMZ_LIMITRATE);
        }
        return rate;
    }

    private static String getExpiration(BucketMeta bucket, ObjectMeta object) throws UnsupportedEncodingException {
        if (bucket.lifecycle != null) {
            List<Rule> rules = bucket.lifecycle.getRules();
            for (Rule r : rules) {
                if (r.getStatus().equals(BucketLifecycleConfiguration.ENABLED)
                        && object.name.startsWith(r.getPrefix())) {
                    String date;
                    if (r.getExpirationDate() == null) {
                        Date d = DateUtils.addDays(new Date(object.lastModified),
                                r.getExpirationInDays());
                        date = TimeUtils
                                .toGMTFormat(cn.ctyun.oos.server.util.Misc.timeToNextMidnight(d));
                    } else {
                        date = TimeUtils.toGMTFormat(r.getExpirationDate());
                    }
                    return "expiry-date=\"" + date + "\",rule-id=\"" + URLEncoder.encode(r.getId(), Consts.STR_UTF8)
                            + "\"";

                }
            }
        }
        return "";
    }

    public static void setExpirationHeader(HttpServletResponse response, BucketMeta bucket,
            ObjectMeta object) throws UnsupportedEncodingException {
        String expiration = getExpiration(bucket, object);
        if (StringUtils.isNotBlank(expiration)) {
            response.setHeader(Headers.EXPIRATION,expiration);
        }
    }

    static void setResponseHeaderFromObject(ObjectMeta object, BucketMeta bucket, HttpServletResponse response,long length) throws IOException {
        // set response header from meta data
        if (StringUtils.isNotBlank(object.etag))
            response.setHeader(Headers.ETAG, "\"" + object.etag + "\"");
        if (object.lastModified != 0)
            response.setHeader(Headers.LAST_MODIFIED,
                    dateUtils.formatRfc822Date(new Date(object.lastModified)));
        if (StringUtils.isNotBlank(object.contentType))
            response.setHeader(Headers.CONTENT_TYPE, object.contentType);
        if (StringUtils.isNotBlank(object.cacheControl))
            response.setHeader(Headers.CACHE_CONTROL, object.cacheControl);
        if (StringUtils.isNotBlank(object.contentDisposition))
            response.setHeader(Headers.CONTENT_DISPOSITION, object.contentDisposition);
        if (StringUtils.isNotBlank(object.contentEncoding))
            response.setHeader(Headers.CONTENT_ENCODING, object.contentEncoding);
        if (StringUtils.isNotBlank(object.contentMD5))
            response.setHeader(Headers.CONTENT_MD5, object.contentMD5);
        if (StringUtils.isNotBlank(object.expires))
            response.setHeader(Headers.EXPIRES, object.expires);
        if (StringUtils.isNotBlank(object.websiteRedirectLocation)){
            response.setHeader(Headers.REDIRECT_LOCATION, object.websiteRedirectLocation);
        }

        //从低频存储开始返回storageclass给客户
        if (!Consts.STORAGE_CLASS_STANDARD.equals(object.storageClassString)) {
            response.setHeader(Headers.STORAGE_CLASS, object.storageClassString);
        }
        Properties p = object.getProperties();
        for (Entry<?, ?> e : p.entrySet()) {
            response.setHeader(Headers.S3_USER_METADATA_PREFIX + e.getKey(), (String) e.getValue());
        }
        response.setHeader(Headers.CONTENT_LENGTH, Long.toString(length));
        setResponseLocationHeader(response, bucket.ownerId, bucket.metaLocation, object.dataRegion);
    }
    
    private static boolean preSignAndLimitRate(HttpServletRequest request) {
        return ((request.getParameter("Expires") != null
                || request.getParameter("Signature") != null
                || request.getParameter("AWSAccessKeyId") != null
                || request.getParameter("X-Amz-Expires") != null) 
                && (StringUtils.isNotBlank(request.getParameter(Consts.X_AMZ_LIMITRATE)) || (
                        StringUtils.isNotBlank(request.getParameter(Consts.X_AMZ_LIMIT))
                        && request.getParameter(Consts.X_AMZ_LIMIT).contains(Consts.X_AMZ_LIMIT_RATE_INDEX))));
    }

    private static void setResponseLocationHeader(HttpServletResponse response, long ownerId,
            String metaLocation, String dataLocation) throws IOException {
        Pair<Set<String>, Set<String>> regions = client.getRegions(ownerId);
        if (regions.first().size() > 1)
            response.setHeader("x-ctyun-metadata-location", metaLocation);
        if (regions.second().size() > 1)
            response.setHeader("x-ctyun-data-location", dataLocation);
    }

    private static void setResponseHeaderFromRequest(HttpServletRequest request,
            HttpServletResponse response) {
        // if ResponseHeaderOverrides in request is not null ,set the response
        // header.
        if (request.getParameter(ResponseHeaderOverrides.RESPONSE_HEADER_CONTENT_TYPE) != null)
            response.setContentType(request
                    .getParameter(ResponseHeaderOverrides.RESPONSE_HEADER_CONTENT_TYPE));
        if (request.getParameter(ResponseHeaderOverrides.RESPONSE_HEADER_CONTENT_LANGUAGE) != null)
            response.setHeader("Content-Language",
                    request.getParameter(ResponseHeaderOverrides.RESPONSE_HEADER_CONTENT_LANGUAGE));
        if (request.getParameter(ResponseHeaderOverrides.RESPONSE_HEADER_CACHE_CONTROL) != null)
            response.setHeader(Headers.CACHE_CONTROL,
                    request.getParameter(ResponseHeaderOverrides.RESPONSE_HEADER_CACHE_CONTROL));
        if (request.getParameter(ResponseHeaderOverrides.RESPONSE_HEADER_CONTENT_DISPOSITION) != null)
            response.setHeader(Headers.CONTENT_DISPOSITION, request
                    .getParameter(ResponseHeaderOverrides.RESPONSE_HEADER_CONTENT_DISPOSITION));
        if (request.getParameter(ResponseHeaderOverrides.RESPONSE_HEADER_CONTENT_ENCODING) != null)
            response.setHeader(Headers.CONTENT_ENCODING,
                    request.getParameter(ResponseHeaderOverrides.RESPONSE_HEADER_CONTENT_ENCODING));
        if (request.getParameter(ResponseHeaderOverrides.RESPONSE_HEADER_EXPIRES) != null)
            response.setHeader(Headers.EXPIRES,
                    request.getParameter(ResponseHeaderOverrides.RESPONSE_HEADER_EXPIRES));
    }
    
    public static void getObjectStatics(HttpServletRequest request, HttpServletResponse response,
            BucketMeta bucket, ObjectMeta object, BucketLog bucketLog, PinData pinData, Pin pin)
            throws BaseException, IOException {
        boolean isPublicIp = Utils.isPublicIP(bucketLog.ipAddress);
        try {
            if (Backoff.checkPutGetDeleteOperationsBackoff(HttpMethodName.GET, isPublicIp)
                    && !BackoffConfig.getWhiteList().contains(bucket.name))
                Backoff.backoff();
            if ((!BackoffConfig.isEnableStreamSlowDown())
                    && (JettyServer.outgoingBytes>BackoffConfig.getMaxOutgoingBytes() && !BackoffConfig.getWhiteList().contains(bucket.name)))
                Backoff.backoff();
            if (BackoffConfig.getBandwidthBackoff() == 1
                    && Backoff.checkBandwidthBackoff(false, isPublicIp, bandwidthBackoff) && !BackoffConfig.getWhiteList().contains(bucket.name))
                Backoff.backoff();
            getObject(request, response, bucket, object, bucketLog, pinData, pin);
        } finally {
            Backoff.decreasePutGetDeleteOperations(HttpMethodName.GET, isPublicIp);
        }
    }
    
    private static long copy(InputStream is, OutputStream os, long size,
            BucketLog bucketLog, boolean hasLimitRate, int limitRate, ObjectMeta object)
            throws IOException {
        long count = 0;
        byte[] buf = ThreadLocalBytes.current().get64KBytes();
        EtagMaker etagMaker = new EtagMaker();
        if (!hasLimitRate) {
            while (size > 0) {
                int read = 0;
                try {
                    read = is.read(buf, 0, (int) Math.min(size, buf.length));
                    //非分段上传，并且不是从内部API过来的才进行md5. 分段上传和内部api的在有它们自己的逻辑处理，不能重复计算。 
                    if (object.ostorId != null && object.ostorId.trim().length() != 0 
                            && object.dataRegion.equals(LOCAL_REGION_NAME))
                        etagMaker.sign(buf, 0, read);
                } catch (Exception e) {
                    bucketLog.exception = 5;// get object过程中，从ostor读数据异常
                    throw e;
                }
                if (read < 0)
                    break;
                try {
                    os.write(buf, 0, read);
                } catch (Exception e) {
                    bucketLog.exception = 8;// get object过程中，向客户端写数据错误
                    //对象下载过程中，如果客户端原因导致下载失败，仍然记录上传过的文件长度，用来计费
                    bucketLog.realResponseLength = count;
                    throw e;
                }

                count += read;
                size -= read;
            }
        } else {
            // 共享链接限速
            int limitRatebps = limitRate * Consts.KB;
            float timeCostPerBuf = buf.length * 1000.0f / limitRatebps;// 按照限速，读取每个buf应该的耗时，单位毫秒
            while (size > 0) {
                long start = System.currentTimeMillis();
                int read = 0;
                try {
                    read = is.read(buf, 0, (int) Math.min(size, buf.length));
                    //非分段上传，并且不是从内部API过来的才进行md5. 分段上传和内部api的在有它们自己的逻辑处理，不能重复计算。 
                    if (object.ostorId != null && object.ostorId.trim().length() != 0 
                            && object.dataRegion.equals(LOCAL_REGION_NAME))
                        etagMaker.sign(buf, 0, read);
                    if (size < buf.length) {
                        timeCostPerBuf = size * 1000.0f / limitRatebps;
                    }
                    long end = System.currentTimeMillis();
                    long spent = end - start;
                    long sleepTime = (long) (timeCostPerBuf - spent);// 读取每个buf应该耗时与实际耗时差值
                    if (sleepTime > 0) {
                        try {
                            Thread.sleep(sleepTime);
                        } catch (InterruptedException e) {} 
                    }
                } catch (Exception e) {
                    bucketLog.exception = 5;// get object过程中，从ostor读数据异常
                    throw e;
                }
                if (read < 0)
                    break;
                try {
                    os.write(buf, 0, read);
                } catch (Exception e) {
                    bucketLog.exception = 8;// get object过程中，向客户端写数据错误
                    //对象下载过程中，如果客户端原因导致下载失败，仍然记录上传过的文件长度，用来计费
                    bucketLog.realResponseLength = count;
                    throw e;
                }
                count += read;
                size -= read;
            }
        }
        bucketLog.etag = etagMaker.digest();
        return count;
    }
    
    private static long copyStreamToClient(BucketLog bucketLog, InputStream content,
            OutputStream out, long length, boolean hasLimitRate, int limitRate, ObjectMeta object) throws IOException,BaseException {
        if (bucketLog.ostorResponseFirstTime == 0) {
            bucketLog.ostorResponseFirstTime = System.currentTimeMillis();
            bucketLog.adapterResponseStartTime = System.currentTimeMillis();
            bucketLog.adapterResponseFirstTime = System.currentTimeMillis();
        }
        long count = 0;
        try {
            if(UserBandWidthLimit.allow(bucketLog.akOwnerName, "GET"))
                content = new BandWidthControlInputStream(content, bucketLog.akOwnerName, "GET");
            count = copy(content, out, length, bucketLog, hasLimitRate, limitRate, object);
            bucketLog.realResponseLength = count;
        } finally {
            try{
                content.close();
            } catch (Exception e){
                log.error(e.getMessage(), e);
            }
        }
        bucketLog.ostorResponseLastTime = System.currentTimeMillis();
        bucketLog.adapterResponseLastTime = System.currentTimeMillis();
        return bucketLog.realResponseLength;
    }

    private static long getObject(HttpServletResponse response,
            String bucketName, ObjectMeta object, long beginRange,
            long endRange, String range, BucketLog bucketLog, String uri,
            long ownerId, Pin pin, PinData pinData, boolean hasLimitRate,
            int limitRate) throws BaseException, IOException {
        OutputStream out = response.getOutputStream();
        long count = 0;
        InputStream content = null;
        InputStream convertContent = null;
        BandWidthControlStream bwContent = null;
        long objSize = (endRange - beginRange + 1);
        response.setHeader(Headers.CONTENT_LENGTH, Long.toString(objSize));
        boolean completeGetObject = true;
        // @oosText和@oosImage参数只能二选一,同时存在报400错误
        if (uri.contains(Utils.OOS_IMAGE) && uri.contains(Utils.OOS_TEXT)) {
            throw new BaseException(400, "InvalidArgument", "The parameters of @oosText and @oosImage can only be selected one.");
        }
        try {
            if (object.storageId == null || object.storageId.trim().length() == 0) {// 未合并的分段片段
                response.setHeader(Headers.ETAG, "\"-\"");
                bucketLog.exception = 2;
            }
            try {
                content = getObjectInputStream(object, beginRange, endRange, range, bucketLog);
            } catch (BaseException e) {
                if (e.status >= 500) {
                    bucketLog.exception = 7; //从ostor读数据异常
                    /*
                    /*  不处理ostor明确的错误 ErrorMessage.ERROR_CODE_SLOW_DOWN
                    /*  同时补充判断是否为object被删除的情况
                    */                     
                    try{
                        if(e.status != 503 && !client.objectSelect(object)) {
                            log.error("found ostor throw 500 , Re-query the database , object is null", e);
                            e = new BaseException(404, ErrorMessage.ERROR_CODE_NO_SUCH_KEY, "the request object is " + object.name);
                        }
                    }catch (Throwable ta){
                        log.error("objectSelect error, object is "+ object.name, ta);
                    }              
                }
                throw e;
            }
            
            if (object.ostorId != null && object.ostorId.trim().length() != 0) {
                if (uri.contains(Utils.OOS_IMAGE) || uri.contains(Utils.OOS_TEXT)) {
                    // 进行图片、文本处理
                    Pair<String, String> p = getTypeAndParams(uri);
                    String type = p.first();
                    String params = p.second();
                    convertContent = content;
                    // 图片处理
                    if (type.equals(Utils.TYPE_IMAGE_PROCESS)) {
                        try {
                            Pair<Long, InputStream> pair = ImageProcessor.process2Stream(convertContent,
                                    bucketName, object.name, objSize, params);
                            if (params.equals(ImageParams.INFO_EXIF)) {
                                response.setHeader(Headers.CONTENT_TYPE, "application/json");
                            }
                            content = pair.second();
                            objSize = pair.first();
                            response.setHeader(Headers.CONTENT_LENGTH, Long.toString(objSize));
                        } catch (Exception e) {
                            if (e instanceof ProcessorException
                                    && ((ProcessorException) e).getErrorCode() == 600) {
                                throw new BaseException(400,
                                        ErrorMessage.ERROR_CODE_BAD_DIGEST,
                                        ErrorMessage.ERROR_MESSAGE_ADVANCE_POSTION_OUTOF_IMAGE);
                            }
                            log.error("image process fail. bucket:" + bucketName + " object:" + object.name + " params:"
                                    + params + " message:" + e.getMessage(), e);
                            // 重新读流
                            bucketLog.exception = 6;
                            content.close();
                            content = getObjectInputStream(object, beginRange, endRange, range, bucketLog);
                        }
                    }
                    // 文本反垃圾
                    if (type.equals(Utils.TYPE_SPAM)) {
                        int code = 0;
                        try {
                            Pair<Long, InputStream> pair = TextProcessor.process2Stream(convertContent, object.name, objSize, params);
                            response.setHeader(Headers.CONTENT_TYPE, "application/json");
                            objSize = pair.first();
                            String respMsg = TextProcessor.convertStreamToString(pair.second());
                            JSONObject respJsonObject = new JSONObject(respMsg);
                            code = respJsonObject.getInt("code");
                            String msg = respJsonObject.getString("msg");
                            if (code != 200) {
                                bucketLog.exception = 6;
                                throw new BaseException(code, msg);
                            }
                            byte[] b = respMsg.getBytes(Consts.CS_UTF8);
                            content = new ByteArrayInputStream(b);
                            response.setHeader(Headers.CONTENT_LENGTH, Long.toString(objSize));
                        } catch (ProcessorException e) {
                            throw new BaseException(e.getErrorCode(), e.getMessage());
                        } finally {
                            Utils.pinSpam(code, ownerId, pin, pinData, bucketName);
                        }
                    }
                    // 图片鉴黄
                    if (type.equals(Utils.TYPE_PORN)) {
                        int code = 0;
                        JSONArray respResultArray = null;
                        try {
                            // 大图先进行压缩
                            if (objSize > ContentSecurityConfig.imageResizeThreshold) {
                                Pair<Long, InputStream> resizePair = ImageProcessor.process2Stream(convertContent, bucketName, object.name, objSize, ContentSecurityConfig.resizeParam);
                                convertContent = resizePair.second();
                                objSize = resizePair.first();
                            }
                            Pair<Long, InputStream> pair = ImageProcessor.process2Stream(convertContent, bucketName, object.name, objSize, params);
                            response.setHeader(Headers.CONTENT_TYPE, "application/json");
                            objSize = pair.first();
                            String respMsg = TextProcessor.convertStreamToString(pair.second());
                            JSONObject respJsonObject = new JSONObject(respMsg);
                            code = respJsonObject.getInt("code");
                            String msg = respJsonObject.getString("msg");
                            if (code != 200) {
                                bucketLog.exception = 6;
                                throw new BaseException(code, msg);
                            }
                            respResultArray = respJsonObject.getJSONArray("result");
                            byte[] b = respMsg.getBytes(Consts.CS_UTF8);
                            content = new ByteArrayInputStream(b);
                            Utils.setCommonHeader(response, new Date(bucketLog.startTime), bucketLog.requestId);
                            response.setHeader(Headers.CONTENT_LENGTH, Long.toString(objSize));
                        } catch (ProcessorException e) {
                            throw new BaseException(e.getErrorCode(), e.getMessage());
                        } finally {
                            Utils.pinPorn(code, respResultArray, ownerId, pin, pinData, bucketName);
                        }
                    }
                }
            }
            
            if (!BackoffConfig.isEnableStreamSlowDown()
                    || BackoffConfig.getWhiteList().contains(bucketLog.bucketName)) {
                count = copyStreamToClient(bucketLog, content, out, objSize, hasLimitRate, limitRate, object);
            } else {
                int controlSize = object.storageClass.ec_m > 0 ? object.ostorPageSize * object.storageClass.ec_n : object.ostorPageSize;
                bwContent = new BandWidthControlStream(content, controlSize,
                        BackoffConfig.getMaxStreamPauseTime(), new ActualExpectedRatio() {
                            @Override
                            public double getRatio() {
                                long maxOutgoingBytes = BackoffConfig.getMaxOutgoingBytes();
                                long outGoingBytes = JettyServer.outgoingBytes;
                                if ((maxOutgoingBytes <= 0) || (outGoingBytes <= 0)
                                        || (outGoingBytes < maxOutgoingBytes)) {
                                    return (0f);
                                } else {
                                    return (((double) outGoingBytes) / ((double) maxOutgoingBytes));
                                }
                            }

                            @Override
                            public int getSpeedLimit() {
                                return 0;
                            }
                        });
                count = copyStreamToClient(bucketLog, bwContent, out, objSize, hasLimitRate, limitRate, object);
                // 图片鉴黄和文本反垃圾不计流量费用
                if (uri.contains(Utils.OOS_IMAGE) || uri.contains(Utils.OOS_TEXT)) {
                    Pair<String, String> p = getTypeAndParams(uri);
                    String type = p.first();
                    if (type.equals(Utils.TYPE_SPAM) || type.equals(Utils.TYPE_PORN)) {
                        count = 0;
                    }
                }
            }
            //非分段上传的etag获取。 分段上传的由ContinuousInputStream类close方法获取。
            //只有本地对象需要校验，异地通过内部API校验。 
            if(null != object.storageId && !object.storageId.isEmpty() && null != object.etag && object.dataRegion.equals(LOCAL_REGION_NAME) 
                    && !object.etag.equals("-") && !bucketLog.etag.equals(object.etag) && objSize == object.size) {
                SendEtagUnmatchMQ.send(bucketLog.etag, object.etag, 
                        Storage.getStorageId(object.storageId), object.ostorId);
            }
        } catch (Exception e) {
            completeGetObject = false;
            if (bucketLog.exception == 2 || bucketLog.exception == 0)
                bucketLog.exception = 1;
            if (e instanceof BaseException)
                throw (BaseException) e;
            log.error(e.getMessage(), e);
            throw new BaseException(500, ErrorMessage.GET_OBJECT_FAILED);
        } finally {
            if (content != null)
                try {
                    if(completeGetObject && content instanceof ContinuousInputStream) 
                        ((ContinuousInputStream)content).closeWithNoException();
                    else 
                        content.close(); 
                } catch (Throwable e) {
                    log.error(e.getMessage(), e);
                }
            if (bwContent != null)
                try {
                    bwContent.close();
                } catch (Throwable e) {
                    log.error(e.getMessage(), e);
                }
            if (convertContent != null)
                try {
                    convertContent.close();
                } catch (Throwable e) {
                    log.error(e.getMessage(), e);
                }
        }
        return count;
    }
    
    /**
     * 返回处理类型及参数，如图片处理、图片鉴黄、文本反垃圾、正常类型
     * @param uri
     * @return
     * @throws IOException 
     */
    public static Pair<String, String> getTypeAndParams(String uri) throws IOException {
        uri = URLDecoder.decode(uri, Constants.DEFAULT_ENCODING);
        Pair<String, String> p = new Pair<String, String>();
        String type = Utils.TYPE_NORMAL;
        String params = null;
        int idx;
        if ((idx = uri.indexOf(Utils.OOS_IMAGE)) >= 0) {
            params = uri.substring(idx + Utils.OOS_IMAGE.length());
            if (params.startsWith("|")) {
                params = params.substring(1);
            }
            String[] commonds = params.split("\\|");
            for (int i = 0; i < commonds.length; i++) {
                if(commonds[i].equals(Utils.PORN)){
                    type = Utils.TYPE_PORN;
                    params = Utils.PORN; 
                    p.first(type);
                    p.second(params);
                    return p;
                }
            }
            type = Utils.TYPE_IMAGE_PROCESS;
            p.first(type);
            p.second(params);
            return p;
        }
        if ((idx = uri.indexOf(Utils.OOS_TEXT)) >= 0) {
            params = uri.substring(idx + Utils.OOS_TEXT.length());
            if (params.startsWith("|")) {
                params = params.substring(1);
            }
            String[] commonds = params.split("\\|");
            for (int i = 0; i < commonds.length; i++) {
                if (commonds[i].equals(Utils.SPAM)) {
                    type = Utils.TYPE_SPAM;
                    params = Utils.SPAM; 
                    p.first(type);
                    p.second(params);
                    return p;
                }
            }
            type = Utils.TYPE_TEXT_PROCESS;
            p.first(type);
            p.second(params);
            return p;
        }
        p.first(type);
        p.second(params);
        return p;
    }

    /**
     * 获取对象的数据流，包括普通对象，分段未合并、已合并对象
     * 
     * @param object
     * @param beginRange
     *            获取对象的一部分，起始位置
     * @param endRange
     *            获取对象的一部分，终止位置
     * @param range
     * @param bucketLog
     * @return 对象的数据流
     * @throws BaseException
     * @throws IOException
     */
    static InputStream getObjectInputStream(ObjectMeta object, long beginRange, long endRange,
            String range, BucketLog bucketLog) throws BaseException, IOException {
        long wholesize = 0;
        long beforesize = 0;
        InputStream content = null;
        if (object.ostorId == null || object.ostorId.trim().length() == 0) {// 分段上传对象
            List<UploadMeta> uploads;
            if (object.storageId == null || object.storageId.trim().length() == 0) {// 未合并的分段片段
                InitialUploadMeta initialUpload = new InitialUploadMeta(object.metaRegionName, object.bucketName,
                        object.name, null, false);
                if(client.isBusy(initialUpload, OOSConfig.getMaxConcurrencyPerRegionServer()))
                    Backoff.backoff();
                if (!client.initialSelectByObjectId(initialUpload))// 没有分段片段，返回404
                    throw new BaseException(404, ErrorMessage.ERROR_CODE_NO_SUCH_UPLOAD,
                            ErrorMessage.ERROR_MESSAGE_404);
                UploadMeta upload = new UploadMeta(object.metaRegionName, initialUpload.uploadId, 0);
                if(client.isBusy(upload, OOSConfig.getMaxConcurrencyPerRegionServer()))
                    Backoff.backoff();
                uploads = client.uploadListAllParts(upload, -1, Consts.UPLOAD_PART_MAXNUM);
                if (StringUtils.isEmpty(initialUpload.uploadId)  && uploads.size() == 0)// 分片上传被abort，返回404
                    throw new BaseException(404, ErrorMessage.ERROR_CODE_NO_SUCH_UPLOAD,
                            ErrorMessage.ERROR_MESSAGE_404);
            } else {// 已合并的分段片段
                uploads = new ArrayList<UploadMeta>();
                boolean first = true;
                for (Integer i : object.partNum) {
                    UploadMeta upload = new UploadMeta(object.metaRegionName, object.initialUploadId, i);
                    if(first && client.isBusy(upload, OOSConfig.getMaxConcurrencyPerRegionServer()))
                        Backoff.backoff();
                    first = false;
                    boolean result = client.uploadSelect(upload);
                    if(!result) {
                        throw new BaseException(404, ErrorMessage.ERROR_CODE_NO_SUCH_UPLOAD,
                                ErrorMessage.ERROR_MESSAGE_404);
                    }
                    uploads.add(upload);
                }
            }

            // 将各个part的数据流拼起来
            List<UploadInputMeta> parts = new ArrayList<>(uploads.size());
            for (int i = 0; i < uploads.size(); i++) {
                UploadMeta part = uploads.get(i);
                long start = 0;
                long size = part.size;
                if (endRange != -1 && wholesize == endRange - beginRange + 1)
                    break;
                if (beforesize < beginRange) {
                    if (beforesize + size <= beginRange) {
                        beforesize += size;
                        continue;
                    } else {
                        start = beginRange - beforesize;
                        size -= start;
                    }
                }
                if (endRange != -1 && beforesize + start + size > endRange)
                    size = endRange - start - beforesize + 1;
                try {
                    String ostorKey = Storage.getStorageId(part.storageId);
                    parts.add(new UploadInputMeta(ostorKey, part.ostorId, object.dataRegion,
                            start, size, part.pageSize, part.replicaMode, bucketLog,
                            part.initialUploadId, part.etag, part.size));
                    wholesize += size;
                    beforesize += part.size;
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                    throw new BaseException(500, ErrorMessage.GET_OBJECT_FAILED);
                }
            }
            ContinuousInputStream inputStream = new ContinuousInputStream(parts, bucketLog.requestId);
            continuousInputStreamPool.execute(inputStream);
            return inputStream;
        } else {// 普通对象
            String ostorKey = Storage.getStorageId(object.storageId);
            bucketLog.ostorId = object.ostorId;
            bucketLog.ostorKey = ostorKey;
            bucketLog.etag = object.etag;
            bucketLog.replicaMode = object.storageClass.toString();
            if (object.dataRegion.equals(LOCAL_REGION_NAME)) {
                long start = beginRange;
                long length = (range != null) ? (endRange - beginRange + 1) : object.size;
                Storage storage = new Storage(bucketLog, object.ostorId);
                content = storage.read(ostorKey, start, length, object.name, object.ostorPageSize,
                        object.storageClass);
            } else {
                content = internalClient.get(object.dataRegion, ostorKey, object.ostorId,
                        object.size, object.ostorPageSize, object.storageClass, range, bucketLog.requestId, object.etag);
            }
            bucketLog.ostorKey = ostorKey;
            return content;
        }
    }
    
    

    public static void putObjectStatics(HttpServletRequest request, HttpServletResponse response,
            final BucketMeta bucket, final ObjectMeta object, boolean objectExists, BucketLog bucketLog, InputStream input,
            boolean isPostObject, FormPostHeaderNotice formPostHeaderNotice, PinData pinData)
            throws BaseException, IOException {
        boolean isPublicIp = Utils.isPublicIP(bucketLog.ipAddress);
        try {
            if (Backoff.checkPutGetDeleteOperationsBackoff(HttpMethodName.PUT, isPublicIp)
                    && !BackoffConfig.getWhiteList().contains(bucket.name))
                Backoff.backoff();
            if ((!BackoffConfig.isEnableStreamSlowDown())
                    && (JettyServer.inComingBytes>BackoffConfig.getMaxIncomingBytes() && !BackoffConfig.getWhiteList().contains(bucket.name))){
                Backoff.backoff();
            }
            if (BackoffConfig.getBandwidthBackoff() == 1
                    && Backoff.checkBandwidthBackoff(true, isPublicIp, bandwidthBackoff) && !BackoffConfig.getWhiteList().contains(bucket.name))
                Backoff.backoff();
            putObject(request, response, bucket, object,objectExists, bucketLog, input,
                    isPostObject, formPostHeaderNotice, pinData);
        } finally {
            Backoff.decreasePutGetDeleteOperations(HttpMethodName.PUT, isPublicIp);
        }
    }
    
    private static String getUserMeta(ObjectMeta object, long ownerId) {
        return "";
    }

    /**
     * 获取对象的数据域，object级的设置优先于bucket级。
     * 
     * @param request
     * @param bucket
     * @param ownerDataRegions
     * @return
     * @throws BaseException
     */
    private static CtyunBucketDataLocation getDataLocation(HttpServletRequest request, BucketMeta bucket,
            Set<String> ownerDataRegions) throws BaseException {
        String header = request.getHeader(Consts.X_CTYUN_DATA_LOCATION);
        if (header == null) {
            return bucket.dataLocation;
        } else if (ownerDataRegions.size() == 1) {
            throw new BaseException(405, ErrorMessage.ERROR_CODE_405,
                    ErrorMessage.ERROR_MESSAGE_INVALID_LOCATION_CONSTRAINT);
        } else {
            Matcher matcher = dataLocationPattern.matcher(header);
            if (!matcher.matches()) {
                throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_LOCATION_CONSTRAINT,
                        ErrorMessage.ERROR_MESSAGE_INVALID_LOCATION_CONSTRAINT);
            }

            CtyunBucketDataLocation dataLocation = new CtyunBucketDataLocation();
            if (matcher.group(1).equals(CtyunBucketDataType.Local.toString())) {
                dataLocation.setType(CtyunBucketDataType.Local);
            } else {
                dataLocation.setType(CtyunBucketDataType.Specified);
            }

            LinkedHashSet<String> headerDataRegions = new LinkedHashSet<>();
            String locationString = matcher.group(3);
            if (locationString != null) {
                for (String dataRegion : locationString.split(",")) {
                    if (!ownerDataRegions.contains(dataRegion)) {
                        throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_LOCATION_CONSTRAINT,
                                ErrorMessage.ERROR_MESSAGE_INVALID_LOCATION_CONSTRAINT);
                    }
                    headerDataRegions.add(dataRegion);
                }
            } else if (dataLocation.getType() == CtyunBucketDataType.Specified) {
                throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_LOCATION_CONSTRAINT,
                        ErrorMessage.ERROR_MESSAGE_INVALID_LOCATION_CONSTRAINT);
            }

            dataLocation.setDataRegions(new ArrayList<>(headerDataRegions));
            if (matcher.group(5).equals(CtyunBucketDataScheduleStrategy.Allowed.toString())) {
                dataLocation.setStragegy(CtyunBucketDataScheduleStrategy.Allowed);
            } else {
                dataLocation.setStragegy(CtyunBucketDataScheduleStrategy.NotAllowed);
            }
            return dataLocation;
        }
    }
    
    /**
     * 获取调度时的数据域顺序
     * 
     * @param regionsTried
     * @param ownerRegions
     * @return
     */
    private static List<String> getSchedulingRegions(List<String> regionsTried, Set<String> ownerRegions) {
        List<String> schedulingRegions = new ArrayList<>(ownerRegions);
        schedulingRegions.removeAll(regionsTried);
        return schedulingRegions;
    }

    /**
     * 向本地写数据
     * 
     * @param input
     * @param bucket
     * @param object
     * @param storageClass
     * @param owner
     * @param contentLength
     * @param isPostObject
     * @param formPostHeaderNotice
     * @param bucketLog
     * @throws BaseException
     */
    private static void putLocal(InputStream input, OwnerMeta owner, BucketMeta bucket, ObjectMeta object, String storageClass,
            long contentLength, boolean isPostObject, FormPostHeaderNotice formPostHeaderNotice, BucketLog bucketLog,
            int limitRate)  throws BaseException {
        setReplicaModeAndSize(object, storageClass, false, owner);
        bucketLog.ostorId = object.ostorId;
        bucketLog.replicaMode = object.storageClass.toString();
        EtagMaker etagmaker = new EtagMaker();
        Storage storage = new Storage(bucketLog, etagmaker, object.ostorId);
        String userMeta = getUserMeta(object, bucket.getOwnerId());
        String storageId;
        if (isPostObject) {
            storageId = storage.create(input, contentLength, userMeta, object.name, object.ostorPageSize, 
                    object.storageClass, object.bucketName, formPostHeaderNotice);
        } else {
            storageId = storage.create(input, contentLength, userMeta, object.name, null, object.ostorPageSize,
                    object.storageClass, object.bucketName, limitRate);
        }
        checkStorageId(storageId);
        object.etag = etagmaker.digest();
        object.storageId = Storage.setStorageId(storageId);
        object.dataRegion = LOCAL_REGION_NAME;
        bucketLog.ostorKey = storageId;
        bucketLog.etag = object.etag;
    }

    /**
     * 向异地写数据
     * 
     * @param input
     * @param dataRegion
     * @param bucket
     * @param object
     * @param storageClass
     * @param contentLength
     * @return
     * @throws BaseException
     */
    private static void putRemote(InputStream input, String dataRegion, BucketMeta bucket, ObjectMeta object, 
            String storageClass, long contentLength, boolean isPostObject, FormPostHeaderNotice formPostHeaderNotice, 
            BucketLog bucketLog, int limitRate) throws BaseException {
        long size;
        if (isPostObject) {
            size = contentLength - formPostHeaderNotice.getHeaderTotalLength();
        } else {
            size = contentLength;
        }
        storageClass = StringUtils.defaultIfBlank(storageClass, StorageClassConfig.getDefaultClass());
        //@TODO 实现限速 
        WriteResult writeResult = internalClient.write(dataRegion, bucket.name, object.name, storageClass, size, input, 
                isPostObject, limitRate, bucketLog.requestId);
        object.storageClass = writeResult.replicaMode;
        object.storageClassString = storageClass;
        object.ostorPageSize = writeResult.pageSize;
        object.ostorId = writeResult.ostorId;
        object.etag = writeResult.etag;
        object.storageId = Storage.setStorageId(writeResult.ostorKey);
        object.dataRegion = writeResult.regionName;
        bucketLog.ostorId = writeResult.ostorId;
        bucketLog.ostorKey = writeResult.ostorKey;
        bucketLog.etag = writeResult.etag;
        bucketLog.replicaMode = writeResult.replicaMode.toString();
        if (isPostObject) {
            formPostHeaderNotice.fileLength = writeResult.objectSize;
            PolicyManager.checkFileLength(formPostHeaderNotice.policy, writeResult.objectSize);
            PolicyManager.checkFileFieldCount(formPostHeaderNotice);
            long totalHeaderLength = formPostHeaderNotice.getHeaderTotalLength();
            long bodyLength = writeResult.objectSize + totalHeaderLength;
            if (bodyLength != contentLength) {
                log.error("Form POST. incomplete body, the real length from ostor is: " + writeResult.objectSize + 
                        ", the body length is: " + bodyLength + ", contentLength is: " + contentLength + 
                        ", request id: " + bucketLog.requestId + ", ostorId: " + object.ostorId);
                throw new BaseException(400, ErrorMessage.ERROR_CODE_INCOMPLETE_BODY, 
                        ErrorMessage.ERROR_MESSAGE_INCOMPLETE_BODY);
            }
        }
    }

    /**
     * 依次尝试向dataRegions中的数据域写
     * @param dataRegions
     * @param input
     * @param bucket
     * @param object
     * @param storageClass
     * @param contentLength
     * @param isPostObject
     * @param formPostHeaderNotice
     * @param bucketLog
     * @return 返回true表示成功，false表示所有数据域均已写满
     * @throws BaseException 写失败
     */
    private static boolean tryPut(List<String> dataRegions, InputStream input, OwnerMeta owner, BucketMeta bucket, ObjectMeta object,
            String storageClass, long contentLength, boolean isPostObject,
            FormPostHeaderNotice formPostHeaderNotice, BucketLog bucketLog, int limitRate) throws BaseException{
        for (String dataRegion : dataRegions) {
            if (internalClient.canWriteRegion(dataRegion)) {
                //获取到dataregion后立刻写入bucket log 。如果写入磁盘失败，accesslog 可查看写入哪个集群失败。
                bucketLog.metaRegion = object.metaRegionName;
                bucketLog.dataRegion = dataRegion;
                putObjectToRegion(dataRegion, input, owner, bucket, object, storageClass, contentLength, isPostObject,
                        formPostHeaderNotice, bucketLog, limitRate);
                return true;
            }
        }
        return false;
    }

    private static void putObjectToRegion(String dataRegion, InputStream input, OwnerMeta owner, BucketMeta bucket, ObjectMeta object, String storageClass,
            long contentLength, boolean isPostObject, FormPostHeaderNotice formPostHeaderNotice, BucketLog bucketLog,
            int limitRate) throws BaseException {
        if (dataRegion.equals(LOCAL_REGION_NAME)) {
            putLocal(input, owner, bucket, object, storageClass, contentLength, isPostObject,
                    formPostHeaderNotice, bucketLog, limitRate);
        } else {
            putRemote(input, dataRegion, bucket, object, storageClass, contentLength, isPostObject,
                    formPostHeaderNotice, bucketLog, limitRate);
        }
    }

    /**
     * 写入指定的数据域，都已写满时进行调度
     * @param request
     * @param bucket
     * @param object
     * @param bucketLog
     * @param input
     * @param isPost
     * @param formPostHeaderNotice
     * @param storageClass
     * @param contentLength
     * @throws IOException
     * @throws BaseException
     */
    private static boolean putWithScheduling(HttpServletRequest request,HttpServletResponse response, BucketMeta bucket, 
            ObjectMeta object, BucketLog bucketLog, InputStream input, boolean isPost, FormPostHeaderNotice formPostHeaderNotice,
            String storageClass, long contentLength) throws IOException, BaseException {

        if (request.getHeader("Expect") != null
                && request.getHeader("Expect").equalsIgnoreCase("100-continue")) {
            response.setStatus(100);
        }

        int limitRate = Consts.USER_NO_LIMIT_RATE;
        String rate = HttpLimitParser.getLimit(request, Consts.X_AMZ_LIMIT_RATE_INDEX);
        if(StringUtils.isNotBlank(rate)) {
            limitRate = validLimitRate(rate.replace("rate=", ""));
        }
        boolean ok;
        // 写数据
        List<String> dataRegionsToTry;
        // 获取数据域
        OwnerMeta owner = new OwnerMeta(bucket.getOwnerId());
        try {
            client.ownerSelectById(owner);
        } catch (Exception e) {
            log.error("select owner error.", e);
            throw new BaseException(500, ErrorMessage.ERROR_CODE_500);
        }
        Set<String> ownerDataRegions = client.getDataRegions(bucket.getOwnerId());
        CtyunBucketDataLocation dataLocation = getDataLocation(request, bucket, ownerDataRegions);
        List<String> objectDataRegions = dataLocation.getDataRegions();

        if (dataLocation.getType() == CtyunBucketDataType.Local) {
            object.originalDataRegion = LOCAL_REGION_NAME;
            dataRegionsToTry = Collections.singletonList(LOCAL_REGION_NAME);
        } else {
            object.originalDataRegion = objectDataRegions.get(0);
            dataRegionsToTry = objectDataRegions;
        }
        ok = tryPut(dataRegionsToTry, input, owner, bucket, object, storageClass, contentLength, isPost,
                formPostHeaderNotice, bucketLog, limitRate);
        
        boolean hasRoam = false;
        if (!object.originalDataRegion.equals(LOCAL_REGION_NAME)) {
            hasRoam = true;
        }

        // 调度
        if (!ok) {
            if (dataLocation.getStragegy() == CtyunBucketDataScheduleStrategy.Allowed) {
                List<String> schedulingDataRegions = getSchedulingRegions(dataRegionsToTry, ownerDataRegions);
                ok = tryPut(schedulingDataRegions, input, owner, bucket, object, storageClass, contentLength,
                        isPost, formPostHeaderNotice, bucketLog, limitRate);
                if (!ok) {
                    throw new BaseException(500, ErrorMessage.ERROR_CODE_500);
                }
            } else {
                throw new BaseException(500, ErrorMessage.ERROR_CODE_500);
            }
        }
        return hasRoam;
    }

    /**
     * 上传时设置pinData
     *
     * @param pinData
     * @param object
     */
    private static void setUploadPinData(PinData pinData, ObjectMeta object, boolean hasRoam) {
        if (object.dataRegion.equals(LOCAL_REGION_NAME)) {
            pinData.setDirectUpload(object.size, LOCAL_REGION_NAME, object.storageClassString);
        } else if (hasRoam) {
            pinData.setRoamUpload(object.size, object.dataRegion, object.storageClassString);
        } else {
            pinData.setDirectUpload(object.size, object.dataRegion, object.storageClassString);
        }
        //是否需要大小补齐
        pinData.setSize(object.size, Utils.getRedundantSize(object.size, object.storageClass), Utils.getAlinSize(object.size, object.ostorPageSize, object.storageClass), object.dataRegion, object.originalDataRegion, object.storageClassString, object.getLastCostTime());
    }

    public static void putObject(HttpServletRequest request, HttpServletResponse response, BucketMeta bucket,
            ObjectMeta object, boolean objectExists, BucketLog bucketLog, InputStream input, boolean isPost,
            FormPostHeaderNotice formPostHeaderNotice, PinData pinData) throws BaseException, IOException {
        // 对象的大小不能超过5T
        long contentLength = Long.parseLong(request.getHeader("Content-Length"));
        if (contentLength > Consts.MAX_LENGTH) {
            throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_ARGUMENT, "the object size should be less than 5T");
        }
        long originalSize = 0;
        long originalRedundantSize = 0;
        long originalAlinSize = 0;
        long lastModifiedBeforeInsert = object.getLastCostTime();
        String storageClassStringBeforeInsert = object.storageClassString;
        String dataRegionBeforeInsert = object.dataRegion;
        String originalDataRegionBeforeInsert = object.originalDataRegion;
        boolean isMultipartUpload = StringUtils.isBlank(object.ostorId);
        if (objectExists) {
            Triple<Long, Long, Long> triple = getObjectSize(object);
            originalSize = triple.first();
            originalRedundantSize = triple.second();
            originalAlinSize = triple.third();
        }
        // 设置时间戳
        if (request.getHeader(Consts.X_CTYUN_LASTMODIFIED) != null) {
            object.lastModified = Long.valueOf(request.getHeader(Consts.X_CTYUN_LASTMODIFIED));
        } else {
            object.lastModified = Utils.getTimeStamp();
        }
        if(MultiVersionsController.gwMultiVersionEnabled(request, bucket)) {
            MultiVersionsController.setVersionInfo(object, request);
        } else {
            object.timestamp = Utils.getNanoTime();
        }
        // 设置meta
        String storageClass = null;
        if (isPost) {
            setMetadataFromMap(formPostHeaderNotice.getFieldMap(), object);
            // 由于putWithScheduling->tryPut->putLocal->setReplicaModeAndSize方法中，
            // 计算object分片大小依赖object.size,所以必须在次之前设置好object.size。
            // 而setMetadataFromMap方法无法从formPostHeaderNotice 中拿到file对象大小，
            // 所以只能通过content-length - formPostHeaderNotice.getHeaderTotalLength() 间接获取
            object.size = contentLength - formPostHeaderNotice.getHeaderTotalLength();
            FormField storageClassField = formPostHeaderNotice.getFieldMap().get(Headers.STORAGE_CLASS.toLowerCase());
            if (storageClassField != null) {
                storageClass = storageClassField.getValue().trim();
            }
        } else {
            setMetadataFromRequest(request, object);
            storageClass = request.getHeader(Headers.STORAGE_CLASS);
        }
        
        boolean hasRoam;
        try {
            // 写数据
            hasRoam = putWithScheduling(request, response, bucket, object, bucketLog, input, isPost, formPostHeaderNotice,
                    storageClass, contentLength);
            if (isPost) {
                object.size = formPostHeaderNotice.fileLength;
            } else {
                object.size = contentLength;
            }
            if (object.contentMD5 != null && object.contentMD5.trim().length() != 0) {
                checkMd5(object.contentMD5, object.etag);
            }
            object.initialUploadId = "";
            object.partNum = new ArrayList<Integer>();
            // 先插元数据，再删除老版本
            bucketLog.putMetaTime = System.currentTimeMillis();
            try {
                if (client.isBusy(object, OOSConfig.getMaxConcurrencyPerRegionServer()))
                    Backoff.backoff();
                client.objectInsert(object);
            } finally {
                bucketLog.putMetaTime = System.currentTimeMillis() - bucketLog.putMetaTime;
            }

            if (!MultiVersionsController.gwMultiVersionEnabled(request, bucket)) {
                clearOldObjectVersions(object, true, bucketLog.requestId);
            }
        } catch (BaseException e) {
            log.error(e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new BaseException(500, "Upload Object Failed");
        }
        if (objectExists) {
            pinData.sourceObject = pinData.new SourceObject(originalSize, originalAlinSize, originalRedundantSize, lastModifiedBeforeInsert, 
                    storageClassStringBeforeInsert, dataRegionBeforeInsert, originalDataRegionBeforeInsert);
            if (isMultipartUpload) {
                pinData.sourceObject.setIsMultipartUploadTrue();
            }
        }

        // 计费
        setUploadPinData(pinData, object, hasRoam);
        
        // 设置响应
        pinData.jettyAttributes.objectAttribute.length = contentLength;
        pinData.jettyAttributes.objectAttribute.objectSize = object.size;
        if (isPost) {
            String redirectUrl = FormPostTool.getSuccessActionRedirectUrl(formPostHeaderNotice.getFieldMap(), bucket,
                    object);
            if (redirectUrl != null) {
                response.setStatus(HttpServletResponse.SC_SEE_OTHER);
                response.setHeader("Location", redirectUrl);
            } else {
                int responseCode = FormPostTool.getSuccessActionStatusCode(formPostHeaderNotice.getFieldMap());
                response.setStatus(responseCode);
            }
        } else {
            response.setStatus(200);
        }
        response.setHeader("ETag", "\"" + object.etag + "\"");
        // 多版本的话，响应头增加版本号ID
        Utils.setVersionIdHeaderIfVersioning(request, response, bucket, object, false);
        setExpirationHeader(response, bucket, object);
    }
    
    /**
     * 删除未指定版本ID的所有老版本
     * @param object
     * @param delInitialUpload
     * @param requestId  调用内部接口时，目标服务要使用调用者request id追踪请求发起的地方
     * @throws BaseException
     */
    private static void clearOldObjectVersions(ObjectMeta object, boolean delInitialUpload, String requestId) throws BaseException {
        boolean updateTooFast = false;
        try {
            long minDeltaTime = OOSConfig.getPutObjectDeltaTime() * 1000000;
            List<ObjectMeta> allVersions = client.objectGetAllVersions(object);
            if (allVersions.size() > 1) {
                for (ObjectMeta version : allVersions) {
                    if(version.version != null)
                        continue;
                    long deltaTime = Math.abs(version.timestamp - object.timestamp);
                    if (deltaTime == 0) {
                        continue;
                    }
                    ObjectMeta toDelete;
                    if (deltaTime > minDeltaTime) {
                        if (version.timestamp > object.timestamp) {
                            toDelete = object;
                        } else {
                            toDelete = version;
                        }
                    } else {
                        toDelete = object;
                        updateTooFast = true;
                    }
                    client.objectDelete(toDelete);
                    
                    if (toDelete.ostorId != null && toDelete.ostorId.trim().length() > 0) {
                        if (toDelete.dataRegion.equals(LOCAL_REGION_NAME)) {
                            Storage storage = new Storage(null, toDelete.ostorId);
                            storage.delete(Storage.getStorageId(toDelete.storageId), toDelete.size, toDelete.name, 
                                    toDelete.ostorPageSize, toDelete.storageClass, true);
                        } else {
                            internalClient.delete(toDelete.dataRegion, Storage.getStorageId(toDelete.storageId), 
                                    toDelete.ostorId, toDelete.size, toDelete.ostorPageSize, toDelete.storageClass, requestId);
                        }
                        log.info("delete object:" + toDelete.storageId + " name:" + toDelete.name
                                + " pageSize:" + toDelete.ostorPageSize + " mode:" + toDelete.storageClass.toString());
                    } else if (delInitialUpload) {
                        deleteAllInitialUpload(toDelete, null);
                    }
                }
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        if (updateTooFast) {
            throw new BaseException(400, "PutObjectTooFast", "Object name:" + object.name + " is updated too fast");
        }
    }
    
    /**
     * 删除未指定版本ID的所有老版本的Meta
     * @param object 要删除meta的object
     * @param requestId  调用内部接口时，目标服务要使用调用者request id追踪请求发起的地方
     * @throws BaseException
     */
    private static void clearOldObjectMetaVersions(ObjectMeta object, String requestId) throws BaseException {
        boolean updateTooFast = false;
        try {
            long minDeltaTime = OOSConfig.getPutObjectDeltaTime() * 1000000;
            List<ObjectMeta> allVersions = client.objectGetAllVersions(object);
            if (allVersions.size() > 1) {
                for (ObjectMeta version : allVersions) {
                    if(version.version != null)
                        continue;
                    long deltaTime = Math.abs(version.timestamp - object.timestamp);
                    if (deltaTime == 0) {
                        continue;
                    }
                    ObjectMeta toDelete;
                    if (deltaTime > minDeltaTime) {
                        if (version.timestamp > object.timestamp) {
                            toDelete = object;
                        } else {
                            toDelete = version;
                        }
                    } else {
                        toDelete = object;
                        updateTooFast = true;
                    }
                    client.objectDelete(toDelete);
                }
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        if (updateTooFast) {
            throw new BaseException(400, "PutObjectTooFast", "Object name:" + object.name + " is updated too fast");
        }
    }

    private static void deleteAllInitialUpload(ObjectMeta object, BucketLog bucketLog)
            throws IOException, BaseException {
        InitialUploadMeta initialUpload = new InitialUploadMeta(object.metaRegionName, object.bucketName, object.name,
                null, false);
        if(client.isBusy(initialUpload, OOSConfig.getMaxConcurrencyPerRegionServer()))
            Backoff.backoff();
        List<InitialUploadMeta> initialUploads = client.initialSelectAllByObjectId(initialUpload);
        for (InitialUploadMeta initial : initialUploads) {
            UploadMeta upload = new UploadMeta(object.metaRegionName, initial.uploadId, true);
            List<UploadMeta> uploads = client.uploadSelectByUploadId(upload);
            abortMultipartUploadMeta(upload, uploads, initial);
            abortMultipartUpload(object, upload, uploads, initial, bucketLog);
        }
    }
    
    private static void checkMd5(String contentMD5, String etag) throws BaseException {
        if (contentMD5 == null || contentMD5.trim().length() == 0)
            return;
        Utils.checkMd5(contentMD5, etag);
    }
    
    private static void setMetadataFromPost(HttpServletRequest request, ObjectMeta object) throws BaseException {
        String cacheControl = request.getHeader(Headers.CACHE_CONTROL);
        if (cacheControl != null && !cacheControl.trim().isEmpty()) {
            object.cacheControl = cacheControl;
        }
        String contentDisposition = request.getHeader(Headers.CONTENT_DISPOSITION);
        if (contentDisposition != null && !contentDisposition.trim().isEmpty()) {
            object.contentDisposition = contentDisposition;
        }
        String contentEncoding = request.getHeader(Headers.CONTENT_ENCODING);
        if (contentEncoding != null && !contentEncoding.trim().isEmpty()) {
            object.contentEncoding = contentEncoding;
        }
        String etag = request.getHeader(Headers.ETAG);
        if (etag != null && !etag.trim().isEmpty()) {
            object.etag = etag;
        }
        if (request.getHeader(Headers.CONTENT_LENGTH) == null) {
            throw new BaseException(411, ErrorMessage.ERROR_CODE_MISSING_CONTENT_LENGTH);
        }
        String contentMD5 = request.getHeader(Headers.CONTENT_MD5);
        if (contentMD5 != null && !contentMD5.trim().isEmpty()) {
            object.contentMD5 = contentMD5;
        }
        object.size = Long.parseLong(request.getParameter("filesize"));
        Properties prop = new Properties();
        object.setProperties(prop);
    }
    
    public static void postObject(HttpServletRequest request, HttpServletResponse response, BucketMeta bucket, 
            ObjectMeta object, InputStream input, BucketLog bucketLog, boolean objectExists, PinData pinData) 
                    throws BaseException, IOException {
        // 检查并设置meta
        if (Long.parseLong(request.getHeader(Headers.CONTENT_LENGTH)) > Consts.MAX_LENGTH) {
            throw new BaseException();
        }
        long originalSize = 0;
        long originalRedundantSize = 0;
        long originalAlinSize = 0;
        long lastModifiedBeforeInsert = object.getLastCostTime();
        String storageClassStringBeforeInsert = object.storageClassString;
        String dataRegionBeforeInsert = object.dataRegion;
        String originalDataRegionBeforeInsert = object.originalDataRegion;
        boolean isMultipartUpload = StringUtils.isBlank(object.ostorId);
        if (objectExists) {
            Triple<Long, Long, Long> triple = OpObject.getObjectSize(object);
            originalSize = triple.first();
            originalRedundantSize = triple.second();
            originalAlinSize = triple.third();
        }
        setMetadataFromPost(request, object);
        
        // 设置时间戳
        object.lastModified = Utils.getTimeStamp();
        object.timestamp = Utils.getNanoTime();
        
        // 检查expect
        String expect = request.getHeader("Expect");
        if (expect != null && expect.equalsIgnoreCase("100-continue")) {
            response.setStatus(100);
        }
        
        // 获取content type
        String contentType = MimeType.getMimeType(object.name);
        if (contentType != null && !contentType.isEmpty()) {
            object.contentType = contentType;
        } else {
            object.contentType = "application/octet-stream";
        }
        object.initialUploadId = "";
        object.partNum = new ArrayList<Integer>();
        // 写数据
        boolean hasRoam;
        try {
            hasRoam = putWithScheduling(request, response, bucket, object, bucketLog, input, false, null, null, object.size);
            bucketLog.putMetaTime = System.currentTimeMillis();
            try {
                if (client.isBusy(object, OOSConfig.getMaxConcurrencyPerRegionServer()))
                    Backoff.backoff();
                client.objectInsert(object);
            } finally {
                bucketLog.putMetaTime = System.currentTimeMillis() - bucketLog.putMetaTime;
            }
            clearOldObjectVersions(object, true, bucketLog.requestId);
        } catch (BaseException e) {
            log.error(e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new BaseException(500, "Post Object Failed");
        }
        if (objectExists) {
            pinData.sourceObject = pinData.new SourceObject(originalSize, originalAlinSize, originalRedundantSize, lastModifiedBeforeInsert, 
                    storageClassStringBeforeInsert, dataRegionBeforeInsert, originalDataRegionBeforeInsert);
            if (isMultipartUpload) {
                pinData.sourceObject.setIsMultipartUploadTrue();
            }
        }

        // 计费
        setUploadPinData(pinData, object, hasRoam);

        // 设置响应
        pinData.jettyAttributes.objectAttribute.objectSize = object.size;
        pinData.jettyAttributes.objectAttribute.length = object.size;
        response.setStatus(200);
        response.setHeader("ETag", "\"" + object.etag + "\"");
        Utils.setCommonHeader(response, new Date(bucketLog.startTime), bucketLog.requestId);
    }

    private static void checkStorageId(String storageId) throws BaseException {
        if (storageId == null || storageId.trim().length() == 0) {
            throw new BaseException(500, "put|post|copy Object Failed");
        }
    }
    
    private static void setMetadataFromRequest(HttpServletRequest request, ObjectMeta object)
            throws BaseException {
        if (request.getHeader(Headers.CACHE_CONTROL) != null
                && request.getHeader(Headers.CACHE_CONTROL).trim().length() != 0) {
            object.cacheControl = request.getHeader(Headers.CACHE_CONTROL);
        } else {
            object.cacheControl = "";
        }
        if (request.getHeader(Headers.CONTENT_DISPOSITION) != null
                && request.getHeader(Headers.CONTENT_DISPOSITION).trim().length() != 0){
            object.contentDisposition = request.getHeader(Headers.CONTENT_DISPOSITION);
        } else {
            object.contentDisposition = "";
        }
        if (request.getHeader(Headers.CONTENT_ENCODING) != null
                && request.getHeader(Headers.CONTENT_ENCODING).trim().length() != 0) {
            object.contentEncoding = request.getHeader(Headers.CONTENT_ENCODING);
        } else {
            object.contentEncoding = "";
        }
        if (request.getHeader(Headers.CONTENT_TYPE) != null
                && request.getHeader(Headers.CONTENT_TYPE).trim().length() != 0) {
            object.contentType = request.getHeader(Headers.CONTENT_TYPE);
        } else {
            object.contentType = "application/octet-stream";
        }
        if (request.getHeader("Content-Length") != null) {
            object.size = Long.parseLong(request.getHeader("Content-Length"));
        } else {
            object.size = 0;
        }
        if (request.getHeader(Headers.CONTENT_MD5) != null
                && request.getHeader(Headers.CONTENT_MD5).trim().length() != 0) {
            object.contentMD5 = request.getHeader(Headers.CONTENT_MD5);
        } else {
            object.contentMD5 = "";
        }
        String expires = request.getHeader(Headers.EXPIRES);
        if (expires != null && expires.trim().length() != 0) {
            expires = expires.trim();
            try {
                Misc.parseDateFormat(expires);
            } catch (Exception e) {
                throw new BaseException(400,ErrorMessage.ERROR_CODE_INVALID_ARGUMENT,"The 'Expires' Header Format Invalid");
            }
            object.expires = expires;
        } else {
            object.expires = "";
        }
        String websiteRedirectLocation = request.getHeader(Headers.REDIRECT_LOCATION);
        if(websiteRedirectLocation!=null){
            checkWebsiteRedirectLocation(websiteRedirectLocation);
            object.websiteRedirectLocation = websiteRedirectLocation;
        } else {
            object.websiteRedirectLocation = "";
        }
        Enumeration<?> en = request.getHeaderNames();
        Properties prop = new Properties();
        int metadataLength = 0;
        while (en.hasMoreElements()) {
            String k = (String) en.nextElement();
            if (k.startsWith(Headers.S3_USER_METADATA_PREFIX)) {
                int index = Headers.S3_USER_METADATA_PREFIX.length();
                prop.setProperty(k.substring(index), request.getHeader(k));
                metadataLength += request.getHeader(k).length();
            }
        }
        if (metadataLength > Consts.MAX_USER_METADATA_LENGTH)
            throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_ARGUMENT,
                    "The metadata size should be less than 2K");
        object.setProperties(prop);
    }
    
    private static void setMetadataFromMap(Map<String, FormField> formFields,
            ObjectMeta object) throws BaseException {
        FormField cacheControlField = formFields.get(Headers.CACHE_CONTROL.toLowerCase());
        if (cacheControlField != null
                && cacheControlField.getValue().trim().length() != 0) {
            object.cacheControl = cacheControlField.getValue();
        } else {
            object.cacheControl = "";
        }
        FormField contentDispositionField = formFields.get(Headers.CONTENT_DISPOSITION.toLowerCase());
        if (contentDispositionField != null
                && contentDispositionField.getValue().trim().length() != 0) {
            object.contentDisposition = contentDispositionField.getValue();
        } else {
            object.contentDisposition = "";
        }
        FormField contentEncodingField = formFields.get(Headers.CONTENT_ENCODING.toLowerCase());
        if (contentEncodingField != null
                && contentEncodingField.getValue().trim().length() != 0) {
            object.contentEncoding = contentEncodingField.getValue();
        } else {
            object.contentEncoding = "";
        }
        FormField contentTypeField = formFields.get(Headers.CONTENT_TYPE.toLowerCase());
        if (contentTypeField != null
                && contentTypeField.getValue().trim().length() != 0) {
            object.contentType = contentTypeField.getValue();
        } else {
            object.contentType = "application/octet-stream";
        }
        FormField eTagField = formFields.get(Headers.ETAG.toLowerCase());
        if (eTagField != null
                && eTagField.getValue().trim().length() != 0) {
            object.etag = eTagField.getValue();
        } else {
            object.etag = "";
        }
        FormField contentLengthField = formFields.get(Headers.CONTENT_LENGTH.toLowerCase());
        if (contentLengthField != null && contentLengthField.getValue().trim().length() != 0) {
            object.size = Long.parseLong(contentLengthField.getValue().trim());
        } else {
            object.size = 0;
        }
        FormField contentMd5Field = formFields.get(Headers.CONTENT_MD5.toLowerCase());
        if (contentMd5Field != null
                && contentMd5Field.getValue().trim().length() != 0) {
            object.contentMD5 = contentMd5Field.getValue();
        } else {
            object.contentMD5 = "";
        }
        FormField expiresField = formFields.get(Headers.EXPIRES.toLowerCase());
        if (expiresField != null
                && expiresField.getValue().trim().length() != 0) {
            String expires = expiresField.getValue().trim();
            try {
                Misc.parseDateFormat(expires);
            } catch (Exception e) {
                throw new BaseException(400,ErrorMessage.ERROR_CODE_INVALID_ARGUMENT,"The 'Expires' Field Format Invalid");
            }
            object.expires = expires;
        } else {
            object.expires = "";
        }
        FormField websiteRedirectLocationField = formFields.get(Headers.REDIRECT_LOCATION.toLowerCase());
        if(websiteRedirectLocationField!=null){
            String websiteRedirectLocation = websiteRedirectLocationField.getValue();
            checkWebsiteRedirectLocation(websiteRedirectLocation);
            object.websiteRedirectLocation = websiteRedirectLocation;
        } else {
            object.websiteRedirectLocation = "";
        }
        Iterator<FormField> iterator = formFields.values().iterator();
        Properties prop = new Properties();
        int metadataLength = 0;
        while (iterator.hasNext()) {
            FormField field = iterator.next();
            String k = field.getFieldName();
            if (k.startsWith(Headers.S3_USER_METADATA_PREFIX)) {
                int index = Headers.S3_USER_METADATA_PREFIX.length();
                prop.setProperty(k.substring(index), field.getValue());
                metadataLength += field.getValue().length();
            }
        }
        if (metadataLength > Consts.MAX_USER_METADATA_LENGTH)
            throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_ARGUMENT,
                    "The metadata size should be less than 2K");
        object.setProperties(prop);
    }
    
    private static void checkWebsiteRedirectLocation(String location) throws BaseException{
        if(location==null || location.trim().length()==0){
            throw new BaseException(400, "InvalidRedirectLocation", "The website redirect location must be non-empty.");
        }
        if(!location.startsWith("http://") && !location.startsWith("https://") && !location.startsWith("/")){
            throw new BaseException(400, "InvalidRedirectLocation", "The website redirect location must have a prefix of 'http://' or 'https://' or '/'.");
        }
        if(location.length()>2048){
            throw new BaseException(400, "InvalidRedirectLocation", "The length of website redirect location cannot exceed 2,048 characters.");
        }
    }

    static boolean storageClassIsVaildInAllOstors(ReplicaMode mode)
            throws BaseException {
        List<OstorProxy> tmpProxies = OstorClient.getAllProxies();
        for (OstorProxy p : tmpProxies) {
            if (!ReplicaMode.checkReplicaMode(mode, p.getOstorProxyConfig().ec_N))
                return false;
        }
        return true;
    }

    /**
     * 选取符合ReplicaMode的Ostor集群
     * @param mode
     * @return
     * @throws BaseException
     */
    static String getOstorId(ReplicaMode mode) {
        if (!OOSConfig.getOstorWeightControl().equals(Consts.OSTOR_WEIGHT_CONTROL_HAPROXY)) {
            List<OstorProxy> allProxy = OstorClient.getAllProxies();
            List<OstorProxy> tmpProxies = new ArrayList<>();
            tmpProxies.addAll(allProxy);
            Collections.shuffle(tmpProxies, ThreadLocalRandom.current());
            if (mode == null)
                return tmpProxies.get(0).getOstorID();
            for (OstorProxy p : tmpProxies) {
                if (ReplicaMode.checkReplicaMode(mode, p.getOstorProxyConfig().ec_N))
                    return p.getOstorID();
            }
            return null;
        } else {
            if (mode == null)
                return JettyServer.ostorId;
            else {
                if (ReplicaMode.checkReplicaMode(mode,
                        OstorClient.getProxy(JettyServer.ostorId).getOstorProxyConfig().ec_N))
                    return JettyServer.ostorId;
                else
                    return null;
            }
        }
    }

    /**
     * 根据owner选取ostor集群，如果owner为空或未设置集群权重，选取一个符合ReplicaMode的Ostor
     * @param ownerMeta
     * @param mode
     * @return
     * @throws BaseException
     */
    static String getOstorId(OwnerMeta ownerMeta, ReplicaMode mode) throws BaseException {
        // 优先选取设置的ostor集群
        if (ownerMeta!=null && ownerMeta.isSetOstorWeight(LOCAL_REGION_NAME)) {
            UserOstorWeightMeta ostorWeightMeta = new UserOstorWeightMeta(ownerMeta.name, LOCAL_REGION_NAME);
            try {
                if (client.userOstorWeightSelect(ostorWeightMeta)) {
                    String ostorId = ostorSelect(ostorWeightMeta.getOstorWeight(), mode);
                    if (ostorId != null) {
                        return ostorId;
                    }
                }
            } catch (IOException e) {
                log.error("io error", e);
                throw new BaseException(500, ErrorMessage.ERROR_CODE_500);
            }
        }
        return getOstorId(mode);
    }

    /**
     * 根据object size和storgeClass 获取归属的ReplicaMode
     *
     * @param size               对象大小
     * @param storageClassHeader 副本模式字符串
     * @return ReplicaMode 副本模式结构
     * @throws BaseException
     */
    static ReplicaMode getReplicaMode(long size, String storageClassHeader) throws BaseException {
        ReplicaMode mode = null;
        if (storageClassHeader != null && !storageClassHeader.equals(Consts.STORAGE_CLASS_STANDARD)
                && !storageClassHeader.equals(Consts.STORAGE_CLASS_REDUCEDREDUNDANCY)
                && !storageClassHeader.equals(Consts.STORAGE_CLASS_STANDARD_IA)) {
            mode = ReplicaMode.parser(storageClassHeader);
            if (mode == null)
                throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_STORAGECLASS,
                        ErrorMessage.ERROR_MESSAGE_INVALID_STORAGECLASS);
        }
        String ostorId = getOstorId(null, mode);
        if (ostorId == null) {
            throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_STORAGECLASS,
                    ErrorMessage.ERROR_MESSAGE_INVALID_STORAGECLASS);
        }
        Pair<Integer, ReplicaMode> pageSizeAndMode = StorageClassConfig
                .getPageSizeAndReplicaMode(size, storageClassHeader, mode, ostorId);
        return pageSizeAndMode.second();
    }

    static void setReplicaModeAndSize(ObjectMeta object, String storageClassHeader,
            boolean isMultipart, OwnerMeta owner) throws BaseException {
        ReplicaMode mode = null;
        if (storageClassHeader != null && !storageClassHeader.equals(Consts.STORAGE_CLASS_STANDARD)
                && !storageClassHeader.equals(Consts.STORAGE_CLASS_REDUCEDREDUNDANCY)
                && !storageClassHeader.equals(Consts.STORAGE_CLASS_STANDARD_IA)) {
            mode = ReplicaMode.parser(storageClassHeader);
            if (mode == null)
                throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_STORAGECLASS,
                        ErrorMessage.ERROR_MESSAGE_INVALID_STORAGECLASS);
        }
        if (!isMultipart) {
            String ostorId = getOstorId(owner, mode);
            if (ostorId == null)
                throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_STORAGECLASS,
                        ErrorMessage.ERROR_MESSAGE_INVALID_STORAGECLASS);
            log.info("selected ostorId is :" + ostorId);
            object.ostorId = ostorId;
            Pair<Integer, ReplicaMode> pageSizeAndMode = StorageClassConfig
                    .getPageSizeAndReplicaMode(object.size, storageClassHeader, mode,
                            object.ostorId);
            object.ostorPageSize = pageSizeAndMode.first();
            object.storageClass = pageSizeAndMode.second();
        } else {
            if (mode != null && !storageClassIsVaildInAllOstors(mode))
                throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_STORAGECLASS,
                        ErrorMessage.ERROR_MESSAGE_INVALID_STORAGECLASS);
            object.storageClass = null;
            object.ostorId = "";
            object.ostorPageSize = 0;
        }
        if (storageClassHeader == null || storageClassHeader.isEmpty()) {
            object.storageClassString = StorageClassConfig.getDefaultClass();
        } else {
            object.storageClassString = storageClassHeader;
        }
        log.info("object name:" + object.name + "  size:" + object.size + " mode:"
                + object.storageClass + " pageSize:" + object.ostorPageSize);
    }
    
    public static void deleteObjectStatics(HttpServletRequest request, BucketMeta bucket, ObjectMeta object,
            BucketLog bucketLog, PinData pinData)  throws BaseException {
        boolean isPublicIp = Utils.isPublicIP(bucketLog.ipAddress);
        try {
            if (Backoff.checkPutGetDeleteOperationsBackoff(HttpMethodName.DELETE, isPublicIp)
                    && !BackoffConfig.getWhiteList().contains(bucket.name))
                Backoff.backoff();
            deleteObject(request, bucket, object, bucketLog, pinData);
        } finally {
            Backoff.decreasePutGetDeleteOperations(HttpMethodName.DELETE, isPublicIp);
        }
    }

    /**
     * 删除某个特定版本的object
     * @param bucket
     * @param object
     * @param bucketLog
     * @param pinData
     * @param gwMultiVersionEnabled
     * @throws BaseException
     */
    public static void deleteObject(BucketMeta bucket, ObjectMeta object,
            BucketLog bucketLog, PinData pinData, boolean gwMultiVersionEnabled) throws BaseException {
        long totalSize = 0;
        long redundantSize = 0;
        long alinSize = 0;
        Map<InitialUploadMeta, List<UploadMeta>> map = new LinkedHashMap<>();
        Triple<Long,Long,Long> triple = deleteObjectMeta(bucket, object, map, bucketLog, gwMultiVersionEnabled);
        totalSize = triple.first();
        redundantSize = triple.second();
        alinSize = triple.third();
        pinData.setSize(totalSize, redundantSize, alinSize, object.dataRegion, object.originalDataRegion, object.storageClassString, object.getLastCostTime());
        pinData.jettyAttributes.objectAttribute.objectSize = triple.first();
        try {
            deleteObject(bucket, object, bucketLog, true, gwMultiVersionEnabled, map);
        } catch (Exception e) {
            //只要deleteObjectMeta成功了就认为删除成功了，但是存在数据没有删除成功的情况。
            log.warn("DeleteObjectMeta success, but deleteObjectData failed. ownerId : " + bucket.getOwnerId() + " MetaRegionName: " + object.metaRegionName
                    + ", bucketName: " + object.bucketName + ", objectName: " + object.name, e);
        }
    }

    public static void deleteObject(HttpServletRequest request, BucketMeta bucket, ObjectMeta object,
            BucketLog bucketLog, PinData pinData) throws BaseException {
        long totalSize = 0;
        long redundantSize = 0;
        long alinSize = 0;
        boolean gwMultiVersionEnabled = false;
        if(MultiVersionsController.gwMultiVersionEnabled(request, bucket)) {
            gwMultiVersionEnabled = true;
        }
        //如果是分段上传的object不进行大小补齐的计算
        if (StringUtils.isBlank(object.ostorId)) {
            pinData.setIsMultipartUploadTrue();
        }
        Map<InitialUploadMeta, List<UploadMeta>> map = new LinkedHashMap<InitialUploadMeta, List<UploadMeta>>();
        Triple<Long,Long,Long> triple = deleteObjectMeta(bucket, object, map, bucketLog, gwMultiVersionEnabled);
        totalSize = triple.first();
        redundantSize = triple.second();
        alinSize = triple.third();
        pinData.setSize(totalSize, redundantSize, alinSize, object.dataRegion, object.originalDataRegion, object.storageClassString, object.getLastCostTime());
        pinData.jettyAttributes.objectAttribute.objectSize = triple.first(); 
        try {
            deleteObject(bucket, object, bucketLog, true, gwMultiVersionEnabled,map);
        } catch (Exception e) {
            //只要deleteObjectMeta成功了就认为删除成功了，但是存在数据没有删除成功的情况。
            log.warn("DeleteObjectMeta success, but deleteObjectData failed. ownerId : " + bucket.getOwnerId() + " MetaRegionName: " + object.metaRegionName
                    + ", bucketName: " + object.bucketName + ", objectName: " + object.name, e);
        }        
    }

    /**
     * 删除objectMeta所有版本
     * @param object
     * @throws IOException
     * @throws BaseException
     */
    private static void deleteObjectMeta(ObjectMeta object) throws IOException, BaseException {
        List<ObjectMeta> metas = client.objectGetAllVersions(object);
        for (ObjectMeta meta : metas) {
            client.objectDelete(meta);
        }
    }
    
    private static void deleteObjectMeta(ObjectMeta object, BucketLog bucketLog) throws IOException, BaseException {
        bucketLog.deleteMetaTime = System.currentTimeMillis();
        try{
            deleteObjectMeta(object);
        }finally{
            bucketLog.deleteMetaTime = System.currentTimeMillis() - bucketLog.deleteMetaTime;
        }
    }
    
    public static Triple<Long, Long, Long> deleteObjectMeta(BucketMeta bucket, ObjectMeta object, 
            Map<InitialUploadMeta, List<UploadMeta>> map, BucketLog bucketLog, boolean gwMultiVersionEnabled) throws BaseException {
        Triple<Long, Long, Long> triple = new Triple<Long, Long, Long>(0L, 0L, 0L);
        long size = 0;
        long redundantSize = 0;
        long alinSize = 0;
        try {
            if(client.isBusy(object, OOSConfig.getMaxConcurrencyPerRegionServer()))
                Backoff.backoff();
            if (object.ostorId != null && object.ostorId.trim().length() > 0) {// 普通对象
                bucketLog.ostorId = object.ostorId;
                bucketLog.ostorKey = StringUtils.isEmpty(object.storageId) ? "-" : Storage.getStorageId(object.storageId);
                bucketLog.etag = object.etag;
                bucketLog.replicaMode = object.storageClass.toString();
                if (gwMultiVersionEnabled) {
                    client.objectDelete(object);
                    if(null != object.version && object.version.equals(VersionType.DELETE))
                        return triple;
                } else {
                    deleteObjectMeta(object, bucketLog);
                }
                size = object.size;
                redundantSize = Utils.getRedundantSize(size, object.storageClass);
                alinSize = Utils.getAlinSize(size, object.ostorPageSize, object.storageClass);
                triple = new Triple<Long, Long, Long>(size, redundantSize, alinSize);
            } else {// 分段对象
                InitialUploadMeta initialUpload = new InitialUploadMeta(bucket.metaLocation, bucket.getName(), object.name,
                        null);               
                if(client.isBusy(initialUpload, OOSConfig.getMaxConcurrencyPerRegionServer()))
                    Backoff.backoff();
                List<InitialUploadMeta> initialUploads = client
                        .initialSelectAllByObjectId(initialUpload);
                for (InitialUploadMeta initial : initialUploads) {                    
                    UploadMeta upload = new UploadMeta(bucket.metaLocation, initial.uploadId, true);
                    List<UploadMeta> uploads = client.uploadSelectByUploadId(upload);
                    map.put(initial, uploads);
                    Triple<Long, Long, Long> tripleTmp = abortMultipartUploadMeta(upload, uploads, initial);
                    triple.first(triple.first() + tripleTmp.first());
                    triple.second(triple.second() + tripleTmp.second());
                    triple.third(triple.third() + tripleTmp.third());
                }
                deleteObjectMeta(object, bucketLog);
            }
        } catch (Exception e) {
            if (e instanceof BaseException) {
                BaseException be = (BaseException)e;
                if (be.status == 503) {
                    throw be;
                }
            }
            log.error(e.getMessage(), e);
            throw new BaseException(500, "Delete Object Failed");
        }
        return triple;
    }
    
    public static void deleteObject(BucketMeta bucket, ObjectMeta object,
            BucketLog bucketLog, boolean isSync, boolean gwMultiVersionEnabled, Map<InitialUploadMeta, List<UploadMeta>> map) throws BaseException {

        try {
            if (object.ostorId != null && object.ostorId.trim().length() > 0) {// 普通对象                
                String ostorKey = Storage.getStorageId(object.storageId);
                try {
                    if (object.dataRegion.equals(LOCAL_REGION_NAME)) {
                        Storage storage = new Storage(bucketLog, object.ostorId);
                        storage.delete(ostorKey, object.size, object.name, object.ostorPageSize,
                                object.storageClass, isSync);
                    } else {
                        internalClient.delete(object.dataRegion, ostorKey, object.ostorId,
                                object.size, object.ostorPageSize, object.storageClass, bucketLog.requestId);
                    }
                } catch (Exception e) {
                    log.error("catch delete object storage error:" + object.storageId);
                    log.error(e.getMessage(), e);
                }
            } else {// 分段对象
                bucketLog.exception = 4;
                // 先删除元数据是因为后面的part删除的失败率更大些
//                deleteObjectMeta(object, bucketLog);                    
                Set<InitialUploadMeta> initialUploads = map.keySet();
                boolean first = true;
                for (InitialUploadMeta initial : initialUploads) {
                    UploadMeta upload = new UploadMeta(bucket.metaLocation, initial.uploadId, true);
                    if(first && client.isBusy(upload, OOSConfig.getMaxConcurrencyPerRegionServer()))
                        Backoff.backoff();
                    first = false;
                    List<UploadMeta> uploads = map.get(initial);
                    abortMultipartUpload(object, upload, uploads, initial, bucketLog);
                }
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new BaseException(500, "Delete Object Failed");
        }
    }

    /**
     * 按照dataRegions指定的顺序尝试复制对象
     * @param dataRegions
     * @param srcBucket
     * @param srcObj
     * @param dstBucket
     * @param dstObj
     * @param storageClass
     * @param bucketLog
     * @param sourceObjectSize 源对象大小
     * @param isMultiObject 是否是分片对象
     * @param isCompleteObject 是否是分片 已合并对象
     * @return Pair<Boolean, Boolean>
     * 第一个boolean true表示成功，false表示所有数据域均已写满
     * 第二个boolean true表示执行了ostor操作，false只是需要处理meta
     * @throws BaseException
     * @throws IOException
     */
    public static Pair<Boolean, Boolean> tryCopy(List<String> dataRegions, BucketMeta srcBucket, ObjectMeta srcObj, OwnerMeta owner, BucketMeta dstBucket, ObjectMeta dstObj, String storageClass, String reqStorageClass, BucketLog bucketLog,
            long sourceObjectSize,
            boolean isMultiObject , boolean isCompleteObject, String metaDataDirective)
            throws BaseException, IOException {
        ReplicaMode dstMode = getReplicaMode(srcObj.size, storageClass);
        for (String dataRegion : dataRegions) {
            if (srcBucket.name.equals(dstBucket.name) &&
                    srcObj.name.equals(dstObj.name)){
                if (srcObj.dataRegion.equals(dataRegion)){
                    if ((!isMultiObject && Objects.equals(srcObj.storageClass, dstMode)) ||    // 普通对象
                            (isMultiObject && !isCompleteObject) ||   // 分段对象未合并 携带storageClass会报错，未携带执行到这里只拷贝meta
                            (isMultiObject && isCompleteObject && (storageClass.equals(srcObj.storageClassString) || null == reqStorageClass))) { // 分段对象已经合并，请求头strorageClass为空，或者和源对象一样，都只处理meta
                        dstObj.ostorPageSize = srcObj.ostorPageSize;
                        dstObj.storageClass = srcObj.storageClass;
                        // 如果 分段对象 是replace并且未携带storage-class头，未生成新对象，则保持原storage-class
                        if ("REPLACE".equals(metaDataDirective) && isMultiObject && StringUtils.isEmpty(reqStorageClass)) {
                            dstObj.storageClassString = srcObj.storageClassString;
                        }else{
                            dstObj.storageClassString = storageClass;
                        }
                        dstObj.ostorId = srcObj.ostorId;
                        dstObj.etag = srcObj.etag;
                        dstObj.dataRegion = dataRegion;
                        dstObj.storageId = srcObj.storageId;
                        dstObj.initialUploadId = srcObj.initialUploadId;
                        dstObj.partNum = srcObj.partNum;
                        bucketLog.ostorId = dstObj.ostorId;
                        bucketLog.ostorKey = StringUtils.isEmpty(srcObj.storageId) ? "-" : Storage.getStorageId(dstObj.storageId);
                        bucketLog.etag = dstObj.etag;
                        bucketLog.replicaMode = dstObj.storageClass == null ? "" : dstObj.storageClass.toString();
                        return new Pair<>(true, false);
                    }
                }
            }
            if (internalClient.canWriteRegion(dataRegion)) {
                bucketLog.metaRegion = "sourceObject:" + srcObj.metaRegionName + "-object:" + dstObj.metaRegionName;
                bucketLog.dataRegion = "sourceObject:" + srcObj.dataRegion + "-object:" + dataRegion;
                //源对象和目标对象都不在本地
                if (!srcObj.dataRegion.equals(LOCAL_REGION_NAME) && !dataRegion.equals(LOCAL_REGION_NAME)) {
                    WriteResult result = internalClient
                            .copy(srcObj.dataRegion, dataRegion, srcBucket.name, srcObj.name, dstBucket.name, dstObj.name,
                                    storageClass, null, bucketLog.requestId);
                    dstObj.ostorPageSize = result.pageSize;
                    dstObj.storageClass = result.replicaMode;
                    dstObj.storageClassString = storageClass;
                    dstObj.ostorId = result.ostorId;
                    dstObj.etag = result.etag;
                    dstObj.dataRegion = dataRegion;
                    dstObj.storageId = Storage.setStorageId(result.ostorKey);
                    bucketLog.ostorId = result.ostorId;
                    bucketLog.ostorKey = result.ostorKey;
                    bucketLog.etag = result.etag;
                    bucketLog.replicaMode = result.replicaMode == null ? "" : result.replicaMode.toString();
                    return new Pair<>(true, true);
                }
                //源对象或目标对象在本地
                InputStream input = null;
                try {
                    input = getObjectInputStream(srcObj, 0, -1, null, bucketLog);
                    putObjectToRegion(dataRegion, input, owner, dstBucket, dstObj, storageClass, sourceObjectSize, false,
                            null, bucketLog, Consts.USER_NO_LIMIT_RATE);
                } finally {
                    if (input != null) {
                        try {
                            input.close();
                        } catch (IOException e) {
                            log.error(e.getMessage(), e);
                        }
                    }
                }
                return new Pair<>(true, true);
            }

        }
        return new Pair<>(false, false);
    }

    public static void copyObject(HttpServletRequest request, HttpServletResponse response, BucketMeta bucket,
            ObjectMeta object, BucketMeta sourceBucket, ObjectMeta sourceObject, BlockingExecutor executor,
            BucketLog bucketLog, boolean objectExists, PinData pinData) throws BaseException, IOException {
        long originalSize = 0;
        long originalRedundantSize = 0;
        long originalAlinSize = 0;
        long originalLastModified = object.getLastCostTime();
        String originalStorageClass = object.storageClassString;
        String dataRegionBeforeInsert = object.dataRegion;
        String originalDataRegionBeforeInsert = object.originalDataRegion;
        object.lastModified = Utils.getTimeStamp();
        boolean isMultipartUpload = StringUtils.isBlank(object.ostorId);
        //获取初始的对象大小
        if (objectExists) {
            Triple<Long, Long, Long> triple = getObjectSize(object);
            originalSize = triple.first();
            originalRedundantSize = triple.second();
            originalAlinSize = triple.third();
        }
        
        checkIfCanOperate(request, sourceBucket, sourceObject);
        // x_amz_metadata_directive的默认值为COPY
        String metaDataDirective = request.getHeader(Headers.METADATA_DIRECTIVE);
        if (metaDataDirective == null) {
            metaDataDirective = "COPY";
        }

       // 如果 源bucket、object 和目的 bucket、object相同 且 x_amz_metadata_directive 是copy 且 storage-class 空
        String storageClass = request.getHeader(Headers.STORAGE_CLASS);
        if (bucket.getName().equals(sourceBucket.getName()) && object.name.equals(sourceObject.name)
                && null == storageClass && metaDataDirective.equals("COPY")) {
            throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_REQUEST,
                    ErrorMessage.ERROR_MESSAGE_COPY_TO_ITSELF_NO_CHANGE);
        }

        boolean isMultiObject = false;  // 是分段对象
        boolean isCompleteObject = false;  // 是否合并
        if (sourceObject.ostorId == null || sourceObject.ostorId.trim().length() == 0) {// 分段上传对象
            isMultiObject = true;
            if (sourceObject.storageId != null && sourceObject.storageId.trim().length() > 0) {// 未合并的分段片段
                isCompleteObject = true;
            }
        }

        // 如果 源bucket、object 和目的 bucket、object相同 且 未合并的分段对象 且携带 storage-class 头域
        if (bucket.getName().equals(sourceBucket.getName()) && object.name.equals(sourceObject.name)) {
            if (isMultiObject && !isCompleteObject && null != storageClass) {
                throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_STORAGECLASS,
                        ErrorMessage.ERROR_MESSAGE_CAN_NOT_SPECIFY_STORAGE_CLASS);
            }
        }

        //如果storage-class 空 设置为标准
        if(null == storageClass) {
            storageClass = StorageClassConfig.getDefaultClass();
        }

        // 设置object属性 如果x_amz_metadata_directive为copy 使用原对象meta；否则使用header携带meta
        object.cacheControl = sourceObject.cacheControl;
        object.contentDisposition = sourceObject.contentDisposition;
        object.contentEncoding = sourceObject.contentEncoding;
        object.contentType = sourceObject.contentType;
        object.etag = sourceObject.etag;
        object.contentMD5 = sourceObject.contentMD5;
        object.expires = sourceObject.expires;
        object.websiteRedirectLocation = sourceObject.websiteRedirectLocation;
        object.setProperties(sourceObject.getProperties());
        if (!metaDataDirective.equals("COPY")) {
            setMetadataFromRequest(request, object);
            // replace 时，忽略用户设置的content-md5请求头
            object.contentMD5 = "";
        }
        long sourceObjectSize = getObjectSize(sourceObject).first();
        object.size =  sourceObjectSize;
        object.timestamp = Utils.getNanoTime();
        
        AtomicBoolean finished = new AtomicBoolean(false);
        try {
            //拷贝对象过程可能比较长，所以向客户端发送心跳
            executor.execute(new ResponseThread(response, finished));
            OwnerMeta owner = new OwnerMeta(bucket.getOwnerId());
            client.ownerSelectById(owner);
            // 获取数据域
            Set<String> ownerDataRegions = client.getDataRegions(bucket.getOwnerId());
            CtyunBucketDataLocation dataLocation = getDataLocation(request, bucket, ownerDataRegions);
            List<String> objectDataRegions = dataLocation.getDataRegions();

            // 复制对象
            List<String> dataRegionsToTry;
            if (dataLocation.getType() == CtyunBucketDataType.Local) {
                object.originalDataRegion = LOCAL_REGION_NAME;
                dataRegionsToTry = Collections.singletonList(LOCAL_REGION_NAME);
            } else {
                object.originalDataRegion = objectDataRegions.get(0);
                dataRegionsToTry = objectDataRegions;
            }
            Pair<Boolean, Boolean> retTryCopy = tryCopy(dataRegionsToTry, sourceBucket, sourceObject, owner, bucket, object, storageClass, request.getHeader(Headers.STORAGE_CLASS),
                    bucketLog, sourceObjectSize, isMultiObject, isCompleteObject, metaDataDirective);

            // 调度
            if (!retTryCopy.first()) {
                if (dataLocation.getStragegy() == CtyunBucketDataScheduleStrategy.Allowed) {
                    List<String> schedulingDataRegions = getSchedulingRegions(dataRegionsToTry, ownerDataRegions);
                    retTryCopy = tryCopy(schedulingDataRegions, sourceBucket, sourceObject, owner, bucket, object, storageClass, request.getHeader(Headers.STORAGE_CLASS),
                            bucketLog, sourceObjectSize, isMultiObject, isCompleteObject, metaDataDirective);
                    if (!retTryCopy.first()) {
                        throw new BaseException(500, ErrorMessage.ERROR_CODE_500);
                    }
                } else {
                    throw new BaseException(500, ErrorMessage.ERROR_CODE_500);
                }
            }
            if (sourceBucket.name.equals(bucket.name)
                    && sourceObject.name.equals(object.name)
                    && !sourceObject.storageClassString.equals(object.storageClassString)) {
                // 由其他转为低频或归档 或 由低频或归档转为其他
                if (Utils.STORAGE_TYPES_RESTORE.contains(object.storageClassString)
                        || Utils.STORAGE_TYPES_RESTORE.contains(sourceObject.storageClassString)) {
                    object.transitionTime = object.lastModified;
                }
            }

            if (sourceObject.ostorId != null && sourceObject.ostorId.trim().length() > 0
                    && !sourceObject.etag.equals(object.etag)) {
                throw new BaseException(500, ErrorMessage.ERROR_CODE_500);
            }
            bucketLog.putMetaTime = System.currentTimeMillis();
            try {
                if (client.isBusy(object, OOSConfig.getMaxConcurrencyPerRegionServer()))
                    Backoff.backoff();
                client.objectInsert(object);
            } finally {
                bucketLog.putMetaTime = System.currentTimeMillis() - bucketLog.putMetaTime;
            }

            // 如果是自己拷贝自己的情况下，没有发生数据拷贝，只需要清理meta
            if (!retTryCopy.second()) {  // 只清理meta数据
                clearOldObjectMetaVersions(object, bucketLog.requestId);
            } else {
                clearOldObjectVersions(object, true, bucketLog.requestId);
            }

            // 计费
            if (sourceObject.dataRegion.equals(object.dataRegion) ||
                    sourceObject.dataRegion.equals(object.originalDataRegion)) {
                pinData.setDirectUpload(object.size, object.dataRegion, object.storageClassString);
            } else {
                pinData.setRoamUpload(object.size, object.dataRegion, object.storageClassString);
            }
            
            if (Utils.STORAGE_TYPES_RESTORE.contains(sourceObject.storageClassString) && retTryCopy.second()) {
                pinData.setRestore(sourceObjectSize, sourceObject.dataRegion, sourceObject.storageClassString, sourceBucket.getName());
            }
            
            if (objectExists) {
                pinData.sourceObject = pinData.new SourceObject(originalSize, originalAlinSize, originalRedundantSize, originalLastModified,
                        originalStorageClass, dataRegionBeforeInsert, originalDataRegionBeforeInsert);
                if (isMultipartUpload) {
                    pinData.sourceObject.setIsMultipartUploadTrue();
                }
            }
            
            Triple<Long, Long, Long> triple = getObjectSize(object);
            long objectSize = triple.first();
            long redundantSize = triple.second();
            long alinSize = triple.third();
            pinData.setSize(objectSize, redundantSize, alinSize, object.dataRegion, object.originalDataRegion, object.storageClassString, object.getLastCostTime());
            if (StringUtils.isBlank(object.ostorId)) {
                pinData.setIsMultipartUploadTrue();
            }
            synchronized (response) {
                response.setStatus(200);
                setExpirationHeader(response, bucket, object);
                setCopyObjectSuccess(response, new Date(), sourceObject.etag);
            }
        } catch (BaseException e) {
            log.error(e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new BaseException(500, "Copy Object Failed");
        } finally {
            finished.set(true);
        }
    }
    
    private static void checkIfCanOperate(HttpServletRequest request, BucketMeta bucket,
            ObjectMeta object) throws BaseException {
        String if_modified_since = null;
        String if_unmodified_since = null;
        String if_match = null;
        String if_none_match = null;
        String x_amz_metadata_directive = null;
        boolean isGet = true;
        if (request.getMethod().equalsIgnoreCase(HttpMethod.GET.toString())
                || request.getMethod().equalsIgnoreCase(HttpMethod.HEAD.toString())) {
            if_modified_since = request.getHeader(Headers.GET_OBJECT_IF_MODIFIED_SINCE);
            if_unmodified_since = request.getHeader(Headers.GET_OBJECT_IF_UNMODIFIED_SINCE);
            if_match = request.getHeader(Headers.GET_OBJECT_IF_MATCH);
            if_none_match = request.getHeader(Headers.GET_OBJECT_IF_NONE_MATCH);
        } else if (request.getMethod().equalsIgnoreCase(HttpMethod.PUT.toString())) {
            x_amz_metadata_directive = request.getHeader(Headers.METADATA_DIRECTIVE);
            if_modified_since = request.getHeader(Headers.COPY_SOURCE_IF_MODIFIED_SINCE);
            if_unmodified_since = request.getHeader(Headers.COPY_SOURCE_IF_UNMODIFIED_SINCE);
            if_match = request.getHeader(Headers.COPY_SOURCE_IF_MATCH);
            if_none_match = request.getHeader(Headers.COPY_SOURCE_IF_NO_MATCH);
            isGet = false;
        }
        /* 与产品确认 时间比较时 舍弃 毫秒信息 */
        Date lastModifiedDate = new Date(object.lastModified/1000*1000);
        String etag = object.etag;
        if (if_modified_since != null) {
            Date date = null;
            try {
                date = TimeUtils.fromGMTFormat(if_modified_since);
            } catch (ParseException e) {
                throw new BaseException();
            }
            // date >= lastModifiedDate
            if (!date.before(lastModifiedDate)) {
                if (isGet) {
                    Map<String, String> responseHeader = null;
                    try {
                        responseHeader = getObjectMetaMap(bucket, object);
                    } catch (UnsupportedEncodingException e) {
                        throw new BaseException();
                    }
                    throw new BaseException(304, "NotModified", responseHeader);
                } else {
                    throw new BaseException(412, "PreconditionFailed",
                            "The pre-conditions "+Headers.COPY_SOURCE_IF_MODIFIED_SINCE+" you specified did not hold");
                }
            }
        }
        if (if_unmodified_since != null) {
            Date date = null;
            try {
                date = TimeUtils.fromGMTFormat(if_unmodified_since);
            } catch (ParseException e) {
                throw new BaseException();
            }
            //If-Unmodified-Since<对象修改时间，那么返回412，
            //如果If-Unmodified-Since>=对象修改时间，那么返回对象
            if (date.before(lastModifiedDate)) {
                if (isGet) {
                    throw new BaseException(412, "PreconditionFailed",
                            "The pre-conditions "+Headers.GET_OBJECT_IF_UNMODIFIED_SINCE+" you specified did not hold");
                } else {
                    throw new BaseException(412, "PreconditionFailed",
                            "The pre-conditions "+Headers.COPY_SOURCE_IF_UNMODIFIED_SINCE+" you specified did not hold");
                }
            }
        }
        if (if_match != null && etag!=null && etag.trim().length()>1 && !etag.equals(StringUtils.strip(if_match,"\""))) {
            throw new BaseException(412, "precondition failed");
        }
        if (if_none_match != null && etag!=null && etag.trim().length()>0 && etag.equals(StringUtils.strip(if_none_match,"\""))) {
            if (isGet) {
                Map<String, String> responseHeader = null;
                try {
                    responseHeader = getObjectMetaMap(bucket, object);
                } catch (UnsupportedEncodingException e) {
                    throw new BaseException();
                }
                throw new BaseException(304, "NotModified", responseHeader);
            }else {
                throw new BaseException(412, "precondition failed");
            }
        }
        if (x_amz_metadata_directive != null && !x_amz_metadata_directive.equals("COPY")
                && !x_amz_metadata_directive.equals("REPLACE")) {
            throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_ARGUMENT);
        }
    }

    private static Map<String, String> getObjectMetaMap(BucketMeta bucketMeta,ObjectMeta objectMeta) throws UnsupportedEncodingException{
        Map<String, String> map = new HashMap<>();
        map.put(Headers.LAST_MODIFIED, dateUtils.formatRfc822Date(new Date(objectMeta.lastModified)));
        if (StringUtils.isEmpty(objectMeta.etag)) {
            map.put(Headers.ETAG, "\"-\"");
        }else{
            map.put(Headers.ETAG, "\"" + objectMeta.etag + "\"");
        }
        map.put("Expires", objectMeta.expires);
        map.put("x-ctyun-metadata-location", bucketMeta.metaLocation);
        map.put("x-ctyun-data-location", objectMeta.dataRegion);
        map.put(Headers.CACHE_CONTROL, objectMeta.cacheControl);
        map.put("x-amz-expiration", getExpiration(bucketMeta, objectMeta));
        Properties properties = objectMeta.getProperties();
        for (String name : properties.stringPropertyNames()) {
            map.put(Headers.S3_USER_METADATA_PREFIX + name, properties.getProperty(name));
        }
        return map;
    }

    private static void setCopyObjectSuccess(HttpServletResponse response, Date lastModifiedDate,
            String ETag) throws BaseException, IOException {
        response.setHeader(Headers.CONTENT_TYPE, "application/xml");
        XmlWriter xml = new XmlWriter();
        xml.start("CopyObjectResult");
        xml.start("LastModified").value(ServiceUtils.formatIso8601Date(lastModifiedDate)).end();
        xml.start("ETag").value(ETag).end();
        xml.end();
        try {
            response.getOutputStream().write(xml.toString().getBytes(Consts.CS_UTF8));
            response.flushBuffer();
        } catch (Exception e) {
            log.error("catch write response error");
        }
        
    }
    
    /**
     * 找出dataRegions中第一个未写满的数据域
     * @param dataRegions
     * @return null表示所有数据域均已写满
     * @throws BaseException
     */
    private static String selectDataRegion(List<String> dataRegions) throws BaseException {
        for (String region : dataRegions) {
            if (internalClient.canWriteRegion(region)) {
                return region;
            }
        }
        return null;
    }
    
    public static String initiateMultipartUpload(HttpServletRequest request,
            HttpServletResponse response, BucketMeta bucket, ObjectMeta object, OwnerMeta owner)
            throws BaseException, IOException {
        // set metadata
        setMetadataFromRequest(request, object);
        String storageClassHeader = request.getHeader(Headers.STORAGE_CLASS);
        long timestamp = Utils.getTimeStamp();
        long ts = Utils.getNanoTime();
        String uploadId = String.valueOf(ts);
        InitialUploadMeta initialUpload = new InitialUploadMeta(bucket.metaLocation, bucket.getName(), object.name, uploadId, false);
        if (owner != null)
            initialUpload.initiator = owner.getId();
        else
            initialUpload.initiator = 0;
        initialUpload.initiated = timestamp;
        if (storageClassHeader != null)
            initialUpload.storageClass = storageClassHeader;
        object.timestamp = ts;
        if (request.getHeader(Consts.X_CTYUN_LASTMODIFIED) != null)
            object.lastModified = Long.valueOf(request.getHeader(Consts.X_CTYUN_LASTMODIFIED));
        else
            object.lastModified = System.currentTimeMillis();
        try {
            OwnerMeta bucketOwner = new OwnerMeta(bucket.ownerId);
            setReplicaModeAndSize(object, storageClassHeader, true, bucketOwner);
            Set<String> ownerDataRegions = client.getDataRegions(bucket.getOwnerId());
            CtyunBucketDataLocation dataLocation = getDataLocation(request, bucket, ownerDataRegions);
            List<String> objectDataRegions = dataLocation.getDataRegions();

            List<String> dataRegionsToTry;
            if (dataLocation.getType() == CtyunBucketDataType.Local) {
                object.originalDataRegion = LOCAL_REGION_NAME;
                dataRegionsToTry = Collections.singletonList(LOCAL_REGION_NAME);
            } else {
                if ((objectDataRegions !=null ) && (objectDataRegions.size()>0)) {
                    object.originalDataRegion = objectDataRegions.get(0);
                } else {
                    throw new BaseException(500, ErrorMessage.ERROR_MESSAGE_500,"originalDataRegion is empty!");
                }
                dataRegionsToTry = objectDataRegions;
            }
            String dataRegion = selectDataRegion(dataRegionsToTry);
            boolean needScheduling = false;
            if (dataRegion == null) {
                needScheduling = true;
            } else {
                object.dataRegion = dataRegion;
            }

            if (needScheduling) {
                if (dataLocation.getStragegy() == CtyunBucketDataScheduleStrategy.Allowed) {
                    List<String> schedulingDataRegions = getSchedulingRegions(dataRegionsToTry, ownerDataRegions);
                    String scheduledDataRegion = selectDataRegion(schedulingDataRegions);
                    if (scheduledDataRegion == null) {
                        throw new BaseException(500, ErrorMessage.ERROR_MESSAGE_500);
                    } else {
                        object.dataRegion = scheduledDataRegion;
                    }
                } else {
                    throw new BaseException(500, ErrorMessage.ERROR_MESSAGE_500);
                }
            }

            object.storageId = "";
            object.initialUploadId = "";
            object.contentMD5="";
            object.etag="";
            object.size = 0;
            object.partNum = new ArrayList<Integer>();
            if(client.isBusy(object, OOSConfig.getMaxConcurrencyPerRegionServer()))
                Backoff.backoff();
            if(client.isBusy(initialUpload, OOSConfig.getMaxConcurrencyPerRegionServer()))
                Backoff.backoff();
            client.objectInsert(object);
            client.initialUploadInsert(initialUpload);
            XmlWriter xml = new XmlWriter();
            xml.start("InitiateMultipartUploadResult", "xmlns", Consts.XMLNS);
            xml.start("Bucket").value(bucket.getName()).end();
            xml.start("Key").value(object.name).end();
            xml.start("UploadId").value(uploadId).end();
            xml.end();
            response.setStatus(200);
            String str = Consts.XML_HEADER + xml.toString();
            str = str.replaceAll("&quote;", "&quot;");
            return str;
        } catch (BaseException e) {
            throw e;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new BaseException(500, "Initiate Multipart Upload Failed");
        }
    }
    
    public static void uploadPartStatics(HttpServletRequest request, HttpServletResponse response,
            BucketMeta bucket, ObjectMeta object, UploadMeta upload, InputStream input,
            BucketLog bucketLog, InitialUploadMeta initialUpload, PinData pinData) throws BaseException, IOException {
        boolean isPublicIp = Utils.isPublicIP(bucketLog.ipAddress);
        try {
            if (Backoff.checkPutGetDeleteOperationsBackoff(HttpMethodName.PUT, isPublicIp) && !BackoffConfig.getWhiteList().contains(bucket.name))
                Backoff.backoff();
            if ((!BackoffConfig.isEnableStreamSlowDown())
                    && (JettyServer.inComingBytes>BackoffConfig.getMaxIncomingBytes() && !BackoffConfig.getWhiteList().contains(bucket.name))){
                Backoff.backoff();
            }
            if (BackoffConfig.getBandwidthBackoff() == 1
                    && Backoff.checkBandwidthBackoff(true, isPublicIp, bandwidthBackoff) && !BackoffConfig.getWhiteList().contains(bucket.name))
                Backoff.backoff();
            uploadPart(request, response, bucket, object, upload, input, bucketLog, initialUpload,pinData);
            bucketLog.exception = 3;
        } finally {
            Backoff.decreasePutGetDeleteOperations(HttpMethodName.PUT, isPublicIp);
        }
    }

    static boolean getMultipartReplicaMeta(ObjectMeta object,
            InitialUploadMeta initialUpload, long length, UploadMeta upload, long ownerId)
            throws BaseException {
        String ostorId = null;
        boolean result;
        for (String id : initialUpload.ostorIds) {
            if (OstorClient.contains(id)) {
                ostorId = id;
                break;
            }
        }
        ReplicaMode mode = null;
        if (initialUpload.storageClass != null
                && !initialUpload.storageClass.equals(Consts.STORAGE_CLASS_STANDARD)
                && !initialUpload.storageClass.equals(Consts.STORAGE_CLASS_REDUCEDREDUNDANCY)) {
            mode = ReplicaMode.parser(initialUpload.storageClass);
        }
        if (ostorId == null) {
            OwnerMeta owner = new OwnerMeta(ownerId);
            try {
                client.ownerSelectById(owner);
            } catch (Exception e) {
                log.error("owner select error", e);
                throw new BaseException(500, ErrorMessage.ERROR_CODE_500);
            }
            ostorId = getOstorId(owner, mode);
            initialUpload.ostorIds.add(ostorId);
            result = true;
        } else
            result = false;
        Pair<Integer, ReplicaMode> pageSizeAndReplica = StorageClassConfig
                .getPageSizeAndReplicaMode(length, initialUpload.storageClass, mode, ostorId);
        upload.ostorId = ostorId;
        upload.pageSize = pageSizeAndReplica.first();
        upload.replicaMode = pageSizeAndReplica.second();
        return result;
    }
    
    public static void uploadPart(HttpServletRequest request, HttpServletResponse response,
            BucketMeta bucket, ObjectMeta object, UploadMeta upload, InputStream input,
            BucketLog bucketLog, InitialUploadMeta initialUpload, PinData pinData) throws BaseException, IOException {
        long ts = Utils.getTimeStamp();
        EtagMaker etagmaker = new EtagMaker();
        if(client.isBusy(upload, OOSConfig.getMaxConcurrencyPerRegionServer()))
            Backoff.backoff();
        if(client.isBusy(initialUpload, OOSConfig.getMaxConcurrencyPerRegionServer()))
            Backoff.backoff();
        boolean ref = client.uploadSelect(upload);// 查询upload表，统计时间
        if (request.getHeader(Headers.CONTENT_MD5) != null
                && request.getHeader(Headers.CONTENT_MD5).trim().length() != 0)
            upload.contentMD5 = request.getHeader(Headers.CONTENT_MD5);
        long length = 0;
        if (request.getHeader("Content-Length") != null)
            length = Long.parseLong(request.getHeader("Content-Length"));
        else
            throw new BaseException(411, ErrorMessage.ERROR_CODE_MISSING_CONTENT_LENGTH);
        if (request.getHeader("Expect") != null
                && request.getHeader("Expect").equalsIgnoreCase("100-continue")) {
            response.setStatus(100);
        }
        
        int limitRate = Consts.USER_NO_LIMIT_RATE;
        String rate = HttpLimitParser.getLimit(request, Consts.X_AMZ_LIMIT_RATE_INDEX);
        if(StringUtils.isNotBlank(rate))
            limitRate = validLimitRate(rate.replace("rate=", ""));
        
        long originalSize = 0;
        long originalRedundantSize = 0;
        long originalAlinSize = 0;
        long lastModify = upload.lastModified;
        String originalStorageClass = object.storageClassString;
        String dataRegionBeforeInsert = object.dataRegion;
        String originalDataRegionBeforeInsert = object.originalDataRegion;

        upload.lastModified = ts;
        String storageId = null;
        String userMeta = getUserMeta(object, bucket.getOwnerId());
        upload.timestamp = Utils.getNanoTime();
        try {
            if (ref) {
                originalSize = upload.size;
                originalRedundantSize = Utils.getRedundantSize(originalSize, upload.replicaMode);
                originalAlinSize = Utils.getAlinSize(originalSize, upload.pageSize,
                        upload.replicaMode);
            }
            if (object.dataRegion.equals(LOCAL_REGION_NAME)) {
                boolean result = getMultipartReplicaMeta(object, initialUpload, length, upload,bucket.getOwnerId());
                bucketLog.ostorId = upload.ostorId;
                Storage storage = new Storage(bucketLog, etagmaker, upload.ostorId);
                storageId = storage.create(input, length, userMeta, object.name, null,
                        upload.pageSize, upload.replicaMode, object.bucketName, limitRate);
                checkStorageId(storageId);
                bucketLog.ostorKey = storageId;
                upload.storageId = Storage.setStorageId(storageId);
                upload.etag = etagmaker.digest();
                if (result)
                    client.initialUploadInsert(initialUpload);
            } else {
                WriteResult putResult = internalClient.write(object.dataRegion, object.bucketName, object.name, 
                        initialUpload.storageClass, length, input, false, limitRate, bucketLog.requestId);
                checkStorageId(putResult.ostorKey);
                bucketLog.ostorKey = putResult.ostorKey;
                bucketLog.ostorId = putResult.ostorId;
                storageId = putResult.ostorKey;
                upload.storageId = Storage.setStorageId(putResult.ostorKey);
                upload.etag = putResult.etag;
                upload.ostorId = putResult.ostorId;
                upload.replicaMode = putResult.replicaMode;
                upload.pageSize = putResult.pageSize;
                if (!initialUpload.ostorIds.contains(putResult.ostorId)) {
                    initialUpload.ostorIds.add(putResult.ostorId);
                    client.initialUploadInsert(initialUpload);
                }
            }
            bucketLog.etag = upload.etag;
            bucketLog.replicaMode = upload.replicaMode.toString();
            if (upload.contentMD5 != null && upload.contentMD5.trim().length() != 0) {
                checkMd5(upload.contentMD5, upload.etag);
            }
            upload.size = length;
            client.uploadInsert(upload);
            clearOldUploadVersions(upload, object, bucketLog);
            response.setStatus(200);
            response.setHeader("ETag", "\"" + upload.etag + "\"");
            
            if (ref) {
                pinData.sourceObject = pinData.new SourceObject(originalSize, originalAlinSize, originalRedundantSize, lastModify,
                        originalStorageClass, dataRegionBeforeInsert, originalDataRegionBeforeInsert);
                pinData.sourceObject.setIsMultipartUploadTrue();
            }

            if (object.dataRegion.equals(LOCAL_REGION_NAME) || object.originalDataRegion.equals(LOCAL_REGION_NAME)) {
                pinData.setDirectUpload(length, object.dataRegion, object.storageClassString);
            } else {
                pinData.setRoamUpload(length, object.dataRegion, object.storageClassString);
            }
            long redundantSize = Utils.getRedundantSize(length, upload.replicaMode);
            long alinSize = Utils.getAlinSize(length, upload.pageSize, upload.replicaMode);
            pinData.setSize(length, redundantSize, alinSize,
                    object.dataRegion, object.originalDataRegion, object.storageClassString, upload.lastModified);
            pinData.setIsMultipartUploadTrue();
            pinData.jettyAttributes.objectAttribute.length = length;
        } catch (Exception error) {
            log.error(error.getMessage(), error);
            if (storageId != null && !ref) {
                if (object.dataRegion.equals(LOCAL_REGION_NAME)) {
                    Storage storage = new Storage(bucketLog, upload.ostorId);
                    storage.delete(storageId, length, upload.initialUploadId, upload.pageSize,
                            upload.replicaMode, true);
                } else {
                    internalClient.delete(object.dataRegion, storageId, upload.ostorId, length,
                            upload.pageSize, upload.replicaMode, bucketLog.requestId);
                }
            }
            if (error instanceof BaseException)
                throw (BaseException) error;
            throw new BaseException(500, "Upload Part Failed");
        }
    }

    private static void deleteUpload(UploadMeta uploadM, ObjectMeta object, BucketLog bucketLog)
            throws BaseException {
        String ostorKey = Storage.getStorageId(uploadM.storageId);
        String ostorId = uploadM.ostorId;
        int pageSize = uploadM.pageSize;
        ReplicaMode rm = uploadM.replicaMode;
        if (object.dataRegion.equals(LOCAL_REGION_NAME)) {
            Storage storage = new Storage(bucketLog, ostorId);
            storage.delete(Storage.getStorageId(uploadM.storageId), uploadM.size, uploadM.initialUploadId, pageSize, rm,
                    true);
        } else {
            internalClient.delete(object.dataRegion, ostorKey, ostorId, uploadM.size, pageSize, rm, bucketLog.requestId);
        }
    }

    private static void clearOldUploadVersions(UploadMeta upload, ObjectMeta object, BucketLog bucketLog) throws BaseException { 
        boolean flag = false;
        try {
            List<UploadMeta> oldUploads = client.uploadGetAllVersions(upload);
            if (oldUploads.size() > 1) {
                for (UploadMeta uploadM : oldUploads) {
                    try {
                        if (uploadM.timestamp < upload.timestamp
                                && upload.timestamp - uploadM.timestamp > OOSConfig
                                        .getPutObjectDeltaTime() * 1000000) {
                            client.uploadDelete(uploadM);
                            deleteUpload(uploadM, object, bucketLog);
                        } else if (uploadM.timestamp > upload.timestamp
                                && uploadM.timestamp - upload.timestamp > OOSConfig
                                        .getPutObjectDeltaTime() * 1000000) {
                            client.uploadDelete(upload);
                            deleteUpload(upload, object, bucketLog);
                        } else if (uploadM.timestamp != upload.timestamp
                                && (Math.abs(uploadM.timestamp - upload.timestamp) <= OOSConfig
                                        .getPutObjectDeltaTime() * 1000000)) {
                            client.uploadDelete(upload);
                            deleteUpload(upload, object, bucketLog);
                            flag = true;
                            break;
                        }
                    } catch (Exception e) {
                        log.error(e.getMessage(), e);
                    }
                }
            }
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }
        if (flag)
            throw new BaseException(400, "PutObjectTooFast", "Upload :" + upload.storageId
                    + " is updated too fast");
    }

    public static void completeMultipartUpload_fake(HttpServletRequest req,
            HttpServletResponse resp, BucketMeta bucket, ObjectMeta object,
            InitialUploadMeta initialUpload, BucketLog bucketLog,InputStream ip, PinData pinData)
            throws AmazonClientException, IOException, BaseException {
        List<PartETag> parts = new XmlResponseSaxParser().parseCompleteUpload(ip)
                .getPartETag();
        if (req.getHeader(Consts.X_CTYUN_LASTMODIFIED) != null)
            object.lastModified = Long.valueOf(req.getHeader(Consts.X_CTYUN_LASTMODIFIED));
        else
            object.lastModified = System.currentTimeMillis();
        int partOrder = 0;
        long size = 0;
        // 检查合并分段请求的合法性
        List<UploadMeta> uploads = new ArrayList<UploadMeta>();
        ArrayList<Integer> partNumList = new ArrayList<Integer>();
        for (int i = 0; i < parts.size(); i++) {
            PartETag part = parts.get(i);
            int partNum = part.getPartNumber();
            UploadMeta upload = new UploadMeta(bucket.metaLocation, initialUpload.uploadId, partNum);
            if(i == 0 && client.isBusy(upload, OOSConfig.getMaxConcurrencyPerRegionServer()))
                Backoff.backoff();
            if (!client.uploadSelect(upload))
                throw new BaseException(400, "InvalidPart");
            if (partOrder < partNum)
                partOrder = partNum;
            else
                throw new BaseException(400, "InvalidPartOrder");
            if (!upload.etag.equals(part.getETag().trim().replace("\"", "")))
                throw new BaseException(400, "InvalidPart");
            if (upload.size < Consts.UPLOAD_PART_MINSIZE && i != (parts.size() - 1))
                throw new BaseException(400, "InvalidPartSize");
            Utils.checkPartNumber(req,partNum);
            uploads.add(upload);
            partNumList.add(partNum);
            size += upload.size;
        }
        // 更新initialUpload三张表，isComplete=true
        initialUpload.isComplete = true;
        if(client.isBusy(initialUpload, OOSConfig.getMaxConcurrencyPerRegionServer()))
            Backoff.backoff();
        client.initialUploadInsert(initialUpload);
        // 设置Object表
        object.storageId = Storage.setStorageId(Storage.generateOstorKey());
        object.ostorId = "";
        object.ostorPageSize = 0;
        object.storageClass = null;
        object.size = size;
        object.timestamp = Utils.getNanoTime();
        object.etag = "-";// 生成不了etag
        // 记录合并分段的initialUploadId和partNum
        object.initialUploadId = initialUpload.uploadId;
        object.partNum = partNumList;
        bucketLog.putMetaTime = System.currentTimeMillis();
        try {
            if (client.isBusy(object, OOSConfig.getMaxConcurrencyPerRegionServer()))
                Backoff.backoff();
            client.objectInsert(object);
        } finally {
            bucketLog.putMetaTime = System.currentTimeMillis() - bucketLog.putMetaTime;
        }
        // 删除旧版本的对象，但是如果旧版本是分段片段，就不删除，以免误删数据
        clearOldObjectVersions(object, false, bucketLog.requestId);
        pinData.jettyAttributes.objectAttribute.objectSize = object.size;
        XmlWriter xml = new XmlWriter();
        xml.start("CompleteMultipartUploadResult", "xmlns", Consts.XMLNS);
        xml.start("Location").value("http://" + bucket.getName() + "."
                + OOSConfig.getDomainSuffix()[0] + "/" + object.name).end();
        xml.start("Bucket").value(bucket.getName()).end();
        xml.start("Key").value(object.name).end();
        // xml.start("ETag").value(object.etag).end();
        xml.end();
        resp.setStatus(200);
        resp.setHeader(Headers.CONTENT_TYPE, "application/xml");
        setExpirationHeader(resp,bucket,object);
        Utils.setCommonHeader(resp,new Date(bucketLog.startTime),bucketLog.requestId);
        try {
            resp.getOutputStream().write(xml.toString().getBytes(Consts.CS_UTF8));
            resp.flushBuffer();
        } catch (Exception e) {
            log.error("catch write response error");
        }
    }
    
    public static void completeMultipartUpload(HttpServletRequest req, HttpServletResponse resp,
            BucketMeta bucket, ObjectMeta object, String initialUploadId, String domainSuffix,
            BlockingExecutor executor, BucketLog bucketLog, PinData pinData) throws AmazonClientException,
            IOException, BaseException {
        List<PartETag> parts = new XmlResponseSaxParser().parseCompleteUpload(req.getInputStream())
                .getPartETag();
        // String originalId = Storage.getStorageId(object.storageId);
        long originalSize = object.size;
        int partOrder = 0;
        String storageId = "";
        AtomicBoolean finished = new AtomicBoolean(false);
//        EtagMaker etagmaker = new EtagMaker();
        Storage storage = new Storage(bucketLog, null, object.ostorId);
        String userMeta = getUserMeta(object, bucket.getOwnerId());
        long redundantSize = 0;
        long alinSize = 0;
        try {
            List<UploadMeta> uploads = new ArrayList<UploadMeta>();
            for (int i = 0; i < parts.size(); i++) {
                PartETag part = parts.get(i);
                int partNum = part.getPartNumber();
                UploadMeta upload = new UploadMeta(bucket.metaLocation, initialUploadId, partNum);
                if(i == 0 && client.isBusy(upload, OOSConfig.getMaxConcurrencyPerRegionServer()))
                    Backoff.backoff();
                if (!client.uploadSelect(upload))
                    throw new BaseException(400, "InvalidPart");
                uploads.add(upload);
                if (partOrder < partNum)
                    partOrder = partNum;
                else
                    throw new BaseException(400, "InvalidPartOrder");
                if (!upload.etag.equals(part.getETag().trim().replace("\"", "")))
                    throw new BaseException(400, "InvalidPart");
                if (upload.size < Consts.UPLOAD_PART_MINSIZE && i != (parts.size() - 1))
                    throw new BaseException(400, "InvalidPartSize");
                Utils.checkPartNumber(req,partNum);
            }
            executor.execute(new ResponseThread(resp, finished));
            UploadMeta dstUploadMeta = uploads.remove(0);
            Long wholeSize = dstUploadMeta.size;
            storageId = dstUploadMeta.storageId;
            OstorObjectMeta dstObjMeta = getOstorObjectMeta(dstUploadMeta, object);
            dstObjMeta.userMeta = userMeta;
            List<OstorObjectMeta> srcObjsMeta = new ArrayList<>();
            for (UploadMeta upload : uploads) {
                OstorObjectMeta srcObjMeta = getOstorObjectMeta(upload, object);
                srcObjsMeta.add(srcObjMeta);
                wholeSize += upload.size;
            }
            try {
                OstorClient.getProxy(object.ostorId).seriesMerge(dstObjMeta, srcObjsMeta);
            } catch (OstorException e2) {
                log.error(e2.getMessage(), e2);
                throw new BaseException(500, "merge failed");
            }
            log.info("assert:" + dstObjMeta.length + " " + wholeSize);
            assert dstObjMeta.length == wholeSize;
            object.size = wholeSize;
            object.etag = "-";// 生成不了etag
            if (req.getHeader(Consts.X_CTYUN_LASTMODIFIED) != null)
                object.lastModified = Long.valueOf(req.getHeader(Consts.X_CTYUN_LASTMODIFIED));
            else
                object.lastModified = System.currentTimeMillis();
            object.storageId = storageId;
            object.timestamp = Utils.getNanoTime();
            bucketLog.putMetaTime = System.currentTimeMillis();
            try {
                if (client.isBusy(object, OOSConfig.getMaxConcurrencyPerRegionServer()))
                    Backoff.backoff();
                client.objectInsert(object);
            } finally {
                bucketLog.putMetaTime = System.currentTimeMillis() - bucketLog.putMetaTime;
            }
            clearOldObjectVersions(object, false, bucketLog.requestId);
            // 删initial upload
            try {
                InitialUploadMeta initialUpload = new InitialUploadMeta(bucket.metaLocation, bucket.getName(),
                        object.name, initialUploadId, false);
                client.initialUploadDelete(initialUpload);
                client.uploadDelete(dstUploadMeta);
                // 最后删upload
                for (UploadMeta upload : uploads) {
                    try {
                        client.uploadDelete(upload);
                        storage.delete(Storage.getStorageId(upload.storageId), upload.size,
                                upload.initialUploadId, object.ostorPageSize, object.storageClass,
                                true);
                         redundantSize = +Utils.getRedundantSize(-upload.size,object.storageClass);
                         alinSize = +Utils.getAlinSize(-upload.size, object.ostorPageSize, object.storageClass);
                    } catch (Exception e) {
                        log.error(e.getMessage(), e);
                    }
                }
            } catch (Exception e1) {
                log.error(e1.getMessage(), e1);
            }
            XmlWriter xml = new XmlWriter();
            xml.start("CompleteMultipartUploadResult", "xmlns", Consts.XMLNS);
            xml.start("Location")
                    .value("http://" + bucket.getName() + "." + domainSuffix + "/" + object.name)
                    .end();
            xml.start("Bucket").value(bucket.getName()).end();
            xml.start("Key").value(object.name).end();
            xml.start("ETag").value(object.etag).end();
            xml.end();
            synchronized (resp) {
                resp.setStatus(200);
                resp.setHeader(Headers.CONTENT_TYPE, "application/xml");
                try {
                    resp.getOutputStream().write(xml.toString().getBytes(Consts.CS_UTF8));
                    resp.flushBuffer();
                } catch (Exception e) {
                    log.error("catch write response error");
                }
            }
        } finally {
            finished.set(true);
        }
        pinData.setSize(-originalSize, redundantSize, alinSize, object.dataRegion, object.originalDataRegion, object.storageClassString, object.getLastCostTime());
    }
    public static OstorObjectMeta getOstorObjectMeta(UploadMeta upload, ObjectMeta object) {
        OstorObjectMeta ostorMeta = new OstorObjectMeta();
        ostorMeta.ostorKey = HexUtils.toByteArray(Storage.getStorageId(upload.storageId));
        ostorMeta.replica = object.storageClass;
        ostorMeta.length = upload.size;
        ostorMeta.pageSize = object.ostorPageSize;
        return ostorMeta;
    }
    
    public static void abortMultipartUpload(ObjectMeta object, UploadMeta upload,
            List<UploadMeta> uploads, InitialUploadMeta initialUpload, BucketLog bucketLog,
            PinData pinData) throws BaseException {
        Triple<Long,Long,Long> size = abortMultipartUploadMeta(upload, uploads, initialUpload);
        pinData.setSize(size.first(), size.second(), size.third(), object.dataRegion, object.originalDataRegion, object.storageClassString, object.getLastCostTime());
        pinData.setIsMultipartUploadTrue();
        try {
            abortMultipartUpload(object, upload, uploads, initialUpload, bucketLog); 
        } catch (Exception e) {
            //只要deleteObjectMeta成功了就认为删除成功了，但是存在数据没有删除成功的情况。
            log.warn("AbortMultipartUploadMeta success, but abortMultipartUpload failed. MetaRegionName: " + object.metaRegionName
                    + ", bucketName: " + object.bucketName + ", objectName: " + object.name, e);
        }
    }
    
    
    private static Triple<Long, Long, Long> abortMultipartUploadMeta(
            UploadMeta upload, List<UploadMeta> uploads, InitialUploadMeta initialUpload) throws BaseException {
        long wholeSize = 0;
        long redundantSize = 0;
        long alinSize = 0;
        if(client.isBusy(initialUpload, OOSConfig.getMaxConcurrencyPerRegionServer()))
            Backoff.backoff();
        if(client.isBusy(upload, OOSConfig.getMaxConcurrencyPerRegionServer()))
            Backoff.backoff();
        try {            
            client.initialUploadDelete(initialUpload);
            client.uploadDeleteByUploadId(upload);
            for (UploadMeta u : uploads) {
                redundantSize += Utils.getRedundantSize(u.size, u.replicaMode);
                alinSize += Utils.getAlinSize(u.size, u.pageSize, u.replicaMode);               
                wholeSize += u.size;
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new BaseException(500, "Abort Multipart Upload Failed");
        }        
        return new Triple<Long, Long, Long>(wholeSize, redundantSize, alinSize);
    }
    
    private static void abortMultipartUpload(ObjectMeta object, 
            UploadMeta upload, List<UploadMeta> uploads, InitialUploadMeta initialUpload, 
            BucketLog bucketLog) throws BaseException {
        try {            
            for (UploadMeta u : uploads) {
                try {
                    String ostorKey = Storage.getStorageId(u.storageId);
                    if (object.dataRegion.equals(LOCAL_REGION_NAME)) {
                        Storage storage = new Storage(bucketLog, u.ostorId);
                        storage.delete(ostorKey, u.size, u.initialUploadId, u.pageSize,
                                u.replicaMode, true);
                    } else {
                        internalClient.delete(object.dataRegion, ostorKey, u.ostorId, u.size, u.pageSize, u.replicaMode,
                                bucketLog == null ? "-" : bucketLog.requestId);
                    }
                } catch (Exception e) {
                    log.error("catch delete object storage error:" + u.storageId);
                    log.error(e.getMessage(), e);
                }
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    public static String listParts(String metaRegion, String bucket, String object, String uploadId, int maxParts, int partNumberMarker, BucketLog bucketLog) throws Exception {
        InitialUploadMeta initialUpload = new InitialUploadMeta(metaRegion, bucket,
                object, uploadId, false);
        boolean iuExists;
        bucketLog.getMetaTime = System.currentTimeMillis();
        try {
            if (client.isBusy(initialUpload, OOSConfig.getMaxConcurrencyPerRegionServer()))
                Backoff.backoff();
            iuExists = client.initialUploadSelect(initialUpload);
        } finally {
            bucketLog.getMetaTime = System.currentTimeMillis() - bucketLog.getMetaTime;
        }
        if (!iuExists) {
            throw new BaseException(404, ErrorMessage.ERROR_CODE_NO_SUCH_UPLOAD);
        }
        if (MetaRegionMapping.needMapping(metaRegion)) {
            String dataRegion = MetaRegionMapping.getMappedDataRegion(metaRegion);
            return internalClient.listParts(dataRegion, metaRegion, bucket, object, uploadId, maxParts, partNumberMarker,
                    bucketLog.requestId);
        }
        OwnerMeta iniOwner = new OwnerMeta(initialUpload.initiator);
        client.ownerSelectById(iniOwner);
        UploadMeta upload = new UploadMeta(metaRegion, uploadId, true);
        List<UploadMeta> uploads = client.uploadListPart(upload, partNumberMarker, maxParts + 1);
        XmlWriter xml = new XmlWriter();
        xml.start("ListPartsResult", "xmlns", Consts.XMLNS);
        xml.start("Bucket").value(bucket).end();
        xml.start("Key").value(object).end();
        xml.start("UploadId").value(uploadId).end();
        xml.start("Initiator");
        xml.start("ID").value(iniOwner.getName()).end();
        if (iniOwner.displayName != null)
            xml.start("DisplayName").value(iniOwner.displayName).end();
        else
            xml.start("DisplayName").value("").end();
        xml.end();
        xml.start("Owner");
        xml.start("ID").value("").end();
        xml.start("DisplayName").value("").end();
        xml.end();
        if (StringUtils.isNotBlank(initialUpload.storageClass))
            xml.start("StorageClass").value(initialUpload.storageClass).end();
        else
            xml.start("StorageClass").value(StorageClassConfig.getDefaultClass()).end();
        if (partNumberMarker != 0)
            xml.start("PartNumberMarker").value(String.valueOf(partNumberMarker)).end();
        xml.start("MaxParts").value(String.valueOf(maxParts)).end();
        int m = 0;
        if (uploads.size() > maxParts) {
            m = maxParts;
            //如果客户传的maxkey是0 。 那么NextPartNumberMarker直接返回PartNumberMarker
            String nextPartNumberMarker = "0";
            if(maxParts == 0) {
                nextPartNumberMarker = partNumberMarker + "";
            }else {
                nextPartNumberMarker = String.valueOf(uploads.get(maxParts - 1).partNum);
            }
            xml.start("NextPartNumberMarker")
                    .value(nextPartNumberMarker).end();
            xml.start("IsTruncated").value("true").end();
        } else {
            m = uploads.size();
            xml.start("IsTruncated").value("false").end();
        }
        for (int i = 0; i < m; i++) {
            UploadMeta part = uploads.get(i);
            xml.start("Part");
            xml.start("PartNumber").value(String.valueOf(part.partNum)).end();
            xml.start("LastModified")
                    .value(ServiceUtils.formatIso8601Date(new Date(part.lastModified))).end();
            xml.start("ETag").value(part.etag).end();
            xml.start("Size").value(String.valueOf(part.size)).end();
            xml.end();
        }
        xml.end();
        String str = Consts.XML_HEADER + xml.toString();
        return str;
    }
    
    public static void uploadPartCopy(HttpServletRequest request, HttpServletResponse response, BucketMeta sourceBucket,
            ObjectMeta sourceObject, BucketMeta bucket, ObjectMeta object, UploadMeta upload, 
            ThreadPoolExecutor executor, BucketLog bucketLog, InitialUploadMeta initialUpload, PinData pinData)
            throws BaseException, IOException {
        long ts = Utils.getTimeStamp();
        checkIfCanOperate(request, sourceBucket, sourceObject);
        long sourceObjectSize = getObjectSize(sourceObject).first();
        String range = request.getHeader(Headers.COPY_PART_RANGE);
        if (range != null && sourceObjectSize < Consts.MIN_COPY_PART_SIZE) {
            throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_REQUEST);
        }
        Pair<Long, Long> pair = HttpUtils.getRange(range, sourceObjectSize);
        long beginRange = pair.first(), endRange = pair.second();
        AtomicBoolean finished = new AtomicBoolean(false);
        String userMeta = getUserMeta(object, bucket.getOwnerId());
        long length = endRange - beginRange + 1;
        if(client.isBusy(upload, OOSConfig.getMaxConcurrencyPerRegionServer()))
            Backoff.backoff();
        if(client.isBusy(initialUpload, OOSConfig.getMaxConcurrencyPerRegionServer()))
            Backoff.backoff();
        boolean ref = client.uploadSelect(upload);
        long originalSize = 0;
        long originalRedundantSize = 0;
        long originalAlinSize = 0;
        long originalLastModified = upload.lastModified;
        String originalStorageClassString = object.storageClassString;
        String dataRegionBeforeInsert = object.dataRegion;
        String originalDataRegionBeforeInsert = object.originalDataRegion;
        if (ref) {
            originalSize = upload.size;
            originalRedundantSize = Utils.getRedundantSize(originalSize, upload.replicaMode);
            originalAlinSize = Utils.getAlinSize(originalSize, upload.pageSize, upload.replicaMode);
        }
        try {
            executor.execute(new ResponseThread(response, finished));
            if (!sourceObject.dataRegion.equals(LOCAL_REGION_NAME) && !object.dataRegion.equals(LOCAL_REGION_NAME)) {
                WriteResult putResult = internalClient.copy(sourceObject.dataRegion,  
                        object.dataRegion,sourceBucket.name,sourceObject.name, bucket.name, object.name,  
                        initialUpload.storageClass, range, bucketLog.requestId);
                bucketLog.ostorKey = putResult.ostorKey;
                bucketLog.ostorId = putResult.ostorId;
                upload.storageId = Storage.setStorageId(putResult.ostorKey);
                upload.etag = putResult.etag;
                upload.ostorId = putResult.ostorId;
                upload.pageSize = putResult.pageSize;
                upload.replicaMode = putResult.replicaMode;
                if (!initialUpload.ostorIds.contains(putResult.ostorId)) {
                    initialUpload.ostorIds.add(putResult.ostorId);
                    client.initialUploadInsert(initialUpload);
                }
            } else {
                String storageId;
                InputStream srcStream = null;
                try {
                    srcStream = getObjectInputStream(sourceObject, beginRange, endRange, range, bucketLog);
                    if (object.dataRegion.equals(LOCAL_REGION_NAME)) {
                        boolean result = getMultipartReplicaMeta(object, initialUpload, length,
                                upload,bucket.getOwnerId());
                        EtagMaker etagmaker = new EtagMaker();
                        Storage desStorage = new Storage(bucketLog, etagmaker, upload.ostorId);
                        try {
                            storageId = desStorage
                                    .create(srcStream, length, userMeta, object.name, null, upload.pageSize, upload.replicaMode, object.bucketName, Consts.USER_NO_LIMIT_RATE);
                        }finally {
                            //异常时记录写入的ostorId
                            bucketLog.ostorId = "sourceObject:"+bucketLog.ostorId + "-object:" + upload.ostorId;
                        }
                        checkStorageId(storageId);
                        bucketLog.ostorKey = storageId;
                        upload.storageId = Storage.setStorageId(storageId);
                        upload.etag = etagmaker.digest();
                        if (result)
                            client.initialUploadInsert(initialUpload);
                    } else {
                        WriteResult putResult = internalClient.write(object.dataRegion, object.bucketName, object.name, 
                                initialUpload.storageClass, length, srcStream, false, Consts.USER_NO_LIMIT_RATE, bucketLog.requestId);
                        checkStorageId(putResult.ostorKey);
                        bucketLog.ostorKey = putResult.ostorKey;
                        bucketLog.ostorId = putResult.ostorId;
                        upload.storageId = Storage.setStorageId(putResult.ostorKey);
                        upload.etag = putResult.etag;
                        upload.ostorId = putResult.ostorId;
                        upload.pageSize = putResult.pageSize;
                        upload.replicaMode = putResult.replicaMode;
                        if (!initialUpload.ostorIds.contains(putResult.ostorId)) {
                            initialUpload.ostorIds.add(putResult.ostorId);
                            client.initialUploadInsert(initialUpload);
                        }
                    }
                    
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                    if (e instanceof BaseException) {
                        throw e;
                    } else {
                        throw new BaseException(500, "Copy Object Fail");
                    }
                } finally {
                    if (srcStream != null) {
                        try {
                            srcStream.close();
                        } catch (IOException e) {
                            log.error(e.getMessage(), e);
                        }
                    }
                }
            }
            bucketLog.etag = upload.etag;
            bucketLog.replicaMode = upload.replicaMode.toString();
            upload.size = endRange - beginRange + 1;
            upload.contentMD5 = sourceObject.contentMD5;
            upload.lastModified = ts;
            upload.timestamp = Utils.getNanoTime();
            client.uploadInsert(upload);
            clearOldUploadVersions(upload, object, bucketLog);
            XmlWriter xml = new XmlWriter();
            xml.start("CopyPartResult", "xmlns", Consts.XMLNS);
            xml.start("LastModified")
                    .value(ServiceUtils.formatIso8601Date(new Date(upload.lastModified))).end();
            xml.start("ETag").value(upload.etag).end();
            xml.end();
            response.setStatus(200);
            Utils.setCommonHeader(response,new Date(bucketLog.startTime),bucketLog.requestId);
            if (sourceObject.dataRegion.equals(object.dataRegion) || 
                    sourceObject.dataRegion.equals(object.originalDataRegion)) {
                pinData.setDirectUpload(length, object.dataRegion, object.storageClassString);
            } else {
                pinData.setRoamUpload(length, object.dataRegion, object.storageClassString);
            }
            
            if (ref) {
                pinData.sourceObject = pinData.new SourceObject(originalSize, originalAlinSize, originalRedundantSize, originalLastModified
                        , originalStorageClassString, dataRegionBeforeInsert, originalDataRegionBeforeInsert);
                pinData.sourceObject.setIsMultipartUploadTrue();
            }
            if (Utils.STORAGE_TYPES_RESTORE.contains(sourceObject.storageClassString)) {
                pinData.setRestore(sourceObjectSize, sourceObject.dataRegion, sourceObject.storageClassString, sourceBucket.getName());
            }
            
            long redundantSize = Utils.getRedundantSize(length, upload.replicaMode);
            long alinSize = Utils.getAlinSize(length, upload.pageSize, upload.replicaMode);
            pinData.setSize(length, redundantSize, alinSize,
                    object.dataRegion, object.originalDataRegion, object.storageClassString, upload.lastModified);
            pinData.setIsMultipartUploadTrue();
            synchronized (response) {
                response.setHeader(Headers.CONTENT_TYPE, "application/xml");
                try {
                    response.getOutputStream().write(xml.toString().getBytes(Consts.CS_UTF8));
                    response.flushBuffer();
                } catch (Exception e) {
                    log.error("catch write response error");
                }
            }
        } catch (Exception error) {
            log.error(error.getMessage(), error);
            throw new BaseException(500, "Copy Upload Part Failed");
        } finally {
            finished.set(true);
        }
    }  
       
    public static String getObjectAcl(PinData pinData, BucketMeta dbBucket) throws Exception {
        OwnerMeta owner = pinData.jettyAttributes.bucketOwnerAttribute.getBucketOwnerFromAttribute(dbBucket.getOwnerId());
        XmlWriter xml = new XmlWriter();
        xml.start("AccessControlPolicy");
        xml.start("Owner");
        xml.start("ID").value(owner.getName()).end();
        if (owner.displayName != null && owner.displayName.trim().length() != 0)
            xml.start("DisplayName").value(owner.displayName).end();
        else
            xml.start("DisplayName").value("").end();
        xml.end();
        xml.start("AccessControlList");
        xml.start("Grant");
        String attrs[] = { "xmlns:xsi", "xsi:type" };
        String values[] = { "http://www.w3.org/2001/XMLSchema-instance", "CanonicalUser" };
        xml.start("Grantee", attrs, values);
        xml.start("ID").value(owner.getName()).end();
        if (owner.displayName != null && owner.displayName.trim().length() != 0)
            xml.start("DisplayName").value(owner.displayName).end();
        else
            xml.start("DisplayName").value("").end();
        xml.end();
        xml.start("Permission").value(Permission.FULL_CONTROL.toString()).end();
        xml.end();
        xml.end();
        xml.end();
        log.info(xml.toString());
        String str = Consts.XML_HEADER + xml.toString();
        return str;
    }

    /**
     * 根据集群权重按概率随机选取ostor
     * @param ostorWeight
     * @return
     */
    private static String ostorSelect(Map<String, Integer> ostorWeight, ReplicaMode mode) {
        Set<String> writeableProxies = OstorClient.getWritableProxys()
                .stream().map(OstorProxy::getOstorID).collect(Collectors.toSet());
        // 1、过滤不可写或不符合mode的ostor
        ostorWeight.entrySet().removeIf(e ->
                //过滤不可写的集群
                !writeableProxies.contains(e.getKey())
                //过滤不符合mode的集群
                || (mode != null && !ReplicaMode.checkReplicaMode(mode, OstorClient.getProxy(e.getKey()).getOstorProxyConfig().ec_N))
        );
        // 2、选取
        int s = ostorWeight.values().stream().mapToInt(Integer::intValue).sum();
        List<String> ostors = new ArrayList<>(ostorWeight.keySet());
        Random random = ThreadLocalRandom.current();
        int r = random.nextInt(s + 1);
        int t = 0;
        for (String ostor : ostors) {
            t += ostorWeight.get(ostor);
            if (r <= t) {
                return ostor;
            }
        }
        return null;
    }
}

class ResponseThread implements Runnable {
    private static final Log log = LogFactory.getLog(ResponseThread.class);
    private HttpServletResponse resp;
    private AtomicBoolean finished;
    
    public ResponseThread(HttpServletResponse resp, AtomicBoolean finished) {
        assert (resp != null);
        this.resp = resp;
        this.finished = finished;
    }
    
    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                Thread.sleep(1000);
                synchronized (resp) {
                    if (finished.get())
                        break;
                    resp.getOutputStream().write(" ".getBytes(Consts.CS_UTF8));
                    resp.getOutputStream().flush();
                }
            } catch (InterruptedException e) {
                log.error(e.getMessage(), e);
            } catch (IOException e) {
                log.error(e.getMessage(), e);
                break;
            }
        }
    }
}

class UploadInputMeta {
    public ReplicaMode replicaMode;
    public int pageSize;
    public long start;
    public String dataRegion;
    public long size;
    public String ostorKey;
    public String ostorId;
    public BucketLog bucketLog;
    public String initialUploadId;
    public EtagMaker etagMaker;
    public String etag;
    public long partSize;

    public UploadInputMeta(String ostorKey, String ostorId, String dataRegion, long start,
            long size, int pageSize, ReplicaMode replicaMode, BucketLog bucketLog,
            String initialUploadId,String etag,long partSize) {
        this.ostorId = ostorId;
        this.ostorKey = ostorKey;
        this.dataRegion = dataRegion;
        this.start = start;
        this.size = size;
        this.pageSize = pageSize;
        this.replicaMode = replicaMode;
        this.bucketLog = bucketLog;
        this.initialUploadId = initialUploadId;
        this.etag = etag;
        this.partSize = partSize;
        etagMaker = new EtagMaker();
    }
}

class ContinuousInputStream extends InputStream implements Runnable{
    private static InternalClient internalClient = InternalClient.getInstance();
    private static final Log log = LogFactory.getLog(ContinuousInputStream.class);
    private List<UploadInputMeta> uploads;
    private String requestId;//调用内部接口时，目标服务要使用调用者request id追踪请求发起的地方
    
    private LinkedBlockingQueue<InputStream> inputstreams = new LinkedBlockingQueue<>();
    private Semaphore cacheNum = new Semaphore(OOSConfig.getMultipartReadPermitsNum());
    private boolean isClose = false;

    ContinuousInputStream(List<UploadInputMeta> uploads,String requestId) {
        this.requestId = requestId;
        this.uploads = uploads;
    }
    
    public void run() {
        ThreadContext.put("Request_ID", this.requestId);
        for (int i = 0; i < uploads.size(); i++) {
            while(!isClose)
                try {
                    if(cacheNum.tryAcquire(100, TimeUnit.MILLISECONDS)) {
                        break;
                    };
                } catch (InterruptedException e1) {
                    log.error(e1.getMessage(), e1);
                    isClose = true;
                    closeAll();
                    break;
                }
            if(isClose) {
                closeAll();
                break;
            }
            UploadInputMeta upload = uploads.get(i);
            upload.bucketLog.ostorId = upload.ostorId;
            upload.bucketLog.ostorKey = upload.ostorKey;
            try {
                InputStream partStream;
                if (upload.dataRegion.equals(DataRegion.getRegion().getName())) {
                    Storage storage = new Storage(upload.bucketLog, upload.ostorId);
                    partStream = storage.read(upload.ostorKey, upload.start, upload.size,
                            upload.initialUploadId, upload.pageSize, upload.replicaMode);
                    partStream = new SignableInputStream(partStream, upload.size, upload.etagMaker);
                } else {
                    partStream = internalClient.get(upload.dataRegion, upload.ostorKey, upload.ostorId,
                            upload.size, upload.pageSize, upload.replicaMode, null, requestId, upload.etag);
                }
                if (partStream == null) {
                    inputstreams.add(new ErrorInputStream());
                    break;
                } else 
                    inputstreams.add(partStream);
            } catch (BaseException e1) {
                log.error(e1.getMessage(), e1);
                inputstreams.add(new ErrorInputStream());
                break;
            }
            if(isClose) {
                closeAll();
                break;
            }
        }
        inputstreams.add(new EndInputStream());
    }
    
    void closeAll() {
        while(true) {
            InputStream is = inputstreams.poll();
            if(is == null)
                break;
            else
                try {
                    is.close();
                } catch (IOException e) {
                }
        }
    }
    
    class ErrorInputStream extends InputStream{

        public int read() throws IOException {
            return 0;
        }
        
        public IOException getException() {
            return new IOException("read upload input stream failed");
        }
    }
    
    class EndInputStream extends InputStream{

        public int read() throws IOException {
            return 0;
        }
    }
    
    private InputStream in;
    
    /**
     *  Continues reading in the next stream if an EOF is reached.
     */
    final void nextStream() throws IOException {
        if (in != null) {
            in.close();
        }
        while(true)
            try {
                in = inputstreams.take();
                cacheNum.release();
                break;
            } catch (InterruptedException e) {
            }

    }

    public int available() throws IOException {
        if (in == null) {
            return 0; // no way to signal EOF from available()
        }
        return in.available();
    }

    public int read() throws IOException {
        if(in == null)
            nextStream();
        if(in instanceof EndInputStream)
            return -1;
        else if(in instanceof ErrorInputStream)
            throw ((ErrorInputStream)in).getException();
            
        int c = in.read();
        if (c != -1) {
            return c;
        }
        nextStream();
        return read();
    }

    public int read(byte b[], int off, int len) throws IOException {
        if (b == null) {
            throw new NullPointerException();
        } else if (off < 0 || len < 0 || len > b.length - off) {
            throw new IndexOutOfBoundsException();
        } else if (len == 0) {
            return 0;
        }
        
        if(in == null)
            nextStream();
        if(in instanceof EndInputStream)
            return -1;
        else if(in instanceof ErrorInputStream)
            throw ((ErrorInputStream)in).getException();
        
        int n = in.read(b, off, len);
        if (n > 0) {
            return n;
        }
        nextStream();
        return read(b, off, len);
    }

    public void close() throws IOException {
        try {
            if (in != null)
                in.close();
        } finally {
            isClose = true;
            //释放一个信号量。 如果有阻塞等待，则可立即退出
            cacheNum.release();
            closeAll();
        }
    }
    
    public void closeWithNoException() throws IOException{
        //先执行close操作，如果抛出异常，本次读取数据出问题的概率大 ，不计算etag
        close();
        try {
            //计算etag 如果取range读流，不完整的分片不计算etag，异地对象不计算etag
            for(int i = 0; i < uploads.size(); i++) {
                String etag = uploads.get(i).etagMaker.digest();
                UploadInputMeta upload = uploads.get(i);
                if(upload.dataRegion.equals(DataRegion.getRegion().getName()) && upload.partSize == upload.size
                        && !upload.etag.equals(etag)) {
                    SendEtagUnmatchMQ.send(etag, upload.etag, upload.ostorKey, upload.ostorId);
                }
            }
        } catch (Exception e) {
            log.error("send etag mq error ", e);
        }
    }
}

class UploadInputStream extends InputStream {
    Enumeration<? extends UploadInputMeta> e;
    InputStream in;
    private String requestId;
    private static InternalClient internalClient = InternalClient.getInstance();
    private static final Log log = LogFactory.getLog(UploadInputStream.class);

    public UploadInputStream(Enumeration<? extends UploadInputMeta> e,String requestId) throws IOException {
        this.e = e;
        this.requestId = requestId;
        nextStream();
    }

    final void nextStream() throws IOException {
        if (in != null) {
            in.close();
        }
        if (e.hasMoreElements()) {
            UploadInputMeta upload = e.nextElement();
            try {
                if (upload.dataRegion.equals(DataRegion.getRegion().getName())) {
                    Storage storage = new Storage(upload.bucketLog, upload.ostorId);
                    in = storage.read(upload.ostorKey, upload.start, upload.size,
                            upload.initialUploadId, upload.pageSize, upload.replicaMode);
                } else {
                    in = internalClient.get(upload.dataRegion, upload.ostorKey, upload.ostorId,
                            upload.size, upload.pageSize, upload.replicaMode, null, requestId, upload.etag);
                }
                if (in == null)
                    throw new IOException("read upload input stream failed");
            } catch (BaseException e1) {
                log.error(e1.getMessage(), e1);
                throw new IOException("read upload input stream failed");
            }
        } else
            in = null;
    }

    @Override
    public int read() throws IOException {
        while (in != null) {
            int c = in.read();
            if (c != -1) {
                return c;
            }
            nextStream();
        }
        return -1;
    }

    @Override
    public int read(byte b[], int off, int len) throws IOException {
        if (in == null) {
            return -1;
        } else if (b == null) {
            throw new NullPointerException();
        } else if (off < 0 || len < 0 || len > b.length - off) {
            throw new IndexOutOfBoundsException();
        } else if (len == 0) {
            return 0;
        }
        do {
            int n = in.read(b, off, len);
            if (n > 0) {
                return n;
            }
            nextStream();
        } while (in != null);
        return -1;
    }

    public void close() throws IOException {
        do {
            nextStream();
        } while (in != null);
    }
}
