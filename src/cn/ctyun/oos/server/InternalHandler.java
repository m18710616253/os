package cn.ctyun.oos.server;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URLDecoder;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import cn.ctyun.common.utils.LogUtils;
import cn.ctyun.oos.common.InternalConst;
import cn.ctyun.oos.common.OOSActions;
import cn.ctyun.oos.metadata.OwnerMeta;
import com.amazonaws.services.s3.model.ObjectListing;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.eclipse.jetty.server.Request;

import com.amazonaws.HttpMethod;
import com.amazonaws.services.s3.Headers;
import com.amazonaws.services.s3.internal.XmlWriter;

import cn.ctyun.common.BaseException;
import cn.ctyun.common.Consts;
import cn.ctyun.common.ReplicaMode;
import cn.ctyun.common.conf.DataRegion;
import cn.ctyun.common.conf.OOSConfig;
import cn.ctyun.common.region.DataRegions.DataRegionInfo;
import cn.ctyun.oos.common.ErrorMessage;
import cn.ctyun.oos.common.SendEtagUnmatchMQ;
import cn.ctyun.oos.common.Utils;
import cn.ctyun.oos.hbase.MetaClient;
import cn.ctyun.oos.metadata.BucketMeta;
import cn.ctyun.oos.metadata.InitialUploadMeta;
import cn.ctyun.oos.metadata.ObjectMeta;
import cn.ctyun.oos.metadata.UploadMeta;
import cn.ctyun.oos.server.InternalClient.WriteResult;
import cn.ctyun.oos.server.backoff.Backoff;
import cn.ctyun.oos.server.count.Pin;
import cn.ctyun.oos.server.count.PinData;
import cn.ctyun.oos.server.log.BucketLog;
import cn.ctyun.oos.server.storage.EtagMaker;
import cn.ctyun.oos.server.storage.Storage;
import cn.ctyun.oos.server.util.HttpUtils;
import cn.ctyun.oos.server.util.Misc;
import common.threadlocal.ThreadLocalBytes;
import common.tuple.Pair;
import common.tuple.Triple;

public class InternalHandler {
    
    private static final String TRUE = Boolean.toString(true);
    private static final Log log = LogFactory.getLog(InternalHandler.class);
    private static final ThreadLocalBytes localBytes = ThreadLocalBytes.current();
    private static final InternalClient internalClient = InternalClient.getInstance();
    private static final DataRegionInfo regionInfo;
    private static MetaClient client = MetaClient.getGlobalClient();
    static {
        try {
            regionInfo = internalClient.getRegion(DataRegion.getRegion().getName());
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }
    
    /**
     * 查看是否为内部请求
     * @param request
     * @return
     */
    public static boolean isForwarded(HttpServletRequest request) {
        String header = request.getHeader(Consts.X_AMZ_FORWARD);
        if (TRUE.equals(header)) {
            return true;
        } else {
            return false;
        }
    }
    
    /**
     * 验证签名
     * @param baseRequest
     * @param request
     * @throws BaseException 签名错误
     */
    private static void checkAuth(Request baseRequest, HttpServletRequest request) throws BaseException {
        String auth = request.getHeader(Consts.AUTHORIZATION);
        String accessKey = Misc.getUserIdFromAuthentication(auth);
        if (!accessKey.equals(regionInfo.getAccessKey())) {
            throw new BaseException(HttpURLConnection.HTTP_FORBIDDEN, "SignatureDoesNotMatch");
        } else {
            String signature = auth.substring(auth.indexOf(':') + 1);
            Utils.checkAuth(signature, regionInfo.getSecretKey(), null, null, baseRequest, request, null);
        }
    }
    
    /**
     * 处理PUT请求
     * @param request
     * @param response
     * @throws BaseException
     */
    private static void handlePut(HttpServletRequest request, HttpServletResponse response, BucketLog bucketLog)
            throws BaseException, IOException {
        String bucket = request.getHeader(Consts.X_AMZ_BUCKET_NAME);
        String encodedObjectName = request.getHeader(Consts.X_AMZ_OBJECT_NAME);
        String object;
        try {
            object = URLDecoder.decode(encodedObjectName, Consts.STR_UTF8);
        } catch (UnsupportedEncodingException e2) {
            log.error(e2.getMessage(), e2);
            throw new BaseException(HttpURLConnection.HTTP_INTERNAL_ERROR, ErrorMessage.ERROR_MESSAGE_500);
        }
        String replicaModeHeader = request.getHeader(Consts.X_AMZ_REPLICA_MODE);
        long length = Long.parseLong(request.getHeader(Headers.CONTENT_LENGTH));
        
        int limitRate = Consts.USER_NO_LIMIT_RATE;
        if(request.getHeader(Consts.X_AMZ_LIMITRATE) != null) {
            limitRate = OpObject.validLimitRate(request.getHeader(Consts.X_AMZ_LIMITRATE));
        }
        
        ObjectMeta objectMeta = new ObjectMeta();
        objectMeta.name = object;
        objectMeta.bucketName = bucket;
        objectMeta.size = length;
        BucketMeta bucketMeta = new BucketMeta(bucket);
        if (client.bucketSelect(bucketMeta)) {
            OwnerMeta ownerMeta = new OwnerMeta(bucketMeta.getOwnerId());
            try {
                client.ownerSelectById(ownerMeta);
            } catch (Exception e) {
                log.error("owner select error", e);
                throw new BaseException(500, ErrorMessage.ERROR_CODE_500);
            }
            OpObject.setReplicaModeAndSize(objectMeta, replicaModeHeader, false, ownerMeta);
        }else{
            throw new BaseException(HttpURLConnection.HTTP_BAD_REQUEST, ErrorMessage.ERROR_CODE_NO_SUCH_BUCKET);
        }
        bucketLog.metaRegion = bucketMeta.metaLocation;
        bucketLog.dataRegion = DataRegion.getRegion().getName();
        bucketLog.ostorId = objectMeta.ostorId;
        bucketLog.replicaMode = objectMeta.storageClass.toString();
        EtagMaker etagMaker = new EtagMaker();
        Storage storage = new Storage(bucketLog, etagMaker, objectMeta.ostorId);
        try (InputStream input = request.getInputStream()) {
            String storageId = storage.create(input, length, null, object, null, objectMeta.ostorPageSize, 
                    objectMeta.storageClass, bucket, limitRate);
            String etag = etagMaker.digest();
            bucketLog.ostorKey = storageId;
            bucketLog.etag = etag;
            response.setStatus(HttpURLConnection.HTTP_CREATED);
            response.setHeader(Consts.X_AMZ_OSTOR_KEY, storageId);
            response.setHeader(Consts.X_AMZ_OSTOR_ID, objectMeta.ostorId);
            response.setHeader(Consts.X_AMZ_REPLICA_MODE, objectMeta.storageClass.toString());
            response.setHeader(Consts.X_AMZ_PAGE_SIZE, String.valueOf(objectMeta.ostorPageSize));
            response.setHeader(Headers.ETAG, etag);
        } catch (IOException e1) {
            log.error(e1.getMessage(), e1);
            throw new BaseException(HttpURLConnection.HTTP_BAD_REQUEST, ErrorMessage.ERROR_CODE_INCOMPLETE_BODY);
        }
    }
    
    private static void handlePost(HttpServletRequest request, HttpServletResponse response, BucketLog bucketLog)
            throws BaseException, IOException {
        String bucket = request.getHeader(Consts.X_AMZ_BUCKET_NAME);
        String encodedObjectName = request.getHeader(Consts.X_AMZ_OBJECT_NAME);
        String object;
        try {
            object = URLDecoder.decode(encodedObjectName, Consts.STR_UTF8);
        } catch (UnsupportedEncodingException e2) {
            log.error(e2.getMessage(), e2);
            throw new BaseException(HttpURLConnection.HTTP_INTERNAL_ERROR, ErrorMessage.ERROR_MESSAGE_500);
        }
        String replicaModeHeader = request.getHeader(Consts.X_AMZ_REPLICA_MODE);
        long length = Long.parseLong(request.getHeader(Consts.X_AMZ_OBJECT_SIZE));
        
        ObjectMeta objectMeta = new ObjectMeta();
        objectMeta.name = object;
        objectMeta.bucketName = bucket;
        objectMeta.size = length;
        BucketMeta bucketMeta = new BucketMeta(bucket);
        if (client.bucketSelect(bucketMeta)) {
            OwnerMeta ownerMeta = new OwnerMeta(bucketMeta.getOwnerId());
            try {
                client.ownerSelectById(ownerMeta);
            } catch (Exception e) {
                log.error("owner select error", e);
                throw new BaseException(500, ErrorMessage.ERROR_CODE_500);
            }
            OpObject.setReplicaModeAndSize(objectMeta, replicaModeHeader, false, ownerMeta);
        }else{
            throw new BaseException(HttpURLConnection.HTTP_BAD_REQUEST, ErrorMessage.ERROR_CODE_NO_SUCH_BUCKET);
        }
        bucketLog.metaRegion = bucketMeta.metaLocation;
        bucketLog.dataRegion = DataRegion.getRegion().getName();
        bucketLog.ostorId = objectMeta.ostorId;
        bucketLog.replicaMode = objectMeta.storageClass.toString();
        EtagMaker etagMaker = new EtagMaker();
        Storage storage = new Storage(bucketLog, etagMaker, objectMeta.ostorId);
        try (InputStream input = request.getInputStream()) {
            Pair<String, Long> result = storage.create(input, length, null, object, objectMeta.ostorPageSize, 
                    objectMeta.storageClass, bucket);
            String etag = etagMaker.digest();
            bucketLog.ostorKey = result.first();
            bucketLog.etag = etag;
            response.setStatus(HttpURLConnection.HTTP_CREATED);
            response.setHeader(Consts.X_AMZ_OSTOR_KEY, result.first());
            response.setHeader(Consts.X_AMZ_OBJECT_SIZE, String.valueOf(result.second()));
            response.setHeader(Consts.X_AMZ_OSTOR_ID, objectMeta.ostorId);
            response.setHeader(Consts.X_AMZ_REPLICA_MODE, objectMeta.storageClass.toString());
            response.setHeader(Consts.X_AMZ_PAGE_SIZE, String.valueOf(objectMeta.ostorPageSize));
            response.setHeader(Headers.ETAG, etag);
        } catch (IOException e1) {
            log.error(e1.getMessage(), e1);
            throw new BaseException(HttpURLConnection.HTTP_BAD_REQUEST, ErrorMessage.ERROR_CODE_INCOMPLETE_BODY);
        }
    }
    
    /**
     * 处理copy请求
     * @param request
     * @param response
     * @param bucketLog
     * @throws BaseException
     * @throws IOException 
     */
    private static void handleCopy(HttpServletRequest request, HttpServletResponse response,
            BucketLog bucketLog) throws BaseException, IOException {
        String dstRegion = request.getHeader(Consts.X_AMZ_DST_REGION);
        String dstBucket = request.getHeader(Consts.X_AMZ_BUCKET_NAME);
        String encodedDstObject = request.getHeader(Consts.X_AMZ_OBJECT_NAME);
        String dstObject;
        try {
            dstObject = URLDecoder.decode(encodedDstObject, Consts.STR_UTF8);
        } catch (UnsupportedEncodingException e2) {
            log.error(e2.getMessage(), e2);
            throw new BaseException(HttpURLConnection.HTTP_INTERNAL_ERROR, ErrorMessage.ERROR_MESSAGE_500);
        }
        String dstRM = request.getHeader(Consts.X_AMZ_DST_REPLICA_MODE);
        String srcBucketName = request.getHeader(Consts.X_AMZ_SOURCE_BUCKET_NAME);
        String encodedSrcObject = request.getHeader(Consts.X_AMZ_SOURCE_OBJECT_NAME);
        String srcObjectName;
        try {
            srcObjectName = URLDecoder.decode(encodedSrcObject, Consts.STR_UTF8);
        } catch (UnsupportedEncodingException e2) {
            log.error(e2.getMessage(), e2);
            throw new BaseException(HttpURLConnection.HTTP_INTERNAL_ERROR, ErrorMessage.ERROR_MESSAGE_500);
        }
        String range = request.getHeader(Headers.RANGE);
        BucketMeta srcBucket = new BucketMeta(srcBucketName);
        client.bucketSelect(srcBucket);
        ObjectMeta srcObject = new ObjectMeta(srcObjectName, srcBucketName, srcBucket.metaLocation);
        bucketLog.getMetaTime = System.currentTimeMillis();
        try{
            if(client.isBusy(srcObject, OOSConfig.getMaxConcurrencyPerRegionServer()))
                Backoff.backoff();
            client.objectSelect(srcObject);
        } finally {
            bucketLog.getMetaTime = System.currentTimeMillis() - bucketLog.getMetaTime;
        }
        long objectSize = OpObject.getObjectSize(srcObject).first();
        Pair<Long, Long> pair = HttpUtils.getRange(range, objectSize);
        long beginRange = pair.first(), endRange = pair.second();
        try (InputStream input = OpObject.getObjectInputStream(srcObject, beginRange, endRange,
                range, bucketLog)) {
            WriteResult result = internalClient.write(dstRegion, dstBucket, dstObject, dstRM,
                    endRange - beginRange + 1, input, false, Consts.USER_NO_LIMIT_RATE, bucketLog.requestId);
            response.setStatus(HttpURLConnection.HTTP_CREATED);
            response.setHeader(Consts.X_AMZ_OSTOR_ID, result.ostorId);
            response.setHeader(Consts.X_AMZ_OSTOR_KEY, result.ostorKey);
            response.setHeader(Consts.X_AMZ_REPLICA_MODE, result.replicaMode.toString());
            response.setHeader(Consts.X_AMZ_PAGE_SIZE, String.valueOf(result.pageSize));
            response.setHeader(Headers.ETAG, result.etag);
        } catch (IOException e) {
            log.error(e.getMessage(), e);
            throw new BaseException(HttpURLConnection.HTTP_INTERNAL_ERROR, e.getMessage());
        }
    }
    
    /**
     * 处理GET请求
     * @param request
     * @param response
     * @throws BaseException
     */
    private static void handleGet(HttpServletRequest request, HttpServletResponse response, BucketLog bucketLog)
            throws BaseException {
        String ostorKey = request.getHeader(Consts.X_AMZ_OSTOR_KEY);
        String ostorId = request.getHeader(Consts.X_AMZ_OSTOR_ID);
        long objectSize = Long.parseLong(request.getHeader(Consts.X_AMZ_OBJECT_SIZE));
        int pageSize = Integer.parseInt(request.getHeader(Consts.X_AMZ_PAGE_SIZE));
        ReplicaMode replicaMode = ReplicaMode.parser(request.getHeader(Consts.X_AMZ_REPLICA_MODE));
        String range = request.getHeader(Headers.RANGE);
        String etag = request.getHeader(Headers.ETAG);
        
        long offset;
        long length;
        if (range == null) {
            offset = 0;
            length = objectSize;
        } else {
            Pair<Long, Long> pair = HttpUtils.getRange(range, objectSize);
            offset = pair.first();
            length = pair.second() - pair.first() + 1;
            response.setStatus(HttpURLConnection.HTTP_PARTIAL);
        }
        bucketLog.replicaMode = replicaMode == null ? "-" : replicaMode.toString();
        bucketLog.ostorId = ostorId;
        bucketLog.ostorKey = ostorKey;
        EtagMaker etagMaker = new EtagMaker();
        Storage storage = new Storage(bucketLog, ostorId);
        try (InputStream in = storage.read(ostorKey, offset, length, ostorKey, pageSize, replicaMode)) {
            try (OutputStream out = response.getOutputStream()) {
                byte[] buffer = localBytes.get8KBytes();
                int bytesRead;
                try {
                    while ((bytesRead = in.read(buffer)) != -1) {
                        try {
                            out.write(buffer, 0, bytesRead);
                            etagMaker.sign(buffer, 0, bytesRead);
                        } catch (IOException e) {
                            log.error(e.getMessage(), e);
                            throw new BaseException(HttpURLConnection.HTTP_BAD_REQUEST, e.getMessage());
                        }
                    }
                    bucketLog.etag = etagMaker.digest();
                    if(length == objectSize && StringUtils.isNotBlank(etag) && !bucketLog.etag.equals(etag)) {
                        SendEtagUnmatchMQ.send(bucketLog.etag, etag, ostorKey, ostorId);
                    }
                } catch (IOException e) {
                    log.error(e.getMessage(), e);
                    throw new BaseException(HttpURLConnection.HTTP_INTERNAL_ERROR, e.getMessage());
                }
            } catch (IOException e) {
                log.error(e.getMessage(), e);
                throw new BaseException(HttpURLConnection.HTTP_BAD_REQUEST, e.getMessage());
            }
        } catch (IOException e) {
            log.error(e.getMessage(), e);
            throw new BaseException(HttpURLConnection.HTTP_INTERNAL_ERROR, e.getMessage());
        }
    }

    private static void handleGetListObjects(HttpServletRequest request, HttpServletResponse response,
            BucketLog bucketLog) throws BaseException, IOException {
        String metaRegion = request.getHeader(InternalConst.X_CTYUN_META_REGION);
        String bucket = urlDecode(request.getHeader(InternalConst.X_CTYUN_BUCKET));
        String prefix = urlDecode(request.getHeader(InternalConst.X_CTYUN_PREFIX));
        String marker = urlDecode(request.getHeader(InternalConst.X_CTYUN_MARKER));
        int maxKeys = request.getHeader(InternalConst.X_CTYUN_MAX_KEYS) == null ?
                Consts.LIST_OBJECTS_MAX_KEY :
                Integer.parseInt(request.getHeader(InternalConst.X_CTYUN_MAX_KEYS));
        String delimiter = urlDecode(request.getHeader(InternalConst.X_CTYUN_DELIMITER));
        ObjectListing ol = null;
        LogUtils.log("Now listing objects from bucket:" + bucket + " prefix:"
                + prefix + " marker:" + marker + " delimiter:" + delimiter
                + " maxkeys:" + maxKeys);
        long listMetaTime = System.currentTimeMillis();
        ObjectMeta object = new ObjectMeta(prefix == null ? "" : prefix, bucket, metaRegion);
        if(client.isBusy(object, OOSConfig.getMaxConcurrencyPerRegionServer()))
            Backoff.backoff();
        ol = client.objectList(metaRegion, bucket, prefix, marker, delimiter, maxKeys);
        listMetaTime = System.currentTimeMillis() - listMetaTime;
        LogUtils.log("List objects from bucket:" + bucket + " prefix:" + prefix
                + " marker:" + marker + " delimiter:" + delimiter + " maxkeys:"
                + maxKeys + " success. Get metadata takes " + listMetaTime + "ms.");
        String respMsg = OpBucket.listObjectsResult(ol);
        HttpHandler.writeResponseEntity(response, respMsg, bucketLog);
    }

    private static void handleGetListParts(HttpServletRequest request, HttpServletResponse response,
            BucketLog bucketLog) throws BaseException {
        String metaRegion = request.getHeader(InternalConst.X_CTYUN_META_REGION);
        String bucket = urlDecode(request.getHeader(InternalConst.X_CTYUN_BUCKET));
        String key = urlDecode(request.getHeader(InternalConst.X_CTYUN_KEY));
        String uploadId = urlDecode(request.getHeader(InternalConst.X_CTYUN_UPLOAD_ID));
        int maxParts = request.getHeader(InternalConst.X_CTYUN_MAX_PARTS) == null ?
                Consts.LIST_UPLOAD_PART_MAXNUM :
                Integer.parseInt(request.getHeader(InternalConst.X_CTYUN_MAX_PARTS));
        int partNumberMarker = request.getHeader(InternalConst.X_CTYUN_PART_NUMBER_MARKER) == null ?
                0 :
                Integer.parseInt(request.getHeader(InternalConst.X_CTYUN_PART_NUMBER_MARKER));
        try {
            String body = OpObject
                    .listParts(metaRegion, bucket, key, uploadId, maxParts,
                            partNumberMarker, bucketLog);
            HttpHandler.writeResponseEntity(response, body, bucketLog);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            if (e instanceof BaseException) {
                throw (BaseException) e;
            }
            throw new BaseException(HttpURLConnection.HTTP_INTERNAL_ERROR, e.getMessage());
        }
    }

    private static void handleGetListUploads(HttpServletRequest request, HttpServletResponse response,
            BucketLog bucketLog) throws BaseException {
        String metaRegion = request.getHeader(InternalConst.X_CTYUN_META_REGION);
        String bucket = urlDecode(request.getHeader(InternalConst.X_CTYUN_BUCKET));
        int maxUploads = request.getHeader(InternalConst.X_CTYUN_MAX_UPLOADS) == null ?
                Consts.LIST_UPLOAD_PART_MAXNUM :
                Integer.parseInt(request.getHeader(InternalConst.X_CTYUN_MAX_UPLOADS));
        String delimiter = urlDecode(request.getHeader(InternalConst.X_CTYUN_DELIMITER));
        String keyMarker = urlDecode(request.getHeader(InternalConst.X_CTYUN_KEY_MARKER));
        String prefix = urlDecode(request.getHeader(InternalConst.X_CTYUN_PREFIX));
        String uploadIdMarker = urlDecode(request.getHeader(InternalConst.X_CTYUN_UPLOAD_ID_MARKER));
        try {
            String body = OpBucket
                    .listMultipartUploads(metaRegion, bucket, maxUploads, delimiter, prefix,
                            keyMarker, uploadIdMarker, request.getHeader(Consts.X_CTYUN_ORIGIN_REQUEST_ID));
            HttpHandler.writeResponseEntity(response, body, bucketLog);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            if (e instanceof BaseException) {
                throw (BaseException) e;
            }
            throw new BaseException(HttpURLConnection.HTTP_INTERNAL_ERROR, e.getMessage());
        }
    }

    private static String urlDecode(String encodeStr) {
        try {
            if (StringUtils.isNotBlank(encodeStr)) {
                return URLDecoder.decode(encodeStr, Consts.STR_UTF8);
            }
        } catch (UnsupportedEncodingException e) {
        }
        return null;
    }

    /**
     * 处理DELETE请求
     * @param request
     * @param response
     * @throws BaseException
     */
    private static void handleDelete(HttpServletRequest request, HttpServletResponse response, BucketLog bucketLog) 
            throws BaseException {
        String ostorKey = request.getHeader(Consts.X_AMZ_OSTOR_KEY);
        String ostorId = request.getHeader(Consts.X_AMZ_OSTOR_ID);
        bucketLog.ostorId = ostorId;
        bucketLog.ostorKey = ostorKey;
        long objectSize = Long.parseLong(request.getHeader(Consts.X_AMZ_OBJECT_SIZE));
        int pageSize = Integer.parseInt(request.getHeader(Consts.X_AMZ_PAGE_SIZE));
        ReplicaMode replicaMode = ReplicaMode.parser(request.getHeader(Consts.X_AMZ_REPLICA_MODE));
        bucketLog.replicaMode = replicaMode == null ? "-" : replicaMode.toString();
        Storage storage = new Storage(bucketLog, ostorId);
        storage.delete(ostorKey, objectSize, ostorKey, pageSize, replicaMode, true);
        response.setStatus(HttpURLConnection.HTTP_NO_CONTENT);
    }
    
    /**
     * 为BSS 提供的批量删除对象接口。 拟批量删除数据库，再异步删除文件。
     * 输入：bucketName , 
     *           本次删除数量。 
     * 输出：删除状态:成功，失败，如果本次查询object数量不足，返回complete，可避免无用调用。  
     * 何时，什么条件下可以删除bucket。 
     * */
    private static void handleBatchDelete(HttpServletRequest request, HttpServletResponse response, 
            BucketLog bucketLog, Pin pin) throws BaseException, IOException{
        String bucketName = request.getHeader(Consts.X_CTYUN_BUCKET_NAME);
        String objectNum = request.getHeader(Consts.X_CTYUN_OBJCETS_NUM);
        String marker = request.getHeader(Consts.X_CTYUN_MARKER);
        String excludePools = request.getHeader(Consts.X_CTYUN_EXCLUDE_POOLS);
        String[] pools = excludePools.split(",");
        List<String> excludePoolList = Arrays.asList(pools);
        
        Integer num;
        try {
            num = new Integer(objectNum);
            if(num <= 0 || num > OOSConfig.getMaxBatchDeleteNum())
                throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_ARGUMENT,
                        "objectNum should between 1~" + OOSConfig.getMaxBatchDeleteNum());
        } catch (NumberFormatException e) {
            throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_ARGUMENT,
                    "objectNum should between 1~" + OOSConfig.getMaxBatchDeleteNum());
        }
        BucketMeta bucket = new BucketMeta(bucketName);
        //查询bucket 失败，反回404
        if(!client.bucketSelect(bucket)) {
            response.setStatus(404);
            response.setHeader("Connection", "close");
            response.getOutputStream().write(getBatchDeleteBody(false, 0, null));
            return;
        }
        
        Triple<String, Boolean, List<ObjectMeta>> triple =  client.objectMetaList(bucket.metaLocation, 
                bucketName, null, marker, null, num);
        marker = triple.first();
        boolean truncated = triple.second();
        List<ObjectMeta> objectMetas = triple.third();
        //过滤出文件位置是私有池的对象。 私有池不删除。 
        objectMetas = objectMetas.stream().filter(e -> !excludePoolList.contains(e.dataRegion)).collect(Collectors.toList());
        if(objectMetas.size() == 0 && !truncated) {
            response.getOutputStream().write(getBatchDeleteBody(truncated, 0, marker));
            response.setStatus(200);// no content
            response.setHeader("Connection", "close");
            try {
                //如果bucket 在第一次删除时发现是空的， 删除bucket。 
                OpBucket.deleteBucket(bucket);
            } catch (Exception e) {
                log.error("internal api delete bucket error " + bucketLog.requestId);
                log.error(e.getMessage(), e);
            }
            return;
        }
        
        if(client.isBusy(objectMetas.get(0), OOSConfig.getMaxConcurrencyPerRegionServer()))
            Backoff.backoff();
        client.batchDeleteObject(objectMetas);
        boolean initialUploadMonitor = true;
        Map<ObjectMeta, List<UploadMeta>> uploadMap = new HashMap<ObjectMeta, List<UploadMeta>>();
        for(ObjectMeta object : objectMetas) {
            if(object.ostorId != null && object.ostorId.trim().length() > 0)
                continue;
            try {
                InitialUploadMeta initialUpload = new InitialUploadMeta(object.metaRegionName, object.bucketName, object.name,
                        null);
                if(initialUploadMonitor && client.isBusy(initialUpload, OOSConfig.getMaxConcurrencyPerRegionServer()))
                    Backoff.backoff();
                initialUploadMonitor = false;
                List<InitialUploadMeta> initialUploads = client.initialSelectAllByObjectId(initialUpload);
                boolean uploadMetaMonitor = true;
                for (InitialUploadMeta initial : initialUploads) {
                    try {
                        UploadMeta upload = new UploadMeta(bucket.metaLocation, initial.uploadId, true);
                        if(uploadMetaMonitor && client.isBusy(upload, OOSConfig.getMaxConcurrencyPerRegionServer()))
                            Backoff.backoff();
                        uploadMetaMonitor = false;
                        List<UploadMeta> uploads = client.uploadSelectByUploadId(upload);
                        uploadMap.put(object, uploads);
                        client.initialUploadDelete(initial);
                        client.uploadDeleteByUploadId(upload);
                    } catch (Exception e) {
                        log.error(e.getMessage(), e);
                    }
                }
            } catch (BaseException be) {
                try {
                    if(be.status == 503)
                        Thread.sleep(1000L);
                } catch (InterruptedException e) {
                    log.warn(e.getMessage(), e);
                }
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }
       
        boolean isSync = false;
        long totalSize = 0;
        
        //删除meta后 ，继续删除文件。
        for(ObjectMeta object : objectMetas) {
            PinData pinData = new PinData();
            long size = 0;
            long redundantSize = 0;
            long alinSize = 0;
            if(object.ostorId != null && object.ostorId.trim().length() > 0) {//普通上传
                try {
                    String ostorKey = Storage.getStorageId(object.storageId);
                    if (object.dataRegion.equals(DataRegion.getRegion().getName())) {
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
                //因为meta 已经删掉了， 如果删除文件失败， 此文件就再不会被删除了。 所以此处不管删除成功与否，容量都重新统计
                size = object.size;
                redundantSize = Utils.getRedundantSize(size, object.storageClass);
                alinSize = Utils.getAlinSize(size, object.ostorPageSize, object.storageClass);
            } else {//分片上传
                List<UploadMeta> uploads = uploadMap.get(object);
                if(null == uploads)
                    continue;
                for (UploadMeta u : uploads) {
                    try {
                        String ostorKey = Storage.getStorageId(u.storageId);
                        if (object.dataRegion.equals(DataRegion.getRegion().getName())) {
                            Storage storage = new Storage(bucketLog, u.ostorId);
                            storage.delete(ostorKey, u.size, u.initialUploadId, u.pageSize,
                                    u.replicaMode, isSync);
                        } else {
                            internalClient.delete(object.dataRegion, ostorKey, u.ostorId, u.size,
                                    u.pageSize, u.replicaMode, bucketLog.requestId);
                        }
                    } catch (Exception e) {
                        log.error("catch delete object storage error:" + u.storageId);
                        log.error(e.getMessage(), e);
                    }
                    size += u.size;
                    redundantSize += Utils.getRedundantSize(u.size, u.replicaMode);
                    alinSize += Utils.getAlinSize(u.size, u.pageSize, u.replicaMode);
                }
            }
            totalSize += size;
            //计费
            pinData.setSize(size, redundantSize, alinSize, object.dataRegion, object.originalDataRegion, object.storageClassString, object.getLastCostTime());
            Utils.pinDecreStorage(bucket.getOwnerId(), pin, pinData, bucket.name);
        }
        
        
        response.getOutputStream().write(getBatchDeleteBody(truncated, totalSize, marker));
        response.setStatus(200);
        response.setHeader("Connection", "close");
        if(!truncated) {
            try {
                //无论删除成功与否，不需要给trustedServer反回删除结果。 
                OpBucket.deleteBucket(bucket);
            } catch (Exception e) {
                log.error("internal api delete bucket error " + bucketLog.requestId);
                log.error(e.getMessage(), e);
            }
        }
    }
    
    /**
     * <apiReturn><status>complete3</status><deletedSize>1</deletedSize><marker>marker2</marker></apiReturn>
     * */
    private static byte[] getBatchDeleteBody(boolean truncated, long totalSize, String marker) {
        XmlWriter xml = new XmlWriter();
        xml.start("BatchDeleteResult");
        xml.start("status").value(truncated ? "success" : "complete");
        xml.end();
        xml.start("deletedSize").value(totalSize + "");
        xml.end();
        if(null != marker) {
            xml.start("marker").value(marker);
            xml.end();
        }
        xml.end();
        return xml.getBytes();
    }
    
    /**
     * 处理来自内部的请求
     * @param baseRequest
     * @param request
     * @param response
     * @throws BaseException
     * @throws IOException 
     */
    public static void handle(Request baseRequest, HttpServletRequest request, HttpServletResponse response,
            BucketLog bucketLog, Pin pin) throws BaseException, IOException {
        bucketLog.internalApiPrepare(request);
        checkAuth(baseRequest, request);
        String method = request.getMethod();
        if (method.equalsIgnoreCase(HttpMethod.PUT.toString())) {
            if (request.getHeader(Consts.X_AMZ_DST_REGION) == null) {
                handlePut(request, response, bucketLog);
            } else {
                handleCopy(request, response, bucketLog);
            }
        } else if (method.equalsIgnoreCase(HttpMethod.POST.toString())) {
            handlePost(request, response, bucketLog);
        } else if (method.equalsIgnoreCase(HttpMethod.GET.toString())) {
            String action = request.getHeader(Consts.X_CTYUN_ACTION);
            if (OOSActions.Bucket_Get.actionName.equals(action)) {
                handleGetListObjects(request, response, bucketLog);
            } else if (OOSActions.Bucket_Get_MultipartUploads.actionName.equals(action)) {
                handleGetListUploads(request, response, bucketLog);
            } else if (OOSActions.Object_Get_ListParts.actionName.equals(action)) {
                handleGetListParts(request, response, bucketLog);
            }else {
                handleGet(request, response, bucketLog);
            }
        } else if (method.equalsIgnoreCase(HttpMethod.DELETE.toString())) {
            if("deleteBucketObjects".equals(request.getParameter("Action")))
                handleBatchDelete(request,response, bucketLog, pin);
            else
                handleDelete(request, response, bucketLog);
        } else {
            throw new BaseException(HttpURLConnection.HTTP_BAD_METHOD, ErrorMessage.ERROR_CODE_INVALID_REQUEST);
        }
    }

}
