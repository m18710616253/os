package cn.ctyun.oos.server;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import cn.ctyun.oos.common.InternalConst;
import cn.ctyun.oos.common.OOSActions;
import common.tuple.Pair;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.CharEncoding;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.auth.SigningAlgorithm;
import com.amazonaws.services.s3.Headers;

import cn.ctyun.common.BaseException;
import cn.ctyun.common.Consts;
import cn.ctyun.common.ReplicaMode;
import cn.ctyun.common.conf.OOSConfig;
import cn.ctyun.common.region.DataRegions;
import cn.ctyun.common.region.DataRegions.DataRegionInfo;
import cn.ctyun.oos.common.ErrorMessage;
import cn.ctyun.oos.common.Utils;
import cn.ctyun.oos.server.signer.V4Signer;
import cn.ctyun.oos.server.storage.EtagMaker;
import common.threadlocal.ThreadLocalBytes;
import common.time.TimeUtils;

public class InternalClient {

    private static final String PROTOCOL= "http";
    private static final String RESOURCE = "/";
    private static final int CHUNCK_LENGTH = 8 * 1024;
    private static final Log log = LogFactory.getLog(InternalClient.class);
    private static final ThreadLocalBytes localBytes = ThreadLocalBytes.current();
    private static final InternalClient instance = new InternalClient();

    private InternalClient() {
    }

    public static InternalClient getInstance() {
        return instance;
    }

    /**
     * 写请求返回的结果
     * @author Erdong
     *
     */
    public static class WriteResult {
        public final String requestId;
        public final String ostorKey;
        public final String ostorId;
        public final ReplicaMode replicaMode;
        public final int pageSize;
        public final String etag;
        public final String regionName;
        public final long objectSize;

        private WriteResult(HttpURLConnection conn, String regionName) {
            requestId = conn.getHeaderField(Headers.REQUEST_ID);
            ostorKey = conn.getHeaderField(Consts.X_AMZ_OSTOR_KEY);
            ostorId = conn.getHeaderField(Consts.X_AMZ_OSTOR_ID);
            replicaMode = ReplicaMode.parser(conn.getHeaderField(Consts.X_AMZ_REPLICA_MODE));
            pageSize = Integer.parseInt(conn.getHeaderField(Consts.X_AMZ_PAGE_SIZE));
            etag = conn.getHeaderField(Headers.ETAG);
            this.regionName = regionName;
            String objSizeHeader = conn.getHeaderField(Consts.X_AMZ_OBJECT_SIZE);
            objectSize = (objSizeHeader == null) ? -1 : Long.valueOf(objSizeHeader);
        }
    }

    /**
     * 获取错误信息
     * @param conn
     * @return
     */
    private static String getErrorMsg(HttpURLConnection conn) {
        try (InputStream in = conn.getInputStream()) {
            return IOUtils.toString(in, Consts.STR_UTF8);
        } catch (IOException e) {
            try (InputStream err = conn.getErrorStream()) {
                if (err != null) {
                    return IOUtils.toString(err, Consts.STR_UTF8);
                } else {
                    return null;
                }
            } catch (IOException e2) {
                log.error(e2.getMessage(), e);
                return null;
            }
        }
    }

    /**
     * 计算签名
     * @param httpVerb
     * @param accessKey
     * @param secretKey
     * @param headers
     * @return
     */
    private static String authorize(String httpVerb, String accessKey, String secretKey, Map<String, String> headers) {
        String contentMD5 = headers.get(Headers.CONTENT_MD5);
        contentMD5 = (contentMD5 == null) ? "" : contentMD5;
        String contentType = headers.get(Headers.CONTENT_TYPE);
        contentType = (contentType == null) ? "" : contentType;
        String date = headers.get(Headers.DATE);
        StringBuilder builder = new StringBuilder();
        builder.append(httpVerb).append("\n").append(contentMD5).append("\n").append(contentType).append("\n")
                .append(date).append("\n");
        List<String> amzHeaders = new ArrayList<>();
        for (Entry<String, String> header : headers.entrySet()) {
            String key = header.getKey().toLowerCase();
            if (key.startsWith("x-amz-")) {
                amzHeaders.add(key.trim() + ":" + header.getValue().trim());
            }
        }
        Collections.sort(amzHeaders);
        for (String header : amzHeaders) {
            builder.append(header).append("\n");
        }
        builder.append(RESOURCE);
        String stringToSign = builder.toString();
        String signature = Utils.sign(stringToSign, secretKey, SigningAlgorithm.HmacSHA1);
        String authorization = "AWS " + accessKey + ":" + signature;
        return authorization;
    }

    DataRegionInfo getRegion(String regionName) throws BaseException {
        return DataRegions.getRegionDataInfo(regionName);
    }

    public boolean canWriteRegion(String regionName) throws BaseException {
        DataRegionInfo region = getRegion(regionName);
        return region.canWrite();
    }

    /**
     * 发起内部PUT/POST请求
     * @param regionName
     * @param bucket
     * @param object
     * @param rm
     * @param size
     * @param input
     * @param requestId 调用内部接口时，目标服务要使用调用者request id追踪请求发起的地方
     * @return
     * @throws BaseException
     */
    public WriteResult write(String regionName, String bucket, String object, String rm, long size,
            InputStream input, boolean isPost,int limitRate,String requestId) throws BaseException {
        DataRegionInfo region = getRegion(regionName);
        try {
            String method = isPost ? "POST" : "PUT";
            URL url = new URL(PROTOCOL, region.getHost(), region.getPort(), RESOURCE);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            Map<String, String> headers = new HashMap<>();
            headers.put(Consts.DATE, TimeUtils.toGMTFormat(new Date()));
            if (rm != null && !rm.isEmpty()) {
                headers.put(Consts.X_AMZ_REPLICA_MODE, rm);
            }
            if (isPost) {
                headers.put(Consts.X_AMZ_OBJECT_SIZE, String.valueOf(size));
            }
            if(limitRate != Consts.USER_NO_LIMIT_RATE)
                headers.put(Consts.X_AMZ_LIMITRATE, limitRate+"");
            headers.put(Headers.CONTENT_TYPE, "application/octet-stream");
            headers.put(Consts.X_AMZ_BUCKET_NAME, bucket);
            headers.put(Consts.X_AMZ_OBJECT_NAME, URLEncoder.encode(object, Consts.STR_UTF8));
            headers.put(Consts.X_AMZ_FORWARD, Boolean.toString(true));
            headers.put(Consts.X_CTYUN_ORIGIN_REQUEST_ID, requestId);
            for (Entry<String, String> header : headers.entrySet()) {
                conn.setRequestProperty(header.getKey(), header.getValue());
            }
            String auth = authorize(method, region.getAccessKey(), region.getSecretKey(), headers);
            conn.setRequestProperty(Consts.AUTHORIZATION, auth);
            conn.setConnectTimeout(OOSConfig.getInternalConnTimeout());
            conn.setReadTimeout(OOSConfig.getInternalReadTimeout());
            conn.setRequestMethod(method);
            conn.setDoOutput(true);
            if (isPost) {
                conn.setChunkedStreamingMode(CHUNCK_LENGTH);
            } else {
                conn.setFixedLengthStreamingMode(size);
            }
            conn.connect();

            EtagMaker etagMaker = new EtagMaker();
            //FIXME 这个地方读流失败应该报客户端异常。
            try (OutputStream out = conn.getOutputStream()) {
                byte[] buffer = localBytes.get8KBytes();
                int bytesRead;
                /*while ((bytesRead = input.read(buffer)) != -1) {
                    etagMaker.sign(buffer, 0, bytesRead);
                    out.write(buffer, 0, bytesRead);
                }*/
                for(;;) {
                    try {
                        bytesRead = input.read(buffer);
                    } catch (IOException e) {
                        log.error("Internal read from client failed, regionName=" + regionName + ", bucket=" + bucket + ", object=" + object +
                                ", message=" + e.getMessage(), e);
                        throw new BaseException(400, ErrorMessage.ERROR_CODE_INCOMPLETE_BODY,
                              ErrorMessage.ERROR_MESSAGE_INCOMPLETE_BODY);
                    }
                    if(bytesRead == -1)
                        break;
                    etagMaker.sign(buffer, 0, bytesRead);
                    out.write(buffer, 0, bytesRead);
                }
            }

            int code = conn.getResponseCode();
            if (code == HttpURLConnection.HTTP_CREATED) {
                V4Signer.checkContentSignatureV4(input);
                WriteResult result = new WriteResult(conn, region.getName());
                if (!etagMaker.digest().equals(result.etag)) {
                    log.error("Etag unmatched, regionName=" + regionName + ", bucket=" + bucket + ", object=" + object);
                    throw new BaseException(HttpURLConnection.HTTP_INTERNAL_ERROR, ErrorMessage.ERROR_MESSAGE_500);
                }
                return result;
            } else {
                log.error("Error response, code=" + code + ", regionName=" + regionName + ", bucket=" + bucket +
                        ", object" + object);
                String msg = getErrorMsg(conn);
                throw new BaseException(code, msg);
            }
        } catch (IOException e) {
            log.error("Internal write failed, regionName=" + regionName + ", bucket=" + bucket + ", object=" + object +
                    ", message=" + e.getMessage(), e);
//            throw new BaseException(400, ErrorMessage.ERROR_CODE_INCOMPLETE_BODY,
//                    ErrorMessage.ERROR_MESSAGE_INCOMPLETE_BODY);
            throw new BaseException(HttpURLConnection.HTTP_INTERNAL_ERROR, ErrorMessage.ERROR_MESSAGE_500);
        }
    }

    /**
     * 发起内部GET请求
     * @param regionName
     * @param ostorKey
     * @param ostorId
     * @param objectSize
     * @param pageSize
     * @param rm
     * @param range
     * @param requestId  调用内部接口时，目标服务要使用调用者request id追踪请求发起的地方
     * @return
     * @throws BaseException
     */
    public InputStream get(String regionName, String ostorKey, String ostorId, long objectSize, int pageSize,
            ReplicaMode rm, String range,String requestId,String etag) throws BaseException {
        DataRegionInfo region = getRegion(regionName);
        try {
            URL url = new URL(PROTOCOL, region.getHost(), region.getPort(), RESOURCE);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            Map<String, String> headers = new HashMap<>();
            headers.put(Consts.DATE, TimeUtils.toGMTFormat(new Date()));
            headers.put(Headers.ETAG, etag);
            headers.put(Consts.X_AMZ_OSTOR_KEY, ostorKey);
            headers.put(Consts.X_AMZ_OSTOR_ID, ostorId);
            headers.put(Consts.X_AMZ_OBJECT_SIZE, String.valueOf(objectSize));
            headers.put(Consts.X_AMZ_PAGE_SIZE, String.valueOf(pageSize));
            headers.put(Consts.X_AMZ_REPLICA_MODE, rm.toString());
            if (range != null) {
                headers.put(Headers.RANGE, range);
            }
            headers.put(Consts.X_AMZ_FORWARD, Boolean.toString(true));
            headers.put(Consts.X_CTYUN_ORIGIN_REQUEST_ID, requestId);
            for (Entry<String, String> header : headers.entrySet()) {
                conn.setRequestProperty(header.getKey(), header.getValue());
            }
            String auth = authorize("GET", region.getAccessKey(), region.getSecretKey(), headers);
            conn.setRequestProperty(Consts.AUTHORIZATION, auth);
            conn.setConnectTimeout(OOSConfig.getInternalConnTimeout());
            conn.setReadTimeout(OOSConfig.getInternalReadTimeout());
            conn.setRequestMethod("GET");
            conn.setDoInput(true);
            conn.connect();

            int code = conn.getResponseCode();
            if (code == HttpURLConnection.HTTP_OK || code == HttpURLConnection.HTTP_PARTIAL) {
                return conn.getInputStream();
            } else {
                String msg = getErrorMsg(conn);
                throw new BaseException(code, msg);
            }
        } catch (IOException e) {
            log.error(e.getMessage(), e);
            throw new BaseException(HttpURLConnection.HTTP_INTERNAL_ERROR, ErrorMessage.ERROR_MESSAGE_500);
        }
    }

    /**
     * 发起内部DELETE请求
     * @param regionName
     * @param ostorKey
     * @param ostorId
     * @param objectSize
     * @param pageSize
     * @param rm
     * @param requestId  调用内部接口时，目标服务要使用调用者request id追踪请求发起的地方
     * @throws BaseException
     */
    public void delete(String regionName, String ostorKey, String ostorId, long objectSize, int pageSize,
            ReplicaMode rm, String requestId) throws BaseException {
        DataRegionInfo region = getRegion(regionName);
        try {
            URL url = new URL(PROTOCOL, region.getHost(), region.getPort(), RESOURCE);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            Map<String, String> headers = new HashMap<>();
            headers.put(Consts.DATE, TimeUtils.toGMTFormat(new Date()));
            headers.put(Consts.X_AMZ_OSTOR_KEY, ostorKey);
            headers.put(Consts.X_AMZ_OSTOR_ID, ostorId);
            headers.put(Consts.X_AMZ_OBJECT_SIZE, String.valueOf(objectSize));
            headers.put(Consts.X_AMZ_PAGE_SIZE, String.valueOf(pageSize));
            headers.put(Consts.X_AMZ_REPLICA_MODE, rm.toString());
            headers.put(Consts.X_AMZ_FORWARD, Boolean.toString(true));
            headers.put(Consts.X_CTYUN_ORIGIN_REQUEST_ID, requestId);
            for (Entry<String, String> header : headers.entrySet()) {
                conn.setRequestProperty(header.getKey(), header.getValue());
            }
            String auth = authorize("DELETE", region.getAccessKey(), region.getSecretKey(), headers);
            conn.setRequestProperty(Consts.AUTHORIZATION, auth);
            conn.setConnectTimeout(OOSConfig.getInternalConnTimeout());
            conn.setReadTimeout(OOSConfig.getInternalReadTimeout());
            conn.setRequestMethod("DELETE");
            conn.setDoInput(true);
            conn.connect();

            int code = conn.getResponseCode();
            if (code != HttpURLConnection.HTTP_NO_CONTENT) {
                String msg = getErrorMsg(conn);
                throw new BaseException(code, msg);
            }
        } catch (IOException e) {
            log.error(e.getMessage(), e);
            throw new BaseException(HttpURLConnection.HTTP_INTERNAL_ERROR, ErrorMessage.ERROR_MESSAGE_500);
        }
    }

    public WriteResult copy(String srcRegionName, String dstRegionName,
            String srcBucketName, String srcObjectName, String dstBucketName,String dstObjectName, String dstRM,
            String range, String requestId) throws BaseException {
        DataRegionInfo srcRegion = getRegion(srcRegionName);
        try {
            URL url = new URL(PROTOCOL, srcRegion.getHost(), srcRegion.getPort(), RESOURCE);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            Map<String, String> headers = new HashMap<>();
            headers.put(Consts.X_AMZ_DST_REGION, dstRegionName);
            headers.put(Consts.X_AMZ_BUCKET_NAME, dstBucketName);
            headers.put(Consts.X_AMZ_OBJECT_NAME, URLEncoder.encode(dstObjectName, Consts.STR_UTF8));
            headers.put(Consts.DATE, TimeUtils.toGMTFormat(new Date()));
            headers.put(Consts.X_AMZ_SOURCE_BUCKET_NAME, srcBucketName);
            headers.put(Consts.X_AMZ_SOURCE_OBJECT_NAME, URLEncoder.encode(srcObjectName, Consts.STR_UTF8));
            if (dstRM != null) {
                headers.put(Consts.X_AMZ_DST_REPLICA_MODE, dstRM);
            }
            if (range != null) {
                headers.put(Headers.RANGE, range);
            }
            headers.put(Consts.X_AMZ_FORWARD, Boolean.toString(true));
            headers.put(Consts.X_CTYUN_ORIGIN_REQUEST_ID, requestId);
            for (Entry<String, String> header : headers.entrySet()) {
                conn.setRequestProperty(header.getKey(), header.getValue());
            }
            String auth = authorize("PUT", srcRegion.getAccessKey(), srcRegion.getSecretKey(), headers);
            conn.setRequestProperty(Consts.AUTHORIZATION, auth);
            conn.setConnectTimeout(OOSConfig.getInternalConnTimeout());
            conn.setReadTimeout(OOSConfig.getInternalReadTimeout());
            conn.setRequestMethod("PUT");
            conn.setDoOutput(true);
            conn.connect();

            int code = conn.getResponseCode();
            if (code == HttpURLConnection.HTTP_CREATED) {
                WriteResult result = new WriteResult(conn, dstRegionName);
                return result;
            } else {
                String msg = getErrorMsg(conn);
                throw new BaseException(code, msg);
            }
        } catch (IOException e) {
            log.error(e.getMessage(), e);
            throw new BaseException(HttpURLConnection.HTTP_INTERNAL_ERROR, ErrorMessage.ERROR_MESSAGE_500);
        }
    }

    /**
     * 去指定资源池查询
     * @param dataRegion
     * @param metaRegion
     * @param bucket
     * @param prefix
     * @param marker
     * @param maxKeys
     * @param delimiter
     * @param requestId
     * @return
     * @throws BaseException
     */
    public String listObjects(String dataRegion, String metaRegion, String bucket, String prefix, String marker, int maxKeys, String delimiter,
            String requestId) throws BaseException {
        Map<String, String> headers = new HashMap<>();
        headers.put(InternalConst.X_CTYUN_META_REGION, metaRegion);
        headers.put(InternalConst.X_CTYUN_BUCKET, bucket);
        headers.put(InternalConst.X_CTYUN_PREFIX, prefix);
        headers.put(InternalConst.X_CTYUN_MARKER, marker);
        headers.put(InternalConst.X_CTYUN_MAX_KEYS, String.valueOf(maxKeys));
        headers.put(InternalConst.X_CTYUN_DELIMITER, delimiter);
        return sendGetRequest(dataRegion, OOSActions.Bucket_Get.actionName, requestId, headers);
    }

    /**
     * @param dataRegion
     * @param metaRegion
     * @param bucket
     * @param key
     * @param uploadId
     * @param maxParts
     * @param partNumberMarker
     * @param requestId
     * @return
     * @throws BaseException
     */
    public String listParts(String dataRegion, String metaRegion, String bucket, String key, String uploadId, int maxParts, int partNumberMarker,
            String requestId) throws BaseException {
        Map<String, String> headers = new HashMap<>();
        headers.put(InternalConst.X_CTYUN_META_REGION, metaRegion);
        headers.put(InternalConst.X_CTYUN_BUCKET, bucket);
        headers.put(InternalConst.X_CTYUN_KEY, key);
        headers.put(InternalConst.X_CTYUN_UPLOAD_ID, uploadId);
        headers.put(InternalConst.X_CTYUN_MAX_PARTS, String.valueOf(maxParts));
        headers.put(InternalConst.X_CTYUN_PART_NUMBER_MARKER, String.valueOf(partNumberMarker));
        return sendGetRequest(dataRegion, OOSActions.Object_Get_ListParts.actionName, requestId, headers);
    }

    /**
     * @param dataRegion
     * @param metaRegion
     * @param bucket
     * @param maxUploads
     * @param delimiter
     * @param prefix
     * @param keyMarker
     * @param uploadIdMarker
     * @param requestId
     * @return
     * @throws BaseException
     */
    public String listUploads(String dataRegion, String metaRegion, String bucket, int maxUploads, String delimiter, String prefix, String keyMarker,
            String uploadIdMarker, String requestId) throws BaseException {
        Map<String, String> headers = new HashMap<>();
        headers.put(InternalConst.X_CTYUN_META_REGION, metaRegion);
        headers.put(InternalConst.X_CTYUN_BUCKET, bucket);
        headers.put(InternalConst.X_CTYUN_MAX_UPLOADS, String.valueOf(maxUploads));
        headers.put(InternalConst.X_CTYUN_DELIMITER, delimiter);
        headers.put(InternalConst.X_CTYUN_PREFIX, prefix);
        headers.put(InternalConst.X_CTYUN_KEY_MARKER, keyMarker);
        headers.put(InternalConst.X_CTYUN_UPLOAD_ID_MARKER, uploadIdMarker);
        return sendGetRequest(dataRegion, OOSActions.Bucket_Get_MultipartUploads.actionName, requestId, headers);
    }

    private String sendGetRequest(String dataRegion, String action, String requestId, Map<String, String> reqHeaders)
            throws BaseException {
        DataRegionInfo region = getRegion(dataRegion);
        InputStream inputStream = null;
        try {
            Map<String, String> headers = new HashMap<>();
            for (String k : reqHeaders.keySet()) {
                String v = reqHeaders.get(k);
                if (StringUtils.isNotBlank(v)) {
                    headers.put(k, URLEncoder.encode(v, Consts.STR_UTF8));
                }
            }
            URL url = new URL(PROTOCOL, region.getHost(), region.getPort(), RESOURCE);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            headers.put(Consts.X_CTYUN_ACTION, action);
            headers.put(Consts.DATE, TimeUtils.toGMTFormat(new Date()));
            headers.put(Consts.X_AMZ_FORWARD, Boolean.toString(true));
            headers.put(Consts.X_CTYUN_ORIGIN_REQUEST_ID, requestId);

            for (Entry<String, String> header : headers.entrySet()) {
                conn.setRequestProperty(header.getKey(), header.getValue());
            }
            String auth = authorize("GET", region.getAccessKey(), region.getSecretKey(), headers);
            conn.setRequestProperty(Consts.AUTHORIZATION, auth);
            conn.setConnectTimeout(OOSConfig.getInternalConnTimeout());
            conn.setReadTimeout(OOSConfig.getInternalReadTimeout());
            conn.setRequestMethod("GET");
            conn.setDoInput(true);
            conn.connect();

            int code = conn.getResponseCode();
            if (code == HttpURLConnection.HTTP_OK) {
                inputStream = conn.getInputStream();
                return IOUtils.toString(inputStream);
            } else {
                inputStream = conn.getErrorStream();
                String msg = IOUtils.toString(inputStream);
                Pair<String, String> codeAndMsg = extractError(msg);
                log.error("requestId: " + requestId + ", code: " + code + ", msg: " + msg);
                throw new BaseException(code, codeAndMsg.first(), codeAndMsg.second());
            }
        } catch (IOException e) {
            log.error(e.getMessage(), e);
            throw new BaseException(HttpURLConnection.HTTP_INTERNAL_ERROR, ErrorMessage.ERROR_MESSAGE_500);
        } finally {
            IOUtils.closeQuietly(inputStream);
        }
    }

    private Pair<String, String> extractError(String exceptStr) {
        Pair<String, String> pair = new Pair<>(StringUtils.EMPTY, StringUtils.EMPTY);
        if (StringUtils.isNotEmpty(exceptStr)) {
            int cs = exceptStr.indexOf("<Code>");
            int ce = exceptStr.indexOf("</Code>");
            if (cs > -1 && ce > -1 && ce > cs) {
                pair.first(exceptStr.substring(cs, ce));
            } else {
                return pair;
            }
            int ms = exceptStr.indexOf("<Message>", ce);
            int me = exceptStr.indexOf("</Message>", ce);
            if (ms > -1 && me > -1 && me > ms) {
                pair.second(exceptStr.substring(ms, me));
            }
        }
        return pair;
    }
}
