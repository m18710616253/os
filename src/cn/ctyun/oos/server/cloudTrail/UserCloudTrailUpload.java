package cn.ctyun.oos.server.cloudTrail;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.net.URLEncoder;
import java.security.SecureRandom;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.Header;
import org.apache.http.client.methods.HttpPut;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.amazonaws.auth.SigningAlgorithm;
import com.amazonaws.services.s3.internal.RestUtils;

import cn.ctyun.common.BaseException;
import cn.ctyun.common.Consts;
import cn.ctyun.common.Program;
import cn.ctyun.common.conf.DataRegion;
import cn.ctyun.common.conf.GlobalHHZConfig;
import cn.ctyun.common.dsync.DLock;
import cn.ctyun.common.dsync.DSyncService;
import cn.ctyun.common.node.OosZKNode;
import cn.ctyun.common.region.DataRegions;
import cn.ctyun.common.region.DataRegions.DataRegionInfo;
import cn.ctyun.common.utils.LogUtils;
import cn.ctyun.oos.common.ErrorMessage;
import cn.ctyun.oos.common.OOSRequest;
import cn.ctyun.oos.common.Utils;
import cn.ctyun.oos.hbase.MetaClient;
import cn.ctyun.oos.metadata.AkSkMeta;
import cn.ctyun.oos.metadata.CloudTrailMeta;
import cn.ctyun.oos.metadata.ManageEventMeta;
import cn.ctyun.oos.metadata.OwnerMeta;
import cn.ctyun.oos.server.conf.CloudTrailConfig;
import common.io.StreamUtils;
import common.time.TimeUtils;
import common.tuple.Pair;

/**
 * 用于收集配置了日志追踪规则的用户的文件内容，并构建成日志文件，上传到用户规定的buckets中
 * 
 * @author wushuang
 *
 */
public class UserCloudTrailUpload implements Program {
    static {
        System.setProperty("log4j.log.app", "userCloudTrailFileUpload");
    }
    private static Log log = LogFactory.getLog(UserCloudTrailUpload.class);
    private static MetaClient metaClient = MetaClient.getGlobalClient();

    /** 所有为开启状态的cloudTrail */
    private static Map<Long, List<CloudTrailMeta>> allCloudTrailCache = new ConcurrentHashMap<Long, List<CloudTrailMeta>>();
    /** 定时更新allCloudTrailCache */
    private static final Timer timer = new Timer();
    /** 任务队列 */
    private static LinkedBlockingQueue<UploadTask> taskQueue = new LinkedBlockingQueue<>();
    /** 执行任务的线程数 */
    private static int threadNum = 3;
    /** 每个跟踪每次取出的管理事件数量 */
    private static int manageEventListResultNum = 500;

    /** 日志文件最大大小 */
    private static long maxFileLength;
    /** 当上传日志文件失败时，重试次数 */
    private static int retryTime;
    private static final long five_minutes = 5 * 60 * 1000;
    private static final long ten_minutes = 10 * 60 * 1000;

    private static final char[] CHAR_32 = new char[] { 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P',
            'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm',
            'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9' };
    private static final DateTimeFormatter formatyyyy_mm_dd_hh_mm_ss = DateTimeFormatter.ofPattern("uuuu-MM-dd-HH-mm-ss");

    public static void main(String[] args) throws Exception {
        new UserCloudTrailUpload().exec(args);
    }

    @Override
    public String usage() {
        return "Usage: \n";
    }

    @Override
    public void exec(String[] args) throws Exception {
        DSyncService dsync = new DSyncService(GlobalHHZConfig.getQuorumServers(), GlobalHHZConfig.getSessionTimeout());
        try {
            DLock lock = dsync.createLock(OosZKNode.userCloudTrailUploadLock, null);
            lock.lock();
            LogUtils.startSuccess();
            try {
                // 每5分钟更新cloudTrail缓存
                CacheUpdate cacheUpdate = new CacheUpdate();
                timer.schedule(cacheUpdate, 0, five_minutes);

                // 每10分钟执行收集与上传日志文件操作
                while (true) {
                    try {
                        maxFileLength = CloudTrailConfig.maxTrailFileLength;
                        retryTime = CloudTrailConfig.trailUploadRetryTime;
                        threadNum = CloudTrailConfig.threadNum;
                        manageEventListResultNum = CloudTrailConfig.manageEventListResultNum;
                        
                        long nowTime = System.currentTimeMillis();
                        for (Entry<Long, List<CloudTrailMeta>> v : allCloudTrailCache.entrySet()) {
                            taskQueue.add(new UploadTask(v.getKey(), v.getValue(), nowTime));
                        }
                        CountDownLatch countDownLatch = new CountDownLatch(threadNum);
                        for (int i = 0; i < threadNum; i++) {
                            new Thread() {
                                public void run() {
                                    while (true) {
                                        try {
                                            UploadTask task = taskQueue.poll(1, TimeUnit.SECONDS);
                                            if (task != null)
                                                task.run();
                                            else
                                                break;
                                        } catch (Exception e) {
                                            log.error(e);
                                        }
                                    }
                                    countDownLatch.countDown();
                                }
                            }.start();
                        }

                        Thread.sleep(ten_minutes);
                        countDownLatch.await();
                    } catch (InterruptedException e) {
                        log.error(e);
                    }
                }
            } catch (Throwable e) {
                log.error(e);
            }
        } finally {
            dsync.close();
        }
    }

    class CacheUpdate extends TimerTask {
        @Override
        public void run() {
            try {
                allCloudTrailCache = metaClient.cloudTrailListByStatus(-1, true);
            } catch (Throwable e) {
                log.error("UserCloudTrailUpload update all cloudtrail cache failed!", e);
            }
        }
    }

    /**
     * 某个用户的管理事件日志文件上传
     *
     */
    class UploadTask {
        long ownerId;
        List<CloudTrailMeta> cloudTrails;
        long nowTime;

        private final Random random = new SecureRandom();

        public UploadTask(long ownerId, List<CloudTrailMeta> cloudTrails, long nowTime) {
            this.ownerId = ownerId;
            this.cloudTrails = cloudTrails;
            this.nowTime = nowTime;
        }

        public void run() {
            try {
                //取用户隐藏aksk，如未取到，取原主aksk，如果用户没有aksk，则创建隐藏aksk
                OwnerMeta ownerMeta = new OwnerMeta(ownerId);
                metaClient.ownerSelectById(ownerMeta);
                AkSkMeta akSkMeta = Utils.getAvailableAk(ownerMeta);
                if (akSkMeta == null || akSkMeta.accessKey == null || akSkMeta.accessKey.length() == 0) {
                    akSkMeta = metaClient.getOrCreateBuiltInAK(ownerMeta);
                }
                if (akSkMeta == null) {
                    log.error("User CloudTrail Upload:user does not have ak, and create hidden ak failed! user:" + ownerMeta.getName() + " ownerId:" + ownerId);
                    return;
                }

                nowTime -= 10000; // 10s 之前

                for (CloudTrailMeta cMeta : cloudTrails) {
                    // 将需要构建成文件的事件的时间范围
                    long lastEventTime = cMeta.lastEventTimeVersion > cMeta.startLoggingTime ? cMeta.lastEventTimeVersion : cMeta.startLoggingTime;
                    String nextToken = "";
                    boolean success = true;
                    while (nextToken != null) {
                        // 追踪是否开启
                        if (cMeta.status == false)
                            continue;
                        // 追踪是否记录管理事件
                        if (!cMeta.getManageEventTrailInfo().first())
                            continue;

                        // 按时间范围，读写类型，一次不超过n条 按时间从小到大排序，在查询接口增加查询处理得到范围数据
                        LinkedList<ManageEventMeta> manageEvents = (LinkedList<ManageEventMeta>) metaClient
                                .manageEventListByTimeRangeWithRWType(ownerId, lastEventTime, nowTime, cMeta.getManageEventTrailInfo().second(), nextToken, manageEventListResultNum);
                        if (manageEvents == null || manageEvents.size() == 0)
                            break;
                        if (manageEvents.size() >= manageEventListResultNum) {
                            nextToken = manageEvents.getLast().getRowKey();
                            nextToken = String.copyValueOf(nextToken.toCharArray(), 0, nextToken.length() - 1) + Character.MIN_VALUE;
                        } else {
                            nextToken = null;
                        }

                        while (true) {
                            if (manageEvents == null || manageEvents.size() == 0)
                                break;
                            Pair<Pair<Long, Long>, String> fileContent = buildTrailFileContent(cMeta, manageEvents);
                            if (fileContent.second().length() != 0) {
                                String fileName = createTrailName(ownerMeta, fileContent.first().first(), cMeta.trailName, cMeta.prefix);
                                // 如果失败则重试n次
                                for (int i = 0; i < retryTime; i++) {
                                    ByteArrayInputStream is = null;
                                    try {
                                        is = new ByteArrayInputStream(fileContent.second().getBytes());
                                        // 根用户不判断iam权限
                                        success = uploadToOOSBucket(ownerMeta, cMeta.targetBucket, fileName, akSkMeta.accessKey,
                                                akSkMeta.getSecretKey(), is, fileContent.second().getBytes().length);
                                        if (success) {
                                            log.info("User CloudTrail Upload: upload success! fileName:" + fileName);
                                            break;
                                        }
                                    } catch (Exception e) {
                                        success = false;
                                        log.error("User CloudTrail Upload: upload failed! fileName:" + fileName + " Retry time:" + i, e);
                                    } finally {
                                        if (is != null)
                                            try {
                                                is.close();
                                            } catch (Throwable t) {
                                                log.error("ByteArrayInputStream close error!", t);
                                            }
                                    }
                                }
                            }
                            // 如果没上传成功，开始处理下个跟踪
                            if (!success)
                                break;
                            // 如果成功更新上传文件时间
                            cMeta.lastEventTimeVersion = fileContent.first().second();
                            cMeta.lastDeliveryTime = LocalDateTime.now().toInstant(ZoneOffset.of("+8")).toEpochMilli();
                            metaClient.CloudTrailUpdateLastTrailTimeAndLastEventTime(cMeta); // FIXME hbase压力？
                        }
                        if (!success)
                            break;
                    }
                }

            } catch (Exception e) {
                log.error("User CloudTrail Upload: error.");
                log.error(e);
            }
        }

        /**
         * 构建日志文件内容
         * 
         * @param cloudTrail
         * @param manageEvents
         * @return Pair<Long, String> 文件中第一个事件的时间，文件内容
         * @throws JSONException
         */
        private Pair<Pair<Long, Long>, String> buildTrailFileContent(CloudTrailMeta cloudTrail,
                LinkedList<ManageEventMeta> manageEvents) throws JSONException {
            int fileLength = 0;
            JSONObject fileJsonObject = new JSONObject();
            JSONArray fileArray = new JSONArray();
            long firstEventTime = 0;
            long lastEventTime = 0;
            while (true) {
                ManageEventMeta e = manageEvents.pollFirst();
                if (e == null)
                    break;
                lastEventTime = e.getEvent().eventTime;
                if (e.getEvent().eventTime < cloudTrail.lastEventTimeVersion)
                    continue;
                String content = e.getShowJson().toString();
                // 日志文件大小不超过限制，超过限制重新加回队列
                if (fileLength + content.getBytes().length >= maxFileLength) {
                    manageEvents.addFirst(e);
                    break;
                }
                fileArray.put(e.getShowJson());
                fileLength += content.getBytes().length;
                if (firstEventTime == 0)
                    firstEventTime = e.getEvent().eventTime;
            }
            fileJsonObject.put("Records", fileArray);
            return new Pair<Pair<Long, Long>, String>(new Pair<Long, Long>(firstEventTime, lastEventTime), fileJsonObject.toString());
        }

        /**
         * 构建日志文件名称
         * 
         * @param fileTime
         * @param trailName
         * @param prefix
         * @return
         */
        public String createTrailName(OwnerMeta owner, long fileTime, String trailName, String prefix) {
            Instant is = Instant.ofEpochMilli(fileTime);
            LocalDateTime ldt = LocalDateTime.ofInstant(is, ZoneOffset.of("+0"));
            // 日志审计日志文件时间格式
            String fileTimeFormat = ldt.format(formatyyyy_mm_dd_hh_mm_ss);
            prefix = prefix == null ? "" : (prefix + "/");
            String path = prefix + "OOSLogs/" + owner.getAccountId() + "/CloudTrail/" + ldt.getYear() + "/" + ldt.getMonthValue() + "/"
                    + ldt.getDayOfMonth() + "/";
            // 生成文件末尾随机12位字符串
            StringBuilder randomString = new StringBuilder();
            for (int i = 0; i < 12; i++)
                randomString.append(CHAR_32[random.nextInt(CHAR_32.length)]);
            String name = owner.getAccountId() + "_CloudTrail_UTC" + fileTimeFormat + "_" + trailName + "_" + randomString.toString();
            return path + name;
        }
        
        /**
         * 获取用户标签数据域
         * @param owner
         * @return
         * @throws IOException
         * @throws BaseException
         */
        private DataRegionInfo getUserDataRegionInfo(OwnerMeta owner) throws IOException, BaseException {
            Pair<Set<String>, Set<String>> regions = metaClient.getRegions(owner.getId());
            if (regions.first().size() == 0 || regions.second().size() == 0)
                throw new BaseException("the user does not allocate the regions.", 403,
                        ErrorMessage.ERROR_CODE_INVALID_USER);
            String cloudTrailUploadRegion = DataRegion.getRegion().getName();
            Set<String> ownerDataRegions = regions.second();
            if (ownerDataRegions.contains(cloudTrailUploadRegion)) {
                return DataRegions.getRegionDataInfo(cloudTrailUploadRegion);
            } else {
                return DataRegions.getRegionDataInfo((String) ownerDataRegions.toArray()[0]);
            }
        }
        
        /**
         * 上传到指定bucket
         * @param owner
         * @param bucketName
         * @param objectName
         * @param ak
         * @param sk
         * @param input
         * @param length
         * @return
         * @throws IOException
         * @throws BaseException
         */
        private boolean uploadToOOSBucket(OwnerMeta owner, String bucketName, String objectName, String ak, String sk,
                InputStream input, long length) throws IOException, BaseException {            
            String host = "";
            int port;
            DataRegionInfo userRegionInfo = getUserDataRegionInfo(owner);
            host = userRegionInfo.getHost();
            port = userRegionInfo.getPort();
            
            String path = "/" + bucketName + "/" + URLEncoder.encode(objectName, Consts.STR_UTF8);
            URL url = new URL("http", host, port, path);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            HttpPut httpPut = new HttpPut();
            httpPut.setURI(URI.create(path));
            String date = TimeUtils.toGMTFormat(new Date());
            String resourcePath = Utils.toResourcePath(bucketName, objectName, false);
            httpPut.addHeader("Date", date);
            httpPut.addHeader("Host", host + ":" + port);
            httpPut.addHeader("User-Agent", "");
            @SuppressWarnings("unchecked")
            String canonicalString = RestUtils.makeS3CanonicalString("PUT", resourcePath,
                    new OOSRequest(httpPut), null);
            String signature = Utils.sign(canonicalString, sk, SigningAlgorithm.HmacSHA1);
            String authorization = "AWS " + ak + ":" + signature;
            conn.setRequestProperty("Authorization", authorization);
            for (Header e : httpPut.getAllHeaders()) {
                conn.setRequestProperty(e.getName(), e.getValue());
            }
            conn.setRequestProperty("Connection", "close");
            conn.setRequestMethod("PUT");
            conn.setDoOutput(true);
            conn.setConnectTimeout(30 * 1000);
            conn.setReadTimeout(30 * 1000);
            conn.setFixedLengthStreamingMode(length);
            conn.connect();
            try (OutputStream out = conn.getOutputStream()) {
                if (input != null) {
                    StreamUtils.copy(input, out, length);
                }
            }
            int oosCode = conn.getResponseCode();
            String oosMessage = conn.getResponseMessage();
            if (oosCode == 200) {
                return true;
            } else {
                throw new BaseException(oosCode, oosMessage);
            }
        }

    }
}
