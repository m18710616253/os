package cn.ctyun.oos.server.lifecycle;

import java.io.IOException;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

import org.apache.commons.lang.time.DateUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.util.Bytes;
import org.json.JSONException;
import org.json.JSONObject;

import com.google.common.collect.Lists;

import cn.ctyun.common.Program;
import cn.ctyun.common.conf.MetaRegion;
import cn.ctyun.common.conf.OOSConfig;
import cn.ctyun.common.conf.RegionHHZConfig;
import cn.ctyun.common.ganglia.GangliaConsts;
import cn.ctyun.common.ganglia.GangliaMan;
import cn.ctyun.common.ganglia.GangliaPlugin;
import cn.ctyun.common.model.BucketLifecycleConfiguration;
import cn.ctyun.common.model.BucketLifecycleConfiguration.Rule;
import cn.ctyun.common.model.BucketLifecycleConfiguration.Transition;
import cn.ctyun.common.model.ObjectLockConfiguration;
import cn.ctyun.common.utils.HostUtils;
import cn.ctyun.common.utils.LogUtils;
import cn.ctyun.oos.common.Utils;
import cn.ctyun.oos.hbase.HBaseConnectionManager;
import cn.ctyun.oos.hbase.HBaseProgress;
import cn.ctyun.oos.hbase.MetaClient;
import cn.ctyun.oos.metadata.BucketMeta;
import cn.ctyun.oos.metadata.InitialUploadMeta;
import cn.ctyun.oos.metadata.LifecycleProgress;
import cn.ctyun.oos.metadata.ObjectMeta;
import cn.ctyun.oos.metadata.OwnerMeta;
import cn.ctyun.oos.metadata.UploadMeta;
import cn.ctyun.oos.server.OpObject;
import cn.ctyun.oos.server.backoff.Backoff;
import cn.ctyun.oos.server.count.Pin;
import cn.ctyun.oos.server.count.PinData;
import cn.ctyun.oos.server.lifecycle.LifecycleConfig.DeleteSpeedLimit;
import cn.ctyun.oos.server.lifecycle.LifecycleConfig.SpeedLimit;
import cn.ctyun.oos.server.lifecycle.LifecycleConfig.TransitionSpeedLimit;
import cn.ctyun.oos.server.log.BucketLog;
import cn.ctyun.oos.server.usage.FiveMinuteUsage;
import cn.ctyun.oos.server.util.Misc;
import cn.ctyun.oos.server.util.ObjectLockUtils;
import common.tuple.Pair;
import common.tuple.Triple;
import common.util.BlockingExecutor;

/**
 * TODO：
 *  1、FindExpiredObjectInBucket筛选出需要被处理的过期对象，不再写入消息队列，而是直接放到内存缓存（messages）中交给DeleteObjects进行处理。
 *  2、在6.0中每个数据域（元数据域）都启动至少一个Lifecycle，且只处理本数据域（元数据域）对应的bucket中的对象。
 *  3、配置文件放在本地zk，锁仍然是全局zk
 *  4、增加统计信息。
 *  5 任务生产线程按原速度不变，
 *      删除和转储使用不同的限速逻辑。
 *      增大转储线程池的队列数，增加承载转储的对象数量。
 *  6 加杀掉进程后刷新缓存代码。
 */
public class Lifecycle implements Program {
    static {
        System.setProperty("log4j.log.app", "lifecycle");
    }
    private static final Log log = LogFactory.getLog(Lifecycle.class);
    private static MetaClient globlaMetaClient;
    private static MetaClient localMetaClient;
    private static String hostName;
    private static BlockingExecutor delObjPool;
    private static BlockingExecutor tranObjPool;
    private static final int TYPE_DELETE = 1;
    private static final int TYPE_TRANSITION = 2;
    private static final int MB = 1024 * 1024;
    private static final LongAdder DELETE_OBJECT_NUMBER_COUNT = new LongAdder();
    public static final Pin PIN = new Pin();
    private static final ScheduledThreadPoolExecutor SCHEDULE_THREAD_POOL = new ScheduledThreadPoolExecutor(1, new ThreadFactory() {
        private final AtomicLong l = new AtomicLong();
        @Override
        public Thread newThread(Runnable r) {
            String name = "Lifecycle-Schedule-Thread-";
            return new Thread(r, name + l.getAndIncrement());
        }
    });

    @Override
    public String usage() {
        return "find and handle expired objects.";
    }

    public static void main(String[] args) throws Exception {
        new Lifecycle().exec(args);
    }

    @Override
    public void exec(String[] args) throws Exception {
        hostName = HostUtils.getHostName();
        globlaMetaClient = MetaClient.getGlobalClient();
        localMetaClient = MetaClient.getRegionClient();
        Configuration regionConf = RegionHHZConfig.getConfig();
        HConnection regionConn = HBaseConnectionManager.createConnection(regionConf);
        HBaseAdmin regionHbaseAdmin = new HBaseAdmin(regionConn);
        HBaseProgress.createTable(regionHbaseAdmin);
        
        int threadNum = LifecycleConfig.deleteObjThreads;
        delObjPool = new BlockingExecutor(threadNum, threadNum,
                10000, 1000, "delObjPool");
        tranObjPool = new BlockingExecutor(LifecycleConfig.transitionObjThreads,
                LifecycleConfig.transitionObjThreads, 10000, 1000, "tranObjPool");
        //钩子，确保关闭时将缓存刷进数据库
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                FiveMinuteUsage.flushBeforeExit();
            }
        });
        FindExpiredObjects findObjects = new FindExpiredObjects();
        findObjects.start();
        LogUtils.startSuccess();

        // 每分钟向ganglia发送 Lifecycle删除的对象数目
        String gangliaFakeIp = GangliaMan.getFakeIpHost(GangliaMan.OOS_GROUP_NAME);
        SCHEDULE_THREAD_POOL.scheduleAtFixedRate(() -> {
            try {
                GangliaPlugin.sendToGangliaWithFakeIp(GangliaConsts.LIFECYCLE_SPEED,
                        String.valueOf(DELETE_OBJECT_NUMBER_COUNT.sumThenReset()),
                        GangliaConsts.YTYPE_INT32,
                        GangliaConsts.YNAME_NUM,
                        GangliaConsts.GROUP_NAME_OOS_LIFECYCLE,
                        "\"lifecycle delete object count per min\"",
                        gangliaFakeIp);
            } catch (Throwable e) {
                log.error("Lifecycle sendToGanglia error! now timestamp : " + System.currentTimeMillis() + " DELETE_OBJECT_NUMBER_COUNT : " + DELETE_OBJECT_NUMBER_COUNT.sumThenReset(), e);
            }
        }, 1, 1, TimeUnit.MINUTES);

        while (true) {
            try {
                Thread.sleep(Long.MAX_VALUE);
            } catch (InterruptedException e) {
            }
        }
    }

    /**
     * 记录统计信息，以bucket为单位，每轮扫描产生一个统计
     */
    static class Stats {
        //统计起始时间
        private Date startTime;
        //已经扫描的object数量
        private AtomicLong totalScannedNum;
        //已经删除的object数量
        private AtomicLong totalDeletedNum;
        //删除对象版本数量
        private AtomicLong totalDeletedVersionNum;
        //已经删除的object总容量
        private AtomicLong totalDeletedSize;

        //已经转储的object数量
        private AtomicLong totalTransitionedNum;
        //已经转储的object总容量
        private AtomicLong totalTransitionedSize;
        
        public void init() {
            startTime = new Date();
            totalScannedNum = new AtomicLong();
            totalDeletedNum = new AtomicLong();
            totalDeletedVersionNum = new AtomicLong();
            totalDeletedSize = new AtomicLong();
            totalTransitionedNum = new AtomicLong();
            totalTransitionedSize = new AtomicLong();
        }
        
        public void init(String jsonString) {
            init();
            if(null == jsonString)
                return;
            try {
                JSONObject jo = new JSONObject(jsonString);
                if(jo.has("startTime"))
                    startTime = Misc.formatyyyymmddhhmmss(jo.getString("startTime"));
                if(jo.has("totalScannedNum"))
                    totalScannedNum = new AtomicLong(jo.getLong("totalScannedNum"));
                if(jo.has("totalDeletedNum"))
                    totalDeletedNum = new AtomicLong(jo.getLong("totalDeletedNum"));
                if(jo.has("totalDeletedSize"))
                    totalDeletedSize = new AtomicLong(jo.getLong("totalDeletedSize"));
                if(jo.has("totalTransitionedNum"))
                    totalTransitionedNum = new AtomicLong(jo.getLong("totalTransitionedNum"));
                if(jo.has("totalTransitionedSize"))
                    totalTransitionedSize = new AtomicLong(jo.getLong("totalTransitionedSize"));
                if (jo.has("totalDeletedVersionNum"))
                    totalDeletedVersionNum = new AtomicLong(jo.getLong("totalDeletedVersionNum"));
            } catch (Exception e) {
                log.warn("lifecycle parse stats json error : " + jsonString, e);
            }
        }
        
        public void add(Stats stats) {
            totalScannedNum.addAndGet(stats.totalScannedNum.get());
            totalDeletedNum.addAndGet(stats.totalDeletedNum.get());
            totalDeletedVersionNum.addAndGet(stats.totalDeletedVersionNum.get());
            totalDeletedSize.addAndGet(stats.totalDeletedSize.get());
            totalTransitionedNum.addAndGet(stats.totalTransitionedNum.get());
            totalTransitionedSize.addAndGet(stats.totalTransitionedSize.get());
        }
        
        public String getString() {
            JSONObject jo = new JSONObject();
            try {
                jo.put("startTime", Misc.formatyyyymmddhhmmss(startTime));
                jo.put("totalScannedNum", totalScannedNum);
                jo.put("totalDeletedNum", totalDeletedNum);
                jo.put("totalDeletedVersionNum", totalDeletedVersionNum);
                jo.put("totalDeletedSize", totalDeletedSize);
                jo.put("totalTransitionedNum", totalTransitionedNum);
                jo.put("totalTransitionedSize", totalTransitionedSize);
                jo.put("totalTime", new Date().getTime() - startTime.getTime());
            } catch (JSONException e) {
            }
            return jo.toString();
        }
        
        public void incrScannedNum(int n) {
            totalScannedNum.addAndGet(n);
        }
        
        public void incrDeletedNum(int n) {
            totalDeletedNum.addAndGet(n);
        }

        public void incrDeletedVersionNum(int n){
            totalDeletedVersionNum.addAndGet(n);
        }

        public void incrDeletedSize(long size) {
            totalDeletedSize.addAndGet(size);
        }
        
        public long getDeletedSize() {
            return totalDeletedSize.get();
        }

        public void incrTransitionedNum(int n) {
            totalTransitionedNum.addAndGet(n);
        }
        public void incrTransitionedSize(long n) {
            totalTransitionedSize.addAndGet(n);
        }
        public long getTransitionedSize() {
            return totalTransitionedSize.get();
        }
    }

    //解析哪些bucket需要进行生命周期处理，并提交任务。
    class FindExpiredObjects extends Thread {
        
        public void run() {
            boolean scanningProgress = false;
            byte[] bucketMarker = null;
            String progressNextBucket = null;
            while (true) {
                try {
                    //1、优先扫描bucket表，获取未被扫描过的bucket。
                    if(!scanningProgress) {
                        String metaRegion = MetaRegion.getRegion().getName();
                        //因为bucket中object的metaRegion可能与bucket的metaRegion不一致，所以要扫描所有的bucket
                        List<BucketMeta> buckets = globlaMetaClient.bucketListAll(null, bucketMarker, 3);
                        if(buckets != null && !buckets.isEmpty()) {
                            for (BucketMeta bucket : buckets) {
                                //判断是否设置了生命周期。
                                if(null == bucket.lifecycle || null == bucket.lifecycle.getRules() || bucket.lifecycle.getRules().isEmpty())
                                    continue;
                                List<Rule> rules = bucket.lifecycle.getRules();
                                TreeMap<String, Rule> shortedRules = shortRules(rules);
                                boolean hasPrefix = shortedRules != null;
                                if (hasPrefix) {
                                    if (shortedRules.isEmpty())
                                        continue;
                                    rules = Lists.newArrayList(shortedRules.values());
                                }
                                LifecycleProgress progress = new LifecycleProgress(bucket.getName());
                                boolean exists = localMetaClient.progressSelect(progress);
                                //如果bucket正在被处理，则查看当前处理者是否是自己，如果不是则验证处理是否超时
                                if(exists && progress.processor != null && !progress.processor.equals(hostName) && progress.timestamp > System.currentTimeMillis()
                                        - LifecycleConfig.progressExpireTime) {
                                    log.info("Bucket is in progress, skip. progressInfo=" + Bytes.toString(progress.write()));
                                    continue;
                                }
                                if(!exists) {
                                    //获取最新处理完成的记录，检查最新处理完成的时间是否超过一定时间，避免频繁处理同一个bucket
                                    List<LifecycleProgress> progressList;
                                    try {
                                        progressList = localMetaClient.progressList(LifecycleProgress.HANDLED, progress.bucketName, 1, true);
                                    } catch (Exception e) {
                                        log.error("processList failed, LifecycleProgress status is: " + LifecycleProgress.HANDLED + ", bucket is: " + progress.bucketName);
                                        throw e;
                                    }
                                    if(progressList != null && !progressList.isEmpty()) {
                                        LifecycleProgress lp = progressList.get(0);
                                        if (lp.bucketName != null && lp.bucketName.equals(progress.bucketName)
                                                && System.currentTimeMillis() - lp.timestamp < LifecycleConfig.scanIntervalTime) {
                                            log.info("Bucket has been progressed recently, skip. progress=" + Bytes.toString(lp.write()));
                                            continue;
                                        }
                                    }
                                    progress = null;
                                }
                                LifecycleProgress newProgress = new LifecycleProgress(progress);
                                newProgress.bucketName = bucket.name;
                                newProgress.processor = hostName;
                                newProgress.timestamp = System.currentTimeMillis();
                                newProgress.metaRegion = metaRegion;
                                // 原子更改处理者信息，如果修改成功，则开始处理。
                                boolean checkAndPut = localMetaClient.progressCheckAndPut(progress, newProgress);
                                if (!checkAndPut) {
                                    log.info("CheckAndPut failed, skip. progressInfo=" + Bytes.toString(progress.write()));
                                    continue;
                                }

                                log.info("Start handle bucket. progressInfo=" + Bytes.toString(newProgress.write()));
                                FindTaskObjectInBucket foi = new FindTaskObjectInBucket(bucket, newProgress, rules, shortedRules);
                                foi.find();
                                log.info("End handle bucket. progressInfo=" + Bytes.toString(newProgress.write()));
                            }
                            BucketMeta lastBucket = buckets.get(buckets.size() - 1);
                            bucketMarker = buildNewMarker(Bytes.toBytes(String.valueOf(lastBucket.getId())));
                        } else {
                            scanningProgress = true;
                            bucketMarker = null;
                        }
                    } else {
                        //2、bucket扫描完之后再扫描正在被处理的bucket，顺序处理每个progress
                        bucketMarker = null;
                        List<LifecycleProgress> progressList;
                        try {
                            progressList = localMetaClient.progressList(LifecycleProgress.HANDLING, progressNextBucket, 10, false);
                        } catch (Exception e) {
                            log.error("processList failed, LifecycleProgress status is: " + LifecycleProgress.HANDLING + ", bucket is: " + progressNextBucket);
                            throw e;
                        }
                        if(progressList != null && !progressList.isEmpty()) {
                            for (LifecycleProgress progress : progressList) {
                                BucketMeta bucket = new BucketMeta(progress.bucketName);
                                globlaMetaClient.bucketSelect(bucket);
                                if(bucket.lifecycle == null || bucket.lifecycle.getRules().isEmpty())
                                    continue;
                                List<Rule> rules = bucket.lifecycle.getRules();
                                TreeMap<String, Rule> shortedRules = shortRules(rules);
                                boolean hasPrefix = shortedRules != null;
                                if (hasPrefix) {
                                    if (shortedRules.isEmpty())
                                        continue;
                                    rules = Lists.newArrayList(shortedRules.values());
                                }
                                log.info("Start handle bucket. progressInfo=" + Bytes.toString(progress.write()));
                                FindTaskObjectInBucket foi = new FindTaskObjectInBucket(bucket, progress, rules, shortedRules);
                                foi.find();
                                log.info("End handle bucket. progressInfo=" + Bytes.toString(progress.write()));
                            }
                            
                            progressNextBucket = progressList.get(progressList.size() - 1).bucketName + Character.MIN_VALUE;
                        } else {
                            progressNextBucket = null;
                            scanningProgress = false;
                        }
                    }
                    Thread.sleep(10 * 1000);
                } catch (Throwable e) {
                    log.error(e.getMessage(), e);
                }
            }
        }
        
        byte[] buildNewMarker(byte[] marker) {
            byte[] newMarker = new byte[marker.length + 1];
            System.arraycopy(marker, 0, newMarker, 0, marker.length);
            newMarker[newMarker.length - 1] = Byte.MIN_VALUE;
            return newMarker;
        }
        
        /**
                    * 对规则按照prefix进行排序，如果某个规则的prefix为null，则返回null
         * @param rules
         * @return
         */
        TreeMap<String, Rule> shortRules(List<Rule> rules) {
            TreeMap<String, Rule> treeMap = new TreeMap<>();
            for(Rule r : rules) {
                if(!r.getStatus().equals(BucketLifecycleConfiguration.ENABLED))
                    continue;
                String prefix = r.getPrefix();
                if (prefix == null || "".equals(prefix))
                    return null;
                else
                    treeMap.put(prefix, r);
            }
            return treeMap;
        }

    }

    
    /**
     * 处理具体bucket
     *
     */
    class FindTaskObjectInBucket {
        private BucketMeta bucket;
        private LifecycleProgress progress;
        private List<Rule> rules;
        private Stats stats = null;
        TreeMap<String, Rule> shortedRules;

        public FindTaskObjectInBucket(BucketMeta bucket, LifecycleProgress progress,List<Rule> rules,TreeMap<String, Rule> shortedRules) {
            this.bucket = bucket;
            this.progress = progress;
            this.rules = rules;
            this.shortedRules=shortedRules;
        }

        public void find() {
            boolean hasPrefix = shortedRules != null;
            Rule lastRule = null;
            if (hasPrefix) {
                lastRule = shortedRules.lastEntry().getValue();
            }
            Stats lastAddedStats = null;

            // 解析当前bucket的合规保留规则时长 pair<unit, duration>
            Pair<ChronoUnit, Integer> lockTimePair = new Pair<>(ChronoUnit.DAYS, 0);
            ObjectLockConfiguration objectLockConfiguration = bucket.objectLockConfiguration;
            if (Objects.nonNull(objectLockConfiguration)) {
                lockTimePair = objectLockConfiguration.getRuleTimePair();
            }
            int continuousFailure = 0;
            while(true) {
                try {
                    LifecycleConfig.checkNeedPaused();//暂停
                    OwnerMeta owner = new OwnerMeta(bucket.getOwnerId());
                    if(!globlaMetaClient.ownerSelectById(owner)) {
                        log.warn(">>bucket:"+ bucket.name + " owner:" + owner.getId() +" is not exist. task break;");
                        return;
                    }

                    //1、获取bucket的进度，并更新时间戳。
                    boolean exists = localMetaClient.progressSelect(progress);
                    if(!exists)
                        return;
                    if(progress.isScanning == 1 && progress.processor != null && !progress.processor.equals(hostName)
                            && progress.timestamp > System.currentTimeMillis() - LifecycleConfig.progressExpireTime) {
                        Thread.sleep(1000);
                        continue;
                    }
                    LifecycleProgress newProgress = new LifecycleProgress(progress);
                    newProgress.processor = hostName;
                    newProgress.timestamp = System.currentTimeMillis();
                    newProgress.isScanning = 1;
                    //将进度时间戳和扫描状态写入到数据库。 如果写入失败，证明有并发操作。跳出循环重新开始。
                    boolean checkAndPut = localMetaClient.progressCheckAndPut(progress, newProgress);
                    if(!checkAndPut)
                        continue;
                    log.info("Start scan bucket=" + progress.bucketName + "");
                    if (stats == null) {
                        stats = new Stats();
                        stats.init();
                    }
                    Pair<Rule, List<ObjectMeta>> listResult = listObjects(hasPrefix, shortedRules, lastRule, progress);
                    Rule rule = listResult.first();
                    List<ObjectMeta> objects = listResult.second();
                    if(objects == null || objects.isEmpty()) {
                        //扫描完成，将progress的状态从HANDLING改成HANDLED
                        newProgress.timestamp = System.currentTimeMillis();
                        newProgress.isScanning = 0;
                        Stats newStats = new Stats();
                        newStats.init(newProgress.stats);
                        updateStats(newStats, lastAddedStats, stats);
                        newProgress.stats = newStats.getString();
                        newProgress.status = LifecycleProgress.HANDLED;
                        newProgress.version = Misc.formatyyyymmddhhmmss(new Date());
                        localMetaClient.progressPutAndDelete(newProgress, progress);
                        log.info("End handle bucket change status, progressInfo=" + Bytes.toString(newProgress.write()));
                        break;
                    }
                    //更新统计信息
                    stats.incrScannedNum(objects.size());
                    newProgress.timestamp = System.currentTimeMillis();
                    newProgress.isScanning = 0;
                    newProgress.objectMarker = objects.get(objects.size() - 1).name + Character.MIN_VALUE;
                    Stats newStats = new Stats();
                    newStats.init(newProgress.stats);
                    updateStats(newStats, lastAddedStats, stats);
                    newProgress.stats = newStats.getString();
                    localMetaClient.progressInsert(newProgress);

                    log.info("STATS::" + stats.getString());

                    if (lastAddedStats == null)
                        lastAddedStats = new Stats();
                    lastAddedStats.init(stats.getString());
                    //解析生命周期规则
                    for(ObjectMeta obj : objects) {
                        if(!hasPrefix)
                            rule = rules.get(0);
                        int i = 1;
                        //循环遍历所有规则，直到对象删除
                        List<ObjectMeta> objectMetaList;
                        try {
                            //如果获取对象所有版本失败，跳过该对象
                            objectMetaList = globlaMetaClient.objectGetAllVersions(obj);
                        } catch (IOException e) {
                            log.error("hbase query object version failed. object key: " + obj.name, e);
                            continue;
                        }
                        boolean objDeleted = false;
                        for (ObjectMeta objectMeta : objectMetaList) {
                            do {
                                boolean isDeleted = delExpiredObjectVersion(stats, owner, objectMeta, rule, lockTimePair);
                                objDeleted = objDeleted || isDeleted;
                                if (isDeleted) {
                                    break;
                                }
                                if (!hasPrefix && i < rules.size())
                                    rule = rules.get(i++);
                                else
                                    break;
                            } while (true);
                        }
                        // 如果某对象存在某个版本被删除，则删除对象数+1
                        if (objDeleted) {
                            stats.incrDeletedNum(1);
                        }
                    }
                    while(delObjPool.getQueue().size() > (LifecycleConfig.deleteObjThreads * 3)) {
                        Thread.sleep(100);
                    }
                    continuousFailure = 0;
                } catch (IOException ie){
                    continuousFailure++;
                    log.error("hbase query failed. bucket name: " + bucket.name, ie);
                } catch (Throwable e) {
                    log.error(e.getMessage(), e);
                }
                // 连续失败次数达到阈值，中止该bucket任务，等下一轮扫描继续任务
                if (continuousFailure >= LifecycleConfig.skipThreshold) {
                    break;
                }
            }

        }

        /**
         * 过滤对象是需要执行删除还是转储。 被规则命中后将返回true。
         * 如果两种规则同时命中，删除优先级最高。
         */
        private boolean filterTaskObject(Rule rule, ObjectMeta object, OwnerMeta owner, Pair<ChronoUnit, Integer> lockDurationPair) {
            // 对象删除前校验是否已经过了合规保留的期限 再进行删除
            boolean objectCanDelete = ObjectLockUtils.objectLockCheck(lockDurationPair, object);

            // 会有只配置了过期或者只配置了转储的情况
            if (rule.getExpirationDate() == null && rule
                    .getExpirationInDays() != BucketLifecycleConfiguration.INITIAL_DAYS) {
                // 对象只存活指定的时间
                if (Misc.timeToNextMidnight(DateUtils.addDays(
                        new Date(object.lastModified), rule.getExpirationInDays()))
                        .before(new Date()) && objectCanDelete) {
                    delObjPool.submit(
                            new DeleteObjects(stats, owner, bucket, object, rule));
                    return true;
                }
            } else if (rule.getExpirationDate() != null) {
                // 指定时间之前的对象都过期。
                if (Misc.timeToNextMidnight(rule.getExpirationDate())
                        .after(new Date(object.lastModified)) && objectCanDelete) {
                    delObjPool.submit(
                            new DeleteObjects(stats, owner, bucket, object, rule));
                    return true;
                }
            }
            if (null == rule.getTransition())
                return false;
            //只能是普通转低频
            if (object.storageClassString.toLowerCase().equals(rule.getTransition().getStorageClass().toLowerCase()))
                return false;
            Transition transition = rule.getTransition();
            if (transition.getDate() == null && transition
                    .getDays() != BucketLifecycleConfiguration.INITIAL_DAYS) {
                // 对象只转储指定的时间以前创建的
                if (Misc.timeToNextMidnight(DateUtils
                        .addDays(new Date(object.lastModified), transition.getDays()))
                        .before(new Date())) {
                    tranObjPool.submit(new TransitionObject(stats, owner,
                            bucket, object, rule));
                    return true;
                }
            } else if (transition.getDate() != null) {
                // 指定时间之前的对象都转储
                if (Misc.timeToNextMidnight(rule.getExpirationDate())
                        .after(new Date(object.lastModified))) {
                    tranObjPool.submit(new TransitionObject(stats, owner,
                            bucket, object, rule));
                    return true;
                }
            }
            return false;
        }

        /**
         * 遍历一个对象的所有版本，删除已到期的版本
         * @param stats
         * @param owner
         * @param object
         * @param rule
         * @return
         * @throws IOException
         */
        private boolean delExpiredObjectVersion(Stats stats, OwnerMeta owner, ObjectMeta object, Rule rule,Pair<ChronoUnit, Integer> lockDurationPair) {
            // 对象删除前校验是否已经过了合规保留的期限 再进行删除
            boolean objectCanDelete = ObjectLockUtils.objectLockCheck(lockDurationPair, object);

            // 会有只配置了过期或者只配置了转储的情况
            if (rule.getExpirationDate() == null && rule
                    .getExpirationInDays() != BucketLifecycleConfiguration.INITIAL_DAYS) {
                // 对象只存活指定的时间
                if (Misc.timeToNextMidnight(DateUtils.addDays(
                        new Date(object.lastModified), rule.getExpirationInDays()))
                        .before(new Date()) && objectCanDelete) {
                    delObjPool.submit(
                            new DeleteObjects(stats, owner, bucket, object, rule));
                    return true;
                }
            } else if (rule.getExpirationDate() != null) {
                // 指定时间之前的对象都过期。
                if (Misc.timeToNextMidnight(rule.getExpirationDate())
                        .after(new Date(object.lastModified)) && objectCanDelete) {
                    delObjPool.submit(
                            new DeleteObjects(stats, owner, bucket, object, rule));
                    return true;
                }
            }
            if (null == rule.getTransition())
                return false;
            //只能是普通转低频
            if (object.storageClassString.toLowerCase().equals(rule.getTransition().getStorageClass().toLowerCase()))
                return false;
            Transition transition = rule.getTransition();
            if (transition.getDate() == null && transition
                    .getDays() != BucketLifecycleConfiguration.INITIAL_DAYS) {
                // 对象只转储指定的时间以前创建的
                if (Misc.timeToNextMidnight(DateUtils
                        .addDays(new Date(object.lastModified), transition.getDays()))
                        .before(new Date())) {
                    tranObjPool.submit(new TransitionObject(stats, owner,
                            bucket, object, rule));
                }
            } else if (transition.getDate() != null) {
                // 指定时间之前的对象都转储
                if (Misc.timeToNextMidnight(transition.getDate())
                        .after(new Date(object.lastModified))) {
                    tranObjPool.submit(new TransitionObject(stats, owner,
                            bucket, object, rule));
                }
            }
            return false;
        }

        void updateStats(Stats newStats, Stats lastAddedStats, Stats stats) {
            if(lastAddedStats != null) {
                newStats.totalScannedNum.addAndGet(stats.totalScannedNum.get() - lastAddedStats.totalScannedNum.get());
                newStats.totalDeletedNum.addAndGet(stats.totalDeletedNum.get() - lastAddedStats.totalDeletedNum.get());
                newStats.totalDeletedVersionNum.addAndGet(stats.totalDeletedVersionNum.get() - lastAddedStats.totalDeletedVersionNum.get());
                newStats.totalDeletedSize.addAndGet(stats.totalDeletedSize.get() - lastAddedStats.totalDeletedSize.get());
                newStats.totalTransitionedNum.addAndGet(stats.totalTransitionedNum.get()
                        - lastAddedStats.totalTransitionedNum.get());
                newStats.totalTransitionedSize.addAndGet(stats.totalTransitionedSize.get()
                        - lastAddedStats.totalTransitionedSize.get());
            } else
                newStats.totalScannedNum.addAndGet(stats.totalScannedNum.get());
        }
        
        Pair<Rule, List<ObjectMeta>> listObjects(boolean hasPrefix,
                TreeMap<String, Rule> rules, Rule lastRule, LifecycleProgress progress) throws IOException {
            String startMarker = null;
            String endMarker = null;
            TreeMap<String, Rule> shortedRules = null;
            if(rules != null && rules.size() != 0) {
                shortedRules = new TreeMap<String,Rule>();
                shortedRules.putAll(rules);
            }
            Rule currentRule = lastRule;
            //顺序处理每个规则，定义每次list的起始key、截止key
            if(!hasPrefix) {
                startMarker = progress.objectMarker;
            } else {
                do{
                    Entry<String, Rule> entry = shortedRules.pollFirstEntry();
                    if(entry != null) {
                        if(progress.objectMarker == null) {
                            startMarker = entry.getValue().getPrefix();
                            endMarker = startMarker + Character.MAX_VALUE;
                        } else if(progress.objectMarker.startsWith(entry.getValue().getPrefix())) {
                            startMarker = progress.objectMarker;
                            endMarker = entry.getValue().getPrefix() + Character.MAX_VALUE;
                        } else
                            continue;
                        currentRule = entry.getValue();
                    } else {
                        startMarker = progress.objectMarker;
                        if(startMarker == null || !startMarker.startsWith(lastRule.getPrefix()))
                            return null;
                        else
                            endMarker = lastRule.getPrefix() + Character.MAX_VALUE;
                    }
                    if(endMarker.compareTo(startMarker) <= 0) {
                        progress.objectMarker = null;
                        continue;
                    }
                    break;
                } while(true);
            }
            
            List<ObjectMeta> objects;
            try {
                objects = globlaMetaClient.objectListRangeKeys(progress.metaRegion,
                        progress.bucketName, startMarker, endMarker, 2000);
            } catch (Exception e) {
                log.error("objectListRangeKey failed, condition is: " + progress.metaRegion + ", " + progress.bucketName + ", " + startMarker + ", " + endMarker);
                throw e;
            }
            //递归调用，看下一个规则里是否能匹配到对象
            if((objects == null || objects.isEmpty()) && shortedRules!=null && !shortedRules.isEmpty()) {
                progress.objectMarker = null;
                return listObjects(hasPrefix, shortedRules, lastRule, progress);
            }
            return new Pair<>(currentRule, objects);
        }
    }

    static class DeleteObjects extends Thread {
        private Stats stats;
        private OwnerMeta owner;
        private BucketMeta bucket;
        private ObjectMeta object;
        private Rule rule;
        
        //ThreadLocal
        private static ThreadLocal<Triple<String, Long, Long>> lastLimit = new ThreadLocal<Triple<String, Long, Long>>() {
            protected Triple<String, Long, Long> initialValue() {
                Triple<String, Long, Long> p = new Triple<>(null, -1l, -1l);
                return p;
            }
        };
        
        public DeleteObjects(Stats stats, OwnerMeta owner, BucketMeta bucket, ObjectMeta obj, Rule rule) {
            this.stats = stats;
            this.owner = owner;
            this.bucket = bucket;
            this.object = obj;
            this.rule = rule;
        }
        
        public void run() {
            LifecycleConfig.checkNeedPaused();
            Triple<String, Long, Long> triple = lastLimit.get();
            long lastLimitTime = triple.second();
            long lastSize = triple.third();
            try {
                if(lastLimitTime == -1) {
                    lastLimitTime = System.currentTimeMillis();
                    lastSize = stats.getDeletedSize();
                    triple.first(bucket.name);
                }
                delete(owner, bucket, object);
                limitTaskSpeed(triple, lastLimitTime, lastSize, stats.getDeletedSize(), TYPE_DELETE);
            } catch (Throwable t) {
                log.error(t.getMessage(), t);
            }
        }

        public void delete(OwnerMeta owner, BucketMeta bucket, ObjectMeta object) {
            BucketLog bucketLog = null;
            PinData pinData = new PinData();
            try {
                //1.获取bucket信息、object信息、owner信息。
                bucketLog = getBucketLog(bucket,object,owner, "OOS.EXPIRE.OBJECT");
                if(globlaMetaClient.isBusy(object, OOSConfig.getMaxConcurrencyPerRegionServer()))
                    Backoff.backoff();
                if(!globlaMetaClient.objectSelect(object))
                    return;
                //2、统计
                Map<InitialUploadMeta, List<UploadMeta>> map = new LinkedHashMap<InitialUploadMeta, List<UploadMeta>>();
                Triple<Long, Long, Long> triple = OpObject.deleteObjectMeta(bucket, object, map, bucketLog, true);
                pinData.setSize(triple.first(), triple.second(), triple.third(),
                        object.dataRegion, object.originalDataRegion,object.storageClassString, object.getLastCostTime());
                stats.incrDeletedVersionNum(1);
                stats.incrDeletedSize(triple.first());
                DELETE_OBJECT_NUMBER_COUNT.increment();

                bucketLog.objectSize = String.valueOf(triple.first());
                log.info("delete object " + object.name + " " + ", in bucket: "
                        + bucket.name + " success, request id:"
                        + bucketLog.requestId + ", ruleId:" + rule.getId()
                        + ", prefix:" + rule.getPrefix() + ", days:"
                        + rule.getExpirationInDays() + ",date:"
                        + rule.getExpirationDate() + ", status: "
                        + rule.getStatus());
                bucketLog.endTime = System.currentTimeMillis();
                bucketLog.status = 204;
                //3、执行删除操作。
                try {
                    OpObject.deleteObject(bucket, object, bucketLog, true, false, map);
                } catch (Exception e) {
                    //只要deleteObjectMeta成功了就认为删除成功了，但是存在数据没有删除成功的情况。
                    log.warn("DeleteObjectMeta success, but deleteObjectData failed. ownerId : " + bucket.getOwnerId() + " MetaRegionName: " + object.metaRegionName
                            + ", bucketName: " + object.bucketName + ", objectName: " + object.name, e);
                }
            } catch (Exception e) {
                log.error(e.getMessage(), e);
                if (bucketLog != null) {
                    bucketLog.status = 500;
                }
            } finally {
                Utils.pinDecreStorage(bucket.getOwnerId(), PIN, pinData, bucket.name);
                if (bucketLog != null && bucketLog.targetBucketName != null) {
                    bucketLog.writeLifecycleLog();
                }
                if (bucketLog != null && bucketLog.bucketOwnerName != null) {
                    bucketLog.writeGlobalLog(null, null);
                }
            }
        }
    }

    /**
     * 1 转储由于有拷贝文件的操作，可能会很慢。 是否还需要限速。
     * 2
     * */
    static class TransitionObject  extends Thread {
        private Pin pin = new Pin();

        private Stats stats;
        private OwnerMeta owner;
        private BucketMeta bucket;
        private ObjectMeta sourceObject;
        private Rule rule;

        //每个线程独立限速，使用static 和ThreadLocal可以保证限速不被类初始化干扰。
        private static ThreadLocal<Triple<String, Long, Long>> lastLimit = new ThreadLocal<Triple<String, Long, Long>>() {
            protected Triple<String, Long, Long> initialValue() {
                Triple<String, Long, Long> p = new Triple<>(null, -1l, -1l);
                return p;
            }
        };

        public TransitionObject(Stats stats, OwnerMeta owner,
                BucketMeta bucket, ObjectMeta object, Rule rule) {
            super();
            this.stats = stats;
            this.owner = owner;
            this.bucket = bucket;
            this.sourceObject = object;
            this.rule = rule;
        }

        public void run() {
            LifecycleConfig.checkNeedPaused();
            Triple<String, Long, Long> triple = lastLimit.get();
            long lastLimitTime = triple.second();
            long lastSize = triple.third();
            try {
                if(lastLimitTime == -1) {
                    lastLimitTime = System.currentTimeMillis();
                    lastSize = this.stats.getTransitionedSize();
                    triple.first(this.bucket.name);
                }
                transition();
                limitTaskSpeed(triple, lastLimitTime, lastSize, this.stats.getTransitionedSize(),TYPE_TRANSITION);
            } catch (Throwable t) {
                log.error(t.getMessage(), t);
            }
        }

        /**
         * 修改 容量统计
         * 修改 源对象存储类型
         * lifecycle 记录删除量和减低频容量
         * */
        private void transition() {
            PinData pinData = new PinData();
            BucketLog bucketLog =null;
            ObjectMeta object = new ObjectMeta(this.sourceObject.name,
                    this.sourceObject.bucketName, this.sourceObject.metaRegionName);
            try {
                bucketLog = getBucketLog(this.bucket, object, this.owner, "OOS.TRANSITION_SIA.OBJECT");
                if(globlaMetaClient.isBusy(sourceObject, OOSConfig.getMaxConcurrencyPerRegionServer())) {
                    Backoff.backoff();
                }
                if(!globlaMetaClient.objectSelect(object)) {
                    return;
                }
                Triple<Long, Long, Long> objectSize = OpObject.getObjectSize(object);
                bucketLog.objectSize = String.valueOf(objectSize.first());
                stats.incrTransitionedNum(1);
                stats.incrTransitionedSize(objectSize.first());
                //用于统计容量，减少原存储类型的容量
                pinData.sourceObject = pinData.new SourceObject(objectSize.first(), objectSize.third(), objectSize.second(), 
                        object.getLastCostTime(), object.storageClassString, object.dataRegion, object.originalDataRegion);
                //开始修改meta信息。
                object.storageClassString = this.rule.getTransition().getStorageClass();
                object.transitionTime = Utils.getTimeStamp();
                //增加目标存储类型容量
                pinData.setSize(objectSize.first(), objectSize.second(), objectSize.third(),
                        object.dataRegion, object.originalDataRegion,
                        this.rule.getTransition().getStorageClass(), object.getLastCostTime());
                bucketLog.putMetaTime = System.currentTimeMillis();
                try {
                    if (globlaMetaClient.isBusy(object, OOSConfig.getMaxConcurrencyPerRegionServer()))
                        Backoff.backoff();
                    globlaMetaClient.objectInsert(object);
                    bucketLog.status = 200;
                    //实际上只有真正put object成功之后才统计容量
                    Utils.pinStorage(bucket.getOwnerId(), pin, pinData, bucket.name);
                } finally {
                    bucketLog.putMetaTime = System.currentTimeMillis() - bucketLog.putMetaTime;
                }

                log.info("transition object " + object.name + " " + ", in bucket: "
                        + bucket.name + " success, request id:" + bucketLog.requestId + ", ruleId:" + rule.getId()
                        + ", prefix:" + rule.getPrefix() + ", days:" + rule.getExpirationInDays() + ",date:"
                        + rule.getExpirationDate() + ", status: " + rule.getStatus());
                bucketLog.endTime = System.currentTimeMillis();
            } catch (Exception e) {
                String errormsg = "transition error " + object.name + " , in bucket: "
                        + this.bucket.name + ", ruleId:" + rule.getId()
                        + ", prefix:" + rule.getPrefix() + ", days:" + rule.getExpirationInDays() + ",date:"
                        + rule.getExpirationDate() + ", status: " + rule.getStatus();
                if (bucketLog != null) {
                    bucketLog.status = 500;
                }
                log.error(errormsg, e);
            } finally {
                if (bucketLog != null) {
                    if (bucketLog.targetBucketName != null) {
                        bucketLog.writeLifecycleLog();
                    }
                    if (bucketLog.bucketOwnerName != null) {
                        bucketLog.writeGlobalLog(null, null);
                    }
                }
            }
        }
    }

    private static BucketLog getBucketLog(BucketMeta bucket, ObjectMeta object, OwnerMeta owner,String operation)
            throws IOException {
        BucketLog bucketLog = new BucketLog(System.currentTimeMillis());
        bucketLog.prepareLogTime = System.currentTimeMillis();
        bucketLog.requesterName = "OOS";
        bucketLog.operation = operation ;
        bucketLog.requestId = Long.toHexString(UUID.randomUUID().getMostSignificantBits());
        bucketLog.bucketName = bucket.name;
        bucketLog.bucketOwnerName = owner.name;
        bucketLog.ownerId = owner.getId();
        bucketLog.objectName = object.name;
        bucketLog.metaRegion = object.metaRegionName;
        bucketLog.dataRegion = object.dataRegion;
        bucketLog.etag = object.etag;
        if (object.ostorId != null && object.ostorId.length() != 0)
            bucketLog.ostorId = object.ostorId;
        if (bucket.logTargetBucket != 0) {
            BucketMeta targetBucket = new BucketMeta(bucket.logTargetBucket);
            //如果目标bucket 不存在了。 将bucket logging去掉。
            if (!globlaMetaClient.bucketSelect(targetBucket)) {
                bucket.logTargetBucket = 0;
                bucket.logTargetPrefix = "";
                globlaMetaClient.bucketUpdate(bucket);
            }
            bucketLog.targetBucketName = targetBucket.name;
        }
        if (bucket.logTargetPrefix != null && bucket.logTargetPrefix.trim().length() != 0)
            bucketLog.targetPrefix = bucket.logTargetPrefix;
        bucketLog.prepareLogTime = System.currentTimeMillis() - bucketLog.prepareLogTime;
        return bucketLog;
    }


    /**
    *转储的速度限制是否单独配置？
     * */
    private static void limitTaskSpeed(Triple<String, Long, Long> triple,
            long lastLimitTime, long lastSize, long taskObjSize, int type) throws Exception{
        //限速
        long now = System.currentTimeMillis();
        //两次限速的间隔时间
        long intervalTime = (now - lastLimitTime) / 1000;
        if (intervalTime > (LifecycleConfig.intervalTime / 1000)) {
            int currentSpeed = (int)((taskObjSize - lastSize) / MB / intervalTime);
            List<SpeedLimit> limits = LifecycleConfig.limits;
            //防止在没配置具体限速时，不执行全局限速问题。
            boolean needExecutedMaxSpeed = true;
            if(limits != null && !limits.isEmpty()) {
                for(SpeedLimit limit : limits) {
                    if (type == TYPE_TRANSITION
                            && !(limit instanceof TransitionSpeedLimit)) {
                        continue;
                    } else if (type == TYPE_DELETE
                            && !(limit instanceof DeleteSpeedLimit)) {
                        continue;
                    }
                    needExecutedMaxSpeed = false;
                    boolean isLimited = limit.limit(currentSpeed, intervalTime);
                    if (isLimited)
                        break;
                }
            }
            if(needExecutedMaxSpeed) {
                int maxSpeed = type == TYPE_TRANSITION
                        ?  LifecycleConfig.maxTransitionSpeed : LifecycleConfig.maxDeleteSpeed;
                int more = currentSpeed - maxSpeed;
                if(more > 0) {
                    long toSleep = more * 1000 * intervalTime / maxSpeed;
                    if(toSleep > 0)
                        try {
                            Thread.sleep(toSleep);
                        } catch (InterruptedException e) {
                        }
                }
            }
            triple.second(now);
            triple.third(taskObjSize);
        }
    }
}
