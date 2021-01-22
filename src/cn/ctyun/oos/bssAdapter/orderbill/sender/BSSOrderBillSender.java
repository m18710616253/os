package cn.ctyun.oos.bssAdapter.orderbill.sender;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.SQLException;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.json.JSONObject;

import com.amazonaws.auth.SigningAlgorithm;
import com.amazonaws.services.s3.internal.RestUtils;

import cn.ctyun.common.Consts;
import cn.ctyun.common.Program;
import cn.ctyun.common.conf.GlobalHHZConfig;
import cn.ctyun.common.conf.OOSConfig;
import cn.ctyun.common.dsync.DLock;
import cn.ctyun.common.dsync.DSyncService;
import cn.ctyun.common.node.OosZKNode;
import cn.ctyun.common.region.DataRegions;
import cn.ctyun.common.utils.HostUtils;
import cn.ctyun.oos.common.Utils;
import cn.ctyun.oos.metadata.MinutesUsageMeta.UsageMetaType;
import cn.ctyun.oos.server.conf.BssAdapterConfig;
import cn.ctyun.oos.server.conf.BssConfig;
import cn.ctyun.oos.server.conf.SynchroConfig;
import cn.ctyun.oos.server.db.DB;
import common.time.TimeUtils;

/**
 * 
 * 每天凌晨开始进行对接，进行OOS和BSS之间的话单对接处理； 
 * 第一步：先从MySQL数据库Order表和initialAccount表中取出电商用户的话单信息放入内存队列中；
 * 第二部：将内存队列中的话单内容实例化成实际的TXT（csv）文件； 第三部：文件实例化完成以后回调函数通知控制台进行上传操作；
 * 
 * 关于启动参数： 
 * 1）正常情况下，服务启动不需要参数，程序自动获取前一天数据进行对接；
 * 2）需要补传数据的情况下，指定一个小于前一天日期的参数，表示将补传从指定日期开始到前一天的数据，
 *    服务器启动第一次执行的时候参数有效，首次执行完毕之后 参数失效（补传数据只对接一次）；
 * 
 * 关于配置文件： 
 * 2）startTime每天开始对接的时间
 * 
 * ===============================================
 * modified by wangwei at 2019.3.11
 * 将取数逻辑从mysql，变为从HBase中取数据 ,其他逻辑不变
 * 
 */
public class BSSOrderBillSender implements Program {
    static {
        //保证变量在log初始化之前赋值
        System.setProperty("log4j.log.app", "BSSOrderBillSender");
    }
    private static final Log log = LogFactory.getLog(BSSOrderBillSender.class);
    final ExecutorService readerExecutor = Executors.newFixedThreadPool(10);

    private List<Future<List<OrderMessage>>> readerFutures = new ArrayList<Future<List<OrderMessage>>>();
    private List<String> orderBillRegions = new ArrayList<>();
    
    private static String beginDate = null;
    private static ScheduledThreadPoolExecutor scheduleThreadPool = new ScheduledThreadPoolExecutor(1);
    private static DateTimeFormatter format_time = DateTimeFormatter.ofPattern("HHmm");
    private static DateTimeFormatter format_day = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    public static final int PAGE_COUNT = 10000;

    static Properties mappingProperty = new Properties();
    static {
        InputStream is;
        try {
            is = new FileInputStream(System.getenv("OOS_HOME") + "/conf/global/bssRegionOosMap.conf");
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
        try {
            mappingProperty.load(is);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                is.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public BSSOrderBillSender() {
    }

    private static String getStartDate() {
        if ("".equals(beginDate)) {
            return LocalDate.now().plusDays(-1).format(format_day);
        }

        return beginDate;
    }

    private static String getEndDate() {
        return LocalDate.now().plusDays(-1).format(format_day);
    }

    /*
     * 根据数据库话单记录数量计算开启线程数
     */
    private int getThreadCount(int userCount) throws SQLException {
        int count = userCount * orderBillRegions.size();
        int pageCount = ((count % PAGE_COUNT)==0)?count/PAGE_COUNT:count/PAGE_COUNT+1;
        
        log.info("get " + pageCount + " pages from DB.");
        
        return pageCount;
    }

    /*
     * 分批次多线程从HBase数据库中读取话单信息放入内存；
     */
    private void loadOrderBillData(int threadNum, HashMap<String,String> bssUsers) throws Exception {
        readerFutures.clear();
        //AccountId，key
        List<String> rowKeyList = createRowKeys(bssUsers,getStartDate(),getEndDate());
        
        //从HBase读取数据
        for (int i = 0; i < threadNum; i++) {
            LoadTask job = new LoadTask(i, bssUsers, new ArrayList<>(rowKeyList.subList(i * PAGE_COUNT,
                    i == (threadNum-1)?rowKeyList.size():(i+1) * PAGE_COUNT)), mappingProperty);
            Future<List<OrderMessage>> feture = readerExecutor.submit(job);
            readerFutures.add(feture);
        }
    }

    /**
     * 按如下格式拼接Hbase中的rowKey
    regionName + "|"  + DAY_OWNER+ "|" + usrId + "|" + time;
    regionName + "|"  + DAY_OWNER+ "|" + usrId + "|" + time;
     */
    private List<String> createRowKeys(HashMap<String,String> ownersList,String beginDate
            ,String endDate) throws Exception {
        if (orderBillRegions.size() == 0) {
            throw new Exception("Invalid DataRegion size[0]");
        }
        ArrayList<String> resultRowKeys = new ArrayList<>();
        
        StringBuilder sb = new StringBuilder("");
        StringBuilder sb2 = new StringBuilder("");
        Date begin = DateUtils.parseDate(beginDate, "yyyy-MM-dd");
        Date end = DateUtils.parseDate(endDate, "yyyy-MM-dd");
        
        Date current = begin;
        while(current.compareTo(end)<=0) {
            for(String regionName:orderBillRegions) {
                Date cur2 = current;                
                ownersList.keySet().forEach(ownerId->{
                    sb.setLength(0);
                    sb2.setLength(0);
                    sb.append(regionName).append("|").append(UsageMetaType.DAY_OWNER)
                      .append("|").append(ownerId)
                      .append("|").append(DateFormatUtils.format(cur2, "yyyy-MM-dd"));
                    sb2.append(regionName).append("|").append(UsageMetaType.DAY_OWNER)
                    .append("|").append(ownerId)
                    .append("|").append(Consts.STORAGE_CLASS_STANDARD_IA)
                    .append("|").append(DateFormatUtils.format(cur2, "yyyy-MM-dd"));
                    resultRowKeys.add(sb2.toString());
                    resultRowKeys.add(sb.toString());
                });
            }
            current = DateUtils.addDays(current, 1);
        }
        
        return resultRowKeys;
    }

    @Override
    public void exec(String[] args) throws Exception {
        HostUtils.writePid(new File(System.getProperty("user.dir"),
                "pid/oos_bssOrderBillSender.pid").getAbsolutePath());
        
        DSyncService dsync = new DSyncService(GlobalHHZConfig.getQuorumServers(),
                GlobalHHZConfig.getSessionTimeout());
        try {
            DLock lock = dsync.createLock(OosZKNode.bssOrderBillSenderLock, null);
            lock.lock();
            try {
                if (args != null && args.length == 1) {
                    LocalDate startDate = LocalDate.parse(args[0], format_day);
                    // 如果有参数，则参数必须是小于当前日期前一天的日期，表示补传数据
                    if (startDate.compareTo(LocalDate.now().plusDays(-1)) > 0) {
                        log.error("Parameter must less than server current date");
                        System.exit(0);
                    }
                    beginDate = args[0];
                } else {
                    beginDate = "";
                }

                scheduleThreadPool.scheduleAtFixedRate(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            try {
                                // 启动时间校验
                                String sendTime = BssConfig.orderBillSenderStartTime;

                                if (sendTime == null || "".equals(sendTime))
                                    sendTime = "0130";
                                // 未到指定时间不执行
                                if (LocalDateTime.now().format(format_time).compareTo(sendTime) < 0) {
                                    log.info("Order bill interface in wrong time:" + LocalDateTime.now());
                                    return;
                                }

                                // 如果带有指定日期的启动参数，则立即执行(用于运维重启)；否则按照默认方式执行，即每天按照执行时间只执行一次；
                                if (beginDate == null || beginDate.trim().length() == 0) {
                                    // 每5分钟执行一次，系统当前时间与指定时间相差1小时之内可以执行；
                                    Duration duration = Duration.between(
                                        LocalDateTime.of(LocalDateTime.now().getYear(),
                                            LocalDateTime.now().getMonth(),
                                            LocalDateTime.now().getDayOfMonth(),
                                            Integer.parseInt(sendTime.substring(0, 2)),
                                            Integer.parseInt(sendTime.substring(2))),
                                        LocalDateTime.now());
                                    if (duration.toMinutes() >= 5) {
                                        log.info("Order bill interface in wrong time:" + LocalDateTime.now());
                                        return;
                                    }
                                }
                            } catch (Exception e) {
                                log.error(e.getMessage(), e);
                                System.exit(0);
                            }

                            log.info("BSS order bill interface begin:" + LocalDateTime.now());
                            batchJob();
                        } catch (Exception e) {
                            log.error(e.getMessage(), e);
                        }
                    }
                }, 0, 5, TimeUnit.MINUTES);

                Thread.currentThread().join();
            } finally {
                lock.unlock();
            }
        } finally {
            dsync.close();
        }
    }

    private void batchJob() throws Exception {
        try {
            //如果私有资源池不对接话单文件
            if (!SynchroConfig.propertyLocalPool.synchroOrderBill){
                log.info(BssAdapterConfig.localPool.name + " does not need to send order bill.");
                return;
            }
            
            initOrderBillRegions();
            
            //清除脏数据并发送标志位信息
            if (!clearHistoryData()) return;
            
            HashMap<String,String> bssUsers = DB.getInstance().getOrderBillUsers();
            
            //统计传送数据记录条数
            int threadCount = getThreadCount(bssUsers.size());
            if (threadCount == 0) {
                log.info("no order data at :" + getStartDate());
                return;
            }

            //汇总数据；
            loadOrderBillData(threadCount,bssUsers);
            
            uploadData();
        } catch (SQLException | InterruptedException | ExecutionException
                | TimeoutException e) {
            log.error("Bss order bill interface error:", e);
        } finally {
            beginDate = "";
        }
    }

    private void initOrderBillRegions() {
        orderBillRegions.clear();
        if (BssConfig.orderBillRegions != null && !BssConfig.orderBillRegions.isEmpty()) {
            orderBillRegions.addAll(Arrays.asList(BssConfig.orderBillRegions.split(",")));
            List<String> v6Regions = DataRegions.getAllRegions();
            
            //检查有效性
            orderBillRegions = orderBillRegions.stream().distinct().filter(x->{
               return v6Regions.contains(x);
            }).collect(Collectors.toList());
        } 

        //未配置此标签时，按全部region对接
        if (orderBillRegions.size() == 0) {
            orderBillRegions.addAll(DataRegions.getAllRegions());    
        }
        log.info("Order bill in regions:" + orderBillRegions + " will be sent later.");
    }

    private void uploadData() throws InterruptedException, ExecutionException {
        for(Future<List<OrderMessage>> feature:readerFutures) {
            List<OrderMessage> orderList = feature.get();
            
            if (orderList != null && orderList.size()>0) {
                uploadList(orderList);
            }
        }
    }

    private void uploadList(List<OrderMessage> orderList) {
        for(OrderMessage orderMsg : orderList) {
            if (orderMsg != null) {
                JSONObject transObj = new JSONObject();
                
                while(true) {
                    try {
                        orderMsg.decrementTimes();
                        transObj.put("regionName",mappingProperty.getProperty(
                                OOSConfig.getPortalDomain(),OOSConfig.getPortalDomain()));
                        transObj.put("isTruncated", false);
                        transObj.put("orderData", orderMsg.getOrders());

                        HttpURLConnection conn = (HttpURLConnection) (new URL(
                                BssAdapterConfig.protocol,
                                BssAdapterConfig.ip,
                                BssAdapterConfig.port,
                                "/?Action=SubmitOrderBill").openConnection());

                        conn.setUseCaches(false);
                        conn.setRequestProperty("Date",TimeUtils.toGMTFormat(new Date()));
                        conn.setRequestProperty("Host",BssAdapterConfig.ip + ":" + BssAdapterConfig.port);
                        conn.setRequestProperty("Content-Type","application/json");
                        conn.setRequestMethod("PUT");
                        conn.setDoOutput(true);
                        @SuppressWarnings("unchecked")
                        String canonicalString = RestUtils.makeS3CanonicalString("PUT",
                                Utils.toResourcePath(null, null,false),
                                new cn.ctyun.oos.common.OOSRequest(conn),null);
                        String signature = Utils.sign(canonicalString,BssAdapterConfig.localPool.getSK(),
                                SigningAlgorithm.HmacSHA1);
                        String authorization = "AWS " + BssAdapterConfig.localPool.ak + ":"+ signature;
                        conn.setRequestProperty("Authorization",authorization);

                        conn.connect();

                        try (OutputStream out = conn.getOutputStream()) {
                            out.write(transObj.toString().getBytes("utf-8"));
                            out.flush();
                        }

                        int code = conn.getResponseCode();

                        if (code == 200) {
                            break;
                        } else {
                            log.error("Fail to transform order bill's data to Server.");
                            if(orderMsg.getRetryTimes()>0) {
                                Thread.sleep(3*1000);
                                continue;
                            } else {
                                break;
                            }
                        }
                    } catch (Exception e) {
                        log.error(e);
                        if(orderMsg.getRetryTimes()>0) {
                            try {
                                Thread.sleep(3*1000);
                                continue;
                            } catch (InterruptedException e1) {}
                        } else {
                            break;
                        }
                    }
                }
            }
        }
    }

    // 当任务重新执行时，确保重复上传的数据要删除；
    private boolean clearHistoryData() {
        try {
            //从配置文件中获取region名称对应的bss那边给定的资源池名字            
            List<String> bssRegionName = orderBillRegions.stream().map(x->{
                return mappingProperty.getProperty(x,x);
            }).collect(Collectors.toList());
            
            HttpURLConnection conn = (HttpURLConnection) (new URL(BssAdapterConfig.protocol, BssAdapterConfig.ip,
                    BssAdapterConfig.port, "/?Action=SubmitOrderBill").openConnection());

            conn.setUseCaches(false);
            conn.setRequestProperty("Date", TimeUtils.toGMTFormat(new Date()));
            conn.setRequestProperty("Host", BssAdapterConfig.ip + ":" + BssAdapterConfig.port);
            conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
            conn.setRequestMethod("POST");
            conn.setConnectTimeout(0);
            conn.setDoOutput(true);
            @SuppressWarnings("unchecked")
            String canonicalString = RestUtils.makeS3CanonicalString("POST",Utils.toResourcePath(null, null, false),
                    new cn.ctyun.oos.common.OOSRequest(conn), null);
            String signature = Utils.sign(canonicalString,BssAdapterConfig.localPool.getSK(),
                    SigningAlgorithm.HmacSHA1);
            String authorization = "AWS " + BssAdapterConfig.localPool.ak + ":" + signature;
            conn.setRequestProperty("Authorization", authorization);
            conn.connect();
            try (OutputStream out = conn.getOutputStream()) {
                StringBuilder buffer = new StringBuilder();
                buffer.append("clear=1&poolName=" + mappingProperty.getProperty(
                            OOSConfig.getPortalDomain(),OOSConfig.getPortalDomain()))
                      .append("&regions="+StringUtils.join(bssRegionName.toArray(), ","))
                      .append("&beginDate=").append(getStartDate())
                      .append("&endDate=").append(getEndDate());

                out.write(Bytes.toBytes(buffer.toString()));
                out.flush();
                log.info("\nSent message to Adapter:" + buffer.toString() + "\n");
            }
            int code = conn.getResponseCode();

            if (code == 200) return true;
            
            log.error("Fail to transform order bill's data to BssAdapterServer.code:" + code);
            return false;
        } catch (Exception e) {
            log.error("Fail to transform order bill's data to BssAdapterServer.error:" + e.getMessage());
            return false;
        }
    }

    public static void main(String[] args) throws Exception {
        new BSSOrderBillSender().exec(args);
    }

    @Override
    public String usage() {
        return "Usage: BSSOrderBillManager \n";
    }
}
