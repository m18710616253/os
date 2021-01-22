package cn.ctyun.oos.server.management;

import java.io.IOException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.amazonaws.HttpMethod;
import com.amazonaws.services.s3.internal.XmlWriter;

import cn.ctyun.common.BaseException;
import cn.ctyun.common.Consts;
import cn.ctyun.common.Session;
import cn.ctyun.common.region.DataRegions;
import cn.ctyun.oos.common.Email;
import cn.ctyun.oos.common.ErrorMessage;
import cn.ctyun.oos.common.Parameters;
import cn.ctyun.oos.common.SocketEmail;
import cn.ctyun.oos.common.Utils;
import cn.ctyun.oos.hbase.ManagementUser;
import cn.ctyun.oos.hbase.MetaClient;
import cn.ctyun.oos.metadata.BucketMeta;
import cn.ctyun.oos.metadata.MinutesUsageMeta;
import cn.ctyun.oos.metadata.MinutesUsageMeta.FlowStats;
import cn.ctyun.oos.metadata.MinutesUsageMeta.RequestStats;
import cn.ctyun.oos.metadata.MinutesUsageMeta.SizeStats;
import cn.ctyun.oos.metadata.MinutesUsageMeta.UsageMetaType;
import cn.ctyun.oos.metadata.OwnerMeta;
import cn.ctyun.oos.metadata.UserTypeMeta;
import cn.ctyun.oos.server.db.dbprice.DBPrice;
import cn.ctyun.oos.server.db.dbprice.DbPackage;
import cn.ctyun.oos.server.db.dbprice.DbSalerUser;
import cn.ctyun.oos.server.db.dbprice.DbSalesman;
import cn.ctyun.oos.server.db.dbprice.DbTieredPrice;
import cn.ctyun.oos.server.db.dbprice.DbUserPackage;
import cn.ctyun.oos.server.db.dbprice.DbUserTieredDiscount;
import cn.ctyun.oos.server.usage.UsageResult;
import cn.ctyun.oos.server.util.DateParser;
import cn.ctyun.oos.server.util.Misc;
import cn.ctyun.oos.website.Portal;
import common.time.TimeUtils;

/**
 * @author: Cui Meng
 */
public class Salesman {
    private static final Log log = LogFactory.getLog(Salesman.class);
    public static Session<String, String> session;
    public static CompositeConfiguration oosConfig = Admin.oosConfig;
    private static MetaClient client = MetaClient.getGlobalClient();
    
    public static void handleSalesmanRequest(HttpServletRequest req, HttpServletResponse resp)
            throws Exception {
        if (req.getHeader(ManagementHeader.SALESMAN).equals("package")) {
            handlePackage(req, resp);
            return;
        }
        if (req.getHeader(ManagementHeader.SALESMAN).equals("tieredPrice")) {
            handleTieredPrice(req, resp);
            return;
        }
        if (req.getHeader(ManagementHeader.SALESMAN).equals("salesman")) {
            handleSalesman(req, resp);
            return;
        }
        if (req.getHeader(ManagementHeader.SALESMAN).equals("user")) {
            handleUser(req, resp);
            return;
        }
        if (req.getHeader(ManagementHeader.SALESMAN).equals("statistics")) {
            handleStatistics(req, resp);
        }
        if (req.getHeader(ManagementHeader.SALESMAN).equals("bill")) {
            handleBill(req, resp);
        }
        //处理服务限制页面请求
        if (req.getHeader(ManagementHeader.SALESMAN).equals("serviceLimit")) {
            handleServiceLimit(req, resp);
        }
    }
    
    /**
     * 处理用户管理-服务限制页面请求
     * @param req
     * @param resp
     * @throws Exception
     */
    private static void handleServiceLimit(HttpServletRequest req, HttpServletResponse resp) throws Exception {
        //存储桶管理
        if(req.getParameter("bucketManage") != null && req.getParameter("bucketManage").trim().length() != 0)
            handleBucketLimitManage(req, resp);        
    }

    /**
     * 处理用户管理-服务限制中的存储桶管理，存储桶管理请求中参数bucketManage包含请求的管理类型，用","分隔
     * @param req
     * @param resp
     * @throws Exception 
     */
    private static void handleBucketLimitManage(HttpServletRequest req, HttpServletResponse resp) throws Exception {
        String bucketManage = req.getParameter("bucketManage");
        //管理类型
        List<String> types = Arrays.asList(bucketManage.split(","));
        Common.checkParameter(req.getParameter("ownerId"));
        long ownerId = Long.parseLong(req.getParameter("ownerId"));
        OwnerMeta owner = new OwnerMeta(ownerId);
        if(!client.ownerSelectByIdNoCaching(owner))
            throw new BaseException(404, "NoSuchUser");
        if(req.getMethod().equals(HttpMethod.PUT.toString())) {
            //修改用户的存储桶上限数量
            if(types.contains("bucketNumCeiling")) {
                Common.checkParameter(req.getParameter("ceiling"));
                int bucketCeilingNum = Integer.parseInt(req.getParameter("ceiling"));
                owner.bucketCeilingNum = bucketCeilingNum;
                client.ownerUpdate(owner);
            }
        } else if(req.getMethod().equals(HttpMethod.GET.toString())) {
            XmlWriter xml = new XmlWriter();
            xml.start("serviceLimit");
            //获取用户的存储桶上限数量
            if(types.contains("bucketNumCeiling")) {
                int num;
                if(owner.bucketCeilingNum != -1) {
                    //为用户单独配置过存储桶上限
                    num = owner.bucketCeilingNum;
                } else {
                    //没有为用户单独配置过存储桶上限，用userType获取
                    UserTypeMeta userTypeMeta = new UserTypeMeta(owner.userType);
                    if(userTypeMeta.getUserType() == null)
                        throw new BaseException(404, "NoSuchUserType");
                    client.userTypeSelect(userTypeMeta);
                    num = userTypeMeta.getBucketNumCeiling().ceiling;
                }
                xml.start("bucketNumCeiling");
                xml.start("ceiling").value(String.valueOf(num)).end();
                xml.end();
            }
            xml.end();
            Common.writeResponseEntity(resp, xml.toString());
        }   
    }

    public static void handleBssGetUserUsage(HttpServletRequest req, HttpServletResponse resp) throws Exception {
        long ownerId = Long.parseLong(req.getParameter("ownerId"));
        Common.checkParameter(ownerId);
        OwnerMeta owner = new OwnerMeta(ownerId);
        if (req.getHeader("x-portaloos-get-user-usage").equalsIgnoreCase("usage")
                && req.getParameter("usageStats").equalsIgnoreCase("totalsize")) {
            //删除欠费用户数据获取用户用量的接口
            String beginDate = req.getParameter("beginDate");
            String endDate = req.getParameter("endDate");
            Common.checkParameter(beginDate);
            Common.checkParameter(endDate);
            
            String usageStats = req.getParameter("usageStats");
            String freq = req.getParameter("freq");

            Common.checkParameter(usageStats);
            // beginDate、endDate格式为yyyy-MM-dd
            Utils.checkUsageReqParamsValid(beginDate, endDate, freq);
            Set<String> dataRegions = new HashSet<String>(DataRegions.getAllRegions());

            dataRegions.add(Consts.GLOBAL_DATA_REGION);
            long t1 = System.currentTimeMillis();
            Map<String, List<MinutesUsageMeta>> usageMap = UsageResult.getSingleCommonUsageStatsQuery(owner.getId(), beginDate,
                    endDate, null, freq, dataRegions, UsageMetaType.DAY_OWNER, Consts.ALL, true);
            long t2 = System.currentTimeMillis();
            log.info("bss get user usage log: get single usage use time " + (t2 - t1));
            List<MinutesUsageMeta> globalUsage = usageMap.get(Consts.GLOBAL_DATA_REGION);
            Map<String, MinutesUsageMeta> currentUsage = new HashMap<String, MinutesUsageMeta>();
            dataRegions.remove(Consts.GLOBAL_DATA_REGION);
            long endTime = Misc.formatyyyymmdd(endDate).getTime();
            long t3 = System.currentTimeMillis();
            
            for (String region : dataRegions) {
                LinkedHashMap<String, TreeMap<Long, MinutesUsageMeta>> minutesUsages = client.minutesUsageRegionCurrent5Mins(0,
                        endTime, region, owner.getId(), null, UsageMetaType.CURRENT_OWNER, null, 1000);
                if (minutesUsages.isEmpty())
                    continue;
                for (Entry<String, TreeMap<Long, MinutesUsageMeta>> entry : minutesUsages.entrySet()) {
                    String k = entry.getKey();
                    // 行key，格式为regionName|CURRENT_XXX|userId|day
                    String date = k.split("\\|")[3];
                    MinutesUsageMeta meta = currentUsage.get(date);
                    if (meta == null) {
                        meta = new MinutesUsageMeta(UsageMetaType.DAY_OWNER, region, owner.getId(), date, "");
                        meta.initStatsExceptConnection();
                    }
                    for (MinutesUsageMeta minutesMeta : entry.getValue().values()) {
                        if (minutesMeta.sizeStats != null)
                            meta.sizeStats.sumAll(minutesMeta.sizeStats);
                    }
                    currentUsage.put(date, meta);
                }
            }
            long t4 = System.currentTimeMillis();
            log.info("bss get user usage log: get current usage use time " + (t4 - t3));

            JSONObject reqJo = new JSONObject();
            JSONArray reqJa = new JSONArray();
            JSONObject regionJo = new JSONObject();
            JSONArray regionJa = new JSONArray();
            for (MinutesUsageMeta globalDayUsage : globalUsage) {
                String date = globalDayUsage.time;
                for (MinutesUsageMeta currentMeta : currentUsage.values()) {
                    if (currentMeta.time.compareTo(date) < 0)
                        globalDayUsage.sizeStats.sumAll(currentMeta.sizeStats);
                }
                JSONObject orderJo = new JSONObject();
                orderJo.put("date", globalDayUsage.time);
                orderJo.put("value", globalDayUsage.sizeStats.size_total);
                regionJa.put(orderJo);
            }
            regionJo.put("order", regionJa);
            regionJo.put("regionName", Consts.GLOBAL_DATA_REGION);
            reqJa.put(regionJo);
            reqJo.put("totalSize", reqJa);
            Common.writeJsonResponseEntity(resp, reqJo.toString());
        } else if (req.getHeader("x-portaloos-get-user-usage").equalsIgnoreCase("userBucket")) {
            Common.writeJsonResponseEntity(resp, getUserBucketInfoForOOS(owner));
        }else {
            Common.writeJsonResponseEntity(resp, getUserBucketInfoForBSS(owner));
        }
    }
    
    private static String getUserBucketInfoForBSS(OwnerMeta owner) throws JSONException, IOException, Exception {
        JSONObject reqJo = new JSONObject();
        long t1 = System.currentTimeMillis();
        if (!client.ownerSelectByIdNoCaching(owner)) {
            reqJo.put("bucketInfo", "");
        } else {
            List<BucketMeta> bucketList = client.bucketList(owner.getId());
            StringBuilder sb = new StringBuilder();
            for (BucketMeta bucket : bucketList) {
                sb.append(bucket.name + ",");
            }
            reqJo.put("bucketInfo", sb.toString());
        }
        log.info("bss get bucket info is:" + reqJo.toString() + " and spend time is:"+((System.currentTimeMillis()-t1)/1000) +"s");
        return reqJo.toString();
    }

    private static String getUserBucketInfoForOOS(OwnerMeta owner) throws IOException, Exception {
     // 注销账号查用户的bucket是否存在
        long t1 = System.currentTimeMillis();
        JSONObject reqJo = new JSONObject();
        if (!client.ownerSelectByIdNoCaching(owner)) {// 如果在owner表中没有账号// 说明用户不可用该资源池。不再查询bucket表。
            reqJo.put("bucketInfo", "");
            reqJo.put("cancelledProcess", true);
        } else {
            List<BucketMeta> bucketList = client.bucketList(owner.getId());
            if (bucketList.size() == 0) {// 有账号但是没有bucket
                owner.auth.setDisable();
                client.ownerUpdate(owner);// 将owner表的auth字段全部置disabled冻结态
                reqJo.put("bucketInfo", "");
            }else{//有账号有bucket返回bucket信息
                StringBuilder sb = new StringBuilder();
                for(BucketMeta bucket:bucketList) {
                    sb.append(bucket.name+",");
                }
                reqJo.put("bucketInfo", sb.toString());
            }
        }
        log.info("oos get bucket info is:" + reqJo.toString() + " and spend time is:"+((System.currentTimeMillis()-t1)/1000) +"s");
        return reqJo.toString();
    }

    private static void handleStatistics(HttpServletRequest req, HttpServletResponse resp)
            throws Exception {
        long ownerId = Long.parseLong(req.getParameter("ownerId"));
        String beginDate = req.getParameter("beginDate");
        String endDate = req.getParameter("endDate");
        Common.checkParameter(ownerId);
        Common.checkParameter(beginDate);
        Common.checkParameter(endDate);
        OwnerMeta owner = new OwnerMeta(ownerId);
        // usageStats表示各个统计项，多个统计项以逗号分隔,带宽95峰值统计数据，包含uploadPeakBW，transferPeakBW
        String usageStats = req.getParameter("usageStats");
        String bucketName = req.getParameter("bucketName");
        String freq = req.getParameter("freq");
        String storageType = Utils.checkAndGetStorageType(req.getParameter("storageClass"));
        String body = "";
        // 查询带宽95峰值统计数据
        List<String> usage = Arrays.asList(usageStats.split(","));
        if (usage.contains("uploadPeakBW") || usage.contains("transferPeakBW")) {
            try {
                Date tmpEndDate = Misc.formatyyyymm(endDate);
                Date tmpBeginDate = Misc.formatyyyymm(beginDate);
                if (tmpEndDate.before(tmpBeginDate))
                    throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_ARGUMENT);
            } catch (ParseException e) {
                throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_ARGUMENT);
            }
            body = UsageResult.get95PeakBWStatsInJson(ownerId, beginDate, endDate, usageStats, bucketName, Consts.STORAGE_CLASS_STANDARD);
        } else {
            // 查询统计项
            Common.checkParameter(usageStats);
            // beginDate、endDate格式为yyyy-MM-dd
            Utils.checkUsageReqParamsValid(beginDate, endDate, freq);
            Set<String> dataRegions = new HashSet<>(DataRegions.getAllRegions());
            dataRegions.add(Consts.GLOBAL_DATA_REGION);
            body = UsageResult.getSingleUsageInJson(owner.getId(), beginDate, endDate, usageStats, bucketName, freq, dataRegions, storageType, false, true);
        }
        Common.writeJsonResponseEntity(resp, body);
    }

    private static void handleBill(HttpServletRequest req, HttpServletResponse resp)
            throws Exception {
        String type = req.getParameter("type");
        String userName = req.getParameter("userName");
        String beginDate = req.getParameter("beginDate");
        String endDate = req.getParameter("endDate");
        Common.checkParameter(type);
        Common.checkParameter(userName);
        Common.checkParameter(beginDate);
        Common.checkParameter(endDate);
        OwnerMeta owner = new OwnerMeta(userName);
        if (!client.ownerSelect(owner))
            throw new BaseException(400, "NoSuchUser");
        switch (type) {
        case "detail":
            Common.writeResponseEntity(resp, getDetailBill(owner.getId(), beginDate, endDate));
            break;
        case "peak":
            Common.writeResponseEntity(resp, getPeakBill(owner.getId(), beginDate, endDate));
            break;
        }
    }

    /**
     * 获取详细用量信息
     * @param ownerId
     * @param dateBegin
     * @param dateEnd
     * @return
     * @throws Exception
     */
    private static String getDetailBill(long ownerId, String dateBegin, String dateEnd) throws Exception  {
        long start = System.currentTimeMillis();
        dateEnd = dateEnd + Character.MAX_VALUE;
        StringBuilder result = new StringBuilder();
        // 添加表头
        result.append("regionName,peakSize,standardIaPeakSize,upload,roamUpload,flow,roamFlow,noNetUpload,noNetRoamUpload,noNetFlow,noNetRoamFlow,ghRequest,otherRequest,noNetGHReq,noNetOtherReq,dateBegin\n");
        // 获取所有的region
        List<String> allRegions = DataRegions.getAllRegions();
        for (String region : allRegions) {
            // 获取region下的使用量
            List<MinutesUsageMeta> metas = client.minutesUsageCommonStatsList(region, ownerId, null, UsageMetaType.DAY_OWNER, dateBegin, dateEnd, Consts.STORAGE_CLASS_STANDARD);
            // 获取region下低频存储的容量
            List<MinutesUsageMeta> metasStandardIa = client.minutesUsageCommonStatsList(region, ownerId, null, UsageMetaType.DAY_OWNER, dateBegin, dateEnd, Consts.STORAGE_CLASS_STANDARD_IA);
            if (CollectionUtils.isEmpty(metas) && CollectionUtils.isEmpty(metasStandardIa)) {
                continue;
            }
            // 将标准存储 和 低频存储 数据按照region和时间 进行统一
            // Set<region + time>
            Set<String> regionTimeSortedSet = new TreeSet<>();
            // Map< region+time , MinutesUsage>
            Map<String, MinutesUsageMeta> mapForStandard = new TreeMap<>();
            Map<String, MinutesUsageMeta> mapForStandardIa = new TreeMap<>();
            for (MinutesUsageMeta meta : metas) {
                mapForStandard.put(region + meta.time, meta);
                regionTimeSortedSet.add(region + meta.time);
            }
            for (MinutesUsageMeta meta : metasStandardIa) {
                mapForStandardIa.put(region + meta.time, meta);
                regionTimeSortedSet.add(region + meta.time);
            }
            for (String key : regionTimeSortedSet) {
                MinutesUsageMeta standard = mapForStandard.get(key);
                MinutesUsageMeta standardIa = mapForStandardIa.get(key);
                String time = Objects.isNull(standard) ? standardIa.time : standard.time;
                SizeStats sizeStatsStandard = Objects.isNull(standard) ? new SizeStats() : standard.sizeStats;
                SizeStats sizeStatsStandardIa = Objects.isNull(standardIa) ? new SizeStats() : standardIa.sizeStats;
                FlowStats flowStatsStandard = Objects.isNull(standard) ? new FlowStats() : standard.flowStats;
                RequestStats requestStandard = Objects.isNull(standard) ? new RequestStats() : standard.requestStats;
                appendCommonStats(result, sizeStatsStandard, flowStatsStandard, requestStandard, region, time, sizeStatsStandardIa);
            }
        }
        log.info("getDetailBill ownerId: " + ownerId + ", dateBegin: " + dateBegin + ", dateEnd: " + dateEnd + ", use time: " + (System.currentTimeMillis() - start) + "ms");
        return result.toString();
    }

    /**
     * 向result追加用量信息
     *
     * @param result
     * @param cs
     * @param region
     * @param dateBegin
     */
    private static void appendCommonStats(StringBuilder result, SizeStats size, FlowStats flow, RequestStats req, String region, String dateBegin, SizeStats sizeStandardIa) {
        result.append(region).append(",")
                .append(size.size_peak / Consts.GB).append(",")
                .append(sizeStandardIa.size_peak / Consts.GB).append(",")
                .append(flow.flow_upload / Consts.GB).append(",")
                .append(flow.flow_roamUpload / Consts.GB).append(",")
                .append(flow.flow_download / Consts.GB).append(",")
                .append(flow.flow_roamDownload / Consts.GB).append(",")
                .append(flow.flow_noNetUpload / Consts.GB).append(",")
                .append(flow.flow_noNetRoamUpload / Consts.GB).append(",")
                .append(flow.flow_noNetDownload / Consts.GB).append(",")
                .append(flow.flow_noNetRoamDownload / Consts.GB).append(",")
                .append((req.req_get + req.req_head) / Consts.TEN_THOUSAND).append(",")
                .append((req.req_other + req.req_put + req.req_post + req.req_delete) / Consts.THOUSAND).append(",")
                .append((req.req_noNetGet + req.req_noNetHead) / Consts.TEN_THOUSAND).append(",")
                .append((req.req_noNetOther + req.req_noNetPut + req.req_noNetPost + req.req_noNetDelete) / Consts.THOUSAND);
        if (dateBegin != null) {
            result.append(",").append(dateBegin);
        }
        result.append("\n");
    }

    /**
     * 返回用户按region统计的峰值容量及其他用量的加和
     * @param ownerId
     * @param dateBegin
     * @param dateEnd
     * @return
     * @throws Exception
     */
    private static String getPeakBill(long ownerId, String dateBegin, String dateEnd) throws Exception  {
        long start = System.currentTimeMillis();
        dateEnd = dateEnd + Character.MAX_VALUE;
        StringBuilder result = new StringBuilder();
        // 添加表头
        result.append("regionName,peakSize,standardIaPeakSize,upload,roamUpload,flow,roamFlow,noNetUpload,noNetRoamUpload,noNetFlow,noNetRoamFlow,ghRequest,otherRequest,noNetGHReq,noNetOtherReq\n");
        // 获取所有的region
        List<String> allRegions = DataRegions.getAllRegions();
        for (String region : allRegions) {
            // 获取region下的 标准存储使用量
            List<MinutesUsageMeta> metas = client.minutesUsageCommonStatsList(region, ownerId, null, UsageMetaType.DAY_OWNER, dateBegin, dateEnd, Consts.STORAGE_CLASS_STANDARD);
            // region下 低频存储使用量
            List<MinutesUsageMeta> metasStandardIa = client.minutesUsageCommonStatsList(region, ownerId, null, UsageMetaType.DAY_OWNER, dateBegin, dateEnd, Consts.STORAGE_CLASS_STANDARD_IA);
            if (CollectionUtils.isEmpty(metas) && CollectionUtils.isEmpty(metasStandardIa)) {
                continue;
            }
            // 记录当前region在指定时间范围内的总用量
            SizeStats regionSizeStats = new SizeStats();
            FlowStats regionFlowStats = new FlowStats();
            RequestStats regionRequestStats = new RequestStats();
            long maxPeakSize = 0;
            // 遍历用户当前region每天的用量数据
            for (MinutesUsageMeta meta : metas) {
                if (meta.sizeStats != null) {
                    regionSizeStats.sumAll(meta.sizeStats);
                    maxPeakSize = Math.max(maxPeakSize, meta.sizeStats.size_peak);
                    regionSizeStats.size_peak = maxPeakSize;
                }
                if (meta.flowStats != null) {
                    regionFlowStats.sumAll(meta.flowStats);
                }
                if (meta.requestStats != null) {
                    regionRequestStats.sumAll(meta.requestStats);
                }
            }
            // 记录region低频存储
            SizeStats regionSizeStatsStandardIa = new SizeStats();
            long maxPeakSizeStandardIa = 0;
            for (MinutesUsageMeta meta : metasStandardIa) {
                if (meta.sizeStats != null) {
                    regionSizeStatsStandardIa.sumAll(meta.sizeStats);
                    maxPeakSizeStandardIa = Math.max(maxPeakSizeStandardIa, meta.sizeStats.size_peak);
                    regionSizeStatsStandardIa.size_peak = maxPeakSizeStandardIa;
                }
            }
            // 向结果中追加当前region在指定时间范围内的总用量
            appendCommonStats(result, regionSizeStats, regionFlowStats, regionRequestStats, region, null, regionSizeStatsStandardIa);
        }
        log.info("getPeakBill ownerId: " + ownerId + ", dateBegin: " + dateBegin + ", dateEnd: " + dateEnd + ", use time: " + (System.currentTimeMillis() - start) + "ms");
        return result.toString();
    }

    private static boolean isManager(DbSalesman dbSalesman) {
        if (dbSalesman.id.equals("admin") || dbSalesman.isManager == 1)
            return true;
        else
            return false;
    }
    
    private static void handleUser(HttpServletRequest req, HttpServletResponse resp)
            throws Exception {
        if (!isManager(getSalesmanFromSession(Utils.getSessionId(req))))
            throw new BaseException(403, "AccessDenied");
        if (req.getMethod().equals(HttpMethod.PUT.toString())) {
            String originSalerId = req.getParameter("originSalerId");
            String ownerName = req.getParameter("ownerName");
            String newSalerId = req.getParameter("newSalerId");
            Common.checkParameter(originSalerId);
            Common.checkParameter(ownerName);
            Common.checkParameter(newSalerId);
            transferUser(originSalerId, ownerName, newSalerId);
        }
    }
    
    private static void transferUser(String originSalerId, String ownerName, String newSalerId)
            throws Exception {
        OwnerMeta owner = new OwnerMeta(ownerName);
        client.ownerSelect(owner);
        DbSalerUser dbSalerUser = new DbSalerUser(owner.getId());
        DBPrice dbPrice = DBPrice.getInstance();
        dbPrice.salerUserSelectByUser(dbSalerUser);
        if (!dbSalerUser.salerId.equals(originSalerId))
            throw new BaseException(403, "OriginSalerIdWrong");
        DbSalesman os = new DbSalesman(originSalerId);
        DbSalesman ns = new DbSalesman(newSalerId);
        if (!dbPrice.salesmanSelect(os) || !dbPrice.salesmanSelect(ns))
            throw new BaseException(400, "NoSuchSalesman");
        if (!os.organizationId.equals(ns.organizationId))
            throw new BaseException(403, "NotInSameOrganization");
        dbSalerUser.salerId = ns.id;
        dbPrice.salerUserUpdate(dbSalerUser);
    }
    
    private static void handleSalesman(HttpServletRequest req, HttpServletResponse resp)
            throws Exception {
        if (req.getMethod().equals(HttpMethod.GET.toString())) {
            Common.writeResponseEntity(
                    resp,
                    getSalesman(getSalesmanFromSession(Utils.getSessionId(req)),
                            req.getParameter("organizationId")));
            return;
        }
        if (req.getMethod().equals(HttpMethod.PUT.toString())) {
            String organizationId = req.getParameter("organizationId");
            String name = req.getParameter("name");
            String email = req.getParameter("email");
            String phone = req.getParameter("phone");
            String id = req.getParameter("id");
            String bestPayAccount = req.getParameter("bestPayAccount");
            String password = req.getParameter("password");
            Common.checkParameter(organizationId, MConsts.ORGANIZATION_ID_LENGTH);
            Common.checkParameter(name, MConsts.SALESMAN_NAME_LENGTH);
            Common.checkParameter(email, MConsts.SALESMAN_EMAIL_LENGTH);
            Common.checkParameter(phone, MConsts.SALESMAN_PHONE_LENGTH);
            Common.checkParameter(id, MConsts.SALESMAN_ID_LENGTH);
            Common.checkParameter(bestPayAccount, MConsts.SALESMAN_BESTPAYACCOUNT_LENGTH);
            Common.checkParameter(password, MConsts.SALESMAN_PASSWORD_MAX_LENGTH,
                    MConsts.SALESMAN_PASSWORD_MIN_LENGTH);
            DbSalesman dbSalesman = new DbSalesman(email);
            dbSalesman.organizationId = organizationId;
            dbSalesman.name = name;
            dbSalesman.id = id;
            dbSalesman.phone = phone;
            dbSalesman.isManager = 0;
            dbSalesman.setPwd(password);
            Admin.registerSalesManager(dbSalesman, false);
        }
    }
    
    private static void handleTieredPrice(HttpServletRequest req, HttpServletResponse resp)
            throws Exception {
        if (req.getMethod().equals(HttpMethod.PUT.toString())) {
            Common.checkParameter(req.getParameter("storageDiscount"));
            Common.checkParameter(req.getParameter("flowDiscount"));
            Common.checkParameter(req.getParameter("requestsDiscount"));
            Common.checkParameter(req.getParameter("roamFlowDiscount"));
            Common.checkParameter(req.getParameter("roamUploadDiscount"));
            Common.checkParameter(req.getParameter("noNetReqDiscount"));
            Common.checkParameter(req.getParameter("noNetFlowDiscount"));
            Common.checkParameter(req.getParameter("noNetRoamFlowDiscount"));
            Common.checkParameter(req.getParameter("noNetRoamUploadDiscount"));
            Common.checkParameter(req.getParameter("startTime"));
            Double storageDiscount = Double.parseDouble(req.getParameter("storageDiscount"));
            Double flowDiscount = Double.parseDouble(req.getParameter("flowDiscount"));
            Double requestsDiscount = Double.parseDouble(req.getParameter("requestsDiscount"));
            Double roamFlowDiscount = Double.parseDouble(req.getParameter("roamFlowDiscount"));
            Double roamUploadDiscount = Double.parseDouble(req.getParameter("roamUploadDiscount"));
            Double noNetReqDiscount = Double.parseDouble(req.getParameter("noNetReqDiscount"));
            Double noNetFlowDiscount = Double.parseDouble(req.getParameter("noNetFlowDiscount"));
            Double noNetRoamFlowDiscount = Double.parseDouble(req.getParameter("noNetRoamFlowDiscount"));
            Double noNetRoamUploadDiscount = Double.parseDouble(req.getParameter("noNetRoamUploadDiscount"));
            String startTime = req.getParameter("startTime");
            if (req.getParameter("orderId") != null) {
                Common.checkParameter(req.getParameter("orderId"));
                modifyTieredPriceDiscount(req.getParameter("orderId"), storageDiscount,
                        flowDiscount, requestsDiscount, startTime,
                        getSalesmanFromSession(Utils.getSessionId(req)), roamFlowDiscount,
                        roamUploadDiscount,noNetReqDiscount,
                        noNetFlowDiscount,noNetRoamFlowDiscount,noNetRoamUploadDiscount);
            } else {
                Common.checkParameter(req.getParameter("userName"));
                addTieredPriceDiscount(req.getParameter("userName"), storageDiscount, flowDiscount,
                        requestsDiscount, startTime,
                        getSalesmanFromSession(Utils.getSessionId(req)), roamFlowDiscount,
                        roamUploadDiscount,noNetReqDiscount,
                        noNetFlowDiscount,noNetRoamFlowDiscount,noNetRoamUploadDiscount);
            }
            return;
        }
    }
    
    private static void handlePackage(HttpServletRequest req, HttpServletResponse resp)
            throws Exception {
        if (req.getMethod().equals(HttpMethod.PUT.toString())) {
            if (req.getParameter("packageDiscount") != null) {
                Common.checkParameter(req.getParameter("packageDiscount"));
                Common.checkParameter(req.getParameter("orderId"));
                Double packageDiscount = Double.parseDouble(req.getParameter("packageDiscount"));
                modifyPackageDiscount(req.getParameter("orderId"), packageDiscount,
                        getSalesmanFromSession(Utils.getSessionId(req)));
            }
        }
    }
    
    public static void registerUser(HttpServletRequest req, HttpServletResponse resp, Email email)
            throws Exception {
        String password = null;
        String ownerEmail = req.getParameter(Parameters.EMAIL);
        String userName = ownerEmail;
        OwnerMeta owner = null;
        if (ownerEmail == null)
            throw new BaseException();
        password = RandomStringUtils.randomAlphanumeric(6);
        owner = new OwnerMeta(userName);
        if (client.ownerSelect(owner))
            throw new BaseException(400, "UserAlreadyExist");
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
        if (!Portal.isLengthRight(ownerEmail, Consts.EMAIL_MIN_LENGTH, Consts.EMAIL_MAX_LENGTH)
                || !Portal
                        .isLengthRight(userName, Consts.EMAIL_MIN_LENGTH, Consts.EMAIL_MAX_LENGTH)
                || !Portal.isLengthRight(password, Consts.PASSWORD_MIN_LENGTH,
                        Consts.PASSWORD_MAX_LENGTH))
            throw new BaseException(400, "InvalidLength");
        if (!StringUtils.isAlphanumeric(password))
            throw new BaseException(400, "InvalidPassword");
        if (!Portal.isLengthRight(displayName, Consts.DISPLAYNAME_MAX_LENGTH))
            throw new BaseException(400, "InvalidDisplayNameLength");
        if (!Portal.isLengthRight(companyName, Consts.COMPANYNAME_MAX_LENGTH))
            throw new BaseException(400, "InvalidCompanyNameLength");
        if (!Portal.isLengthRight(companyAddress, Consts.COMPANYADDRESS_MAX_LENGTH))
            throw new BaseException(400, "InvalidCompanyAddressLength");
        if (mobilePhone != null
                && mobilePhone.trim().length() != 0
                && (mobilePhone.length() > Consts.MOBILEPHONE_MAX_LENGTH || !StringUtils
                        .isNumeric(mobilePhone)))
            throw new BaseException(400, "InvalidMobilePhone");
        if (!Portal.isLengthRight(phone, Consts.PHONE_MAX_LENGTH))
            throw new BaseException(400, "InvalidPhoneLength");
        if (phone != null && phone.trim().length() == 0 && mobilePhone != null
                && mobilePhone.trim().length() == 0)
            throw new BaseException();
        owner.displayName = displayName;
        owner.setPwd(password);
        owner.email = ownerEmail;
        owner.companyName = companyName;
        owner.companyAddress = companyAddress;
        owner.mobilePhone = mobilePhone;
        owner.phone = phone;
        owner.verify = null;
        owner.maxAKNum = Consts.MAX_AK_NUM;
        client.ownerInsert(owner);
        email.setSubject(email.getRegisterSubjectFromYunPortal());
        email.setNick(email.getEmailNick());
        SocketEmail.sendEmail(email);
        log.info("user: " + userName + " register success");
    }
    
    public static void verifySalesman(String verify, HttpServletResponse resp) throws SQLException,
            IOException {
        DbSalesman dbSalesman = new DbSalesman();
        dbSalesman.verify = verify;
        DBPrice dbPrice = DBPrice.getInstance();
        if (dbPrice.salesmanSelectByVerify(dbSalesman)) {
            dbSalesman.verify = null;
            dbPrice.salesmanUpdate(dbSalesman);
            log.info("verify salesman:" + dbSalesman.name + " success");
            resp.sendRedirect(oosConfig.getString("website.webpages") + "/activatesuccess.html");
        } else {
            resp.sendRedirect(oosConfig.getString("website.webpages") + "/error.html?msg="
                    + URLEncoder.encode("该客户经理不存在，或者已经被激活", "UTF-8"));
        }
    }
    
    public static DbSalesman getSalesmanFromSession(String sessionId) throws Exception {
        String name = session.get(sessionId);
        if (name == null)
            throw new BaseException(403, "InvalidSession");
        DBPrice dbPrice = DBPrice.getInstance();
        DbSalesman dbSalesman = new DbSalesman(name);
        ManagementUser user = new ManagementUser(name);
        if (client.managementUserGet(user)) {
            dbSalesman.id = MConsts.ADMIN;
            dbSalesman.isManager = 1;
            return dbSalesman;
        }
        if (!dbPrice.salesmanSelect(dbSalesman))
            throw new BaseException(403, "NoSuchSalesman");
        return dbSalesman;
    }
    
    public static boolean isBelongTo(DbSalesman dbSalesman, long ownerId) throws SQLException {
        if (dbSalesman.name.equals("admin"))
            return true;
        DBPrice dbPrice = DBPrice.getInstance();
        DbSalerUser dbSalerUser = new DbSalerUser();
        dbSalerUser.usrId = ownerId;
        dbPrice.salerUserSelectByUser(dbSalerUser);
        if (dbSalerUser.salerId.equals(dbSalesman.id))
            return true;
        DbSalesman manager = new DbSalesman();
        manager.organizationId = dbSalesman.organizationId;
        dbPrice.salesmanSelectByOrganizationId(dbSalesman, true);
        for (DbSalesman m : manager.sms) {
            if (m.id.equals(dbSalesman.id))
                return true;
        }
        return false;
    }
    
    public static void modifyPackageDiscount(String orderId, double discount, DbSalesman dbSalesman)
            throws SQLException, BaseException {
        DBPrice dbPrice = DBPrice.getInstance();
        DbUserPackage dbUserPackage = new DbUserPackage(orderId);
        dbPrice.userPackageSelectByOrderId(dbUserPackage);
        if (!isBelongTo(dbSalesman, dbUserPackage.usrId))
            throw new BaseException(403, "AccessDenied");
        if (dbUserPackage.isPaid == 1)
            throw new BaseException(403, "PackageAlreadyPaid");
        DbPackage dbPackage = new DbPackage(dbUserPackage.packageId);
        dbPrice.packageSelect(dbPackage);
        if (dbPackage.discount < discount)
            throw new BaseException(400, "DiscountCanNotLowerThanPackageDiscount");
        dbUserPackage.packageDiscount = discount;
        dbPrice.userPackageUpdate(dbUserPackage);
    }
    
    private static void addTieredPriceDiscount(String ownerName, Double storageDiscount,
            Double flowDiscount, Double requestDiscount, String startTime, DbSalesman dbSalesman,
            Double roamFlowDiscount, Double roamUploadDiscount,
            Double noNetReqDiscount, Double noNetFlowDiscount,
            Double noNetRoamFlowDiscount, Double noNetRoamUploadDiscount) throws Exception {
        DBPrice dbPrice = DBPrice.getInstance();
        OwnerMeta owner = new OwnerMeta(ownerName);
        if (client.ownerSelect(owner))
            throw new BaseException(400, "NoSuchUser");
        if (!isBelongTo(dbSalesman, owner.getId()))
            throw new BaseException(403, "AccessDenied");
        DbTieredPrice dbTieredPrice = new DbTieredPrice(1);
        dbPrice.tieredPriceSelect(dbTieredPrice);
        if (dbTieredPrice.discount < storageDiscount)
            throw new BaseException(400, "DiscountCanNotLowerThanTieredPriceDiscount");
        dbTieredPrice = new DbTieredPrice(11);
        dbPrice.tieredPriceSelect(dbTieredPrice);
        if (dbTieredPrice.discount < flowDiscount)
            throw new BaseException(400, "DiscountCanNotLowerThanTieredPriceDiscount");
        dbTieredPrice = new DbTieredPrice(21);
        dbPrice.tieredPriceSelect(dbTieredPrice);
        if (dbTieredPrice.discount < requestDiscount)
            throw new BaseException(400, "DiscountCanNotLowerThanTieredPriceDiscount");
        DbUserTieredDiscount dut = new DbUserTieredDiscount(owner.getId());
        dut.date = startTime;
        if (dbPrice.userTieredDiscountSelect(dut))
            throw new BaseException(400, "TieredPriceAlreadyExists");
        String endDate = TimeUtils.toYYYY_MM_dd(DateUtils.addMonths(
                DateParser.parseDate(startTime), (int) dbTieredPrice.expiryDate));
        dut.date = endDate;
        if (dbPrice.userTieredDiscountSelect(dut))
            throw new BaseException(400, "TieredPriceAlreadyExists");
        dut.storageDiscount = storageDiscount;
        dut.flowDiscount = flowDiscount;
        dut.requestDiscount = requestDiscount;
        dut.roamFlowDiscount = roamFlowDiscount;
        dut.roamUploadDiscount = roamUploadDiscount;
        dut.noNetReqDiscount = noNetReqDiscount;
        dut.noNetFlowDiscount = noNetFlowDiscount;
        dut.noNetRoamFlowDiscount = noNetRoamFlowDiscount;
        dut.noNetRoamUploadDiscount = noNetRoamUploadDiscount;
        dut.start = startTime;
        dut.end = endDate;
        dut.salerId = dbSalesman.id;
        dbPrice.userTieredDiscountInsert(dut);
    }
    
    public static void modifyTieredPriceDiscount(String orderId, double storageDiscount,
            double flowDiscount, double requestDiscount, String startTime, DbSalesman dbSalesman,
            Double roamFlowDiscount, Double roamUploadDiscount,
            Double noNetReqDiscount, Double noNetFlowDiscount,
            Double noNetRoamFlowDiscount, Double noNetRoamUploadDiscount)
            throws SQLException, BaseException, ParseException {
        DBPrice dbPrice = DBPrice.getInstance();
        DbUserTieredDiscount dbUserTieredDiscount = new DbUserTieredDiscount(orderId);
        dbPrice.userTieredDiscountSelectByOrderId(dbUserTieredDiscount);
        if (!isBelongTo(dbSalesman, dbUserTieredDiscount.usrId))
            throw new BaseException(403, "AccessDenied");
        DbTieredPrice dbTieredPrice = new DbTieredPrice(1);
        dbPrice.tieredPriceSelect(dbTieredPrice);
        if (dbTieredPrice.discount < storageDiscount)
            throw new BaseException(400, "DiscountCanNotLowerThanTieredPriceDiscount");
        dbTieredPrice = new DbTieredPrice(11);
        dbPrice.tieredPriceSelect(dbTieredPrice);
        if (dbTieredPrice.discount < flowDiscount)
            throw new BaseException(400, "DiscountCanNotLowerThanTieredPriceDiscount");
        dbTieredPrice = new DbTieredPrice(21);
        dbPrice.tieredPriceSelect(dbTieredPrice);
        if (dbTieredPrice.discount < requestDiscount)
            throw new BaseException(400, "DiscountCanNotLowerThanTieredPriceDiscount");
        DbUserTieredDiscount dut = new DbUserTieredDiscount(orderId);
        dbPrice.userTieredDiscountSelectByOrderId(dut);
        dut.date = startTime;
        if (dbPrice.userTieredDiscountSelect(dut) && !dut.orderId.equals(orderId))
            throw new BaseException(400, "TieredPriceAlreadyExists");
        String endDate = TimeUtils.toYYYY_MM_dd(DateUtils.addMonths(
                DateParser.parseDate(startTime), (int) dbTieredPrice.expiryDate));
        dut.date = endDate;
        if (dbPrice.userTieredDiscountSelect(dut) && !dut.orderId.equals(orderId))
            throw new BaseException(400, "TieredPriceAlreadyExists");
        dbUserTieredDiscount.storageDiscount = storageDiscount;
        dbUserTieredDiscount.flowDiscount = flowDiscount;
        dbUserTieredDiscount.requestDiscount = requestDiscount;
        dbUserTieredDiscount.roamFlowDiscount = roamFlowDiscount;
        dbUserTieredDiscount.roamUploadDiscount = roamUploadDiscount;
        dbUserTieredDiscount.noNetReqDiscount = noNetReqDiscount;
        dbUserTieredDiscount.noNetFlowDiscount = noNetFlowDiscount;
        dbUserTieredDiscount.noNetRoamFlowDiscount = noNetRoamFlowDiscount;
        dbUserTieredDiscount.noNetRoamUploadDiscount = noNetRoamUploadDiscount;
        dbUserTieredDiscount.start = startTime;
        dbUserTieredDiscount.end = endDate;
        dbUserTieredDiscount.salerId = dbSalesman.id;
        dbPrice.userTieredDiscountUpdate(dbUserTieredDiscount);
    }
    
    public static String getSalesman(DbSalesman dbSalesman, String organizationId)
            throws SQLException {
        if (dbSalesman.name.equals(MConsts.ADMIN))
            dbSalesman.organizationId = organizationId;
        XmlWriter xml = new XmlWriter();
        xml.start("Salesmans");
        if (dbSalesman.isManager == 0) {
            xml.start("Salesman");
            xml.start("Name").value(dbSalesman.name).end();
            xml.start("Email").value(dbSalesman.email).end();
            xml.start("Phone").value(dbSalesman.phone).end();
            xml.start("bestPayAccount").value(dbSalesman.bestPayAccount).end();
            xml.start("ID").value(dbSalesman.id).end();
            xml.end();
            xml.end();
            return xml.toString();
        } else {
            DBPrice dbPrice = DBPrice.getInstance();
            dbPrice.salesmanSelectByOrganizationId(dbSalesman, false);
            for (DbSalesman sm : dbSalesman.sms) {
                xml.start("Salesman");
                xml.start("Name").value(sm.name).end();
                xml.start("Email").value(sm.email).end();
                xml.start("Phone").value(sm.phone).end();
                xml.start("bestPayAccount").value(sm.bestPayAccount).end();
                xml.start("ID").value(sm.id).end();
                xml.end();
            }
            xml.end();
            return xml.toString();
        }
    }
}
