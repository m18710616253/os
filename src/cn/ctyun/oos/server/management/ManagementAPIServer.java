package cn.ctyun.oos.server.management;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.text.ParseException;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

import javax.management.MBeanServer;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang3.time.DateUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpHeaders;
import org.eclipse.jetty.jmx.MBeanContainer;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.server.nio.SelectChannelConnector;
import org.eclipse.jetty.server.ssl.SslSelectChannelConnector;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.json.JSONObject;

import com.amazonaws.HttpMethod;
import com.amazonaws.services.s3.Headers;
import com.amazonaws.services.s3.internal.XmlWriter;

import cn.ctyun.common.BaseException;
import cn.ctyun.common.Consts;
import cn.ctyun.common.Program;
import cn.ctyun.common.conf.DataRegion;
import cn.ctyun.common.conf.GlobalHHZConfig;
import cn.ctyun.common.conf.OOSConfig;
import cn.ctyun.common.dsync.DLock;
import cn.ctyun.common.dsync.DSyncService;
import cn.ctyun.common.node.OosZKNode;
import cn.ctyun.common.utils.LogUtils;
import cn.ctyun.oos.common.AuthResult;
import cn.ctyun.oos.common.ErrorMessage;
import cn.ctyun.oos.common.Utils;
import cn.ctyun.oos.hbase.MetaClient;
import cn.ctyun.oos.iam.accesscontroller.AccessController;
import cn.ctyun.oos.iam.accesscontroller.AccessEffect;
import cn.ctyun.oos.iam.accesscontroller.RequestInfo;
import cn.ctyun.oos.metadata.BucketMeta;
import cn.ctyun.oos.metadata.CloudTrailEvent;
import cn.ctyun.oos.metadata.ManageEventMeta;
import cn.ctyun.oos.metadata.MinutesUsageMeta;
import cn.ctyun.oos.metadata.MinutesUsageMeta.UsageMetaType;
import cn.ctyun.oos.metadata.OwnerMeta;
import cn.ctyun.oos.server.conf.PeriodUsageStatsConfig;
import cn.ctyun.oos.server.signer.V4Signer;
import cn.ctyun.oos.server.usage.UsageResult;
import cn.ctyun.oos.server.util.Misc;
import common.util.GetOpt;

/**
 * 管理API
 * 
 * @author CuiMeng
 * 
 */
class MgAPIHttpHandler extends AbstractHandler {
    private static final Log log = LogFactory.getLog(MgAPIHttpHandler.class);
    private static MetaClient client;

    /** 访问控制 */
    private AccessController accessController = new AccessController();
    
    public MgAPIHttpHandler() throws Exception {
        client = MetaClient.getGlobalClient();
    }

    @Override
    public void handle(String target, Request basereq, HttpServletRequest req,
            HttpServletResponse resp) throws ServletException, IOException {
        ManageEventMeta manageEvent = null;
        try {
            String reqid = Long.toHexString(UUID.randomUUID().getMostSignificantBits());
            req.setAttribute(Headers.REQUEST_ID, reqid);
            req.setAttribute(Headers.DATE, new Date());
            Utils.log(req, Utils.getIpAddr(req));
            Utils.checkDate(req);
            AuthResult authResult = Utils.auth(basereq, req, null, null, false, true, V4Signer.S3_SERVICE_NAME);
            OwnerMeta owner = authResult.owner;
            if (owner == null) {
                throw new BaseException(403, ErrorMessage.ERROR_CODE_403);
            }
            if (!req.getMethod().equalsIgnoreCase(HttpMethod.GET.toString())) {
                throw new BaseException(405, ErrorMessage.ERROR_CODE_405);
            }

            String action = req.getParameter("Action");
            String beginDate = req.getParameter("BeginDate");
            String endDate = req.getParameter("EndDate");
            // 记录管理事件
            if (authResult.owner.ifRecordManageEvent) {
                manageEvent = new ManageEventMeta();
                CloudTrailEvent event = manageEvent.getEvent();
                // 固定参数
                event.eventSource = "oos-cn-mg.ctyunapi.cn";
                event.eventType = "ApiCall";
                event.serviceName = "Management";
                event.managementEvent = true;
                //用户参数
                if (authResult.isSts) {
                        event.userType = "STSUser";
                        event.accessKeyId = authResult.tokenMeta.stsAccessKey;
                    }else if (authResult.isRoot()) {
                    event.userType = "Root";
                    event.principalId = authResult.owner.getAccountId();event.arn = authResult.getUserArn();
                        event.accessKeyId = authResult.accessKey.accessKey;
                } else {
                    event.userType = "IAMUser";
                    event.userName = authResult.accessKey.userName;
                    event.principalId = authResult.accessKey.userId;

                event.arn = authResult.getUserArn();
                event.accessKeyId = authResult.accessKey.accessKey;
                    }event.ownerId = owner.getId();
                event.eventOwnerId = owner.getId();


                //请求的事件参数
                Date date = (Date) req.getAttribute(Headers.DATE);
                event.eventTime = date.getTime();
                event.requestId = (String) req.getAttribute(Headers.REQUEST_ID);
                event.userAgent = req.getHeader(HttpHeaders.USER_AGENT);
                event.sourceIpAddress = Utils.getIpAddr(req);
                event.requestRegion = Utils.getRegionNameFromReq(req, V4Signer.MANAGE_API_SERVICE_NAME);
                String protocol = req.getHeader("x-forwarded-proto");
                String requestURLString = req.getRequestURL().toString();
                int reqPort = req.getServerPort();
                requestURLString = requestURLString.replace(":" + reqPort, "");
                if (protocol != null && requestURLString.contains("https://")) {
                    requestURLString = requestURLString.replace("https://", protocol + "://");
                } else if (protocol != null && requestURLString.contains("http://")) {
                    requestURLString = requestURLString.replace("http://", protocol + "://");
                }
                event.requestURL = requestURLString;

                event.eventName = action;
                event.readOnly = true;
                JSONObject jo = new JSONObject();
                for (Entry<String, String[]> entry : req.getParameterMap().entrySet()) {
                    if (!entry.getKey().equalsIgnoreCase("action")) {
                        if (entry.getValue().length == 1)
                            jo.put(entry.getKey().toLowerCase(Locale.ROOT), entry.getValue()[0]);
                        else
                            jo.put(entry.getKey().toLowerCase(Locale.ROOT), entry.getValue());
                    }
                }
                event.requestParameters = jo.toString();
                event.responseElements = null;
                event.resources = null;
            }
            Common.checkParameter(action);
            Common.checkParameter(beginDate);
            Common.checkParameter(endDate);

            // 对非根用户进行访问控制
            if (!authResult.isRoot()) {
                String resource = "arn:ctyun:statistics::" + authResult.owner.getAccountId() + ":*";
                // 访问控制
                RequestInfo requestInfo = new RequestInfo("statistics:GetAccountStatistcsSummary", resource, authResult.owner.getAccountId(),
                        authResult.accessKey.userId, authResult.accessKey.userName, req);
                // 进行访问控制操作
                AccessEffect accessEffect = accessController.allow(requestInfo);
                // 拒绝或隐式拒绝
                if (accessEffect == AccessEffect.Deny || accessEffect == AccessEffect.ImplicitDeny) {
                    // 拒绝访问处理
                    throw new BaseException(403, "AccessDenied", requestInfo.getErrorMessage());
                }
            }

            if (resp.getHeader("Connection") == null)
                resp.setHeader("Connection", "close");
            resp.setContentType("application/xml");

            if (action.equals(MConsts.API_GET_USAGE)) {
                Utils.writeResponseEntity(resp, getUsage(owner, beginDate, endDate, req.getParameter("BucketName")), req);
            } else if (action.equals(MConsts.API_GET_BW)) {
                Utils.writeResponseEntity(resp, getBandwidth(owner, beginDate, endDate,
                        req.getParameter("BucketName")), req);
            } else if (action.equals(MConsts.API_GET_ABW)) {
                String reqPools = req.getParameter("Pools");
                Utils.writeResponseEntity(resp, getAvailBandwidth(owner, beginDate, endDate, reqPools), req);
            } else if (action.equals(MConsts.API_GET_CONNECTION)) {
                Utils.writeResponseEntity(resp, getConnection(owner, beginDate, endDate,
                        req.getParameter("BucketName")), req);
            } else if (action.equals(MConsts.API_GET_CAPACITY)) {
                Utils.writeResponseEntity(resp, UsageResult.getCapacityInXML(authResult, beginDate, endDate, req), req);
            } else if (action.equals(MConsts.API_GET_DELETE_CAPACITY)) {
                Utils.writeResponseEntity(resp, UsageResult.getDeleteCapacityInXML(authResult, beginDate, endDate, req), req);
            } else if (action.equals(MConsts.API_GET_TRAFFICS)) {
                Utils.writeResponseEntity(resp, UsageResult.getTrafficsInXML(authResult, beginDate, endDate, req), req);
            } else if (action.equals(MConsts.API_GET_AVAILABLE_BANDWIDTH)) {
                Utils.writeResponseEntity(resp, UsageResult.getAvailableBandwidthInXML(authResult, beginDate, endDate, req), req);
            } else if (action.equals(MConsts.API_GET_REQUEST)) {
                Utils.writeResponseEntity(resp, UsageResult.getRequestInXML(authResult, beginDate, endDate, req), req);
            } else if (action.equals(MConsts.API_GET_RETURNCODE)) {
                Utils.writeResponseEntity(resp, UsageResult.getReturnCodeInXML(authResult, beginDate, endDate, req), req);
            } else if (action.equals(MConsts.API_GET_CONCURRENT_CONNECTION)) {
                Utils.writeResponseEntity(resp, UsageResult.getConcurrentConnectionInXML(authResult, beginDate, endDate, req), req);
            } else if (action.equals(MConsts.API_GET_BILLED_STORAGE_USAGE)) {
                Utils.writeResponseEntity(resp, UsageResult.getBilledStorageUsageInXml(authResult, beginDate, endDate, req), req);
            } else if (action.equals(MConsts.API_GET_RESTORE_CAPACITY)) {
                Utils.writeResponseEntity(resp, UsageResult.getRestoreCapacityInXml(authResult, beginDate, endDate, req), req);
            }
            throw new BaseException(405, ErrorMessage.ERROR_CODE_405);
        } catch (BaseException e) {
            log.error(e.getMessage(), e);
            resp.setContentType("text/xml");
            resp.setStatus(e.status);
            e.reqId = (String) req.getAttribute(Headers.REQUEST_ID);
            e.resource = req.getRequestURI();
            if(manageEvent != null) {
                manageEvent.getEvent().errorStatus = e.status;
                manageEvent.getEvent().errorCode = e.code;
                manageEvent.getEvent().errorMessage = e.message;
            }
            try {
                Utils.writeResponseEntity(resp, e.toXmlWriter().toString(), req);
            } catch (IOException e1) {
                log.error(e1.getMessage(), e1);
            }
        } catch (IllegalArgumentException e) {
            log.error(e.getMessage(), e);
            BaseException be = new BaseException();
            be.status = 400;
            be.reqId = (String) req.getAttribute(Headers.REQUEST_ID);
            be.resource = req.getRequestURI();
            be.message = "Invalid Argument";
            be.code = "Invalid Argument";
            resp.setStatus(be.status);
            if(manageEvent != null) {
                manageEvent.getEvent().errorStatus = be.status;
                manageEvent.getEvent().errorCode = be.code;
                manageEvent.getEvent().errorMessage = be.message;
            }
            try {
                Utils.writeResponseEntity(resp, be.toXmlWriter().toString(), req);
            } catch (IOException e1) {
                log.error(e1.getMessage(), e1);
            }
        } catch (Throwable e) {
            log.error(e.getMessage(), e);
            BaseException be = new BaseException();
            be.status = 500;
            be.reqId = (String) req.getAttribute(Headers.REQUEST_ID);
            be.resource = req.getRequestURI();
            be.message = "Internal Error";
            be.code = "InternalError";
            resp.setContentType("text/xml");
            resp.setStatus(be.status);
            if(manageEvent != null) {
                manageEvent.getEvent().errorStatus = be.status;
                manageEvent.getEvent().errorCode = be.code;
                manageEvent.getEvent().errorMessage = be.message;
            }
            try {
                Utils.writeResponseEntity(resp, be.toXmlWriter().toString(), req);
            } catch (IOException e1) {
                log.error(e1.getMessage(), e1);
            }
        } finally {
            basereq.setHandled(true);
            if(manageEvent != null && manageEvent.getEvent().errorStatus != 405)
                try {
                    client.manageEventInsert(manageEvent);
                } catch (Exception e) {
                    log.error("cloudTrailServer error: insert manageEvent error!" + manageEvent.getRowKey());
                }
        }
    }

    /**
     * 查询使用量
     * @param owner
     * @param date
     * @param bucketName
     * @param freq
     * @return
     * @throws Exception
     */
    private String getUsage(final OwnerMeta owner, String begin, String end, String bucketName) throws Exception {
        XmlWriter xml = new XmlWriter();
        xml.start("GetUsageResult");
        xml.start("UserName").value(owner.name).end();
        UsageResult.checkBucketExist(owner.getId(), bucketName);
        if (bucketName != null && !bucketName.trim().isEmpty()) {
            xml.start("BucketName").value(bucketName).end();
        }
        
        Date beginDate = null;
        Date endDate = null;
        String freq = null;
        boolean isMinutes = false;
        //如果长度不相等，或者长度不等于10或16
        if (begin.length() != end.length() || (begin.length() != 10 && begin.length() != 16)) {
            throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_ARGUMENT, "Invalid BeginDate or EndDate.");
        }
        try {
            beginDate = Misc.formatyyyymmddhhmm2(begin);
            endDate = Misc.formatyyyymmddhhmm2(end);
            isMinutes = true;
        } catch (ParseException e) {
            try {
                beginDate = Misc.formatyyyymmdd(begin);
                endDate = Misc.formatyyyymmdd(end);
                isMinutes = false;
            } catch (ParseException e2) {
                throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_ARGUMENT, "Invalid BeginDate or EndDate.");
            }
        }
        if (isMinutes) {
            checkValidDate(beginDate, endDate);
            begin = Misc.formatyyyymmddhhmm(beginDate);
            end = Misc.formatyyyymmddhhmm(endDate);
            freq = Utils.BY_5_MIN;
        } else {
            // beginDate早于endDate
            if (beginDate.after(endDate))
                throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_ARGUMENT, "Invalid BeginDate and EndDate.");
            freq = Utils.BY_DAY;
        }
        
        Set<String> dataRegions = client.getDataRegions(owner.getId());
        dataRegions.add(Consts.GLOBAL_DATA_REGION);
        UsageMetaType type = UsageResult.getUsageStatsQueryType(bucketName, freq);
        Map<String, List<MinutesUsageMeta>> region2usage = UsageResult.getSingleCommonUsageStatsQuery(owner.getId(), begin, end, bucketName, freq, dataRegions, type, Consts.STORAGE_CLASS_STANDARD, true);
        buildUsageXML(xml, region2usage, freq, bucketName);
        xml.end();
        String str = Consts.XML_HEADER + xml.toString();
        return str;
    }
    
    /**
     * 查询已用带宽
     * @param owner
     * @param date
     * @param bucketName
     * @return
     * @throws Exception
     */
    private String getBandwidth(final OwnerMeta owner, String begin, String end, String bucketName) throws Exception {
        XmlWriter xml = new XmlWriter();
        xml.start("GetBandwidthResult");
        xml.start("UserName").value(owner.name).end();
        UsageResult.checkBucketExist(owner.getId(), bucketName);
        if (bucketName!=null && !bucketName.trim().isEmpty()) {
            xml.start("BucketName").value(bucketName).end();
        }
        Date beginDate = null;
        Date endDate = null;
        try {
            beginDate = Misc.formatyyyymmddhhmm2(begin);
            endDate = Misc.formatyyyymmddhhmm2(end);
            checkValidDate(beginDate, endDate);
            begin = Misc.formatyyyymmddhhmm(beginDate);
            end = Misc.formatyyyymmddhhmm(endDate);
        } catch (ParseException e) {
            throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_ARGUMENT, "Invalid BeginDate or EndDate.");
        }
        Set<String> dataRegions = client.getDataRegions(owner.getId());
        dataRegions.add(Consts.GLOBAL_DATA_REGION);
        UsageMetaType type = UsageResult.getUsageStatsQueryType(bucketName, Utils.BY_5_MIN);
        Map<String, List<MinutesUsageMeta>> region2usage = UsageResult.getSingleCommonUsageStatsQuery(owner.getId(), begin, end, bucketName, Utils.BY_5_MIN, dataRegions, type, Consts.STORAGE_CLASS_STANDARD, true);
        
        buildBandwidthXML(xml, region2usage);
        xml.end();
        String str = Consts.XML_HEADER + xml.toString();
        str = str.replaceAll("&quote;", "&quot;");
        return str;
    }

    /**
     * 查询剩余可用带宽
     * @param owner
     * @param date
     * @param reqPools
     * @return
     * @throws Exception
     */
    private String getAvailBandwidth(final OwnerMeta owner, String begin, String end, String reqPools) throws Exception {
        XmlWriter xml = new XmlWriter();
        xml.start("GetAvailBWResult");
        xml.start("UserName").value(owner.name).end();
        Date beginDate = null;
        Date endDate = null;
        try {
            beginDate = Misc.formatyyyymmddhhmm2(begin);
            endDate = Misc.formatyyyymmddhhmm2(end);
            checkValidDate(beginDate, endDate);
            begin = Misc.formatyyyymmddhhmm(beginDate);
            end = Misc.formatyyyymmddhhmm(endDate);
        } catch (ParseException e) {
            throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_ARGUMENT, "Invalid BeginDate or EndDate.");
        }
        String localPoolName = DataRegion.getRegion().getName();
        List<String> userPools = Utils.getABWPermissionAndTagRegions(owner.getId());
        // 用户未分配数据域tag
        if (userPools == null)
            throw new BaseException(403, ErrorMessage.ERROR_MESSAGE_AUTHORITY_NOT_MATCH);
        Set<String> userPermissionPools = new HashSet<String>();
        if (reqPools == null) {
            if (!userPools.contains(localPoolName))
                throw new BaseException(403, ErrorMessage.ERROR_MESSAGE_AUTHORITY_NOT_MATCH);
            // 没有Pools请求参数，默认返回本地资源池的信息（用户需要有查看本地资源池可用带宽权限）
            userPermissionPools.add(localPoolName);
        } else {
            String[] reqPoolsArr = reqPools.split(MConsts.API_POOLS_SPLIT);
            for (String p:reqPoolsArr) {
                if (userPools.contains(p))
                    userPermissionPools.add(p);
            }
            if (userPermissionPools.size() == 0) {
                throw new BaseException(403, ErrorMessage.ERROR_MESSAGE_AUTHORITY_NOT_MATCH);
            }
        }
        Map<String, List<MinutesUsageMeta>> region2Usages = UsageResult.getSingleAvailBandwidthUsageStatsQuery(owner.getId(), begin, end,  userPermissionPools, true);

        buildAvailBandwidthXML(xml, region2Usages);
        xml.end();
        String str = Consts.XML_HEADER + xml.toString();
        str = str.replaceAll("&quote;", "&quot;");
        return str;
    }

    /**
     * 查询账号级别或bucket级别连接数
     * @param owner
     * @param date
     * @param bucketName
     * @return
     * @throws Exception
     */
    private String getConnection(final OwnerMeta owner, String begin, String end, String bucketName) throws Exception {
        XmlWriter xml = new XmlWriter();
        xml.start("GetConnectionResult");
        xml.start("UserName").value(owner.name).end();
        UsageResult.checkBucketExist(owner.getId(), bucketName);
        if (bucketName!=null && !bucketName.trim().isEmpty()) {
            xml.start("BucketName").value(bucketName).end();
        }
        Date beginDate = null;
        Date endDate = null;
        try {
            beginDate = Misc.formatyyyymmddhhmm2(begin);
            endDate = Misc.formatyyyymmddhhmm2(end);
            checkValidDateForConnectionReq(beginDate, endDate);
            begin = Misc.formatyyyymmddhhmm(beginDate);
            end = Misc.formatyyyymmddhhmm(endDate);
        } catch (ParseException e) {
            throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_ARGUMENT, "Invalid BeginDate or EndDate.");
        }
        Set<String> dataRegions = client.getDataRegions(owner.getId());
        dataRegions.add(Consts.GLOBAL_DATA_REGION);
        UsageMetaType type = UsageResult.getUsageStatsQueryType(bucketName, Utils.BY_5_MIN);
        Map<String, List<MinutesUsageMeta>> usage = UsageResult.getSingleConnectionUsageStatsQuery(owner.getId(), begin, end, bucketName, dataRegions, type);

        buildConnectionXML(xml, usage);
        xml.end();
        String str = Consts.XML_HEADER + xml.toString();
        str = str.replaceAll("&quote;", "&quot;");
        return str;
    }

    private void buildUsageXML(XmlWriter xml, Map<String, List<MinutesUsageMeta>> usages, String freq, String bucket) throws IOException, ParseException {
        for (String region : usages.keySet()) {
            List<MinutesUsageMeta> metas = usages.get(region);
            xml.start("Region");
            xml.start("Name").value(region).end();
            for (MinutesUsageMeta usage : metas) {
                xml.start("Data");
                xml.start("Date").value(usage.time).end();
                usage.checkAndinitCommonStats();
                if (checkBucketIsNew(bucket)) {
                    // 返回给用户看的容量统一显示为归属地总容量,按天查询返回平均容量
                    if (freq.equals(Utils.BY_DAY)) {
                        xml.start("Capacity").value(String.valueOf(usage.sizeStats.size_avgTotal)).end();
                    } else {
                        xml.start("Capacity").value(String.valueOf(usage.sizeStats.size_total)).end();
                    }
                } else {
                    // 线上已有bucket不支持容量查询
                    xml.start("Capacity").value("-").end();
                }
                xml.start("Upload").value(String.valueOf(usage.flowStats.flow_upload)).end();
                xml.start("Download").value(String.valueOf(usage.flowStats.flow_download)).end();
                xml.start("DeleteFlow").value(String.valueOf(usage.flowStats.flow_delete)).end();
                xml.start("RoamUpload").value(String.valueOf(usage.flowStats.flow_roamUpload)).end();
                xml.start("RoamDownload").value(String.valueOf(usage.flowStats.flow_roamDownload)).end();
                xml.start("GetRequest").value(String.valueOf(usage.requestStats.req_get)).end();
                xml.start("HeadRequest").value(String.valueOf(usage.requestStats.req_head)).end();
                xml.start("PutRequest").value(String.valueOf(usage.requestStats.req_put)).end();
                xml.start("PostRequest").value(String.valueOf(usage.requestStats.req_post)).end();
                xml.start("DeleteRequest").value(String.valueOf(usage.requestStats.req_delete)).end();
                xml.start("OtherRequest").value(String.valueOf(usage.requestStats.req_other)).end();
                xml.start("NonInternetUpload").value(String.valueOf(usage.flowStats.flow_noNetUpload)).end();
                xml.start("NonInternetDownload").value(String.valueOf(usage.flowStats.flow_noNetDownload)).end();
                xml.start("NonInternetDeleteFlow").value(String.valueOf(usage.flowStats.flow_noNetDelete)).end();
                xml.start("NonInternetRoamUpload").value(String.valueOf(usage.flowStats.flow_noNetRoamUpload)).end();
                xml.start("NonInternetRoamDownload").value(String.valueOf(usage.flowStats.flow_noNetRoamDownload)).end();
                xml.start("NonInternetGetRequest").value(String.valueOf(usage.requestStats.req_noNetGet)).end();
                xml.start("NonInternetHeadRequest").value(String.valueOf(usage.requestStats.req_noNetHead)).end();
                xml.start("NonInternetPutRequest").value(String.valueOf(usage.requestStats.req_noNetPut)).end();
                xml.start("NonInternetPostRequest").value(String.valueOf(usage.requestStats.req_noNetPost)).end();
                xml.start("NonInternetDeleteRequest").value(String.valueOf(usage.requestStats.req_noNetDelete)).end();
                xml.start("NonInternetOtherRequest").value(String.valueOf(usage.requestStats.req_noNetOther)).end();
                xml.start("SpamRequest").value(String.valueOf(usage.requestStats.req_spam)).end();
                xml.start("PornReviewFalse").value(String.valueOf(usage.requestStats.req_pornReviewFalse)).end();
                xml.start("PornReviewTrue").value(String.valueOf(usage.requestStats.req_pornReviewTrue)).end();
                xml.end();
            }
            xml.end();
        }
    }

    private void buildBandwidthXML(XmlWriter xml, Map<String, List<MinutesUsageMeta>> usages) {
        for (String region : usages.keySet()) {
            List<MinutesUsageMeta> metas = usages.get(region);
            xml.start("Region");
            xml.start("Name").value(region).end();
            for (MinutesUsageMeta bw:metas) {
                xml.start("Data");
                xml.start("Date").value(bw.time).end();
                if (bw.bandwidthMeta != null) {
                    xml.start("UploadBW").value(String.valueOf(bw.bandwidthMeta.upload)).end();
                    xml.start("DownloadBW").value(String.valueOf(bw.bandwidthMeta.transfer)).end();
                    xml.start("RoamUploadBW").value(String.valueOf(bw.bandwidthMeta.roamUpload)).end();
                    xml.start("RoamDownloadBW").value(String.valueOf(bw.bandwidthMeta.roamFlow)).end();
                    xml.start("NonInternetUploadBW").value(String.valueOf(bw.bandwidthMeta.noNetUpload)).end();
                    xml.start("NonInternetDownloadBW").value(String.valueOf(bw.bandwidthMeta.noNetTransfer)).end();
                    xml.start("NonInternetRoamUploadBW").value(String.valueOf(bw.bandwidthMeta.noNetRoamUpload)).end();
                    xml.start("NonInternetRoamDownloadBW").value(String.valueOf(bw.bandwidthMeta.noNetRoamFlow)).end();
                } else {
                    xml.start("UploadBW").value("0").end();
                    xml.start("DownloadBW").value("0").end();
                    xml.start("RoamUploadBW").value("0").end();
                    xml.start("RoamDownloadBW").value("0").end();
                    xml.start("NonInternetUploadBW").value("0").end();
                    xml.start("NonInternetDownloadBW").value("0").end();
                    xml.start("NonInternetRoamUploadBW").value("0").end();
                    xml.start("NonInternetRoamDownloadBW").value("0").end();
                }
                xml.end();
            }
            xml.end();
        }
    }

    private void buildAvailBandwidthXML(XmlWriter xml, Map<String, List<MinutesUsageMeta>> region2Usages) {
        for (Entry<String, List<MinutesUsageMeta>> usageKV : region2Usages
                .entrySet()) {
            xml.start("Region");
            xml.start("Name").value(usageKV.getKey()).end();
            for (MinutesUsageMeta abw : usageKV.getValue()) {
                xml.start("Data");
                xml.start("Date").value(abw.time).end();
                if (abw.availableBandwidth != null) {
                    xml.start("AvailableUploadBW").value(String.valueOf(abw.availableBandwidth.pubInBW)).end();
                    xml.start("AvailableDownloadBW").value(String.valueOf(abw.availableBandwidth.pubOutBW)).end();
                    xml.start("NonInternetAvailableUploadBW").value(String.valueOf(abw.availableBandwidth.priInBW)).end();
                    xml.start("NonInternetAvailableDownloadBW").value(String.valueOf(abw.availableBandwidth.priOutBW)).end();
                } else {
                    xml.start("AvailableUploadBW").value("0").end();
                    xml.start("AvailableDownloadBW").value("0").end();
                    xml.start("NonInternetAvailableUploadBW").value("0").end();
                    xml.start("NonInternetAvailableDownloadBW").value("0").end();
                }
                xml.end();
            }
            xml.end();
        }
    }

    private void buildConnectionXML(XmlWriter xml, Map<String, List<MinutesUsageMeta>> usages) {
        for (String region : usages.keySet()) {
            List<MinutesUsageMeta> data = usages.get(region);
            xml.start("Region");
            xml.start("Name").value(region).end();
            for (MinutesUsageMeta d:data) {
                xml.start("Data");
                xml.start("Date").value(d.time).end();
                xml.start("Connection").value(String.valueOf(d.connection.connections + d.connection.noNet_connections)).end();
                xml.end();
            }
            xml.end();
        }
    }

    /**
     * 校验请求时间参数是否合法
     * @param beginDate
     * @param endDate
     * @param maxQueryDays 最多查询多少天以内的
     * @throws BaseException
     */
    private void checkValidDate(Date beginDate, Date endDate)
            throws BaseException {
        Date now = new Date();
        // endDate早于new Date
        if (endDate.after(now))
            throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_ARGUMENT, "Invalid EndDate.");
        // beginDate早于endDate
        if (beginDate.after(endDate))
            throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_ARGUMENT, "Invalid BeginDate and EndDate.");
        // beginDate最多为近一个月
        Date dateMin = DateUtils.addDays(now, -31);
        if (dateMin.compareTo(beginDate) == 1) {
            throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_ARGUMENT, "Invalid BeginDate.");
        }
        // beginDate为整5分钟时刻
        Calendar c1 = Calendar.getInstance();
        c1.setTime(beginDate);
        int minBegin = c1.get(Calendar.MINUTE);
        if (minBegin % Misc.FIVE_MINS != 0)
            throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_ARGUMENT, "Invalid BeginDate.");
        // endDate为整5分钟时刻
        Calendar c2 = Calendar.getInstance();
        c2.setTime(endDate);
        int minEnd = c2.get(Calendar.MINUTE);
        //需求文档 必须准5分钟
        if (minEnd % Misc.FIVE_MINS != 0)
            throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_ARGUMENT, "Invalid EndDate.");
    }
    
    /**
     * 校验获取连接数请求时间参数是否合法，beginDate和endDate之间时间间隔不能大于5分钟
     * @param beginDate
     * @param endDate
     * @throws BaseException
     */
    private void checkValidDateForConnectionReq(Date beginDate, Date endDate)
            throws BaseException {
        checkValidDate(beginDate, endDate);
        if (endDate.getTime() - beginDate.getTime() > 300000) {
            throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_ARGUMENT, "Invalid BeginDate and EndDate. Too large time gap.");
        }
    }

    
    /**
     * bucket创建时间晚于统计功能上线时间
     * @param isAccountLevel
     * @param bucketName
     * @throws IOException
     * @throws ParseException
     * @throws BaseException
     */
    private static boolean checkBucketIsNew(String bucketName) throws IOException, ParseException{
        if (bucketName != null && bucketName.trim().length() != 0) {
            BucketMeta bucket = new BucketMeta(bucketName);
            if (client.bucketSelect(bucket)) {
                if (bucket.createDate <= Misc.formatyyyymmddhhmm(PeriodUsageStatsConfig.deployDate+" 23:59").getTime()) {
                    return false;
                }
            }
        }
        return true;
    }
}

public class ManagementAPIServer implements Program {
    static {
        System.setProperty("log4j.log.app", "mgapi");
    }
    private static final Log log = LogFactory.getLog(ManagementAPIServer.class);

    public static void main(String[] args) throws Exception {
        new ManagementAPIServer().exec(args);
    }

    @Override
    public String usage() {
        return "Usage: \n";
    }

    @Override
    public void exec(String[] args) throws Exception {
        GetOpt opts = new GetOpt("[p]:[sslp]:", args);
        int port = opts.getInt("p", 9099);
        int sslPort = opts.getInt("sslp", 9462);
        DSyncService dsync = new DSyncService(GlobalHHZConfig.getQuorumServers(),
                GlobalHHZConfig.getSessionTimeout());
        try {
            DLock lock = dsync.createLock(OosZKNode.mgAPILock, null);
            lock.lock();
            try {
                QueuedThreadPool pool = new QueuedThreadPool();
                int maxThreads = OOSConfig.getMaxThreads();
                pool.setMaxQueued(maxThreads);
                pool.setMaxThreads(maxThreads);
                QueuedThreadPool sslPool = new QueuedThreadPool();
                sslPool.setMaxQueued(maxThreads);
                sslPool.setMaxThreads(maxThreads);
                /* http://wiki.eclipse.org/Jetty/Tutorial/Embedding_Jetty */
                SelectChannelConnector connector0 = new SelectChannelConnector();
                connector0.setAcceptQueueSize(OOSConfig.getAcceptQueueSize());// 增加backlog
                connector0.setThreadPool(pool);
                connector0.setPort(port);
                connector0.setMaxIdleTime(OOSConfig.getSocketlIdleTimeout());
                connector0.setRequestHeaderSize(Consts.CONNECTOR_REQUEST_HEADER_SIZE);
                SslSelectChannelConnector ssl_connector = new SslSelectChannelConnector();
                ssl_connector.setPort(sslPort);
                ssl_connector.setMaxIdleTime(OOSConfig.getSocketlIdleTimeout());
                ssl_connector.setRequestHeaderSize(Consts.CONNECTOR_REQUEST_HEADER_SIZE);
                ssl_connector.setAcceptQueueSize(OOSConfig.getAcceptQueueSize());// 增加backlog
                ssl_connector.setThreadPool(sslPool);
                SslContextFactory cf = ssl_connector.getSslContextFactory();
                cf.setKeyStorePath(System.getenv("OOS_HOME") + OOSConfig.getOosKeyStore());
                cf.setKeyStorePassword(OOSConfig.getOosSslPasswd());
                Server server = new Server();
                server.setThreadPool(new QueuedThreadPool(maxThreads)); // 增加最大线程数
                server.setConnectors(new Connector[] { connector0, ssl_connector });
                server.setHandler(new MgAPIHttpHandler());
                MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
                MBeanContainer mBeanContainer = new MBeanContainer(mBeanServer);
                server.getContainer().addEventListener(mBeanContainer);
                mBeanContainer.start();
                try {
                    server.start();
                    LogUtils.startSuccess();
                    server.join();
                } catch (Throwable e) {
                    log.error(e.getMessage(), e);
                    System.exit(-1);
                }
            } finally {
                lock.unlock();
            }
        } finally {
            dsync.close();
        }
    }
}
