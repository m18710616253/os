package cn.ctyun.oos.server;

import java.io.IOException;
import java.time.DateTimeException;
import java.time.LocalDate;
import java.time.YearMonth;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang3.time.DateUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.server.nio.SelectChannelConnector;
import org.eclipse.jetty.server.ssl.SslSelectChannelConnector;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.amazonaws.HttpMethod;
import com.amazonaws.services.s3.Headers;

import cn.ctyun.common.BaseException;
import cn.ctyun.common.Consts;
import cn.ctyun.common.Program;
import cn.ctyun.common.conf.GlobalHHZConfig;
import cn.ctyun.common.conf.OOSConfig;
import cn.ctyun.common.dsync.DLock;
import cn.ctyun.common.dsync.DSyncService;
import cn.ctyun.common.node.OosZKNode;
import cn.ctyun.common.region.DataRegions;
import cn.ctyun.common.utils.LogUtils;
import cn.ctyun.oos.common.OOSPicAPIServerConsts;
import cn.ctyun.oos.common.Utils;
import cn.ctyun.oos.hbase.HBaseOwner;
import cn.ctyun.oos.hbase.HBasePicHistory;
import cn.ctyun.oos.hbase.MetaClient;
import cn.ctyun.oos.metadata.PicHistoryMeta;
import cn.ctyun.oos.server.conf.PicAPIServerConfig;
import cn.ctyun.oos.server.util.Misc;
import common.util.GetOpt;

/**
 * 资源信息查询API服务，PIC为Pool Information Collection缩写，提供查询各资源池容量、流量、请求次数、用户总数等功能。
 * @author WangJing
 *
 */
class PicAPIHttpHandler extends AbstractHandler {
    private static final Log log = LogFactory.getLog(PicAPIHttpHandler.class);
    protected final static DateTimeFormatter formatyyyy_mm_dd = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    protected final static DateTimeFormatter formatyyyy_mm = DateTimeFormatter.ofPattern("yyyy-MM");
    private static MetaClient client = MetaClient.getGlobalClient();
    //最近一次查询用户数请求时刻
    private static Date lastReqTime;
    //最近一次查询用户数请求的用户数
    private static int lastReqOwnerSize;

    public PicAPIHttpHandler() throws Exception {
    }

    @Override
    public void handle(String target, Request basereq, HttpServletRequest req,
            HttpServletResponse resp) throws ServletException, IOException {
        try {
            String reqid = Long.toHexString(UUID.randomUUID().getMostSignificantBits());
            req.setAttribute(Headers.REQUEST_ID, reqid);
            req.setAttribute(Headers.DATE, new Date());
            Utils.log(req, Utils.getIpAddr(req));
            Utils.checkDate(req);
            //验证签名
            checkAuth(basereq, req);
            if (req.getMethod().equalsIgnoreCase(HttpMethod.GET.toString())) {
                String action = req.getParameter("action");
                if (action != null && !action.equals("")) {
                    if (action.equals(OOSPicAPIServerConsts.PARAM_PICRESOURCEUSAGE)) {
                        Utils.writeResponseEntity(resp,
                                getPicResourceUsage(
                                        req.getParameter("fromDate"),
                                        req.getParameter("toDate")),
                                req);
                    } else if (action.equals(OOSPicAPIServerConsts.PARAM_PICOWNERNUMBER)) {
                        Utils.writeResponseEntity(resp, getPicOwnerNumber(), req);
                    } else
                        throw new BaseException(405, "MethodNotAllowed");
                } else
                    throw new BaseException(405, "MethodNotAllowed");
            } else
                throw new BaseException(405, "MethodNotAllowed");
        } catch (BaseException e) {
            log.error(e.getMessage(), e);
            resp.setContentType("text/xml");
            resp.setStatus(e.status);
            e.reqId = (String) req.getAttribute(Headers.REQUEST_ID);
            e.resource = req.getRequestURI();
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
            try {
                Utils.writeResponseEntity(resp, be.toXmlWriter().toString(), req);
            } catch (IOException e1) {
                log.error(e1.getMessage(), e1);
            }
        } finally {
            basereq.setHandled(true);
        }
    }

    // 查询各资源池用量信息
    private String getPicResourceUsage(final String fromDate, final String toDate) throws Exception {
        boolean isDay = checkDateValid(fromDate, toDate);
        int cacheNum = 2000;
        List<PicHistoryMeta> res = client.picHistoryList(isDay, fromDate, toDate, null, cacheNum);
        String result = buildPicHistoryJson(isDay, fromDate, toDate, res);
        return result;
    }

    // 生成json
    private String buildPicHistoryJson(boolean isDay, String fromDate, String toDate, List<PicHistoryMeta> phms) throws JSONException,Exception {
        JSONObject res = new JSONObject();
        List<String> dates = null;
        if (isDay) {
            dates = Utils.getDatesBetweenTwoDate(fromDate, toDate);
        } else {
            dates = Utils.getMonsBetweenTwoMon(fromDate, toDate);
        }
        List<String> regions = DataRegions.getAllRegions();
        JSONArray usageArr = new JSONArray();
        for (String date : dates) {
            //数据库中若没有那天的数据，返回json不显示那天的信息
            if (!checkPicHistoryHasDate(date,phms))
                continue;
            JSONObject dateUsage = new JSONObject();
            JSONArray regionUsageArr = new JSONArray();
            for (String region : regions) {
                for (PicHistoryMeta phm : phms) {
                    if (phm.regionName.equals(region) && phm.date.equals(date)) {
                        JSONObject picHistory = new JSONObject();
                        String regionChineseName = DataRegions.getRegionDataInfo(phm.regionName).getCHName();
                        picHistory.put("name", regionChineseName);
                        picHistory.put("totalCapacity", phm.totalCapacity);
                        picHistory.put("usedCapacity", phm.usedCapacity);
                        picHistory.put("upload", phm.upload);
                        picHistory.put("download", phm.download);
                        picHistory.put("getHeadRequest", phm.ghReq);
                        picHistory.put("otherRequest", phm.otherReq);
                        regionUsageArr.put(picHistory);
                        break;
                    }
                }
            }
            dateUsage.put("regionUsage", regionUsageArr);
            dateUsage.put("date", date);
            usageArr.put(dateUsage);
        }
        res.put("usage", usageArr);
        return res.toString();
    }
    
    /**
     * 检查picHistoryList是否有指定日期的数据
     * @param date
     * @param picHistoryList
     * @return
     */
    public static boolean checkPicHistoryHasDate(String date, List<PicHistoryMeta> picHistoryList) {
        if (picHistoryList.isEmpty())
            return false;
        for (PicHistoryMeta p:picHistoryList) {
            if (p.date.equals(date)) {
                return true;
            }
        }
        return false;
    }

    /**
     * 校验时间参数合法性，返回时间参数单位是否为天
     * @param fromDate
     * @param toDate
     * @return true查询某天，false查询某月
     * @throws BaseException
     */
    private boolean checkDateValid(final String fromDate, final String toDate) throws BaseException {
        if (fromDate == null || fromDate.trim().length() == 0 || toDate == null || toDate.trim().length() == 0)
            throw new BaseException(400, "InvalidArgument");
        String[] splitted = fromDate.split("-");
        boolean isDay = false;
        if (splitted.length == 3) {
            isDay = true;
        }
        //验证日期格式yyyy-MM-dd或yyyy-MM，且为正确日期
        checkValidDate(isDay, fromDate,toDate);
        if (!compareDate(fromDate, toDate, isDay)) {
            throw new BaseException(400, "InvalidArgument");
        }
        //目前不支持实时数据查询，由于hbase中数据没有当天或者当月，toDate不能包含当天或者当月
        if (isRealTime(toDate, isDay))
            throw new BaseException(400, "InvalidArgument");
        return isDay;
    }
    
    /**
     * 验证日期格式yyyy-MM-dd或yyyy-MM，且为正确日期
     * @param isDay
     * @param fromDate
     * @param toDate
     * @throws BaseException
     */
    private static void checkValidDate(boolean isDay, String fromDate, String toDate) throws BaseException {
        if (isDay) {
            try {
                LocalDate.parse(fromDate);
                LocalDate.parse(toDate);
            } catch (DateTimeException e) {
                throw new BaseException(400, "InvalidArgument");
            }
        } else {
            try {
                YearMonth.parse(fromDate, formatyyyy_mm);
                YearMonth.parse(toDate, formatyyyy_mm);
            } catch (DateTimeException e) {
                throw new BaseException(400, "InvalidArgument");
            }
        }       
    }

    /**
     * 检查toDate是否晚于fromDate
     * @param fromDate
     * @param toDate
     * @param isDay
     * @return
     */
    private static boolean compareDate(String fromDate, String toDate, Boolean isDay) {
        DateTimeFormatter dtf = null;
        if (isDay) {
            dtf = formatyyyy_mm_dd;
            LocalDate from = LocalDate.parse(fromDate, dtf);
            LocalDate to = LocalDate.parse(toDate, dtf);
            return to.compareTo(from) >= 0 ? true : false;
        } else {
            dtf = formatyyyy_mm;
            YearMonth from = YearMonth.parse(fromDate, dtf);
            YearMonth to = YearMonth.parse(toDate, dtf);
            return to.compareTo(from) >= 0 ? true : false;
        }
    }

    /**
     * 检查toDateStr是否为当天或当月
     * @param toDateStr
     * @param isDay
     * @return
     */
    private static boolean isRealTime(String toDateStr, boolean isDay) {
        String now = isDay ? LocalDate.now().toString() : YearMonth.now().toString();
        return compareDate(now,toDateStr,isDay);
    }
    
    /**
     * 查询用户总数，如果是频繁请求（半小时以内）就返回之前的值
     * @return
     * @throws JSONException
     * @throws Exception
     * @throws Throwable
     */
    private String getPicOwnerNumber() throws JSONException, Exception, Throwable {
        JSONObject ownerNumObj = new JSONObject();
        int totalNum = 0;
        synchronized (this) {
            if (checkIsFrequentReq()) {
                totalNum = lastReqOwnerSize;
            } else {
                totalNum = HBaseOwner.getOwnerSize(GlobalHHZConfig.getConfig());
                updateLastReqOwnerSize(totalNum);
            }
        }
        ownerNumObj.put("userNumber", totalNum);
        return ownerNumObj.toString();
    }
    
    /**
     * 检查是否为频繁请求（半小时以内），并更新最近一次请求时刻
     * @return
     */
    private static boolean checkIsFrequentReq() {
        Date now = new Date();        
        if (lastReqTime == null) {
            updateLastReqTime(now);
            return false;
        }
        //全表扫描间隔时间
        Date baseDate = DateUtils.addMinutes(lastReqTime, PicAPIServerConfig.getScanFullTableInterval());
        updateLastReqTime(now);
        if (baseDate.compareTo(now) == -1) {
            return false;
        } else {
            return true;
        }
    }
    
    private static void updateLastReqTime(Date date) {
        if (date != null)
            lastReqTime = date;
    }
    
    private static void updateLastReqOwnerSize(int num) {
        lastReqOwnerSize = num;
    }

    // 验证签名，其中SK从zk配置文件获取，签名加上了query参数
    public void checkAuth(Request basereq, HttpServletRequest req) throws Exception {
        String auth = req.getHeader("Authorization");
        if (auth == null || auth.length() == 0)
            throw new BaseException(403, "AccessDenied");
        String origId = null;
        String origSign = null;
        try {
            origId = Misc.getUserIdFromAuthentication(auth);
            origSign = auth.substring(auth.indexOf(':') + 1);
        } catch (Exception e) {
            throw new BaseException();
        }
        String secret = PicAPIServerConfig.getSecretKey(origId);
        if (secret == null)
            throw new BaseException(403, "AccessDenied");
        Utils.checkAuth2(origSign, secret, null, null, basereq, req, null);
    }
}

public class PicAPIServer implements Program {
    static {
        System.setProperty("log4j.log.app", "picAPIServer");
    }
    private static final Log log = LogFactory.getLog(PicAPIServer.class);

    public static void main(String[] args) throws Exception {
        new PicAPIServer().exec(args);
    }

    @Override
    public String usage() {
        return "Usage: PIC(Pool Information Collection) API Server \n";
    }

    @Override
    public void exec(String[] args) throws Exception {
        GetOpt opts = new GetOpt("[p]:[sslp]:", args);
        int port = opts.getInt("p", 9113);
        int sslPort = opts.getInt("sslp", 9476);
        DSyncService dsync = new DSyncService(GlobalHHZConfig.getQuorumServers(),
                GlobalHHZConfig.getSessionTimeout());    
        try {
            //picHistory表不存在的话，建表
            HBasePicHistory.createTable(GlobalHHZConfig.getConfig());
        } catch (Throwable t) {
            t.printStackTrace();
            System.exit(-1);
        }
        try {
            DLock lock = dsync.createLock(OosZKNode.picAPIServerLock, null);
            lock.lock();
            try {
                Server server = new Server();
                server.setSendServerVersion(false);
                SelectChannelConnector connector = new SelectChannelConnector();
                connector.setPort(port);
                connector.setMaxIdleTime(OOSConfig.getSocketlIdleTimeout());
                connector.setRequestHeaderSize(Consts.CONNECTOR_REQUEST_HEADER_SIZE);
                connector.setAcceptQueueSize(OOSConfig.getAcceptQueueSize());// 增加backlog
                final SslSelectChannelConnector ssl_connector = new SslSelectChannelConnector();
                ssl_connector.setPort(sslPort);
                ssl_connector.setMaxIdleTime(OOSConfig.getSocketlIdleTimeout());
                ssl_connector.setRequestHeaderSize(Consts.CONNECTOR_REQUEST_HEADER_SIZE);
                ssl_connector.setAcceptQueueSize(OOSConfig.getAcceptQueueSize());// 增加backlog
                SslContextFactory cf = ssl_connector.getSslContextFactory();
                cf.setKeyStorePath(System.getenv("OOS_HOME") + OOSConfig.getOosKeyStore());
                cf.setKeyStorePassword(OOSConfig.getOosSslPasswd());
                server.setConnectors(new Connector[] { connector, ssl_connector });
                server.setHandler(new PicAPIHttpHandler());
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
