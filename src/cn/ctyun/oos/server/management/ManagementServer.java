package cn.ctyun.oos.server.management;

import java.awt.*;
import java.awt.image.BufferedImage;
import java.awt.image.RenderedImage;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.text.ParseException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.imageio.ImageIO;
import javax.imageio.stream.ImageOutputStream;
import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.http.HttpEntity;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.BasicHttpEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.tsccm.ThreadSafeClientConnManager;
import org.apache.http.protocol.HTTP;
import org.apache.http.util.EntityUtils;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.server.nio.SelectChannelConnector;
import org.eclipse.jetty.server.ssl.SslSelectChannelConnector;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.jdom.JDOMException;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.amazonaws.HttpMethod;
import com.amazonaws.services.s3.Headers;
import com.amazonaws.services.s3.internal.XmlWriter;

import cn.ctyun.common.BaseException;
import cn.ctyun.common.Consts;
import cn.ctyun.common.Program;
import cn.ctyun.common.Session;
import cn.ctyun.common.conf.GlobalHHZConfig;
import cn.ctyun.common.conf.OOSConfig;
import cn.ctyun.common.dsync.DLock;
import cn.ctyun.common.dsync.DSyncService;
import cn.ctyun.common.node.OosZKNode;
import cn.ctyun.common.region.DataRegions;
import cn.ctyun.common.region.MetaRegions;
import cn.ctyun.common.utils.LogUtils;
import cn.ctyun.oos.bssAdapter.server.BssAdapterClient;
import cn.ctyun.oos.common.ErrorMessage;
import cn.ctyun.oos.common.OOSRequest;
import cn.ctyun.oos.common.Parameters;
import cn.ctyun.oos.common.Utils;
import cn.ctyun.oos.common.WebsiteHeader;
import cn.ctyun.oos.hbase.HBaseConnectionManager;
import cn.ctyun.oos.hbase.HBaseUserOstorWeight;
import cn.ctyun.oos.hbase.ManagementUser;
import cn.ctyun.oos.hbase.MetaClient;
import cn.ctyun.oos.iam.server.entity.AccountSummary;
import cn.ctyun.oos.iam.server.internal.api.IAMInternalAPI;
import cn.ctyun.oos.metadata.BucketMeta;
import cn.ctyun.oos.metadata.CdnVendorMeta;
import cn.ctyun.oos.metadata.DailySpaceMeta;
import cn.ctyun.oos.metadata.ManagerLogMeta;
import cn.ctyun.oos.metadata.ManagerLogMeta.Operation;
import cn.ctyun.oos.metadata.MinutesUsageMeta;
import cn.ctyun.oos.metadata.OwnerMeta;
import cn.ctyun.oos.metadata.Permission;
import cn.ctyun.oos.metadata.RoleMeta.RolePermission;
import cn.ctyun.oos.metadata.UserToTagMeta;
import cn.ctyun.oos.metadata.UserTypeMeta;
import cn.ctyun.oos.metadata.UserTypeMeta.UserType;
import cn.ctyun.oos.metadata.VSNTagMeta;
import cn.ctyun.oos.metadata.VSNTagMeta.VSNTagType;
import cn.ctyun.oos.server.conf.BssAdapterConfig;
import cn.ctyun.oos.server.conf.BssAdapterConfig.Pool;
import cn.ctyun.oos.server.conf.PeriodUsageStatsConfig;
import cn.ctyun.oos.server.db.dbpay.DBPay;
import cn.ctyun.oos.server.db.dbpay.DbAccount;
import cn.ctyun.oos.server.db.dbpay.DbPrice;
import cn.ctyun.oos.server.db.dbprice.DBPrice;
import cn.ctyun.oos.server.db.dbprice.DbSalesman;
import cn.ctyun.oos.server.db.dbprice.DbUserPackage;
import cn.ctyun.oos.server.signer.SignerUtils;
import cn.ctyun.oos.server.usage.UsageResult;
import cn.ctyun.oos.server.util.CSVUtils.UsageType;
import cn.ctyun.oos.server.util.DateParser;
import cn.ctyun.oos.server.util.Misc;
import cn.ctyun.oos.website.Portal;
import common.MimeType;
import common.io.ArrayOutputStream;
import common.io.FileCache;
import common.time.TimeUtils;
import common.tuple.Pair;
import common.util.ConfigUtils;
import common.util.GetOpt;
import common.util.MD5Hash;

/**
 * @author: Cui Meng
 */
public class ManagementServer implements Program {
    static {
        System.setProperty("log4j.log.app", "manage");
    }
    private static final Log log = LogFactory.getLog(ManagementServer.class);

    public static void main(String[] args) throws Exception {
        new ManagementServer().exec(args);
    }
    
    @Override
    public String usage() {
        return "usage /n";
    }
    
    @Override
    public void exec(String[] args) throws Exception {
        GetOpt opts = new GetOpt("[p]:[sslp]:", args);
        int port = opts.getInt("p", 9096);
        int sslPort = opts.getInt("sslp", 9459);
        CompositeConfiguration config, oosConfig = null;
        File[] xmlConfs = { new File(System.getenv("OOS_HOME") + "/conf/management-online.xml"),
                new File(System.getenv("OOS_HOME") + "/conf/management.xml") };
        config = (CompositeConfiguration) ConfigUtils.loadXmlConfig(xmlConfs);
        File[] oosXmlConfs = { new File(System.getenv("OOS_HOME") + "/conf/oos-online.xml"),
                new File(System.getenv("OOS_HOME") + "/conf/oos.xml") };
        oosConfig = (CompositeConfiguration) ConfigUtils.loadXmlConfig(oosXmlConfs);
        DSyncService dsync = new DSyncService(GlobalHHZConfig.getQuorumServers(),
                GlobalHHZConfig.getSessionTimeout());
        try {
            Configuration globalConf = GlobalHHZConfig.getConfig();
            HConnection globalConn = HBaseConnectionManager.createConnection(globalConf);
            HBaseAdmin globalHbaseAdmin = new HBaseAdmin(globalConn);
            HBaseUserOstorWeight.createTable(globalHbaseAdmin);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
        try {
            DLock lock = dsync.createLock(OosZKNode.managerServerLock, null);
            lock.lock();
            try {
                ManagementUser.init();
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
                cf.setKeyStorePath(
                        System.getenv("OOS_HOME") + oosConfig.getString("website.websiteKeyStore"));
                cf.setKeyStorePassword(oosConfig.getString("website.websiteSslPasswd"));
                server.setConnectors(new Connector[] { connector, ssl_connector });
                server.setHandler(new HttpHandler(config, oosConfig));
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

class HttpHandler extends AbstractHandler {
    private static final Log log = LogFactory.getLog(ManagementServer.class);
    
    private Session<String, String> session = new Session<String, String>(Consts.MAX_SESSION_TIME,
            null);
    private File webdir;
    private CompositeConfiguration config;
    private Date systemBeginDate;
    private HttpClient httpClient = null;
    static MetaClient client = MetaClient.getGlobalClient();
    private CompositeConfiguration oosConfig;

    public HttpHandler(CompositeConfiguration config,
            CompositeConfiguration oosConfig) throws Exception {
        this.webdir = new File(System.getenv("OOS_HOME")).getAbsoluteFile();
        this.config = config;
        this.oosConfig = oosConfig;
        httpClient = new DefaultHttpClient(new ThreadSafeClientConnManager());
        this.systemBeginDate = DateParser.parseDate(config.getString("systemBeginDate"));
        Admin.session = session;
        Salesman.session = session;
    }
    
    public void handle(String target, Request basereq, HttpServletRequest req,
            HttpServletResponse resp) throws IOException, ServletException {
        try {
            if (handleWebPages(target, basereq, req, resp))
                return;
            log(req);
            if (req.getParameter("verifySalesman") != null) {
                Salesman.verifySalesman(req.getParameter("verifySalesman"), resp);
                return;
            }
            if (handleVerifyCode(req, resp))
                return;
            if (handleUser(req, resp))
                return;
            if (handleUserBucket(req, resp))
                return;
            if (changeUser(req, resp))
                return;
            if (handleManagerLog(req, resp))
                return;
            if (handleCDNVendorList(req, resp))
                return;
            // 统计概览查询
            if (handleStatistics(req, resp)) {
                return;
            }
            if (handleUserStatistics(req, resp)) {
                return;
            }
            if (handlePrice(req, resp))
                return;
            if (handleUserPrice(req, resp))
                return;
            if (handleUserInfo(req, resp)) {
                return;
            }
            if (handleUserDataDeleteInfo(req, resp)) {
                resp.setHeader(Headers.CONTENT_TYPE, "application/json");
                return;
            }
            //处理查询资源池空间使用情况
            if (handlePoolDailySpaceQuery(target, req, resp)) {
                resp.setHeader(Headers.CONTENT_TYPE, "application/json");
                return;
            }
            if (handleUserDeposit(req, resp))
                return;
            if (req.getHeader(ManagementHeader.ADMIN) != null
                    || req.getParameter(ManagementHeader.ADMIN) != null) {
                //处理中心节点的请求
                if (req.getHeader("x-portaloos-modify-user")!=null) {
                    Admin.handleBssAdapterUserModify(req);
                    return;
                }
                //处理网站管理员的页面请求
                Admin.handleAdminRequest(req, resp);
                return;
            }
            if (req.getHeader(ManagementHeader.SALESMAN) != null) {
                //处理中心节点的请求
                if (req.getHeader("x-portaloos-get-user-usage")!=null) {
                    Salesman.handleBssGetUserUsage(req, resp);
                    return;
                }
                Salesman.handleSalesmanRequest(req, resp);
                return;
            }
            if (handleVSNTag(req, resp))
                return;
            if (handleUserToTag(req, resp))
                return;
            if (handlePermission(req, resp))
                return;
            if (handleRegions(req, resp))
                return;
            
            // 处理模块管理页面请求
            if (req.getHeader(ManagementHeader.MODULE_MANAGE) != null) {
                handleModuleManage(req, resp);
                return;
            }
            // 统计数据下载，目前仅支持CSV格式
            if (req.getParameter("usageDownloadFormat") != null) {
                handleUsageStatsDownload(req, resp);
                return;
            }
            
            if (req.getHeader(ManagementHeader.IAM_ACTION) != null) {
                HttpRequestBase httpReq = buildIAMInternalApiRequest(req);
                HttpHost httpHost = new HttpHost(OOSConfig.getIAMDomainSuffix(), oosConfig.getInt("website.iamPort"));
                preProcess(httpReq);
                HttpResponse newResponse = httpClient.execute(httpHost, httpReq);
                setResponse(resp, newResponse);
                return;
            }
        } catch (BaseException e) {
            log.error("Remote Addr:" + req.getRemoteAddr() + " User-Agent:"
                    + req.getHeader("User-Agent"));
            log.error(e.message, e);
            resp.setContentType("text/xml");
            resp.setStatus(e.status);
            e.reqId = "";
            e.resource = req.getRequestURI();
            try {
                resp.getWriter().write(e.toXmlWriter().toString());
            } catch (IOException e1) {
                log.error(e1.getMessage(), e1);
            }
        } catch (JSONException | IllegalArgumentException e) {
            log.error(e.getMessage(), e);
            BaseException be = new BaseException();
            be.status = 400;
            be.reqId = "";
            be.resource = req.getRequestURI();
            be.message = "Invalid Argument";
            be.code = "Invalid Argument";
            resp.setContentType("text/xml");
            resp.setStatus(be.status);
            try {
                resp.getWriter().write(be.toXmlWriter().toString());
            } catch (IOException e1) {
                log.error(e1.getMessage(), e1);
            }
        } catch (Throwable e) {
            log.error(e.getMessage(), e);
            BaseException be = new BaseException();
            be.status = 500;
            be.reqId = "";
            be.resource = req.getRequestURI();
            be.message = "Internal Error";
            be.code = "InternalError";
            resp.setContentType("text/xml");
            resp.setStatus(be.status);
            try {
                resp.getWriter().write(be.toXmlWriter().toString());
            } catch (IOException e1) {
                log.error(e1.getMessage(), e1);
            }
        } finally {
            basereq.setHandled(true);
        }
    }
    
    private void preProcess(HttpRequest request) {
        if (request == null) {
            throw new IllegalArgumentException("HTTP request may not be null");
        }
        if (request instanceof HttpEntityEnclosingRequest) {
            if (request.containsHeader(HTTP.TRANSFER_ENCODING)) {
                request.removeHeaders(HTTP.TRANSFER_ENCODING);
            }
            if (request.containsHeader(HTTP.CONTENT_LEN)) {
                request.removeHeaders(HTTP.CONTENT_LEN);
            }
        }
    }
    
    private boolean handleUserBucket(HttpServletRequest req,
            HttpServletResponse resp) throws IOException, BaseException, ParseException {
        if (req.getHeader(ManagementHeader.ADMIN) != null && 
                req.getHeader(ManagementHeader.ADMIN).equals("getBucketList")){
            long ownerId = Long.parseLong(req.getParameter("ownerId"));
            Common.checkParameter(ownerId);
            
            List<BucketMeta> bucketList = client.bucketList(ownerId);
            XmlWriter xml = new XmlWriter();
            xml.start("data");
            xml.start("bucketNames");
            for(BucketMeta bucket:bucketList) {
                xml.start("bucket").value(bucket.name).end();
            }
            xml.end();
            xml.start("newBucketNames");
            for(BucketMeta bucket:bucketList) {
                // bucket创建时间晚于统计功能上线时间
                if (bucket.createDate > Misc.formatyyyymmddhhmm(PeriodUsageStatsConfig.deployDate+" 23:59").getTime())
                    xml.start("bucket").value(bucket.name).end();
            }
            xml.end();
            xml.end();
            Common.writeResponseEntity(resp, xml.toString());
            return true;
        }
        return false;
    }

    private boolean handleVerifyCode(HttpServletRequest req,
            HttpServletResponse resp) throws IOException {
        if(req.getParameter(Parameters.GET_VERIFY_IMAGE)!=null
                && req.getParameter(Parameters.GET_VERIFY_IMAGE).equals("GET")) {
            String sessionId = req.getParameter(Parameters.IMAGE_SESSIONID);
            generateImage(resp,sessionId);
            resp.setStatus(200);
            return true;
        } else if(req.getHeader(WebsiteHeader.VERIFY_IMAGE) != null
                && req.getHeader(WebsiteHeader.VERIFY_IMAGE).equals("1")) {
            String sessionId = UUID.randomUUID().toString();
            session.remove(sessionId);
            resp.setHeader(Parameters.IMAGE_SESSIONID, sessionId);
            resp.setStatus(200);
            return true;
        }
        
        return false;
    }

    private void generateImage(HttpServletResponse resp, String sessionId) throws IOException {
        String randomStr = RandomStringUtils.randomAlphanumeric(4);
        resp.setHeader("Cache-Control", "no-cache");
        resp.setDateHeader("Expires", 0);
        resp.setContentType("image/png");
        resp.setHeader(Parameters.IMAGE_SESSIONID, sessionId);
        ImageOutputStream stream = null;
        try {
            stream = ImageIO.createImageOutputStream(resp.getOutputStream());
            ImageIO.write(create(Consts.VERIFY_IMAGE_WIDTH, Consts.VERIFY_IMAGE_HEIGHT,
                    randomStr), "PNG", stream);
        } finally {
            if (stream != null)
                stream.close();
        }
        session.update(sessionId, randomStr);
    }

    public static RenderedImage create(int w, int h, String code)
    {
      //画布  
      BufferedImage image = new BufferedImage(w, h, 1);
      Graphics g = image.getGraphics();
      g.setColor(new Color(14474460));
      g.fillRect(0, 0, w, h);
      g.setColor(Color.black);
      g.drawRect(0, 0, w - 1, h - 1);
      
      //画字
      int size = 26;
      g.setFont(new Font("Axure Handwriting", 0, size));
      for(int i=0;i<code.length();i++) {
          g.drawChars(new char[]{code.charAt(i)}, 0, 1, 5+i*20, (h + size) / 2 - 3);
      }
      
      //画扰乱字体
      Random random = new Random();
      for (int i = 0; i < 30; i++)
      {
        int x = random.nextInt(w);
        int y = random.nextInt(h);
        g.drawLine(x, y, x + random.nextInt(4), y + random.nextInt(4));
      }
      g.dispose();

      return image;
    }

    /**
     * 处理权限相关请求
     * @param req
     * @param resp
     * @return
     * @throws BaseException
     * @throws JSONException
     * @throws IOException
     * @author wushuang
     * @throws JDOMException 
     */
    private boolean handlePermission(HttpServletRequest req,HttpServletResponse resp)
            throws BaseException, JSONException, IOException, JDOMException {
        if (req.getHeader(ManagementHeader.PERMISSIONS) != null) {          
            if (req.getMethod().equalsIgnoreCase(HttpMethod.GET.toString())) {
                XmlWriter xml = new XmlWriter();
                xml.start("Permissions");
                xml.start("Permission");
                xml.start("Type").value(RolePermission.PERMISSION_AVAIL_BW.toString()).end();
                xml.start("ChName").value(RolePermission.PERMISSION_AVAIL_BW.getCNName()).end();
                List<String> scopes = getScope(RolePermission.PERMISSION_AVAIL_BW);
                if (scopes != null && scopes.size()>0) {
                    xml.start("Scopes");
                    for (String scope : scopes) {
                        xml.start("Scope").value(scope).end();
                    }
                    xml.end();    
                }
                xml.end();
                xml.end();
                Common.writeResponseEntity(resp, xml.toString()); 
            }
            return true;
        } else
            return false;
    }
    
    /**
     * 根据权限类型获取对应region可选范围
     * @param type
     * @return
     * @author wushuang
     * @throws JDOMException 
     * @throws IOException 
     * @throws BaseException 
     */
    private List<String> getScope(RolePermission type) throws BaseException, IOException, JDOMException { 
        switch (type) {
        case PERMISSION_AVAIL_BW:
            return DataRegions.getAllRegions();
        case PERMISSION_AVAIL_DATAREGION:
            List<Pair<String, Pool>> poolList = BssAdapterClient.getAvailablePoolsAsList(
                    BssAdapterConfig.localPool.ak,BssAdapterConfig.localPool.getSK());
            
            return poolList.parallelStream().map(new Function<Pair<String, Pool>, String>() {
                public String apply(Pair<String, Pool> t) {
                    return t.first();
                }
            }).collect(Collectors.toList());
        default:
            return null;
        }
    }

    /**
     * 处理模块管理页面相关请求
     * @param req
     * @param resp
     * @return
     * @throws IOException 
     * @throws BaseException 
     */
    private void handleModuleManage(HttpServletRequest req,HttpServletResponse resp) throws IOException, BaseException {
        // 存储桶管理
        if (req.getHeader(ManagementHeader.MODULE_MANAGE).equals("bucketManage"))
            handleBucketManage(req, resp);
    }
    
    /**
     * 处理存储桶管理请求，存储桶管理请求中参数manageType包含请求的管理类型，用","分隔
     * @param req
     * @param resp
     * @throws IOException 
     * @throws BaseException 
     */
    private static void handleBucketManage(HttpServletRequest req,HttpServletResponse resp) throws IOException, BaseException {
        // manageType中必须包含配置参数,[bucketNumCeiling]
        String manageType = req.getParameter("manageType");
        Common.checkParameter(manageType);
        List<String> types = Arrays.asList(manageType.split(","));
        if (req.getMethod().equalsIgnoreCase(HttpMethod.PUT.toString())) {
            //修改某种用户类型的存储桶上限数量
            if(types.contains("bucketNumCeiling")) {
                Common.checkParameter(req.getParameter("userTypeId"));
                Common.checkParameter(req.getParameter("ceiling"));
                int userTypeId = Integer.parseInt(req.getParameter("userTypeId"));
                int ceiling = Integer.parseInt(req.getParameter("ceiling"));
                UserTypeMeta meta = new UserTypeMeta(userTypeId);
                if(meta.getUserType() == null)
                    throw new BaseException(404, "NoSuchUserType");
                meta.getBucketNumCeiling().set(ceiling, Misc.formatyyyymmddhhmm(new Date()), "");
                client.userTypeInsert(meta);                
            }
        } else if (req.getMethod().equalsIgnoreCase(HttpMethod.GET.toString())) {
            //获取全部用户类型及其存储桶上限
            Map<String, UserTypeMeta> map = client.userTypeListAll();
            for (UserType type : UserType.values()) {
                UserTypeMeta meta = new UserTypeMeta(type.getIntValue());
                if(map.get(String.valueOf(meta.getUserType().getIntValue())) == null)
                    map.put(String.valueOf(type.getIntValue()), meta);
            }
            XmlWriter xml = new XmlWriter();
            xml.start("bucketManage");
            xml.start("userType");
            for (Entry<String, UserTypeMeta> v : map.entrySet()) {
                UserTypeMeta userTypeMeta = v.getValue();
                xml.start("data");
                xml.start("id").value(String.valueOf(userTypeMeta.getUserType().getIntValue())).end();
                xml.start("name").value(userTypeMeta.getUserType().getDesc()).end();
                if(types.contains("bucketNumCeiling")) {
                    xml.start("bucketNumCeiling");
                    xml.start("ceiling").value(String.valueOf(userTypeMeta.getBucketNumCeiling().ceiling)).end();
                    xml.start("lastModifyTime").value(userTypeMeta.getBucketNumCeiling().lastModifyTime).end();
                    xml.start("modifier").value(userTypeMeta.getBucketNumCeiling().modifier).end();
                    xml.end();
                }
                xml.end();
            }
            xml.end();
            xml.end();
            Common.writeResponseEntity(resp, xml.toString());
        }
    }
    
    private boolean handleUserToTag(HttpServletRequest req, HttpServletResponse resp)
            throws Exception {
        if (req.getHeader(ManagementHeader.USER_TO_TAG) != null) {
            if (req.getMethod().equalsIgnoreCase(HttpMethod.PUT.toString())) {
                Common.checkParameter(req.getParameter(Parameters.USER_NAME));
                String userName = req.getParameter(Parameters.USER_NAME);
                OwnerMeta owner = new OwnerMeta(userName);
                if (!client.ownerSelect(owner))
                    throw new BaseException(404, "NoSuchUser");
                Common.checkParameter(req.getParameter(Parameters.META_TAG_NAME));
                String[] metaTagNames = req.getParameter(Parameters.META_TAG_NAME).split(",");
                Common.checkParameter(req.getParameter(Parameters.DATA_TAG_NAME));
                String[] dataTagNames = req.getParameter(Parameters.DATA_TAG_NAME).split(",");
                // 检查tagName的合法性
                List<String> tagNames = VSNTagMeta
                        .getTagNames(client.vsnTagSelectAll(VSNTagType.META));
                for (String tag : metaTagNames)
                    if (!tagNames.contains(tag))
                        throw new BaseException(400, "InvalidTag");
                tagNames = VSNTagMeta.getTagNames(client.vsnTagSelectAll(VSNTagType.DATA));
                for (String tag : dataTagNames)
                    if (!tagNames.contains(tag))
                        throw new BaseException(400, "InvalidTag");
                
                UserToTagMeta metaUserToTag = new UserToTagMeta(VSNTagType.META, owner.getId());
                UserToTagMeta dataUserToTag = new UserToTagMeta(VSNTagType.DATA, owner.getId());
                client.userToTagDelete(metaUserToTag,dataUserToTag);
                metaUserToTag.getTagName().clear();
                metaUserToTag.getTagName().addAll(Arrays.asList(metaTagNames));
                List<String> newDataTagNames = Arrays.asList(dataTagNames);
                dataUserToTag.getTagName().clear();
                dataUserToTag.getTagName().addAll(newDataTagNames);
                client.userToTagInsert(metaUserToTag,dataUserToTag);                
            } else if (req.getMethod().equalsIgnoreCase(HttpMethod.GET.toString())) {
                if (req.getParameter(Parameters.USER_NAME) != null) {
                    String userName = req.getParameter(Parameters.USER_NAME);
                    OwnerMeta owner = new OwnerMeta(userName);
                    if (!client.ownerSelect(owner))
                        throw new BaseException(404, "NoSuchUser");
                    UserToTagMeta userToTag = new UserToTagMeta(VSNTagType.META, owner.getId());
                    client.userToTagSelect(userToTag);
                    List<String> metaTagNames = userToTag.getTagName();
                    userToTag = new UserToTagMeta(VSNTagType.DATA, owner.getId());
                    client.userToTagSelect(userToTag);
                    List<String> dataTagNames = userToTag.getTagName();
                    if (metaTagNames.isEmpty() && dataTagNames.isEmpty())
                        throw new BaseException(404, "UserNoTag");
                    XmlWriter xml = new XmlWriter();
                    xml.start("UserToTags");
                    xml.start("MetaTagNames");
                    for (String t : metaTagNames) {
                        xml.start("MetaTagName").value(t).end();
                    }
                    xml.end();
                    xml.start("DataTagNames");
                    for (String t : dataTagNames) {
                        xml.start("DataTagName").value(t).end();
                    }
                    xml.end();
                    xml.end();
                    Common.writeResponseEntity(resp, xml.toString());
                } else if (req.getParameter(Parameters.TAG_NAME) != null) {
                    String tagName = req.getParameter(Parameters.TAG_NAME);
                    VSNTagType type = null;
                    if (req.getParameter(Parameters.VSNTAG_TYPE) != null)
                        type = VSNTagType.valueOf(req.getParameter(Parameters.VSNTAG_TYPE));
                    else
                        throw new BaseException();
                    String startOwnerName = req.getParameter(Parameters.START_OWNER_NAME);
                    String startOwnerId = (startOwnerName == null) ? ""
                            : String.valueOf(new OwnerMeta(startOwnerName).getId());
                    int maxKeys = MConsts.MAX_LIST_SIZE;
                    List<UserToTagMeta> userToTags = client.userToTagScanByTag(type, tagName,
                            startOwnerId, maxKeys + 1);
                    XmlWriter xml = new XmlWriter();
                    xml.start("UserToTags");
                    xml.start("UserNames");
                    for (int i = 0; i < Math.min(userToTags.size(), maxKeys); i++) {
                        OwnerMeta owner = new OwnerMeta(userToTags.get(i).getOwnerId());
                        if (client.ownerSelectById(owner))
                            xml.start("UserName").value(owner.getName()).end();
                    }
                    xml.end();
                    xml.start("Truncated").value(userToTags.size() > maxKeys ? "true" : "false")
                            .end();
                    if (userToTags.size() > maxKeys) {
                        OwnerMeta owner = new OwnerMeta(
                                userToTags.get(userToTags.size() - 1).getOwnerId());
                        if (client.ownerSelectById(owner))
                            xml.start("NextMarker").value(owner.getName()).end();
                    }
                    xml.end();
                    Common.writeResponseEntity(resp, xml.toString());
                }
            }
            return true;
        } else
            return false;
    }

    private boolean handleVSNTag(HttpServletRequest req, HttpServletResponse resp)
            throws BaseException, IOException {
        if (req.getHeader(ManagementHeader.VSN_TAG) != null) {
            VSNTagType type = null;
            if (req.getParameter(Parameters.VSNTAG_TYPE) != null)
                type = VSNTagType.valueOf(req.getParameter(Parameters.VSNTAG_TYPE));
            else
                throw new BaseException();
            if (req.getMethod().equalsIgnoreCase(HttpMethod.PUT.toString())) {
                Common.checkParameter(req.getParameter(Parameters.TAG_NAME));
                String tagName = req.getParameter(Parameters.TAG_NAME);
                Common.checkParameter(req.getParameter(Parameters.REGION_NAME));
                String[] regionName = req.getParameter(Parameters.REGION_NAME).split(",");
                // 检查tag的合法性
                if (tagName.length() > Consts.TAG_NAME_MAX_LENGTH
                        || !StringUtils.isAlphanumeric(tagName) || tagName.equals("v"))// 不能等于默认的column名
                    throw new BaseException(400, "InvalidTag");
                // 检查region的合法性
                for (String r : regionName) {
                    switch (type) {
                    case DATA:
                        if (!DataRegions.getAllRegions().contains(r))
                            throw new BaseException(400, "InvalidRegion");
                        break;
                    case META:
                        if (!MetaRegions.getAllRegions().contains(r))
                            throw new BaseException(400, "InvalidRegion");
                        break;
                    }
                }
                VSNTagMeta tag = new VSNTagMeta(type, tagName);
                client.vsnTagDelete(tag);
                tag.getRegionName().addAll(Arrays.asList(regionName));
                client.vsnTagInsert(tag);
            } else if (req.getMethod().equalsIgnoreCase(HttpMethod.GET.toString())) {
                if (req.getParameter(Parameters.REGION_NAME) == null
                        && req.getParameter(Parameters.TAG_NAME) == null) {
                    List<VSNTagMeta> allRegions = client.vsnTagSelectAll(type);
                    XmlWriter xml = new XmlWriter();
                    xml.start("VSNTags");
                    for (VSNTagMeta tag : allRegions) {
                        xml.start("VSNTag");
                        xml.start("TagName").value(tag.getTagName()).end();
                        StringBuilder region = new StringBuilder();
                        for (String r : tag.getRegionName())
                            region.append(r).append(",");
                        xml.start("RegionName").value(
                                region.toString().substring(0, region.toString().length() - 1))
                                .end();
                        xml.end();
                    }
                    xml.end();
                    Common.writeResponseEntity(resp, xml.toString());
                } else if (req.getParameter(Parameters.REGION_NAME) != null) {
                    String regionName = req.getParameter(Parameters.REGION_NAME);
                    List<VSNTagMeta> tags = client.vsnTagScanByRegion(type, regionName);
                    XmlWriter xml = new XmlWriter();
                    xml.start("VSNTags");
                    for (VSNTagMeta t : tags) {
                        xml.start("TagName").value(t.getTagName()).end();
                    }
                    xml.end();
                    Common.writeResponseEntity(resp, xml.toString());
                }
            } else if (req.getMethod().equalsIgnoreCase(HttpMethod.DELETE.toString())) {
                Common.checkParameter(req.getParameter(Parameters.TAG_NAME));
                String tagName = URLDecoder.decode(
                                    req.getParameter(Parameters.TAG_NAME), Consts.STR_UTF8);
                VSNTagMeta tag = new VSNTagMeta(type, tagName);
                if (client.userToTagScanByTag(type, tagName, "", 1).size() > 0)
                    throw new BaseException(400, "TagAlreadyUsed");
                client.vsnTagDelete(tag);
            }
            return true;
        } else
            return false;
    }

    private boolean handleRegions(HttpServletRequest req, HttpServletResponse resp)
            throws BaseException, IOException {
        if (req.getHeader(ManagementHeader.REGION) != null) {
            VSNTagType type = null;
            if (req.getParameter(Parameters.VSNTAG_TYPE) != null)
                type = VSNTagType.valueOf(req.getParameter(Parameters.VSNTAG_TYPE));
            else
                throw new BaseException();
            if (req.getMethod().equalsIgnoreCase(HttpMethod.GET.toString())) {
                
                XmlWriter xml = new XmlWriter();
                xml.start("Regions");
                switch (type) {
                case DATA:
                    for (String r : DataRegions.getAllRegions())
                        xml.start("RegionName").value(r).end();
                    break;
                case META:
                    for (String r : MetaRegions.getAllRegions())
                        xml.start("RegionName").value(r).end();
                    break;
                }
                xml.end();
                Common.writeResponseEntity(resp, xml.toString());
            }
            
            return true;
        }
        return false;
    }
    
    private void handleUsageStatsDownload(HttpServletRequest req,
            HttpServletResponse resp) throws Exception {
        long ownerId = Long.parseLong(req.getParameter("ownerId"));
        String beginDate = req.getParameter("beginDate");
        String endDate = req.getParameter("endDate");
        String bucketName = req.getParameter("bucketName");
        String freq = req.getParameter("freq");
        String format = req.getParameter("usageDownloadFormat");
        String file = req.getParameter("downloadFileName");
        String type = req.getParameter("downloadType");
        String needAllRegionUsage = req.getParameter("needAllRegionUsage");
        String storageType = Utils.checkAndGetStorageType(req.getParameter("storageClass"));
        if (Consts.ALL.equals(storageType)) {
            // 下载数据不支持所有存储类型下载
            throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_ARGUMENT, "Invalid StorageClass.");
        }
        // beginDate、endDate参数为必填项
        boolean validDateParam = beginDate != null && beginDate.trim().length() != 0
                && endDate != null && endDate.trim().length() != 0;
        if (!validDateParam) {
            throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_ARGUMENT);
        }
        Utils.checkParameter(file);
        Utils.checkParameter(type);
        if (!Utils.CSV.equals(format)) {
            throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_ARGUMENT, "Invalid usageDownloadFormat.");
        }
        UsageType usageType = Utils.checkAndGetUsageStatsTypeForDownload(type);

        //带宽95峰值统计下载
        if (usageType.equals(UsageType.PEAK_BANDWIDTH_95)) {
            // beginDate、endDate格式为yyyy-MM
            if (Utils.monthFormatIsValid(endDate) && Utils.monthFormatIsValid(beginDate)) {
                try {
                    Date tmpEndDate = Misc.formatyyyymm(endDate);
                    Date tmpBeginDate = Misc.formatyyyymm(beginDate);
                    if (tmpEndDate.before(tmpBeginDate))
                        throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_ARGUMENT);
                } catch (ParseException e) {
                    throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_ARGUMENT);
                }
            } else {
                throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_ARGUMENT);
            }
            String body = UsageResult.get95PeakBWStatsInCsv(ownerId, beginDate, endDate, usageType, bucketName, Consts.STORAGE_CLASS_STANDARD);
            Common.writeResponseEntity(resp, body, format, file);
            return;
        }

        // 统计项下载
        // beginDate、endDate格式为yyyy-MM-dd
        Utils.checkUsageReqParamsValid(beginDate, endDate, freq);
        // 所有数据域
        Set<String> allRegions = new HashSet<>(DataRegions.getAllRegions());
        // 用户有标签的数据域
        Set<String> dataRegions = client.getDataRegions(ownerId);
        //// 此处添加global的region没有带到后面的方法中去，影响最后的csv文件没有global相关数据，无其他影响。 todo 是否修改待定。
        //List<String> dataRegionList = new ArrayList<>(dataRegions);
        dataRegions.add(Consts.GLOBAL_DATA_REGION);

        // 有数据的
        List<String> hasDataRegions = new ArrayList<>();
        String today = LocalDate.now().format(Misc.format_uuuu_MM_dd);
        Map<String, List<MinutesUsageMeta>> data = UsageResult.getSingleCommonUsageStatsQuery(ownerId, today, today, null,
                Utils.BY_DAY, allRegions, MinutesUsageMeta.UsageMetaType.DAY_OWNER, Consts.ALL, true);
        data.forEach((region,list)->{
            if (CollectionUtils.isNotEmpty(list)) {
                hasDataRegions.add(region);
            }
        });
        dataRegions.addAll(hasDataRegions);

        String body = UsageResult.getSingleUsageInCsv(ownerId, beginDate, endDate, bucketName, freq, usageType, dataRegions, storageType, true, BooleanUtils.toBoolean(needAllRegionUsage));
        Common.writeResponseEntity(resp, body, format, file);
    }
    
    private boolean changeUser(HttpServletRequest req, HttpServletResponse resp)
            throws BaseException, IOException {
        if (req.getHeader(ManagementHeader.CHANGE_USER) != null) {
            String userName = req.getParameter(Parameters.USER_NAME);
            String password = req.getParameter(Parameters.PASSWORD);
            if (userName == null || password == null || userName.trim().length() == 0
                    || password.trim().length() == 0)
                throw new BaseException();
            password = MD5Hash.digest(password).toString();
            ManagementUser user = new ManagementUser(userName, password);
            client.managementUserPut(user);
            session.update(Utils.getSessionId(req), userName);
            return true;
        }
        return false;
    }
    
    private boolean handleManagerLog(HttpServletRequest req, HttpServletResponse resp)
            throws BaseException {
        if (req.getHeader(ManagementHeader.GET_MANAGER_LOG) != null) {
            try {
                List<String> managerLog = client.managerLogList();
                resp.getOutputStream().write(managerLog.toString().getBytes(Consts.STR_UTF8));
            } catch (Exception e) {
                log.error(e.getMessage(), e);
                throw new BaseException(500, "Server Error");
            }
            return true;
        }
        return false;
    }
    
    private boolean handleCDNVendorList(HttpServletRequest req, HttpServletResponse resp)
            throws IOException,BaseException, JSONException {
        if (req.getHeader(ManagementHeader.GET_CDN_LIST) != null) {
            try {
                String vendorName = req.getParameter("vendorname");
                List<CdnVendorMeta> vendorList = null;
                
                if (vendorName != null && vendorName.length()>0){
                    Common.checkParameter(vendorName.trim());
                    CdnVendorMeta vendor = client.vendorSelect(vendorName.trim());
                    if (vendor != null){
                        vendorList = new ArrayList<>();
                        vendorList.add(vendor);
                    }
                } else {
                    vendorList = client.vendorList();
                }
                
                String vendorXml = vendorXmlString(vendorList).toString();
                
                resp.getOutputStream().write(vendorXml.getBytes(Consts.STR_UTF8));
            } catch (BaseException e) {
                log.error(e.getMessage(), e);
                throw new BaseException(403, "NoVendorInformation");
            } catch (JSONException e) {
                log.error(e.getMessage(), e);
                throw new BaseException(500, "Server Error");
            } catch (Exception e) {
                log.error(e.getMessage(), e);
                throw new BaseException(500, "Server Error");
            }
            return true;
        } else if (req.getHeader(ManagementHeader.SET_CDN_LIST) != null ||
                req.getHeader(ManagementHeader.ADD_CDN_LIST) != null) {
            String vendorName = req.getParameter("vendorname");
            String iplist = req.getParameter("iplist");
            boolean addData  = req.getHeader(ManagementHeader.ADD_CDN_LIST) != null;
            boolean setData  = req.getHeader(ManagementHeader.SET_CDN_LIST) != null;
            
            Common.checkParameter(vendorName);
            Common.checkParameter(iplist);
            
            CdnVendorMeta vendorMeta = client.vendorSelect(vendorName.trim());
            if (addData){
                if (vendorMeta != null){
                    if (vendorMeta.ipWhiteList == null){
                        vendorMeta.ipWhiteList = Arrays.asList(iplist.trim().split("\n"));
                    } else {
                        vendorMeta.ipWhiteList.addAll(Arrays.asList(iplist.trim().split("\n")));
                    }
                } else {
                    vendorMeta = new CdnVendorMeta(vendorName,Arrays.asList(iplist.split("\n")));
                }
            } else if (setData){
                if (vendorMeta == null) {
                    throw new BaseException(403, "NoVendorInformation");
                }
                if (vendorMeta != null){
                    vendorMeta.ipWhiteList.clear();
                    vendorMeta.ipWhiteList.addAll(Arrays.asList(iplist.trim().split("\n")));
                } 
            }
            
            //去重
            vendorMeta.ipWhiteList = vendorMeta.ipWhiteList.stream()
                                                           .filter(s->s.trim().length()>0)
                                                           .distinct()
                                                           .collect(Collectors.toList());
            
            //验证IP合法性
            Common.checkIPParameter(vendorMeta.ipWhiteList);
            
            client.vendorAdd(vendorMeta);
            resp.getOutputStream().write(vendorXmlString(client.vendorList()).toString().getBytes(Consts.STR_UTF8));
            
            return true;
        } else if (req.getHeader(ManagementHeader.DELETE_CDN_LIST) != null) {
            String vendorName = req.getParameter("vendorname");
            String ip = req.getParameter("ip");
            
            Common.checkParameter(vendorName);
            Common.checkParameter(ip);
            
            CdnVendorMeta vendorMeta = client.vendorSelect(vendorName.trim());
            if (vendorMeta == null){
                throw new BaseException(403, "NoVendorInformation");
            }
            
            vendorMeta.ipWhiteList.remove(ip);
            client.vendorAdd(vendorMeta);
            resp.getOutputStream().write(vendorXmlString(client.vendorList()).toString().getBytes(Consts.STR_UTF8));
            return true;
        }
        return false;
    }
    
    private List<String> vendorXmlString(List<CdnVendorMeta> vendorList) throws BaseException, JSONException {
        if (vendorList == null || vendorList.size() == 0){
            throw new BaseException(403, "NoVendorInformation");
        }
         
        List<String> result = new ArrayList<String>();
        JSONObject jo = new JSONObject();
        for(CdnVendorMeta vendorMeta:vendorList){
            if (vendorMeta.ipWhiteList == null) continue;
            for(String ip:vendorMeta.ipWhiteList){
                jo.put("vendorName", vendorMeta.vendorName);
                jo.put("ip", ip);
                result.add(jo.toString());
            }
        }
        return result;
    }
    
    private boolean handleUserDeposit(HttpServletRequest req, HttpServletResponse resp)
            throws Exception {
        if (req.getHeader(ManagementHeader.RECHARGE_FROM_MANAGEMENT) != null) {
            String userName = req.getParameter("userName");
            String deposit = req.getParameter("deposit");
            Common.checkParameter(userName);
            Common.checkParameter(deposit);
            Portal.checkAmount(deposit, 2, true);
            DBPay dbPay = DBPay.getInstance();
            OwnerMeta owner = new OwnerMeta(userName);
            if (!client.ownerSelect(owner))
                throw new BaseException(404, "NoSuchUser");
            DbAccount dbAccount = new DbAccount(owner.getId());
            dbPay.accountInsertFromManagement(dbAccount, Double.parseDouble(deposit));
            resp.setStatus(HttpServletResponse.SC_OK);
            return true;
        }
        return false;
    }
    
    private boolean handleUserInfo(HttpServletRequest req, HttpServletResponse resp)
            throws Exception {
        if (req.getHeader(ManagementHeader.SELECT_USER) != null) {
            Common.writeResponseEntity(resp, selectUser(req));
            return true;
        }
        return false;
    }
    private boolean handleUserDataDeleteInfo(HttpServletRequest req, HttpServletResponse resp)
            throws Exception {
        if (req.getHeader(ManagementHeader.SELECT_USER_DATA_DELETE_INFO) != null) {
            Common.writeJsonResponseEntity(resp, selectUserDataDeleteInfo(req));
            return true;
        }
        return false;
    }
    private boolean handleUserPrice(HttpServletRequest req, HttpServletResponse resp)
            throws Exception {
        // update user price
        if (req.getHeader(ManagementHeader.UPDATE_USER_PRICE) != null) {
            Common.checkParameter(req.getParameter("email"));
            Common.checkParameter(req.getParameter("priceId"));
            updateUserPrice(req.getParameter("email"),
                    Integer.parseInt(req.getParameter("priceId")));
            resp.setStatus(HttpServletResponse.SC_OK);
            return true;
        }
        return false;
    }
    
    private boolean handlePrice(HttpServletRequest req, HttpServletResponse resp) throws Exception {
        // add and modify price
        if (req.getHeader(ManagementHeader.ADD_PRICE) != null
                || req.getHeader(ManagementHeader.MODIFY_PRICE) != null) {
            String credit = req.getParameter("credit");
            String desc = req.getParameter("desc");
            desc = URLDecoder.decode(desc,"utf-8");
            Common.checkParameter(credit);
            Common.checkParameter(desc);
            Portal.checkAmount(credit, 0, false);
            if (req.getHeader(ManagementHeader.ADD_PRICE) != null)
                addPrice(credit, desc);// 默认的strategy=0
            else {
                String id = req.getParameter("id");
                Common.checkParameter(id);
                if (!modifyPrice(id, credit, desc))
                    Common.writeResponseEntity(resp, "UserUesd");
            }
            resp.setStatus(HttpServletResponse.SC_OK);
            return true;
        }
        // delete price
        if (req.getHeader(ManagementHeader.DELETE_PRICE) != null) {
            String id = req.getParameter("id");
            Common.checkParameter(id);
            deletePrice(Long.parseLong(id));
            resp.setStatus(HttpServletResponse.SC_OK);
            return true;
        }
        // select price
        if (req.getHeader(ManagementHeader.SELECT_PRICE) != null) {
            Common.writeResponseEntity(resp, selectPrice());
            resp.setHeader(Headers.CONTENT_TYPE, "application/xml");
            return true;
        }
        return false;
    }
    
    private String changeBeginDate(String date) throws ParseException {
        Date d = DateParser.parseDate(date);
        if (systemBeginDate.after(d))
            return TimeUtils.toYYYY_MM_dd(systemBeginDate);
        else
            return date;
    }

    private boolean handleStatistics(HttpServletRequest req, HttpServletResponse resp)
            throws Exception {
     // 使用量、已用带宽、可用带宽、连接数所有用户之和统计值统一查询接口
        String beginDate = req.getParameter("beginDate");
        String endDate = req.getParameter("endDate");
        String freq = req.getParameter("freq");
        if (req.getHeader(WebsiteHeader.USAGE) != null) {
            Utils.checkUsageReqParamsValid(beginDate, endDate, freq);
            String storageType = Utils.checkAndGetStorageType(req.getParameter("storageClass"));
            String usageStats = req.getParameter("usageStats");
            Utils.checkParameter(usageStats);
            String body = UsageResult.getRegionUsageInJson(beginDate, endDate, usageStats, freq, storageType);
            Common.writeJsonResponseEntity(resp, body);
            return true;
        }

        // 统计数据下载（region范围），目前仅支持CSV格式
        if (req.getParameter("usageDownloadFormat") != null && req.getParameter("bucketName") == null) {
            Utils.checkUsageReqParamsValid(beginDate, endDate, freq);
            String storageType = Utils.checkAndGetStorageType(req.getParameter("storageClass"));
            if (Consts.ALL.equals(storageType)) {
                // 下载数据不支持所有存储类型下载
                throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_ARGUMENT, "Invalid StorageClass.");
            }
            String format = req.getParameter("usageDownloadFormat");
            String file = req.getParameter("downloadFileName");
            String type = req.getParameter("downloadType");
            String needAllRegionUsage = req.getParameter("needAllRegionUsage");

            Utils.checkParameter(file);
            Utils.checkParameter(type);
            if (!Objects.equals(Utils.CSV, format)) {
                throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_ARGUMENT, "Invalid usageDownloadFormat.");
            }
            UsageType usageType = Utils.checkAndGetUsageStatsTypeForDownload(type);
            String body = UsageResult.getRegionUsageInCsv(beginDate, endDate, usageType, freq, storageType, BooleanUtils.toBoolean(needAllRegionUsage));
            Common.writeResponseEntity(resp, body, format, file);
            return true;
        }
        return false;
    }

    private boolean handleUserStatistics(HttpServletRequest req, HttpServletResponse resp)
            throws IOException, ParseException, Exception {
        // total user
        if (req.getHeader(ManagementHeader.TOTAL_USER) != null) {
            String beginDate = req.getParameter("beginDate");
            String endDate = req.getParameter("endDate");
            Common.checkParameter(beginDate);
            Common.checkParameter(endDate);
            Common.writeResponseEntity(resp,
                    getTotalUser(resp, changeBeginDate(beginDate), endDate));
            return true;
        }
        // new user
        if (req.getHeader(ManagementHeader.NEW_USER) != null) {
            String beginDate = req.getParameter("beginDate");
            String endDate = req.getParameter("endDate");
            Common.checkParameter(beginDate);
            Common.checkParameter(endDate);
            Common.writeResponseEntity(resp, getNewlUser(resp, beginDate, endDate));
            return true;
        }
        return false;
    }

    private boolean handleUser(HttpServletRequest req, HttpServletResponse resp)
            throws BaseException, NoSuchAlgorithmException, IOException {
        // user login
        if (req.getHeader(ManagementHeader.LOGIN_USER) != null) {
            login(req, resp);
            return true;
        }
        // user logout
        if (req.getHeader(ManagementHeader.LOGOUT_USER) != null) {
            logout(req, resp);
            return true;
        }
        //中心节点发起的更新请求；
        if ((req.getHeader(ManagementHeader.ADMIN) != null
                || req.getParameter(ManagementHeader.ADMIN) != null) && 
                req.getHeader("x-portaloos-modify-user")!=null) {
            return false;
        }
        //中心节点发起的获取用量请求；
        if ((req.getHeader(ManagementHeader.SALESMAN) != null
                || req.getParameter(ManagementHeader.SALESMAN) != null) && 
                req.getHeader("x-portaloos-get-user-usage")!=null) {
            return false;
        }
        //中心节点发起的获取资源池使用量请求
        if ((req.getHeader(ManagementHeader.ADMIN) != null
                || req.getParameter(ManagementHeader.ADMIN) != null)
                && "daily-space".equalsIgnoreCase(req.getHeader(ManagementHeader.BSS_QUERY))) {
            return false;
        }
        // user access
        if (req.getHeader(ManagementHeader.LOGIN_SESSIONID) != null
                || req.getParameter("sessionId") != null) {
            if (!isValidSession(req, session))
                throw new BaseException(403, "NotLogin");
        } else
            throw new BaseException(403, "NotLogin");
        return false;
    }

    // 先支持查询1000个用户
    private String selectUser(HttpServletRequest req) throws Exception {
        String marker = req.getParameter("marker");
        if (marker == null || marker.isEmpty()) {
            marker = "";
        }
        
        String flag = req.getParameter("countFlag");
        boolean countFlag=false;
        if (flag != null && !flag.isEmpty()) {
            countFlag = Boolean.parseBoolean(flag);
        }
        OwnerMeta owner = new OwnerMeta();
        int pageSize = 15;
        long total = 0;
        String type = req.getParameter("type");
        List<OwnerMeta> owners = new ArrayList<OwnerMeta>();
        Pair<Integer, List<OwnerMeta>> users = new Pair<>();
        if (type != null && type.equals("email")) {
            owner = new OwnerMeta(req.getParameter("keywords"));
            users = client.ownerSelectByColumn("email", pageSize, owner.name, marker, countFlag);
            if (StringUtils.isNotEmpty(owner.name)) {
                Pair<Integer, List<OwnerMeta>> user2 = client.ownerSelectByColumn("newEmail", pageSize, owner.name, marker, countFlag);
                if (user2.first() > 0) {
                    List<OwnerMeta> emailList = users.second();
                    List<OwnerMeta> newEmailList = user2.second();
                    for (OwnerMeta newEO : newEmailList) {
                        if (!emailList.contains(newEO)) {
                            emailList.add(newEO);
                        }
                    }
                    users.first(emailList.size());
                }
            }
        }
        if (type != null && type.equals("companyName")) {
            owner.companyName = URLDecoder.decode(req.getParameter("keywords"), Consts.STR_UTF8);
            users = client.ownerSelectByColumn("companyName", pageSize, owner.companyName,marker,countFlag);
        }
        if (type != null && type.equals("mobilePhone")) {
            owner.mobilePhone = req.getParameter("keywords");
            users = client.ownerSelectByColumn("mobilePhone", pageSize, owner.mobilePhone,marker,countFlag);
        }
        if (type != null && type.equals("phone")) {
            owner.phone = req.getParameter("keywords");
            users = client.ownerSelectByColumn("phone", pageSize, owner.phone,marker,countFlag);
        }
        if (type != null && type.equals("createDate")) {
            owner.createDate = req.getParameter("keywords");
            users = client.ownerSelectByColumn("createDate", pageSize, owner.createDate,marker,countFlag);
        }
        if (type != null && type.equals("comment")) {
            owner.comment = URLEncoder.encode(req.getParameter("keywords"), Consts.STR_UTF8);
            users = client.ownerSelectByColumn("comment", pageSize, owner.comment,marker,countFlag);
        }
        if (type != null && type.equals("userType")) {
            try {
                owner.userType = Integer.parseInt(req.getParameter("keywords"));
            }catch (NumberFormatException e) {
                throw new BaseException(400, "InvalidUserType");
            }
            users = client.ownerSelectByIntColumn("userType", pageSize, owner.userType,marker,countFlag);
        }
                
        if (type != null && type.equals("packageId")) {
            owners.clear();
            
            DbUserPackage dup = new DbUserPackage();
            dup.packageId = Integer.parseInt(req.getParameter("keywords"));
            DBPrice dbPrice = DBPrice.getInstance();
            dbPrice.userPackageSelectByPackageId(dup);
            for (DbUserPackage up : dup.ups) {
                OwnerMeta o = new OwnerMeta(up.usrId);
                client.ownerSelectById(o);
                owners.add(o);
            }
            total = dup.ups.size();
        } else {
            total = users.first();
            owners = users.second();
        }
        
        
        XmlWriter xml = new XmlWriter();
        xml.start("Users");
        xml.start("total").value(total+"").end();
        if (countFlag) {
            xml.start("poolChName").value(BssAdapterClient.getGlobalPoolsXML(BssAdapterConfig.localPool.ak,
                    BssAdapterConfig.localPool.getSK())).end();
        }
        boolean isManager = Admin.isAdmin(req);
        DbSalesman dbSalesman = Salesman.getSalesmanFromSession(Utils.getSessionId(req));
        for (OwnerMeta o : owners) {
            if (!isManager) {
                Salesman.isBelongTo(dbSalesman, o.getId());
            }
            xml.start("User");
            if (countFlag) {
                xml.start("id").value(String.valueOf(o.getId())).end();
            } else {
                xml.start("id").value(String.valueOf(o.getId())).end();
                xml.start("name").value(o.name).end();
                xml.start("email").value(o.email).end();
                xml.start("newEmail").value(o.newEmail).end();
                xml.start("displayName").value(o.displayName).end();
                xml.start("companyName").value(o.companyName).end();
                xml.start("companyAddress").value(o.companyAddress).end();
                xml.start("mobilePhone").value(o.mobilePhone).end();
                xml.start("phone").value(o.phone).end();
                xml.start("priceDesc").value("").end();
                xml.start("Auth")
                .start("reason")
                .start("chinese").value(o.auth.getReason_ch()).end()
                .start("english").value(o.auth.getReason_en()).end()
                .end()
                .start("servicePermission")
                .start("GET").value(Permission.getPermission(o.auth.getServicePermission().hasGet())).end().end()
                .start("bucketPermission")
                .start("GET").value(Permission.getPermission(o.auth.getBucketPermission().hasGet())).end()
                .start("PUT").value(Permission.getPermission(o.auth.getBucketPermission().hasPut())).end()
                .start("DELETE").value(Permission.getPermission(o.auth.getBucketPermission().hasDelete())).end().end()
                .start("objectPermission")
                .start("GET").value(Permission.getPermission(o.auth.getObjectPermission().hasGet())).end()
                .start("PUT").value(Permission.getPermission(o.auth.getObjectPermission().hasPut())).end()
                .start("DELETE").value(Permission.getPermission(o.auth.getObjectPermission().hasDelete())).end().end()
                .end();
                if (o.frozenDate != null && o.frozenDate.trim().length() != 0)
                    xml.start("fronzenDate").value("fronzen").end();
                else
                    xml.start("fronzenDate").value("notFronzen").end();
                xml.start("userType").value(String.valueOf(o.userType)).end();
                xml.start("credit").value(String.valueOf(o.credit)).end();
                xml.start("createDate").value(o.createDate).end();
                if (o.type == OwnerMeta.TYPE_OOS)
                    xml.start("type").value("OOS").end();
                else if (o.type == OwnerMeta.TYPE_CRM)
                    xml.start("type").value("CRM").end();
                else if (o.type == OwnerMeta.TYPE_TY)
                    xml.start("type").value("TianYi").end();
                else if (o.type == OwnerMeta.TYPE_UDB)
                    xml.start("type").value("UDB").end();
                else if (o.type == OwnerMeta.TYPE_ZHQ)
                    xml.start("type").value("ZHQ").end();
                AccountSummary iamAccountSummary = IAMInternalAPI.getAccountSummary(o.getAccountId());
                xml.start("maxAKNum").value(String.valueOf(iamAccountSummary.accessKeysPerAccountQuota)).end();
                xml.start("comment").value(o.comment).end();
                if (o.verify != null)
                    xml.start("verify").value("notVerify").end();
                else
                    xml.start("verify").value("verify").end();
                
                /*List<Pair<String,Pool>> pools = BssAdapterClient.getAvailablePoolsAsListById(o.getId(), BssAdapterConfig.localPool.ak, 
                        BssAdapterConfig.localPool.getSK());
                
                List<String> pools2 = pools.stream().map(item->{return item.first();}).collect(Collectors.toList());
                xml.start("availablePools").value(StringUtils.join(pools2,",")).end();*/
            }
            xml.end();
        }
        xml.end();
        return xml.toString();
    }

    private void updateUserPrice(String email, int priceId) throws Exception {
        DBPay dbPay = DBPay.getInstance();
        DbPrice dbPrice = new DbPrice(priceId);
        if (!dbPay.priceSelect(dbPrice))
            throw new BaseException(404, "NoSuchPrice");
        ;
        OwnerMeta owner = new OwnerMeta(email);
        if (!client.ownerSelect(owner))
            throw new BaseException(404, "NoSuchUser");
        owner.credit = dbPrice.credit;
        client.ownerUpdate(owner);
    }

    private String selectUserDataDeleteInfo(HttpServletRequest req) throws Exception {
        String email = req.getParameter("email");
        OwnerMeta owner = new OwnerMeta(email);
        String deleteUserInfo = BssAdapterClient
                .getDeleteUserInfoV2(String.valueOf(owner.getId()), BssAdapterConfig.localPool.ak,
                        BssAdapterConfig.localPool.getSK(),"manager");
        return deleteUserInfo;
    }
    private String selectPrice() throws SQLException {
        DBPay dbPay = DBPay.getInstance();
        DbPrice dbPrice = new DbPrice();
        dbPay.priceSelectAll(dbPrice);
        XmlWriter xml = new XmlWriter();
        xml.start("Prices");
        for (String price : dbPrice.prices) {
            xml.start("Price");
            String[] prices = price.split(" ");
            xml.start("id").value(prices[0]).end();
            xml.start("des").value(prices[1]).end();
            xml.start("credit").value(prices[2]).end();
            xml.end();
        }
        xml.end();
        return xml.toString();
    }

    private void deletePrice(Long id) throws SQLException, BaseException {
        DBPay dbPay = DBPay.getInstance();
        DbPrice dbPrice = new DbPrice(id);
        dbPay.priceDelete(dbPrice);
    }
    
    private boolean modifyPrice(String id, String credit, String desc) throws Exception {
        DBPay dbPay = DBPay.getInstance();
        DbPrice dbPrice = new DbPrice(Long.parseLong(id));
        if (!dbPay.priceSelect(dbPrice))
            throw new BaseException(404, "NoSuchPrice");
        dbPrice.credit = Integer.parseInt(credit);
        dbPrice.des = desc;
        dbPay.priceUpdate(dbPrice);
        return true;
    }
    
    private void addPrice( String credit, String desc) throws SQLException {
        DBPay dbPay = DBPay.getInstance();
        DbPrice dbPrice = new DbPrice();
        dbPrice.credit = Integer.parseInt(credit);
        dbPrice.des = desc;
        dbPay.priceInsert(dbPrice);
    }
    
    private String getTotalUser(HttpServletResponse resp, String beginDate, String endDate)
            throws Exception {
        Set<String> dates = DateParser.parseDate(beginDate, endDate);
        XmlWriter xml = new XmlWriter();
        xml.start("TotalUser");
        long maxNum = 0;
        for (String date : dates) {
            HashMap<String, Long> owners = client.ownerSelectNewOwnerByRange(null,
                    Misc.formatyyyymmdd(DateUtils.addDays(Misc.formatyyyymmdd(date), 1)));
            Iterator<Entry<String, Long>> it = owners.entrySet().iterator();
            int dayTotalNum = 0;
            while (it.hasNext()) {
                Entry<String, Long> user = it.next();
                if (maxNum < user.getValue())
                    maxNum = user.getValue();
                dayTotalNum += user.getValue();
            }
            xml.start("Users");
            xml.start("Date").value(date).end();
            xml.start("TotalNum").value(String.valueOf(dayTotalNum)).end();
            xml.end();
        }
        xml.start("MaxTotalUser").value(String.valueOf(maxNum)).end();
        xml.end();
        return xml.toString();
    }
    
    private String getNewlUser(HttpServletResponse resp, String beginDate, String endDate)
            throws Exception {
        HashMap<String, Long> res = client.ownerSelectNewOwnerByRange(beginDate,
                Misc.formatyyyymmdd(DateUtils.addDays(Misc.formatyyyymmdd(endDate), 1)));// 把endDate加1
        XmlWriter xml = new XmlWriter();
        xml.start("NewUser");
        List<String> dates = Utils.getDatesBetweenTwoDate(beginDate, endDate);
        long maxNum = 0;
        for (String date : dates) {
            xml.start("Users");
            xml.start("Date").value(date).end();
            long num = 0;
            if (res.containsKey(date)) {
                num = res.get(date);
                if (maxNum < num)
                    maxNum = num;
            }
            xml.start("NewNum").value(String.valueOf(num)).end();
            xml.end();
        }
        xml.start("MaxNewUser").value(String.valueOf(maxNum)).end();
        xml.end();
        return xml.toString();
    }

    private void login(HttpServletRequest req, HttpServletResponse resp) throws BaseException,
            NoSuchAlgorithmException, IOException {
        if (!req.getMethod().equalsIgnoreCase(HttpMethod.POST.toString())) {
            throw new BaseException(403, "InvalidLoginRequest");
        }
        
        String userName = req.getParameter(Parameters.USER_NAME);
        String password = req.getParameter(Parameters.PASSWORD);
        String verifyCode = req.getParameter(Parameters.VERIFY_CODE);

        if (verifyCode == null || verifyCode.trim().length() == 0 || !verifyCode
                .equalsIgnoreCase(session.get(req.getParameter(Parameters.IMAGE_SESSIONID))))
            throw new BaseException(403, "InvalidVerifyCode");
        if (userName == null || password == null || userName.trim().length() == 0
                || password.trim().length() == 0)
            throw new BaseException();
        ManagementUser user=new ManagementUser(userName);
        if (!client.managementUserGet(user))
            throw new BaseException(403, "InvalidUser");
        if (!user.getPwd().equals(password))
            throw new BaseException(403, "InvalidPassword");
        String sessionId = UUID.randomUUID().toString();
        session.update(sessionId, userName);
        try {
            resp.getWriter().write("<SessionId>" + sessionId + "</SessionId>");
            resp.setHeader(ManagementHeader.LOGIN_SESSIONID, sessionId);
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }
        resp.setStatus(200);
        ManagerLogMeta managerLog = new ManagerLogMeta(userName, Operation.Login);
        try {
            client.managerLogPut(managerLog);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        log.info("manager: " + userName + " login success, sessionId is:" + sessionId);
        session.remove(req.getParameter(Parameters.IMAGE_SESSIONID));
        Cookie cookie = new Cookie(WebsiteHeader.MANAGER_COOKIE, sessionId);
        resp.addCookie(cookie);
    }
    
    private void logout(HttpServletRequest req, HttpServletResponse resp) throws BaseException {
        String sessionId = req.getHeader(ManagementHeader.LOGIN_SESSIONID);
        log.info("manager: " + session.get(sessionId) + " logout success, sessionId is:"
                + sessionId);
        if (sessionId != null && session.get(sessionId) != null) {
            ManagerLogMeta managerLog = new ManagerLogMeta(session.get(sessionId), Operation.Logout);
            try {
                client.managerLogPut(managerLog);
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }
        session.remove(sessionId);
        resp.setStatus(200);
    }
    
    private boolean isValidSession(HttpServletRequest req, Session<String, String> session) {
        String sid = Utils.getSessionId(req);
        String userName = session.get(sid);
        if (userName == null)
            return false;
        session.update(sid, userName);
        return true;
    }
    
    boolean handleWebPages(String target, Request basereq, HttpServletRequest req,
            HttpServletResponse resp) throws ServletException, IOException, BaseException {
        if (!(target.startsWith("/" + config.getString("webpages") + "/")
                || target.startsWith("/crossdomain.xml")
                || target.equalsIgnoreCase("/favicon.icon") || target
                    .equalsIgnoreCase("/favicon.ico")))
            return false;
        if (target.startsWith("/" + config.getString("webpages") + "/manager")
                && target.endsWith("html") && !target.contains("login")) {
            Cookie[] cookies = req.getCookies();
            boolean isLogin = false;
            for (Cookie c : cookies)
                if (c.getName().equals(WebsiteHeader.MANAGER_COOKIE)) {
                    if (session.get(c.getValue())!=null) {
                        isLogin = true;
                        break;
                    }
                }
            if (!isLogin) {
                String protocal = req.getHeader("X-Forwarded-Proto");
                String requestURL = req.getRequestURL().toString();
                String redirectURL = ((protocal != null && protocal.trim().length() > 0) ? protocal
                        : requestURL.split("://")[0]) + "://"
                        + (requestURL.substring(0, requestURL.indexOf("/manager"))
                                + "/manager/login.html").split("://")[1];
                resp.sendRedirect(redirectURL);
                return true;
            }
        }
        File file = new File(webdir, target);
        if (file.isDirectory())
            return true;
        FileCache fc = FileCache.THIS;
        FileCache.FileAttr fa = fc.get(file, false);
        if (fa == null)
            return true;
        String mimeType = MimeType.getMimeType(file.getPath());
        ArrayOutputStream data = fa.data;
        resp.setContentType(mimeType);
        resp.setStatus(200);
        resp.getOutputStream().write(data.data(), 0, data.size());
        return true;
    }
    /**
     * 获取资源池空间使用情况
     * @param req
     * @param resp
     * @return
     */
    private boolean handlePoolDailySpaceQuery(String target,
            HttpServletRequest req,
            HttpServletResponse resp) throws IOException, JSONException {
        //处理资源池使用情况查询请求
        if ("/dailyspacemeta".equals(target)) {
            String bDate = req.getParameter("beginDate");
            String eDate = req.getParameter("endDate");
            LocalDate beginDate = LocalDate.parse(bDate, DateTimeFormatter.ISO_LOCAL_DATE);
            LocalDate endDate = LocalDate.parse(eDate, DateTimeFormatter.ISO_LOCAL_DATE);
            List<DailySpaceMeta> spaceMetaList = queryPeriodDailySpaceMeta(
                    beginDate, endDate);
            JSONArray array = new JSONArray();
            for (DailySpaceMeta dailySpaceMeta : spaceMetaList) {
                JSONObject object = new JSONObject();
                object.put("poolName", dailySpaceMeta.getRegionName());
                object.put("date", dailySpaceMeta.getDate().toString());
                object.put("used", dailySpaceMeta.getUsed());
                object.put("capacity", dailySpaceMeta.getCapacity());
                array.put(object);
            }
            resp.getOutputStream().write(array.toString()
                    .getBytes(Consts.STR_UTF8));
            resp.setStatus(HttpServletResponse.SC_OK);
            return true;
        }
        return false;
    }

    /**
     * 获取所有资源池在一段时间内的空间使用情况
     *
     * @param beginDate
     * @param endDate
     * @return
     */
    private List<DailySpaceMeta> queryPeriodDailySpaceMeta(
            final LocalDate beginDate, final LocalDate endDate) {
        List<String> dataRegions = DataRegions.getAllRegions();
        return dataRegions.parallelStream().map(region -> {
            try {
                return client.dailySpaceSelectRangeWithEnd(region, beginDate, endDate);
            } catch (IOException e) {
                log.error("error", e);
            }
            return null;
        }).filter(Objects::nonNull).flatMap(Collection::stream)
                .collect(Collectors.toList());
    }

    private HttpRequestBase buildIAMInternalApiRequest(HttpServletRequest request) throws Exception {
        String method = request.getMethod();
        if (method == null)
            throw new BaseException(405, "Method Not Allowed");
        @SuppressWarnings("rawtypes")
        OOSRequest iamRequest = new OOSRequest(request);
        String dateTimeStamp =  SignerUtils.formatTimestamp(new Date().getTime());
        String uri;
        uri = request.getRequestURI();
        HttpRequestBase httpRequest = null;
        if (method.equals("GET")) {
            httpRequest = new HttpGet(uri);
        } else if (method.equals("PUT")) {
            httpRequest = new HttpPut(uri);
        } else if (method.equals("DELETE")) {
            httpRequest = new HttpDelete(uri);
        } else if (method.equals("HEAD")) {
            httpRequest = new HttpHead(uri);
        } else if (method.equals("POST")) {
            httpRequest = new HttpPost(uri);
        }
        Enumeration<?> e = request.getHeaderNames();
        String header = null;
        String value = null;
        while (e.hasMoreElements()) {
            header = (String) e.nextElement();
            value = request.getHeader(header);
            if (header.contains("x-ctyun")) {
                iamRequest.getHeaders().remove(header);
                continue;
            }
            if (header.equalsIgnoreCase("host")) {
                continue;
            }
            if (header.equalsIgnoreCase("date")) {
                continue;
            }
            httpRequest.addHeader(header, value);
        }
        httpRequest.addHeader("Date", dateTimeStamp);
        httpRequest.addHeader("Host", OOSConfig.getIAMDomainSuffix() + ":" + oosConfig.getInt("website.iamPort"));
        httpRequest.addHeader("OOS-PROXY-HOST", "oos-proxy");
        if (method.equals("POST")) {
            BasicHttpEntity entity = new BasicHttpEntity();
            JSONObject bodyString = new JSONObject();
            e = request.getParameterNames();
            while (e.hasMoreElements()) {
              String k = (String) e.nextElement();
              String v = request.getParameter(k);
              if(k.equalsIgnoreCase("ownerId")) {
                  bodyString.put("accountId", getAccountId(Long.parseLong(v)));
                  continue;
              }
              bodyString.put(k, v);
            }
            entity.setContent(new ByteArrayInputStream(bodyString.toString().getBytes()));
            ((HttpPost) httpRequest).setEntity(entity);
        }
        httpRequest.addHeader(WebsiteHeader.CLIENT_IP, Utils.getIpAddr(request));
        return httpRequest;
    }
    
    public static String getAccountId(long ownerId) {
        String accountId = Long.toUnsignedString(ownerId, 36);
        accountId = String.format("%13s", accountId).replaceAll(" ", "0");
        return accountId;
    }
    
    private void setResponse(HttpServletResponse resp, HttpResponse response) throws IOException {
        org.apache.http.Header[] headers = response.getAllHeaders();
        for (org.apache.http.Header header : headers) {
            resp.addHeader(header.getName(), header.getValue());
        }
        StatusLine status = response.getStatusLine();
        resp.setStatus(status.getStatusCode());
        HttpEntity entity = response.getEntity();
        if (entity != null) {
            ServletOutputStream outstream = null;
            try {
                outstream = resp.getOutputStream();
                entity.writeTo(outstream);
                outstream.flush();
                outstream.close();
            } catch (IOException e) {
                log.error(e.getMessage(), e);
            } finally {
                if (outstream != null)
                    try {
                        outstream.close();
                    } catch (Throwable e) {
                        log.error(e.getMessage(), e);
                    }
                EntityUtils.consume(entity);
            }
        }
    }
    
    private void log(HttpServletRequest req) {
        log.info("**************************");
        log.info(" Http Method:" + req.getMethod() + " User-Agent:" + req.getHeader("User-Agent")
                + " RequestURI:" + req.getRequestURI());
        Enumeration<?> e = req.getHeaderNames();
        String logs = "Headers:";
        while (e.hasMoreElements()) {
            String k = (String) e.nextElement();
            String v = req.getHeader(k);
            logs += k + "=" + v + " ";
        }
        log.info(logs);
        logs = "Parameters:";
        e = req.getParameterNames();
        while (e.hasMoreElements()) {
            String k = (String) e.nextElement();
            String v = req.getParameter(k);
            logs += k + "=" + v + " ";
        }
        log.info(logs);
    }
}