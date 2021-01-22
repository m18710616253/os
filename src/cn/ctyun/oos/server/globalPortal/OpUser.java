package cn.ctyun.oos.server.globalPortal;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.json.JSONException;
import org.json.JSONObject;

import cn.ctyun.common.BaseException;
import cn.ctyun.common.Consts;
import cn.ctyun.oos.hbase.MetaClient;
import cn.ctyun.oos.metadata.OwnerMeta;
import cn.ctyun.oos.metadata.UserToTagMeta;
import cn.ctyun.oos.metadata.VSNTagMeta;
import cn.ctyun.oos.metadata.VSNTagMeta.VSNTagType;
import cn.ctyun.oos.server.db.DB;
import cn.ctyun.oos.server.db.DbInitialAccount;
import cn.ctyun.oos.server.db.dbprice.DBPrice;
import cn.ctyun.oos.server.db.dbprice.DbPackage;
import cn.ctyun.oos.server.db.dbprice.DbUserPackage;
import cn.ctyun.oos.server.util.DateParser;
import common.time.TimeUtils;
import common.util.ConfigUtils;

/**
 * @author: Cui Meng
 */
public class OpUser {
    public static CompositeConfiguration config;
    private static final Log log = LogFactory.getLog(OpUser.class);
    private static String host;
    private static int port;
    private static String saleEntryId;
    private static int packageId;
    private static int defaultCredit;
    private static String protocol;
    private static String[] initMetaTag;
    private static String[] initDataTag;
    private static MetaClient client = MetaClient.getGlobalClient();
    static {
        File[] xmlConfs = { new File(System.getenv("OOS_HOME") + "/conf/oos-online.xml"),
                new File(System.getenv("OOS_HOME") + "/conf/oos.xml") };
        try {
            config = (CompositeConfiguration) ConfigUtils.loadXmlConfig(xmlConfs);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        host = config.getString("portal.host");
        port = config.getInt("portal.port");
        protocol = config.getString("portal.queryAccountProtocol","http");
        saleEntryId = config.getString("portal.saleEntryId");
        packageId = config.getInt("website.registerPackeageId");
        defaultCredit = config.getInt("website.defaultCredit");
        // extraInfo = config.getString("portal.extraInfo");
        //为Protal对接用户默认分配的数据域和元数据域的标签；
        initMetaTag = config.getString("portal.InitialMetaRegionTag","").split(",");
        initDataTag = config.getString("portal.InitialDataRegionTag","").split(",");
    }
    
    public static void registerUser(String accountId, String userId, String orderId)
            throws Exception {
        // 存account
        if (accountId == null || accountId.trim().length() == 0)
            throw new BaseException();
        DB db = DB.getInstance();
        DbInitialAccount dbInitialAccount = new DbInitialAccount(accountId);
        OwnerMeta owner = new OwnerMeta();
        if (db.initialAccountSelect(dbInitialAccount)) {
            // throw new BaseException(400, "AccountAlreadyExists");
            log.info("accountId already exists in oos, return");
            return;
        }
        try {
            owner = getOwnerInfo(accountId);
            if (owner == null)
                throw new BaseException(400, "CanNotGetUserInfo");
            owner.credit = -Long.MAX_VALUE;
            owner.maxAKNum = Consts.MAX_AK_NUM;
        } catch (IOException e) {
            log.error(e.getMessage(), e);
            throw e;
        }
        dbInitialAccount.userId = userId;
        dbInitialAccount.orderId = orderId;
        
        //针对OOS已经有这个用户的情况
        if (client.ownerSelect(owner)) {
            // throw new BaseException(400, "UserAlreadyExists");
            //log.info("owner already exists in oos, return");
            /**
             * 如果owner中已经有这个用户，那么只要在mysql表的 
             * initialAccount中建立关联即可；
             * 先忽略oos原有客户和电商对接客户其他属性不一致的情况
             */
            dbInitialAccount.ownerId = owner.getId();
            db.initialAccountInsert(dbInitialAccount);
            
            return;
        }
        // 存owner
        try {
            client.ownerInsert(owner);
            
            //分配默认标签
            if (initMetaTag != null && initMetaTag.length>0 && !"".equals(initMetaTag[0])){
                //验证配置文件中tag标签的有效性；
                for (String tagName:initMetaTag){
                    if (!client.vsnTagSelect(new VSNTagMeta(VSNTagType.META,tagName))){
                        throw new BaseException(500, "InvalidMetaTagName" ,"Invalid tag name in config:"+tagName);
                    }    
                }
                
                UserToTagMeta userTagMeta = new UserToTagMeta(owner.getId(),
                        Arrays.asList(initMetaTag), VSNTagType.META);
                client.userToTagInsert(userTagMeta);
            }
            if (initDataTag != null && initDataTag.length>0 && !"".equals(initDataTag[0])){
                //验证配置文件中tag标签的有效性；
                for (String tagName:initDataTag){
                    if (!client.vsnTagSelect(new VSNTagMeta(VSNTagType.DATA,tagName)))
                        throw new BaseException(500, "InvalidDataTagName" ,"Invalid tag name in config:"+tagName);
                }
                UserToTagMeta userTagData = new UserToTagMeta(owner.getId(),
                        Arrays.asList(initDataTag), VSNTagType.DATA);
                client.userToTagInsert(userTagData);
            }
            dbInitialAccount.ownerId = owner.getId();
            db.initialAccountInsert(dbInitialAccount);
            if (packageId != 0 && owner.type != OwnerMeta.TYPE_ZHQ)
                orderPackage(owner);
        } catch (SQLException e) {
            log.error(e.getMessage(), e);
            throw new BaseException(500, "InsertUserError");
        }
    }
    
    public static void registerUser(String accountId, String userId, String orderId, 
            OwnerMeta bssUser) throws Exception {
        if (accountId == null || accountId.trim().length() == 0)
            throw new BaseException(400,"InvalidAccountid");
        
        DB db = DB.getInstance();
        DbInitialAccount dbInitialAccount = new DbInitialAccount(accountId);
        
        if (!db.initialAccountSelect(dbInitialAccount)) {
            dbInitialAccount.userId = userId;
            dbInitialAccount.orderId = orderId;
            dbInitialAccount.ownerId = bssUser.getId();
            db.initialAccountInsert(dbInitialAccount);
            
            if (packageId != 0 && bssUser.type != OwnerMeta.TYPE_ZHQ)
                orderPackage(bssUser);
        } 
        
        if (!client.ownerSelect(bssUser)) {
            // 存owner
            try {
                client.ownerInsert(bssUser);
                
                //分配默认标签
                if (initMetaTag != null && initMetaTag.length>0 && !"".equals(initMetaTag[0])){
                    //验证配置文件中tag标签的有效性；
                    for (String tagName:initMetaTag){
                        if (!client.vsnTagSelect(new VSNTagMeta(VSNTagType.META,tagName))){
                            throw new BaseException(500, "InvalidMetaTagName" ,"Invalid tag name in config:"+tagName);
                        }    
                    }
                    
                    UserToTagMeta userTagMeta = new UserToTagMeta(bssUser.getId(),
                            Arrays.asList(initMetaTag), VSNTagType.META);
                    client.userToTagInsert(userTagMeta);
                }
                if (initDataTag != null && initDataTag.length>0 && !"".equals(initDataTag[0])){
                    //验证配置文件中tag标签的有效性；
                    for (String tagName:initDataTag){
                        if (!client.vsnTagSelect(new VSNTagMeta(VSNTagType.DATA,tagName)))
                            throw new BaseException(500, "InvalidDataTagName" ,"Invalid tag name in config:"+tagName);
                    }
                    UserToTagMeta userTagData = new UserToTagMeta(bssUser.getId(),
                            Arrays.asList(initDataTag), VSNTagType.DATA);
                    client.userToTagInsert(userTagData);
                }
            } catch (SQLException e) {
                log.error(e.getMessage(), e);
                throw new BaseException(500, "InsertUserError");
            }           
        }
    }
    
    private static void orderPackage(OwnerMeta owner) throws SQLException, BaseException,
            ParseException {
        DBPrice dbPrice = DBPrice.getInstance();
        DbPackage dbPackage = new DbPackage(packageId);
        if (!dbPrice.packageSelect(dbPackage))
            throw new BaseException(400, "InvalidPackageId");
        DbUserPackage dbUserPackage = new DbUserPackage(owner.getId());
        dbUserPackage.isPaid = 1;
        String startTime = TimeUtils.toYYYY_MM_dd(new Date());
        dbUserPackage.date = startTime;
        String endDate = TimeUtils.toYYYY_MM_dd(DateUtils.addMonths(
                DateParser.parseDate(startTime), (int) dbPackage.duration));
        dbUserPackage.packageId = packageId;
        dbUserPackage.packageDiscount = 0;
        dbUserPackage.packageStart = startTime;
        dbUserPackage.orderId = String.valueOf(owner.getId()) + System.nanoTime();
        dbUserPackage.packageEnd = endDate;
        dbUserPackage.storage = dbPackage.storage;
        dbUserPackage.flow = dbPackage.flow;
        dbUserPackage.ghRequest = dbPackage.ghRequest;
        dbUserPackage.otherRequest = dbPackage.otherRequest;
        dbUserPackage.roamFlow = dbPackage.roamFlow;
        dbUserPackage.roamUpload = dbPackage.roamUpload;
        try {
            dbPrice.userPackageInsert(dbUserPackage);
        } catch (SQLException e) {
            log.error(e.getMessage(), e);
            throw new BaseException(500, "InsertUserError");
        }
    }
    
    public static OwnerMeta getOwnerInfo(String accountId) throws IOException {
        String path = "/account/QueryAccountInfo?accountId="+accountId;
        URL url = new URL(protocol, host, port, path);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestProperty("host", host);
        conn.setRequestProperty("accountId", accountId);
        conn.setRequestMethod("GET");
        conn.setUseCaches(false);
        conn.setDoInput(true);
        conn.connect();
        
        InputStream ip = null;
        OwnerMeta owner = null;
        int code = conn.getResponseCode(); 
        if (code == 200){
            try {
                ip = conn.getInputStream();
                JSONObject result = new JSONObject(IOUtils.toString(ip, Consts.CS_UTF8));
                log.info("getOwnerInfo:" + result.toString());
                if (result.getInt("statusCode") != 800)
                    throw new BaseException(400, "NotFoundUserInfo");
                JSONObject account = result.getJSONObject("returnObj");
                JSONObject accountMetaBO = account.getJSONObject("accountMetaBO");
                owner = new OwnerMeta(accountMetaBO.getString("loginEmail"));
                owner.email = accountMetaBO.getString("loginEmail");

                if (account.has("companyName") && !account.getString("companyName").equalsIgnoreCase("")) {
                    owner.companyName = account.getString("companyName");
                }

                //赋值顺序1 loginName 2 name 3 companyName，按顺序那个先非空那个为displayName实际值
                if (account.has("loginName") && account.getString("loginName").length() > 0) {
                    owner.displayName = account.getString("loginName");
                } else if (account.has("name") && account.getString("name").length() > 0) {
                    owner.displayName = account.getString("name");
                } else if (account.has("companyName") && account.getString("companyName").length() > 0) {
                    owner.displayName = account.getString("companyName");
                }

                if (accountMetaBO.has("accountType") && accountMetaBO.getInt("accountType") == 3)
                    owner.type = OwnerMeta.TYPE_ZHQ;
                else
                    owner.type = OwnerMeta.TYPE_TY;
                if (account.has("address"))
                    owner.companyAddress = account.getString("address");
                if (account.has("phone"))
                    owner.phone = account.getString("phone");

                //mobilephone根据accountType值得不同，可能存在于returnObj节点内或其子节点accountMetaBO内
                if (account.has("mobilephone") && account.getString("mobilephone").length() > 0) {
                    owner.mobilePhone = account.getString("mobilephone");
                } else if (accountMetaBO.has("mobilephone") && accountMetaBO.getString("mobilephone").length() > 0) {
                    owner.mobilePhone = accountMetaBO.getString("mobilephone");
                }

            } catch (Exception e) {
                log.error(e.getMessage(), e);
            } finally {
                if (ip != null)
                    ip.close();
            }
        }
        
        return owner;
    }
    
    public static void checkInitialAccountInfo(String accountId) 
            throws IOException, SQLException, BaseException {
        if (accountId == null || accountId.trim().length() == 0)
            throw new BaseException();
        
        InputStream ip = null; 
        DbInitialAccount initialAccount = new DbInitialAccount(accountId);
        
        DB db = DB.getInstance();
        if (db.initialAccountSelect(initialAccount)) {
            log.info("account info already in DB,accountid:" + accountId);
            return;
        }
        
        String path = "/account/QueryAccountInfo?accountId="+accountId;
        URL url = new URL(protocol, host, port, path);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestProperty("host", host);
        conn.setRequestProperty("accountId", accountId);
        conn.setRequestMethod("GET");
        conn.setUseCaches(false);
        conn.setDoInput(true);
        conn.connect();
        
        int code = conn.getResponseCode(); 
        if (code == 200){
            try {
                ip = conn.getInputStream();
                JSONObject result = new JSONObject(IOUtils.toString(ip, Consts.CS_UTF8));
                log.info("getOwnerInfo:" + result.toString());
                if (result.getInt("statusCode") != 800)
                    throw new BaseException(400, "NotFoundUserInfo");
                JSONObject account = result.getJSONObject("returnObj");
                JSONObject accountMetaBO = account.getJSONObject("accountMetaBO");
                
                if (accountMetaBO.has("rootUserid"))
                    initialAccount.userId = accountMetaBO.getString("rootUserid");
                if (accountMetaBO.has("loginEmail")){
                    initialAccount.ownerId = 
                            new OwnerMeta(accountMetaBO.getString("loginEmail")).getId();
                }                
                
                if (!db.initialAccountSelect(initialAccount)){
                    //对于已经存在oos用户的情况，补充mysql表中的orderId信息；
                    initialAccount.orderId = "AUTO-checkInitialAccountInfo";
                    db.initialAccountInsert(initialAccount);
                }
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            } finally {
                if (ip != null)
                    ip.close();
            }
        } 
    }
    
    /**
     * 
     * @param accountId
     * @return 单位元
     * @throws IOException
     * @throws ClientProtocolException
     * @throws Exception
     */
    public static double getOwnerBill(String accountId) throws ClientProtocolException, IOException {
        String uri = protocol + "://" + host + ":" + port + "/billing/GetBilling";
        HttpGet httpRequest = new HttpGet(uri);
        httpRequest.setHeader("accountId", accountId);
        HttpHost httpHost = new HttpHost(host, port);
        HttpClient client = new DefaultHttpClient();
        HttpResponse newResponse = client.execute(httpHost, httpRequest);
        InputStream ip = null;
        double balance = 0;
        try {
            ip = newResponse.getEntity().getContent();
            JSONObject result = new JSONObject(IOUtils.toString(ip));
            log.info("getOwnerBill:" + result.toString());
            if (result.getInt("statusCode") != 800)
                throw new BaseException(400, "GetBillFailed");
            JSONObject account = result.getJSONObject("returnObj");
            if (account.has("cashPoints"))
                balance += account.getDouble("cashPoints");// 单位元
            // if (account.has("creditPoints"))
            // balance += account.getLong("creditPoints");// 单位分
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        } finally {
            if (ip != null)
                ip.close();
        }
        return balance;
    }
    
    /**
     * 
     * @param accountId
     * @param userID
     * @param deposit
     *            单位分
     * @param type
     * @param cashFirst
     * @return
     * @throws IOException
     * @throws ClientProtocolException
     * @throws SQLException
     * @throws Exception
     */
    public static int deduction(String accountId, double deposit, String orderSeq,
            String description) throws ClientProtocolException, IOException, SQLException {
        // String payPlan = "{\"" + type + "\":" + deposit + ",\"cashFirst\":"
        // + cashFirst + "}";
        String uri = protocol + "://" + host + ":" + port + "/trade/CreateRTPaymentTrade";
        HttpPost httpRequest = new HttpPost(uri);
        httpRequest.setHeader("accountId", accountId);
        DB db = DB.getInstance();
        DbInitialAccount dbInitialAccount = new DbInitialAccount(accountId);
        db.initialAccountSelect(dbInitialAccount);
        httpRequest.setHeader("userID", dbInitialAccount.userId);
        httpRequest.setHeader("saleEntryId", saleEntryId);
        List<NameValuePair> params = new ArrayList<NameValuePair>();
        params.add(new BasicNameValuePair("cashAmount", String.valueOf(deposit)));
        params.add(new BasicNameValuePair("creditAmount", "0"));
        params.add(new BasicNameValuePair("serviceTag", "OOS"));
        params.add(new BasicNameValuePair("orderNo", "OOS_" + orderSeq));
        params.add(new BasicNameValuePair("description", description));
        httpRequest.setEntity(new UrlEncodedFormEntity(params, Consts.STR_UTF8));
        HttpClient client = new DefaultHttpClient();
        HttpResponse newResponse = client.execute(httpRequest);
        InputStream ip = null;
        int status = 0;
        try {
            ip = newResponse.getEntity().getContent();
            JSONObject result = new JSONObject(IOUtils.toString(ip));
            log.info("deduction:" + result.toString());
            if (result.getInt("statusCode") != 800)
                throw new BaseException(400, "DeductionFailed");
            JSONObject returnObj = result.getJSONObject("returnObj");
            status = returnObj.getInt("status");
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        } finally {
            if (ip != null)
                ip.close();
        }
        return status;
    }
    
    /**
     * 每天调用一次账单流水接口,amount 单位元
     * 
     * @param accountId
     * @param deposit
     * @param orderSeq
     * @param description
     * @return
     * @throws ClientProtocolException
     * @throws IOException
     * @throws BaseException
     * @throws SQLException
     * @throws JSONException
     */
    public static int payBill(String accountId, String userId, String serviceTag,
            String productTag, double amount) throws BaseException, IOException {
        String uri = protocol + "://" + host + ":" + port + "/postpaid/NewPostpaidBillBalanceHandler";
        HttpPost httpRequest = new HttpPost(uri);
        JSONObject body = new JSONObject();
        try {
            body.put("accountId", accountId);
            body.put("userId", userId);
            body.put("serviceTag", serviceTag);
            body.put("productTag", productTag);
            body.put("amount", amount);
        } catch (JSONException e1) {
            log.error(e1.getMessage(), e1);
            throw new BaseException();
        }
        log.info("the request body is:" + body.toString());
        List<NameValuePair> params = new ArrayList<NameValuePair>();
        params.add(new BasicNameValuePair("jsonMsg", body.toString()));
        httpRequest.setEntity(new UrlEncodedFormEntity(params, Consts.STR_UTF8));
        HttpClient client = new DefaultHttpClient();
        HttpResponse newResponse = client.execute(httpRequest);
        InputStream ip = null;
        int status = 0;
        try {
            ip = newResponse.getEntity().getContent();
            JSONObject result = new JSONObject(IOUtils.toString(ip));
            log.info("payBill:" + result.toString());
            status = result.getInt("statusCode");
            return status;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new BaseException(500, "PayBillFailed");
        } finally {
            if (ip != null)
                ip.close();
        }
    }
    
    public static void updateSession(String sessionId) throws ClientProtocolException, IOException {
        String uri = protocol + "://" + host + ":" + port + "/credential/updateVisitTime";
        HttpPost httpRequest = new HttpPost(uri);
        httpRequest.setHeader("sessionId", sessionId);
        HttpClient client = new DefaultHttpClient();
        HttpResponse newResponse = client.execute(httpRequest);
        InputStream ip = null;
        int status = 0;
        try {
            ip = newResponse.getEntity().getContent();
            JSONObject result = new JSONObject(IOUtils.toString(ip));
            log.info("updateSession:" + result.toString());
            status = result.getInt("statusCode");
            if (status != 800)
                throw new BaseException(400, "updateSessionFailed");
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        } finally {
            if (ip != null)
                ip.close();
        }
    }
    
    public static void querySession(String sessionId, String accountId)
            throws ClientProtocolException, IOException {
        String uri = protocol + "://" + host + ":" + port + "/credential/querySession";
        HttpPost httpRequest = new HttpPost(uri);
        httpRequest.setHeader("sessionId", sessionId);
        HttpClient client = new DefaultHttpClient();
        HttpResponse newResponse = client.execute(httpRequest);
        InputStream ip = null;
        int status = 0;
        try {
            ip = newResponse.getEntity().getContent();
            JSONObject result = new JSONObject(IOUtils.toString(ip));
            log.info("querySession:" + result.toString());
            status = result.getInt("statusCode");
            if (status != 800)
                throw new BaseException(400, "querySessionFailed");
            JSONObject returnObj = result.getJSONObject("returnObj");
            if (!accountId.equals(returnObj.getString("accountId")))
                throw new BaseException(400, "AccountIdNotMatch");
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        } finally {
            if (ip != null)
                ip.close();
        }
    }
    
    public static void deductionAll(String accountId, double deposit, String orderSeq,
            String description) throws ClientProtocolException, IOException, SQLException,
            BaseException {
        double balance = getOwnerBill(accountId);
        if (balance < deposit)
            throw new BaseException(400, "CashBalanceNotEnough");
        int status = deduction(accountId, deposit, orderSeq, description);
        if (status != 1016)
            throw new BaseException(400, "DeductFromGloblePortalFailed");
        else {
            log.info("deduct from globle portal success, account id is :" + accountId
                    + ", deposit is:" + deposit);
        }
    }
    
    public static void main(String args[]) throws Exception {
        // log.info(getOwnerBill("3f50df31f7c248439f489356fefa0bcf"));
        // OpUser.deductionAll("3f50df31f7c248439f489356fefa0bcf", 0.1, "1",
        // "21");// 单位元
        // log.info(getOwnerBill("3f50df31f7c248439f489356fefa0bcf"));
        // log.info(getOwnerInfo("3f50df31f7c248439f489356fefa0bcf"));
        // log.info(getOwnerInfo("12761886db7d4b8190e361746165212e"));
        // OwnerMeta owner=new OwnerMeta("3f50df31f7c248439f489356fefa0bcf");
        // getOwnerInfo("4ae4e89146a64b5bbf469ed9ae6d0eeb");
        // updateSession("00b2769dcf934f1891b97697db7efa97");
        // querySession("00b2769dcf934f1891b97697db7efa97",
        // "12eeb3556981440bbb6fcd1d9a9e7939");
        // log.info(getOwnerBill("80e25529ec1a43bda355fe7f12b89a52"));
        //registerUser("f960b0c71854449d9f0035bf392269ea", "2caa135a5f1542ca8ee8a83221c104a9", "testOrderId");
        // getOwnerBill("3f50df31f7c248439f489356fefa0bcf");
        // payBill("0469b71748354150a8f678e4101ac1c4",
        // "fc0d127971d840ce871af79a48dd8488", "OOS",
        // "OOS", 1.0);
         }
}