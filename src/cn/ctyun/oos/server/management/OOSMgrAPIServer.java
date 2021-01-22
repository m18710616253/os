package cn.ctyun.oos.server.management;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.Enumeration;
import java.util.List;
import java.util.Properties;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.server.nio.SelectChannelConnector;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.input.SAXBuilder;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.amazonaws.HttpMethod;
import com.amazonaws.services.s3.internal.ServiceUtils;

import cn.ctyun.common.BaseException;
import cn.ctyun.common.Consts;
import cn.ctyun.common.Program;
import cn.ctyun.common.conf.OOSConfig;
import cn.ctyun.oos.common.OOSMgrAPIServerConsts;
import cn.ctyun.oos.hbase.MetaClient;
import cn.ctyun.oos.metadata.BucketMeta;
import cn.ctyun.oos.metadata.OwnerMeta;
import cn.ctyun.oos.server.conf.OOSMgrAPIConfig;
import cn.ctyun.oos.server.db.dbpay.DBPay;
import cn.ctyun.oos.server.db.dbpay.DbOrder;
import cn.ctyun.oos.server.db.dbprice.DBPrice;
import cn.ctyun.oos.server.db.dbprice.DbPackage;
import cn.ctyun.oos.server.db.dbprice.DbUserPackage;
import cn.ctyun.oos.server.util.DateParser;
import cn.ctyun.oos.server.util.Misc;
import common.time.TimeUtils;

public class OOSMgrAPIServer extends AbstractHandler implements Program {
    static {
        System.setProperty("log4j.log.app", "oosMgrApiServer");
    }
    private static final Log log = LogFactory.getLog(OOSMgrAPIServer.class);
    private String baseUri;
    private static MetaClient metaClient = MetaClient.getGlobalClient();
    public OOSMgrAPIServer(){}
    
    public OOSMgrAPIServer(String baseUri){
        this.baseUri = baseUri;
    }
    
    @Override
    public void handle(String target, Request baseRequest,
            HttpServletRequest request, HttpServletResponse response)
            throws IOException, ServletException {
        try {
            log.info(log(request));
            if(null == target || !target.startsWith(baseUri)){
                response.setStatus(400);
                return;
            }else{
                target = target.replace(baseUri, "");
            }
            
            if (target.startsWith(OOSMgrAPIServerConsts.URI_USER)) {
                handleUserURI(target, request, response);
                return;
            }
            if (target.startsWith(OOSMgrAPIServerConsts.URI_BUCKET)) {
                handleBucketURI(target, request, response);
                return;
            }
            if (target.startsWith(OOSMgrAPIServerConsts.URI_PACKAGE)) {
                handlePackageURI(target, request, response);
                return;
            }
            if (target.startsWith(OOSMgrAPIServerConsts.URI_METADATA)) {
                handleMetaDataURI(target, request, response);
                return;
            }
            
            response.setStatus(OOSMgrAPIServerConsts.CODE_INVALID_REQUEST);

        } catch (Throwable t) {
            log.error(t.getMessage(), t);
            response.setStatus(500);
        } finally {
            baseRequest.setHandled(true);
        }
    }

    private void handleUserURI(String target, HttpServletRequest request,
            HttpServletResponse response) {

        String path = formatRequestURI(target);
        String method = request.getMethod();
        if (path.equalsIgnoreCase(OOSMgrAPIServerConsts.URI_USER)) {

            if (method.equals(HttpMethod.PUT.toString())) {
                putAddUser(request, response);
                return;
            }
            if (method.equals(HttpMethod.GET.toString())) {
                getUser(request, response);
                return;
            }
            if (method.equals(HttpMethod.POST.toString())) {
                postModifyUser(request, response);
                return;
            }
            if (method.equals(HttpMethod.HEAD.toString())) {
                headUserExist(request, response);
                return;
            }
        } else if (path.equalsIgnoreCase(OOSMgrAPIServerConsts.URI_USER_COUNT)) {

            if (method.equals(HttpMethod.GET.toString())) {
                getUserCount(request, response);
                return;
            }
        } else if (path.equalsIgnoreCase(OOSMgrAPIServerConsts.URI_USER_PWD)) {

            if (method.equals(HttpMethod.POST.toString())) {
                postModifyUserPwd(request, response);
                return;
            }
        } else if (path.equalsIgnoreCase(OOSMgrAPIServerConsts.URI_USER_MAXAKNUM)) {

            if (method.equals(HttpMethod.POST.toString())) {
                postModifyUserMaxAkNum(request, response);
                return;
            }
        } else if (path.equalsIgnoreCase(OOSMgrAPIServerConsts.URI_USER_PACKAGE)) {

            if (method.equals(HttpMethod.GET.toString())) {
                getUserPackage(request, response);
                return;
            }
            if (method.equals(HttpMethod.PUT.toString())) {
                putUserPackage(request, response);
                return;
            }
            if (method.equals(HttpMethod.POST.toString())) {
                postModifyUserPackage(request, response);
                return;
            }
        } else if (path.equalsIgnoreCase(OOSMgrAPIServerConsts.URI_USER_ACTIVE)) {

            if (method.equals(HttpMethod.PUT.toString())) {
                putActiveUser(request, response);
                return;
            }
        } else if (path.equalsIgnoreCase(OOSMgrAPIServerConsts.URI_USER_FROZEN)) {

            if (method.equals(HttpMethod.PUT.toString())) {
                putFrozenUser(request, response);
                return;
            }
            if (method.equals(HttpMethod.DELETE.toString())) {
                deleteFrozenUser(request, response);
                return;
            }
        } else if (path.equalsIgnoreCase(OOSMgrAPIServerConsts.URI_USER_ORDER)) {

            if (method.equals(HttpMethod.GET.toString())) {
                getUserOrder(request, response);
                return;
            }
        } else if (path.equalsIgnoreCase(OOSMgrAPIServerConsts.URI_USER_LIST)) {

            if (method.equals(HttpMethod.GET.toString())) {
                getUserList(request, response);
                return;
            }
        }
    }

    private void handleBucketURI(String target, HttpServletRequest request,
            HttpServletResponse response) {
        String path = formatRequestURI(target);
        String method = request.getMethod();
        if (path.equalsIgnoreCase(OOSMgrAPIServerConsts.URI_BUCKET)) {
            if (method.equals(HttpMethod.PUT.toString())) {
                putAddBucket(request, response);
                return;
            }
        }
    }

    private void handlePackageURI(String target, HttpServletRequest request,
            HttpServletResponse response) {
        String path = formatRequestURI(target);
        String method = request.getMethod();
        if (path.equalsIgnoreCase(OOSMgrAPIServerConsts.URI_PACKAGE)) {
            if (method.equals(HttpMethod.PUT.toString())) {
                putAddPkg(request, response);
                return;
            }
            if (method.equals(HttpMethod.GET.toString())) {
                getPkg(request, response);
                return;
            }
        }
    }

    private void handleMetaDataURI(String target, HttpServletRequest request,
            HttpServletResponse response) {
        String path = formatRequestURI(target);
        String method = request.getMethod();
        if (path.equalsIgnoreCase(OOSMgrAPIServerConsts.URI_METADATA_IP)) {
            if (method.equals(HttpMethod.GET.toString())) {
                getMetaDataIp(request, response);
                return;
            }
        }
    }

    /**
     * 添加用户
     * 
     * @param request
     * @param response
     */
    private void putAddUser(HttpServletRequest request,
            HttpServletResponse response) {

        try {
            baseCheck(request);
            JSONObject obj = new JSONObject(
                    request.getParameter(OOSMgrAPIServerConsts.PARAM_KEY));
            if (obj.isNull(OOSMgrAPIServerConsts.PARAM_USER_NAME)
                    || obj.isNull(OOSMgrAPIServerConsts.PARAM_USER_PWD))
                throw new BaseException(OOSMgrAPIServerConsts.CODE_PARAM_ERROR,
                        "InvalidArguments", "Parameter - "
                                + OOSMgrAPIServerConsts.PARAM_USER_NAME + ", "
                                + OOSMgrAPIServerConsts.PARAM_USER_PWD
                                + " can not be null.");
            OwnerMeta owner = new OwnerMeta(
                    obj.getString(OOSMgrAPIServerConsts.PARAM_USER_NAME));
            if (metaClient.ownerSelect(owner))
                throw new BaseException(OOSMgrAPIServerConsts.CODE_CLIENT_ERROR,
                        "UserAlreadyExist",
                        "The specified user has already been exist.");
            owner.setPwd(obj.getString(OOSMgrAPIServerConsts.PARAM_USER_NAME));
            if (!obj.isNull(OOSMgrAPIServerConsts.PARAM_USER_DISPLAY_NAME))
                owner.displayName = obj
                        .getString(OOSMgrAPIServerConsts.PARAM_USER_DISPLAY_NAME);
            if (!obj.isNull(OOSMgrAPIServerConsts.PARAM_USER_EMAIL))
                owner.email = obj.getString(OOSMgrAPIServerConsts.PARAM_USER_EMAIL);
            if (!obj.isNull(OOSMgrAPIServerConsts.PARAM_USER_COMPANY_NAME))
                owner.companyName = obj
                        .getString(OOSMgrAPIServerConsts.PARAM_USER_COMPANY_NAME);
            if (!obj.isNull(OOSMgrAPIServerConsts.PARAM_USER_COMPANY_ADDRESS))
                owner.companyAddress = obj
                        .getString(OOSMgrAPIServerConsts.PARAM_USER_COMPANY_ADDRESS);
            if (!obj.isNull(OOSMgrAPIServerConsts.PARAM_USER_MOBILE_PHONE))
                owner.mobilePhone = obj
                        .getString(OOSMgrAPIServerConsts.PARAM_USER_MOBILE_PHONE);
            if (!obj.isNull(OOSMgrAPIServerConsts.PARAM_USER_PHONE))
                owner.phone = obj.getString(OOSMgrAPIServerConsts.PARAM_USER_PHONE);

           
            owner.maxAKNum = Consts.MAX_AK_NUM;
            owner.type = OwnerMeta.TYPE_OOS;
            metaClient.ownerInsert(owner);

            JSONObject uobj = new JSONObject();
            uobj.put(OOSMgrAPIServerConsts.PARAM_USER_ID, owner.getId());
            
            writeMsg(response, OOSMgrAPIServerConsts.CODE_PUT, uobj.toString());
        } catch (Throwable t) {
            handleError(t, response);
            return;
        }
    }

    /**
     * 查看用户属性
     * 
     * @param request
     * @param response
     */
    private void getUser(HttpServletRequest request,
            HttpServletResponse response) {
        try {
            baseCheck(request);
            JSONObject obj = new JSONObject(
                    request.getParameter(OOSMgrAPIServerConsts.PARAM_KEY));
            if (obj.isNull(OOSMgrAPIServerConsts.PARAM_USER_ID))
                throw new BaseException(OOSMgrAPIServerConsts.CODE_PARAM_ERROR,
                        "InvalidArguments", "Parameter - "
                                + OOSMgrAPIServerConsts.PARAM_USER_ID
                                + " can not be null.");
            OwnerMeta owner = new OwnerMeta(Long.parseLong(obj
                    .getString(OOSMgrAPIServerConsts.PARAM_USER_ID)));
            if (!metaClient.ownerSelectById(owner))
                throw new BaseException(OOSMgrAPIServerConsts.CODE_CLIENT_ERROR,
                        "NoSuchUser", "The specified user is not exist.");

            JSONObject uobj = new JSONObject();
            uobj.put(OOSMgrAPIServerConsts.PARAM_USER_ID, owner.getId());
            uobj.put(OOSMgrAPIServerConsts.PARAM_USER_NAME, owner.name);
            uobj.put(OOSMgrAPIServerConsts.PARAM_USER_DISPLAY_NAME, owner.displayName);
            uobj.put(OOSMgrAPIServerConsts.PARAM_USER_EMAIL, owner.email);
            uobj.put(OOSMgrAPIServerConsts.PARAM_USER_COMPANY_NAME, owner.companyName);
            uobj.put(OOSMgrAPIServerConsts.PARAM_USER_COMPANY_ADDRESS,
                    owner.companyAddress);
            uobj.put(OOSMgrAPIServerConsts.PARAM_USER_MOBILE_PHONE, owner.mobilePhone);
            uobj.put(OOSMgrAPIServerConsts.PARAM_USER_PHONE, owner.phone);
            uobj.put(OOSMgrAPIServerConsts.PARAM_USER_MAX_AKNUM, owner.maxAKNum);
            uobj.put(OOSMgrAPIServerConsts.PARAM_USER_CURRENT_AKNUM, owner.currentAKNum);

            writeMsg(response, OOSMgrAPIServerConsts.CODE_GET, uobj.toString());
            
        } catch (Throwable t) {
            handleError(t, response);
            return;
        }
    }

    /**
     * 修改用户属性
     * 
     * @param request
     * @param response
     */
    private void postModifyUser(HttpServletRequest request,
            HttpServletResponse response) {

        try {
            baseCheck(request);
            JSONObject obj = new JSONObject(
                    request.getParameter(OOSMgrAPIServerConsts.PARAM_KEY));
            if (obj.isNull(OOSMgrAPIServerConsts.PARAM_USER_ID))
                throw new BaseException(OOSMgrAPIServerConsts.CODE_PARAM_ERROR,
                        "InvalidArguments", "Parameter - "
                                + OOSMgrAPIServerConsts.PARAM_USER_ID
                                + " can not be null.");
            OwnerMeta owner = new OwnerMeta(obj.getLong(OOSMgrAPIServerConsts.PARAM_USER_ID));
            if (!metaClient.ownerSelectById(owner))
                throw new BaseException(OOSMgrAPIServerConsts.CODE_CLIENT_ERROR,
                        "NoSuchUser", "The specified user is not exist.");

            if (!obj.isNull(OOSMgrAPIServerConsts.PARAM_USER_DISPLAY_NAME))
                owner.displayName = obj
                        .getString(OOSMgrAPIServerConsts.PARAM_USER_DISPLAY_NAME);
            if (!obj.isNull(OOSMgrAPIServerConsts.PARAM_USER_EMAIL))
                owner.email = obj.getString(OOSMgrAPIServerConsts.PARAM_USER_EMAIL);
            if (!obj.isNull(OOSMgrAPIServerConsts.PARAM_USER_COMPANY_NAME))
                owner.companyName = obj
                        .getString(OOSMgrAPIServerConsts.PARAM_USER_COMPANY_NAME);
            if (!obj.isNull(OOSMgrAPIServerConsts.PARAM_USER_COMPANY_ADDRESS))
                owner.companyAddress = obj
                        .getString(OOSMgrAPIServerConsts.PARAM_USER_COMPANY_ADDRESS);
            if (!obj.isNull(OOSMgrAPIServerConsts.PARAM_USER_MOBILE_PHONE))
                owner.mobilePhone = obj
                        .getString(OOSMgrAPIServerConsts.PARAM_USER_MOBILE_PHONE);
            if (!obj.isNull(OOSMgrAPIServerConsts.PARAM_USER_PHONE))
                owner.phone = obj.getString(OOSMgrAPIServerConsts.PARAM_USER_PHONE);

            metaClient.ownerUpdate(owner);
            response.setStatus(OOSMgrAPIServerConsts.CODE_POST);

        } catch (Throwable t) {
            handleError(t, response);
            return;
        }
    }

    /**
     * 查看用户是否存在
     * 
     * @param request
     * @param response
     */
    private void headUserExist(HttpServletRequest request,
            HttpServletResponse response) {

        try {
            baseCheck(request);
            JSONObject obj = new JSONObject(
                    request.getParameter(OOSMgrAPIServerConsts.PARAM_KEY));
            if (obj.isNull(OOSMgrAPIServerConsts.PARAM_USER_ID))
                throw new BaseException(OOSMgrAPIServerConsts.CODE_PARAM_ERROR,
                        "InvalidArguments", "Parameter - "
                                + OOSMgrAPIServerConsts.PARAM_USER_ID
                                + " can not be null.");
            OwnerMeta owner = new OwnerMeta(Long.parseLong(obj
                    .getString(OOSMgrAPIServerConsts.PARAM_USER_ID)));
            boolean exist = metaClient.ownerSelectById(owner);
            response.setHeader(OOSMgrAPIServerConsts.HEAD_USER_EXIST,
                    Boolean.toString(exist));
            response.setStatus(OOSMgrAPIServerConsts.CODE_HEAD);
        } catch (Throwable t) {
            handleError(t, response);
            return;
        }
    }

    /**
     * 修改用户密码
     * 
     * @param request
     * @param response
     */
    private void postModifyUserPwd(HttpServletRequest request,
            HttpServletResponse response) {

        try {
            baseCheck(request);
            JSONObject obj = new JSONObject(
                    request.getParameter(OOSMgrAPIServerConsts.PARAM_KEY));
            if (obj.isNull(OOSMgrAPIServerConsts.PARAM_USER_ID)
                    || obj.isNull(OOSMgrAPIServerConsts.PARAM_USER_OLD_PWD)
                    || obj.isNull(OOSMgrAPIServerConsts.PARAM_USER_PWD))
                throw new BaseException(OOSMgrAPIServerConsts.CODE_PARAM_ERROR,
                        "InvalidArguments", "Parameter - "
                                + OOSMgrAPIServerConsts.PARAM_USER_ID + ", "
                                + OOSMgrAPIServerConsts.PARAM_USER_OLD_PWD + ", "
                                + OOSMgrAPIServerConsts.PARAM_USER_PWD
                                + " can not be null.");
            OwnerMeta owner = new OwnerMeta(Long.parseLong(obj
                    .getString(OOSMgrAPIServerConsts.PARAM_USER_ID)));
            String oldPwd = obj.getString(OOSMgrAPIServerConsts.PARAM_USER_OLD_PWD);
            String newPwd = obj.getString(OOSMgrAPIServerConsts.PARAM_USER_PWD);
            if (!metaClient.ownerSelectById(owner))
                throw new BaseException(OOSMgrAPIServerConsts.CODE_CLIENT_ERROR,
                        "NoSuchUser", "The specified user is not exist.");

            if (!owner.getPwd().equals(oldPwd))
                throw new BaseException(OOSMgrAPIServerConsts.CODE_CLIENT_ERROR,
                        "OldPwdError", "The old password is not correct.");

            if (oldPwd.equals(newPwd))
                throw new BaseException(OOSMgrAPIServerConsts.CODE_CLIENT_ERROR,
                        "InvalidNewPwd",
                        "The new password can not be same as old password.");

            owner.setPwd(newPwd);
            metaClient.ownerUpdate(owner);
            response.setStatus(OOSMgrAPIServerConsts.CODE_POST);

        } catch (Throwable t) {
            handleError(t, response);
            return;
        }
    }

    /**
     * 修改用户最多AKSK对数
     * 
     * @param request
     * @param response
     */
    private void postModifyUserMaxAkNum(HttpServletRequest request,
            HttpServletResponse response) {

        try {
            baseCheck(request);
            JSONObject obj = new JSONObject(
                    request.getParameter(OOSMgrAPIServerConsts.PARAM_KEY));
            if (obj.isNull(OOSMgrAPIServerConsts.PARAM_USER_ID)
                    || obj.isNull(OOSMgrAPIServerConsts.PARAM_USER_MAX_AKNUM))
                throw new BaseException(OOSMgrAPIServerConsts.CODE_PARAM_ERROR,
                        "InvalidArguments",
                        "The specified parmeter can not be null.");
            long userId = Long.parseLong(obj
                    .getString(OOSMgrAPIServerConsts.PARAM_USER_ID));
            int maxAkNum = Integer.parseInt(obj
                    .getString(OOSMgrAPIServerConsts.PARAM_USER_MAX_AKNUM));
            OwnerMeta owner = new OwnerMeta(userId);
            if (!metaClient.ownerSelectById(owner))
                throw new BaseException(OOSMgrAPIServerConsts.CODE_CLIENT_ERROR,
                        "NoSuchUser", "The specified user is not exist.");
            if (maxAkNum <= 0)
                throw new BaseException(OOSMgrAPIServerConsts.CODE_CLIENT_ERROR,
                        "InvalidArguments", "Paremter - "
                                + OOSMgrAPIServerConsts.PARAM_USER_MAX_AKNUM
                                + " should be between 0 ~ " + Integer.MAX_VALUE);

            owner.maxAKNum = maxAkNum;
            metaClient.ownerUpdate(owner);
            response.setStatus(OOSMgrAPIServerConsts.CODE_POST);

        } catch (Throwable t) {
            handleError(t, response);
            return;
        }
    }

    /**
     * 获取用户套餐使用情况
     * 
     * @param request
     * @param response
     */
    private void getUserOrder(HttpServletRequest request,
            HttpServletResponse response) {

        try {
            baseCheck(request);
            DBPay dbPay = DBPay.getInstance();
            JSONObject obj = new JSONObject(
                    request.getParameter(OOSMgrAPIServerConsts.PARAM_KEY));
            String orderDate = "";
            if (obj.isNull(OOSMgrAPIServerConsts.PARAM_USER_ID))
                throw new BaseException(OOSMgrAPIServerConsts.CODE_PARAM_ERROR,
                        "InvalidArguments", "Parameter - "
                                + OOSMgrAPIServerConsts.PARAM_USER_ID
                                + " can not be null.");
            if (obj.isNull(OOSMgrAPIServerConsts.PARAM_ORDER_DATE)) {
                Calendar calendar = Calendar.getInstance();
                calendar.add(Calendar.DATE, -1);
                orderDate = TimeUtils.toYYYY_MM_dd(calendar.getTime());
            } else {
                orderDate = obj.getString(OOSMgrAPIServerConsts.PARAM_ORDER_DATE);
            }
            
            long userId = obj.getLong(OOSMgrAPIServerConsts.PARAM_USER_ID);
            OwnerMeta owner = new OwnerMeta(obj.getLong(OOSMgrAPIServerConsts.PARAM_USER_ID));
            if (!metaClient.ownerSelectById(owner))
                throw new BaseException(OOSMgrAPIServerConsts.CODE_CLIENT_ERROR,
                        "NoSuchUser", "The specified user is not exist.");
            DbOrder order = new DbOrder(userId, Consts.GLOBAL_DATA_REGION);
            order.date = orderDate;
            dbPay.orderSelectSum(order);
            JSONObject uobj = new JSONObject();
            uobj.put(OOSMgrAPIServerConsts.PARAM_ORDER_TOTAL_SIZE, order.totalSize);
            uobj.put(OOSMgrAPIServerConsts.PARAM_ORDER_PEAK_SIZE, order.peakSize);
            uobj.put(OOSMgrAPIServerConsts.PARAM_ORDER_UPLOAD, order.upload);
            uobj.put(OOSMgrAPIServerConsts.PARAM_ORDER_FLOW, order.flow);
            uobj.put(OOSMgrAPIServerConsts.PARAM_ORDER_GHREQUEST, order.ghRequest);
            uobj.put(OOSMgrAPIServerConsts.PARAM_ORDER_OTHERREQUEST, order.otherRequest);
            uobj.put(OOSMgrAPIServerConsts.PARAM_ORDER_STORAGE_PER_GB,
                    order.storagePerGB);
            uobj.put(OOSMgrAPIServerConsts.PARAM_ORDER_FLOW_PER_GB, order.flowPerGB);
            uobj.put(OOSMgrAPIServerConsts.PARAM_ORDER_GHREQUEST_PER_TENTHOUS,
                    order.ghReqPerTenThous);
            uobj.put(OOSMgrAPIServerConsts.PARAM_ORDER_OTHERREQUEST_PER_TENTHOUS,
                    order.otherReqPerThous);
            uobj.put(OOSMgrAPIServerConsts.PARAM_ORDER_STORAGE_CONSUMPTION,
                    order.storageConsume);
            uobj.put(OOSMgrAPIServerConsts.PARAM_ORDER_FLOW_CONSUMPTION,
                    order.flowConsume);
            uobj.put(OOSMgrAPIServerConsts.PARAM_ORDER_GHREQUEST_CONSUMPTION,
                    order.ghReqConsume);
            uobj.put(OOSMgrAPIServerConsts.PARAM_ORDER_OTHERREQUEST_CONSUMPTION,
                    order.otherReqConsume);
            uobj.put(OOSMgrAPIServerConsts.PARAM_ORDER_TOTAL_CONSUMPTION,
                    order.totalConsume);

            writeMsg(response, OOSMgrAPIServerConsts.CODE_GET, uobj.toString());

        } catch (Throwable t) {
            handleError(t, response);
            return;
        }
    }

    /**
     * 查看用户列表
     * 
     * @param request
     * @param response
     */
    private void getUserList(HttpServletRequest request,
            HttpServletResponse response) {
        try {
            List<OwnerMeta> ownerList = metaClient.ownerList(null,1000);
            int total = 0;
            JSONObject obj = new JSONObject();
            JSONArray arr = new JSONArray();
            if (null != ownerList) {
                for (OwnerMeta o : ownerList) {
                    JSONObject uobj = new JSONObject();
                    uobj.put(OOSMgrAPIServerConsts.PARAM_USER_ID, o.getId());
                    uobj.put(OOSMgrAPIServerConsts.PARAM_USER_NAME, o.name);
                    uobj.put(OOSMgrAPIServerConsts.PARAM_USER_DISPLAY_NAME,
                            o.displayName);
                    uobj.put(OOSMgrAPIServerConsts.PARAM_USER_EMAIL, o.email);
                    uobj.put(OOSMgrAPIServerConsts.PARAM_USER_COMPANY_NAME,
                            o.companyName);
                    uobj.put(OOSMgrAPIServerConsts.PARAM_USER_COMPANY_ADDRESS,
                            o.companyAddress);
                    uobj.put(OOSMgrAPIServerConsts.PARAM_USER_MOBILE_PHONE,
                            o.mobilePhone);
                    uobj.put(OOSMgrAPIServerConsts.PARAM_USER_PHONE, o.phone);
                    uobj.put(OOSMgrAPIServerConsts.PARAM_USER_MAX_AKNUM, o.maxAKNum);
                    uobj.put(OOSMgrAPIServerConsts.PARAM_USER_CURRENT_AKNUM,
                            o.currentAKNum);
                    arr.put(uobj);
                }
                obj.put(OOSMgrAPIServerConsts.PARAM_USERS, arr);
                total = ownerList.size();
            }
            obj.put(OOSMgrAPIServerConsts.PARAM_TOTAL, total);
            
            writeMsg(response, OOSMgrAPIServerConsts.CODE_GET, obj.toString());
            
        } catch (Throwable t) {
            handleError(t, response);
            return;
        }
    }

    /**
     * 激活用户
     * 
     * @param request
     * @param response
     */
    private void putActiveUser(HttpServletRequest request,
            HttpServletResponse response) {
        try {
            baseCheck(request);
            JSONObject obj = new JSONObject(
                    request.getParameter(OOSMgrAPIServerConsts.PARAM_KEY));
            if (obj.isNull(OOSMgrAPIServerConsts.PARAM_USER_ID))
                throw new BaseException(OOSMgrAPIServerConsts.CODE_PARAM_ERROR,
                        "InvalidArguments", "Parameter - "
                                + OOSMgrAPIServerConsts.PARAM_USER_ID
                                + " can not be null.");
            OwnerMeta owner = new OwnerMeta(obj.getLong(OOSMgrAPIServerConsts.PARAM_USER_ID));
            if (!metaClient.ownerSelectById(owner))
                throw new BaseException(OOSMgrAPIServerConsts.CODE_CLIENT_ERROR,
                        "NoSuchUser", "The specified user is not exist.");

            owner.verify = null;
            metaClient.ownerUpdate(owner);
            response.setStatus(OOSMgrAPIServerConsts.CODE_PUT);

        } catch (Throwable t) {
            handleError(t, response);
            return;
        }
    }

    /**
     * 冻结用户
     * 
     * @param request
     * @param response
     */
    private void putFrozenUser(HttpServletRequest request,
            HttpServletResponse response) {
        try {
            baseCheck(request);
            JSONObject obj = new JSONObject(
                    request.getParameter(OOSMgrAPIServerConsts.PARAM_KEY));
            if (obj.isNull(OOSMgrAPIServerConsts.PARAM_USER_ID))
                throw new BaseException(OOSMgrAPIServerConsts.CODE_PARAM_ERROR,
                        "InvalidArguments", "Parameter - "
                                + OOSMgrAPIServerConsts.PARAM_USER_ID
                                + " can not be null.");
            OwnerMeta owner = new OwnerMeta(Long.parseLong(obj
                    .getString(OOSMgrAPIServerConsts.PARAM_USER_ID)));
            if (!metaClient.ownerSelectById(owner))
                throw new BaseException(OOSMgrAPIServerConsts.CODE_CLIENT_ERROR,
                        "NoSuchUser", "The specified user is not exist.");

            owner.frozenDate = ServiceUtils.formatIso8601Date(DateUtils
                    .addDays(new Date(), -1));
            metaClient.ownerUpdate(owner);
            response.setStatus(OOSMgrAPIServerConsts.CODE_PUT);

        } catch (Throwable t) {
            handleError(t, response);
            return;
        }
    }

    /**
     * 解冻用户
     * 
     * @param request
     * @param response
     */
    private void deleteFrozenUser(HttpServletRequest request,
            HttpServletResponse response) {
        try {
            baseCheck(request);
            JSONObject obj = new JSONObject(
                    request.getParameter(OOSMgrAPIServerConsts.PARAM_KEY));
            if (obj.isNull(OOSMgrAPIServerConsts.PARAM_USER_ID))
                throw new BaseException(OOSMgrAPIServerConsts.CODE_PARAM_ERROR,
                        "InvalidArguments", "Parameter - "
                                + OOSMgrAPIServerConsts.PARAM_USER_ID
                                + " can not be null.");
            OwnerMeta owner = new OwnerMeta(Long.parseLong(obj
                    .getString(OOSMgrAPIServerConsts.PARAM_USER_ID)));
            if (!metaClient.ownerSelectById(owner))
                throw new BaseException(OOSMgrAPIServerConsts.CODE_CLIENT_ERROR,
                        "NoSuchUser", "The specified user is not exist.");

            owner.frozenDate = null;
            metaClient.ownerUpdate(owner);
            response.setStatus(OOSMgrAPIServerConsts.CODE_DELETE);

        } catch (Throwable t) {
            handleError(t, response);
            return;
        }
    }

    /**
     * 查看用户总个数
     * 
     * @param request
     * @param response
     */
    private void getUserCount(HttpServletRequest request,
            HttpServletResponse response) {
        try {
            List<OwnerMeta> ownerList = metaClient.ownerList(null,1000);
            JSONObject obj = new JSONObject();
            obj.put(OOSMgrAPIServerConsts.PARAM_USER_COUNT, ownerList.size());
            writeMsg(response, OOSMgrAPIServerConsts.CODE_GET, obj.toString());
        } catch (Throwable t) {
            handleError(t, response);
            return;
        }
    }

    /**
     * 为用户添加套餐 如果当前有有效的套餐，则添加套餐会失败
     * 
     * @param request
     * @param response
     */
    private void putUserPackage(HttpServletRequest request,
            HttpServletResponse response) {

        try {
            baseCheck(request);
            DBPrice dbPrice = DBPrice.getInstance();
            JSONObject obj = new JSONObject(
                    request.getParameter(OOSMgrAPIServerConsts.PARAM_KEY));
            if (obj.isNull(OOSMgrAPIServerConsts.PARAM_USER_ID)
                    || obj.isNull(OOSMgrAPIServerConsts.PARAM_PKG_ID))
                throw new BaseException(OOSMgrAPIServerConsts.CODE_PARAM_ERROR,
                        "InvalidArguments", "Parameter - "
                                + OOSMgrAPIServerConsts.PARAM_USER_ID + ", "
                                + OOSMgrAPIServerConsts.PARAM_PKG_ID
                                + " can not be null.");
            int pkgId = Integer.parseInt(obj
                    .getString(OOSMgrAPIServerConsts.PARAM_PKG_ID));
            long userId = Long.parseLong(obj
                    .getString(OOSMgrAPIServerConsts.PARAM_USER_ID));

            OwnerMeta owner = new OwnerMeta(userId);
            if (!metaClient.ownerSelectById(owner))
                throw new BaseException(OOSMgrAPIServerConsts.CODE_CLIENT_ERROR,
                        "NoSuchUser", "The specified user is not exist.");
            DbPackage pkg = new DbPackage(pkgId);
            if (!dbPrice.packageSelect(pkg))
                throw new BaseException(OOSMgrAPIServerConsts.CODE_CLIENT_ERROR,
                        "NoSuchPackage", "The specified package is not exist.");

            DbUserPackage userPkg = new DbUserPackage(userId);
            userPkg.isPaid = 1;
            userPkg.date = TimeUtils.toYYYY_MM_dd(new Date());
            if (dbPrice.userPackageSelect(userPkg))
                throw new BaseException(OOSMgrAPIServerConsts.CODE_CLIENT_ERROR,
                        "UserPkgAlreadyExist",
                        "The specified user has already used a package.");

            userPkg.startDate = userPkg.date;
            String endDate = TimeUtils
                    .toYYYY_MM_dd(DateUtils.addMonths(
                            DateParser.parseDate(userPkg.startDate),
                            (int) pkg.duration));
            userPkg.endDate = endDate;
            userPkg.packageId = pkg.packageId;
            userPkg.orderId = String.valueOf(owner.getId()) + System.nanoTime();
            userPkg.packageDiscount = pkg.discount;
            userPkg.packageStart = userPkg.startDate;
            userPkg.packageEnd = userPkg.endDate;
            userPkg.storage = pkg.storage;
            userPkg.flow = pkg.flow;
            userPkg.ghRequest = pkg.ghRequest;
            userPkg.otherRequest = pkg.otherRequest;
            userPkg.isPaid = 1;
            userPkg.salerId = "admin";
            dbPrice.userPackageInsert(userPkg);
            response.setStatus(OOSMgrAPIServerConsts.CODE_PUT);

        } catch (Throwable t) {
            handleError(t, response);
            return;
        }
    }

    /**
     * 修改用户套餐
     * 
     * @param request
     * @param response
     */
    private void postModifyUserPackage(HttpServletRequest request,
            HttpServletResponse response) {

        try {
            baseCheck(request);
            DBPrice dbPrice = DBPrice.getInstance();
            JSONObject obj = new JSONObject(
                    request.getParameter(OOSMgrAPIServerConsts.PARAM_KEY));
            if (obj.isNull(OOSMgrAPIServerConsts.PARAM_USER_ID)
                    || obj.isNull(OOSMgrAPIServerConsts.PARAM_PKG_ID))
                throw new BaseException(OOSMgrAPIServerConsts.CODE_PARAM_ERROR,
                        "InvalidArguments", "Parameter - "
                                + OOSMgrAPIServerConsts.PARAM_USER_ID + ", "
                                + OOSMgrAPIServerConsts.PARAM_PKG_ID
                                + " can not be null.");
            int pkgId = Integer.parseInt(obj
                    .getString(OOSMgrAPIServerConsts.PARAM_PKG_ID));
            long userId = Long.parseLong(obj
                    .getString(OOSMgrAPIServerConsts.PARAM_USER_ID));

            OwnerMeta owner = new OwnerMeta(userId);
            if (!metaClient.ownerSelectById(owner))
                throw new BaseException(OOSMgrAPIServerConsts.CODE_CLIENT_ERROR,
                        "NoSuchUser", "The specified user is not exist.");
            DbPackage pkg = new DbPackage(pkgId);
            if (!dbPrice.packageSelect(pkg))
                throw new BaseException(OOSMgrAPIServerConsts.CODE_CLIENT_ERROR,
                        "NoSuchPackage", "The specified package is not exist.");

            DbUserPackage userPkg = new DbUserPackage(userId);
            userPkg.isPaid = 1;
            userPkg.date = TimeUtils.toYYYY_MM_dd(new Date());
            if (!dbPrice.userPackageSelect(userPkg))
                throw new BaseException(OOSMgrAPIServerConsts.CODE_CLIENT_ERROR,
                        "UserPkgNotExist",
                        "The spcified user is not using any package.");

            String endDate = TimeUtils.toYYYY_MM_dd(DateUtils.addMonths(
                    DateParser.parseDate(userPkg.packageStart),
                    (int) pkg.duration));
            userPkg.endDate = endDate;
            userPkg.packageEnd = userPkg.endDate;
            userPkg.packageId = pkg.packageId;
            userPkg.packageDiscount = pkg.discount;
            userPkg.storage = pkg.storage;
            userPkg.flow = pkg.flow;
            userPkg.ghRequest = pkg.ghRequest;
            userPkg.otherRequest = pkg.otherRequest;
            dbPrice.userPackageUpdateAll(userPkg);
            response.setStatus(OOSMgrAPIServerConsts.CODE_PUT);

        } catch (Throwable t) {
            handleError(t, response);
            return;
        }
    }

    /**
     * 获取套餐信息
     * 
     * @param request
     * @param response
     */
    private void getUserPackage(HttpServletRequest request,
            HttpServletResponse response) {

        try {
            baseCheck(request);
            DBPrice dbPrice = DBPrice.getInstance();
            JSONObject obj = new JSONObject(
                    request.getParameter(OOSMgrAPIServerConsts.PARAM_KEY));
            if (obj.isNull(OOSMgrAPIServerConsts.PARAM_USER_ID))
                throw new BaseException(OOSMgrAPIServerConsts.CODE_PARAM_ERROR,
                        "InvalidArguments", "Parameter - "
                                + OOSMgrAPIServerConsts.PARAM_USER_ID
                                + " can not be null.");
            long userId = Long.parseLong(obj
                    .getString(OOSMgrAPIServerConsts.PARAM_USER_ID));

            OwnerMeta owner = new OwnerMeta(userId);
            if (!metaClient.ownerSelectById(owner))
                throw new BaseException(OOSMgrAPIServerConsts.CODE_CLIENT_ERROR,
                        "NoSuchUser", "The specified user is not exist.");

            DbUserPackage userPkg = new DbUserPackage(userId);
            userPkg.isPaid = 1;
            userPkg.date = TimeUtils.toYYYY_MM_dd(new Date());
            if (!dbPrice.userPackageSelect(userPkg))
                throw new BaseException(OOSMgrAPIServerConsts.CODE_CLIENT_ERROR,
                        "UserPkgNotExist",
                        "The spcified user is not using any package.");

            JSONObject uobj = new JSONObject();

            uobj.put(OOSMgrAPIServerConsts.PARAM_PKG_ID, userPkg.packageId);
            uobj.put(OOSMgrAPIServerConsts.PARAM_PKG_STORAGE, userPkg.storage);
            uobj.put(OOSMgrAPIServerConsts.PARAM_PKG_FLOW, userPkg.flow);
            uobj.put(OOSMgrAPIServerConsts.PARAM_PKG_GHREQUEST, userPkg.ghRequest);
            uobj.put(OOSMgrAPIServerConsts.PARAM_PKG_OTHERREQUEST, userPkg.otherRequest);
            uobj.put(OOSMgrAPIServerConsts.PARAM_PKG_START, userPkg.packageStart);
            uobj.put(OOSMgrAPIServerConsts.PARAM_PKG_END, userPkg.packageEnd);

            writeMsg(response, OOSMgrAPIServerConsts.CODE_GET, uobj.toString());
            
        } catch (Throwable t) {
            handleError(t, response);
            return;
        }
    }

    /**
     * 获取元数据的ip地址
     * 
     * @param request
     * @param response
     */
    @SuppressWarnings("unchecked")
    private void getMetaDataIp(HttpServletRequest request,
            HttpServletResponse response) {

        try {
            String metaDataIp = "";
            File hbaseConf = new File(System.getenv("OOS_HOME")
                    + "/conf/hbase-site.xml");
            SAXBuilder builder = new SAXBuilder();
            Document hbase = builder.build(hbaseConf);
            List<Element> propties = hbase.getRootElement().getChildren(
                    "property");
            for (Element e : propties) {
                if (e.getChild("name").getText()
                        .equals("hbase.zookeeper.quorum")) {
                    metaDataIp = e.getChild("value").getText();
                    break;
                }
            }

            JSONObject uobj = new JSONObject();
            uobj.put(OOSMgrAPIServerConsts.PARAM_METADATA_IP, metaDataIp);

            writeMsg(response, OOSMgrAPIServerConsts.CODE_GET, uobj.toString());

        } catch (Throwable t) {
            handleError(t, response);
            return;
        }
    }

    /**
     * 添加新套餐
     * 
     * @param request
     * @param response
     */
    private void putAddPkg(HttpServletRequest request,
            HttpServletResponse response) {

        try {
            baseCheck(request);
            DBPrice dbPrice = DBPrice.getInstance();
            JSONObject obj = new JSONObject(
                    request.getParameter(OOSMgrAPIServerConsts.PARAM_KEY));
            if (obj.isNull(OOSMgrAPIServerConsts.PARAM_PKG_NAME)
                    || obj.isNull(OOSMgrAPIServerConsts.PARAM_PKG_STORAGE)
                    || obj.isNull(OOSMgrAPIServerConsts.PARAM_PKG_FLOW)
                    || obj.isNull(OOSMgrAPIServerConsts.PARAM_PKG_GHREQUEST)
                    || obj.isNull(OOSMgrAPIServerConsts.PARAM_PKG_OTHERREQUEST)
                    || obj.isNull(OOSMgrAPIServerConsts.PARAM_PKG_DURATION))
                throw new BaseException(OOSMgrAPIServerConsts.CODE_PARAM_ERROR,
                        "InvalidArguments", "Parameter - "
                                + OOSMgrAPIServerConsts.PARAM_USER_ID + ", "
                                + OOSMgrAPIServerConsts.PARAM_PKG_STORAGE + ", "
                                + OOSMgrAPIServerConsts.PARAM_PKG_FLOW + ", "
                                + OOSMgrAPIServerConsts.PARAM_PKG_GHREQUEST + ", "
                                + OOSMgrAPIServerConsts.PARAM_PKG_OTHERREQUEST + ", "
                                + OOSMgrAPIServerConsts.PARAM_PKG_DURATION
                                + " can not be null.");
            String pkgName = obj.getString(OOSMgrAPIServerConsts.PARAM_PKG_NAME);
            long storage = obj.getLong(OOSMgrAPIServerConsts.PARAM_PKG_STORAGE);
            long flow = obj.getLong(OOSMgrAPIServerConsts.PARAM_PKG_FLOW);
            long ghRequest = obj.getLong(OOSMgrAPIServerConsts.PARAM_PKG_GHREQUEST);
            long otherRequest = obj
                    .getLong(OOSMgrAPIServerConsts.PARAM_PKG_OTHERREQUEST);
            int duration = obj.getInt(OOSMgrAPIServerConsts.PARAM_PKG_DURATION);
            if (storage < 0 || flow < 0 || ghRequest < 0 || duration < 0
                    || duration > 255)
                throw new BaseException(OOSMgrAPIServerConsts.CODE_PARAM_ERROR,
                        "InvalidArguments", "Parameter - "
                                + OOSMgrAPIServerConsts.PARAM_PKG_DURATION
                                + " - shuould be betwen 0 ~ 255.");

            DbPackage pkg = new DbPackage();
            pkg.name = pkgName;
            pkg.storage = storage;
            pkg.flow = flow;
            pkg.ghRequest = ghRequest;
            pkg.otherRequest = otherRequest;
            pkg.duration = duration;
            pkg.isValid = 1;
            dbPrice.packageInsert(pkg);

            JSONObject uobj = new JSONObject();
            uobj.put(OOSMgrAPIServerConsts.PARAM_PKG_ID, pkg.packageId);
            
            writeMsg(response, OOSMgrAPIServerConsts.CODE_PUT, uobj.toString());
            
        } catch (Throwable t) {
            handleError(t, response);
            return;
        }
    }

    /**
     * 查看套餐包
     * 
     * @param request
     * @param response
     */
    private void getPkg(HttpServletRequest request, HttpServletResponse response) {

        try {
            baseCheck(request);
            DBPrice dbPrice = DBPrice.getInstance();
            JSONObject obj = new JSONObject(
                    request.getParameter(OOSMgrAPIServerConsts.PARAM_KEY));
            if (obj.isNull(OOSMgrAPIServerConsts.PARAM_PKG_ID))
                throw new BaseException(OOSMgrAPIServerConsts.CODE_PARAM_ERROR,
                        "InvalidArguments", "Parameter - "
                                + OOSMgrAPIServerConsts.PARAM_PKG_ID
                                + " can not be null.");
            int pkgId = obj.getInt(OOSMgrAPIServerConsts.PARAM_PKG_ID);
            DbPackage pkg = new DbPackage(pkgId);
            if (!dbPrice.packageSelect(pkg))
                throw new BaseException(OOSMgrAPIServerConsts.CODE_CLIENT_ERROR,
                        "NoSuchPackage", "The specified package is not exist.");

            JSONObject uobj = new JSONObject();
            uobj.put(OOSMgrAPIServerConsts.PARAM_PKG_ID, pkg.packageId);
            uobj.put(OOSMgrAPIServerConsts.PARAM_PKG_NAME, pkg.name);
            uobj.put(OOSMgrAPIServerConsts.PARAM_PKG_STORAGE, pkg.storage);
            uobj.put(OOSMgrAPIServerConsts.PARAM_PKG_FLOW, pkg.flow);
            uobj.put(OOSMgrAPIServerConsts.PARAM_PKG_GHREQUEST, pkg.ghRequest);
            uobj.put(OOSMgrAPIServerConsts.PARAM_PKG_OTHERREQUEST, pkg.otherRequest);
            uobj.put(OOSMgrAPIServerConsts.PARAM_PKG_DURATION, pkg.duration);

            writeMsg(response, OOSMgrAPIServerConsts.CODE_GET, uobj.toString());
            
        } catch (Throwable t) {
            handleError(t, response);
            return;
        }
    }

    /**
     * 添加Bucket
     * 
     * @param request
     * @param response
     */
    private void putAddBucket(HttpServletRequest request,
            HttpServletResponse response) {

        try {
            baseCheck(request);
            JSONObject obj = new JSONObject(
                    request.getParameter(OOSMgrAPIServerConsts.PARAM_KEY));
            if (obj.isNull(OOSMgrAPIServerConsts.PARAM_USER_ID)
                    || obj.isNull(OOSMgrAPIServerConsts.PARAM_BUCKET_NAME)
                    || obj.isNull(OOSMgrAPIServerConsts.PARAM_BUCKET_ACL))
                throw new BaseException(OOSMgrAPIServerConsts.CODE_PARAM_ERROR,
                        "InvalidArguments", "Parameter - "
                                + OOSMgrAPIServerConsts.PARAM_USER_ID + ", "
                                + OOSMgrAPIServerConsts.PARAM_BUCKET_NAME + ", "
                                + OOSMgrAPIServerConsts.PARAM_BUCKET_ACL
                                + " can not be null.");
            OwnerMeta owner = new OwnerMeta(obj.getLong(OOSMgrAPIServerConsts.PARAM_USER_ID));
            if (!metaClient.ownerSelectById(owner))
                throw new BaseException(OOSMgrAPIServerConsts.CODE_CLIENT_ERROR,
                        "NoSuchUser", "The specified user is not exist.");
            String bucketName = obj.getString(OOSMgrAPIServerConsts.PARAM_BUCKET_NAME);
            int acl = obj.getInt(OOSMgrAPIServerConsts.PARAM_BUCKET_ACL);
            Misc.validateBucketName(bucketName,
                    OOSConfig.getInvalidBucketName());
            if (acl != BucketMeta.PERM_PRIVATE
                    && acl != BucketMeta.PERM_PUBLIC_READ
                    && acl != BucketMeta.PERM_PUBLIC_READ_WRITE)
                throw new BaseException(OOSMgrAPIServerConsts.CODE_PARAM_ERROR,
                        "InvalidArguments", "Parameter - "
                                + OOSMgrAPIServerConsts.CODE_PARAM_ERROR
                                + " should be one of [0, 1, 3].");

            BucketMeta bucket = new BucketMeta(bucketName);
            bucket.permission = acl;
            metaClient.bucketInsert(bucket);
            
            JSONObject uobj = new JSONObject();
            uobj.put(OOSMgrAPIServerConsts.PARAM_BUCKET_ID, bucket.getId());
            
            writeMsg(response, OOSMgrAPIServerConsts.CODE_PUT, uobj.toString());
        } catch (Throwable t) {
            handleError(t, response);
            return;
        }
    }

    @Override
    public String usage() {

        return null;
    }

    @Override
    public void exec(String[] args) throws Exception {
        Properties p = new Properties();
        File config = new File(System.getenv("OOS_HOME") + "/conf/global/oosMgrAPIServer.conf");
        p.load(new FileInputStream(config));
        Server server = new Server();
        server.setSendServerVersion(false);
        SelectChannelConnector connector = new SelectChannelConnector();
        QueuedThreadPool threadPool = new QueuedThreadPool();
        threadPool.setMinThreads(OOSMgrAPIConfig.minThreads);
        threadPool.setMaxThreads(OOSMgrAPIConfig.maxThreads);
        threadPool.setMaxQueued(OOSMgrAPIConfig.threadPoolQueue);
        connector.setThreadPool(threadPool);
        connector.setPort(OOSMgrAPIConfig.serverPort);
        connector.setMaxIdleTime(OOSMgrAPIConfig.maxIdleTime);
        connector.setRequestHeaderSize(OOSMgrAPIConfig.requestHeaderSize);
        String baseUri = OOSMgrAPIConfig.baseUri;
        server.setConnectors(new Connector[] { connector });
        server.setHandler(new OOSMgrAPIServer(baseUri));
        try {
            server.start();
            server.join();
        } catch (Throwable e) {
            log.error(e.getMessage(), e);
            System.exit(-1);
        }
    }

    private String formatRequestURI(String path) {

        if (StringUtils.isEmpty(path))
            return null;
        if (path.endsWith("/"))
            path = path.substring(0, path.length() - 1);
        return path;
    }

    private String getErrorDesc(String code, String msg, int status)
            throws JSONException {

        JSONObject obj = new JSONObject();
        obj.put("message", msg);
        obj.put("code", code);
        obj.put("status", status);
        JSONObject result = new JSONObject();
        result.put("error", obj);
        return result.toString();
    }

    /**
     * 对参数datafrom进行检查
     * 
     * @param request
     * @throws BaseException
     */
    private void baseCheck(HttpServletRequest request) throws BaseException {

        String param = request.getParameter(OOSMgrAPIServerConsts.PARAM_KEY);
        if (!request.getParameterMap().containsKey(OOSMgrAPIServerConsts.PARAM_KEY)
                || StringUtils.isEmpty(param))
            throw new BaseException(400, "InvalidArguments.", "Parameter - "
                    + OOSMgrAPIServerConsts.PARAM_KEY + " can not be null.");
    }
    
    /**
     * response的基本操作
     * @param response
     * @param code
     * @param msg
     * @throws IOException
     */
    private void writeMsg(HttpServletResponse response, int code, String msg) throws IOException{
        response.setStatus(code);
        response.setHeader("ContentType", "text/json");  
        response.setCharacterEncoding("utf-8"); 
        response.getWriter().write(msg);
        response.getWriter().flush();
    }

    /**
     * 统一错误处理
     * 
     * @param t
     * @param response
     */
    private void handleError(Throwable t, HttpServletResponse response) {
        try {
            String message = t.getMessage();
            String code = "InternalError";
            int status = OOSMgrAPIServerConsts.CODE_SERVER_ERROR;
            if (t instanceof BaseException) {
                code = ((BaseException) t).code;
                message = ((BaseException) t).message;
                status = ((BaseException) t).status;
            }
            log.error(message, t);
            
            writeMsg(response, status, getErrorDesc(code, message, status));
        } catch (Throwable t1) {
            log.error(t1.getMessage(), t1);
        }
    }
    
    private String log(HttpServletRequest req) {
        final StringBuilder sb = new StringBuilder(128);
        sb.append(req.getMethod()).append(" ");
        sb.append("\"").append(req.getRequestURI()).append("\" ");
        String k = null;
        String v = null;
        sb.append("Headers: ");
        Enumeration<?> e = req.getHeaderNames();
        while (e.hasMoreElements()) {
            k = (String) e.nextElement();
            v = req.getHeader(k);
            sb.append(k).append("=").append(v).append(" ");
        }
        sb.append("queryString:").append(req.getQueryString());
        return sb.toString();
    }

    public static void main(String[] args) throws Exception {
        new OOSMgrAPIServer().exec(args);
    }
}
