package cn.ctyun.oos.bssAdapter.server;

import org.apache.commons.lang.StringUtils;

import cn.ctyun.oos.server.conf.BssAdapterConfig;

public class BssAdapterConsts {
    public static String ACTION_CREATE_USER = "CreateUser";
    public static String ACTION_GET_USER = "SelectUser";
    public static String ACTION_CREATE_BUCKET = "CreateBucket";
    public static String ACTION_DELETE_BUCKET = "DeleteBucket";
    public static String ACTION_GET_POOLS = "GetAvailablePools";
    public static String ACTION_LOGOUT_REQUEST = "LogoutRequest";
    public static String ACTION_PERMISSION_REQUEST = "UserPermission";
    public static String ACTION_PUT_ORDERBILL = "SubmitOrderBill";
    public static String ACTION_REGISTER_USER_PROXY = "RegisterUserFromProxy";
    public static String ACTION_REGISTER_USER_BSS = "RegisterUserFromBSS";
    public static String ACTION_UPDATE_USER = "UpdateUser";
    public static String ACTION_PUT_ROLE = "UpdateRole";
    public static String ACTION_LOGIN_USER = "LoginUser";
    public static String ACTION_PUT_USER_SESSION = "RegisteUserSession";
    public static String ACTION_GET_USER_SESSION = "GetUserSession";
    public static String ACTION_REFRESH_USER_SESSION_EXPTIME = "RefreshUserSessionExpTime";//刷新session过期时间(用于其他资源池向中心刷新session)
    public static String ACTION_ACCEPT_OWNERMETA = "PutOwnerMeta";
    public static String ACTION_PUT_PACKAGE = "SubmitUserPackage";
    public static String ACTION_PUT_USERTOROLE = "UpdateUserToRole";
    public static String HEADER_OWNER = "x-amz-owner";
    public static String HEADER_BUCKET = "x-amz-bucket";
    public static String HEADER_POOL = "x-amz-pool";
    public static String ACTION_GET_DELETE_USERINFO = "GetDeleteUserInfo";
    public static final String ACTION_BSSPACKAGE_LIST = "ListBssPackage";

    public static String DEFAULT_CENTER_POOL_NAME = StringUtils
            .defaultIfEmpty(BssAdapterConfig.localPool.name, "BssAdapter");
}