package cn.ctyun.oos.common;

public class OOSMgrAPIServerConsts {

    public static String HEAD_USER_EXIST = "oos-user-exist";
    /**
     * URI
     */
    public static String URI_USER = "/user";
    public static String URI_USER_COUNT = "/user/count";
    public static String URI_USER_ACTIVE = "/user/active";
    public static String URI_USER_FROZEN = "/user/frozen";
    public static String URI_USER_PWD = "/user/pwd";
    public static String URI_USER_MAXAKNUM = "/user/maxaknum";
    public static String URI_USER_PACKAGE = "/user/package";
    public static String URI_USER_ORDER = "/user/order";
    public static String URI_USER_LIST = "/user/list";
    public static String URI_BUCKET = "/bucket";
    public static String URI_PACKAGE = "/package";
    public static String URI_METADATA = "/meta";
    public static String URI_METADATA_IP = "/meta/ip";

    public static String URI_CLOUD = "/cloud";
    public static String URI_CLOUD_DATANODE = "/cloud/datanode";
    public static String URI_CLOUD_THRESHOLD = "/cloud/threshold";
    public static String URI_DATANODE = "/datanode";
    public static String URI_DATANODE_IPMI = "/datanode/ipmi";

    /**
     * http参数
     */
    public static String PARAM_KEY = "params";
    public static String PARAM_USERS = "users";
    public static String PARAM_TOTAL = "total";
    public static String PARAM_USER_ID = "userId";
    public static String PARAM_USER_NAME = "userName";
    public static String PARAM_USER_DISPLAY_NAME = "displayName";
    public static String PARAM_USER_PWD = "pwd";
    public static String PARAM_USER_OLD_PWD = "oldPwd";
    public static String PARAM_USER_EMAIL = "email";
    public static String PARAM_USER_COMPANY_NAME = "companyName";
    public static String PARAM_USER_COMPANY_ADDRESS = "companyAddress";
    public static String PARAM_USER_MOBILE_PHONE = "mobilePhone";
    public static String PARAM_USER_PHONE = "phone";
    public static String PARAM_USER_CURRENT_AKNUM = "currentAkNum";
    public static String PARAM_USER_MAX_AKNUM = "maxAkNum";
    public static String PARAM_USER_STORAGE = "storage";
    public static String PARAM_USER_USED_STORAGE = "usedStorage";
    public static String PARAM_USER_COUNT = "count";
    public static String PARAM_PKG_ID = "packageId";
    public static String PARAM_PKG_NAME = "packageName";
    public static String PARAM_PKG_STORAGE = "storage";
    public static String PARAM_PKG_FLOW = "flow";
    public static String PARAM_PKG_GHREQUEST = "ghRequest";
    public static String PARAM_PKG_OTHERREQUEST = "otherRequest";
    public static String PARAM_PKG_START = "packageStart";
    public static String PARAM_PKG_END = "packageEnd";
    public static String PARAM_PKG_DURATION = "duration";
    public static String PARAM_PKG_COSTPRICE = "costPrice";
    public static String PARAM_BUCKET_NAME = "bucketName";
    public static String PARAM_BUCKET_ACL = "bucketACL";
    public static String PARAM_BUCKET_ID = "bucketId";
    public static String PARAM_ORDER_DATE = "orderDate";
    public static String PARAM_ORDER_TOTAL_SIZE = "totalSize";
    public static String PARAM_ORDER_PEAK_SIZE = "peakSize";
    public static String PARAM_ORDER_UPLOAD = "upload";
    public static String PARAM_ORDER_FLOW = "flow";
    public static String PARAM_ORDER_GHREQUEST = "ghRequest";
    public static String PARAM_ORDER_OTHERREQUEST = "otherRequest";
    public static String PARAM_ORDER_STORAGE_PER_GB = "storagePerGB";
    public static String PARAM_ORDER_FLOW_PER_GB = "flowPerGB";
    public static String PARAM_ORDER_GHREQUEST_PER_TENTHOUS = "ghReqPerTenThous";
    public static String PARAM_ORDER_OTHERREQUEST_PER_TENTHOUS = "otherReqPerThous";
    public static String PARAM_ORDER_STORAGE_CONSUMPTION = "storageConsume";
    public static String PARAM_ORDER_FLOW_CONSUMPTION = "flowConsume";
    public static String PARAM_ORDER_GHREQUEST_CONSUMPTION = "ghReqConsume";
    public static String PARAM_ORDER_OTHERREQUEST_CONSUMPTION = "otherReqConsume";
    public static String PARAM_ORDER_TOTAL_CONSUMPTION = "totalConsume";
    public static String PARAM_METADATA_IP = "metaDataIp";

    /**
     * 状态码
     */
    public static final int CODE_GET = 200;
    public static final int CODE_PUT = 200;
    public static final int CODE_POST = 200;
    public static final int CODE_DELETE = 204;
    public static final int CODE_HEAD = 200;
    public static final int CODE_PARAM_ERROR = 400;
    public static final int CODE_CLIENT_ERROR = 400;
    public static final int CODE_SERVER_ERROR = 500;
    public static final int CODE_INVALID_REQUEST = 400;
}
