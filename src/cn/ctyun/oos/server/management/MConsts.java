package cn.ctyun.oos.server.management;

public class MConsts {
    public static int ORGANIZATION_ID_LENGTH = 10;
    public static int ORGANIZATION_NAME_LENGTH = 50;
    public static int SALESMAN_NAME_LENGTH = 50;
    public static int SALESMAN_EMAIL_LENGTH = 64;
    public static int SALESMAN_PHONE_LENGTH = 20;
    public static int SALESMAN_ID_LENGTH = 30;
    public static int SALESMAN_BESTPAYACCOUNT_LENGTH = 20;
    public static int SALESMAN_PASSWORD_MIN_LENGTH = 6;
    public static int SALESMAN_PASSWORD_MAX_LENGTH = 20;
    public static String ADMIN = "admin";
    public static int OWNER_COMMENT_MAX_LENGTH = 128;
    public static final String API_GET_USAGE = "GetUsage";
    public static final String API_GET_BW = "GetBandwidth";
    public static final String API_GET_ABW = "GetAvailBW";
    public static final String API_GET_CONNECTION = "GetConnection";
    public static final String API_GET_CAPACITY = "GetCapacity";
    public static final String API_GET_DELETE_CAPACITY = "GetDeleteCapacity";
    public static final String API_GET_TRAFFICS = "GetTraffics";
    public static final String API_GET_AVAILABLE_BANDWIDTH = "GetAvailableBandwidth";
    public static final String API_GET_REQUEST = "GetRequests";
    public static final String API_GET_RETURNCODE = "GetReturnCode";
    public static final String API_GET_CONCURRENT_CONNECTION = "GetConcurrentConnection";
    public static final String API_GET_BILLED_STORAGE_USAGE = "GetBilledStorageUsage";
    public static final String API_GET_RESTORE_CAPACITY = "GetRestoreCapacity";

    public static String API_LOCAL_HOST = "localhost";
    public static String API_POOLS_SPLIT = "\\+";
    public static int MAX_LIST_SIZE = 1000;
}