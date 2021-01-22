package cn.ctyun.oos.server.pay;

import java.io.File;
import java.util.Date;

import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.s3.internal.ServiceUtils;
import common.util.ConfigUtils;

public class BestPay {
    public static CompositeConfiguration config = null;
    private static final Log log = LogFactory.getLog(BestPay.class);
    static {
        File[] xmlConfs = {
                new File(System.getenv("OOS_HOME")
                        + "/conf/pay-online.xml"),
                new File(System.getenv("OOS_HOME")
                        + "/conf/pay-default.xml") };
        try {
            config = (CompositeConfiguration) ConfigUtils
                    .loadXmlConfig(xmlConfs);
        } catch (Exception e) {
            log.error(ServiceUtils.formatIso8601Date(new Date()) + " "
                    + e.getMessage());
        }
        if (config != null) {
            MERCHANTID = Long.parseLong(config.getString("MERCHANTID"));
            MERCHANTURL = config.getString("MERCHANTURL");
            BACKMERCHANTURL = config.getString("BACKMERCHANTURL");
            BUSICODE = config.getString("BUSICODE");
            PRODUCTID = config.getString("PRODUCTID");
            TMNUM = config.getString("TMNUM");
            KEY = config.getString("KEY");
            URL = config.getString("URL");
            ATTACH = config.getString("ATTACH");
            PRODUCTDESC = config.getString("PRODUCTDESC");
            CURTYPE = config.getString("CURTYPE");
        }
    }
    public static long MERCHANTID;
    public static int ENCODETYPE = 1;
    public static String CURTYPE;
    public static String MERCHANTURL;
    public static String BACKMERCHANTURL;
    public static String BUSICODE;
    public static String PRODUCTID;
    public static String TMNUM;
    public static String PRODUCTDESC;
    public static String KEY;
    public static String ATTACH;
    public static String URL;
}