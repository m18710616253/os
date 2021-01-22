package cn.ctyun.oos.server.pay;

import java.text.SimpleDateFormat;
import java.util.Date;

import cn.ctyun.oos.hbase.MetaClient;
import cn.ctyun.oos.metadata.OwnerMeta;
import cn.ctyun.oos.server.db.dbpay.DBPay;
import cn.ctyun.oos.server.db.dbpay.DbBill;

import common.time.TimeUtils;
import common.util.MD5Hash;

public class Bill {
    public static int count = 0001;
    private static MetaClient client = MetaClient.getGlobalClient();
    
    public static DbBill generalBill(long ownerId, double orderAmount, String clientIp,
            String orderSeq) throws Exception {
        OwnerMeta owner = new OwnerMeta(ownerId);// 不对ownerId为空做判断
        client.ownerSelectById(owner);
        DbBill bill = new DbBill();
        MERCHANTID = BestPay.MERCHANTID;
        bill.setMERCHANTID(MERCHANTID);
        SUBMERCHANTID = "0";
        bill.SUBMERCHANTID = SUBMERCHANTID;
        if (orderSeq != null)
            ORDERSEQ = orderSeq;
        else
            ORDERSEQ = "" + ownerId + System.nanoTime();
        bill.setORDERSEQ(ORDERSEQ);
        if (count < 10000)
            count++;
        else
            count = 0;
        ORDERREQTRANSEQ = orderReqTranseq() + count;
        bill.setORDERREQTRANSEQ(ORDERREQTRANSEQ);
        ORDERDATE = TimeUtils.toYYYYMMDD(new Date());
        bill.setORDERDATE(TimeUtils.toYYYYMMddHHmmss(new Date()));
        /* BestPay uses cent as the basic unit */
        ORDERAMOUNT = (long) (orderAmount * 100);
        bill.setORDERAMOUNT(ORDERAMOUNT);
        /* BestPay uses cent as the basic unit */
        PRODUCTAMOUNT = (long) (orderAmount * 100);
        bill.setPRODUCTAMOUNT(PRODUCTAMOUNT);
        ATTACHAMOUNT = 0;
        bill.setATTACHAMOUNT(ATTACHAMOUNT);
        CURTYPE = BestPay.CURTYPE;
        bill.setCURTYPE(CURTYPE);
        ENCODETYPE = BestPay.ENCODETYPE;
        bill.setENCODETYPE(ENCODETYPE);
        MERCHANTURL = BestPay.MERCHANTURL;
        bill.setMERCHANTURL(MERCHANTURL);
        BACKMERCHANTURL = BestPay.BACKMERCHANTURL;
        bill.setBACKMERCHANTURL(BACKMERCHANTURL);
        ATTACH = BestPay.ATTACH;
        bill.ATTACH = ATTACH;
        BUSICODE = BestPay.BUSICODE;
        bill.setBUSICODE(BUSICODE);
        PRODUCTID = BestPay.PRODUCTID;
        bill.setPRODUCTID(PRODUCTID);
        if ((owner.mobilePhone != null) && (!owner.mobilePhone.equals(""))) {
            TMNUM = owner.mobilePhone;// customer mobilephone number
            bill.setTMNUM(TMNUM);
        } else if ((owner.phone != null) && (!owner.phone.equals(""))) {
            TMNUM = owner.phone;// customer phone number
            bill.setTMNUM(TMNUM);
        } else {
            TMNUM = BestPay.TMNUM;
            bill.setTMNUM(TMNUM);
        }
        CUSTOMERID = String.valueOf(ownerId);
        bill.setCUSTOMERID(CUSTOMERID);
        PRODUCTDESC = BestPay.PRODUCTDESC;
        bill.setPRODUCTDESC(PRODUCTDESC);
        CLIENTIP = clientIp;
        bill.setCLIENTIP(CLIENTIP);
        KEY = BestPay.KEY;
        MAC = getMAC();
        bill.setMAC(MAC);
        tag = 0;
        bill.tag = tag;
        DBPay dbp = DBPay.getInstance();
        dbp.billInsert(bill);
        return bill;
    }
    
    /**
     * MERCHANTID=123456789&ORDERSEQ=20060314000001&ORDERDATE=20060314&
     * ORDERAMOUNT=10000&CLIENTIP=127.0.0.1&KEY=123456
     */
    public static String getMAC() {
        StringBuffer buffer = new StringBuffer();
        buffer.append("MERCHANTID=" + MERCHANTID);
        buffer.append("&ORDERSEQ=" + ORDERSEQ);
        buffer.append("&ORDERDATE=" + ORDERDATE);
        buffer.append("&ORDERAMOUNT=" + ORDERAMOUNT);
        buffer.append("&CLIENTIP=" + CLIENTIP);
        buffer.append("&KEY=" + KEY);
        return MD5Hash.digest(buffer.toString()).toString().toUpperCase();
    }
    
    public static String orderReqTranseq() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddhhmmss");
        return sdf.format(new Date());
    }
    
    /* ID of merchant, related to the same variable in DbBill */
    public static long MERCHANTID;
    /* Sub ID of merchant */
    public static String SUBMERCHANTID;
    /* Order sequence */
    public static String ORDERSEQ;
    /* Order request transaction sequence, generate by merchant */
    public static String ORDERREQTRANSEQ;
    /* Date of order, format yyyyMMDD */
    public static String ORDERDATE;
    /* Amount money of order */
    public static long ORDERAMOUNT;
    /* Amount money of product */
    public static long PRODUCTAMOUNT;
    /* Attach amount of product default is 0 */
    public static long ATTACHAMOUNT;
    /* Currency type default RMB */
    public static String CURTYPE;
    /* Encode type, 0 for No encryption, 1 for encryption by MD5 */
    public static int ENCODETYPE;
    /* URL address for foreground, only for display in foreground */
    public static String MERCHANTURL;
    /* Backend URL address for transaction processing */
    public static String BACKMERCHANTURL;
    /* Attach information for merchant */
    public static String ATTACH;
    /* Business type, 0001 is default */
    public static String BUSICODE;
    /* Product id for service identifier */
    public static String PRODUCTID;
    /* Terminal number */
    public static String TMNUM;
    /* Customer identification */
    public static String CUSTOMERID;
    /* Description of product */
    public static String PRODUCTDESC;
    /* Check domain, for data integrity */
    public static String MAC;
    /* IP address of merchant */
    public static String CLIENTIP;
    /* use tag to identify the current status of bill */
    public static int tag;
    /* Related to the key of merchant */
    private static String KEY;
}