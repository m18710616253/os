package cn.ctyun.oos.server.pay;

import cn.ctyun.oos.server.db.dbpay.DBPay;
import cn.ctyun.oos.server.db.dbpay.DbAccount;
import cn.ctyun.oos.server.db.dbpay.DbBill;
import cn.ctyun.oos.server.db.dbpay.DbResponse;
import cn.ctyun.oos.server.db.dbprice.DBPrice;
import cn.ctyun.oos.server.db.dbprice.DbUserPackage;

import common.util.MD5Hash;

public class ResponseBill {
    public static String checkPayment(DbResponse dbResponse) throws Exception {
        if (verifySignature(dbResponse)) {
            DbBill bill = new DbBill();
            bill.setORDERSEQ(dbResponse.ORDERSEQ);
            DBPay dbp = DBPay.getInstance();
            dbp.billSelect(bill);
            if ((bill.getORDERAMOUNT() == dbResponse.ORDERAMOUNT)
                    && ((dbResponse.RETNCODE == "0000") || (dbResponse.RETNCODE.equals("0000")))) {
                DBPrice dbPrice = DBPrice.getInstance();
                DbUserPackage up = new DbUserPackage(dbResponse.ORDERSEQ);
                if (dbPrice.userPackageSelectByOrderId(up)) {
                    dbPrice.userPackageBillUpdate(up);
                    return "UPTRANSEQ_" + dbResponse.UPTRANSEQ;
                } else {
                    DbAccount dbAccount = new DbAccount(Long.parseLong(bill.getCUSTOMERID()));
                    dbAccount.orderSeq = dbResponse.ORDERSEQ;
                    dbp.accountInsert(dbAccount, dbResponse);
                    return "UPTRANSEQ_" + dbResponse.UPTRANSEQ;
                }
            }
        }// end of outside if
        return null;
    }
    
    public static boolean verifySignature(DbResponse dbResponse) {
        /**
         * UPTRANSEQ=20080101000001&MERCHANTID=0250000001&ORDERID(ORDERSEQ)=
         * 2006050112564931556
         * &PAYMENT(ORDERAMOUNT)=10000&RETNCODE=0000&RETNINFO
         * =0000&PAYDATE(TRANDATE)= 20060101&KEY=123456
         */
        StringBuffer bf = new StringBuffer();
        bf.append("UPTRANSEQ=" + dbResponse.UPTRANSEQ);
        bf.append("&MERCHANTID=" + BestPay.MERCHANTID);
        bf.append("&ORDERID=" + dbResponse.ORDERSEQ);
        bf.append("&PAYMENT=" + dbResponse.ORDERAMOUNT);
        bf.append("&RETNCODE=" + dbResponse.RETNCODE);
        bf.append("&RETNINFO=" + dbResponse.RETNINFO);
        bf.append("&PAYDATE=" + dbResponse.TRANDATE);
        bf.append("&KEY=" + BestPay.KEY);
        String vs = MD5Hash.digest(bf.toString()).toString().toUpperCase();
        if ((vs.equals(dbResponse.SIGN)) || (vs == dbResponse.SIGN))
            return true;
        return false;
    }
}
