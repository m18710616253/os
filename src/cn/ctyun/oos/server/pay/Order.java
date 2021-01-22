package cn.ctyun.oos.server.pay;

import java.sql.SQLException;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import cn.ctyun.common.Program;
import cn.ctyun.oos.server.db.dbpay.DBPay;
import cn.ctyun.oos.server.db.dbpay.DbOrder;
import cn.ctyun.oos.server.util.DateParser;

/**
 * @author Dong chk
 * 
 */
/**
 * @author Administrator
 * 
 */
public class Order implements Program {
    private static final Log log = LogFactory.getLog(Order.class);
    
    public static void main(String[] args) throws Exception {
        new Order().exec(args);
    }
  
    
    /**
     * 用于查询详单
     * 
     * @param ownerId
     * @param beginDate
     * @param endDate
     * @param payed
     * @return 记录的集合，每条记录的格式是 |date dateBegin dateEnd totalSize peakSize flow
     *         ghRequest otherRequest storagePerGB flowPerGB ghReqPerTenThous
     *         otherReqPerThous storageConsume flowConsume ghReqConsume
     *         otherReqConsume totalConsume|
     * @throws SQLException
     */
    public Set<String> selectRange(long ownerId, String beginDate, String endDate, String regionName)
            throws SQLException {
        DBPay db = DBPay.getInstance();
        DbOrder dbOrder = new DbOrder(ownerId, regionName);
        dbOrder.dateBegin = beginDate;
        dbOrder.dateEnd = endDate;
        db.orderSelectRange(dbOrder);
        return dbOrder.orders;
    }

    /**
     * 获取每天所有用户统计量的总和
     * 
     * @param beginDate
     * @param endDate
     * @return 记录格式|date|totalSize|upload|flow|ghRequest|otherRequest|
     * @throws SQLException
     */
    public String[] selectSumByDate(String beginDate, String endDate, String regionName) throws SQLException {
        DBPay db = DBPay.getInstance();
        DbOrder dbOrder = new DbOrder();
        Set<String> set = DateParser.parseDate(beginDate, endDate);
        String[] result = new String[set.size() + 1];
        int i = 0;
        for (String date : set) {
            dbOrder.dateBegin = date;
            dbOrder.dateEnd = date;
            dbOrder.regionName = regionName;
            db.orderSelectSumByDay(dbOrder);
            result[i++] = dbOrder.order;
        }
        StringBuilder sb = new StringBuilder();
        sb.append(dbOrder.dateBegin).append(" ").append(dbOrder.maxPeakSize).append(" ")
                .append(dbOrder.maxUpload).append(" ").append(dbOrder.maxFlow).append(" ")
                .append(dbOrder.maxGhRequest).append(" ").append(dbOrder.maxOtherRequest)
                .append(" ").append(dbOrder.maxTotalConsume).append(" ")
                .append(dbOrder.minPeakSize).append(" ").append(dbOrder.maxOriginalPeakSize).append(" ")
                .append(dbOrder.minOriginalPeakSize).append(" ").append(dbOrder.maxRoamUpload)
                .append(" ").append(dbOrder.maxRoamFlow)
                .append(" ").append(dbOrder.maxNoNetFlow)
                .append(" ").append(dbOrder.maxNoNetUpload)
                .append(" ").append(dbOrder.maxNoNetRoamFlow)
                .append(" ").append(dbOrder.maxNoNetRoamUpload)
                .append(" ").append(dbOrder.maxNoNetGHReq)
                .append(" ").append(dbOrder.maxNoNetOtherReq)
                .append(" ").append(dbOrder.maxRedundantSize)
                .append(" ").append(dbOrder.maxAlinSize)
                .append(" ").append(dbOrder.minRedundantSize)
                .append(" ").append(dbOrder.minAlinSize)
                .append(" ").append(dbOrder.maxTotalGHReq)
                .append(" ").append(dbOrder.maxTotalOtherReq)
                .append(" ").append(dbOrder.maxTotalUpload)
                .append(" ").append(dbOrder.maxTotalFlow)
                .append(" ").append(dbOrder.maxTotalRoamUpload)
                .append(" ").append(dbOrder.maxTotalRoamFlow)
                .append(" ").append(dbOrder.maxSpamRequest)
                .append(" ").append(dbOrder.maxPornReviewFalse)
                .append(" ").append(dbOrder.maxPornReviewTrue)
                .append(" ").append(dbOrder.maxPorn);
        result[i] = sb.toString();
        return result;
    }
    
    @Override
    public String usage() {
        return "usage /n";
    }
    
    @Override
    public void exec(String[] args) throws Exception {
    }
}