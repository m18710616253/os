package cn.ctyun.oos.server.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.json.JSONException;
import org.json.JSONObject;

import cn.ctyun.common.Consts;
import cn.ctyun.oos.bssAdapter.orderbill.sender.OrderMessage;
import cn.ctyun.oos.server.conf.BssAdapterConfig;

/**
 * @author:王伟 
 *  负责查询数据库中话单信息保存到内存中
 */
public class DbOrderBill {  
    public String beginDate;
    public String endDate;
    public int pageIndex;
    public static final int PAGE_COUNT = 1000;
    
    private static String lineSeparator = System.getProperty("line.separator", "\n");
    
    public DbOrderBill(int pageIndex,String beginDate,String endDate) {
        this.beginDate = beginDate;
        this.endDate = endDate;
        this.pageIndex = pageIndex;
    }
    
    public static int getTotalPageCount(Connection conn,String begin, String end) throws SQLException { 
        PreparedStatement st = conn.prepareStatement(
                "SELECT COUNT(1) as record_cnt              " + 
                "FROM   oos.initialaccount T1               " + 
                "       ,oos.order T2                       " + 
                "WHERE  T1.ownerId = T2.ownerId             " +        
                "AND    T2.dateBegin >= ?                   " +
                "AND    T2.dateEnd <= ?                     " + 
                "AND    T2.regionName= ?                    " );
        try {
            st.setString(1, begin);
            st.setString(2, end);
            st.setString(3, Consts.GLOBAL_DATA_REGION);
            ResultSet rs = st.executeQuery();
            try {
                boolean ret = rs.next();
                
                if (ret){
                   int count = rs.getInt(1); 
                   if (count == 0) return 0;
                   return ((count % PAGE_COUNT)==0)?count/PAGE_COUNT:count/PAGE_COUNT+1;
                }

                return -1;
            } finally {
                rs.close();
            }
        } finally {
            st.close();
        }
    }
     
    public List<OrderMessage> select(Connection conn) throws SQLException, InterruptedException, JSONException { 
    	int count=0;
    	List<OrderMessage> result = new ArrayList<OrderMessage>();
    	
        PreparedStatement st = conn.prepareStatement(
                "SELECT  T1.accountId           " +
                "       ,T1.userId              " +    
                "       ,T2.dateBegin           " +
                "       ,TRUNCATE(T2.PEAKSIZE/1024/1024/1024,4)     AS PEAKSIZE " +
                "       ,TRUNCATE(T2.FLOW/1024/1024/1024,4)         AS FLOW     " +
                "       ,TRUNCATE((T2.GHREQUEST)/10000,4) 			AS GH_TIMES  " +
                "       ,TRUNCATE((T2.OTHERREQUEST)/10000,4) 		AS OTHERS_TIMES  " +
                "FROM   oos.initialaccount T1       " + 
                "       ,oos.order T2               " + 
                "WHERE  T1.ownerId = T2.ownerId     " +        
                "AND    T2.dateBegin >= ?           " +
                "AND    T2.dateEnd <= ?             " + 
                "AND    T2.regionName= ?            " +
                "AND    T2.ID >= (SELECT TB.ID      " +
                "                 FROM   oos.initialaccount TA      " +
                "                        ,oos.order TB              " +
                "                 WHERE  TA.ownerId = TB.ownerId    " +
                "                 AND TB.dateBegin  >= ?            " +
                "                 AND TB.dateEnd    <= ?            " +
                "                 AND TB.regionName= ?              " +
                "                 ORDER BY TB.ID                    " +
                "                 LIMIT " + pageIndex*PAGE_COUNT +  ",1) " +
                "ORDER BY T2.ID                                          " +   
                "LIMIT  "+ PAGE_COUNT
                );
        try {
            st.setString(1, this.beginDate);
            st.setString(2, this.endDate);
            st.setString(3, Consts.GLOBAL_DATA_REGION);
            st.setString(4, this.beginDate);
            st.setString(5, this.endDate);
            st.setString(6, Consts.GLOBAL_DATA_REGION);
            ResultSet rs = st.executeQuery();
            try {
            	//JSONArray jsonOrders = new JSONArray();
            	OrderMessage msg = new OrderMessage(new ArrayList(),
            			BssAdapterConfig.asynchronizedRetryTimes);
                while (rs.next()){
                	count++;
                	JSONObject order = new JSONObject();
                	order.put("accountId", 	rs.getString("accountId"));
                	order.put("userId", 	rs.getString("userId"));
                	order.put("date", 		rs.getString("dateBegin"));
                	order.put("peakSize", 	rs.getDouble("PEAKSIZE"));
                	order.put("flow", 		rs.getDouble("FLOW"));
                	order.put("ghRequest", 	rs.getDouble("GH_TIMES"));
                	order.put("otherRequest", rs.getDouble("OTHERS_TIMES"));
                	
                	msg.getOrders().add(order);
                	
                	if ((count > 0 && count%500==0)) {
                	    result.add(msg);
                		msg = new OrderMessage(new ArrayList(),
                				BssAdapterConfig.asynchronizedRetryTimes);
                	}
                }
                
                //剩余的数量
                if (msg.getOrders().size()>0) {
                    result.add(msg);
                }
                
                return result;
            } finally {
                rs.close();
            }
        } finally {
            st.close();
        }
    }
}
