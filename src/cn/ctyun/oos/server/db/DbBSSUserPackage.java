package cn.ctyun.oos.server.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.json.JSONException;
import org.json.JSONObject;

/**
 * @author:王伟 
 *  负责查询数据库中套餐信息保存到内存中
 */
public class DbBSSUserPackage {  
    public int pageIndex;
    public static final int PAGE_COUNT = 1000;
    
    private static String lineSeparator = System.getProperty("line.separator", "\n");
    
    public DbBSSUserPackage(int pageIndex) {
        this.pageIndex = pageIndex;
    }
    
    public static int getTotalPageCount(Connection conn) throws SQLException { 
        PreparedStatement st = conn.prepareStatement(
                "SELECT COUNT(1) as record_cnt     " + 
                "FROM   oos.userPackage T2         ");
        try {
            ResultSet rs = st.executeQuery();
            try {
                boolean ret = rs.next();
                
                if (ret){
                   int count = rs.getInt(1); 
                   if (count == 0) return 0;
                   return ((count % PAGE_COUNT)==0)?count/PAGE_COUNT:count/PAGE_COUNT+1;
                }

                return 0;
            } finally {
                rs.close();
            }
        } finally {
            st.close();
        }
    }
     
    public List<JSONObject> select(Connection conn) throws SQLException, JSONException { 
    	int count=0;
    	
        PreparedStatement st = conn.prepareStatement(
                "SELECT  TA.ownerId                                                      " +
                "       ,TA.packageId                                                    " +  
                "       ,TA.packageStart                                                 " +  
                "       ,TA.packageEnd                                                   " +  
                "       ,TA.orderId                                                      " +  
                "       ,TA.isClose                                                      " +  
                "       ,IFNULL(TB.storage,0)                           storage          " +
                "       ,IFNULL(TB.flow,0)                              flow             " +
                "       ,IFNULL(TB.ghRequest,0)                         ghRequest        " +
                "       ,IFNULL(TB.otherRequest,0)                      otherRequest     " +
                "       ,IFNULL(TB.storage,0)       - TA.storage        usedStorage      " +    
                "       ,IFNULL(TB.flow,0)          - TA.flow           usedFlow         " +
                "       ,IFNULL(TB.ghRequest,0)     - TA.ghRequest      usedGhRequest    " +
                "       ,IFNULL(TB.otherRequest,0)  - TA.otherRequest   usedOtherRequest " +
                "FROM   (                           " + 
                "       SELECT  T2.usrId ownerId    " + 
                "               ,T2.packageId       " +        
                "               ,T2.packageStart    " +
                "               ,T2.packageEnd      " + 
                "               ,T2.storage         " +
                "               ,T2.flow            " +
                "               ,T2.ghRequest       " +
                "               ,T2.otherRequest    " +
                "               ,T2.orderId         " +
                "               ,T2.isClose         " +
                "       FROM    oos.userPackage T2  " +
                "       WHERE   T2.orderId >= (SELECT TD.orderId              " + 
                "                              FROM   oos.userPackage   TD    " + 
                "                              ORDER BY TD.orderId            " + 
                "                              LIMIT " + pageIndex*PAGE_COUNT +",1) " +
                ") TA                                   " +
                "  LEFT JOIN oos.package TB             " +
                "       ON   TA.packageId = TB.packageId" + 
                "  ORDER BY TA.orderId                    " +   
                "  LIMIT "+ PAGE_COUNT
                );
        try {
            ResultSet rs = st.executeQuery();
            try {
                List<JSONObject> list = new ArrayList<JSONObject>();
                while (rs.next()){
                	JSONObject order = new JSONObject();
                	order.put("ownerId",   rs.getLong("ownerId"));
                	order.put("packageId", rs.getLong("packageId"));
                	
                	order.put("storage",   rs.getLong("storage"));
                	order.put("flow", 	   rs.getLong("flow"));
                	order.put("ghRequest", rs.getLong("ghRequest"));
                	order.put("otherRequest",  rs.getLong("otherRequest"));
                	
                	order.put("usedStorage",   rs.getLong("usedStorage"));
                    order.put("usedFlow",      rs.getLong("usedFlow"));
                    order.put("usedGhRequest", rs.getLong("usedGhRequest"));
                    order.put("usedOtherRequest", rs.getLong("usedOtherRequest"));
                	
                    order.put("packageStart", rs.getString("packageStart"));
                    order.put("packageEnd", rs.getString("packageEnd"));
                    order.put("isClose", rs.getInt("isClose"));
                    
                	list.add(order); 
                } 
                
                return list;
            } finally {
                rs.close();
            }
        } finally {
            st.close();
        }
    }
}
