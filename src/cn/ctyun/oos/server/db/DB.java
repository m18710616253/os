package cn.ctyun.oos.server.db;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;

import org.json.JSONException;
import org.json.JSONObject;

import cn.ctyun.common.db.DbSource;
import cn.ctyun.oos.bssAdapter.orderbill.sender.OrderMessage;

/**
 * @author: Jiang Feng
 */
public class DB {
    protected static DB THIS = new DB();
    
    protected DB() {
    }
    
    public static DB getInstance() {
        return THIS;
    }
    
    /**
     * Purpose: during develop period, invoke the method in DB() to finish
     * inition。 during test period, use test case to rewrite this method to
     * create a demo database
     * 
     * @throws SQLException
     * 
     **/
    public void init() throws SQLException {
        // Connection conn = dataSource.getConnection();
        // try {
        // // create();
        // DbBucketWebsite.create(conn);
        // DbInitialAccount.create(conn);
        // DbAccount.create(conn);
        // } finally {
        // conn.close();
        // }
    }
    
    /**
     * Purpose: during develop period, clear the last data
     * 
     * @throws SQLException
     */
    public void uninit() throws SQLException {
        // Connection conn = dataSource.getConnection();
        // try {
        // DbBucketWebsite.drop(conn);
        // DbInitialAccount.drop(conn);
        // DbAccount.drop(conn);
        // } finally {
        // conn.close();
        // }
    }
    
    public void create() throws SQLException {
        Connection conn = null;
        String create = "CREATE DATABASE IF NOT EXISTS oos DEFAULT CHARACTER SET utf8";
        try {
            conn = DbSource.getConnection();
            Statement st = conn.createStatement();
            try {
                st.execute(create);
            } finally {
                st.close();
            }
        } finally {
            if (conn != null)
                conn.close();
        }
    }
    
    public String selectPasswd(String origId) {
        return "secret";
    }
    
    // initial account
    public void initialAccountInsert(DbInitialAccount dbInitialAccount) throws SQLException {
        Connection conn = null;
        try {
            conn = DbSource.getConnection();
            dbInitialAccount.insert(conn);
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }
    
    public boolean initialAccountSelect(DbInitialAccount dbInitialAccount) throws SQLException {
        Connection conn = null;
        try {
            conn = DbSource.getConnection();
            return dbInitialAccount.select(conn);
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }
    
    public boolean initialAccountSelectByOwnerId(DbInitialAccount dbInitialAccount)
            throws SQLException {
        Connection conn = null;
        try {
            conn = DbSource.getConnection();
            return dbInitialAccount.selectByOwnerId(conn);
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }
    
    public long initialAccountSelectByUserId(DbInitialAccount dbInitialAccount)
            throws SQLException {
        Connection conn = null;
        try {
            conn = DbSource.getConnection();
            return dbInitialAccount.selectByUserId(conn);
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }
    
    // initial delete
    public void initialAccountDeleteByOwnerId(DbInitialAccount dbInitialAccount) throws SQLException {
        Connection conn = null;
        try {
            conn = DbSource.getConnection();
            dbInitialAccount.deleteByOwnerId(conn);
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }
    
    // bucketWebsite ops
    public void bucketWebsiteInsert(DbBucketWebsite bucketWebsite) throws SQLException {
        Connection conn = null;
        try {
            conn = DbSource.getConnection();
            bucketWebsite.insert(conn);
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }
    
    public void bucketWebsiteUpdate(DbBucketWebsite bucketWebsite) throws SQLException {
        Connection conn = null;
        try {
            conn = DbSource.getConnection();
            bucketWebsite.update(conn);
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }
    
    public boolean bucketWebsiteSelect(DbBucketWebsite bucketWebsite) throws SQLException {
        Connection conn = null;
        try {
            conn = DbSource.getConnection();
            return bucketWebsite.select(conn);
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }
    
    public void bucketWebsiteDelete(DbBucketWebsite bucketWebsite) throws SQLException {
        Connection conn = null;
        try {
            conn = DbSource.getConnection();
            bucketWebsite.delete(conn);
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }   
    
    public List<OrderMessage> orderBillSelect(DbOrderBill orderBill) throws SQLException, 
            InterruptedException, JSONException {
        Connection conn = null;
        try {
            conn = DbSource.getConnection();
            return orderBill.select(conn);
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }
       
    public HashMap<String,String> getOrderBillUsers() throws SQLException {
        Connection conn = null;
        try {
            conn = DbSource.getConnection();
            return DbInitialAccount.getInitialaccountUsers(conn);
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    } 
    
    public int getUserPackagePageCount() throws SQLException {
        Connection conn = null;
        try {
            conn = DbSource.getConnection();
            return DbBSSUserPackage.getTotalPageCount(conn);
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }
   
    public List<JSONObject> userePackageSelect(DbBSSUserPackage userPackage) throws SQLException, 
        InterruptedException, JSONException {
        Connection conn = null;
        try {
            conn = DbSource.getConnection();
            return userPackage.select(conn);
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }
}
