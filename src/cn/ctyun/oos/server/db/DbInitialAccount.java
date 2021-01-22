package cn.ctyun.oos.server.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import cn.ctyun.common.db.JdbcRow;
import common.tuple.Pair;

public class DbInitialAccount implements JdbcRow {
    public String accountId;
    public String userId;
    public String orderId;
    public long ownerId;
    
    public DbInitialAccount(String accountId) {
        this.accountId = accountId;
    }
    
    public DbInitialAccount() {
    }
    
    @Override
    public void setBy(ResultSet resultSet, boolean ignore) throws SQLException {
        ResultSetMetaData metadata = resultSet.getMetaData();
        for (int i = 1; i <= metadata.getColumnCount(); i++) {
            String aname = metadata.getColumnName(i);
            if (aname.equals("accountId")) {
                accountId = resultSet.getString(i);
            } else if (aname.equals("userId")) {
                userId = resultSet.getString(i);
            } else if (aname.equals("orderId")) {
                orderId = resultSet.getString(i);
            } else if (aname.equals("ownerId")) {
                ownerId = resultSet.getLong(i);
            } else {
                if (!ignore)
                    throw new RuntimeException("Unknown column name : " + aname);
            }
        }
    }
    
    @Override
    public boolean select(Connection conn) throws SQLException {
        PreparedStatement st = conn.prepareStatement("SELECT *  FROM " + getTableName()
                + " WHERE accountId=?");
        try {
            st.setString(1, accountId);
            ResultSet rs = st.executeQuery();
            try {
                boolean ret = rs.next();
                if (ret) {
                    setBy(rs, false);
                    assert (!rs.next());
                    return true;
                }
                return false;
            } finally {
                rs.close();
            }
        } finally {
            st.close();
        }
    }
    
    public boolean selectByOwnerId(Connection conn) throws SQLException {
        PreparedStatement st = conn.prepareStatement("SELECT *  FROM " + getTableName()
                + " WHERE ownerId=?");
        try {
            st.setLong(1, ownerId);
            ResultSet rs = st.executeQuery();
            try {
                boolean ret = rs.next();
                if (ret) {
                    setBy(rs, false);
                    assert (!rs.next());
                    return true;
                }
                return false;
            } finally {
                rs.close();
            }
        } finally {
            st.close();
        }
    }
    
    public long selectByUserId(Connection conn) throws SQLException {
        PreparedStatement st = conn.prepareStatement("SELECT ownerId FROM " + getTableName()
                + " WHERE userId=?");
        try {
            st.setString(1, userId);
            ResultSet rs = st.executeQuery();
            try {
                boolean ret = rs.next();
                if (ret) {
                    setBy(rs, false);
                    assert (!rs.next());
                    return rs.getLong("ownerId");
                }
                return -1L;
            } finally {
                rs.close();
            }
        } finally {
            st.close();
        }
    }
    
    @Override
    public void insert(Connection conn) throws SQLException {
        PreparedStatement st = conn.prepareStatement("INSERT " + getTableName()
                + "( accountId,userId,orderId,ownerId) " + "VALUES ( ?, ?, ?,?)",
                PreparedStatement.RETURN_GENERATED_KEYS);
        try {
            st.setString(1, accountId);
            st.setString(2, userId);
            st.setString(3, orderId);
            st.setLong(4, ownerId);
            st.executeUpdate();
        } finally {
            st.close();
        }
    }
    
    public static void create(Connection conn) throws SQLException {
        String create = "CREATE TABLE `" + getTableName() + "` (\n"
                + "  `accountId` varchar(40) NOT NULL,\n" + "  `userId` varchar(40) NOT NULL,\n"
                + "  `orderId` varchar(40) NOT NULL, \n `ownerId` bigint(20) DEFAULT NULL,"
                + " PRIMARY KEY (`accountId`)\n" + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        Statement st = conn.createStatement();
        try {
            st.execute(create);
        } finally {
            st.close();
        }
    }
    
    @Override
    public void update(Connection conn) throws SQLException {
        PreparedStatement st = null;
        try {
            st = conn.prepareStatement("UPDATE " + getTableName() + " SET " + "userId=? "
                    + "WHERE accountId=? ");
            st.setString(1, userId);
            st.setString(2, accountId);
            st.executeUpdate();
        } finally {
            if (st != null)
                st.close();
        }
    }
    
    @Override
    public void delete(Connection conn) throws SQLException {
        PreparedStatement st = conn.prepareStatement("DELETE FROM " + getTableName()
                + " WHERE accountId=?");
        try {
            st.setString(1, accountId);
            st.executeUpdate();
        } finally {
            st.close();
        }
    }
    
    public static HashMap<String,String> getInitialaccountUsers(Connection conn) throws SQLException {
        HashMap<String,String> users = new HashMap<>();
        
        PreparedStatement st = conn.prepareStatement(
                  "SELECT ownerId,accountId,userId"
                + " FROM " + getTableName());
        try {
            ResultSet rs = st.executeQuery();
            try {
                while(rs.next()) {
                    users.put(rs.getString("ownerId"),
                            rs.getString("accountId")
                            + "," + rs.getString("userId")
                    );
                }
                return users;
            } finally {
                rs.close();
            }
        } finally {
            st.close();
        }
    }
    
    public void deleteByOwnerId(Connection conn) throws SQLException {
        PreparedStatement st = conn.prepareStatement("DELETE FROM " + getTableName()
                + " WHERE ownerId=?");
        try {
            st.setLong(1, ownerId);
            st.executeUpdate();
        } finally {
            st.close();
        }
    }
    
    public static String getTableName() {
        return "initialaccount";
    }
    
    public static void drop(Connection conn) throws SQLException {
        Statement st = conn.createStatement();
        try {
            st.execute("DROP TABLE IF EXISTS " + getTableName());
        } finally {
            st.close();
        }
    }
}
