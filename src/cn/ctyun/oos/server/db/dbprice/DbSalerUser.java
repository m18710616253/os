package cn.ctyun.oos.server.db.dbprice;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;

import cn.ctyun.common.db.JdbcRow;

public class DbSalerUser implements JdbcRow {
    /* 客户经理id */
    public String salerId;
    /* 用户id */
    public long usrId;
    /* 客户经理所有的客户集合 */
    Set<Long> set = new HashSet<Long>();
    
    public DbSalerUser() {
    }
    
    public DbSalerUser(long usrId) {
        this.usrId = usrId;
    }
    
    @Override
    public void setBy(ResultSet resultSet, boolean ignore) throws SQLException {
        ResultSetMetaData metadata = resultSet.getMetaData();
        for (int i = 1; i <= metadata.getColumnCount(); i++) {
            String aname = metadata.getColumnName(i);
            if (aname.equals("salerId")) {
                salerId = resultSet.getString(i);
            } else if (aname.equals("usrId")) {
                usrId = resultSet.getLong(i);
            } else {
                if (!ignore)
                    throw new RuntimeException("Unknown column name : " + aname);
            }
        }// end of for
        set.add(usrId);
    }
    
    public static void create(Connection conn) throws SQLException {
        String create = "CREATE TABLE `" + getTableName() + "` (\n"
                + "  `salerId` VARCHAR(30) NOT NULL,\n"
                + "  `usrId` INT(20) UNSIGNED NOT NULL,\n"
                + "  PRIMARY KEY (`salerId`, `usrId`)\n"
                + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        Statement st = null;
        try {
            st = conn.createStatement();
            st.execute(create);
        } finally {
            if (st != null)
                st.close();
        }
    }
    
    @Override
    public boolean select(Connection conn) throws SQLException {
        PreparedStatement st = null;
        try {
            st = conn.prepareStatement("SELECT * FROM " + getTableName()
                    + " WHERE salerId=? ");
            st.setString(1, salerId);
            ResultSet rs = null;
            try {
                rs = st.executeQuery();
                while (rs.next()) {
                    setBy(rs, false);
                }
                return true;
            } finally {
                if (rs != null)
                    rs.close();
            }
        } finally {
            if (st != null)
                st.close();
        }
    }
    
    public boolean selectByUser(Connection conn) throws SQLException {
        PreparedStatement st = null;
        try {
            st = conn.prepareStatement("SELECT * FROM " + getTableName()
                    + " WHERE usrId=?");
            st.setLong(1, usrId);
            ResultSet rs = null;
            try {
                rs = st.executeQuery();
                boolean ret = rs.next();
                if (ret) {
                    setBy(rs, false);
                    assert (!rs.next());
                    return true;
                }
                return false;
            } finally {
                if (rs != null)
                    rs.close();
            }
        } finally {
            if (st != null)
                st.close();
        }
    }
    
    @Override
    public void insert(Connection conn) throws SQLException {
        PreparedStatement st = null;
        try {
            st = conn.prepareStatement("INSERT " + getTableName()
                    + "( salerId, usrId) " + "VALUES (?, ?)");
            st.setString(1, salerId);
            st.setLong(2, usrId);
            st.executeUpdate();
        } finally {
            if (st != null)
                st.close();
        }
    }
    
    @Override
    public void update(Connection conn) throws SQLException {
        PreparedStatement updateState = conn.prepareStatement("UPDATE "
                + getTableName() + " SET " + "salerId=? " + "WHERE usrId=? ");
        try {
            updateState.setString(1, salerId);
            updateState.setLong(2, usrId);
            updateState.executeUpdate();
        } finally {
            updateState.close();
        }
    }
    
    @Override
    public void delete(Connection conn) throws SQLException {
        PreparedStatement st = null;
        try {
            st = conn.prepareStatement("DELETE FROM " + getTableName()
                    + " WHERE usrId=?");
            st.setLong(1, usrId);
            st.executeUpdate();
        } finally {
            if (st != null)
                st.close();
        }
    }
    
    public static void drop(Connection conn) throws SQLException {
        Statement st = conn.createStatement();
        try {
            st.execute("DROP TABLE IF EXISTS `" + getTableName() + "`");
        } finally {
            st.close();
        }
    }
    
    public static String getTableName() {
        return "salerUser";
    }
}
