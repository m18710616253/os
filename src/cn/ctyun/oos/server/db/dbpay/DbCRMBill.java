package cn.ctyun.oos.server.db.dbpay;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import cn.ctyun.common.db.JdbcRow;

/**
 * 用户账户表
 * 
 * @author Dong chk
 * 
 */
public class DbCRMBill implements JdbcRow {
    /* crm id, for identify the crm bill information */
    private long id;
    /* ownerId, used to identify the unique owner */
    public long ownerId;
    public String date = "";
    public double amount = 0;
    /* withdraw */
    /* 电商返回的status code，800成功，其他都是失败 */
    public int status = 0;
    
    public DbCRMBill(long ownerId, String date) {
        this.ownerId = ownerId;
        this.date = date;
    }
    
    public DbCRMBill() {
    }
    
    @Override
    public void setBy(ResultSet resultSet, boolean ignore) throws SQLException {
        ResultSetMetaData metadata = resultSet.getMetaData();
        for (int i = 1; i <= metadata.getColumnCount(); i++) {
            String oname = metadata.getColumnName(i);
            if (oname.equals("id")) {
                id = resultSet.getLong(i);
            } else if (oname.equals("ownerId")) {
                ownerId = resultSet.getLong(i);
            } else if (oname.equals("date")) {
                date = resultSet.getString(i);
            } else if (oname.equals("amount")) {
                amount = resultSet.getDouble(i);
            } else if (oname.equals("status")) {
                status = resultSet.getInt(i);
            } else {
                if (!ignore)
                    throw new RuntimeException("Unknown column name : " + oname);
            }
        }
    }
    
    @Override
    public boolean select(Connection conn) throws SQLException {
        PreparedStatement st = conn.prepareStatement("SELECT id, date, amount, status FROM `"
                + getTableName() + "` WHERE ownerId=? " + "AND " + "date=?");
        try {
            st.setLong(1, ownerId);
            st.setString(2, date);
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
        PreparedStatement st = conn.prepareStatement("SELECT id, date, amount, status FROM `"
                + getTableName() + "` WHERE ownerId=? order by date desc limit 1");
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
    
    @Override
    public void insert(Connection conn) throws SQLException {
        PreparedStatement st = conn.prepareStatement("INSERT `" + getTableName()
                + "` ( ownerId, date, amount, status) " + "VALUES ( ?, ?, ?, ?)");
        try {
            st.setLong(1, ownerId);
            st.setString(2, date);
            st.setDouble(3, amount);
            st.setDouble(4, status);
            st.executeUpdate();
        } finally {
            st.close();
        }
    }
    
    @Override
    public void update(Connection conn) throws SQLException {
    }
    
    @Override
    public void delete(Connection conn) throws SQLException {
        PreparedStatement st = conn.prepareStatement("DELETE FROM `" + getTableName()
                + "` WHERE ownerId=? " + "AND " + "date=? ");
        try {
            st.setLong(1, ownerId);
            st.setString(2, date);
            st.executeUpdate();
        } finally {
            st.close();
        }
    }
    
    public int batchDelete(Connection conn) throws SQLException {
        PreparedStatement st = conn.prepareStatement("DELETE FROM `" + getTableName()
                + "` WHERE ownerId=? "
                + "AND date<=? "
                + "ORDER BY id ASC LIMIT " + 100);
        try {
            st.setLong(1, ownerId);
            st.setString(2, date);
            return st.executeUpdate();
        } finally {
            st.close();
        }
    }
    
    public static void create(Connection conn) throws SQLException {
        String create = "CREATE TABLE IF NOT EXISTS`" + getTableName()
                + "`(`id`  BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT,\n"
                + "  `ownerId` bigint(20) NOT NULL,"
                + "  `amount` double(12,3) NOT NULL COMMENT '出账金额，单位元，两位小数',"
                + "   `date` varchar(20) NOT NULL COMMENT '格式yyyy-mm-dd hh:mm:ss',"
                + "  `status` int(3) NOT NULL COMMENT '电商返回的statusCode，800表示成功，500表示失败',"
                + " PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8";
        Statement st = conn.createStatement();
        try {
            st.execute(create);
        } finally {
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
        return "crmBill";
    }
}
