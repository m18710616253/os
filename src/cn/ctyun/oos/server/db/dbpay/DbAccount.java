package cn.ctyun.oos.server.db.dbpay;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedHashSet;

import cn.ctyun.common.db.JdbcRow;

/**
 * 用户账户表
 * 
 * @author Dong chk
 * 
 */
public class DbAccount implements JdbcRow {
    /* account id, for identify the account information */
    private long id;
    /* ownerId, used to identify the unique owner */
    private long ownerId;
    /* tradeDate, used to record withdraw and deposit time */
    public String tradeDate;
    /* deposit */
    public double deposit = 0;
    /* withdraw */
    public double withdraw = 0;
    /* balance */
    public double balance = 0;
    /* order number */
    public String orderSeq;
    // 指示是否是最新的账户信息，1表示是最新的，0表示历史数据
    public byte isLast = 1;
    public String dateBegin;
    public String dateEnd;
    public LinkedHashSet<String> records = new LinkedHashSet<String>();

    public long getOwnerId() {
        return ownerId;
    }

    public void setOwnerId(long ownerId) {
        this.ownerId = ownerId;
    }

    public DbAccount(long ownerId) {
        this.ownerId = ownerId;
    }

    public DbAccount() {
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
            } else if (oname.equals("tradeDate")) {
                tradeDate = resultSet.getString(i);
            } else if (oname.equals("balance")) {
                balance = resultSet.getDouble(i);
            } else if (oname.equals("deposit")) {
                deposit = resultSet.getDouble(i);
            } else if (oname.equals("withdraw")) {
                withdraw = resultSet.getDouble(i);
            } else if (oname.equals("orderSeq")) {
                orderSeq = resultSet.getString(i);
            } else if (oname.equals("isLast")) {
                isLast = resultSet.getByte(i);
            } else {
                if (!ignore)
                    throw new RuntimeException("Unknown column name : " + oname);
            }
        }
    }

    public void handle(ResultSet rs) throws SQLException {
        StringBuilder sb = new StringBuilder();
        sb.append(rs.getString("tradeDate")).append(" ")
                .append(rs.getDouble("deposit")).append(" ")
                .append(rs.getDouble("withdraw")).append(" ")
                .append(rs.getDouble("balance")).append(" ")
                .append(rs.getString("orderSeq"));
        records.add(sb.toString());
    }

    @Override
    public boolean select(Connection conn) throws SQLException {
        PreparedStatement st = conn
                .prepareStatement("SELECT id, tradeDate, balance, deposit, withdraw, orderSeq, isLast FROM `"
                        + getTableName()
                        + "` WHERE ownerId=? "
                        + "AND "
                        + "isLast=1");
        try {
            st.setLong(1, ownerId);
            ResultSet rs = st.executeQuery();
            try {
                boolean ret = rs.next();
                if (ret) {
                    setBy(rs, false);
                    assert (!rs.next()); // 只有一条记录是最新的
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

    public boolean selectById(Connection conn) throws SQLException {
        PreparedStatement st = conn
                .prepareStatement("SELECT ownerId, tradeDate, balance, deposit, withdraw, orderSeq FROM `"
                        + getTableName() + "` WHERE id=? ");
        try {
            st.setLong(1, id);
            ResultSet rs = st.executeQuery();
            try {
                boolean ret = rs.next();
                if (ret) {
                    setBy(rs, false);
                    assert (!rs.next()); // 根据主键的选择不可能出现两个结果
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

    private void handle2(ResultSet rs) throws SQLException {
        StringBuilder sb = new StringBuilder();
        sb.append(rs.getString("ownerId")).append(" ")
                .append(rs.getDouble("balance"));
        records.add(sb.toString());
    }

    public void selectAllBalance(Connection conn) throws SQLException {
        PreparedStatement st = conn
                .prepareStatement("SELECT ownerId, balance FROM `"
                        + getTableName() + "` WHERE isLast=1  ");
        try {
            ResultSet rs = st.executeQuery();
            try {
                while (rs.next()) {
                    handle2(rs);
                }
            } finally {
                rs.close();
            }
        } finally {
            st.close();
        }
    }

    public void selectRange(Connection conn) throws SQLException {
        PreparedStatement st = conn
                .prepareStatement("SELECT tradeDate, balance, deposit, withdraw, orderSeq FROM `"
                        + getTableName()
                        + "` WHERE ownerId=? "
                        + "AND "
                        + "tradeDate>=? " + "AND " + "tradeDate<?");
        try {
            st.setLong(1, ownerId);
            st.setString(2, dateBegin);
            st.setString(3, dateEnd);
            ResultSet rs = st.executeQuery();
            try {
                while (rs.next()) {
                    handle(rs);
                }
            } finally {
                rs.close();
            }
        } finally {
            st.close();
        }
    }

    public boolean selectByOrderSeq(Connection conn) throws SQLException {
        PreparedStatement st = conn
                .prepareStatement("SELECT id, tradeDate, balance, deposit, withdraw, orderSeq FROM `"
                        + getTableName() + "` WHERE orderSeq=? ");
        try {
            st.setString(1, orderSeq);
            ResultSet rs = st.executeQuery();
            try {
                boolean ret = rs.next();
                if (ret) {
                    setBy(rs, false);
                    assert (!rs.next()); // 根据主键的选择不可能出现两个结果
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
        PreparedStatement st = conn
                .prepareStatement("INSERT `"
                        + getTableName()
                        + "` ( ownerId, tradeDate, balance, deposit, withdraw, orderSeq, isLast) "
                        + "VALUES ( ?, ?, ?, ?, ?, ?, ?)");
        try {
            st.setLong(1, ownerId);
            st.setString(2, tradeDate);
            st.setDouble(3, balance);
            st.setDouble(4, deposit);
            st.setDouble(5, withdraw);
            st.setString(6, orderSeq);
            st.setByte(7, isLast);
            st.executeUpdate();
        } finally {
            st.close();
        }
    }

    @Override
    public void update(Connection conn) throws SQLException {
        PreparedStatement updateState = conn.prepareStatement("UPDATE `"
                + getTableName() + "` SET " + "isLast=? " + "WHERE id=? ");
        try {
            updateState.setByte(1, isLast);
            updateState.setLong(2, id);
            updateState.execute();
        } finally {
            updateState.close();
        }
    }

    @Override
    public void delete(Connection conn) throws SQLException {
        PreparedStatement st = conn.prepareStatement("DELETE FROM `"
                + getTableName() + "` WHERE ownerId=? " + "AND "
                + "tradeDate>=? " + "AND " + "tradeDate<?");
        try {
            st.setLong(1, ownerId);
            st.setString(2, dateBegin);
            st.setString(3, dateEnd);
            st.executeUpdate();
        } finally {
            st.close();
        }
    }

    public static void create(Connection conn) throws SQLException {
        String create = "CREATE TABLE IF NOT EXISTS`" + getTableName()
                + "` (\n"
                + "  `id`  BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT,\n"
                + "  `ownerId` BIGINT(30) NOT NULL ,\n"
                + "  `tradeDate` VARCHAR(20) NOT NULL ,\n"
                + "  `balance` double(12,3) DEFAULT '0',\n"
                + "  `deposit` double(12,3) DEFAULT '0',\n"
                + "  `withdraw` double(12,3) DEFAULT '0',\n"
                + "  `orderSeq` VARCHAR(64) DEFAULT NULL,\n"
                + "  `isLast` TINYINT(1) DEFAULT '0',\n"
                + "  PRIMARY KEY (`id`,`ownerId`)\n"
                + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
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
        return "account";
    }
}
