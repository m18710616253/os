package cn.ctyun.oos.server.db.dbpay;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedHashSet;

import cn.ctyun.common.db.JdbcRow;

public class DbPrice implements JdbcRow {
    /* primary key。 */
    private long id = 0;
    /* Description of this price id */
    public String des;
    public int credit;
    public LinkedHashSet<String> prices = new LinkedHashSet<String>();

    public DbPrice(long id) {
        this.id = id;
    }

    public DbPrice() {
    }

    @Override
    public void setBy(ResultSet resultSet, boolean ignore) throws SQLException {
        ResultSetMetaData metadata = resultSet.getMetaData();
        for (int i = 1; i <= metadata.getColumnCount(); i++) {
            String aname = metadata.getColumnName(i);
            if (aname.equals("id")) {
                id = resultSet.getLong(i);
            } else if (aname.equals("des")) {
                des = resultSet.getString(i);
            } else if (aname.equals("credit")) {
                credit = resultSet.getInt(i);
            } else {
                if (!ignore)
                    throw new RuntimeException("Unknown column name : " + aname);
            }
        } // end of for
    }

    public static void create(Connection conn) throws SQLException {
        String create = "CREATE TABLE `" + getTableName() + "` (\n"
                + "  `id` BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT,\n"
                + "  `des` varchar(255) DEFAULT NULL,\n" + "  `credit` BIGINT(20) NOT NULL,\n"
                + "  PRIMARY KEY (`id`)\n" + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
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
            st = conn.prepareStatement("SELECT des,credit FROM " + getTableName() + " WHERE id=?");
            st.setLong(1, id);
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

    private void handle(ResultSet rs) throws SQLException {
        StringBuilder sb = new StringBuilder();
        sb.append(rs.getString("id")).append(" ").append(rs.getString("des")).append(" ")
                .append(rs.getLong("credit"));
        prices.add(sb.toString());
    }

    public void selectAll(Connection conn) throws SQLException {
        PreparedStatement st = null;
        try {
            st = conn.prepareStatement("SELECT id,des, credit FROM " + getTableName());
            ResultSet rs = null;
            try {
                rs = st.executeQuery();
                while (rs.next()) {
                    handle(rs);
                }
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
            st = conn.prepareStatement(
                    "INSERT " + getTableName() + "( id, des,credit) " + "VALUES (?, ?, ?)");
            st.setLong(1, id);
            st.setString(2, des);
            st.setInt(3, credit);
            st.executeUpdate();
        } finally {
            if (st != null)
                st.close();
        }
    }

    @Override
    public void update(Connection conn) throws SQLException {
        PreparedStatement st = null;
        try {
            st = conn.prepareStatement(
                    "UPDATE " + getTableName() + " SET " + "des=?,  credit=? " + "WHERE id=? ");
            st.setString(1, des);
            st.setInt(2, credit);
            st.setLong(3, id);
            st.executeUpdate();
        } finally {
            if (st != null)
                st.close();
        }
    }

    @Override
    public void delete(Connection conn) throws SQLException {
        PreparedStatement st = null;
        try {
            st = conn.prepareStatement("DELETE FROM " + getTableName() + " WHERE id=?");
            st.setLong(1, id);
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
        return "price";
    }
}
