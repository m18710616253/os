package cn.ctyun.oos.server.db.dbprice;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import cn.ctyun.common.db.JdbcRow;

/*
 * 阶梯定价
 */
public class DbTieredPrice implements JdbcRow {
    /*
     * primary key。
     * 01代表存储空间的第一个阶梯id，11代表流出流量的第一个阶梯id，21代表get/head请求的第一个阶梯id，31代表other请求的第一个阶梯id
     * 41代表漫游上行第一个阶梯id, 51代表漫游下行第一个阶梯id
     * 61代表非互联网get/head请求的第一个阶梯id，71代表非互联网other请求的第一个阶梯id
     * 81代表非互联网流出流量的第一个阶梯id，91代表非互联网漫游流出流量的第一个阶梯id
     * 101代表非互联网漫游上行的第一个阶梯id
     * 111代表文本反垃圾spam请求的第一个阶梯id,121代表图片鉴黄确定部分张数pornReviewFalse的第一个阶梯id,131代表图片鉴黄不确定部分张数pornReviewTrue的第一个阶梯id
     */
    public int id;
    /* 范围下限 */
    public long low;
    /* 范围上限 */
    public long up;
    public int idStart;
    public int idEnd;
    public double value;
    /* 单价，单位是元/GB*天 ,get请求的单位是千次，other请求的单位是千次 */
    public double price;
    /* 全局折扣，客户经理设置的折扣不能超过这个底线 */
    public double discount = 100;
    // 折扣的有效期，单位月
    public int expiryDate = 12;
    /* 阶梯的类型，可以是storage,flow,ghRequest,otherRequest */
    public String type;
    public List<DbTieredPrice> tps = new ArrayList<DbTieredPrice>();
    public static final int MAX_STORAGE_ID = 10;
    public static final int MAX_FLOW_ID = 20;
    public static final int MAX_GH_REQUEST_ID = 30;
    public static final int MAX_OTHER_REQUEST_ID = 40;
    public static final int MAX_ROAM_UPLOAD_ID = 50;
    public static final int MAX_ROAM_FLOW_ID = 60;
    public static final int MAX_NONET_GH_REQ_ID = 70;
    public static final int MAX_NONET_OTHER_REQ_ID = 80;
    public static final int MAX_NONET_FLOW_ID = 90;
    public static final int MAX_NONET_ROAM_FLOW_ID = 100;
    public static final int MAX_NONET_ROAM_UPLOAD_ID = 110;
    public static final int MAX_SPAM_REQUEST_ID = 120;
    public static final int MAX_PORN_REVIEW_FALSE_ID = 130;
    public static final int MAX_PORN_REVIEW_TRUE_ID = 140;
    
    public DbTieredPrice() {
    }
    
    public DbTieredPrice(int id) {
        this.id = id;
    }
    
    @Override
    public void setBy(ResultSet resultSet, boolean ignore) throws SQLException {
        ResultSetMetaData metadata = resultSet.getMetaData();
        for (int i = 1; i <= metadata.getColumnCount(); i++) {
            String aname = metadata.getColumnName(i);
            if (aname.equals("id")) {
                id = resultSet.getInt(i);
            } else if (aname.equals("low")) {
                low = resultSet.getLong(i);
            } else if (aname.equals("up")) {
                up = resultSet.getLong(i);
            } else if (aname.equals("price")) {
                price = resultSet.getDouble(i);
            } else if (aname.equals("discount")) {
                discount = resultSet.getDouble(i);
            } else if (aname.equals("expiryDate")) {
                expiryDate = resultSet.getInt(i);
            } else {
                if (!ignore)
                    throw new RuntimeException("Unknown column name : " + aname);
            }
        }// end of for
    }
    
    public static void create(Connection conn) throws SQLException {
        String create = "CREATE TABLE `" + getTableName() + "` (\n"
                + "  `id` TINYINT(2) UNSIGNED NOT NULL,\n"
                + "  `low` bigint(20) UNSIGNED NOT NULL,\n"
                + "  `up` bigint(20) UNSIGNED NOT NULL,\n" + "  `price` double(12, 3) NOT NULL,\n"
                + "  `discount` double(7, 3) NOT NULL,\n"
                + "  `expiryDate` TINYINT(3) UNSIGNED Default NULL,\n" + "  PRIMARY KEY (`id`)\n"
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
            st = conn.prepareStatement("SELECT * FROM " + getTableName() + " WHERE id=?");
            st.setInt(1, id);
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
    
    public void selectAll(Connection conn) throws SQLException {
        PreparedStatement st = null;
        try {
            st = conn.prepareStatement("SELECT * FROM " + getTableName() + " order by id");
            ResultSet rs = null;
            try {
                rs = st.executeQuery();
                while (rs.next()) {
                    DbTieredPrice tp = new DbTieredPrice();
                    tp.id = rs.getInt("id");
                    tp.low = rs.getLong("low");
                    tp.up = rs.getLong("up");
                    tp.price = rs.getDouble("price");
                    tp.discount = rs.getDouble("discount");
                    tps.add(tp);
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
    
    public boolean selectByRange(Connection conn) throws SQLException {
        PreparedStatement st = null;
        try {
            st = conn.prepareStatement("SELECT * FROM " + getTableName()
                    + " WHERE id>? AND id<=? AND low<? AND up>=?");
            st.setInt(1, idStart);
            st.setInt(2, idEnd);
            st.setDouble(3, value);
            st.setDouble(4, value);
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
                    + "( id, low, up, price, discount, expiryDate) " + "VALUES (?, ?, ?, ?, ?, ?)");
            st.setInt(1, id);
            st.setLong(2, low);
            st.setLong(3, up);
            st.setDouble(4, price);
            st.setDouble(5, discount);
            st.setInt(6, expiryDate);
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
            st = conn.prepareStatement("UPDATE " + getTableName() + " SET " + "low=?, " + "up=?, "
                    + "price=?, " + "discount=?, " + "expiryDate=? " + "WHERE id=? ");
            st.setLong(1, low);
            st.setLong(2, up);
            st.setDouble(3, price);
            st.setDouble(4, discount);
            st.setInt(5, expiryDate);
            st.setInt(6, id);
            st.executeUpdate();
        } finally {
            if (st != null)
                st.close();
        }
    }
    
    public void updateDiscount(Connection conn) throws SQLException {
        PreparedStatement st = null;
        try {
            st = conn.prepareStatement("UPDATE " + getTableName() + " SET " + "discount=? "
                    + " WHERE id>? and id<=?  ");
            st.setDouble(1, discount);
            st.setInt(2, idStart);
            st.setInt(3, idEnd);
            st.executeUpdate();
        } finally {
            if (st != null)
                st.close();
        }
    }
    
    public void updateExpiryDate(Connection conn) throws SQLException {
        PreparedStatement st = null;
        try {
            st = conn.prepareStatement("UPDATE " + getTableName() + " SET " + "expiryDate=? ");
            st.setInt(1, expiryDate);
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
            st.setInt(1, id);
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
        return "tieredPrice";
    }
}
