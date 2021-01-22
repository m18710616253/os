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
 * 套餐表
 */
public class DbPackage implements JdbcRow {
    /* primary key。 自增 */
    public int packageId;
    public String name = "";
    /* 存储空间，单位GB */
    public long storage;
    /* 流出流量，单位GB */
    public long flow;
    /* Get/Head请求，单位千次 */
    public long ghRequest;
    /* other请求，单位千次 */
    public long otherRequest;
    /* 时长，单位月 */
    public long duration;
    /* 原价，单位元 */
    public double costPrice;
    /* 套餐价，单位元 */
    public double packagePrice;
    /* 折扣 */
    public double discount;
    /* 代表是否有效，1有效，0无效， 2后台管理可见，前端不可见，有效 */
    public byte isValid = 1;
    public byte isVisible = 1;
    public List<DbPackage> packages = new ArrayList<DbPackage>();
    /* 互联网漫游下行流量，单位GB */
    public long roamFlow;
    /* 互联网漫游上行流量，单位GB */
    public long roamUpload;
    /* 非互联网漫游上行流量，单位GB */
    public long noNetRoamUpload;
    /* 非互联网漫游下行流量，单位GB */
    public long noNetRoamFlow;
    /* 非互联网Get/Head请求，单位千次 */
    public long noNetGHReq;
    /* 非互联网other请求，单位千次 */
    public long noNetOtherReq;
    /* 非互联网流出流量，单位GB */
    public long noNetFlow;
    /* 文本反垃圾调用量，单位千条 */
    public long spamRequest;
    /* 图片鉴黄总量，单位千张 */
    public long porn;
    public DbPackage() {
    }
    
    public DbPackage(int id) {
        this.packageId = id;
    }
    
    @Override
    public void setBy(ResultSet resultSet, boolean ignore) throws SQLException {
        ResultSetMetaData metadata = resultSet.getMetaData();
        for (int i = 1; i <= metadata.getColumnCount(); i++) {
            String aname = metadata.getColumnName(i);
            if (aname.equals("name")) {
                name = resultSet.getString(i);
            } else if (aname.equals("packageId")) {
                packageId = resultSet.getInt(i);
            } else if (aname.equals("storage")) {
                storage = resultSet.getLong(i);
            } else if (aname.equals("flow")) {
                flow = resultSet.getLong(i);
            } else if (aname.equals("ghRequest")) {
                ghRequest = resultSet.getLong(i);
            } else if (aname.equals("otherRequest")) {
                otherRequest = resultSet.getLong(i);
            } else if (aname.equals("duration")) {
                duration = resultSet.getLong(i);
            } else if (aname.equals("costPrice")) {
                costPrice = resultSet.getDouble(i);
            } else if (aname.equals("packagePrice")) {
                packagePrice = resultSet.getDouble(i);
            } else if (aname.equals("discount")) {
                discount = resultSet.getDouble(i);
            } else if (aname.equals("isValid")) {
                isValid = resultSet.getByte(i);
            } else if (aname.equals("roamFlow")) {
                roamFlow = resultSet.getLong(i);
            } else if (aname.equals("roamUpload")) {
                roamUpload = resultSet.getLong(i);
            } else if (aname.equals("noNetRoamUpload")) {
                noNetRoamUpload = resultSet.getLong(i);
            } else if (aname.equals("noNetRoamFlow")) {
                noNetRoamFlow = resultSet.getLong(i);
            } else if (aname.equals("noNetFlow")) {
                noNetFlow = resultSet.getLong(i);
            } else if (aname.equals("noNetGHReq")) {
                noNetGHReq = resultSet.getLong(i);
            } else if (aname.equals("noNetOtherReq")) {
                noNetOtherReq = resultSet.getLong(i);
            } else if (aname.equals("spamRequest")) {
                spamRequest = resultSet.getLong(i);
            } else if (aname.equals("porn")) {
                porn = resultSet.getLong(i);
            } else {
                if (!ignore)
                    throw new RuntimeException("Unknown column name : " + aname);
            }
        }// end of for
    }
    
    public static void create(Connection conn) throws SQLException {
        String create = "CREATE TABLE `" + getTableName() + "` (\n"
                + "  `packageId` INT(10) NOT NULL AUTO_INCREMENT,\n"
                + "  `name` varchar(10) NOT NULL,\n"
                + "  `storage` INT(10) unsigned NOT NULL DEFAULT '0',\n"
                + "  `flow` INT(10)  unsigned NOT NULL DEFAULT '0',\n"
                + "  `ghRequest` bigint(20) unsigned NOT NULL,\n"
                + "  `otherRequest` bigint(20) unsigned NOT NULL,\n"
                + "  `duration` TINYINT(3) unsigned NOT NULL,\n"
                + "  `costPrice` double(12, 3) NOT NULL,\n"
                + "  `packagePrice` double(12, 3) NOT NULL,\n"
                + "  `discount` double(7, 3) NOT NULL,\n" + "  `isValid` TINYINT(1) NOT NULL,\n"
                + "  `roamFlow` INT(10)  unsigned NOT NULL DEFAULT '0',\n"
                + "  `roamUpload` INT(10)  unsigned NOT NULL DEFAULT '0',\n"
                + "  `noNetRoamUpload` INT(10)  unsigned NOT NULL DEFAULT '0',\n"
                + "  `noNetRoamFlow` INT(10)  unsigned NOT NULL DEFAULT '0',\n"
                + "  `noNetGHReq` bigint(20) unsigned NOT NULL DEFAULT '0',\n"
                + "  `noNetOtherReq` bigint(20) unsigned NOT NULL DEFAULT '0',\n"
                + "  `noNetFlow` INT(10)  unsigned NOT NULL DEFAULT '0',\n"
                + "  `spamRequest` bigint(20) unsigned NOT NULL,\n"
                + "  `porn` bigint(20) unsigned NOT NULL,\n"
                + "  PRIMARY KEY (`packageId`)\n" + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
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
            st = conn.prepareStatement("SELECT * FROM " + getTableName() + " WHERE packageId=?");
            st.setInt(1, packageId);
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
    
    public void selectAll(Connection conn, boolean isManager) throws SQLException {
        PreparedStatement st = null;
        try {
            if (!isManager)
                st = conn.prepareStatement("SELECT * FROM " + getTableName() + " WHERE isValid=1");
            else
                st = conn.prepareStatement("SELECT * FROM " + getTableName());
            ResultSet rs = null;
            try {
                rs = st.executeQuery();
                while (rs.next()) {
                    DbPackage p = new DbPackage();
                    p.name = rs.getString("name");
                    p.storage = rs.getLong("storage");
                    p.flow = rs.getLong("flow");
                    p.ghRequest = rs.getLong("ghRequest");
                    p.otherRequest = rs.getLong("otherRequest");
                    p.duration = rs.getLong("duration");
                    p.costPrice = rs.getDouble("costPrice");
                    p.discount = rs.getDouble("discount");
                    p.packagePrice = rs.getDouble("packagePrice");
                    p.isValid = rs.getByte("isValid");
                    p.packageId = rs.getInt("packageId");
                    p.roamFlow = rs.getLong("roamFlow");
                    p.roamUpload = rs.getLong("roamUpload");
                    p.noNetGHReq = rs.getLong("noNetGHReq");
                    p.noNetOtherReq = rs.getLong("noNetOtherReq");
                    p.noNetFlow = rs.getLong("noNetFlow");
                    p.noNetRoamFlow = rs.getLong("noNetRoamFlow");
                    p.noNetRoamUpload = rs.getLong("noNetRoamUpload");
                    p.spamRequest = rs.getLong("spamRequest");
                    p.porn = rs.getLong("porn");
                    packages.add(p);
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
            st = conn
                    .prepareStatement(
                            "INSERT "
                                    + getTableName()
                                    + "( name, storage, flow, ghRequest, otherRequest, duration, costPrice, packagePrice, discount, isValid,roamFlow,roamUpload,noNetGHReq,noNetOtherReq,noNetFlow,noNetRoamUpload,noNetRoamFlow,spamRequest,porn) "
                                    + "VALUES (  ?, ?, ?, ?, ?, ?, ?, ?,?,?,?,?,?,?,?,?,?,?,?)",
                            PreparedStatement.RETURN_GENERATED_KEYS);
            st.setString(1, name);
            st.setLong(2, storage);
            st.setLong(3, flow);
            st.setLong(4, ghRequest);
            st.setLong(5, otherRequest);
            st.setLong(6, duration);
            st.setDouble(7, costPrice);
            st.setDouble(8, packagePrice);
            st.setDouble(9, discount);
            st.setByte(10, isValid);
            st.setLong(11, roamFlow);
            st.setLong(12, roamUpload);
            st.setLong(13, noNetGHReq);
            st.setLong(14, noNetOtherReq);
            st.setLong(15, noNetFlow);
            st.setLong(16, noNetRoamUpload);
            st.setLong(17, noNetRoamFlow);
            st.setLong(18, spamRequest);
            st.setLong(19, porn);
            st.executeUpdate();
            ResultSet rs = null;
            try {
                rs = st.getGeneratedKeys();
                rs.next();
                packageId = rs.getInt(1);
            } finally {
                if (rs != null) {
                    rs.close();
                }
            }
        } finally {
            if (st != null)
                st.close();
        }
    }
    
    @Override
    public void update(Connection conn) throws SQLException {
        PreparedStatement st = null;
        try {
            st = conn.prepareStatement("UPDATE " + getTableName() + " SET " + "storage=?, "
                    + "name=?, " + "flow=?, " + "ghRequest=?, " + "otherRequest=?, "
                    + "duration=?, " + "costPrice=?, " + "packagePrice=?, " + "discount=?, "
                    + "isValid=?, "  + "roamFlow=?, " + "roamUpload=?, "
                    + "noNetGHReq=?, " + "noNetOtherReq=?, "
                    + "noNetFlow=?, " + "noNetRoamUpload=?, " + "noNetRoamFlow=?, "
                    + "spamRequest=?, " + "porn=? "
                    + "WHERE packageId=? ");
            st.setLong(1, storage);
            st.setString(2, name);
            st.setLong(3, flow);
            st.setLong(4, ghRequest);
            st.setLong(5, otherRequest);
            st.setLong(6, duration);
            st.setDouble(7, costPrice);
            st.setDouble(8, packagePrice);
            st.setDouble(9, discount);
            st.setByte(10, isValid);
            st.setLong(11, roamFlow);
            st.setLong(12, roamUpload);
            st.setLong(13, noNetGHReq);
            st.setLong(14, noNetOtherReq);
            st.setLong(15, noNetFlow);
            st.setLong(16, noNetRoamUpload);
            st.setLong(17, noNetRoamFlow);
            st.setLong(18, spamRequest);
            st.setLong(19, porn);
            st.setInt(20, packageId);
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
            st = conn.prepareStatement("DELETE FROM " + getTableName() + " WHERE packageId=?");
            st.setInt(1, packageId);
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
        return "package";
    }
}
