package cn.ctyun.oos.server.db.dbprice;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import cn.ctyun.common.db.JdbcRow;

import common.time.TimeUtils;

public class DbUserPackage implements JdbcRow {
    public String orderId;
    /* 用户id */
    public long usrId;
    /* 批价的客户经理id，如果没有就是Null */
    public String salerId;
    /* 对应套餐表里面的packageId */
    public int packageId;
    /* 客户经理定制的套餐折扣，不能超过全局的折扣，默认是100，即没有折扣 */
    public double packageDiscount = 100;
    /* 套餐开始时间，FORMAT:YYYYMMDD */
    public double storage;
    public double flow;
    public double roamFlow;
    public double roamUpload;
    public double ghRequest;
    public double otherRequest;
    //非互联网流量、请求
    public double noNetGHReq;
    public double noNetOtherReq;
    public double noNetFlow;
    public double noNetRoamFlow;
    public double noNetRoamUpload;
    // 图片鉴黄、文本反垃圾调用量
    public double spamRequest;
    public double porn;
    public String packageStart;
    /* 套餐结束时间，FORMAT:YYYYMMDD */
    public String packageEnd;
    /* 0表示未支付，1表示已支付 */
    public byte isPaid = 0;
    public String date = TimeUtils.toYYYY_MM_dd(new Date());
    public String startDate;
    public String endDate;
    /* 是否终止 ，1终止，0正常 */
    public byte isClose = 0;
    /* 更新时间*/
    public String updateTime;
    public List<DbUserPackage> ups = new ArrayList<DbUserPackage>();
    
    public DbUserPackage() {
    }
    
    public DbUserPackage(long usrId) {//该代码类也在oos-front包中。注意同步
        this.usrId = usrId;
    }
    
    public DbUserPackage(String orderId) {
        this.orderId = orderId;
    }
    
    @Override
    public void setBy(ResultSet resultSet, boolean ignore) throws SQLException {
        ResultSetMetaData metadata = resultSet.getMetaData();
        for (int i = 1; i <= metadata.getColumnCount(); i++) {
            String aname = metadata.getColumnName(i);
            if (aname.equals("orderId")) {
                orderId = resultSet.getString(i);
            } else if (aname.equals("usrId")) {
                usrId = resultSet.getLong(i);
            } else if (aname.equals("salerId")) {
                salerId = resultSet.getString(i);
            } else if (aname.equals("packageId")) {
                packageId = resultSet.getInt(i);
            } else if (aname.equals("packageDiscount")) {
                packageDiscount = resultSet.getDouble(i);
            } else if (aname.equals("storage")) {
                storage = resultSet.getDouble(i);
            } else if (aname.equals("flow")) {
                flow = resultSet.getDouble(i);
            } else if (aname.equals("ghRequest")) {
                ghRequest = resultSet.getDouble(i);
            } else if (aname.equals("otherRequest")) {
                otherRequest = resultSet.getDouble(i);
            } else if (aname.equals("packageStart")) {
                packageStart = resultSet.getString(i);
            } else if (aname.equals("packageEnd")) {
                packageEnd = resultSet.getString(i);
            } else if (aname.equals("isPaid")) {
                isPaid = resultSet.getByte(i);
            } else if (aname.equals("isClose")) {
                isClose = resultSet.getByte(i);
            } else if (aname.equals("roamFlow")) {
                roamFlow = resultSet.getDouble(i);
            } else if (aname.equals("roamUpload")) {
                roamUpload = resultSet.getDouble(i);
            } else if (aname.equals("noNetRoamUpload")) {
                noNetRoamUpload = resultSet.getDouble(i);
            } else if (aname.equals("noNetRoamFlow")) {
                noNetRoamFlow = resultSet.getDouble(i);
            } else if (aname.equals("noNetFlow")) {
                noNetFlow = resultSet.getDouble(i);
            } else if (aname.equals("noNetGHReq")) {
                noNetGHReq = resultSet.getDouble(i);
            } else if (aname.equals("noNetOtherReq")) {
                noNetOtherReq = resultSet.getDouble(i);
            } else if (aname.equals("spamRequest")) {
                spamRequest = resultSet.getDouble(i);
            } else if (aname.equals("porn")) {
                porn = resultSet.getDouble(i);
            } else if (aname.equals("updateTime")) {
                updateTime = resultSet.getString(i);
            } else {
                if (!ignore)
                    throw new RuntimeException("Unknown column name : " + aname);
            }
        }// end of for
    }
    
    public static void create(Connection conn) throws SQLException {
        String create = "CREATE TABLE `" + getTableName() + "` (\n"
                + "  `orderId` VARCHAR(50) NOT NULL,\n" + "  `usrId` BIGINT(30) NOT NULL,\n"
                + "  `salerId` VARCHAR(30) DEFAULT NULL,\n"
                + "  `packageId` INT(10) UNSIGNED DEFAULT NULL,\n"
                + "  `packageDiscount` Double(7,3) DEFAULT 100,\n"
                + "  `storage` DOUBLE(17,3)  DEFAULT NULL,\n"
                + "  `flow`  DOUBLE(17,3)  DEFAULT NULL,\n"
                + "  `ghRequest`  DOUBLE(17,3)  DEFAULT NULL,\n"
                + "  `otherRequest`  DOUBLE(17,3)  DEFAULT NULL,\n"
                + "  `packageStart` VARCHAR(20) DEFAULT NULL,\n"
                + "  `packageEnd` VARCHAR(20) DEFAULT NULL,\n"
                + "  `isPaid` TINYINT(1) DEFAULT 0,\n" + "  `isClose` TINYINT(1) DEFAULT 0,\n"
                + "  `roamFlow`  DOUBLE(17,3)  DEFAULT NULL,\n"
                + "  `roamUpload`  DOUBLE(17,3)  DEFAULT NULL,\n"    
                + "  `noNetGHReq`  DOUBLE(17,3)  DEFAULT NULL,\n"
                + "  `noNetOtherReq`  DOUBLE(17,3)  DEFAULT NULL,\n"
                + "  `noNetFlow`  DOUBLE(17,3)  DEFAULT NULL,\n"
                + "  `noNetRoamFlow`  DOUBLE(17,3)  DEFAULT NULL,\n"
                + "  `noNetRoamUpload`  DOUBLE(17,3)  DEFAULT NULL,\n"
                + "  `spamRequest`  DOUBLE(17,3)  DEFAULT NULL,\n"
                + "  `porn`  DOUBLE(17,3)  DEFAULT NULL,\n"
                + "  `updateTime`  VARCHAR(20)  DEFAULT '2019-12-10',\n"
                + "  PRIMARY KEY (`orderId`),"
                + "  KEY `userId_time` (`usrId`,`isPaid`,`packageEnd`,`packageStart`) USING BTREE"
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
            st = conn
                    .prepareStatement("SELECT * FROM "
                            + getTableName()
                            + " WHERE usrId=? AND isPaid=? AND packageEnd>=? AND packageStart<=? and isClose=0");
            st.setLong(1, usrId);
            st.setByte(2, isPaid);
            st.setString(3, date);
            st.setString(4, date);
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
    
    /**
     * 统计用户过期的套餐数
     * @param conn
     * @return
     * @throws SQLException
     */
    public int countExpirePackage(Connection conn) throws SQLException {
        PreparedStatement st = null;
        try {
            st = conn.prepareStatement("SELECT COUNT(1) FROM "
                            + getTableName()
                            + " WHERE usrId=? AND isPaid=1 AND packageEnd<? AND  isClose=0");
            st.setLong(1, usrId);//count(1)是统计表中满足条件的行数
            st.setString(2, date);
            ResultSet rs = null;
            try {
                rs = st.executeQuery();
                rs.next();
                return rs.getInt(1);
            } finally {
                if (rs != null)
                    rs.close();
            }
        } finally {
            if (st != null)
                st.close();
        }
    }
    
    public boolean selectInRange(Connection conn) throws SQLException {
        PreparedStatement st = null;
        try {
            st = conn
                    .prepareStatement("SELECT * FROM "
                            + getTableName()
                            + " WHERE usrId=? AND isPaid=? AND packageEnd<=? AND packageStart>=? and isClose=0");
            st.setLong(1, usrId);
            st.setByte(2, isPaid);
            st.setString(3, endDate);
            st.setString(4, startDate);
            ResultSet rs = null;
            try {
                rs = st.executeQuery();
                boolean ret = rs.next();
                if (ret) {
                    setBy(rs, false);
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
    
    public void selectByUsrId(Connection conn) throws SQLException {
        PreparedStatement st = null;
        try {
            st = conn.prepareStatement("SELECT * FROM " + getTableName()
                    + " WHERE usrId=? order by packageStart");
            st.setLong(1, usrId);
            ResultSet rs = null;
            try {
                rs = st.executeQuery();
                while (rs.next()) {
                    DbUserPackage up = new DbUserPackage();
                    up.usrId = usrId;
                    up.isPaid = rs.getByte("isPaid");
                    up.packageStart = rs.getString("packageStart");
                    up.packageEnd = rs.getString("packageEnd");
                    up.packageDiscount = rs.getDouble("packageDiscount");
                    up.packageId = rs.getInt("packageId");
                    up.salerId = rs.getString("salerId");
                    up.isClose = rs.getByte("isClose");
                    up.orderId = rs.getString("orderId");
                    ups.add(up);
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
    
    public void selectByPackageId(Connection conn) throws SQLException {
        PreparedStatement st = null;
        try {
            st = conn.prepareStatement("SELECT * FROM " + getTableName()
                    + " WHERE packageId=? order by usrId");
            st.setLong(1, packageId);
            ResultSet rs = null;
            try {
                rs = st.executeQuery();
                while (rs.next()) {
                    DbUserPackage up = new DbUserPackage();
                    up.usrId = rs.getLong("usrId");
                    up.isPaid = rs.getByte("isPaid");
                    up.packageStart = rs.getString("packageStart");
                    up.packageEnd = rs.getString("packageEnd");
                    up.packageDiscount = rs.getDouble("packageDiscount");
                    up.packageId = rs.getInt("packageId");
                    up.salerId = rs.getString("salerId");
                    up.isClose = rs.getByte("isClose");
                    ups.add(up);
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
    
    public boolean selectByOrderId(Connection conn) throws SQLException {
        PreparedStatement st = null;
        try {
            st = conn.prepareStatement("SELECT * FROM " + getTableName() + " WHERE orderId=?");
            st.setString(1, orderId);
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
    
    
    public List<Long> selectAllOwnerId(Connection conn) throws SQLException {
        PreparedStatement st = null;
        List<Long> ownerIds = new ArrayList<Long>();
        try {
            st = conn.prepareStatement("SELECT distinct usrId FROM " + getTableName());
            ResultSet rs = null;
            try {
                rs = st.executeQuery();
                while (rs.next()) {
                    ownerIds.add(rs.getLong("usrId"));
                }
            } finally {
                if (rs != null)
                    rs.close();
            }
        } finally {
            if (st != null)
                st.close();
        }
        return ownerIds;
    }
    
    @Override
    public void insert(Connection conn) throws SQLException {
        PreparedStatement st = null;
        try {
            st = conn
                    .prepareStatement("INSERT "
                            + getTableName()
                            + "( orderId, usrId, salerId, packageId, packageDiscount, storage, flow, ghRequest, otherRequest, packageStart,packageEnd,isPaid,isClose,roamFlow,roamUpload,noNetGHReq,noNetOtherReq,noNetFlow,noNetRoamUpload,noNetRoamFlow,spamRequest,porn) "
                            + "VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?,?,?,?,?,?,?,?,?,?,?,?)");
            st.setString(1, orderId);
            st.setLong(2, usrId);
            st.setString(3, salerId);
            st.setLong(4, packageId);
            st.setDouble(5, packageDiscount);
            st.setDouble(6, storage);
            st.setDouble(7, flow);
            st.setDouble(8, ghRequest);
            st.setDouble(9, otherRequest);
            st.setString(10, packageStart);
            st.setString(11, packageEnd);
            st.setByte(12, isPaid);
            st.setByte(13, isClose);
            st.setDouble(14, roamFlow);
            st.setDouble(15, roamUpload);
            st.setDouble(16, noNetGHReq);
            st.setDouble(17, noNetOtherReq);
            st.setDouble(18, noNetFlow);
            st.setDouble(19, noNetRoamUpload);
            st.setDouble(20, noNetRoamFlow);
            st.setDouble(21, spamRequest);
            st.setDouble(22, porn);
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
            st = conn.prepareStatement("UPDATE " + getTableName() + " SET " + "packageDiscount=?, "
                    + "storage=?, " + "flow=?, " + "ghRequest=?, " + "otherRequest=?, "
                    + "packageStart=?, " + "packageEnd=?, "
                    + "isPaid=?, isClose=?, "+ "roamFlow=?, "+ "roamUpload=?, "
                    + "noNetGHReq=?, noNetOtherReq=?, "+ "noNetFlow=?, "+ "noNetRoamFlow=?, " + "noNetRoamUpload=?, "
                    + "spamRequest=?, porn=? ,updateTime=? "
                    + "WHERE orderId=? ");
            st.setDouble(1, packageDiscount);
            st.setDouble(2, storage);
            st.setDouble(3, flow);
            st.setDouble(4, ghRequest);
            st.setDouble(5, otherRequest);
            st.setString(6, packageStart);
            st.setString(7, packageEnd);
            st.setByte(8, isPaid);
            st.setByte(9, isClose);
            st.setDouble(10, roamFlow);
            st.setDouble(11, roamUpload);
            st.setDouble(12, noNetGHReq);
            st.setDouble(13, noNetOtherReq);
            st.setDouble(14, noNetFlow);
            st.setDouble(15, noNetRoamFlow);
            st.setDouble(16, noNetRoamUpload);
            st.setDouble(17, spamRequest);
            st.setDouble(18, porn);
            st.setString(19, updateTime);
            st.setString(20, orderId);
            st.executeUpdate();
        } finally {
            if (st != null)
                st.close();
        }
    }
    
    public void updateAll(Connection conn) throws SQLException {
        PreparedStatement st = null;
        try {
            st = conn.prepareStatement("UPDATE " + getTableName() + " SET " + "packageDiscount=?, "
                    + "storage=?, " + "flow=?, " + "packageid=?," + "ghRequest=?, "
                    + "otherRequest=?, " + "packageStart=?, " + "packageEnd=?, "
                    + "isPaid=?, isClose=?, roamFlow=?, roamUpload=?, "
                    + "noNetGHReq=?, noNetOtherReq=?, "+ "noNetFlow=?, "+ "noNetRoamFlow=?, " + "noNetRoamUpload=?, "
                    + "spamRequest=?, porn=? ,updateTime=? "
                    + "WHERE orderId=? ");
            st.setDouble(1, packageDiscount);
            st.setDouble(2, storage);
            st.setDouble(3, flow);
            st.setInt(4, packageId);
            st.setDouble(5, ghRequest);
            st.setDouble(6, otherRequest);
            st.setString(7, packageStart);
            st.setString(8, packageEnd);
            st.setByte(9, isPaid);
            st.setByte(10, isClose);
            st.setDouble(11, roamFlow);
            st.setDouble(12, roamUpload);
            st.setDouble(13, noNetGHReq);
            st.setDouble(14, noNetOtherReq);
            st.setDouble(15, noNetFlow);
            st.setDouble(16, noNetRoamFlow);
            st.setDouble(17, noNetRoamUpload);
            st.setDouble(18, spamRequest);
            st.setDouble(19, porn);
            st.setString(20, updateTime);
            st.setString(21, orderId);
            st.executeUpdate();
        } finally {
            if (st != null)
                st.close();
        }
    }
    
    public void closeUserAllPackage(Connection conn) throws SQLException {
        PreparedStatement st = null;
        try {
            st = conn.prepareStatement("UPDATE " + getTableName() + " SET  isClose=1 WHERE usrId=? ");
            st.setLong(1, usrId);
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
            st = conn.prepareStatement("DELETE FROM " + getTableName() + " WHERE orderId=?");
            st.setString(1, orderId);
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
        return "userPackage";
    }

    public void deleteByOwnerId(Connection conn) throws SQLException {
        PreparedStatement st = null;
        try {
            st = conn.prepareStatement("DELETE FROM " + getTableName() + " WHERE usrId=? ");
            st.setLong(1, usrId);
            st.executeUpdate();
        } finally {
            if (st != null)
                st.close();
        }
    }
}