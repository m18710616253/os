package cn.ctyun.oos.server.db.dbprice;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;

import cn.ctyun.common.Consts;
import cn.ctyun.common.db.JdbcRow;

import common.time.TimeUtils;

public class DbUserTieredDiscount implements JdbcRow {
    public String orderId;
    /* 用户id */
    public long usrId;
    /* 批价的客户经理id */
    public String salerId;
    public double storageDiscount = 100;
    public double flowDiscount = 100;
    public double requestDiscount = 100;
    public double roamFlowDiscount = 100;
    public double roamUploadDiscount = 100;
    //非互联网请求折扣，非互联网下行流量、漫游上行、下行流量折扣
    public double noNetReqDiscount = 100;
    public double noNetFlowDiscount = 100;
    public double noNetRoamFlowDiscount = 100;
    public double noNetRoamUploadDiscount = 100;
    public double spamRequestDiscount = 100;
    public double pornReviewFalseDiscount = 100;
    public double pornReviewTrueDiscount = 100;
    /* FORMAT:YYYYMMDD */
    public String start;
    public String end;
    public String date = TimeUtils.toYYYYMMDD(new Date());
    
    public DbUserTieredDiscount() {
    }
    
    public DbUserTieredDiscount(long usrId) {
        this.usrId = usrId;
    }
    
    public DbUserTieredDiscount(String orderId) {
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
            } else if (aname.equals("storageDiscount")) {
                storageDiscount = resultSet.getDouble(i);
            } else if (aname.equals("flowDiscount")) {
                flowDiscount = resultSet.getDouble(i);
            } else if (aname.equals("requestDiscount")) {
                requestDiscount = resultSet.getDouble(i);
            } else if (aname.equals("start")) {
                start = resultSet.getString(i);
            } else if (aname.equals("end")) {
                end = resultSet.getString(i);
            } else if (aname.equals("roamFlowDiscount")) {
                roamFlowDiscount = resultSet.getDouble(i);
            } else if (aname.equals("roamUploadDiscount")) {
                roamUploadDiscount = resultSet.getDouble(i);
            } else if (aname.equals("noNetReqDiscount")) {
                noNetReqDiscount = resultSet.getDouble(i);
            } else if (aname.equals("noNetFlowDiscount")) {
                noNetFlowDiscount = resultSet.getDouble(i);
            } else if (aname.equals("noNetRoamFlowDiscount")) {
                noNetRoamFlowDiscount = resultSet.getDouble(i);
            } else if (aname.equals("noNetRoamUploadDiscount")) {
                noNetRoamUploadDiscount = resultSet.getDouble(i);
            } else if (aname.equals("spamRequestDiscount")) {
                spamRequestDiscount = resultSet.getDouble(i);
            } else if (aname.equals("pornReviewFalseDiscount")) {
                pornReviewFalseDiscount = resultSet.getDouble(i);
            } else if (aname.equals("pornReviewTrueDiscount")) {
                pornReviewTrueDiscount = resultSet.getDouble(i);
            } else {
                if (!ignore)
                    throw new RuntimeException("Unknown column name : " + aname);
            }
        }// end of for
    }
    
    public static void create(Connection conn) throws SQLException {
        String create = "CREATE TABLE `" + getTableName() + "` (\n"
                + "  `orderId` VARCHAR(30) NOT NULL,\n" + "  `usrId` BIGINT(30) NOT NULL,\n"
                + "  `salerId` VARCHAR(30) DEFAULT NULL,\n"
                + "  `storageDiscount` Double(7,3) NOT NULL DEFAULT '100.000',\n"
                + "  `flowDiscount` Double(7,3) NOT NULL DEFAULT '100.000',\n"
                + "  `requestDiscount` Double(7,3) NOT NULL DEFAULT '100.000',\n"
                + "  `roamFlowDiscount` Double(7,3) NOT NULL DEFAULT '100.000',\n"
                + "  `roamUploadDiscount` Double(7,3) NOT NULL DEFAULT '100.000',\n"
                + "  `noNetReqDiscount` Double(7,3) NOT NULL DEFAULT '100.000',\n"
                + "  `noNetFlowDiscount` Double(7,3) NOT NULL DEFAULT '100.000',\n"
                + "  `noNetRoamFlowDiscount` Double(7,3) NOT NULL DEFAULT '100.000',\n"
                + "  `noNetRoamUploadDiscount` Double(7,3) NOT NULL DEFAULT '100.000',\n"
                + "  `spamRequestDiscount` Double(7,3) NOT NULL DEFAULT '100.000',\n"
                + "  `pornReviewFalseDiscount` Double(7,3) NOT NULL DEFAULT '100.000',\n"
                + "  `pornReviewTrueDiscount` Double(7,3) NOT NULL DEFAULT '100.000',\n"
                + "  `start` VARCHAR(20) DEFAULT NULL,\n" + "  `end` VARCHAR(20) DEFAULT NULL,\n"
                + "  PRIMARY KEY (`orderId`),\n" + "  KEY `owner_time` (`usrId`,`start`,`end`)"
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
                    + " WHERE usrId=?  AND start<=? AND end>=?");
            st.setLong(1, usrId);
            st.setString(2, date);
            st.setString(3, date);
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
    
    public boolean selectByOrderId(Connection conn) throws SQLException {
        PreparedStatement st = null;
        try {
            st = conn.prepareStatement("SELECT * FROM " + getTableName() + " WHERE orderId=? ");
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
    
    @Override
    public void insert(Connection conn) throws SQLException {
        PreparedStatement st = null;
        try {
            st = conn
                    .prepareStatement("INSERT "
                            + getTableName()
                            + "( orderId, usrId, salerId, storageDiscount, flowDiscount, requestDiscount,roamFlowDiscount, roamUploadDiscount,noNetReqDiscount,noNetFlowDiscount,noNetRoamFlowDiscount,noNetRoamUploadDiscount,spamRequestDiscount,pornReviewFalseDiscount,pornReviewTrueDiscount,start,end) "
                            + "VALUES (?, ?, ?, ?, ?, ?, ?, ?,?,?,?,?,?,?,?,?,?)");
            st.setString(1, orderId);
            st.setLong(2, usrId);
            st.setString(3, salerId);
            st.setDouble(4, storageDiscount);
            st.setDouble(5, flowDiscount);
            st.setDouble(6, requestDiscount);
            st.setDouble(7, roamFlowDiscount);
            st.setDouble(8, roamUploadDiscount);
            st.setDouble(9, noNetReqDiscount);
            st.setDouble(10, noNetFlowDiscount);
            st.setDouble(11, noNetRoamFlowDiscount);
            st.setDouble(12, noNetRoamUploadDiscount);
            st.setDouble(13, spamRequestDiscount);
            st.setDouble(14, pornReviewFalseDiscount);
            st.setDouble(15, pornReviewTrueDiscount);
            st.setString(16, start);
            st.setString(17, end);
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
            st = conn.prepareStatement("UPDATE " + getTableName() + " SET " + "storageDiscount=?, "
                    + "flowDiscount=?, " + "requestDiscount=?, " 
                    + "roamFlowDiscount=?, " + "roamUploadDiscount=?, "
                    + "noNetReqDiscount=?, " + "noNetFlowDiscount=?, "+ "noNetRoamFlowDiscount=?, " + "noNetRoamUploadDiscount=?, " + "start=?, " + "end=?, "
                    + "spamRequestDiscount=?, " + "pornReviewFalseDiscount=?, " + "pornReviewTrueDiscount=? "
                    + "WHERE orderId=?  AND salerId=? ");
            st.setDouble(1, storageDiscount);
            st.setDouble(2, flowDiscount);
            st.setDouble(3, requestDiscount);
            st.setDouble(4, roamFlowDiscount);
            st.setDouble(5, roamUploadDiscount);
            st.setDouble(6, noNetReqDiscount);
            st.setDouble(7, noNetFlowDiscount);
            st.setDouble(8, noNetRoamFlowDiscount);
            st.setDouble(9, noNetRoamUploadDiscount);
            st.setString(10, start);
            st.setString(11, end);
            st.setDouble(8, spamRequestDiscount);
            st.setDouble(9, pornReviewFalseDiscount);
            st.setDouble(10, pornReviewTrueDiscount);
            st.setString(12, orderId);
            st.setString(13, salerId);
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
            st = conn.prepareStatement("DELETE FROM " + getTableName()
                    + " WHERE orderId=? AND salerId=?");
            st.setString(1, orderId);
            st.setString(2, salerId);
            st.executeUpdate();
        } finally {
            if (st != null)
                st.close();
        }
    }
    
    public void deleteByOwnerId(Connection conn) throws SQLException {
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
        return "userTieredDiscount";
    }
}