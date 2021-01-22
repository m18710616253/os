package cn.ctyun.oos.server.db.dbpay;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;

import cn.ctyun.common.db.JdbcRow;
import common.tuple.Pair;

/**
 * 用户历史统计量表
 *
 * @author Dong chk
 *
 */
public class DbUsageHistory implements JdbcRow {
    public static final int PAGE_COUNT = 10000;
    
    public long id;
    public long usrId;
    public String date;
    public String dateBegin;
    public String dateEnd;
    public long totalSize = 0;
    public long peakSize = 0;
    public long upload = 0;
    public long transfer = 0;
    public long ghRequest = 0;
    public long otherRequest = 0;
    // 指示是否计过费,0表示未付费，1表示已付费
    public byte flag = 0;
    public long restTransfer = 0;
    public long restGhRequest = 0;
    public long restOtherRequest = 0;
    public HashSet<Pair<Long,String>> ids = new HashSet<Pair<Long,String>>();//ownerId, regionName
    public LinkedHashSet<String> records = new LinkedHashSet<String>();
    public String regionName;
    public long roamUpload = 0;
    public long roamFlow = 0;
    public long restRoamFlow = 0;
    public long restRoamUpload = 0;
    public long originalTotalSize = 0;
    public long originalPeakSize = 0;
    //非互联网流量
    public long noNetFlow = 0;
    public long noNetUpload = 0;
    public long noNetRoamFlow = 0;
    public long noNetRoamUpload = 0;
    //非互联网剩余流量
    public long restNoNetFlow = 0;
    public long restNoNetRoamFlow = 0;
    public long restNoNetRoamUpload = 0;
    //非互联网请求
    public long noNetGHReq = 0;
    public long noNetOtherReq = 0;
    //非互联网剩余请求
    public long restNoNetGHReq = 0;
    public long restNoNetOtherReq = 0;
    //冗余容量、对齐后容量
    public long redundantSize = 0;
    public long alinSize = 0;
    // 图片鉴黄、文本反垃圾调用量
    public long spamRequest = 0;
    public long pornReviewFalse = 0;
    public long pornReviewTrue = 0;
    public long restSpamRequest = 0;
    public long restPornReviewFalse = 0;
    public long restPornReviewTrue = 0;
    public DbUsageHistory(long usrId, String regionName) {
        this.usrId = usrId;
        this.regionName = regionName;
    }

    public DbUsageHistory() {
    }

    public static void create(Connection conn) throws SQLException {
        String create = "CREATE TABLE IF NOT EXISTS `" + getTableName() + "` (\n"
                + "  `id`  BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT,\n"
                + "  `usrId` bigint(30) NOT NULL,\n"
                + "  `regionName` VARCHAR(20) NOT NULL,\n"
                + "  `date` VARCHAR(20) NOT NULL ,\n" + "  `totalSize` BIGINT(20) DEFAULT '0',\n"
                + "  `peakSize` BIGINT(20) DEFAULT '0',\n" + "  `upload` BIGINT(20) DEFAULT '0',\n"
                + "  `transfer` BIGINT(20) UNSIGNED DEFAULT '0',\n"
                + "  `ghRequest` BIGINT(20) UNSIGNED DEFAULT '0',\n"
                + "  `otherRequest` BIGINT(20) UNSIGNED DEFAULT '0',\n"
                + "  `restTransfer` INT(10) UNSIGNED DEFAULT '0',\n"
                + "  `restGhRequest` INT(10) UNSIGNED DEFAULT '0',\n"
                + "  `restOtherRequest` INT(10) UNSIGNED DEFAULT '0',\n"
                + "  `flag` TINYINT(1) DEFAULT '0',\n"
                + "  `roamUpload` BIGINT(20) DEFAULT '0',\n"
                + "  `roamFlow` BIGINT(20) DEFAULT '0',\n"
                + "  `restRoamUpload` INT(10) UNSIGNED DEFAULT '0',\n"
                + "  `restRoamFlow` INT(10) UNSIGNED DEFAULT '0',\n"
                + "  `originalTotalSize` BIGINT(20) DEFAULT '0',\n"
                + "  `originalPeakSize` BIGINT(20) DEFAULT '0',\n"
                + "  `noNetFlow` BIGINT(20) DEFAULT '0',\n"
                + "  `noNetUpload` BIGINT(20) DEFAULT '0',\n"
                + "  `noNetRoamFlow` BIGINT(20) DEFAULT '0',\n"
                + "  `noNetRoamUpload` BIGINT(20) DEFAULT '0',\n"
                + "  `restNoNetFlow` INT(10) UNSIGNED DEFAULT '0',\n"
                + "  `restNoNetRoamFlow` INT(10) UNSIGNED DEFAULT '0',\n"
                + "  `restNoNetRoamUpload` INT(10) UNSIGNED DEFAULT '0',\n"
                + "  `noNetGHReq` BIGINT(20) UNSIGNED DEFAULT '0',\n"
                + "  `noNetOtherReq` BIGINT(20) UNSIGNED DEFAULT '0',\n"
                + "  `restNoNetGHReq` INT(10) UNSIGNED DEFAULT '0',\n"
                + "  `restNoNetOtherReq` INT(10) UNSIGNED DEFAULT '0',\n"
                + "  `redundantSize` BIGINT(20) DEFAULT '0',\n"
                + "  `alinSize` BIGINT(20) DEFAULT '0',\n"
                + "  `spamRequest` BIGINT(20) UNSIGNED DEFAULT '0',\n"
                + "  `pornReviewFalse` BIGINT(20) UNSIGNED DEFAULT '0',\n"
                + "  `pornReviewTrue` BIGINT(20) UNSIGNED DEFAULT '0',\n"
                + "  `restSpamRequest` INT(10) UNSIGNED DEFAULT '0',\n"
                + "  `restPornReviewFalse` INT(10) UNSIGNED DEFAULT '0',\n"
                + "  `restPornReviewTrue` INT(10) UNSIGNED DEFAULT '0',\n"
                + "   PRIMARY KEY (`id`,`usrId`,`regionName`)"
                + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        Statement st = conn.createStatement();
        try {
            st.execute(create);
        } finally {
            st.close();
        }
    }

    @Override
    public void setBy(ResultSet resultSet, boolean ignore) throws SQLException {
        ResultSetMetaData metadata = resultSet.getMetaData();
        for (int i = 1; i <= metadata.getColumnCount(); i++) {
            String oname = metadata.getColumnName(i);
            if (oname.equals("id")) {
                id = resultSet.getLong(i);
            } else if (oname.equals("usrId")) {
                usrId = resultSet.getLong(i);
            } else if (oname.equals("date")) {
                date = resultSet.getString(i);
            } else if (oname.equals("totalSize")) {
                totalSize = resultSet.getLong(i);
            } else if (oname.equals("peakSize")) {
                peakSize = resultSet.getLong(i);
            } else if (oname.equals("upload")) {
                upload = resultSet.getLong(i);
            } else if (oname.equals("transfer")) {
                transfer = resultSet.getLong(i);
            } else if (oname.equals("ghRequest")) {
                ghRequest = resultSet.getLong(i);
            } else if (oname.equals("otherRequest")) {
                otherRequest = resultSet.getLong(i);
            } else if (oname.equals("restTransfer")) {
                restTransfer = resultSet.getLong(i);
            } else if (oname.equals("restGhRequest")) {
                restGhRequest = resultSet.getLong(i);
            } else if (oname.equals("restOtherRequest")) {
                restOtherRequest = resultSet.getLong(i);
            } else if (oname.equals("flag")) {
                flag = resultSet.getByte(i);
            } else if (oname.equals("regionName")) {
                regionName = resultSet.getString(i);
            } else if (oname.equals("roamUpload")) {
                roamUpload = resultSet.getLong(i);
            } else if (oname.equals("roamFlow")) {
                roamFlow = resultSet.getLong(i);
            } else if (oname.equals("originalTotalSize")) {
                originalTotalSize = resultSet.getLong(i);
            } else if (oname.equals("originalPeakSize")) {
                originalPeakSize = resultSet.getLong(i);
            } else if (oname.equals("restRoamFlow")) {
                restRoamFlow = resultSet.getLong(i);
            } else if (oname.equals("restRoamUpload")) {
                restRoamUpload = resultSet.getLong(i);
            } else if (oname.equals("noNetFlow")) {
                noNetFlow = resultSet.getLong(i);
            } else if (oname.equals("noNetUpload")) {
                noNetUpload = resultSet.getLong(i);
            } else if (oname.equals("noNetRoamFlow")) {
                noNetRoamFlow = resultSet.getLong(i);
            } else if (oname.equals("noNetRoamUpload")) {
                noNetRoamUpload = resultSet.getLong(i);
            } else if (oname.equals("restNoNetFlow")) {
                restNoNetFlow = resultSet.getLong(i);
            } else if (oname.equals("restNoNetRoamFlow")) {
                restNoNetRoamFlow = resultSet.getLong(i);
            } else if (oname.equals("restNoNetRoamUpload")) {
                restNoNetRoamUpload = resultSet.getLong(i);
            } else if (oname.equals("noNetGHReq")) {
                noNetGHReq = resultSet.getLong(i);
            } else if (oname.equals("noNetOtherReq")) {
                noNetOtherReq = resultSet.getLong(i);
            } else if (oname.equals("restNoNetGHReq")) {
                restNoNetGHReq = resultSet.getLong(i);
            } else if (oname.equals("restNoNetOtherReq")) {
                restNoNetOtherReq = resultSet.getLong(i);
            } else if (oname.equals("redundantSize")) {
                redundantSize = resultSet.getLong(i);
            } else if (oname.equals("alinSize")) {
                alinSize = resultSet.getLong(i);
            } else if (oname.equals("spamRequest")) {
                spamRequest = resultSet.getLong(i);
            } else if (oname.equals("pornReviewFalse")) {
                pornReviewFalse = resultSet.getLong(i);
            } else if (oname.equals("pornReviewTrue")) {
                pornReviewTrue = resultSet.getLong(i);
            } else if (oname.equals("restSpamRequest")) {
                restSpamRequest = resultSet.getLong(i);
            } else if (oname.equals("restPornReviewFalse")) {
                restPornReviewFalse = resultSet.getLong(i);
            } else if (oname.equals("restPornReviewTrue")) {
                restPornReviewTrue = resultSet.getLong(i);
            } else {
                if (!ignore)
                    throw new RuntimeException("Unknown column name : " + oname);
            }
        }
    }

    public void handle(ResultSet rs, Connection conn) throws SQLException {
        ids.add(new Pair<Long, String>(rs.getLong("usrId"), rs.getString("regionName")));
        totalSize += rs.getLong("totalSize");
        peakSize += rs.getLong("peakSize");
        upload += rs.getLong("upload");
        transfer += rs.getLong("transfer");
        ghRequest += rs.getLong("ghRequest");
        otherRequest += rs.getLong("otherRequest");
        originalTotalSize += rs.getLong("originalTotalSize");
        originalPeakSize += rs.getLong("originalPeakSize");
        roamUpload += rs.getLong("roamUpload");
        roamFlow += rs.getLong("roamFlow");
        noNetFlow += rs.getLong("noNetFlow");
        noNetUpload += rs.getLong("noNetUpload");
        noNetRoamFlow += rs.getLong("noNetRoamFlow");
        noNetRoamUpload += rs.getLong("noNetRoamUpload");
        restNoNetFlow += rs.getLong("restNoNetFlow");
        restNoNetRoamFlow += rs.getLong("restNoNetRoamFlow");
        restNoNetRoamUpload += rs.getLong("restNoNetRoamUpload");
        noNetGHReq += rs.getLong("noNetGHReq");
        noNetOtherReq += rs.getLong("noNetOtherReq");
        restNoNetGHReq += rs.getLong("restNoNetGHReq");
        restNoNetOtherReq += rs.getLong("restNoNetOtherReq");
        redundantSize += rs.getLong("redundantSize");
        alinSize += rs.getLong("alinSize");
        spamRequest += rs.getLong("spamRequest");
        pornReviewFalse += rs.getLong("pornReviewFalse");
        pornReviewTrue += rs.getLong("pornReviewTrue");
    }

    @Override
    public boolean select(Connection conn) throws SQLException {
        PreparedStatement st = conn.prepareStatement("SELECT * FROM `" + getTableName()
                + "` WHERE usrId=? " + "AND " + "date>=? " + "AND " + "date<=? " + "AND "
                + "flag=? AND regionName=?");
        try {
            st.setLong(1, usrId);
            st.setString(2, dateBegin);
            st.setString(3, dateEnd);
            st.setByte(4, flag);
            st.setString(5, regionName);
            ResultSet rs = st.executeQuery();
            try {
                while (rs.next()) {
                    handle(rs, conn);
                }
                return true;
            } finally {
                rs.close();
            }
        } finally {
            st.close();
        }
    }

    public boolean selectByDate(Connection conn) throws SQLException {
        PreparedStatement st = conn.prepareStatement("SELECT * FROM `" + getTableName()
                + "` WHERE usrId=? AND regionName=? AND date=?");
        try {
            st.setLong(1, usrId);
            st.setString(2, regionName);
            st.setString(3, date);
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

    public void selectSum(Connection conn) throws SQLException {
        PreparedStatement st = conn.prepareStatement("SELECT regionName," + " SUM(upload) AS upload,"
                + " SUM(transfer) AS transfer," + " SUM(ghRequest) AS ghRequest,"
                + " SUM(otherRequest) AS otherRequest" + " FROM `" + getTableName()
                + "` WHERE date>=? " + "AND " + "date<=? " + "AND " + "regionName=?");
        try {
            st.setString(1, dateBegin);
            st.setString(2, dateEnd);
            st.setString(3, regionName);
            ResultSet rs = st.executeQuery();
            try {
                if (rs.next()) {
                    handle3(rs);
                }
            } finally {
                rs.close();
            }
        } finally {
            st.close();
        }
    }

    public void handle3(ResultSet resultSet) throws SQLException {
        ResultSetMetaData metadata = resultSet.getMetaData();
        for (int i = 1; i <= metadata.getColumnCount(); i++) {
            String oname = metadata.getColumnName(i);
            if (oname.equals("regionName")) {
                regionName = resultSet.getString(i);
            } else if (oname.equals("upload")) {
                upload = resultSet.getLong(i);
            } else if (oname.equals("transfer")) {
                transfer = resultSet.getLong(i);
            } else if (oname.equals("ghRequest")) {
                ghRequest = resultSet.getLong(i);
            } else if (oname.equals("otherRequest")) {
                otherRequest = resultSet.getLong(i);
            } else {
                continue;
            }
        }
    }

    public boolean selectById(Connection conn) throws SQLException {
        PreparedStatement st = conn.prepareStatement("SELECT * FROM `" + getTableName()
                + "` WHERE id = ?");
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

    public void selectUsrIds(Connection conn) throws SQLException {
        PreparedStatement st = conn.prepareStatement("SELECT usrId,regionName FROM `" + getTableName()
                + "` where flag=? AND date>=? AND date<=?");
        try {
            st.setByte(1, flag);
            st.setString(2, dateBegin);
            st.setString(3, dateEnd);
            ResultSet rs = st.executeQuery();
            try {
                while (rs.next()) {
                    ids.add(new Pair<Long, String>(rs.getLong("usrId"), rs.getString("regionName")));
                }
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
                        + "` ( usrId, date, totalSize, peakSize, upload, transfer, ghRequest, otherRequest, restTransfer, restGhRequest, restOtherRequest, regionName,originalTotalSize,originalPeakSize,roamUpload,roamFlow,restRoamFlow,restRoamUpload,noNetFlow,noNetUpload,noNetRoamFlow,noNetRoamUpload,restNoNetFlow,restNoNetRoamFlow,restNoNetRoamUpload,noNetGHReq,noNetOtherReq,restNoNetGHReq,restNoNetOtherReq,redundantSize,alinSize,spamRequest,pornReviewFalse,pornReviewTrue,restSpamRequest,restPornReviewFalse,restPornReviewTrue) "
                        + "VALUES (?, ?, ?, ?, ?, ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
        try {
            st.setLong(1, usrId);
            st.setString(2, date);
            st.setLong(3, totalSize);
            st.setLong(4, peakSize);
            st.setLong(5, upload);
            st.setLong(6, transfer);
            st.setLong(7, ghRequest);
            st.setLong(8, otherRequest);
            st.setLong(9, restTransfer);
            st.setLong(10, restGhRequest);
            st.setLong(11, restOtherRequest);
            st.setString(12, regionName);
            st.setLong(13, originalTotalSize);
            st.setLong(14, originalPeakSize);
            st.setLong(15, roamUpload);
            st.setLong(16, roamFlow);
            st.setLong(17, restRoamFlow);
            st.setLong(18, restRoamUpload);
            st.setLong(19, noNetFlow);
            st.setLong(20, noNetUpload);
            st.setLong(21, noNetRoamFlow);
            st.setLong(22, noNetRoamUpload);
            st.setLong(23, restNoNetFlow);
            st.setLong(24, restNoNetRoamFlow);
            st.setLong(25, restNoNetRoamUpload);
            st.setLong(26, noNetGHReq);
            st.setLong(27, noNetOtherReq);
            st.setLong(28, restNoNetGHReq);
            st.setLong(29, restNoNetOtherReq);
            st.setLong(30, redundantSize);
            st.setLong(31, alinSize);
            st.setLong(32, spamRequest);
            st.setLong(33, pornReviewFalse);
            st.setLong(34, pornReviewTrue);
            st.setLong(35, restSpamRequest);
            st.setLong(36, restPornReviewFalse);
            st.setLong(37, restPornReviewTrue);
            st.executeUpdate();
        } finally {
            st.close();
        }
    }

    @Override
    public void update(Connection conn) throws SQLException {
        PreparedStatement updateState = conn.prepareStatement("UPDATE `" + getTableName()
                + "` SET " + "flag=? " + "WHERE usrId=? AND regionName=? ");
        try {
            updateState.setShort(1, flag);
            updateState.setLong(2, usrId);
            updateState.setString(3, regionName);
            updateState.executeUpdate();
        } finally {
            updateState.close();
        }
    }

    public void update2(Connection conn) throws SQLException {
        flag = 1;
        Iterator<Pair<Long,String>> itr = ids.iterator();
        while (itr.hasNext()) {
            Pair<Long, String> it = itr.next();
            usrId = it.first();
            regionName=it.second();
            update(conn);
        }
    }

    public void updateRecord(Connection conn) throws SQLException {
        PreparedStatement updateState = conn.prepareStatement("UPDATE `" + getTableName()
                + "` SET " + "totalSize=? " + ",peakSize=? " + ",upload=? " + ",transfer=? "
                + ",ghRequest=? " + ",otherRequest=? "
                + ",originalTotalSize=? " + ",originalPeakSize=? "
                + ",roamUpload=? " + ",roamFlow=? "
                + ",noNetFlow=? " + ",noNetUpload=? "
                + ",noNetRoamFlow=? " + ",noNetRoamUpload=? "
                + ",noNetGHReq=? " + ",noNetOtherReq=? "
                + ",redundantSize=? " + ",alinSize=? "
                + ", spamRequest  =? " + ", pornReviewFalse  =? " + ", pornReviewTrue  =? "
                + "WHERE usrId=? AND date=? and regionName=? ");
        try {
            updateState.setLong(1, totalSize);
            updateState.setLong(2, peakSize);
            updateState.setLong(3, upload);
            updateState.setLong(4, transfer);
            updateState.setLong(5, ghRequest);
            updateState.setLong(6, otherRequest);
            updateState.setLong(7, originalTotalSize);
            updateState.setLong(8, originalPeakSize);
            updateState.setLong(9, roamUpload);
            updateState.setLong(10, roamFlow);
            updateState.setLong(11, noNetFlow);
            updateState.setLong(12, noNetUpload);
            updateState.setLong(13, noNetRoamFlow);
            updateState.setLong(14, noNetRoamUpload);
            updateState.setLong(15, noNetGHReq);
            updateState.setLong(16, noNetOtherReq);
            updateState.setLong(17, redundantSize);
            updateState.setLong(18, alinSize);
            updateState.setLong(19, spamRequest);
            updateState.setLong(20, pornReviewFalse);
            updateState.setLong(21, pornReviewTrue);
            updateState.setLong(22, usrId);
            updateState.setString(23, date);
            updateState.setString(24, regionName);
            updateState.executeUpdate();
        } finally {
            updateState.close();
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

    @Override
    public void delete(Connection conn) throws SQLException {
        PreparedStatement st = conn.prepareStatement("DELETE FROM `" + getTableName()
                + "` WHERE usrId=? " + "AND " + "date>=? " + "AND " + "date<?");
        try {
            st.setLong(1, usrId);
            st.setString(2, dateBegin);
            st.setString(3, dateEnd);
            st.executeUpdate();
        } finally {
            st.close();
        }
    }

    public static String getTableName() {
        return "usageHistory";
    }
}
