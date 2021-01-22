package cn.ctyun.oos.server.db.dbpay;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedHashSet;

import cn.ctyun.common.db.JdbcRow;

public class DbBill implements JdbcRow {
    /*
     * M = must, which means field cannot be null, O = option, which means field
     * is option describe length option
     */
    /* ID of merchant, offered by Bestpay integer(30) M */
    private long MERCHANTID;
    /* Sub ID of merchant, specified by marchant varchar(30) O */
    public String SUBMERCHANTID;
    /* Order sequence,primary key varchar(30) M */
    private String ORDERSEQ;
    /* Order request transaction sequence, generate by merchant varchar(30) M */
    private String ORDERREQTRANSEQ;
    /* Date of order, format yyyyMMDD integer(8) M */
    private String ORDERDATE;
    /* Amount money of order integer(10) M */
    private long ORDERAMOUNT;
    /* Amount money of product integer(10) M */
    private long PRODUCTAMOUNT;
    /* Attach amount of product default is 0 integer(10) M */
    private long ATTACHAMOUNT;
    /* Currency type default RMB varchar(3) M */
    private String CURTYPE;
    /* Encode type, 0 for No encryption, 1 for encryption by MD5 integer(1) M */
    private int ENCODETYPE;
    /* URL address for foreground, only for display in foreground varchar(255) M */
    private String MERCHANTURL;
    /* Backend URL address for transaction processing varchar(255) M */
    private String BACKMERCHANTURL;
    /* Attach information for merchant varchar(128) O */
    public String ATTACH;
    /* Business type, 0001 is default integer(4) M */
    private String BUSICODE;
    /* Product id for service identifier integer(2) M */
    private String PRODUCTID;
    /* Terminal number integer(40) M */
    private String TMNUM;
    /* Customer identification, store the information of ownerId varchar(32) M */
    private String CUSTOMERID;
    /* Description of product varchar(512) M */
    private String PRODUCTDESC;
    /* Check domain, for data integrity varchar(256) M */
    private String MAC;
    // /* Account detail varchar(256) O */
    // public String DIVDETAILS;
    // /*
    // * The numbers of payment by instalments, only for user from China
    // Merchants
    // * Bank integer(2) O
    // */
    // public int PEDCNT;
    /* IP address of merchant varchar(15) M */
    private String CLIENTIP;
    /*
     * use tag to identify the current status of bill, 0 means fail 1 means
     * success
     */
    public int tag;
    public String dateBegin;
    public String dateEnd;
    public LinkedHashSet<String> records = new LinkedHashSet<String>();

    public static void create(Connection conn) throws SQLException {
        String create = "CREATE TABLE `" + getTableName() + "` (\n"
                + "  `ORDERSEQ` varchar(30) NOT NULL,\n"
                + "  `MERCHANTID` INTEGER(30) NOT NULL,\n"
                + "  `SUBMERCHANTID` varchar(30) DEFAULT NULL,\n"
                + "  `ORDERREQTRANSEQ` varchar(30) NOT NULL,\n"
                + "  `ORDERDATE` varchar(19) NOT NULL,\n"
                + "  `ORDERAMOUNT` BIGINT(10) NOT NULL,\n"
                + "  `PRODUCTAMOUNT` BIGINT(10) NOT NULL,\n"
                + "  `ATTACHAMOUNT` BIGINT(10) NOT NULL,\n"
                + "  `CURTYPE` varchar(3) NOT NULL,\n"
                + "  `ENCODETYPE` INTEGER(1) NOT NULL,\n"
                + "  `MERCHANTURL` varchar(255) NOT NULL,\n"
                + "  `BACKMERCHANTURL` varchar(255) NOT NULL,\n"
                + "  `ATTACH` varchar(128) DEFAULT NULL,\n"
                + "  `BUSICODE` varchar(4) NOT NULL,\n"
                + "  `PRODUCTID` varchar(2) NOT NULL,\n"
                + "  `TMNUM` varchar(40) NOT NULL,\n"
                + "  `CUSTOMERID` varchar(32) NOT NULL,\n"
                + "  `PRODUCTDESC` varchar(512) NOT NULL,\n"
                + "  `MAC` varchar(256) NOT NULL,\n"
                + "  `CLIENTIP` varchar(15) NOT NULL,\n"
                + "  `tag` INTEGER(1) DEFAULT '0',\n"
                + "  PRIMARY KEY (`ORDERSEQ`)\n"
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
    public void setBy(ResultSet resultSet, boolean ignore) throws SQLException {
        ResultSetMetaData metadata = resultSet.getMetaData();
        for (int i = 1; i <= metadata.getColumnCount(); i++) {
            String aname = metadata.getColumnName(i);
            if (aname.equals("ORDERSEQ")) {
                ORDERSEQ = resultSet.getString(i);
            } else if (aname.equals("MERCHANTID")) {
                MERCHANTID = resultSet.getLong(i);
            } else if (aname.equals("SUBMERCHANTID")) {
                SUBMERCHANTID = resultSet.getString(i);
            } else if (aname.equals("ORDERREQTRANSEQ")) {
                ORDERREQTRANSEQ = resultSet.getString(i);
            } else if (aname.equals("ORDERDATE")) {
                ORDERDATE = resultSet.getString(i);
            } else if (aname.equals("ORDERAMOUNT")) {
                ORDERAMOUNT = resultSet.getLong(i);
            } else if (aname.equals("PRODUCTAMOUNT")) {
                PRODUCTAMOUNT = resultSet.getLong(i);
            } else if (aname.equals("ATTACHAMOUNT")) {
                ATTACHAMOUNT = resultSet.getLong(i);
            } else if (aname.equals("CURTYPE")) {
                CURTYPE = resultSet.getString(i);
            } else if (aname.equals("ENCODETYPE")) {
                ENCODETYPE = resultSet.getInt(i);
            } else if (aname.equals("MERCHANTURL")) {
                MERCHANTURL = resultSet.getString(i);
            } else if (aname.equals("BACKMERCHANTURL")) {
                BACKMERCHANTURL = resultSet.getString(i);
            } else if (aname.equals("ATTACH")) {
                ATTACH = resultSet.getString(i);
            } else if (aname.equals("BUSICODE")) {
                BUSICODE = resultSet.getString(i);
            } else if (aname.equals("PRODUCTID")) {
                PRODUCTID = resultSet.getString(i);
            } else if (aname.equals("TMNUM")) {
                TMNUM = resultSet.getString(i);
            } else if (aname.equals("CUSTOMERID")) {
                CUSTOMERID = resultSet.getString(i);
            } else if (aname.equals("PRODUCTDESC")) {
                PRODUCTDESC = resultSet.getString(i);
            } else if (aname.equals("MAC")) {
                MAC = resultSet.getString(i);
            } else if (aname.equals("CLIENTIP")) {
                CLIENTIP = resultSet.getString(i);
            } else if (aname.equals("tag")) {
                tag = resultSet.getInt(i);
            } else {
                if (!ignore)
                    throw new RuntimeException("Unknown column name : " + aname);
            }
        }
    }

    @Override
    public boolean select(Connection conn) throws SQLException {
        PreparedStatement st = null;
        try {
            st = conn
                    .prepareStatement("SELECT MERCHANTID, SUBMERCHANTID, ORDERREQTRANSEQ, ORDERDATE, ORDERAMOUNT, PRODUCTAMOUNT, ATTACHAMOUNT, CURTYPE, "
                            + "ENCODETYPE, MERCHANTURL, BACKMERCHANTURL, ATTACH, BUSICODE, PRODUCTID, TMNUM, CUSTOMERID, PRODUCTDESC, MAC, CLIENTIP, tag FROM "
                            + getTableName() + " WHERE ORDERSEQ = ?");
            st.setString(1, ORDERSEQ);
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
                if (rs != null) {
                    rs.close();
                }
            }
        } finally {
            if (st != null)
                st.close();
        }
    }

    private void handle(ResultSet rs) throws SQLException {
        StringBuilder sb = new StringBuilder();
        sb.append(rs.getString("ORDERSEQ")).append(" ")
                .append(rs.getDouble("ORDERAMOUNT")).append(" ")
                .append(rs.getString("ORDERDATE")).append(" ")
                .append(rs.getInt("tag"));
        records.add(sb.toString());
    }

    public void selectByRange(Connection conn) throws SQLException {
        PreparedStatement st = null;
        try {
            st = conn
                    .prepareStatement("SELECT ORDERSEQ, MERCHANTID, SUBMERCHANTID, ORDERREQTRANSEQ, ORDERDATE, ORDERAMOUNT, PRODUCTAMOUNT, ATTACHAMOUNT, CURTYPE, "
                            + "ENCODETYPE, MERCHANTURL, BACKMERCHANTURL, ATTACH, BUSICODE, PRODUCTID, TMNUM, PRODUCTDESC, MAC, CLIENTIP, tag FROM "
                            + getTableName()
                            + " WHERE CUSTOMERID = ? and ORDERDATE>=? and ORDERDATE<= ? order by ORDERDATE desc");
            st.setString(1, CUSTOMERID);
            st.setString(2, dateBegin);
            st.setString(3, dateEnd);
            ResultSet rs = null;
            try {
                rs = st.executeQuery();
                while (rs.next()) {
                    handle(rs);
                }
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

    public boolean selectByOwner(Connection conn) throws SQLException {
        PreparedStatement st = null;
        try {
            st = conn
                    .prepareStatement("SELECT MERCHANTID, ORDERSEQ, SUBMERCHANTID, ORDERREQTRANSEQ, ORDERDATE, ORDERAMOUNT, PRODUCTAMOUNT, ATTACHAMOUNT, CURTYPE, "
                            + "ENCODETYPE, MERCHANTURL, BACKMERCHANTURL, ATTACH, BUSICODE, PRODUCTID, TMNUM, PRODUCTDESC, MAC, CLIENTIP, tag FROM "
                            + getTableName() + " WHERE CUSTOMERID = ?");
            st.setString(1, CUSTOMERID);
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
    public void insert(Connection conn) throws SQLException {
        PreparedStatement st = null;
        try {
            st = conn
                    .prepareStatement("INSERT "
                            + getTableName()
                            + "( ORDERSEQ, MERCHANTID, SUBMERCHANTID, ORDERREQTRANSEQ, ORDERDATE, ORDERAMOUNT, PRODUCTAMOUNT, ATTACHAMOUNT, CURTYPE, "
                            + "ENCODETYPE, MERCHANTURL, BACKMERCHANTURL, ATTACH, BUSICODE, PRODUCTID, TMNUM, CUSTOMERID, PRODUCTDESC, MAC, CLIENTIP, tag )"
                            + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
            st.setString(1, ORDERSEQ);
            st.setLong(2, MERCHANTID);
            st.setString(3, SUBMERCHANTID);
            st.setString(4, ORDERREQTRANSEQ);
            st.setString(5, ORDERDATE);
            st.setLong(6, ORDERAMOUNT);
            st.setLong(7, PRODUCTAMOUNT);
            st.setLong(8, ATTACHAMOUNT);
            st.setString(9, CURTYPE);
            st.setInt(10, ENCODETYPE);
            st.setString(11, MERCHANTURL);
            st.setString(12, BACKMERCHANTURL);
            st.setString(13, ATTACH);
            st.setString(14, BUSICODE);
            st.setString(15, PRODUCTID);
            st.setString(16, TMNUM);
            st.setString(17, CUSTOMERID);
            st.setString(18, PRODUCTDESC);
            st.setString(19, MAC);
            st.setString(20, CLIENTIP);
            st.setInt(21, tag);
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
            st = conn.prepareStatement("UPDATE " + getTableName() + " SET "
                    + "tag=? " + "WHERE ORDERSEQ=? ");
            st.setInt(1, tag);
            st.setString(2, ORDERSEQ);
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
                    + " WHERE ORDERSEQ=?");
            st.setString(1, ORDERSEQ);
            st.executeUpdate();
        } finally {
            if (st != null)
                st.close();
        }
    }
    
    public int deleteBillByOwnerId(Connection conn) throws SQLException {
        PreparedStatement st = null;
        try {
            st = conn.prepareStatement("DELETE FROM " + getTableName()
            + " WHERE CUSTOMERID = ?"
            + " AND  ORDERDATE<=?" + " LIMIT " + 100); 
        st.setString(1, CUSTOMERID);
        st.setString(2, ORDERDATE);
        return st.executeUpdate();
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
        return "bill";
    }

    public long getMERCHANTID() {
        return MERCHANTID;
    }

    public void setMERCHANTID(long mERCHANTID) {
        MERCHANTID = mERCHANTID;
    }

    public String getORDERSEQ() {
        return ORDERSEQ;
    }

    public void setORDERSEQ(String oRDERSEQ) {
        ORDERSEQ = oRDERSEQ;
    }

    public String getORDERREQTRANSEQ() {
        return ORDERREQTRANSEQ;
    }

    public void setORDERREQTRANSEQ(String oRDERREQTRANSEQ) {
        ORDERREQTRANSEQ = oRDERREQTRANSEQ;
    }

    public String getORDERDATE() {
        return ORDERDATE.substring(0, 4) + ORDERDATE.substring(5, 7)
                + ORDERDATE.substring(8, 10);
    }

    public void setORDERDATE(String oRDERDATE) {
        ORDERDATE = oRDERDATE;
    }

    public long getORDERAMOUNT() {
        return ORDERAMOUNT;
    }

    public void setORDERAMOUNT(long oRDERAMOUNT) {
        ORDERAMOUNT = oRDERAMOUNT;
    }

    public long getPRODUCTAMOUNT() {
        return PRODUCTAMOUNT;
    }

    public void setPRODUCTAMOUNT(long pRODUCTAMOUNT) {
        PRODUCTAMOUNT = pRODUCTAMOUNT;
    }

    public long getATTACHAMOUNT() {
        return ATTACHAMOUNT;
    }

    public void setATTACHAMOUNT(long aTTACHAMOUNT) {
        ATTACHAMOUNT = aTTACHAMOUNT;
    }

    public String getCURTYPE() {
        return CURTYPE;
    }

    public void setCURTYPE(String cURTYPE) {
        CURTYPE = cURTYPE;
    }

    public String getBUSICODE() {
        return BUSICODE;
    }

    public void setBUSICODE(String bUSICODE) {
        BUSICODE = bUSICODE;
    }

    public int getENCODETYPE() {
        return ENCODETYPE;
    }

    public void setENCODETYPE(int eNCODETYPE) {
        ENCODETYPE = eNCODETYPE;
    }

    public String getMERCHANTURL() {
        return MERCHANTURL;
    }

    public void setMERCHANTURL(String mERCHANTURL) {
        MERCHANTURL = mERCHANTURL;
    }

    public String getBACKMERCHANTURL() {
        return BACKMERCHANTURL;
    }

    public void setBACKMERCHANTURL(String bACKMERCHANTURL) {
        BACKMERCHANTURL = bACKMERCHANTURL;
    }

    public String getPRODUCTID() {
        return PRODUCTID;
    }

    public void setPRODUCTID(String pRODUCTID) {
        PRODUCTID = pRODUCTID;
    }

    public String getTMNUM() {
        return TMNUM;
    }

    public void setTMNUM(String tMNUM) {
        TMNUM = tMNUM;
    }

    public String getCUSTOMERID() {
        return CUSTOMERID;
    }

    public void setCUSTOMERID(String cUSTOMERID) {
        CUSTOMERID = cUSTOMERID;
    }

    public String getPRODUCTDESC() {
        return PRODUCTDESC;
    }

    public void setPRODUCTDESC(String pRODUCTDESC) {
        PRODUCTDESC = pRODUCTDESC;
    }

    public String getMAC() {
        return MAC;
    }

    public void setMAC(String mAC) {
        MAC = mAC;
    }

    public String getCLIENTIP() {
        return CLIENTIP;
    }

    public void setCLIENTIP(String cLIENTIP) {
        CLIENTIP = cLIENTIP;
    }
}
