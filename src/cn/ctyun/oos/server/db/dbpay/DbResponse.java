package cn.ctyun.oos.server.db.dbpay;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import cn.ctyun.common.db.JdbcRow;

public class DbResponse implements JdbcRow {
    /* M = must, O = option, */
    /*
     * The transaction serial number from Bestpay,merchant need to keep it for
     * check up bigint(30) M
     */
    public String UPTRANSEQ;
    /* Transaction date in Bestpay, format is yyyyMMDD int(8) M */
    public int TRANDATE;
    /*
     * Processing results from Bestpay, used for check up, 0000 means success
     * int(4) M
     */
    public String RETNCODE;
    /* interpretive code for processing result code varchar(10) M */
    public String RETNINFO;
    /* Order request transaction sequence from merchant varchar(32) M */
    public String ORDERREQTRANSEQ;
    /* Order sequence number for merchant varchar(32) M */
    public String ORDERSEQ;
    /* Amount of order from merchant int(10) M */
    public int ORDERAMOUNT;
    /* Amount of product, from merchant int(10) M */
    public int PRODUCTAMOUNT;
    /* Attached amount from merchant, int(10) M */
    public int ATTACHAMOUNT;
    /* Currency type, RMB is default varchar(3) M */
    public String CURTYPE;
    /* encode type from merchant, 1 for MD5 encode int(1) M */
    public int ENCODETYPE;
    /* Bank id for client who used to pay the bill varchar(10) M */
    public String BANKID;
    /* Attached information from client varchar(32) O */
    public String ATTACH;
    /* signature from Bestpay for data integrity verification varchar(256) M */
    public String SIGN;

    public static void creat(Connection conn) throws SQLException {
        String create = "CREATE TABLE `" + getTableName() + "` (\n"
                + "  `UPTRANSEQ`  varchar(30) NOT NULL,\n"
                + "  `TRANDATE` INTEGER(8) NOT NULL,\n"
                + "  `RETNCODE` varchar(4) NOT NULL,\n"
                + "  `RETNINFO` varchar(10) NOT NULL,\n"
                + "  `ORDERREQTRANSEQ` varchar(32) NOT NULL,\n"
                + "  `ORDERSEQ` varchar(32) NOT NULL,\n"
                + "  `ORDERAMOUNT` INTEGER(10) NOT NULL,\n"
                + "  `PRODUCTAMOUNT` INTEGER(10) NOT NULL,\n"
                + "  `ATTACHAMOUNT` INTEGER(20) NOT NULL,\n"
                + "  `CURTYPE` varchar(10) NOT NULL,\n"
                + "  `ENCODETYPE` INTEGER(1) NOT NULL,\n"
                + "  `BANKID` varchar(10) NOT NULL,\n"
                + "  `ATTACH` varchar(32) DEFAULT NULL,\n"
                + "  `SIGN` varchar(256) NOT NULL,\n"
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
            if (aname.equals("UPTRANSEQ")) {
                UPTRANSEQ = resultSet.getString(i);
            } else if (aname.equals("TRANDATE")) {
                TRANDATE = resultSet.getInt(i);
            } else if (aname.equals("RETNCODE")) {
                RETNCODE = resultSet.getString(i);
            } else if (aname.equals("RETNINFO")) {
                RETNINFO = resultSet.getString(i);
            } else if (aname.equals("ORDERREQTRANSEQ")) {
                ORDERREQTRANSEQ = resultSet.getString(i);
            } else if (aname.equals("ORDERSEQ")) {
                ORDERSEQ = resultSet.getString(i);
            } else if (aname.equals("ORDERAMOUNT")) {
                ORDERAMOUNT = resultSet.getInt(i);
            } else if (aname.equals("PRODUCTAMOUNT")) {
                PRODUCTAMOUNT = resultSet.getInt(i);
            } else if (aname.equals("ATTACHAMOUNT")) {
                ATTACHAMOUNT = resultSet.getInt(i);
            } else if (aname.equals("CURTYPE")) {
                CURTYPE = resultSet.getString(i);
            } else if (aname.equals("ENCODETYPE")) {
                ENCODETYPE = resultSet.getInt(i);
            } else if (aname.equals("BANKID")) {
                BANKID = resultSet.getString(i);
            } else if (aname.equals("ATTACH")) {
                ATTACH = resultSet.getString(i);
            } else if (aname.equals("SIGN")) {
                SIGN = resultSet.getString(i);
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
                    .prepareStatement("SELECT　UPTRANSEQ, TRANDATE, RETNCODE, RETNINFO, ORDERREQTRANSEQ, ORDERAMOUNT, PRODUCTAMOUNT,"
                            + " ATTACHAMOUNT, CURTYPE, ENCODETYPE, BANKID, ATTACH, SIGN FROM "
                            + getTableName() + " WHERE ORDERSEQ=?");
            st.setString(1, ORDERSEQ);
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
                            + "(UPTRANSEQ, TRANDATE, RETNCODE, RETNINFO, ORDERREQTRANSEQ, ORDERSEQ, ORDERAMOUNT, PRODUCTAMOUNT, "
                            + "ATTACHAMOUNT, CURTYPE, ENCODETYPE, BANKID, ATTACH, SIGN) "
                            + "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
            st.setString(1, UPTRANSEQ);
            st.setInt(2, TRANDATE);
            st.setString(3, RETNCODE);
            st.setString(4, RETNINFO);
            st.setString(5, ORDERREQTRANSEQ);
            st.setString(6, ORDERSEQ);
            st.setLong(7, ORDERAMOUNT);
            st.setLong(8, PRODUCTAMOUNT);
            st.setLong(9, ATTACHAMOUNT);
            st.setString(10, CURTYPE);
            st.setInt(11, ENCODETYPE);
            st.setString(12, BANKID);
            st.setString(13, ATTACH);
            st.setString(14, SIGN);
            st.executeUpdate();
        } finally {
            if (st != null)
                st.close();
        }
    }

    @Override
    public void update(Connection conn) throws SQLException {
        throw new RuntimeException("not implemented yet.");
    }

    @Override
    public void delete(Connection conn) throws SQLException {
        throw new RuntimeException("not implemented yet.");
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
        return "response";
    }
}
