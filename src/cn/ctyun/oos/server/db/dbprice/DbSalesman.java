package cn.ctyun.oos.server.db.dbprice;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import common.util.HexUtils;
import cn.ctyun.common.Consts;
import cn.ctyun.common.db.JdbcRow;

/**
 * @author: Cui Meng
 */
public class DbSalesman implements JdbcRow {
    /** 销售姓名 */
    public String name = "";
    /** 销售工号 */
    public String id = "";
    /** 销售邮箱 */
    public String email = "";
    public String phone = "";
    /** 是否销售管理员，1是，0不是，默认0 */
    public int isManager = 0;
    private String password;
    /** 销售所在组织的id */
    public String organizationId;
    /** 翼支付账号 */
    public String bestPayAccount;
    public String verify;
    public List<DbSalesman> sms = new ArrayList<DbSalesman>();
    
    public DbSalesman(String email) {
        this.email = email;
    }
    
    public DbSalesman() {
    }
    
    public String getPwd() {
        if (password == null)
            return null;
        byte[] buf = HexUtils.toByteArray(password);
        for (int i = 0; i < buf.length / 2; i++) {
            byte tmp = buf[i];
            buf[i] = buf[buf.length - 1 - i];
            buf[buf.length - 1 - i] = tmp;
        }
        return new String(buf, Consts.CS_UTF8);
    }
    
    public void setPwd(String pwd) {
        if (pwd == null)
            pwd = "";
        byte[] buf = pwd.getBytes(Consts.CS_UTF8);
        for (int i = 0; i < buf.length / 2; i++) {
            byte tmp = buf[i];
            buf[i] = buf[buf.length - 1 - i];
            buf[buf.length - 1 - i] = tmp;
        }
        this.password = HexUtils.toHexString(buf);
    }
    
    public static void create(Connection conn) throws SQLException {
        String create = "CREATE TABLE `"
                + getTableName()
                + "` (\n"
                + "   `id` varchar(30) NOT NULL COMMENT '销售人员的电信员工号',"
                + "   `name` varchar(50) DEFAULT NULL,"
                + "   `email` varchar(64) NOT NULL,"
                + "   `organizationId` varchar(10) DEFAULT '0' COMMENT '对应organization表的id字段',"
                + "   `phone` varchar(20) DEFAULT NULL,"
                + "   `isManager` tinyint(1) DEFAULT NULL,"
                + "   `password` varchar(32) DEFAULT NULL,"
                + "     `bestPayAccount` varchar(64) DEFAULT NULL,"
                + "  `verify` varchar(50) DEFAULT NULL,\n"
                + "    PRIMARY KEY (`email`),"
                + "    KEY `organizationId` (`organizationId`) USING BTREE"
                + "   ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='销售人员信息表'";
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
            String aname = metadata.getColumnName(i);
            if (aname.equals("id")) {
                id = resultSet.getString(i);
            } else if (aname.equals("name")) {
                name = resultSet.getString(i);
            } else if (aname.equals("email")) {
                email = resultSet.getString(i);
            } else if (aname.equals("organizationId")) {
                organizationId = resultSet.getString(i);
            } else if (aname.equals("phone")) {
                phone = resultSet.getString(i);
            } else if (aname.equals("password")) {
                password = resultSet.getString(i);
            } else if (aname.equals("isManager")) {
                isManager = resultSet.getInt(i);
            } else if (aname.equals("bestPayAccount")) {
                bestPayAccount = resultSet.getString(i);
            } else if (aname.equals("verify")) {
                verify = resultSet.getString(i);
            } else {
                if (!ignore)
                    throw new RuntimeException("Unknown column name : " + aname);
            }
        }
    }
    
    @Override
    public boolean select(Connection conn) throws SQLException {
        PreparedStatement st = conn
                .prepareStatement("SELECT name,email,organizationId, phone,isManager,bestPayAccount,verify FROM "
                        + getTableName() + " WHERE email=?");
        try {
            st.setString(1, email);
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
    
    public boolean selectById(Connection conn) throws SQLException {
        PreparedStatement st = conn
                .prepareStatement("SELECT name,email,organizationId, phone,isManager,bestPayAccount,verify FROM "
                        + getTableName() + " WHERE id=?");
        try {
            st.setString(1, id);
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
    
    public boolean selectByVerify(Connection conn) throws SQLException {
        PreparedStatement st = conn
                .prepareStatement("SELECT id,name,email,organizationId, phone,isManager,bestPayAccount,verify FROM "
                        + getTableName() + " WHERE verify=?");
        try {
            st.setString(1, verify);
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
    
    public void selectByOrganizationId(Connection conn, boolean isManager)
            throws SQLException {
        PreparedStatement st;
        if (isManager)
            st = conn
                    .prepareStatement("SELECT id,name,email,organizationId, phone,isManager,bestPayAccount,verify FROM "
                            + getTableName()
                            + " WHERE organizationId=? and isManager=1 and verify is null");
        else
            st = conn
                    .prepareStatement("SELECT id,name,email,organizationId, phone,isManager,bestPayAccount,verify FROM "
                            + getTableName()
                            + " WHERE organizationId=? and verify is null");
        try {
            st.setString(1, organizationId);
            ResultSet rs = st.executeQuery();
            try {
                while (rs.next()) {
                    DbSalesman sm = new DbSalesman();
                    sm.id = rs.getString("id");
                    sm.email = rs.getString("email");
                    sm.bestPayAccount = rs.getString("bestPayAccount");
                    sm.isManager = rs.getInt("isManager");
                    sm.organizationId = rs.getString("organizationId");
                    sm.phone = rs.getString("phone");
                    sms.add(sm);
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
                .prepareStatement(
                        "INSERT "
                                + getTableName()
                                + "( id,name, email,organizationId, phone,isManager,password,bestPayAccount,verify) "
                                + "VALUES ( ?, ?, ?,?, ?, ?,?,?,?)",
                        PreparedStatement.RETURN_GENERATED_KEYS);
        try {
            st.setString(1, id);
            st.setString(2, name);
            st.setString(3, email);
            st.setString(4, organizationId);
            st.setString(5, phone);
            st.setInt(6, isManager);
            st.setString(7, password);
            st.setString(8, bestPayAccount);
            st.setString(9, verify);
            st.executeUpdate();
            ResultSet rs = null;
            try {
                rs = st.getGeneratedKeys();
                rs.next();
            } finally {
                if (rs != null) {
                    rs.close();
                }
            }
        } finally {
            st.close();
        }
    }
    
    @Override
    public void update(Connection conn) throws SQLException {
        PreparedStatement updateState = conn
                .prepareStatement("UPDATE "
                        + getTableName()
                        + " SET "
                        + "name=?, "
                        + "email=?,organizationId=?,phone=?,password=?,isManager=? ,bestPayAccount=?,verify=?"
                        + " WHERE email=? ");
        try {
            updateState.setString(1, name);
            updateState.setString(2, email);
            updateState.setString(3, organizationId);
            updateState.setString(4, phone);
            updateState.setString(5, password);
            updateState.setInt(6, isManager);
            updateState.setString(7, bestPayAccount);
            updateState.setString(8, verify);
            updateState.setString(9, email);
            updateState.executeUpdate();
        } finally {
            updateState.close();
        }
    }
    
    @Override
    public void delete(Connection conn) throws SQLException {
        PreparedStatement st = conn.prepareStatement("DELETE FROM "
                + getTableName() + " WHERE email=?");
        try {
            st.setString(1, email);
            st.executeUpdate();
        } finally {
            st.close();
        }
    }
    
    public static String getTableName() {
        return "salesman";
    }
    
    public static void drop(Connection conn) throws SQLException {
        Statement st = conn.createStatement();
        try {
            st.execute("DROP TABLE IF EXISTS " + getTableName());
        } finally {
            st.close();
        }
    }
}