package cn.ctyun.oos.server.db.dbprice;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import cn.ctyun.common.db.JdbcRow;

/**
 * @author: Cui Meng
 */
public class DbOrganization implements JdbcRow {
    /** 组织Id */
    public String organizationId;
    /** 组织名称 */
    public String name;
    public ArrayList<DbOrganization> orgs = new ArrayList<DbOrganization>();
    
    public DbOrganization(String organizationId) {
        this.organizationId = organizationId;
    }
    
    public DbOrganization() {
    }
    
    public static void create(Connection conn) throws SQLException {
        String create = "CREATE TABLE `"
                + getTableName()
                + "` (\n"
                + "     `organizationId` varchar(10) NOT NULL DEFAULT '' COMMENT '组织id',"
                + "    `name` varchar(50) DEFAULT NULL COMMENT '组织名称',"
                + "     PRIMARY KEY (`organizationId`)"
                + "   ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='组织表'";
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
            if (aname.equals("organizationId")) {
                organizationId = resultSet.getString(i);
            } else if (aname.equals("name")) {
                name = resultSet.getString(i);
            } else {
                if (!ignore)
                    throw new RuntimeException("Unknown column name : " + aname);
            }
        }
    }
    
    @Override
    public boolean select(Connection conn) throws SQLException {
        PreparedStatement st = conn.prepareStatement("SELECT *  FROM "
                + getTableName() + " WHERE organizationId=?");
        try {
            st.setString(1, organizationId);
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
    
    public void selectAll(Connection conn) throws SQLException {
        PreparedStatement st = conn.prepareStatement("SELECT *  FROM "
                + getTableName());
        try {
            ResultSet rs = st.executeQuery();
            try {
                while (rs.next()) {
                    DbOrganization o = new DbOrganization(
                            rs.getString("organizationId"));
                    o.name = rs.getString("name");
                    orgs.add(o);
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
        PreparedStatement st = conn.prepareStatement("INSERT " + getTableName()
                + "( organizationId,name) " + "VALUES ( ?, ?)",
                PreparedStatement.RETURN_GENERATED_KEYS);
        try {
            st.setString(1, organizationId);
            st.setString(2, name);
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
        PreparedStatement updateState = conn.prepareStatement("UPDATE "
                + getTableName() + " SET " + "name=? "
                + "WHERE organizationId=? ");
        try {
            updateState.setString(1, name);
            updateState.setString(2, organizationId);
            updateState.executeUpdate();
        } finally {
            updateState.close();
        }
    }
    
    @Override
    public void delete(Connection conn) throws SQLException {
        PreparedStatement st = conn.prepareStatement("DELETE FROM "
                + getTableName() + " WHERE organizationId=?");
        try {
            st.setString(1, organizationId);
            st.executeUpdate();
        } finally {
            st.close();
        }
    }
    
    public static String getTableName() {
        return "organization";
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