package cn.ctyun.oos.server.db;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.commons.io.IOUtils;

import cn.ctyun.common.db.DbSource;
import common.io.StreamUtils;
import common.tuple.Pair;

/**
 * @author: Cui Meng
 */
public class DBSelect {
    protected static DBSelect THIS = new DBSelect();
    
    protected DBSelect() {
    }
    
    public static DBSelect getInstance() {
        return THIS;
    }
    
    public static void main(String[] args) throws Exception {
        // commonSelect("select * from owner");
    }
    
    public Pair<InputStream, Long> commonSelect(String sql)
            throws SQLException, IOException {
        Connection conn = null;
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        long size = 0;
        try {
            conn = DbSource.getConnection();
            PreparedStatement st = conn.prepareStatement(sql);
            try {
                ResultSet rs = st.executeQuery();
                int colNum = rs.getMetaData().getColumnCount();
                StringBuilder sb = new StringBuilder();
                Pair<InputStream, Long> p = new Pair<InputStream, Long>();
                try {
                    for (int i = 1; i <= colNum; i++) {
                        sb.append(rs.getMetaData().getColumnName(i));
                        if (i != colNum)
                            sb.append(",");
                    }
                    sb.append("\n");
                    size += sb.length();
                    StreamUtils.copy(IOUtils.toInputStream(sb.toString()), out,
                            sb.length());
                    while (rs.next()) {
                        sb = new StringBuilder();
                        for (int i = 1; i <= colNum; i++) {
                            sb.append(rs.getObject(i));
                            if (i != colNum)
                                sb.append(",");
                        }
                        sb.append("\n");
                        size += sb.length();
                        StreamUtils.copy(IOUtils.toInputStream(sb.toString()),
                                out, sb.length());
                    }
                    p.first(new ByteArrayInputStream(out.toByteArray()));
                    p.second(size);
                    return p;
                } finally {
                    rs.close();
                    if (out != null)
                        out.close();
                }
            } finally {
                st.close();
            }
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }
}
