package cn.ctyun.oos.server.stress;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import cn.ctyun.common.Program;
import cn.ctyun.common.conf.GlobalHHZConfig;
import cn.ctyun.oos.hbase.HBaseConnectionManager;

public class HBaseMonitorAlive implements Program{
    private static HConnection conn;
    private static Configuration conf;
    static {
        conf = GlobalHHZConfig.getConfig();
        conf = HBaseConfiguration.create(conf);
        try {
            conn = HBaseConnectionManager.createConnection(conf);
        } catch (IOException e) {
            throw new RuntimeException("Create connection to hbase failed.", e);
        }
    }
    private static byte[] tableName = Bytes.toBytes("TableForTest");
    private static byte[] fyName = Bytes.toBytes("fy");
    private static byte[] clName = Bytes.toBytes("v");

    public String usage() {
        return null;
    }

    public void exec(String[] args) throws Exception {
        for(int i = 0; i < 100; i++)
            does();
        dropTable();
        System.out.println("HBase is normal!");
    }
    
    public static void main(String[] args) throws Exception{
        HBaseMonitorAlive hma = new HBaseMonitorAlive();
        for(int i = 0; i < 100; i++)
            hma.does();
        hma.dropTable();
        System.out.println("HBase is normal!");
    }
    
    static Random rand = new Random();
    
    public void does() throws IOException{
        createTable();
        byte[] row = new byte[16];
        rand.nextBytes(row);
        byte[] value = new byte[100];
        rand.nextBytes(value);
        put(row, value);
        byte[] v = get(row);
        assert(Bytes.compareTo(value, v) == 0);
    }
    
    public void createTable() throws IOException{
        HBaseAdmin hadmin = new HBaseAdmin(conf);
        try {
            if(hadmin.tableExists(tableName))
                return;
            HTableDescriptor hdesc = new HTableDescriptor(TableName.valueOf(tableName));
            HColumnDescriptor hfamily = new HColumnDescriptor("fy");
            hdesc.addFamily(hfamily);
            hadmin.createTable(hdesc);
        } finally {
            hadmin.close();
        }
    }
    
    public void put(byte[] row, byte[] value) throws IOException{
        HTableInterface htable = conn.getTable(tableName);
        try {
            Put put = new Put(row);
            put.add(fyName, clName, value);
            htable.put(put);
        } finally {
            htable.close();
        }
    }
    
    public byte[] get(byte[] row) throws IOException{
        HTableInterface htable = conn.getTable(tableName);
        try {
            Get get = new Get(row);
            Result rs = htable.get(get);
            return rs.getValue(fyName, clName);
        } finally {
            htable.close();
        }
    }
    
    public void dropTable() throws IOException{
        HBaseAdmin hadmin = new HBaseAdmin(conf);
        try {
            hadmin.disableTable(tableName);
            hadmin.deleteTable(tableName);
        } finally {
            hadmin.close();
        }
    }
}
