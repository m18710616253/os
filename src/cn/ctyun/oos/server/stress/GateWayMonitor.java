package cn.ctyun.oos.server.stress;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import cn.ctyun.common.Program;
import cn.ctyun.common.ganglia.GangliaPlugin;
import cn.ctyun.oos.ostor.utils.RemoteCmdUtil;

import common.tuple.Pair;
import common.util.BlockingExecutor;

public class GateWayMonitor implements Program {
    
    private static final Log log = LogFactory.getLog(GateWayMonitor.class);
    
    private static Map<String, List<String>> hostMonutMap = new HashMap<>();;
    private static String user;
    private static String password;
    private static String owner;
    private static int port;
    private static int listNum;
    private static long lsInterval;
    private static long runInterval;
    private static long recordInterval;
    private static Map<String, RemoteCmdUtil> connections = new HashMap<>();
    private static List<Pair<Long, Map<String, Stat>>> statList = new ArrayList<>();
    private static BlockingExecutor executePool = new BlockingExecutor(100,
            200, 400, 30000, "execute pool");
    static {
        InputStream is = null;
        try {
            is = new FileInputStream(new File(System.getProperty("user.dir") + "/conf/gatewayMonitor.txt"));
            String result = IOUtils.toString(is);
            JSONObject obj = new JSONObject(result);
            user = obj.getString("user");
            password = obj.getString("password");
            port = obj.getInt("port");
            owner = obj.getString("owner");
            listNum = obj.getInt("listNum");
            runInterval = obj.getLong("runInterval");
            lsInterval = obj.getLong("lsInterval");
            recordInterval = obj.getLong("recordInterval");
            JSONArray arr = obj.getJSONArray("hosts");
            for(int i = 0; i < arr.length(); i++){
                JSONObject o = arr.getJSONObject(i);
                String mountPoints = o.getString("mountPoints");
                hostMonutMap.put(o.getString("host"), Arrays.asList(mountPoints.split(",")));
            }
            for(Map.Entry<String, List<String>> e : hostMonutMap.entrySet()){
                String host = e.getKey();
                RemoteCmdUtil conn = new RemoteCmdUtil(user, password, host, port);
                connections.put(host, conn);
                for(String point : e.getValue()){
                    String baseListPath = point + "/listTest";
                    try {
                        conn.mkdir(point + "/putGetDelTest", true);
                        conn.mkdir(point + "/mkdirTest", true);
                        conn.mkdir(baseListPath, true);
                        long count = conn.lsCount(baseListPath);
                        if(count >= listNum)
                            continue;
                        for(int i = 0; i < listNum - count; i++){
                            conn.createFile(baseListPath + "/" + System.currentTimeMillis() + i,  1);
                        }
                    } catch (Exception e1) {
                        log.error(e1.getMessage(), e1);
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally{
            if(null != is)
                try {
                    is.close();
                } catch (IOException e) {}
        }
    }
    
    public static class PutGetDelTask extends TimerTask{
        
        @Override
        public void run() {
            final AtomicLong count = new AtomicLong(0);
            for(Map.Entry<String, List<String>> e : hostMonutMap.entrySet()){
                final String host = e.getKey();
                for(final String point : e.getValue()){
                    executePool.submit(new Runnable() {
                        @Override
                        public void run() {
                            count.incrementAndGet();
                            try{
                                String basePath = point + "/putGetDelTest";
                                RemoteCmdUtil conn = connections.get(host);
                                String file = basePath + "/" + System.currentTimeMillis() 
                                        + RandomStringUtils.randomAlphabetic(5);
                                long fileSize = 2; //单位MB
                                
                                String putKey = buildKey(host, point, "putFile");
                                String getKey = buildKey(host, point, "getFile");
                                String delKey = buildKey(host, point, "delFile");
                                
                                if(!conn.createFile(file, fileSize)){
                                    recordStat(putKey, false);
                                    return;
                                }else{
                                    recordStat(putKey, true);
                                }
                                if(conn.readFile(file, fileSize)){
                                    recordStat(getKey, true);
                                }else{
                                    recordStat(getKey, false);
                                }
                                if(conn.remove(file, false)){
                                    recordStat(delKey, true);
                                }else{
                                    recordStat(delKey, false);
                                }
                            } finally{
                                count.decrementAndGet();
                            }
                        }
                    }); 
                }
            }
            while(count.get() > 0){
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e1) {}
            }
        }
    }
    
    public static class ListTask extends TimerTask{
        
        @Override
        public void run() {
            for(Map.Entry<String, List<String>> e : hostMonutMap.entrySet()){
                String host = e.getKey();
                for(final String point : e.getValue()){
                    String basePath = point + "/listTest";
                    RemoteCmdUtil conn = connections.get(host);
                    String listKey = buildKey(host, point, "listFile");
                    try{
                        conn.lsCount(basePath);
                        recordStat(listKey, true);
                    } catch(Exception e1){
                        recordStat(listKey, false);
                        log.error(e1.getMessage(), e1);
                    }
                }
            }
        }
    }
    
    public static class MkdirTask extends TimerTask{
        
        @Override
        public void run() {
            final AtomicLong count = new AtomicLong(0);
            for(Map.Entry<String, List<String>> e : hostMonutMap.entrySet()){
                final String host = e.getKey();
                for(final String point : e.getValue()){
                    executePool.submit(new Runnable() {
                        @Override
                        public void run() {
                            count.incrementAndGet();
                            try{
                                String basePath = point + "/mkdirTest";
                                RemoteCmdUtil conn = connections.get(host);
                                String fileName = System.currentTimeMillis() + RandomStringUtils.randomAlphabetic(5);
                                String file = basePath + "/" + fileName;
    
                                String mkdirKey = buildKey(host, point, "mkdir");
                                String listDir = buildKey(host, point, "listDir");
                                String chownKey = buildKey(host, point, "chown");
                                String chmodKey = buildKey(host, point, "chmod");
                                String delDirKey = buildKey(host, point, "delDir");
                                
                                // 执行mkdir
                                if(!conn.mkdir(file, false)){
                                    recordStat(mkdirKey, false);
                                    return;
                                }else{
                                    recordStat(mkdirKey, true);
                                }
                                
                                // 执行ls
                                if(!conn.ls(basePath)){
                                    recordStat(listDir, false);
                                }else{
                                    recordStat(listDir, true);
                                }
                                // 执行chown
                                conn.chown(file, owner, false);
                                String ow = null;
                                try {
                                    ow = conn.getOwner(file);
                                } catch (Exception e1) {
                                    log.error(e1.getMessage(), e1);
                                }
                                if(null != ow && ow.equals(owner + "," + owner)){
                                    recordStat(chownKey, true);
                                }else{
                                    recordStat(chownKey, false);
                                }
                                // 执行chmod
                                String limit = "777";
                                conn.chmod(file, limit, false);
                                String lm = null;
                                try {
                                    lm = conn.getLimits(file);
                                } catch (Exception e1) {
                                    log.error(e1.getMessage(), e1);
                                }
                                if(null != lm && lm.contains("drwxrwxrwx")){
                                    recordStat(chmodKey, true);
                                }else{
                                    recordStat(chmodKey, false);
                                }
                                if(conn.remove(file, true)){
                                    recordStat(delDirKey, true);
                                }else{
                                    recordStat(delDirKey, false);
                                }
                            } finally{
                                count.decrementAndGet();
                            }
                        }
                    });
                }
            }
            while(count.get() > 0){
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e1) {}
            }
        }
    }
    
    public static class Stat{
        
        public String key;
        public AtomicLong total = new AtomicLong(0);
        public AtomicLong success = new AtomicLong(0);
        public AtomicLong failed = new AtomicLong(0);
        
        public Stat(String key){
            this.key = key;
        }
        
        @Override
        public String toString() {
            JSONObject obj = new JSONObject();
            try {
                obj.put("key", key);
                obj.put("total", total);
                obj.put("success", success);
                obj.put("failed", failed);
                String percentage = "0";
                if(total.get() != 0)
                    percentage = String.format("%.2f", (double)failed.get() / total.get() * 100);
                obj.put("percentage", percentage);
            } catch (JSONException e) {
                log.info(e.getMessage(), e);
            }
            return obj.toString();
        }
    }
    
    public static String buildKey(String host, String mountPoint, String type){
        
        int index = mountPoint.lastIndexOf("/");
        if(index > 0){
            mountPoint = mountPoint.substring(index + 1, mountPoint.length());
        }
        return type + "-" + host + "-" + mountPoint;
    }
    
    private static synchronized void recordStat(String key, boolean success){
        
        Map<String, Stat> statMap = new HashMap<>();
        long interval = System.currentTimeMillis() / recordInterval;
        int size = statList.size();
        if(size == 0){
            statList.add(new Pair<>(interval, statMap));
        }else if(statList.get(size - 1).first() != interval){
            Map<String, Stat> ns = statList.get(size - 1).second();
            log.info("Start to sending information to ganglia...");
            for(Stat s : ns.values()){
                String percentage = "0";
                if(s.total.get() != 0)
                    percentage = String.format(" %.2f", (double)s.failed.get() / s.total.get() * 100);
                GangliaPlugin.sendToGanglia(s.key, percentage, 
                        "double", "%", "gateway metrics", s.toString());
                log.info(s.toString());
            }
            log.info("Send information to ganglia finish...");
            statList.remove(size - 1);
            statList.add(new Pair<>(interval, statMap));
        }else{
            statMap = statList.get(size - 1).second();
        }
        
        Stat stat = statMap.get(key);
        if(null == stat){
            stat = new Stat(key);
            statMap.put(key, stat);
        }
        stat.total.incrementAndGet();
        if(success){
            stat.success.incrementAndGet();
        } else{
            stat.failed.incrementAndGet();
        }
    }
    
    @Override
    public void exec(String[] args){
        
        Timer putGetDelTimer = new Timer();
        putGetDelTimer.schedule(new PutGetDelTask(), 0, runInterval);
        Timer listTimer = new Timer();
        listTimer.schedule(new ListTask(), 0, lsInterval);
        Timer mkdirTimer = new Timer();
        mkdirTimer.schedule(new MkdirTask(), 0, runInterval);
    }
    
    @Override
    public String usage() {
        return null;
    }
    
    public static void main(String[] args) {
        System.out.println(buildKey("fjoos01", "/mnt/vss1", "put"));
    }
}
