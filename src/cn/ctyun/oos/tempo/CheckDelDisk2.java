package cn.ctyun.oos.tempo;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;

import cn.ctyun.common.ReplicaMode;
import cn.ctyun.common.conf.RegionHHZConfig;
import cn.ctyun.oos.ostor.Route;
import cn.ctyun.oos.ostor.Route.RouteTable;
import cn.ctyun.oos.ostor.common.HttpParams;
import cn.ctyun.oos.ostor.http.HttpHead;
import cn.ctyun.oos.ostor.utils.IOUtils;

public class CheckDelDisk2 {
    public static Log log = LogFactory.getLog(CheckDelDisk2.class);
    private static FileSystem fs;
    static{
        try {
            fs = DistributedFileSystem.get(RegionHHZConfig.getConfig());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    File parseFile;
    long pdiskOld=670;
    String tablePath = "table.172";
    private static RouteTable routeTable = new RouteTable();
    
    public CheckDelDisk2(String file) throws Exception{
        parseFile = new File(file);
        if(!parseFile.exists()){
            throw new RuntimeException();
        }
        Map<Integer, List<Long>> vp = getOldVpMap();
        BufferedReader br = new BufferedReader(new FileReader(parseFile));
        try{
            String line = null;
            while((line = br.readLine()) != null){
                String[] parts = line.split(" ");
                Integer vdisk = Integer.valueOf(parts[0]);
                long pdisk0 = Route.getOldPDisk(vp.get(vdisk).get(0).longValue());
                if(pdisk0 != pdiskOld){
                    if(!pageExist(vdisk.intValue(), pdisk0, ReplicaMode.parser(parts[2]), parts[3])){
                        log.info("xx check ok");
                    }else{
                        log.error("find bad");
                    }
                }else{
                    if(parts[2].equals("EC_1_0_1")){
                        log.info("xx this is lost EC_1_0_1");
                    }else{
                        long pdisk1 = Route.getOldPDisk(vp.get(vdisk).get(1).longValue());
                        if(!pageExist(vdisk.intValue(), pdisk1, ReplicaMode.parser(parts[2]), parts[3])){
                            log.info("xx check ok");
                        }else{
                            log.error("find bad");
                        }
                    }
                }
            }
        }finally{
            br.close();
        }
    }
    
    public boolean pageExist(int vdisk, long pdiskId, ReplicaMode mode, 
            String pageKey) throws Exception{
        
        boolean exist = false;
        int respCode = 0;
        String host = null;
        StringBuilder reqUrl = new StringBuilder(30);
        try {
            host = routeTable.getPDTable().get(pdiskId);
            if(null == host){
                throw new RuntimeException("Host not exist of pdisk:" + pdiskId);
            }
            reqUrl.append("/?")
                    .append(HttpParams.PARAM_PDISK).append("=").append(pdiskId)
                    .append("&").append(HttpParams.PARAM_VDISK).append("=")
                    .append(vdisk).append("&")
                    .append(HttpParams.PARAM_REPLICATE).append("=")
                    .append(mode.toString()).append("&")
                    .append(HttpParams.PARAM_PAGEKEY).append("=")
                    .append(pageKey);

            respCode = new HttpHead(toInetSocketAddress(host), reqUrl.toString()).call();
            if(respCode == HttpParams.GET_CODE){
                exist = true;
            } else if(respCode != HttpParams.NOT_FOUNT_CODE){
                throw new RuntimeException("Check replication exist faild. code:" + respCode);
            }
        } catch (Exception e) {
            log.error("Check Replication exist faild: pageKey=" + pageKey + " pdiskId=" + pdiskId + " host= " + host + " vdisk=" + vdisk);
            throw e;
        }
        return exist;
    }
    
    private InetSocketAddress toInetSocketAddress(String hp){
        int pos = hp.indexOf(":");
        String host = hp;
        int port = 80;
        if(pos != -1 && pos != hp.length() - 1){
            host = hp.substring(0, pos);
            port = Integer.parseInt(hp.substring(pos + 1));
        }
        return new InetSocketAddress(host, port);
    }
    
    private Map<Integer, List<Long>> getOldVpMap(){
        String routePath = "/ostor/id/route/"+tablePath;
        try {
            InputStream is = IOUtils.loadFromHdfs(fs, routePath);
            try {
                Map<?, ?>[] maps = IOUtils.toMaps(is);
                Map<Integer, List<Long>> vpMap = (Map<Integer, List<Long>>)maps[1];
                return vpMap;
            } finally{
                is.close();
            }
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }
    
    public static void main(String[] args){
        try {
            new CheckDelDisk2(args[0]);
        } catch (Throwable e) {
            log.error(e.getMessage(), e);
        }
    }

}
