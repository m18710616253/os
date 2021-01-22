package cn.ctyun.oos.tempo;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.URLEncoder;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import cn.ctyun.common.ReplicaMode;
import cn.ctyun.oos.ostor.Route.RouteTable;
import cn.ctyun.oos.ostor.common.HttpParams;
import cn.ctyun.oos.ostor.common.OstorConsts;
import cn.ctyun.oos.ostor.http.HttpPut;

/**
 * 在旧盘没有损坏的情况下下掉，但是有的数据在旧盘中有而参考盘中没有，所以通过参考盘来下盘，而导致有些数据旧盘有而新盘没有
 * 这支程序的目的是：把旧盘中有而新盘中没有的数据，再拷贝到新盘去
 * @author jiazg
 *
 */
public class RecoverFromOldDisk {
    public static Log log = LogFactory.getLog(RecoverFromOldDisk.class);
    AtomicLong total = new AtomicLong(0L);
    AtomicLong failed = new AtomicLong(0L);
    private static RouteTable routeTable = new RouteTable();
    public RecoverFromOldDisk() {
        Thread t = new Thread() {
            @Override
            public void run() {
                for (;;) {
                    log.info("Total = " + total.get());
                    log.info("failed = " + failed.get());
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                    }
                }
            }
        };
        t.setDaemon(true);
        t.start();
    }
    
    
    /**
     * @param basePath 旧盘的基础路径，例如/mnt/ostor/disk8/data
     * @param logPath  待恢复的pageKey的日志路径
     * @throws FileNotFoundException
     */
    public void recover(String basePath, String logPath) throws FileNotFoundException{
        
        File baseFile = new File(basePath);
        File logFile = new File(logPath);
        if(!logFile.isFile() || !baseFile.isDirectory()){
            throw new FileNotFoundException("File not exist. file:" + logPath);
        }
            
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(logFile));
            String line = null;
            while(null != (line = reader.readLine())){
                int tryCount = 10;
                InputStream in = null;
                boolean success = false;
                while(tryCount-- > 0){
                    try {
                        String[] items = line.split(",");
                        int vdisk = Integer.parseInt(items[0]);
                        long pdisk = Long.parseLong(items[1]);
                        ReplicaMode mode = ReplicaMode.parser(items[2]);
                        String pageKey = items[3];
                        File srcFile = new File(basePath, 
                                vdisk + "/" + mode.toString() + "/" + pageKey.substring(0, 3) + "/" + pageKey);
                        if(!srcFile.isFile()){
                            log.error("File not exist. file:" + srcFile.getAbsolutePath());
                            continue;
                        }
                        in = new FileInputStream(srcFile);
//                        MetaInfo meta = PDiskIO.getAllMetaNew(srcFile);
//                        putReplics(pdisk, vdisk, mode, meta.ostorKey, pageKey, meta.userMeta, 
//                                meta.totalPageNum, meta.pageNum, ((Long)srcFile.length()).intValue(), 
//                                IOUtils.toByteArray(in));
                        success = true;
                        break;
                    } catch (Exception e) {
                        log.error(e.getMessage(), e);
                    } finally{
                        if(null != in)
                            try {
                                in.close();
                            } catch (IOException e) {}
                    }
                }
                total.incrementAndGet();
                if(!success){
                    log.error("FAILED_PAGE:" + line);
                    failed.incrementAndGet();
                }
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        } finally{
            if(null != reader)
                try {
                    reader.close();
                } catch (IOException e) {}
        }
        
    }
    public void putReplics(long pdiskId, int vdisk,
            ReplicaMode mode, String oStorKey, String pageKey,
            String userMeta, long totalPage, long currentPage,
            int contentLength, byte[] data) throws Exception {
        StringBuilder reqUrl = new StringBuilder(30);
        String host = null;
        try {
            host = routeTable.getPDTable().get(pdiskId);
            if(null == host){
                throw new RuntimeException("Host not exist of pdisk:" + pdiskId);
            }
            reqUrl.append("/?").append(HttpParams.PARAM_PDISK).append("=").append(pdiskId)
                    .append("&").append(HttpParams.PARAM_VDISK).append("=")
                    .append(vdisk).append("&")
                    .append(HttpParams.PARAM_REPLICATE).append("=")
                    .append(mode.toString()).append("&")
                    .append(HttpParams.PARAM_REALKEY).append("=")
                    .append(oStorKey).append("&")
                    .append(HttpParams.PARAM_PAGEKEY).append("=")
                    .append(pageKey).append("&")
                    .append(HttpParams.PARAM_TOTALPAGE).append("=")
                    .append(totalPage).append("&")
                    .append(HttpParams.PARAM_CURRPAGE).append("=")
                    .append(currentPage);

            if (null != userMeta && !userMeta.equals("")) {
                reqUrl.append("&")
                        .append(HttpParams.PARAM_USERMETA)
                        .append("=")
                        .append(URLEncoder.encode(userMeta,
                                OstorConsts.STR_UTF8));
            }
            
            new HttpPut(toInetSocketAddress(host), reqUrl.toString(), data, contentLength).call();

        } catch (Exception e) {
            log.error("Put page failed: " + "pageKey=" + pageKey + " pdiskId=" + pdiskId + " host= " + host + " vdisk=" + vdisk, e);
            throw e;
        }
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
    
    /**
     * @param args
     * @throws IOException 
     */
    public static void main(String[] args) throws IOException {
        if(args.length != 2){
            log.info("Usage: RecoverFromOldDisk oldDiskBasePath logFilePath");
            System.exit(1);
        }
        try{
            new RecoverFromOldDisk().recover(args[0], args[1]);
            System.exit(0);
        }catch(Throwable e){
            log.error(e.getMessage(), e);
        }
    }
}
