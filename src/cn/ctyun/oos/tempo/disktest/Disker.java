package cn.ctyun.oos.tempo.disktest;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.UserDefinedFileAttributeView;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.Properties;
import java.util.TimeZone;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.KeeperException;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.server.nio.SelectChannelConnector;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import cn.ctyun.common.Program;
import cn.ctyun.oos.ostor.common.HttpParams;
import cn.ctyun.oos.ostor.common.OstorConsts;
import cn.ctyun.oos.ostor.utils.LogOneLine;

import com.amazonaws.services.s3.Headers;
import com.amazonaws.services.s3.internal.ServiceUtils;
import common.time.TimeStat;
import common.time.TimeUtils;
import common.util.BlockingExecutor;

public class Disker extends AbstractHandler implements Program{
    static{
        System.setProperty("log4j.log.app", "disker");
    }
    private static Log log = LogFactory.getLog(Disker.class);
//    private String hostAdd = DiskUtils.getLocalHostNamePort();
//    private static MessageQueue mq = MessageQueue.getInstance();
//    private DiskerNode diskerNode;
//    private PdiskDirMap pdisk2Dir = PdiskDirMap.getInstance();
//    private RouteTable routeTable = RouteTable.getInstance();
    final static int size = 128*1024;
    public static ThreadLocal<byte[]> localBytes = new ThreadLocal<byte[]>() {
        protected byte[] initialValue() {
            return new byte[size];
        };
    };
    TimeStat stat = new TimeStat();
    public String pdiskDir;
    
    public Disker() throws KeeperException, InterruptedException{
//        routeTable.loadAndListen();
        
//        diskerNode = new DiskerNode();
//        diskerNode.setDaemon(true);
//        diskerNode.start();
        log.info("end construtor");
    }
    
    public static void main(String[] args) throws DiskException, KeeperException, InterruptedException {
        if(args.length != 1){
            throw new IllegalArgumentException("usage: program <pdiskdir>");
        }
        Disker d = new Disker();
        d.pdiskDir = args[0];
        d.startServer();
    }
    
    public void startServer(){
        new Thread(){
            @Override
            public void run(){
                while(true){
                    String used = getPartitionUsedSpace(pdiskDir);//k
                    double usedReadable = Long.parseLong(used)/1024.0/1024.0;//G
                    String logout = String.format("now MountPoint %s used space:%.2f GB. In last half hour, Stat:%s", pdiskDir, usedReadable, stat.toString());
                    log.info(logout);
                    stat = new TimeStat();
                    try { Thread.sleep(1800000); } catch (InterruptedException e) {}
                }
            }
        }.start();
//        try {
//            Utils.writePid(DiskerConfig.getDiskerPidPath());
//        } catch (IOException e1) {
//            throw new RuntimeException("Failed to get Disker pid");
//        }

        SelectChannelConnector conn = new SelectChannelConnector();
        QueuedThreadPool threadPool = new QueuedThreadPool();
//        threadPool.setMinThreads(DiskerConfig.minThreads); threadPool.setMaxThreads(DiskerConfig.maxThreads); threadPool.setMaxQueued(DiskerConfig.threadPoolQueue);
//        conn.setThreadPool(threadPool);
//        conn.setPort(DiskerConfig.port);
//        conn.setMaxIdleTime(DiskerConfig.maxIdleTime);
        threadPool.setMinThreads(1000);
        threadPool.setMaxThreads(5000);
        threadPool.setMaxQueued(30000);
        conn.setThreadPool(threadPool);
        conn.setPort(9000);
        conn.setMaxIdleTime(30000);
        conn.setStatsOn(true);
        
        Server server = new Server();
        server.setConnectors(new Connector[] { conn });
        server.setHandler(this);
//        server.setThreadPool(new QueuedThreadPool(DiskerConfig.threadPoolQueue));
        server.setThreadPool(new QueuedThreadPool(30000));
        try {
            server.start();
            log.info("start server");
            server.join();
        } catch (Exception e) {
            log.error(e);
        }
    }
    
    @Override
    public void handle(String target, Request baseRequest,
            HttpServletRequest request, HttpServletResponse response){
        long totalS = System.currentTimeMillis();
        AccessLog accessLog = new AccessLog();
        String method = request.getMethod();  
        accessLog.method = method;
        try{
            long start1 = System.currentTimeMillis();
//            LogUtils.log(DiskUtils.log(request));
            response.setHeader(Headers.DATE, ServiceUtils.formatRfc822Date(new Date()));
            
            //TODO fortest
            String pdisk = request.getParameter(HttpParams.PARAM_PDISK);
//            String pdiskDir = pdisk != null ? pdisk2Dir.getDirOfPdisk(pdisk) : null;
            String vdisk = request.getParameter(HttpParams.PARAM_VDISK);
            String replicate = request.getParameter(HttpParams.PARAM_REPLICATE);
            String pageKey = request.getParameter(HttpParams.PARAM_PAGEKEY);
            
            
//            String requestHost = routeTable.getPDTable().get(Long.valueOf(pdisk));//测试阶段
//            if(requestHost == null){
//                log.error("host of " + pdisk + " is null. Maybe Error of PdiskMap. pdiskId->null");
//                response.setStatus(HttpParams.PDISK_DIR_NOEXIST);
//                return;
//            }
//            if(! requestHost.equalsIgnoreCase(hostAdd)){//测试阶段
//                log.error(pdisk + " doesnot exist on localhost. Maybe Error of PdiskMap. pdiskId->host(not this host)");
//                response.setStatus(HttpParams.PDISK_DIR_NOEXIST);
//                return;
//            }
            
            if(method.equalsIgnoreCase(HttpParams.GET) && request.getParameter(HttpParams.PARAM_LIST_NUM) != null){//list directory
                handleListDir(request, response, pdiskDir);
                return;
            }
            
            File keyDataLocation = PDiskIO.getKeyDataLocation(pdiskDir, vdisk, replicate, pageKey);
            accessLog.statsTime += "[preCheckTotal]"+(System.currentTimeMillis() - start1) + ":";
            if(method.equalsIgnoreCase(HttpParams.PUT)){
                long start = System.currentTimeMillis();
                handlePut(request, response, keyDataLocation, accessLog);
                stat.record("put", (System.currentTimeMillis() - start));
                accessLog.statsTime += ":[putHandleTotal]" + (System.currentTimeMillis() - start);
                return;
            }else if(method.equalsIgnoreCase(HttpParams.GET)){
                long start = System.currentTimeMillis();
                handleGet(request, response, keyDataLocation, accessLog);
                stat.record("get", (System.currentTimeMillis() - start));
                accessLog.statsTime += ":[getHandleTotal]" + (System.currentTimeMillis() - start);
                return;
            }else if(method.equalsIgnoreCase(HttpParams.DELETE)){
                long start = System.currentTimeMillis();
                handleDelete(request, response, keyDataLocation, pdiskDir, accessLog);
                stat.record("delete", (System.currentTimeMillis() - start));
                accessLog.statsTime += ":[deleteHandleTotal]" + (System.currentTimeMillis() - start);
                return;
            }else if(method.equalsIgnoreCase(HttpParams.HEAD)){
                handleHead(request, response, keyDataLocation);
            }else if(method.equalsIgnoreCase(HttpParams.POST)){
                handleCopy(request, response, keyDataLocation);
                return;
            }
        }catch(IllegalArgumentException e){
            log.error(e.getMessage(), e);
            response.setStatus(400);
        }catch(DiskException e){
            log.error(e.getMessage(), e);
            response.setStatus(e.status);
        }catch(Throwable e){
            log.error(e.getMessage(), e);
            response.setStatus(500);
        }finally{
            baseRequest.setHandled(true);
            accessLog.respCode = response.getStatus();
            accessLog.statsTime = accessLog.statsTime + ":[total]" + (System.currentTimeMillis() - totalS);
            if((method.equalsIgnoreCase(HttpParams.PUT) || method.equalsIgnoreCase(HttpParams.DELETE)
                    || (method.equalsIgnoreCase(HttpParams.GET) && request.getParameter(HttpParams.PARAM_LIST) == null
                            && request.getParameter(HttpParams.PARAM_GET_META) == null)) 
                    && accessLog.respCode != 400)
                AccessLogWriter.writeLog(accessLog);
        }
    }
    
    //将来可以去掉
    
    private void handlePut(HttpServletRequest request, HttpServletResponse response, File keyData, AccessLog accessLog) throws DiskException{
        response.setHeader("Connection", "keep-alive");
//      response.setHeader("Keep-Alive", "timeout=1500, max=1000");
      accessLog.method = HttpParams.PUT;
      accessLog.keyDataLocation = keyData.getAbsolutePath();
      accessLog.uri = request.getQueryString();
      
      try (CheckedInputStream in = new CheckedInputStream(request.getInputStream(), new CRC32())) {
          long mkdirS = System.currentTimeMillis();
          boolean createDir = keyData.getParentFile().mkdirs();
          if(createDir){
              stat.record("put_mkdir_true", System.currentTimeMillis()-mkdirS);
              accessLog.statsTime += "[put_mkdir_true]" + (System.currentTimeMillis() - mkdirS);
          }else{
              stat.record("put_mkdir_false", System.currentTimeMillis()-mkdirS);
              accessLog.statsTime += "[put_mkdir_false]" + (System.currentTimeMillis() - mkdirS);
          }
          stat.record("put_mkdir_total", System.currentTimeMillis() - mkdirS);
          
          long openS = System.currentTimeMillis();
          try (FileOutputStream fos = new FileOutputStream(keyData)) {
              stat.record("put_openfile", System.currentTimeMillis() - openS);
              accessLog.statsTime += ":[put_openfile]" + (System.currentTimeMillis() - openS);
              long writeBobyS = System.currentTimeMillis();
              long contentLen = Long.parseLong(request.getHeader(HttpParams.HEADER_ContenLen));
              long putNum = PDiskIO.streamCopy(in, fos, contentLen, stat, accessLog);
              stat.record("put_body", System.currentTimeMillis() - writeBobyS);
              accessLog.statsTime += "[writeBody]" + (System.currentTimeMillis() - writeBobyS); 
              if(putNum < contentLen){
                  log.error("PUT:readBytes len < contentLen. putNum:" + putNum + " contentLen:" + contentLen);
              }
              long checkSum = ((CheckedInputStream) in).getChecksum().getValue();
              long writeMetaS = System.currentTimeMillis();
              PDiskIO.writeMeta(keyData, request, checkSum);
              stat.record("put_meta", System.currentTimeMillis() - writeMetaS);
              accessLog.statsTime += ":[writeMeta]" + (System.currentTimeMillis() - writeMetaS);
              response.setStatus(201);
          }catch(FileNotFoundException e){
              throw new DiskException(e, 500, "PUT: keyData "+keyData.getAbsolutePath()+" not found.");
          }catch(NumberFormatException e){
              throw new DiskException(e, 401, "PUT: content-len header illegal.");
          }catch(ReadException e){
              throw new DiskException(e, 401, "PUT: failed to read from inputStream.");
          }catch(WriteException e){
              throw new DiskException(e, 500, "PUT: failed to write data.");
          }
      }catch(IOException e){
          throw new DiskException(e, 401, "PUT: failed to get inputStream");                   
      }
  }
    
    //读object文件或�?读取meta属�?两种情况
    private void handleGet(HttpServletRequest request, HttpServletResponse response, File keyData, AccessLog accessLog) throws DiskException{
        response.setHeader("Connection", "keep-alive");
//        response.setHeader("Keep-Alive", "timeout=1500, max=1000");
        long start = System.currentTimeMillis();
        accessLog.method = HttpParams.GET;
        accessLog.keyDataLocation = keyData.getAbsolutePath();
        accessLog.uri = request.getQueryString();
        boolean readObject = false;
        int pageOffset = 0, len = 0;
        
        if(! keyData.isFile()){
            throw new DiskException(404, "Get: " + keyData.getAbsolutePath() + " file not found");
        }
        byte[] writeBytes = null;
        if(! request.getParameterMap().containsKey(HttpParams.PARAM_GET_META)){
            accessLog.statsTime += "[preGet]" + (System.currentTimeMillis() - start);
            readObject = true;
            
            if(request.getParameterMap().containsKey(HttpParams.PARAM_WITH_META)){
                try {
                    String metaData = PDiskIO.getAllMeta(keyData);
                    response.setHeader(HttpParams.HEADER_WITHMETA, URLEncoder.encode(metaData, "UTF-8"));
                } catch (IOException e) {
                    response.setStatus(500);
                    log.error(e.getMessage(), e);
                    return;
                }
            }
            
            writeBytes = localBytes.get();
            try {
                if(! checkKeyDataCrc(keyData, writeBytes, accessLog)){//校验crc的同时拿到object file的大�?
                    putMessageQueueCrcError(request);
                    response.setStatus(411);
                    return;
                }
            } catch (IOException e) {
                log.error("failed to read object", e);
                response.setStatus(500);
                return;
            }
            
            try{
                int totalLen = (int) keyData.length();
                pageOffset = (int) Long.parseLong(request.getParameter(HttpParams.PARAM_PAGEOFFSET));
                if(pageOffset >= totalLen){
                    log.error("pageOffset >= totalLen");
                    response.setStatus(400);
                    return;
                }
                len = (int) Long.parseLong(request.getParameter(HttpParams.PARAM_PAGELEN));
                if(len < 0 || pageOffset + len > totalLen){
                    len =  totalLen - pageOffset;
                }
                response.setHeader(Headers.CONTENT_LENGTH, String.valueOf(len));
            }catch(NumberFormatException e){
                log.error("Illegal Agrument pageoffset or len", e);
                response.setStatus(400);
                return;
            }
        }else{
            try {
                writeBytes = PDiskIO.getAllMeta(keyData).getBytes("UTF-8");
                response.setIntHeader(Headers.CONTENT_LENGTH, writeBytes.length);
            } catch (IOException e) {
                response.setStatus(500);
                log.error(e.getMessage(), e);
                return;
            }
        }
        
        long writeS = System.currentTimeMillis();
        OutputStream os = null;
        try{
            try{
                os = response.getOutputStream();
            }catch(IOException e){
                response.setStatus(401);
                log.error(e.getMessage(), e);
                return;
            }
            
            try {
                if(readObject){
                    os.write(writeBytes, pageOffset, len);
                    stat.record("get_writebody", System.currentTimeMillis() - writeS);
                    accessLog.statsTime = accessLog.statsTime + ":[getBody]" + (System.currentTimeMillis() - writeS);
                }else{
                    os.write(writeBytes);
                }
            } catch (IOException e) {
                log.error("failed to write bytes into outstream", e);
                response.setStatus(401);
            }
        }finally{
            if(os != null){
                try{
                    os.close();
                }catch(IOException e){
                    log.error(e.getMessage(), e);
                }
            }
        }
    }

    @SuppressWarnings("unused")
    private void handleDelete(HttpServletRequest request, HttpServletResponse response, File keyDataLocation, String pdiskRoot, AccessLog accessLog) throws DiskException{
        response.setHeader("Connection", "keep-alive");
//        response.setHeader("Keep-Alive", "timeout=1500, max=1000");
        accessLog.method = "Delete";
        accessLog.keyDataLocation = keyDataLocation.getAbsolutePath();
        accessLog.uri = request.getQueryString();
        
        String vdisk = request.getParameter(HttpParams.PARAM_VDISK);
        String replicate = request.getParameter(HttpParams.PARAM_REPLICATE);
        String pageKey = request.getParameter(HttpParams.PARAM_PAGEKEY);
        File keyDelDataLocation = PDiskIO.getKeyDelDataLocation(pdiskRoot, vdisk, replicate, pageKey);
        boolean directDelete = request.getParameterMap().containsKey("directdelete");
        
        try{
            if(log.isDebugEnabled())
                log.debug("delete " + keyDataLocation.getAbsolutePath());
            if(/*DiskerConfig.isTotalDeleteData() || directDelete */ true){
                Files.delete(keyDataLocation.toPath());
            }else{
                long creatDS = System.currentTimeMillis();
                File parentDir = keyDelDataLocation.getParentFile();
                parentDir.mkdirs();
                stat.record("delete_createdir", System.currentTimeMillis() - creatDS);
                accessLog.statsTime += "[createDir]" + (System.currentTimeMillis() - creatDS);
                long moveS = System.currentTimeMillis();
                Files.move(keyDataLocation.toPath(), keyDelDataLocation.toPath(), StandardCopyOption.REPLACE_EXISTING);
                stat.record("delete_move", System.currentTimeMillis() - moveS);
                accessLog.statsTime += ":[delete]" + (System.currentTimeMillis() - moveS);
            }
            response.setStatus(204);
        }catch(NoSuchFileException e){
            throw new DiskException(e, 404, "Delete: file" + keyDataLocation.getAbsolutePath() + " does not exist.");
        }catch(IOException e){
            throw new DiskException(e, 500, "Delete: failed to move file" + keyDataLocation.getAbsolutePath() + ".");
        }
    }

    private void handleHead(HttpServletRequest request, HttpServletResponse response, File keyDataLocation){
        boolean checkCrc = request.getParameterMap().containsKey(HttpParams.PARAM_HEAD_CRC);
        boolean getLen = request.getParameterMap().containsKey(HttpParams.PARAM_HEAD_LEN);
        boolean getLM = request.getParameterMap().containsKey(HttpParams.PARAM_HEAD_LASTMODIFY);
        if(! keyDataLocation.isFile()){
            response.setStatus(404);
            return;                    
        }
        if(checkCrc){
            byte[] writeBytes = localBytes.get();
            try {
                String crc = PDiskIO.getMeta(keyDataLocation, HttpParams.META_NAME_CRC);
                if(crc == null || crc.equalsIgnoreCase("")){
                    response.setStatus(HttpParams.REPLIC_CRC_NOEXIST);
                    return;
                }
                long crcL = CRCUtils.getCRC32ChecksumAndContent(keyDataLocation, writeBytes, stat);
                if( Long.parseLong(crc) != crcL ){
                    response.setStatus(HttpParams.REPLIC_CRC_UNMATCH);
                    return;
                }else{
                    response.setStatus(HttpParams.REPLIC_OK);
                    return;
                }
            } catch (IOException e) {
                log.error("failed to read object", e);
                response.setStatus(500);
                return;
            }
        }else if(getLen){
            response.setStatus(200);
            response.setHeader(HttpParams.HEADER_ContenLen, String.valueOf(keyDataLocation.length()));
        }else if(getLM){
            response.setStatus(200);
            response.setHeader(HttpParams.HEADER_LASTMODIFIED, String.valueOf(keyDataLocation.lastModified()));
        }else{
            response.setStatus(200);
        }
    }
    
    private void putMessageQueueCrcError(HttpServletRequest request){
        String vdisk = request.getParameter(HttpParams.PARAM_VDISK);
        String replicate = request.getParameter(HttpParams.PARAM_REPLICATE);
        String pageKey = request.getParameter(HttpParams.PARAM_PAGEKEY);
//        try {
//            String mk = MessageType.KeyType.PAGE.buildPageKey(Integer.parseInt(vdisk), replicate, pageKey, 0);
//            mq.put(MessageType.TODO, Bytes.toBytes(mk), Bytes.toBytes(Status.UNHANDLED.intValue()));
//        } catch (IOException e) {
//            log.error("Put message to queue failed", e);
//        }
    }
    
    private void handleCopy(HttpServletRequest request, HttpServletResponse response, File keyDataLocation){
        String dstPdisk = request.getParameter(HttpParams.PARAM_DST_PDISK);
        String dstVdisk = request.getParameter(HttpParams.PARAM_DST_VDISK);
        String dstReplica = request.getParameter(HttpParams.PARAM_REPLICATE);
        String dstReakKey = request.getParameter(HttpParams.PARAM_DST_REALKEY);
        String dstPageKey = request.getParameter(HttpParams.PARAM_DST_PAGEKEY);
//        String dstHost = routeTable.getPDTable().get(Integer.valueOf(dstPdisk));
        long pageOffset = 0L, len = -1L;
        try{
            long totalLen = keyDataLocation.length();
            pageOffset = Long.parseLong(request.getParameter(HttpParams.PARAM_PAGEOFFSET));
            if(pageOffset >= totalLen){
                log.error("pageOffset >= totalLen");
                response.setStatus(400);
                return;
            }
            len = Long.parseLong(request.getParameter(HttpParams.PARAM_PAGELEN));
            if(len < 0 || pageOffset + len > totalLen){
                len =  totalLen - pageOffset;
            }
        }catch(NumberFormatException e){
            log.error("Illegal Agrument pageoffset or len", e);
            response.setStatus(400);
            return;
        }
        try {
            StringBuilder sb = new StringBuilder(128);
//            sb.append("http://" + dstHost + "/?");
            sb.append(HttpParams.PARAM_PDISK+"="+dstPdisk+"&");
            sb.append(HttpParams.PARAM_VDISK+"="+dstVdisk+"&");
            sb.append(HttpParams.PARAM_REPLICATE+"="+dstReplica+"&");
            sb.append(HttpParams.PARAM_REALKEY+"="+dstReakKey+"&");
            sb.append(HttpParams.PARAM_PAGEKEY+"="+dstPageKey+"&");
            sb.append(PDiskIO.buildCopyParam(keyDataLocation));
            if(log.isDebugEnabled()) log.debug("post:" + sb.toString());
            URL url = new URL(sb.toString());
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("PUT");
            conn.setDoOutput(true);
            conn.setFixedLengthStreamingMode(len);
            conn.connect();
            InputStream in = PDiskIO.readFromPdisk(keyDataLocation, pageOffset, len); OutputStream os = conn.getOutputStream();
            try {
                PDiskIO.streamCopy(in, os, len, stat, null);
            } catch (ReadException | WriteException e) {
                response.setStatus(500);
                log.error(e.getMessage(), e);
                return;
            }finally{
                try{
                    in.close();
                }catch(IOException e){
                    log.error(e.getMessage(), e);
                }finally{
                    try{
                        os.close();
                    }catch(IOException e){
                        log.error(e.getMessage(), e);    
                    }
                }
            }
            int respCode = conn.getResponseCode();
            if(respCode != 201){
                response.setStatus(500);
                log.error("copy: get response code:" + respCode);
                return;                            
            }
            response.setStatus(200);
            return;
        } catch (IOException e) {
            log.error(e.getMessage(), e);
            response.setStatus(500);
            return;
        }
    }
    
    private void handleListDir(HttpServletRequest request, HttpServletResponse response, String pdiskDir){
        String vdisk = request.getParameter(HttpParams.PARAM_VDISK);
        String start = request.getParameter(HttpParams.PARAM_LIST_START);
        
        int num = -1, minReplicaNum = -1;
        try{
            num = Integer.parseInt(request.getParameter(HttpParams.PARAM_LIST_NUM));
            minReplicaNum = Integer.parseInt(request.getParameter(HttpParams.PARAM_LIST_MIN_REPLIC_NUM));
        }catch(NumberFormatException e){
            log.error(e.getMessage(), e);
            response.setStatus(400);
            return;
        }
        
        File dir = Paths.get(pdiskDir, OstorConsts.DIR_DATA, vdisk).toFile();
        
        response.setStatus(200);
        byte[] bodyBytes = null;
        try{
            long s = System.currentTimeMillis();
            bodyBytes = PDiskIO.listDir(dir, start, num, minReplicaNum).getBytes("UTF-8");
            stat.record("listdir", System.currentTimeMillis() - s);
        }catch(JSONException | IOException e){
            response.setStatus(500);
            log.error(e.getMessage(), e);
            return;                
        }
        
        assert bodyBytes != null;
        OutputStream  os = null;
        try{
            os = response.getOutputStream();
            response.setIntHeader(Headers.CONTENT_LENGTH, bodyBytes.length);
            os.write(bodyBytes);
        }catch(IOException e){
            response.setStatus(401);
            log.error(e.getMessage(), e);
            return;                
        }finally{
            if(os != null){
                try {
                    os.close();
                } catch (IOException e) {
                    log.error(e.getMessage(),e);
                }
            }
        }
    }
    
    private boolean checkKeyDataCrc(File keyData, byte[] pageContent, AccessLog accessLog) throws IOException{//ok return true
        long start = System.currentTimeMillis();
        String crc = PDiskIO.getMeta(keyData, HttpParams.META_NAME_CRC);
        stat.record("get_meta", System.currentTimeMillis() - start);
        accessLog.statsTime += ":[GetMeta]" + (System.currentTimeMillis() - start);
        if(crc == null || crc.equalsIgnoreCase("")){
            log.error("File " + keyData.getAbsolutePath() + " get File crc meta is:" + crc);
            return false;            
        }
        long start1 = System.currentTimeMillis();
        long crcL = CRCUtils.getCRC32ChecksumAndContent(keyData, pageContent, stat);
        stat.record("get_readbody", System.currentTimeMillis() - start1);
        accessLog.statsTime += ":[GetData]" + (System.currentTimeMillis() - start1);        
        if(Long.parseLong(crc) != crcL){
            log.error("File " + keyData.getAbsolutePath() + " crc32 Check Error. Crc32 Meta is:" + crc + ". Calculated Crc32 is:" + crcL);
            return false;
        }
        return true;
    }
    
    @Override
    public String usage() {
        return "usage: ";
    }

    @Override
    public void exec(String[] args) throws Exception {
        startServer();
    }
    
    
    
    /**
     * ����ֵΪ����ִ����ȷ������µ������Ϣ������ִ�д��󣬷���ֵΪnull��
     * @param cmd
     * @return
     */
    private static String doLinuxComandAndGetMsg(String[] cmd){
        ProcessBuilder p = new ProcessBuilder(cmd);
        while(true){
            try{
                try {
                    Process pro = p.start();
                    pro.waitFor();
                    if(pro.exitValue() == 0){
                        log.info("Execute cmd \"" + cmd[2] + "\" successfully");
                        InputStream in = pro.getInputStream();
                        return IOUtils.toString(in, "UTF-8");
                    }else{
                        InputStream in = pro.getErrorStream();
                        String s = IOUtils.toString(in);
                        log.error("Execute cmd \"" + cmd[2] + "\" failed. ReturnCode is:" + pro.exitValue() + ". Error Message is:" + s);
                        return null;
                    }
                } catch (IOException e) {
                    log.error("Execute cmd \"" + cmd[2] + "\" failed. " + e.getMessage(), e);
                    return null;
                }
            }catch(InterruptedException e){
                log.error(e.getMessage(), e);
                continue;
            }
        }
    }
    
    /**
     * @param partition
     * @return
     */
    public static String getPartitionUsedSpace(String partition){ //disk�����Ǵ��̣������ǹ��ص�, ���ص�λ��bytes
        String[] cmd = new String[] {"/bin/bash", "-c", "df  | grep -w " + partition + " | awk '{print $3}'"};
        String msg = doLinuxComandAndGetMsg(cmd);
        if(msg != null){
            msg = msg.trim();
            log.info("Availbable Space of disk " + partition + " is:" + msg);
            return msg;
        }else{
            log.error("Failed to get available space of disk " + partition);
        }
        return null;
    }
}

class PDiskIO{
    private static Log log = LogFactory.getLog(PDiskIO.class);
    private static String[] params = new String[] { HttpParams.PARAM_REALKEY, HttpParams.PARAM_TOTALPAGE, 
            HttpParams.PARAM_CURRPAGE};
    private static Charset cs = Charset.forName("UTF-8");
    
    public static void formatPdisk(File file) throws IOException{
        if(! file.exists()){
            log.error(file.getAbsoluteFile() + " does not exist");
            throw new RuntimeException(file.getAbsoluteFile() + " does not exist");
        }
        try {
            Files.createDirectories(Paths.get(file.getAbsolutePath(), OstorConsts.DIR_ROUTE));
            Files.createDirectories(Paths.get(file.getAbsolutePath(), OstorConsts.DIR_DATA));
            Files.createDirectories(Paths.get(file.getAbsolutePath(), OstorConsts.DIR_META));
            Files.createDirectories(Paths.get(file.getAbsolutePath(), OstorConsts.DIR_DELETED));
        } catch (IOException e) {
            log.error("failed to make dirs", e);
            throw e;
        }
    }
    
    public static InputStream readFromPdisk(File keyDataLocation,
            long pageOffset, final long len) throws IOException {
        final FileInputStream fis = new FileInputStream(keyDataLocation);
        fis.skip(pageOffset);
        return fis;
    }
       
    public static File getKeyDataLocation(String pdiskRoot, String vdisk, String replica, String keyPage){
        Path p1 = Paths.get(pdiskRoot, OstorConsts.DIR_DATA, vdisk, replica);
        String[] splitKeys = splitKeypage(keyPage);
        Path p2 = Paths.get(p1.toString(), splitKeys);
        return new File(p2.toFile(), keyPage);
    }
       
    public static File getKeyDelDataLocation(String pdiskRoot, String vdisk, String replica, String keyPage){
        String yyyyMMDD = TimeUtils.toYYYYMMDD(new Date(), TimeZone.getTimeZone("GMT"));
        Path p1 = Paths.get(pdiskRoot, OstorConsts.DIR_DELETED, "GMT"+yyyyMMDD, OstorConsts.DIR_DATA, vdisk, replica);
        String[] splitKeys = splitKeypage(keyPage);
        Path p2 = Paths.get(p1.toString(), splitKeys);
        return new File(p2.toFile(), keyPage);
    }
    
    public static String[] splitKeypage(String keyPage){
        String[] res = new String[] { keyPage.substring(0, 3)};
        return res;
    }
    
    public static void writeMeta(File dstfile, HttpServletRequest request, long checkSum) throws IOException{       
        UserDefinedFileAttributeView udfa = Files.getFileAttributeView(dstfile.toPath(), UserDefinedFileAttributeView.class);
        for(String param : params){
            udfa.write(param, cs.encode(request.getParameter(param)));
        }
        String userMeta = (String) request.getParameter(HttpParams.PARAM_USERMETA);
        if(userMeta != null && ! userMeta.equalsIgnoreCase("")){
            udfa.write(HttpParams.PARAM_USERMETA, cs.encode(userMeta));
        }else{//再次Put,userMeta为空，�?原来userMeta不为�?删除�?
            if(udfa.list().contains(HttpParams.PARAM_USERMETA)){
                udfa.delete(HttpParams.PARAM_USERMETA);
            }
        }
        udfa.write(HttpParams.META_NAME_CRC, cs.encode(String.valueOf(checkSum)));
    }
    
    public static void readUserDefinedFileAttribute(UserDefinedFileAttributeView udfa, String metaKey, ByteBuffer bb, File metaFile) throws IOException{
        for(int retry=0; retry<10; retry++){
            int readNum = udfa.read(metaKey, bb);
            if(bb.position() > 0){
                break;
            }
            log.error("ERROR:Read metakey " + metaKey + " from file "+ metaFile.getCanonicalPath() + " failed. RetryNum:" + retry + ". readNum:" + readNum + ". bytebuffer limit:" + bb.limit());
        }
    }
    
    public static String getMeta(File metaFile, String metaKey) throws IOException{
        UserDefinedFileAttributeView udfa = Files.getFileAttributeView(metaFile.toPath(), UserDefinedFileAttributeView.class);
        if(udfa.list().contains(metaKey)){
            byte[] buf = Disker.localBytes.get();
            ByteBuffer bb = ByteBuffer.wrap(buf, 0, buf.length);
            try{
                readUserDefinedFileAttribute(udfa, metaKey, bb, metaFile);
            }catch(Exception e){
                if(e.getMessage().contains("cannot allocate memory")){
                    log.error("GetMemoryException:" + e.getMessage(), e);
                    readUserDefinedFileAttribute(udfa, metaKey, bb, metaFile);
                }
            }
            bb.flip();
            return cs.decode(bb).toString();
        }else{
            return null;
        }
    }
        
    public static String getAllMeta(File metaFile) throws IOException{
        return "";
    }
    
    public static boolean checkFileBeingWrited(File f) throws IOException{
        UserDefinedFileAttributeView udfa = Files.getFileAttributeView(f.toPath(), UserDefinedFileAttributeView.class);
        if(udfa.list().contains(HttpParams.META_NAME_CRC)){
            return false;
        }else{
            return true;
        }
    }
    
    public static String buildCopyParam(File metaFile) throws IOException{
        Properties p = new Properties();
        InputStream is = new FileInputStream(metaFile);
        try{
            p.load(is);
            return HttpParams.PARAM_TOTALPAGE+"="+getMeta(metaFile, HttpParams.PARAM_TOTALPAGE) + "&" + HttpParams.PARAM_CURRPAGE+"="+getMeta(metaFile, HttpParams.PARAM_CURRPAGE); 
        }finally{
            is.close();
        }        
    }
    
    public static String listDir(File dir, String start, int num, final int minReplicaNum) throws JSONException, IOException{
        String startReplica = null; 
        String startPrefix = null;
        JSONObject jo = new JSONObject();
        JSONArray jsonReplicas = new JSONArray();
        
        if(! dir.isDirectory()){
            jo.put(HttpParams.JSON_LIST_REPLIC, jsonReplicas);
            return jo.toString();
        }
        if(start != null && start.length() != 0){
            String[] startSplits = start.split("/");//start合法形式 null("")、replicate/prefix、replicate
            assert startSplits.length >=1;
            if(startSplits.length == 1){
                startReplica = startSplits[0];
            }else{
                startReplica = startSplits[0];
                startPrefix = startSplits[1];
            }
        }
        
        final String startReplicaFinal = startReplica;
        final String startPrefixFinal = startPrefix;
        FilenameFilter filterReplica = new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                DiskStress.ReplicaMode replica = DiskStress.ReplicaMode.parser(name);
                if(startReplicaFinal != null){
                    if(name.compareTo(startReplicaFinal) < 0){
                        return false;
                    }
                }
                if(replica.ec_m + replica.ec_n >= minReplicaNum){
                    return true;
                }
                return false;
            }
        };
        FilenameFilter filterPrefix = new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                if(startPrefixFinal != null){
                    if(name.compareTo(startPrefixFinal) < 0){
                        return false;
                    }
                }
                return true;
            }
        };
        
        ArrayList<String> replicas = new ArrayList<String>(Arrays.asList(dir.list(filterReplica)));
        Collections.sort(replicas);
        
        boolean findEnd = false;
        String nextReplica = null;
        String nextPrefix = null;
        int filesCount = 0;
        for(int replicaPos=0; replicaPos<replicas.size(); replicaPos++){
            if(findEnd == true){
                break;
            }
            String replica = replicas.get(replicaPos);
            File replicaDir = new File(dir, replica);
            ArrayList<String> prefixs = null;
            if(replica.equalsIgnoreCase(startReplica) && startPrefix != null){
                prefixs = new ArrayList<String>(Arrays.asList(replicaDir.list(filterPrefix)));
            }else{
                prefixs = new ArrayList<String>(Arrays.asList(replicaDir.list()));
            }
            Collections.sort(prefixs);
            
            /***/
            JSONObject joReplica = new JSONObject();
            joReplica.put(HttpParams.JSON_LIST_REPLIC_NAME, replica);
            JSONArray jaWritingFiles = new JSONArray();
            JSONArray jaNotWritingFiles = new JSONArray();
            /***/
            
            for(int prefixPos=0; prefixPos<prefixs.size(); prefixPos++){
                File prefixDir = new File(replicaDir, prefixs.get(prefixPos));
                File[] pages = prefixDir.listFiles();
                for(File f : pages){
                    if(checkFileBeingWrited(f)){
                        jaWritingFiles.put(f.getName());
                    }else{
                        jaNotWritingFiles.put(f.getName());
                    }
                }
                
                filesCount += pages.length;
                if(filesCount >= num){
                    findEnd = true;
                    if(prefixPos == prefixs.size()-1){
                        if(replicaPos == replicas.size() -1){
                            nextReplica = null;
                            nextPrefix = null;
                        }else{
                            nextReplica = replicas.get(replicaPos + 1);
                            nextPrefix = null;
                        }
                    }else{
                        nextReplica = replica;
                        nextPrefix = prefixs.get(prefixPos + 1); 
                    }
                    break;
                }            
            }
            
            joReplica.put(HttpParams.LIST_RES_WRITINGFILES, jaWritingFiles);
            joReplica.put(HttpParams.LIST_RES_WRITTENFILES, jaNotWritingFiles);
            jsonReplicas.put(joReplica);
        }
        
        String next = null;
        if(findEnd){
            if(nextReplica == null && nextPrefix == null){
                next = "";
            }else if(nextReplica != null && nextPrefix == null){
                next = nextReplica;
            }else if(nextReplica != null && nextPrefix != null){
                next = nextReplica + "/" + nextPrefix;
            }
        }else{
            next = "";
        }
        jo.put(HttpParams.JSON_LIST_NEXT, next);
        jo.put(HttpParams.JSON_LIST_REPLIC, jsonReplicas);
        return jo.toString();
    }
    
    public static int streamCopy(InputStream in, OutputStream out, long len, TimeStat stat, AccessLog accessLog) throws ReadException, WriteException{
        byte[] pageBytes = Disker.localBytes.get();
        int r;
        int alreadyRead = 0; int toRead = (int) len;
        long readS = System.currentTimeMillis();
        while(alreadyRead < toRead){
            try {
                r = in.read(pageBytes, alreadyRead, toRead-alreadyRead);
            } catch (IOException e) {
                throw new ReadException(e);
            }
            if(r == -1){
                break; 
            }
            alreadyRead += r;
        }
        stat.record("put_readStream", System.currentTimeMillis()-readS);
        if(accessLog != null){
            accessLog.statsTime += ":[readstream]" + (System.currentTimeMillis()-readS) + ":";
        }
        long writeS = System.currentTimeMillis();
        try {
            out.write(pageBytes, 0, alreadyRead);
        } catch (IOException e) {
            throw new WriteException(e);
        }
        stat.record("put_writefile", System.currentTimeMillis()-writeS);
        if(accessLog != null){
            accessLog.statsTime += "[writefile]" + (System.currentTimeMillis()-writeS) + ":";
        }
        return alreadyRead;
    }
    
    public static void main(String[] args){
    }

}


/**
 * @author liuyt
 * @author Dongchk
 */
/*class DiskerNode extends Thread{
    private static Log log = LogFactory.getLog(DiskerNode.class);
    private String diskerIp;
    private ZKClient zk;
    private String znodeData = "";
    
    public DiskerNode(){
        this.diskerIp = DiskUtils.getLocalHostNamePort();
//        this.diskerIp = DiskUtils.getHostPortAddr();
        try {
            zk = new ZKClient(ZKConfig.getQuorumServers(), ZKConfig.getSessionTimeout());
        } catch (IOException e) {
            log.info("Register datanode to zk failed." );
            throw new RuntimeException("Register datanode to zk failed." );
        }
        this.setDaemon(true);
    }
    
    @Override
    public void run(){
        
        while (true){
            try {
                //先创建代表物理机的永久节点，再创建临时节�?
                zk.createWithParents(ZKNode.DATA_NODE_PERSISTENT_PREFIX + "/" + diskerIp, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                log.info("Set:" + ZKNode.DATA_NODE_PERSISTENT_PREFIX + "/" + diskerIp);
                zk.createWithParents(ZKNode.DATA_NODE_EPHEMERAL_PREFIX, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                String path = ZKNode.DATA_NODE_EPHEMERAL_PREFIX + "/" + diskerIp ;
                try{
                    zk.delete(path, -1);
                } catch (NoNodeException e){}
                zk.create(path, Bytes.toBytes(znodeData), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                break;
            } catch (KeeperException | InterruptedException e) {
                log.error(e.getMessage(), e);
            }
        }
       
        while (true){
            try {
                Thread.sleep(Long.MAX_VALUE);
            } catch (InterruptedException e) {
                log.info("I am interrupted" );
            }
        }
    }
    
    public static void main(String[] args) throws FileNotFoundException, IOException{
        DiskerNode diskerNode = new DiskerNode();
        diskerNode.run();
    }
}*/

class ReadException extends Exception{
    private static final long serialVersionUID = 8770386391901647828L;
    public ReadException(Exception e){
        super(e);
    }
    public ReadException(String msg){
        super(msg);
    }
}
class WriteException extends Exception{
    private static final long serialVersionUID = -6189195752177819632L;
    public WriteException(Exception e){
        super(e);
    }
    public WriteException(String msg){
        super(msg);
    }
}
class DiskException extends Exception{
    private static final long serialVersionUID = 3139247800766707682L;
    int status;
    String msg;
    
    public DiskException(){
    }
    public DiskException(Exception e){
        super(e);
    }
    public DiskException(String msg){
        super(msg);
    }
    public DiskException(int status, String msg){
        super(msg);
        this.status = status;
    }
    public DiskException(Exception e, int status, String msg){
        super(msg, e);
        this.status = status;
    }
}
class AccessLogWriter{
    private static Log log = LogFactory.getLog(AccessLogWriter.class);
    private static String filePath = Paths.get(System.getProperty("user.dir"), "ostore-accesslog", "accessOstor.log").toString();
    private static BlockingExecutor exe = new BlockingExecutor(100, 100, 100, 1000, "accesslog");
    private static LogOneLine oneLine = new LogOneLine(filePath, Integer.MAX_VALUE, 1021*1024*100, 40);
    public static void writeLog(final AccessLog access){
        exe.submit(new Runnable(){
            @Override
            public void run() {
                try {
                    access.date = TimeUtils.toYYYYMMddHHmmss(new Date());
                    oneLine.writeOneLine(access.toString());
                } catch (IOException e) {
                    log.error("failed to write one log", e);
                }
            }
        });
    }
}

class AccessLog{
    public String method;
    public String keyDataLocation;
    public int respCode = -1;
    public String errorMsg;
    public String uri;
    public String date;
    public String statsTime = "";
    
    public String toString(){
        StringBuilder sb = new StringBuilder(256);
        sb.append("[" + date + "]" + " ");
        sb.append(method + " ");
        sb.append("statsTime:" + statsTime + " ");
        sb.append("keyDataLocation:" + keyDataLocation + " ");
        sb.append("respCode:" + respCode + " ");
        sb.append("errorMsg:" + (errorMsg != null ? "\""+errorMsg+"\"" : "-1") + " ");
        sb.append("uri:" + uri);
        return sb.toString();
    }
}   
