package cn.ctyun.oos.tempo.disktest;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONArray;
import org.json.JSONObject;

import cn.ctyun.common.Program;
import cn.ctyun.oos.ostor.common.HttpParams;
import cn.ctyun.oos.ostor.http.HttpDelete;
import cn.ctyun.oos.ostor.http.HttpGet;
import cn.ctyun.oos.ostor.http.HttpPut;

import common.threadlocal.ThreadLocalBytes;
import common.tuple.Pair;
import common.util.BlockingExecutor;
import common.util.HexUtils;
import common.util.NumberUtils;

public class DiskStress implements Program{
    private static Log log = LogFactory.getLog(DiskStress.class);

    private static InetSocketAddress soaddr = new InetSocketAddress("127.0.0.1", 9000); 
    private static int SIZE = 128*1024;
//    private BufferPool bufferPool = new BufferPool(4096, SIZE, SIZE * 1024); //5g
    private static List<ReplicaMode> relicas = ReplicaMode.replicaModes();
    private static SecureRandom random = new SecureRandom();
    static{
        random.nextBytes(new byte[1]);
    }
    private BlockingExecutor exec = new BlockingExecutor(200, 600, 100000, 1000, "diskstress");
    
    public static void main(String[] args) {
        new DiskStress().start(args);
    }
    
    public void start(String[] args){
        if(args.length != 4){
            throw new IllegalArgumentException("argc error");
        }
        final int threadnum = Integer.parseInt(args[0]);
        final double getRatio = Double.parseDouble(args[1]);
        final double delRatio = Double.parseDouble(args[2]);
        final double listRatio = Double.parseDouble(args[3]);
        for(int i=0; i <threadnum; i++){
            new Thread(){
                @Override
                public void run(){
                    while(true){
                        try{
                            Pair<StringBuilder, Integer> res = formatQueryStr();
                            StringBuilder basePath = res.first(); int vdisk = res.second(); 
                            putPage(basePath);
                            boolean isGet = random.nextDouble() < getRatio ? true : false;
                            boolean isDel = random.nextDouble() < delRatio ? true : false;
                            boolean isList = random.nextDouble() < listRatio ? true : false;
                            if(isGet){
                                getPage(basePath);
                            }
                            if(isDel){
                                deletePage(basePath);
                            }
                            if(isList){
                                listPage(vdisk);
                            }
                        }catch(Throwable e){
                            log.error(e.getMessage(), e);
                        }
                    }
                }
            }.start();
        }
    }
    
    public void putPage(StringBuilder basePath){
        basePath.append("&realKey=thisisrealkey&totalPageNum=1&pageNum=0");
//        ByteBuffer bb = bufferPool.allocByteBuffer();
        ByteBuffer bb = ByteBuffer.allocate(SIZE);
        random.nextBytes(bb.array());
        try{
            HttpPut put = new HttpPut(soaddr, basePath.toString(), bb);
            put.call();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }finally{
//            bufferPool.freeByteBuffer(bb);
        }
    }
   
    public void getPage(StringBuilder basePath) throws Exception{
        basePath.append("&pageOffset=0&length=-1");
        HttpGet get = new HttpGet(soaddr, basePath.toString());
        Pair<InputStream, Integer> in = get.call();
        byte[] tmp = new byte[1024];
        int pos = 0;
        while(pos < in.second()){
            int res = in.first().read(tmp);
            if(res == -1){
                break;
            }
            pos+=res;
        }
    }
    
    public void deletePage(StringBuilder basePath) throws Exception{
        HttpDelete delete = new HttpDelete(soaddr, basePath.toString());
        delete.call();
    }
    
    public void listPage(int vdisk) throws Exception{
        listFiles(1, vdisk, 2000, 1, "");
    }
    
    public static byte[] generateOstorKey(){
        byte[] ostorKey = new byte[16];
        random.nextBytes(ostorKey);
        return ostorKey;
    }
    
    public static byte[] getPageKey(byte[] ostorKey, long pageNum){
        byte[] page = ThreadLocalBytes.current().get24Bytes();
        System.arraycopy(ostorKey, 0, page, 0, ostorKey.length);
        NumberUtils.longToByteArray(pageNum, page, page.length - 8);
        return page;
    }
    
    public static Pair<StringBuilder, Integer> formatQueryStr(){
        StringBuilder sb = new StringBuilder(60);
//        int vdisk = random.nextInt(10);
        int vdisk = 600;
//        int relicaPos = random.nextInt(relicas.size());
        int relicaPos = 0;
        int pageNum = random.nextInt(10);
        byte[] ostorKey = generateOstorKey();
        byte[] pageKey = getPageKey(ostorKey, pageNum);
        sb.append("/?pdisk=1&vdisk=").append(vdisk).append("&replicate=").append(relicas.get(relicaPos))
        .append("&pageKey=").append(HexUtils.toHexString(pageKey));
        return new Pair<StringBuilder, Integer>(sb, vdisk);
    }
    
    public static class ReplicaMode {
        public int ec_n;
        public int ec_m;
        public int minReplicaNum;

        public ReplicaMode(int ec_n, int ec_m, int minReplicaNum) {
            this.ec_n = ec_n;
            this.ec_m = ec_m;
            this.minReplicaNum = minReplicaNum;
        }

        public String toString() {
            return "EC_" + ec_n + "_" + ec_m + "_" + minReplicaNum;
        }
        
        public String getReplicaMode(){
            return "EC_" + ec_n + "_" + ec_m;
        }
        
        public static ReplicaMode parser(String data){
            String[] tmp = data.split("_");
            assert (tmp.length == 3 || tmp.length == 4);
            if(tmp.length == 3)
                return new ReplicaMode(Integer.parseInt(tmp[1]), Integer.parseInt(tmp[2]), Integer.parseInt(tmp[1]) + Integer.parseInt(tmp[2]));
            else
                return new ReplicaMode(Integer.parseInt(tmp[1]), Integer.parseInt(tmp[2]), Integer.parseInt(tmp[3]));
        }

        static List<ReplicaMode> modes = new ArrayList<ReplicaMode>();
        static{
            for(int i = 1; i <= 10; i++)
                modes.add(new ReplicaMode(i, 0, i));
            for(int i = 2; i < 10; i++)
                modes.add(new ReplicaMode(i, 1, i + 1));
            for(int i = 2; i < 10; i++)
                modes.add(new ReplicaMode(i, 2, i + 2));
        }
        
        public static List<ReplicaMode> replicaModes() {
            return modes;
        }
        
    }

    /**
     * @return                 
     * @throws Exception
     */
    public static ListDetail listFiles(long pdiskId, 
            int vdisk, int num, int miniReplicNum, String startKey) throws Exception {
        
        ListDetail detail = new ListDetail();
        Map<String, Pair<List<String>, List<String>>> dMap = new HashMap<>();
        StringBuilder reqUrl = new StringBuilder(30);
        InputStream in = null;
        String host = null;
        try {
            if(null == startKey || startKey.trim().isEmpty())
                startKey = "";
            
            reqUrl.append("/?")
                .append(HttpParams.PARAM_PDISK).append("=").append(pdiskId)
                .append("&").append(HttpParams.PARAM_VDISK).append("=").append(vdisk)
                .append("&").append(HttpParams.PARAM_LIST_NUM).append("=").append(num)
                .append("&").append(HttpParams.PARAM_LIST_MIN_REPLIC_NUM).append("=").append(miniReplicNum)
                .append("&").append(HttpParams.PARAM_LIST_START).append("=").append(startKey);
            
            log.info("List Directores url:" + reqUrl.toString());

            Pair<InputStream, Integer> ins = new HttpGet(soaddr, reqUrl.toString()).call();
            in = ins.first();
            
            JSONObject o = new JSONObject(IOUtils.toString(in));
            if(o.has(HttpParams.JSON_LIST_NEXT)){
                detail.next = o.getString(HttpParams.JSON_LIST_NEXT);
            }
            if(null != detail.next && detail.next.trim().isEmpty())
                detail.next = null;
            
            JSONArray arr = o.getJSONArray(HttpParams.JSON_LIST_REPLIC);
            for(int i = 0; i < arr.length(); i++){
                JSONObject e = arr.getJSONObject(i);
                String replicName = e.getString(HttpParams.JSON_LIST_REPLIC_NAME);
                JSONArray w1 = e.getJSONArray(HttpParams.LIST_RES_WRITINGFILES);
                JSONArray w2 = e.getJSONArray(HttpParams.LIST_RES_WRITTENFILES);
                List<String> writtingFiles = new ArrayList<>();
                List<String> writtenFiles = new ArrayList<>();
                for(int j = 0; j < w1.length(); j++){
                    writtingFiles.add(w1.getString(j));
                }
                for(int j = 0; j < w2.length(); j++){
                    writtenFiles.add(w2.getString(j));
                }
                dMap.put(replicName, new Pair<>(writtingFiles, writtenFiles));
            }
            detail.detail = dMap;
        } catch (Exception e) {
            log.error("List Dirs failed: pdiskId=" + pdiskId + " host= " + host, e);
            throw e;
        } finally{
            if(null != in)
                try {
                    in.close();
                } catch (IOException e) {}
        }
        return detail;
    }
    
    @Override
    public String usage() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void exec(String[] args) throws Exception {
        start(args);
    }
    
    public static class ListDetail{
        public String next;
        public Map<String, Pair<List<String>, List<String>>> detail;
    }
}
