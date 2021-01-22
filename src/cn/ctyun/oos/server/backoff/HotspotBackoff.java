package cn.ctyun.oos.server.backoff;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import cn.ctyun.oos.common.MaxHeapCache;
import cn.ctyun.oos.metadata.BucketMeta;
import cn.ctyun.oos.metadata.ObjectMeta;
import cn.ctyun.oos.server.conf.BackoffConfig;

public class HotspotBackoff {
    static {
        System.setProperty("log4j.log.app", "HotspotBackoff");
    }
    private static final Log log = LogFactory.getLog(HotspotBackoff.class);
    
    public static int MAX_HEAPSIZE = 100;     //堆大小
    public static long EXPIREDTIME = 60000;  //过期时间
    
    public volatile static MaxHeapCache<String> cache;
    
    static{
        MAX_HEAPSIZE = BackoffConfig.getBackoffHeapSize();
        EXPIREDTIME = BackoffConfig.getObjectBackoffExpiredTime();
        cache = new MaxHeapCache<String>(MAX_HEAPSIZE,EXPIREDTIME);
    }
    
    
    //定时打印堆信息
    static{
        new Thread(){
            public void run(){
                long last = System.currentTimeMillis();
                while(true){
                    if(System.currentTimeMillis()-last<10000){
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }else{
                        log.info("print backoff heap top 5 objects:\n"+cache.dump(5));
                        last = System.currentTimeMillis();
                    }
                }
            }
        }.start();
    }
    
    public static void main(String []args){
        final List<String> list = new ArrayList<String>();
        list.add("aaa");
        list.add("bbb");
        list.add("ccc");
        list.add("ddd");
        list.add("eee");
        list.add("aaa1");
        list.add("bbb1");
        list.add("ccc1");
        list.add("ddd1");
        list.add("eee1");
        list.add("aaa3");
        list.add("bbb3");
        list.add("ccc3");
        list.add("ddd3");
        list.add("eee3");
        final Random ran = new Random();
        
        ExecutorService exec = Executors.newFixedThreadPool(10);
        while(true){
            exec.submit(new Runnable() {
                @Override
                public void run() {
                    String key = list.get(ran.nextInt(list.size()));
                    cache.add(key);
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    checkBackoff(key);
                }
            });
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        
    }
    public static String getKey(BucketMeta bucket, ObjectMeta object){
        return bucket.name+"/"+object.name;
    }
    
    public static synchronized boolean checkBackoff(BucketMeta bucket, ObjectMeta object){
        return checkBackoff(getKey(bucket, object));
    }
    
    public static synchronized void increaseConnection(BucketMeta bucket, ObjectMeta object){
        increaseConnection(getKey(bucket, object));
    }
    
    public static synchronized void increaseConnection(String key){
        cache.add(key);
    }
    
    /**
     * 从堆中取出最大值，如果对象正好是堆顶对象，则backoff
     * @param key
     * @return
     */
    public static synchronized boolean checkBackoff(String key){
        String maxKey = cache.peek();
        //TODO 是否加一个阈值
        if(maxKey.equals(key)){
            log.info("BackOff key:"+key);
            return true;
        }
        return false;
    }
}
