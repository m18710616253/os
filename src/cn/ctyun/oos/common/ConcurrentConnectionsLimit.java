package cn.ctyun.oos.common;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang3.StringUtils;

import com.amazonaws.HttpMethod;

import cn.ctyun.common.BaseException;
import cn.ctyun.common.Consts;
import cn.ctyun.common.conf.OOSConfig;
import cn.ctyun.oos.server.signer.V4Signer;
import cn.ctyun.oos.server.util.HttpLimitParser;
import common.tuple.Pair;

/**
 * {@code ConcurrentConnectionsLimit}实现了并发数限制功能。
 * 需要所有资源池全部升级之后才能使用此功能。 否则老代码可能会报错。
 * 增加了一个oosconfig配置。 
 * 1 如果只有一个concurrency 的值与其他的不一样，那样会导致某台机器并发数小，其他机器并发数大。 
 *  
 * @author wangxingshan
 */
public class ConcurrentConnectionsLimit {
    //Map<分类（预签名还是普通对象），Map<预签名是签名字符串普通对象是bucket+对象名， 原子增>> 
    public static Map<String, Concurrence> map  = new ConcurrentHashMap<String, Concurrence>();
    
    private static final String KEY_PREFIX = "limit--/--";
    private static final String POSTFIX = "_concurrent";
    
    
    /**
      * 判断本次请求是否超过了并发数限制。
      * 如果不满足允许条件，直接抛异常出去，计数器不会被加一。
     */
    public static Pair<String, String> allow(HttpServletRequest req, String bucket, String objectKey) throws BaseException {
        if(!HttpMethod.GET.toString().equals(req.getMethod()) && !HttpMethod.PUT.toString().equals(req.getMethod())) {
            return null;
        }
        Pair<String, String> p = getKeyAndLimit(req, bucket, objectKey);
        if(null == p)
            return null;
        String key = p.first();
        String limit = p.second();
        int concurrency = getConcurrency(limit);
        //先判断key是否为空，如果为空创建这个key ，然后拿锁。
        synchronized (key.intern()) {
            if(null == map.get(key)) {
                map.computeIfAbsent(key, k -> new Concurrence(concurrency));
            }
            if(map.get(key).counter > concurrency)
                map.get(key).counter = concurrency;
            if(map.get(key).isNotFull())
                map.get(key).ai.incrementAndGet();
            else
                throw new BaseException(400,
                        ErrorMessage.ERROR_OVER_CONCURRENT,
                        ErrorMessage.ERROR_OVER_CONCURRENT_MESSAGE);
        }
        return p;
    }
    
    public static void release(Pair<String, String> p) {
        if(null == p)
            return;
        String key = p.first();
        Concurrence concurrence = map.get(key);
        if(null == concurrence)
            return;
        synchronized (key.intern()) {
            if(0 >= concurrence.ai.decrementAndGet()) {
                map.remove(key);
            }
        }
    }
    
    private static int getConcurrency(String seed) throws BaseException{
         return convert(seed.split("="));
    }
    
    /**
     * 1 预签名GET请求使用签名做key ，因为只要签名不变，请求url 就不会变。 
     * 2 普通get请求使用bucket+objectkey做为key 。可以达到对对象进行并发数限制的目的。  
     * 3 put请求使用upload id 作为key ，可以达到所有分片使用一个并发数限制进行控制的目的。 
     * */
    private static Pair<String, String> getKeyAndLimit(HttpServletRequest req, String bucket, String objectKey){
        String key = null;
        String limit = null;
        limit = HttpLimitParser.getLimit(req, Consts.X_AMZ_LIMIT_CONCURRENCY_INDEX);
        if(null == limit)
            return null;
        boolean isPut = req.getMethod().equalsIgnoreCase(HttpMethod.PUT.toString());
        boolean isGet = req.getMethod().equalsIgnoreCase(HttpMethod.GET.toString());
        if(isPut && StringUtils.isNotBlank(req.getParameter("uploadId"))) {
            key = req.getParameter("uploadId");
        } else if(isGet && Utils.isPresignedUrl(req)){
            key = req.getParameter("Signature");
            if(StringUtils.isBlank(key))
                key = req.getParameter(V4Signer.X_AMZ_SIGNATURE_CAPITAL);
        } else 
            key = bucket + "/" + objectKey; 
        key = KEY_PREFIX + key + POSTFIX;
        Pair<String, String> pair = new Pair<String,String>(key, limit);
        return pair;
    }
    
    private static int convert(String[] concurrency) throws BaseException {
        if(concurrency.length != 2)
            throw new BaseException(400,
                    ErrorMessage.ERROR_CODE_INVALID_ARGUMENT,
                    ErrorMessage.ERROR_MESSAGE_INVALID_X_AMZ_CONCURRENCY);
        int rate = 0;
        try {
            rate = Integer.parseInt(concurrency[1].trim());
            if (rate < 1) {
                throw new BaseException(400,
                        ErrorMessage.ERROR_CODE_INVALID_ARGUMENT,
                        ErrorMessage.ERROR_MESSAGE_INVALID_X_AMZ_CONCURRENCY);
            }
        } catch (NumberFormatException e) {
            throw new BaseException(400,
                    ErrorMessage.ERROR_CODE_INVALID_ARGUMENT,
                    ErrorMessage.ERROR_MESSAGE_INVALID_X_AMZ_CONCURRENCY);
        }
        return rate;
    }
    
}


class Concurrence{
    
    int counter = 0;
    AtomicInteger ai = new AtomicInteger(0);
    
    Concurrence(int counter){
        this.counter = counter;
    }
    
    public boolean isFull() {
        if(counter <= OOSConfig.getApiNodeNum())
            return ai.get() >= 1;
        return ai.get() >= (counter / OOSConfig.getApiNodeNum());
    }
    
    public boolean isNotFull() {
        return !isFull();
    }
    
}




