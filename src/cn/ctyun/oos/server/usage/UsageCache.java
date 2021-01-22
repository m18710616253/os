package cn.ctyun.oos.server.usage;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map.Entry;
import java.util.TreeMap;

/**
 * 非线程安全的
 *
 */
public class UsageCache<T> {
    private int n;
    private int capital;
    private LinkedHashMap<String, T> linkedMap;
    private TreeMap<Long, String> lifeCycle = new TreeMap<>();
    private long expireTime = -1;
    
    UsageCache(int capital) {
        this.capital = capital;
        this.linkedMap = new LinkedHashMap<String, T>(capital);
    }
    
    public synchronized void put(String k, T v) {
        put(k, v, -1);
    }
    
    /**
     * 指定cache的时长mills，如果不指定则LifeCycle设为-1
     * @param k
     * @param v
     * @param lifeCycleMills
     */
    public synchronized void put(String k, T v, long lifeCycleMills) {
        linkedMap.put(k, v);
        n++;
        if(lifeCycleMills > 0) {
            long t = System.currentTimeMillis() + lifeCycleMills;
            if(expireTime == -1 || t < expireTime)
                expireTime = t;
            lifeCycle.put(t, k);
        }
        //如果有超时的，则清理
        if(expireTime > 0 && expireTime < System.currentTimeMillis()) {
            for(Iterator<Entry<Long, String>> itr = lifeCycle.entrySet().iterator();itr.hasNext();) {
                Entry<Long, String> entry = itr.next();
                if(entry.getKey() < System.currentTimeMillis()) {
                    itr.remove();
                    if(linkedMap.remove(entry.getValue()) != null)
                        n--;
                } else {
                    expireTime = entry.getKey();
                    break;
                }
            }
        }
        if(n > capital) {
            for(Iterator<Entry<String, T>> itr = linkedMap.entrySet().iterator();itr.hasNext() && n > capital;) {
                itr.next();
                itr.remove();
                n--;
            }
        }
        
    }

    public T get(String key) {
        return linkedMap.get(key);
    }
    
    public synchronized void clear() {
        n = 0;
        linkedMap.clear();
    }
}
