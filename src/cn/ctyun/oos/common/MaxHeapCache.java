package cn.ctyun.oos.common;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import cn.ctyun.common.Time;

/**
 * @author Dongchk
 *
 */
public class MaxHeapCache<E> {
    private final long expiredTime;
    private Object lock = new Object();
    private TreeMap<Integer, List<Entry<E>>> cache;
    private Map<E, Entry<E>> kvs;

    public MaxHeapCache(final int size, final long expiredTime) {
        this.expiredTime = expiredTime;
        cache = new TreeMap<Integer, List<Entry<E>>>(new MaxComparator());
        kvs = new HashMap<>();
        final int checkInterval = 2000;
        new Thread() {
            public void run() {
                while (true) {
                    try {
                        Thread.sleep(checkInterval);
                    } catch (InterruptedException e1) {
                    }
                    if (cache.size() == 0)
                        continue;
                    synchronized (lock) {
                        TreeMap<Integer, List<Entry<E>>> tmpCache = new TreeMap<Integer, List<Entry<E>>>(
                                new MaxComparator());
                        java.util.Map.Entry<Integer, List<Entry<E>>> entry = null;
                        int totalNum = 0;
                        //System.out.println(cache.size());
                        //System.out.println(kvs.size());
                        while ((entry = cache.pollFirstEntry()) != null) {
                            List<Entry<E>> vs = entry.getValue();

                            for (Entry<E> v : vs) {
                                v.clear();
                                if (v.size() != 0) {
                                    List<Entry<E>> vs0 = tmpCache.get(v.size());
                                    if (vs0 == null) {
                                        vs0 = new ArrayList<Entry<E>>();
                                        tmpCache.put(v.size(), vs0);
                                    }
                                    vs0.add(v);
                                    totalNum++;
                                } else
                                    kvs.remove(v.getK());
                            }
                        }
                        cache = tmpCache;
                        if (totalNum <= size)
                            continue;
                        int del = totalNum - size;
                        int del2 = Math.min(del * 2, size / 3 + del);
                        TreeMap<Entry<E>, Integer> tmpMap = new TreeMap<Entry<E>, Integer>(
                                new MaxComparator2());
                        int num = del2;
                        while (num > 0) {
                            java.util.Map.Entry<Integer, List<Entry<E>>> e = cache
                                    .pollLastEntry();
                            List<Entry<E>> vs = e.getValue();
                            int n = Math.min(vs.size(), num);
                            for (int i = 0; i < n; i++)
                                tmpMap.put(vs.remove(0), e.getKey());
                            num -= n;
                            if (!vs.isEmpty())
                                cache.put(e.getKey(), e.getValue());
                        }
                        int validNum = del2 - del;
                        for (int i = 0; i < validNum; i++) {
                            java.util.Map.Entry<Entry<E>, Integer> e = tmpMap
                                    .pollFirstEntry();
                            List<Entry<E>> vs0 = cache.get(e.getValue());
                            if (vs0 == null) {
                                vs0 = new ArrayList<Entry<E>>();
                                cache.put(e.getValue(), vs0);
                            }
                            vs0.add(e.getKey());
                        }
                        java.util.Map.Entry<Entry<E>, Integer> e;
                        while ((e = tmpMap.pollFirstEntry()) != null)
                            kvs.remove(e.getKey().getK());
                    }
                }
            };
        }.start();
    }

    /**
     * 
     * 在cache中增加Key的访问次数
     * 
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void add(E key) {
        synchronized (lock) {
            Entry e = kvs.get(key);
            if (e == null) {
                e = new Entry(key, new FifoList(expiredTime));
                kvs.put(key, e);
            }
            List<Entry<E>> vs = cache.get(e.size());
            if (vs != null) {
                if (!vs.isEmpty())
                    vs.remove(e);
                if (vs.isEmpty())
                    cache.remove(e.size());
            }
            e.add(Time.now());
            vs = cache.get(e.size());
            if (vs == null) {
                vs = new ArrayList<Entry<E>>();
                cache.put(e.size(), vs);
            }
            vs.add(e);
        }
    }

    /**
     * 
     * 返回并删除堆中访问次数最大的key
     * 
     */
    public E poll() {
        synchronized (lock) {
            java.util.Map.Entry<Integer, List<Entry<E>>> e = cache.firstEntry();
            if (e == null || e.getValue().isEmpty())
                return null;
            E k = (E) e.getValue().remove(0).getK();
            if (e.getValue().isEmpty())
                cache.remove(e.getKey());
            return k;
        }
    }

    /**
     *
     * 返回但不删除堆中访问次数最大的key
     *
     */
    public E peek() {
        synchronized (lock) {
            java.util.Map.Entry<Integer, List<Entry<E>>> e = cache.firstEntry();
            if (e == null || e.getValue().size() == 0)
                return null;
            return (E) e.getValue().get(0).getK();
        }
    }

    /**
     * 
     * 返回并删除堆中访问次数最大的num个key
     * 
     */
    public List<E> poll(int num) {
        synchronized (lock) {
            List<E> rs = new ArrayList<E>();
            while (num > 0) {
                java.util.Map.Entry<Integer, List<Entry<E>>> e = cache
                        .pollFirstEntry();
                if (e == null)
                    return rs;
                List<Entry<E>> vs = e.getValue();
                int n = Math.min(vs.size(), num);
                for (int i = 0; i < n; i++)
                    rs.add(vs.remove(0).getK());
                num -= n;
                if (!vs.isEmpty())
                    cache.put(e.getKey(), e.getValue());
            }
            return rs;
        }
    }

    /**
     *
     * 返回但不删除堆中访问次数最大的num个key
     *
     */
    public List<E> peek(int num) {
        synchronized (lock) {
            List<E> rs = new ArrayList<E>();
            List<java.util.Map.Entry<Integer, List<Entry<E>>>> tmp = new ArrayList<>();
            while (num > 0) {
                java.util.Map.Entry<Integer, List<Entry<E>>> e = cache
                        .pollFirstEntry();
                if (e == null)
                    return rs;
                List<Entry<E>> vs = e.getValue();
                int n = Math.min(vs.size(), num);
                for (int i = 0; i < n; i++)
                    rs.add(vs.get(i).getK());
                num -= n;
                tmp.add(e);
            }
            for (java.util.Map.Entry<Integer, List<Entry<E>>> e : tmp)
                cache.put(e.getKey(), e.getValue());
            return rs;
        }
    }

    /**
     * 
     * 将key的访问次数减一
     * 
     */
    public void removeOne(E key) {
        synchronized (lock) {
            Entry<E> e = new Entry<E>(key, null);
            java.util.Map.Entry<Integer, List<Entry<E>>> kv = cache
                    .pollFirstEntry();
            List<Entry<E>> vs = kv.getValue();
            Entry<E> entry = vs.remove(vs.indexOf(e));
            entry.remove();
            cache.put(kv.getKey(), kv.getValue());
            vs = cache.get(entry.size());
            if (vs == null) {
                vs = new ArrayList<Entry<E>>();
                cache.put(e.size(), vs);
            }
            vs.add(entry);
        }
    }
    
    /**
     * 删除key对应的统计
     * @param key
     */
    public void remove(E key) {
        synchronized(lock) {
            Entry<E> e = kvs.remove(key);
            if(e != null) {
                List<Entry<E>> vs = cache.get(e.size());
                if(vs != null)
                    vs.remove(e);
            }
                
        }
    }

    public String dump(int size) {
        StringBuilder sb = new StringBuilder();
        int i=0;
        synchronized (lock) {
            for (List<Entry<E>> e : cache.values())
                for (Entry<E> e1 : e) {
                    if(++i>size) return sb.toString();
                    sb.append(e1.getK()).append(":").append(e1.size()).append(", ");
                }
        }
        return sb.toString();
    }

    static class Entry<E> {
        private E e;
        private List<Long> v;

        Entry(E e, List<Long> v) {
            this.e = e;
            this.v = v;
        }

        void add(Long l) {
            v.add(l);
        }

        void remove() {
            v.remove(0);
        }

        int size() {
            return v.size();
        }

        E getK() {
            return e;
        }

        List<Long> getV() {
            return v;
        }

        void clear() {
            v.clear();
        }

        @SuppressWarnings("unchecked")
        public boolean equals(Object that) {
            Class<?> clazz = that.getClass();
            if (that == null || !clazz.isAssignableFrom(Entry.class))
                return false;
            if (this.e.equals(((Entry<E>) that).e))
                return true;
            return false;
        }
    }

    @SuppressWarnings("serial")
    static class FifoList extends ArrayList<Long> {
        long expiredTime;

        FifoList(long expiredTime) {
            this.expiredTime = expiredTime;
        }

        public boolean add(Long e) {
            clear();
            return super.add(e);
        }

        public void clear() {
            if(isEmpty())
                return;
            int max = size() - 1;
            int min = 0;
            int i = (min + max) / 2;
            while(i > min){
                if(expired(i))
                    min = i;
                else
                    max = i;
                i = (min + max) / 2;
            }
            if(expired(max))
                i = max;
            else if(expired(min))
                i = min;
            else
                i--;
            if(i == size() - 1)
                super.clear();
            else if(i >= 0) 
                removeRange(0, i + 1);
        }

        boolean expired(int index) {
            if (Time.now() - get(index) > expiredTime)
                return true;
            return false;
        }
    }

    @SuppressWarnings("rawtypes")
    static class MaxComparator2 implements Comparator<Entry> {
        public int compare(Entry o1, Entry o2) {
            long v2 = (long) o2.getV().get(o2.getV().size() - 1);
            long v1 = (long) o1.getV().get(o1.getV().size() - 1);
            return v1 > v2 ? -1 : v1 == v2 ? 0 : 1;
        }
    }

    static class MaxComparator implements Comparator<Integer> {
        public int compare(Integer o1, Integer o2) {
            return o1 > o2 ? -1 : o1.equals(o2) ? 0 : 1;
        }
    }
}
