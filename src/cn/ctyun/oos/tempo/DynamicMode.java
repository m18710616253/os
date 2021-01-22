package cn.ctyun.oos.tempo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import cn.ctyun.common.ReplicaMode;
import common.tuple.Pair;

/**
 * pageSize=512k
 * 两地资源池都不能有单副本
 * 两地资源池要达到11个9
 * @author jiazg
 *
 */
public class DynamicMode {

    private Log log = LogFactory.getLog(DynamicMode.class);
    private Map<String, Integer> modes = new HashMap<>();
    private Pair<String, Integer> ec_1_0 = new Pair<>("EC_1_0", 5);
    public DynamicMode(Map<String, Integer> modes){
        this.modes = modes;
    }
    
    /**
     * 双资源池，存储<tt>total<tt>大小数据，在持久度大于等于<tt>durability<tt>的情况下，获取最优副本模式
     * @param total       有效数据大小
     * @param pageSize    page大小
     * @param durability  持久度
     * @param existSingle 是否有一个资源池是单副本，true:存在，false:不存在
     * @return 最优副本模式，以及该模式下实际存储大小
     */
    public Pair<Pair<String, String>, Long> getOptimizedModes(long total, long pageSize, 
            int durability, boolean existSingle){
        Pair<Pair<String, String>, Long> r = null;
        Pair<String, String> pair = null;
        long min = Long.MAX_VALUE;
        List<Pair<String, String>> list = modesFilter(durability, existSingle);
        for(Pair<String, String> p : list){
            long s = getCapacity(total, pageSize, p.first());
            s += getCapacity(total, pageSize, p.second());
            if(s < min || ((null != pair) && (null != p) && (s == min) 
                    && (getDurability(p.first()) + getDurability(p.second()) > 
                    getDurability(pair.first()) + getDurability(pair.second())))){
                min = s; 
                pair = p;
            }
        }
        if(null != pair){
            r = new Pair<>(pair, min);
            log.info(pair.first() + ", " + pair.second() + ", min:" + min + ", total:" + total);
        }
        return r;
    }
    
    /**
     * 根据<tt>mode<tt>模式，获取该模式的持久度
     * @param mode  副本模式
     * @return 如果不存在该模式返回null，否则返回对应的持久度
     */
    private int getDurability(String mode){
        Integer d = null;
        if(null == (d = modes.get(mode)) && mode.equals(ec_1_0.first()))
            d = ec_1_0.second();
        return d;
    }
    
    /**
     * 单资源池，存储<tt>total<tt>大小数据，在持久度大于等于<tt>durability<tt>的情况下，获取最优副本模式
     * @param total       有效数据大小
     * @param pageSize    page大小
     * @param durability  持久度
     * @return 最优副本模式，以及该模式下实际存储大小
     */
    public Pair<String, Long> getOptimizedMode(long total, long pageSize, 
            int durability){
        Pair<String, Long> r = null;
        String mode = null;
        long min = Long.MAX_VALUE;
        for(Map.Entry<String, Integer> e : modes.entrySet()){
            int d = e.getValue();
            if(d < durability)
                continue;
            String m = e.getKey();
            long s = getCapacity(total, pageSize, m);
            if(s < min || ((s == min) &&  d > getDurability(mode))){
                min = s; 
                mode = m;
            }
        }
        
        if(null != mode){
            r = new Pair<>(mode, min);
            log.info(mode + ", min:" + min + ", total:" + total);
        }
        return r;
    }
    
    /**
     * 双资源池中，过滤出持久度大于等于<tt>durability<tt>的副本模式组合列表
     * @param durability   持久度
     * @param existSingle  是否有一个资源池是单副本，true:存在，false:不存在
     * @return 满足要求的副本模式组合列表
     */
    private List<Pair<String, String>> modesFilter(int durability, boolean existSingle){
        
        List<Pair<String, String>> list = new ArrayList<>();
        Set<String> ms = modes.keySet();
        if(existSingle){
            for(String m : ms){
                if(modes.get(m) + ec_1_0.second() - 1 >= durability){
                    list.add(new Pair<String, String>(m, ec_1_0.first()));
                }
            }
        }else{
            Set<String> ms_1 = new HashSet<>(ms);
            for(String m : ms){
                for(String m1 : ms_1){
                    if(modes.get(m) + modes.get(m1) - 1 >= durability){
                        list.add(new Pair<String, String>(m, m1));
                    }
                }
                ms_1.remove(m);
            }
        }
        return list;
    }
    
    /**
     * 按照<tt>mode<tt>模式去存储<tt>total<tt>的数据，得到实际占用空间大小
     * @param total      有效数据大小
     * @param pageSize   page大小
     * @param mode       副本模式
     * @return 实际占用空间大小
     */
    private long getCapacity(long total, long pageSize, String mode){
        
        ReplicaMode m = ReplicaMode.parser(mode);
        long cp = 0L;
        // 简单副本
        if(m.ec_m == 0){
            long pageNum = total / pageSize;
            pageNum = (total % pageSize > 0) ? pageNum + 1 : pageNum;
            if(pageNum > 1){
                long lastSize = total - (pageSize * (pageNum - 1));
                cp += ((pageNum - 1) * pageSize + lastSize) * m.ec_n;
            }else{
                cp = total * m.ec_n;
            }
        }else{
        // EC模式
            long groupSize = m.ec_n * pageSize;
            long pageNum = total / groupSize;
            pageNum = (total % groupSize > 0) ? pageNum + 1 : pageNum;
            cp = pageNum * (m.ec_m + m.ec_n) * pageSize;
        }
        return cp;
    }
    
    /**
     * 双资源池，在满足<tt>durability<tt>的情况下，得到实际容量与有效容量的比值
     * @param map          大小比例分布
     * @param pageSize     page大小
     * @param durability   持久度
     * @param existSingle  是否有一个资源池是单副本，true:存在，false:不存在
     * @return 实际容量与有效容量的比值
     */
    public double getDoublePoolRatio(Map<Long, Integer> map, long pageSize, 
            int durability, boolean existSingle){
        
        long source = 0L;
        long target = 0L;
        for(Map.Entry<Long, Integer> e : map.entrySet()){
            long size = e.getKey();
            int per = e.getValue();
            source += size * per;
            target += getOptimizedModes(size, pageSize, durability, existSingle).second() * per;
        }
        log.info("DoublePool: existSingle:" + existSingle + ", source:" + source + ", target:" + target);
        if(source == 0)
            return 0;
        return (double)target / source;
    }
    
    /**
     * 单资源池，在满足<tt>durability<tt>的情况下，得到实际容量与有效容量的比值
     * @param map         大小比例分布
     * @param pageSize    page大小
     * @param durability  持久度
     * @return 实际容量与有效容量的比值
     */
    public double getSinglePoolRatio(Map<Long, Integer> map, long pageSize, 
            int durability){
        
        long source = 0L;
        long target = 0L;
        for(Map.Entry<Long, Integer> e : map.entrySet()){
            long size = e.getKey();
            int per = e.getValue();
            source += size * per;
            target += getOptimizedMode(size, pageSize, durability).second() * per;
        }
        log.info("SinglePool: source:" + source + ", target:" + target);
        if(source == 0)
            return 0;
        return (double)target / source;
    }
    
    public static void main(String[] args) {
        
        long KB = 1024;
        long MB = KB * KB;
        Map<String, Integer> modes = new HashMap<>();
        modes.put("EC_9_3", 12);
        modes.put("EC_3_0", 12);
        modes.put("EC_2_1", 8);
        DynamicMode dm = new DynamicMode(modes);
        Map<Long, Integer> map = new HashMap<>();
        map.put(4 * KB, 5);
        map.put(8 * KB, 1);
        map.put(16 * KB, 2);
        map.put(32 * KB, 6);
        map.put(64 * KB, 10);
        map.put(128 * KB, 7);
        map.put(256 * KB, 8);
        map.put(512 * KB, 10);
        map.put(1 * MB, 12);
        map.put(2 * MB, 28);
        map.put(4 * MB, 6);
        map.put(12 * MB, 1);
        map.put(24 * MB, 4);
        System.out.println(dm.getDoublePoolRatio(map, 512 * KB, 11, false));
        System.out.println(dm.getDoublePoolRatio(map, 512 * KB, 11, true));
    }
}
