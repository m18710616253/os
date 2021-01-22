package cn.ctyun.oos.common;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import cn.ctyun.common.BaseException;
import cn.ctyun.oos.server.conf.BackoffConfig;
import cn.ctyun.oos.server.conf.BackoffConfig.UserLimit;

/**
 * 1 定义一个新的inputSteam ，增加一个ownername字段。在read方法里进行累加，到一定的值后put到中控类
 * UserBandWidthLimit。 2 使用滑动集合来保存上报上来的容量值，新请求进来后，计算当前时间段内的带宽，如果超限。将backoff。 3
 * 每次上报都按秒计算，每一秒生成一个数据， 然后同秒内的容量累加。 backoff 的线程也是使用当前时间戳取秒获取当前容量。 做成可配置的
 */
public class UserBandWidthLimit {

    private static Map<String, Map<String, Map<Long, LongAdder>>> userBandWidthMap = new HashMap<String, Map<String, Map<Long, LongAdder>>>();

    private static final Log log = LogFactory.getLog(UserBandWidthLimit.class);

    static {
        // remove 操作未加锁，使用concurrentHashMap防止死链。
        // classLoad时初始化map，防止高并发情况下重建和销毁产生错误。
        userBandWidthMap.put("PUT",
                new ConcurrentHashMap<String, Map<Long, LongAdder>>());
        userBandWidthMap.put("GET",
                new ConcurrentHashMap<String, Map<Long, LongAdder>>());
    }

    /**
     * 过滤本次请求，如果backoff，抛出异常。
     * 
     * @param ownerName
     *            用户名
     * @param type
     *            请求类型
     *
     * @return 是否需要加入带宽限制。true 加入，false 不加入
     */
    public static boolean allow(String ownerName, String type)
            throws BaseException {
        if (null == BackoffConfig.getUserMaxBandWidth().get(type))
            return false;
        UserLimit userLimit;
        if (null == (userLimit = BackoffConfig.getUserMaxBandWidth().get(type)
                .get(ownerName)))
            return false;
        // 可能会有zk刚去掉了限制，但是代码已经走到此处。
        Map<Long, LongAdder> userBandwidth = userBandWidthMap.get(type)
                .get(ownerName);
        // 如果不存在此类型的限制，重建它。
        if (null == userBandwidth) {
            userBandwidth = userBandWidthMap.get(type).computeIfAbsent(
                    ownerName, e -> new ConcurrentHashMap<Long, LongAdder>());
        }
        long second = getTimeStamp();
        //取前1s和当前ms时间内的容量平均值。
        //B
        double sum = 0;
        //ms
        double time = 0;
        if (null != userBandwidth.get(second - 1)) {
            sum = userBandwidth.get(second - 1).sum();
            time = 1000; 
        }
        if(null != userBandwidth.get(second)) {
            time += System.currentTimeMillis() - second * 1000;
            sum += userBandwidth.get(second).sum();
        }

        if(time == 0)
            return true;

        //M/s
        double average = (sum / 1024 / 1024) / (time / 1000); 
        
        // 配置文件单位是M/s， 此处注意转换单位 backoff 的详细说明
        if (userLimit.getLimitElement() <= average)
            throw new BaseException(
                    "band width limit backoff " + ownerName + " - " + type, 503,
                    ErrorMessage.ERROR_CODE_SLOW_DOWN,
                    ErrorMessage.ERROR_OVER_BANDWIDTH_MESSAGE);
        return true;
    }

    /**
     * 释放资源
     * 
     * @todo 有用户累加不被删除的可能。
     */
    public static void release(String ownerName, String type) {
        try {
            if (null == userBandWidthMap.get(type))
                return;
            Map<Long, LongAdder> userLimits;
            // 如果zk上不再配置这个用户了。 需要销毁
            if (null == (userLimits = userBandWidthMap.get(type)
                    .get(ownerName)))
                return;
            if (null == (BackoffConfig.getUserMaxBandWidth().get(type)
                    .get(ownerName))) {
                userBandWidthMap.get(type).remove(ownerName);
                return;
            }
            long second = getTimeStamp();
            // 将所有当前时间之前的数据删除
            userLimits.forEach((k, v) -> {
                if (k < (second - 1)) {
                    userLimits.remove(k);
                }
            });
        } catch (Throwable e) {
            // 捕获极端情况下可能出现的异常，此异常不应抛出。程序应该正确结束
            log.error(e.getMessage(), e);
        }
    }

    /**
     * 对于加入带宽限制的请求，上报自己当前使用的带宽数。
     * 
     * @todo 调查下每次read了多少字节
     */
    public static void increaseFlow(String ownerName, String type, long flow) {
        long second = getTimeStamp();
        Map<String, Map<Long, LongAdder>> userBandwidth = userBandWidthMap
                .get(type);
        // 顺次判断，防止空指针.如果是空值，说明zk上这个用户的限制解除了。 即不受限，不用记录带宽
        if (null != userBandwidth) {
            Map<Long, LongAdder> bandwidth = userBandwidth.get(ownerName);
            if (null != bandwidth) {
                LongAdder totalFlow = bandwidth.get(second);
                if (null == totalFlow) {
                    totalFlow = bandwidth.computeIfAbsent(second,
                            e -> new LongAdder());
                }
                totalFlow.add(flow);
            }
        }
    }

    private static long getTimeStamp() {
        long now = System.currentTimeMillis();
        return now / 1000;// 可配置
    }

}
