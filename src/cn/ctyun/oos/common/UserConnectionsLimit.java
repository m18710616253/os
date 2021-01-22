package cn.ctyun.oos.common;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import cn.ctyun.common.BaseException;
import cn.ctyun.oos.server.conf.BackoffConfig;

/**
 * @author wangxs
 * 
 *         用户连接并发数限制。 使用map 记录当前用户的并发数，和backoff 配置进行比较。
 *         
 *   带宽考虑在sleep时，连接数的动态变化。 
 */
public class UserConnectionsLimit {

    private static Map<String, Map<String, Integer>> userConnectionsMap = new HashMap<String, Map<String, Integer>>();
    static {
        // remove 操作未加锁，使用concurrentHashMap防止死链。
        // classLoad时初始化map，防止高并发情况下重建和销毁产生错误。
        userConnectionsMap.put("PUT", new ConcurrentHashMap<String, Integer>());
        userConnectionsMap.put("GET", new ConcurrentHashMap<String, Integer>());
        userConnectionsMap.put("DELETE",
                new ConcurrentHashMap<String, Integer>());
    }

    /**
     * @param ownerName
     *            用户注册名，邮箱。
     * @param type
     *            请求类型，PUT GET DELETE
     */
    public static boolean allow(String ownerName, String type)
            throws BaseException {
        if (null == BackoffConfig.getUserMaxConnections().get(type))
            return false;
        BackoffConfig.UserLimit userMaxConnections = BackoffConfig
                .getUserMaxConnections().get(type).get(ownerName);
        if (null == userMaxConnections)
            return false;
        Map<String, Integer> connectionMap = userConnectionsMap.get(type);
        synchronized (userMaxConnections) {
            if (null == connectionMap.get(ownerName)
                    && userMaxConnections.getLimitElement() >= 1) {
                connectionMap.put(userMaxConnections.getUserName(), 1);
            } else if (userMaxConnections.getLimitElement() < 1 || connectionMap
                    .get(ownerName) >= userMaxConnections.getLimitElement()) {
                throw new BaseException(
                        "user connection limit backoff " + ownerName + " - "
                                + type,
                        503, ErrorMessage.ERROR_CODE_SLOW_DOWN,
                        ErrorMessage.ERROR_OVER_CONCURRENT_MESSAGE);
            } else {
                connectionMap.put(ownerName, connectionMap.get(ownerName) + 1);
            }
        }
        return true;
    }

    public static void release(String ownerName, String type) {
        // 当删除了某种类型之后，缓存将不被释放。
        if (null == BackoffConfig.getUserMaxConnections().get(type)) {
            if (null != userConnectionsMap.get(type))
                userConnectionsMap.get(type).clear();
            return;
        }

        BackoffConfig.UserLimit userMaxConnections = BackoffConfig
                .getUserMaxConnections().get(type).get(ownerName);

        if (null == userMaxConnections) {
            if (null != userConnectionsMap.get(type).get(ownerName))
                userConnectionsMap.get(type).remove(ownerName);
            return;
        }
        Map<String, Integer> connectionMap = userConnectionsMap.get(type);
        synchronized (userMaxConnections) {
            // 如果不存在，说明被其他线程clear了。 不做处理。
            if (null != connectionMap.get(ownerName))
                connectionMap.put(ownerName, connectionMap.get(ownerName) - 1);
        }
    }

}
