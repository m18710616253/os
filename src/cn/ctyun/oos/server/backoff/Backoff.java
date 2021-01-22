package cn.ctyun.oos.server.backoff;

import java.io.File;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.http.HttpMethodName;

import cn.ctyun.common.BaseException;
import cn.ctyun.common.LinuxTrafficMonitor;
import cn.ctyun.common.conf.OOSConfig;
import cn.ctyun.common.utils.LogUtils;
import cn.ctyun.oos.common.ErrorMessage;
import cn.ctyun.oos.common.MailServer;
import cn.ctyun.oos.server.conf.BackoffConfig;
import common.tuple.Pair;

public class Backoff {
    private static AtomicInteger publicPutConnections = new AtomicInteger();
    private static AtomicInteger privatePutConnections = new AtomicInteger();
    private static AtomicInteger publicGetConnections = new AtomicInteger();
    private static AtomicInteger privateGetConnections = new AtomicInteger();
    private static AtomicInteger publicDeleteConnections = new AtomicInteger();
    private static AtomicInteger privateDeleteConnections = new AtomicInteger();
    public static AtomicInteger getConnections = new AtomicInteger();
    public static AtomicInteger putConnections = new AtomicInteger();
    public static AtomicInteger deleteConnections = new AtomicInteger();
    private static AtomicInteger totalConnections = new AtomicInteger();
    private static final Log log = LogFactory.getLog(Backoff.class);
    private static MailServer mailServer = new MailServer(OOSConfig.getSmtpHost(),
            OOSConfig.getEmailPort(), OOSConfig.getFromEmail(), OOSConfig.getEmailPassword());
    private static long bandwidthEmailLastAlarm = System.currentTimeMillis();
    
    // NOTE
    // 此处不应该在函数learnBandwidth中分配！t.getBindwidth()，需要预热1s，而每次在函数中分配不仅浪费内存，而且无法预热
    private static LinuxTrafficMonitor t = new LinuxTrafficMonitor(new File(
            BackoffConfig.getBandwidthFile()));
    
    public static boolean checkPutGetDeleteOperationsBackoff(HttpMethodName httpMethod, boolean isPublicIp) {
        switch (httpMethod) {
        case PUT:
            putConnections.incrementAndGet();
            if (isPublicIp) {
                publicPutConnections.incrementAndGet();
                LogUtils.log("public put connections:" + publicPutConnections.get());
                if (publicPutConnections.get() > BackoffConfig.getBackoffPublicPutMaxConnections()) {
                    return true;
                }
            } else {
                privatePutConnections.incrementAndGet();
                LogUtils.log("private put connections:" + privatePutConnections.get());
                if (privatePutConnections.get() > BackoffConfig
                        .getBackoffPrivatePutMaxConnections()) {
                    return true;
                }
            }
            break;
        case GET:
            getConnections.incrementAndGet();    
            if (isPublicIp) {
                publicGetConnections.incrementAndGet();
                LogUtils.log("public get connections:" + publicGetConnections.get());
                if (publicGetConnections.get() > BackoffConfig.getBackoffPublicGetMaxConnections()) {
                    return true;
                }
            } else {
                privateGetConnections.incrementAndGet();
                LogUtils.log("private get connections:" + privateGetConnections.get());
                if (privateGetConnections.get() > BackoffConfig
                        .getBackoffPrivateGetMaxConnections()) {
                    return true;
                }
            }
            break;
        case DELETE:
            deleteConnections.incrementAndGet();
            if (isPublicIp) {
                  publicDeleteConnections.incrementAndGet();
                  LogUtils.log("public delete connections:" + publicDeleteConnections.get());
                  if (publicDeleteConnections.get() > BackoffConfig.getBackoffPublicDeleteMaxConnections()) {
                      return true;
                  }
            } else {
                  privateDeleteConnections.incrementAndGet();
                  LogUtils.log("private delete connections:" + privateDeleteConnections.get());
                  if (privateDeleteConnections.get() > BackoffConfig
                          .getBackoffPrivateDeleteMaxConnections()) {
                      return true;
                  }
            }
            break;
        default:
            break;
        }
        return false;
    }
    
    public static void backoff() throws BaseException {       
        throw new BaseException(503, ErrorMessage.ERROR_CODE_SLOW_DOWN,
                ErrorMessage.ERROR_MESSAGE_SLOW_DOWN);
    }
    
    public static void decreasePutGetDeleteOperations(HttpMethodName httpMethod, boolean isPublicIp) {
        switch (httpMethod) {
        case PUT:
            if (isPublicIp)
                publicPutConnections.decrementAndGet();
            else
                privatePutConnections.decrementAndGet();
            putConnections.decrementAndGet();
            break;
        case GET:
            if (isPublicIp)
                publicGetConnections.decrementAndGet();
            else
                privateGetConnections.decrementAndGet();
            getConnections.decrementAndGet();
            break;
        case DELETE:
            if (isPublicIp)
                publicDeleteConnections.decrementAndGet();
            else
                privateDeleteConnections.decrementAndGet();
            deleteConnections.decrementAndGet();
            break;
        default:
            break;
        }
    }
    
    public static void releaseHandler() {
        totalConnections.decrementAndGet();
    }
    
    public static boolean checkHandlerBackoff() {
        totalConnections.incrementAndGet();
        LogUtils.log("total connections:" + totalConnections.get());
        if (totalConnections.get() > BackoffConfig.getBackoffMaxConnections()) {
            return true;
        } else
            return false;
    }
    
    public static BandwidthBackoff learnBandwidth() {
        if (t == null) {
            throw new RuntimeException("bandwidth is not open");
        }
        Pair<Double, Double> readPublicEth = t.getBandwidth(BackoffConfig.getReadPublicEth());
        Pair<Double, Double> writePublicEth = t.getBandwidth(BackoffConfig.getWritePublicEth());
        Pair<Double, Double> readPrivateEth = t.getBandwidth(BackoffConfig.getReadPrivateEth());
        Pair<Double, Double> writePrivateEth = t.getBandwidth(BackoffConfig.getWritePrivateEth());
        // log.info(body);
        BandwidthBackoff bandwidthBackoff = new BandwidthBackoff();
        if (readPublicEth.first() > BackoffConfig.getBackoffBandwidthPublicRxkB())
            bandwidthBackoff.readPublicBackoff = true;
        if (readPrivateEth.first() > BackoffConfig.getBackoffBandwidthPrivateRxkB())
            bandwidthBackoff.readPrivateBackoff = true;
        if (writePublicEth.second() > BackoffConfig.getBackoffBandwidthPublicTxkB())
            bandwidthBackoff.writePublicBackoff = true;
        if (writePrivateEth.second() > BackoffConfig.getBackoffBandwidthPrivateTxkB())
            bandwidthBackoff.writePrivateBackoff = true;
        if (bandwidthBackoff.readPublicBackoff || bandwidthBackoff.readPrivateBackoff
                || bandwidthBackoff.writePublicBackoff || bandwidthBackoff.writePrivateBackoff) {
            String body = "the public rxkB is:" + readPublicEth.first()
                    + "kB, the private rxkB is:" + readPrivateEth.first()
                    + "kB, the public txkB is:" + writePublicEth.second()
                    + "kB, the private txkB is:" + writePrivateEth.second() + "kB";
            log.warn("bandwidth over used:" + body);
        }
        return bandwidthBackoff;
    }
    
    public static boolean checkBandwidthBackoff(Boolean isPutObject, boolean isPublicIp,
            BandwidthBackoff bandwidthBackoff) {
        if (isPutObject) {
            if (isPublicIp) {
                if (bandwidthBackoff.readPublicBackoff)
                    return true;
            } else {
                if (bandwidthBackoff.readPrivateBackoff)
                    return true;
            }
        } else {
            if (isPublicIp) {
                if (bandwidthBackoff.writePublicBackoff)
                    return true;
            } else {
                if (bandwidthBackoff.writePrivateBackoff)
                    return true;
            }
        }
        return false;
    }
    
}
