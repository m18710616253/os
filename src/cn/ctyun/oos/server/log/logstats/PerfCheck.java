package cn.ctyun.oos.server.log.logstats;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.DoubleAdder;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.HttpMethod;
import com.google.common.util.concurrent.AtomicDouble;

import cn.ctyun.common.conf.DataRegion;
import cn.ctyun.common.conf.OOSConfig;
import cn.ctyun.common.ganglia.GangliaConsts;
import cn.ctyun.common.ganglia.GangliaMan;
import cn.ctyun.common.ganglia.GangliaPlugin;
import cn.ctyun.common.utils.HostUtils;
import cn.ctyun.oos.server.conf.LogConfig;
import cn.ctyun.oos.server.log.BucketLog;
import common.math.StandardDeviation;

//对所有log进行分析,得到性能指标，发送到Ganglia
public class PerfCheck {
    private static Log log = LogFactory.getLog(PerfCheck.class);
    private static String strPutObject = "REST." + HttpMethod.PUT.toString() + ".OBJECT";
    private static String strGetObject = "REST." + HttpMethod.GET.toString() + ".OBJECT";
    private static long size1M = 1024 * 1024L;
    AtomicLong putRequestNum = new AtomicLong(0L);
    AtomicLong getRequestNum = new AtomicLong(0L);
    AtomicLong totalRequestNum = new AtomicLong(0L);
    AtomicLong writeBucketLogNum = new AtomicLong(0L);
    AtomicLong costTime = new AtomicLong(0L);
    AtomicLong preProcessTime = new AtomicLong(0L);
    AtomicLong putMeta = new AtomicLong(0L);
    AtomicLong getMeta = new AtomicLong(0L);
    AtomicLong putTime = new AtomicLong(0L);
    AtomicLong getTime = new AtomicLong(0L);
    AtomicLong putDiffTime = new AtomicLong(0L);
    AtomicLong getDiffTime = new AtomicLong(0L);
    AtomicLong pinTime = new AtomicLong(0L);
    AtomicLong prepareLogTime = new AtomicLong(0L);
    AtomicLong writeBucketLogTime = new AtomicLong(0L);
    // last mail time
    long lastPutMetaAlarmTime = -1L;
    long lastGetMetaAlarmTime = -1L;
    // 最近是否告过警
    boolean lastAlarmPutMeta = false;
    boolean lastAlarmGetMeta = false;

    AtomicLong putMetaMaxTime = new AtomicLong(-1L);
    AtomicLong getMetaMaxTime = new AtomicLong(-1L);
    AtomicLong putMetaMinTime = new AtomicLong(Long.MAX_VALUE);
    AtomicLong getMetaMinTime = new AtomicLong(Long.MAX_VALUE);
    AtomicDouble putTimeMillSecPerM = new AtomicDouble(0.0);
    AtomicDouble getTimeMillSecPerM = new AtomicDouble(0.0);
    AtomicLong illgalPutNum = new AtomicLong(0L);
    AtomicLong illgalGetNum = new AtomicLong(0L);
    StandardDeviation stdPutMeta = new StandardDeviation();
    StandardDeviation stdGetMeta = new StandardDeviation();
    
    //记录每个节点的putget时间
    OOSPutGetAvgTimePerNode oosPutGetTimePerNode = new OOSPutGetAvgTimePerNode();

    // get object时，返回客户端200响应码，但是在向客户端发送响应体过程中，产生错误的数量统计
    // 从ostor中读数据时的错误数量统计
    AtomicLong responseGet200_exception_5 = new AtomicLong(0L);
    // 向客户端写数据的错误数量统计
    AtomicLong responseGet200_exception_8 = new AtomicLong(0L);
    // 其他错误
    AtomicLong responseGet200_exception_other = new AtomicLong(0L);

    String id = String.valueOf(LogConfig.getId(HostUtils.getHostName()));
    String gangliaFakeIp = GangliaMan.getFakeIpHost(GangliaMan.OOS_GROUP_NAME);

    public void parseLogLine(BucketLog logLine, String hostName) {
        if (logLine.operation != null && (logLine.operation.equals(strPutObject) || logLine.operation.equals(strGetObject))) {
            if (logLine.operation.equals(strGetObject)) {
                // get object时，返回客户端200响应码，但是在向客户端发送响应体过程中，产生错误的数量统计
                if (logLine.exception == 5 || logLine.exception == 7)
                    responseGet200_exception_5.incrementAndGet();
                if (logLine.exception == 8)
                    responseGet200_exception_8.incrementAndGet();
                if (logLine.exception == 1)
                    responseGet200_exception_other.incrementAndGet();
            }
            if (!check(logLine))
                return;
            long len = -1L;
            if(logLine.contentLength != null && logLine.contentLength.length() != 0 && !logLine.contentLength.equalsIgnoreCase("-")){
                try {
                    len = Long.parseLong(logLine.contentLength);
                } catch (NumberFormatException e) {
                    log.error("NumberFormatException ", e);
                }
            }
            costTime.addAndGet(logLine.endTime);// endTime已经改为与startTime的相对时间
            preProcessTime.addAndGet(logLine.preProcessTime);
            pinTime.addAndGet(logLine.pinTime);
            prepareLogTime.addAndGet(logLine.prepareLogTime);
            if (logLine.writeBucketLogTime != 0) {
                writeBucketLogTime.addAndGet(logLine.writeBucketLogTime);
                writeBucketLogNum.incrementAndGet();
            }
            if (logLine.operation.equals(strPutObject)) {
                putTime.addAndGet(logLine.ostorResponseStartTime - logLine.adapterRequestStartTime);
                putMeta.addAndGet(logLine.putMetaTime);
                putRequestNum.incrementAndGet();
                putDiffTime.addAndGet(logLine.endTime - logLine.preProcessTime - logLine.getMetaTime - logLine.putMetaTime-logLine.pinTime-(logLine.ostorResponseStartTime - logLine.adapterRequestStartTime)-logLine.prepareLogTime-logLine.writeBucketLogTime);

                synchronized (putMetaMaxTime) {
                    if (logLine.putMetaTime > putMetaMaxTime.get()) {
                        putMetaMaxTime.set(logLine.putMetaTime);
                    }
                }
                synchronized (putMetaMinTime) {
                    if (logLine.putMetaTime < putMetaMinTime.get()) {
                        putMetaMinTime.set(logLine.putMetaTime);
                    }
                }
                synchronized (stdPutMeta) {
                    stdPutMeta.add(logLine.putMetaTime);
                }
                if (len != -1 && len != 0) {
                    oosPutGetTimePerNode.addPutTimeAndSize(hostName, (double) (logLine.adapterResponseStartTime - logLine.clientRequestFirstTime), len);
                    if (logLine.status >= 200 && logLine.status <= 299 && len >= size1M && logLine.exception != 3) {
                        illgalPutNum.incrementAndGet();
                        putTimeMillSecPerM.addAndGet(((double)(logLine.ostorResponseStartTime - logLine.adapterRequestStartTime)) / (len / size1M));
                    }
                }
            } else {
                getTime.addAndGet(logLine.ostorResponseLastTime - logLine.adapterRequestStartTime);
                getRequestNum.incrementAndGet();
                getDiffTime.addAndGet(logLine.endTime - logLine.preProcessTime - logLine.getMetaTime - logLine.putMetaTime-logLine.pinTime-( logLine.ostorResponseLastTime - logLine.adapterRequestStartTime)-logLine.prepareLogTime-logLine.writeBucketLogTime);

                if (len != -1 && len != 0) {
                    oosPutGetTimePerNode.addGetTimeAndSize(hostName, (double) (logLine.adapterResponseLastTime - logLine.clientRequestLastTime), len);
                    if (logLine.status >= 200 && logLine.status <= 299 && len >= size1M && logLine.exception != 2 && logLine.exception != 1 && logLine.exception != 5) {
                        illgalGetNum.incrementAndGet();
                        getTimeMillSecPerM.addAndGet(((double) (logLine.ostorResponseLastTime - logLine.adapterRequestStartTime)) / (len / size1M));
                    }
                }
            }

            getMeta.addAndGet(logLine.getMetaTime);
            synchronized (getMetaMaxTime) {
                if (logLine.getMetaTime > getMetaMaxTime.get()) {
                    getMetaMaxTime.set(logLine.getMetaTime);
                }
            }
            synchronized (getMetaMinTime) {
                if (logLine.getMetaTime < getMetaMinTime.get()) {
                    getMetaMinTime.set(logLine.getMetaTime);
                }
            }
            synchronized (stdGetMeta) {
                stdGetMeta.add(logLine.getMetaTime);
            }

            totalRequestNum.incrementAndGet();
        }
    }

    public void sendToGanglia() {
        int costTimeAver = totalRequestNum.get() != 0 ? (int) (costTime.get() / totalRequestNum.get()) : 0;// 取整数
        int preProcessTimeAver = totalRequestNum.get() != 0 ? (int) (preProcessTime.get() / totalRequestNum.get()) : 0;
        final int putMetaAver = putRequestNum.get() != 0 ? (int) (putMeta.get() / putRequestNum.get()) : 0;
        final int getMetaAver = getRequestNum.get() != 0 ? (int) (getMeta.get() / (getRequestNum.get() + putRequestNum.get())) : 0;
        int putTimeAver = putRequestNum.get() != 0 ? (int) (putTime.get() / putRequestNum.get()) : 0;
        int getTimeAver = getRequestNum.get() != 0 ? (int) (getTime.get() / getRequestNum.get()) : 0;
        int putDiffTimeAver = putRequestNum.get() != 0 ? (int) (putDiffTime.get() / putRequestNum.get()) : 0;
        int getDiffTimeAver = getRequestNum.get() != 0 ? (int) (getDiffTime.get() / getRequestNum.get()) : 0;
        int pinTimeAver = totalRequestNum.get() != 0 ? (int) (pinTime.get() / totalRequestNum.get()) : 0;
        int prepareLogTimeAver = totalRequestNum.get() != 0 ? (int) (prepareLogTime.get() / totalRequestNum.get()) : 0;
        int writeBucketLogTimeAver = writeBucketLogNum.get() != 0 ? (int) (writeBucketLogTime.get() / writeBucketLogNum.get()) : 0;

        double putTimeAverMillSecPerM = illgalPutNum.get() != 0 ? (putTimeMillSecPerM.get() / illgalPutNum.get()) : 0;
        double getTimeAverMillSecPerM = illgalGetNum.get() != 0 ? (getTimeMillSecPerM.get() / illgalGetNum.get()) : 0;

        GangliaPlugin.sendToGangliaWithFakeIp(GangliaConsts.TOTAL_PUT_GET_AVERAGE_TIME + "_" + id, String.valueOf(costTimeAver), GangliaConsts.YTYPE_INT32, GangliaConsts.YNAME_MILLISECOND, GangliaConsts.GROUP_NAME_LOG_PUT_GET_TIME, "\"In the past five minutes, the average time of total put and get object time\"", gangliaFakeIp);
        GangliaPlugin.sendToGangliaWithFakeIp(GangliaConsts.PRE_PROCESS_AVERAGE_TIME + "_" + id, String.valueOf(preProcessTimeAver), GangliaConsts.YTYPE_INT32, GangliaConsts.YNAME_MILLISECOND, GangliaConsts.GROUP_NAME_LOG_PUT_GET_TIME, "\"In the past five minutes, the average time of check auth time\"", gangliaFakeIp);
        GangliaPlugin.sendToGangliaWithFakeIp(GangliaConsts.PUT_AVERAGE_TIME + "_" + id, String.valueOf(putTimeAver), GangliaConsts.YTYPE_INT32, GangliaConsts.YNAME_MILLISECOND, GangliaConsts.GROUP_NAME_LOG_PUT_GET_TIME,"\"In the past five minutes, the average time of put object, from (start connection to ostor time) to (ostor response time)\"", gangliaFakeIp);
        GangliaPlugin.sendToGangliaWithFakeIp(GangliaConsts.GET_AVERAGE_TIME + "_" + id, String.valueOf(getTimeAver), GangliaConsts.YTYPE_INT32, GangliaConsts.YNAME_MILLISECOND, GangliaConsts.GROUP_NAME_LOG_PUT_GET_TIME,"\"In the past five minutes, the average time of get object, from (start connection to ostor time) to (ostor response last byte time)\"", gangliaFakeIp);
        GangliaPlugin.sendToGangliaWithFakeIp(GangliaConsts.PUT_DIFF_AVERAGE_TIME + "_" + id, String.valueOf(putDiffTimeAver), GangliaConsts.YTYPE_INT32, GangliaConsts.YNAME_MILLISECOND, GangliaConsts.GROUP_NAME_LOG_PUT_GET_TIME,"\"In the past five minutes, the average time of not statistic time when put object\"", gangliaFakeIp);
        GangliaPlugin.sendToGangliaWithFakeIp(GangliaConsts.GET_DIFF_AVERAGE_TIME + "_" + id, String.valueOf(getDiffTimeAver), GangliaConsts.YTYPE_INT32, GangliaConsts.YNAME_MILLISECOND, GangliaConsts.GROUP_NAME_LOG_PUT_GET_TIME,"\"In the past five minutes, the average time of not statistic time when get object\"", gangliaFakeIp);
        GangliaPlugin.sendToGangliaWithFakeIp(GangliaConsts.PIN_AVERAGE_TIME + "_" + id, String.valueOf(pinTimeAver), GangliaConsts.YTYPE_INT32, GangliaConsts.YNAME_MILLISECOND, GangliaConsts.GROUP_NAME_LOG_PUT_GET_TIME,"\"In the past five minutes, the average time of put request num, storage, flow... to mysql time\"", gangliaFakeIp);
        GangliaPlugin.sendToGangliaWithFakeIp(GangliaConsts.PUT_AVERAGE_TIME_PER_M + "_" + id, String.valueOf((int)putTimeAverMillSecPerM), GangliaConsts.YTYPE_INT32, GangliaConsts.YNAME_MILLISECOND, GangliaConsts.GROUP_NAME_LOG_PUT_GET_TIME,"\"In the past five minutes,The average Put Meta Time per MB\"", gangliaFakeIp);
        GangliaPlugin.sendToGangliaWithFakeIp(GangliaConsts.GET_AVERAGE_TIME_PER_M + "_" + id, String.valueOf((int)getTimeAverMillSecPerM), GangliaConsts.YTYPE_INT32, GangliaConsts.YNAME_MILLISECOND, GangliaConsts.GROUP_NAME_LOG_PUT_GET_TIME,"\"In the past five minutes,The average Get Meta Time per MB\"", gangliaFakeIp);
        GangliaPlugin.sendToGangliaWithFakeIp(GangliaConsts.WRITE_BUCKET_LOG_AVERAGE_TIME + "_" + id, String.valueOf(writeBucketLogTimeAver), GangliaConsts.YTYPE_INT32, GangliaConsts.YNAME_MILLISECOND, GangliaConsts.GROUP_NAME_LOG_PUT_GET_TIME,"\"In the past five minutes,The average write bucket log time\"", gangliaFakeIp);
        GangliaPlugin.sendToGangliaWithFakeIp(GangliaConsts.PREPARE_LOG_AVERAGE_TIME + "_" + id, String.valueOf(prepareLogTimeAver), GangliaConsts.YTYPE_INT32, GangliaConsts.YNAME_MILLISECOND, GangliaConsts.GROUP_NAME_LOG_PUT_GET_TIME,"\"In the past five minutes,The average prepare log time\"", gangliaFakeIp);
        // 返回客户端200响应码，但是在向客户端发送响应体过程中，产生错误的数量统计
        GangliaPlugin.sendToGangliaWithFakeIp(GangliaConsts.RESPONSE_CODE_GET_200_BUT_READ_FROM_OSTOR_ERROR + "_" + id, String.valueOf(responseGet200_exception_5.get()), GangliaConsts.YTYPE_INT32, GangliaConsts.YNAME_NUM, GangliaConsts.GROUP_NAME_LOG_RESPONSE_CODE,"\"In the past five minutes, num of response code get 200 but read from ostor error\"", gangliaFakeIp);
        GangliaPlugin.sendToGangliaWithFakeIp(GangliaConsts.RESPONSE_CODE_GET_200_BUT_WRITE_TO_CLIENT_ERROR + "_" + id, String.valueOf(responseGet200_exception_8.get()), GangliaConsts.YTYPE_INT32, GangliaConsts.YNAME_NUM, GangliaConsts.GROUP_NAME_LOG_RESPONSE_CODE,"\"In the past five minutes, num of response code get 200 but write to client error\"", gangliaFakeIp);
        GangliaPlugin.sendToGangliaWithFakeIp(GangliaConsts.RESPONSE_CODE_GET_200_OTHER_ERROR + "_" + id, String.valueOf(responseGet200_exception_other.get()), GangliaConsts.YTYPE_INT32, GangliaConsts.YNAME_NUM, GangliaConsts.GROUP_NAME_LOG_RESPONSE_CODE,"\"In the past five minutes,num of response code get 200 but occur other error\"", gangliaFakeIp);

        //将每个节点的发送到ganglia
        oosPutGetTimePerNode.sendToGanglia();

        if (putMetaMinTime.get() == Long.MAX_VALUE) {
            putMetaMinTime.set(0L);
        }
        if (getMetaMinTime.get() == Long.MAX_VALUE) {
            getMetaMinTime.set(0L);
        }
        if (putMetaMaxTime.get() == -1L) {
            putMetaMaxTime.set(0L);
        }
        if (getMetaMaxTime.get() == -1L) {
            getMetaMaxTime.set(0L);
        }

        GangliaPlugin.sendToGangliaWithFakeIp(GangliaConsts.PUT_META_MAX_TIME + "_" + id, String.valueOf(putMetaMaxTime.get()), GangliaConsts.YTYPE_INT32, GangliaConsts.YNAME_MILLISECOND, GangliaConsts.GROUP_NAME_LOG_PUT_GET_META_TIME,"\"In the past five minutes,The Max Put Meta Time\"", gangliaFakeIp);
        GangliaPlugin.sendToGangliaWithFakeIp(GangliaConsts.GET_META_MAX_TIME + "_" + id, String.valueOf(getMetaMaxTime.get()), GangliaConsts.YTYPE_INT32, GangliaConsts.YNAME_MILLISECOND, GangliaConsts.GROUP_NAME_LOG_PUT_GET_META_TIME,"\"In the past five minutes,The Max Get Meta Time\"", gangliaFakeIp);
        GangliaPlugin.sendToGangliaWithFakeIp(GangliaConsts.PUT_META_MIN_TIME + "_" + id, String.valueOf(putMetaMinTime.get()), GangliaConsts.YTYPE_INT32, GangliaConsts.YNAME_MILLISECOND, GangliaConsts.GROUP_NAME_LOG_PUT_GET_META_TIME,"\"In the past five minutes,The Min Put Meta Time\"", gangliaFakeIp);
        GangliaPlugin.sendToGangliaWithFakeIp(GangliaConsts.GET_META_MIN_TIME + "_" + id, String.valueOf(getMetaMinTime.get()), GangliaConsts.YTYPE_INT32, GangliaConsts.YNAME_MILLISECOND, GangliaConsts.GROUP_NAME_LOG_PUT_GET_META_TIME,"\"In the past five minutes,The Min Get Meta Time\"", gangliaFakeIp);
        GangliaPlugin.sendToGangliaWithFakeIp(GangliaConsts.PUT_META_STD_TIME + "_" + id, String.valueOf((long)stdPutMeta.getDeviation()), GangliaConsts.YTYPE_INT32, GangliaConsts.YNAME_MILLISECOND, GangliaConsts.GROUP_NAME_LOG_PUT_GET_META_TIME,"\"In the past five minutes,The Standard Devaition Put Meta Time\"", gangliaFakeIp);
        GangliaPlugin.sendToGangliaWithFakeIp(GangliaConsts.GET_META_STD_TIME + "_" + id, String.valueOf((long)stdGetMeta.getDeviation()), GangliaConsts.YTYPE_INT32, GangliaConsts.YNAME_MILLISECOND, GangliaConsts.GROUP_NAME_LOG_PUT_GET_META_TIME,"\"In the past five minutes,The Standard Devaition Get Meta Time\"", gangliaFakeIp);
        GangliaPlugin.sendToGangliaWithFakeIp(GangliaConsts.PUT_META_AVERAGE_TIME + "_" + id, String.valueOf(putMetaAver), GangliaConsts.YTYPE_INT32, GangliaConsts.YNAME_MILLISECOND, GangliaConsts.GROUP_NAME_LOG_PUT_GET_META_TIME,"\"In the past five minutes, the average time of put object to hbase\"", gangliaFakeIp);
        GangliaPlugin.sendToGangliaWithFakeIp(GangliaConsts.GET_META_AVERAGE_TIME + "_" + id, String.valueOf(getMetaAver), GangliaConsts.YTYPE_INT32, GangliaConsts.YNAME_MILLISECOND, GangliaConsts.GROUP_NAME_LOG_PUT_GET_META_TIME,"\"In the past five minutes, the average time of get object from hbase\"", gangliaFakeIp);

        log.info(String.format("Perf Data:hostID:%s,putRequestNum:%d,getRequestNum:%d,costTimeAver:%d,preProcessTimeAver:%d,putMetaAver:%d,getMetaAver:%d,putTimeAver:%d,getTimeAver:%d,putDiffTimeAver:%d,getDiffTimeAver:%d,pinTimeAver:%d,putMetaMaxTime:%d,getMetaMaxTime:%d,putMetaMinTime:%d,getMetaMinTime:%d,stdPutMeta:%f,stdGetMeta:%f,putTimePerKAver:%f,getTimePerKAver:%f,prepareLogTimeAver:%d,writeBucketLogTimeAver:%d,getMeta:%d,putMeta:%d",
              id,putRequestNum.get(), getRequestNum.get(), costTimeAver, preProcessTimeAver, putMetaAver, getMetaAver, putTimeAver,getTimeAver, putDiffTimeAver, getDiffTimeAver,pinTimeAver,putMetaMaxTime.get(),getMetaMaxTime.get(),putMetaMinTime.get(),getMetaMinTime.get(),stdPutMeta.getDeviation(),stdGetMeta.getDeviation(),putTimeAverMillSecPerM,getTimeAverMillSecPerM,prepareLogTimeAver,writeBucketLogTimeAver,getMeta.get(),putMeta.get()));

        long now = System.currentTimeMillis();
        // PutMeta耗时过长告警与恢复
        final long putMetaAlarmLimit = LogConfig.getPutMetaAlarmLimit();
        if (lastAlarmPutMeta && putMetaAver < putMetaAlarmLimit) {// 恢复正常，发邮件通知
            lastAlarmPutMeta = false;
            String body = String.format("Recovery.In the Past Five Minutes,%s LogServerNode %s Put Metadata Averagely takes %d MilliSeconds and Upper Bound is %d", DataRegion.getRegion().getName(), id, putMetaAver, putMetaAlarmLimit); 
            String info = String.format("%s: %s", "Put Meta Cost Time Recovery", body);
            log.warn(info);
        }
        if(putMetaAver >= putMetaAlarmLimit && (lastPutMetaAlarmTime == -1 || (now - lastPutMetaAlarmTime >= OOSConfig.getEmailInterval())) ){
            lastPutMetaAlarmTime = now;
            lastAlarmPutMeta = true;
            String body = String.format("Alarm.In the Past Five Minutes, %s LogServerNode %s Put Metadata Averagely takes %d MilliSeconds which exceeds Upper Bound %d", DataRegion.getRegion().getName(),id, putMetaAver, putMetaAlarmLimit); 
            String info = String.format("%s: %s", "Put Meta Cost Time Alarm", body);
            log.warn(info);
        }

        // GetMeta耗时过长告警与恢复
        final long getMetaAlarmLimit = LogConfig.getGetMetaAlarmLimit();
        if (lastAlarmGetMeta && getMetaAver < getMetaAlarmLimit) {// 恢复正常，发邮件通知
            lastAlarmGetMeta = false;
            String body = String.format("Recovery.In the Past Five Minutes, %s LogServerNode %s Get Metadata Averagely takes %d MilliSeconds and Upper Bound is %d", DataRegion.getRegion().getName(), id, getMetaAver, getMetaAlarmLimit); 
            String info = String.format("%s: %s", "Get Meta Cost Time Recovery", body);
            log.warn(info);
        }
        if(getMetaAver >= getMetaAlarmLimit && (lastGetMetaAlarmTime == -1 || (now - lastGetMetaAlarmTime >= OOSConfig.getEmailInterval())) ){
            lastGetMetaAlarmTime = now;
            lastAlarmGetMeta = true;
            String body = String.format("Alarm.In the Past Five Minutes, %s LogServerNode %s Get Metadata Averagely takes %d MilliSeconds which exceeds Upper Bound %d", DataRegion.getRegion().getName(), id, getMetaAver, getMetaAlarmLimit); 
            String info = String.format("%s: %s", "Get Meta Cost Time Alarm", body);
            log.warn(info);
        }
    }

    public void test() {
        double putTimePerMAverSec = illgalPutNum.get() != 0 ? (putTimeMillSecPerM.get() / illgalPutNum.get()) : 0;
        double getTimePerMAverSec = illgalGetNum.get() != 0 ? (getTimeMillSecPerM.get() / illgalGetNum.get()) : 0;
        System.out.println(putTimePerMAverSec);
        System.out.println(getTimePerMAverSec);
    }

    public void clear() {
        totalRequestNum.set(0L);
        putRequestNum.set(0L);
        getRequestNum.set(0L);
        writeBucketLogNum.set(0L);
        costTime.set(0L);
        preProcessTime.set(0L);
        putMeta.set(0L);
        getMeta.set(0L);
        putTime.set(0L);
        getTime.set(0L);
        putDiffTime.set(0L);
        getDiffTime.set(0L);
        pinTime.set(0L);
        prepareLogTime.set(0L);
        writeBucketLogTime.set(0L);

        putMetaMaxTime.set(-1L);
        getMetaMaxTime.set(-1L);
        putMetaMinTime.set(Long.MAX_VALUE);
        getMetaMinTime.set(Long.MAX_VALUE);
        putTimeMillSecPerM.set(0.0);
        getTimeMillSecPerM.set(0.0);
        illgalPutNum.set(0L);
        illgalGetNum.set(0L);
        stdPutMeta = new StandardDeviation();
        stdGetMeta = new StandardDeviation();

        // oos节点
        oosPutGetTimePerNode.clear();

        responseGet200_exception_5.set(0L);
        responseGet200_exception_8.set(0L);
        responseGet200_exception_other.set(0L);
    }

    public boolean check(BucketLog logLine) {
        if (logLine.status > 201)
            return false;
        long MAX = 10000000;
        if (logLine.endTime > MAX || logLine.endTime < 0) // endTime已经改为与startTime的相对时间
            return false;
        if (logLine.preProcessTime > MAX || logLine.preProcessTime < 0)
            return false;
        if (logLine.getMetaTime > MAX || logLine.getMetaTime < 0)
            return false;
        if (logLine.operation.equals(strPutObject)) {
            if (logLine.putMetaTime > MAX || logLine.putMetaTime < 0)
                return false;
            if ((logLine.ostorResponseStartTime - logLine.adapterRequestStartTime) > MAX
                    || (logLine.ostorResponseStartTime - logLine.adapterRequestStartTime) < 0)// put
                return false;
        } else {
            if ((logLine.ostorResponseLastTime - logLine.adapterRequestStartTime) > MAX
                    || (logLine.ostorResponseLastTime - logLine.adapterRequestStartTime) < 0)// get
                return false;
        }

        if (logLine.pinTime > MAX || logLine.pinTime < 0)
            return false;
        if (logLine.prepareLogTime > MAX || logLine.prepareLogTime < 0)
            return false;
        if (logLine.writeBucketLogTime > MAX || logLine.writeBucketLogTime < 0)
            return false;
        return true;
    }

    class OOSPutGetAvgTimePerNode {

        // 记录每个节点的putget请求, key为host,value 为一个length为4的数组依次是PutTimeMillSec，GetTimeMillSec，PutSize，GetSize
        Map<String, DoubleAdder[]> oosMap = new ConcurrentHashMap<>();        

        private void addPutTimeAndSize(String hostName, double timeValue, double sizeValue) {
            oosMap.computeIfAbsent(hostName, v -> new DoubleAdder[] {new DoubleAdder(), new DoubleAdder(), new DoubleAdder(), new DoubleAdder()});
            oosMap.get(hostName)[0].add(timeValue);
            oosMap.get(hostName)[2].add(sizeValue);
        }
        
        private void addGetTimeAndSize(String hostName, double timeValue, double sizeValue) {
            oosMap.computeIfAbsent(hostName, v -> new DoubleAdder[] {new DoubleAdder(), new DoubleAdder(), new DoubleAdder(), new DoubleAdder()});
            oosMap.get(hostName)[1].add(timeValue);
            oosMap.get(hostName)[3].add(sizeValue);
        }

        void sendToGanglia() {
            // 对每个计算节点的put/get average time per MB统计
            for (String host : oosMap.keySet()) {
                // oos put和get
                DoubleAdder oosPutTimeMillSec = oosMap.get(host)[0];
                DoubleAdder oosGetTimeMillSec = oosMap.get(host)[1];
                DoubleAdder oosPutSize = oosMap.get(host)[2];
                DoubleAdder oosGetSize = oosMap.get(host)[3];
                double oosPutTimeAverMillSecPerM = oosPutSize.doubleValue() != 0
                        ? (oosPutTimeMillSec.doubleValue() / (oosPutSize.doubleValue() / size1M)) : 0;
                double oosGetTimeAverMillSecPerM = oosGetSize.doubleValue() != 0
                        ? (oosGetTimeMillSec.doubleValue() / (oosGetSize.doubleValue() / size1M)) : 0;

                GangliaPlugin.sendToGangliaWithFakeIp(GangliaConsts.PUT_AVERAGE_TIME_PER_M + "_" + oosMap.size() + "_" + host,
                        String.valueOf((int) oosPutTimeAverMillSecPerM), GangliaConsts.YTYPE_INT32,
                        GangliaConsts.YNAME_MILLISECOND, GangliaConsts.GROUP_NAME_PUT_GET_TIME_FOR_EACH_API_NODE,
                        "\"In the past five minutes,The average oos Put Time per MB\"", gangliaFakeIp);
                GangliaPlugin.sendToGangliaWithFakeIp(GangliaConsts.GET_AVERAGE_TIME_PER_M + "_" + oosMap.size() + "_" + host,
                        String.valueOf((int) oosGetTimeAverMillSecPerM), GangliaConsts.YTYPE_INT32,
                        GangliaConsts.YNAME_MILLISECOND, GangliaConsts.GROUP_NAME_PUT_GET_TIME_FOR_EACH_API_NODE,
                        "\"In the past five minutes,The average oos Get Time per MB\"", gangliaFakeIp);
            }
        }

        void clear() {
            oosMap.clear();
        }
    }
}
