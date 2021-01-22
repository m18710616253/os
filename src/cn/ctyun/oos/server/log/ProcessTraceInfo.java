package cn.ctyun.oos.server.log;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * 跟踪processLog的相关信息
 * @author xiaojun
 *
 */
public class ProcessTraceInfo {
    public int processCount = 0;
    public int generateCount = 0;
    public AtomicInteger processSucCount = new AtomicInteger(0); // 处理成功数量
    public AtomicInteger processErrNoMoveCount = new AtomicInteger(0); // 处理失败数量
    public AtomicInteger processErrMoveDamagedCount = new AtomicInteger(0); // 处理失败移动数量
    public AtomicInteger sendSucCount = new AtomicInteger(0); // 发送成功数量
    public AtomicInteger sendErrMoveResendCount = new AtomicInteger(0); // 发送失败数量
    public AtomicInteger sendErrMoveDamagedCount = new AtomicInteger(0); // 处理失败移动数量
    /**
     * 生成跟踪信息的字符串
     */
    public String toString() {
        // 打印本周期的处理详情
        StringBuilder build = new StringBuilder();
        build.append(" total : ").append(processCount);
        build.append(" deal sucess: ").append(processSucCount.get());
        build.append(" deal err no move: ").append(processErrNoMoveCount.get());
        build.append(" deal err to damaged: ").append(processErrMoveDamagedCount.get());
        build.append(" generate : ").append(generateCount);
        build.append(" send sucess: ").append(sendSucCount.get());
        build.append(" send err to resend: ").append(sendErrMoveResendCount.get());
        build.append(" send err to damaged: ").append(sendErrMoveDamagedCount.get());
        return build.toString();
    }
}
