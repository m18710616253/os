package cn.ctyun.oos.common;

import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.lang.NotImplementedException;

/**
 * {@code BandWidthControlStream}是一个可以动态限速的{@link InputStream}。
 * 它封装了用户提供的原始的{@code InputStream}，每次读完一段数据就通过回调
 * {@link ActualExpectedRatio}接口获得实际速度与期望速度的比率R。
 * 如果R>1，则该{@code BandWidthControlStream}会通过sleep一段时间的方式实现降速目标。
 * 如果sleep过程被中断，不会抛出{@code InterruptedException}。
 * 如果R<=1，则不会启用限速机制。

 * @author Jiang Feng
 */
public class BandWidthControlStream extends InputStream {
    private InputStream input;
    private int controlSize;
    private long maxPauseTime;
    private volatile long startTime;
    private long readTime;
    private volatile int readLen;
    private volatile long totalReadTime;
    private volatile int totalReadLen;
    public ActualExpectedRatio ratioImpl;
    
    private int privateSpeedLimit = 0;
    private volatile boolean nextPause = false;
    private volatile boolean closed = false;
    
    public static abstract class ActualExpectedRatio {
        /**
         * 返回实际速度与期望速度的比例。当>1时，
         * 该{@code BandWidthControlStream}会通过sleep一段时间的方式实现降速目标。
         * 由于该方法会被读取流的线程频繁的同步调用，因此该方法的实现应该非常快速。
         * 
         * @return 实际速度与期望速度的比例。
         */
        public abstract double getRatio();
        
        public abstract int getSpeedLimit();
    }
    
    public BandWidthControlStream(InputStream input, int controlSize, long maxPauseTime, ActualExpectedRatio inf) {
        this.input = input;
        this.controlSize = controlSize;
        this.maxPauseTime = maxPauseTime;
        this.ratioImpl = inf;
        this.readLen = 0;
        this.startTime = 0;
        this.readTime = 0;
        this.totalReadTime = 0;
        this.totalReadLen = 0;
        this.privateSpeedLimit = ratioImpl.getSpeedLimit();
    }

    @Override
    public int read() throws NotImplementedException {
        throw new NotImplementedException("not support read only a byte");
    }
    
    @Override
    /**
     * 个人限速逻辑： 每次检查限速使用总的上传长度除以限速速率，减去当前时间和开始上传的时间，这样可以保证总的速率是无限接近限速速率的。 
     * 如果当前读的数据长度大于controlSize 或 3倍于限速速率时间内的数据长度，开始执行限速，最大限maxPauseTime s。 如果本次限速没有使平均速度降到
     * 限速速率。则nextPause 为true ，下次读取只能读一个字节，然后继续执行sleep。最终目的是使平均上传时间无限接近于限速速率。 
     * */
    public int read(byte[] b, int offset, int len) throws IOException {
        if (totalReadLen == 0) {
            startTime = System.currentTimeMillis();
        }
        long st = System.currentTimeMillis();
        int res = input.read(b, offset, nextPause ? 1 : len);
        readTime += System.currentTimeMillis() - st;
        if (res > 0) {
            readLen += res;
            totalReadLen += res;
            if (readLen > Math.min(controlSize, privateSpeedLimit > 0 ? privateSpeedLimit : controlSize) || nextPause) {
                totalReadTime = System.currentTimeMillis() - startTime;
                long pauseTime = 0;
                if(privateSpeedLimit > 0) {
                    pauseTime = (long)(1000f * totalReadLen / privateSpeedLimit) - totalReadTime;
                    if(pauseTime > 0) {
                        if(pauseTime > maxPauseTime) {
                            pauseTime = maxPauseTime;
                            nextPause = true;
                        } else
                            nextPause = false;
                        try {
                            Thread.sleep(Math.min(pauseTime, maxPauseTime));
                        } catch (InterruptedException e) {
                        }
                    }
                }
                
                double ratio = ratioImpl.getRatio();
                if(ratio > 1.0f) {
                    pauseTime =  (long) ((ratio - 1) * readTime) - (pauseTime > 0 ? pauseTime : 0);
                    if (pauseTime > 0) {
                        pauseTime = (pauseTime > maxPauseTime) ? maxPauseTime : pauseTime;
                        try {
                            Thread.sleep(pauseTime);
                        } catch (InterruptedException e) {}
                    }
                }
                readTime = 0;
                readLen = 0;
            }
        }
        return res;
    }
    
    
    /**
     * 如果上传限速没有使速率降到限速速率，此处继续进行限速 处理。 但线程最多只sleep maxPauseTime s
     * */
    @Override
    public void close() throws IOException {
        if(closed) 
            return;
        if(privateSpeedLimit > 0) {
            totalReadTime = System.currentTimeMillis() - startTime;
            long pauseTime = (long)(1000f * totalReadLen / privateSpeedLimit) - totalReadTime;
            if(pauseTime > 0)
                try {
                    Thread.sleep(Math.min(pauseTime, maxPauseTime));
                } catch (InterruptedException e) {
                }
//            System.out.println("CLOSE;; totalReadLen=" + totalReadLen + ", readLen=" + readLen + ", controlSize=" + controlSize + ", speedLimit=" + privateSpeedLimit + ", readTime=" + readTime + ", pauseTime=" + pauseTime + ", maxPauseTime=" + maxPauseTime);
            
        }
        closed = true;
    }
}