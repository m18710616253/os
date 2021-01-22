package cn.ctyun.oos.server.processor;

public abstract class Processor {
    
    protected EventBase event;
    public Processor(EventBase event) {
        this.event = event;
    }
    
    @SuppressWarnings("serial")
    public static class ProcessorException extends Exception {
        //code ==400 或 code >=500 需要重新读流，否则不需要
        // 600  高级裁剪：起始横纵坐标超出图像范围
        final int code;
        
        public ProcessorException(int code, String msg, Throwable t) {
            super(msg, t);
            this.code = code;
        }
        
        public ProcessorException(int code, String msg) {
            super(msg);
            this.code = code;
        }
        
        public int getErrorCode() {
            return code;
        }
    }

    public abstract void process() throws Exception;

    public abstract Object getResult();
    
    public EventBase getEvent() {
        return event;
    }
}
