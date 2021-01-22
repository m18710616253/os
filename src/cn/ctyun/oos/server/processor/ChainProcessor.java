package cn.ctyun.oos.server.processor;

import java.util.LinkedList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import common.tuple.Pair;

/**
 * 所有Processor串行处理
 * 
 * @author Dongchk
 *
 */
public class ChainProcessor {
    private static final Log LOG = LogFactory.getLog(ChainProcessor.class);
    private LinkedList<Pair<String, Processor>> list = new LinkedList<Pair<String, Processor>>();
    private EventBase event;
    
    public ChainProcessor(EventBase event){
        this.event = event;
    }
    
    public ChainProcessor() {
        
    }
    
    public void register(String describeInfo, Processor processor) {
        list.add(new Pair<>(describeInfo, processor));
    }

    public void start() throws Exception {
        Pair<String, Processor> p;
        if(list.isEmpty())
            return;
        Object data = null;
        while(null != (p = list.poll())){
            try {
                Processor processor = p.second();
                if(null != data)
                    processor.getEvent().setData(data);
                processor.process();
                data = processor.getEvent().getData();
            } catch (Exception e) {
                LOG.error("Exception happens when process message:" + p.first(), e);
                throw e;
            }
        }
    }
    
}
