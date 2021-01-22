package cn.ctyun.oos.server.processor;

import java.util.HashMap;
import java.util.Map;

public abstract class EventBase {
    protected HashMap<String, Object> params = new HashMap<>();
    protected Object data;
    
    public EventBase(){}
    
    public EventBase(HashMap<String, Object> params, Object data){
        this.data = data;
        this.params = params;
    }
    
    public Object getParamValue(String k) {
        return params.get(k);
    }
    
    public void setParamValue(String k, Object v) {
        params.put(k, v);
    }
    
    public void setParams(Map<String, Object> params) {
        this.params.putAll(params);
    }
    
    public Object getData(){
        return this.data;
    };
    
    public void setData(Object data){
        this.data = data;
    };
}
