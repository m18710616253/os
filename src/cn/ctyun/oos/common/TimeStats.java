package cn.ctyun.oos.common;

import java.util.LinkedHashMap;
import java.util.Map.Entry;

public class TimeStats {
    private LinkedHashMap<String, Long> stats = new LinkedHashMap<>();
    
    /**
     * 记录操作的起始时间
     * @param optName
     * @param time
     */
    public void startOpt(String optName, long time) {
        stats.put(optName, time);
    }
    
    /**
     * 计算操作的总耗时，在调用该方法之前同一个操作要先调用startOpt
     * @param optName
     * @param time
     */
    public void endOpt(String optName, long time) {
        Long st = stats.get(optName);
        if(st != null)
            stats.put(optName, time - st);
    }
    
    /**
     * 获取时间统计信息
     * @return
     */
    public String getTimeStats() {
        StringBuilder sb = new StringBuilder("{");
        int n = stats.size();
        for(Entry<String, Long> entry : stats.entrySet()) {
            sb.append(entry.getKey()).append(":").append(entry.getValue());
            if(--n > 0)
                sb.append(", ");
            else
                sb.append("}");
        }
        return sb.toString();
    }
}
