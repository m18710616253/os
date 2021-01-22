package cn.ctyun.oos.server.management;

import java.util.HashMap;
import java.util.Map;

import cn.ctyun.oos.metadata.MinutesUsageMeta;

/**
 * 活跃用户用量
 * 
 * @author wangduo
 */
public class ActivityOwner implements Comparable<ActivityOwner> {
    
    /** 账户ID */
    public long ownerId;
    
    /** 总流量  */
    public long flow;
    
    /** get, head的请求次数 */
    public long ghRequest;
    
    /** 其他请求的次数 */
    public long otherRequest;

    /** 按region保存最后的总容量 */
    private Map<String, Long> totalSizeRegionMap = new HashMap<>();
    
    /**
     * 获取请求总数 
     */
    public long getTotal() {
        return ghRequest + otherRequest;
    }

    /**
     * 对每个region的最后的总容量进行加和，算出全局的总容量
     * @return
     */
    public long getTotalSize() {
        long totalSize = 0;
        for (long regionSize : totalSizeRegionMap.values()) {
            totalSize += regionSize;
        }
        return totalSize;
    }
    
    /**
     * 添加用量
     * @param usage
     */
    public void addUsage(MinutesUsageMeta usage) {
        // 记录每个region的最后的总容量
        totalSizeRegionMap.put(usage.regionName, usage.sizeStats.size_total);
        // 互联网及非互联网下行流量
        flow += usage.flowStats.flow_download + usage.flowStats.flow_noNetDownload 
                + usage.flowStats.flow_noNetRoamDownload + usage.flowStats.flow_roamDownload;
        // 互联网及非互联网get、head请求数
        ghRequest +=  usage.requestStats.req_get + usage.requestStats.req_head 
                + usage.requestStats.req_noNetGet + usage.requestStats.req_noNetHead;
        // 互联网及非互联网other请求数
        otherRequest += usage.requestStats.req_other + usage.requestStats.req_put 
                + usage.requestStats.req_post + usage.requestStats.req_delete 
                + usage.requestStats.req_noNetOther + usage.requestStats.req_noNetPut 
                + usage.requestStats.req_noNetPost + usage.requestStats.req_noNetDelete;
    }
    
    /**
     * 用于活跃度排序使用，总请求数的降序排序
     */
    @Override
    public int compareTo(ActivityOwner o) {
        long thisTotal = getTotal();
        long compareTotal = o.getTotal();
        if (thisTotal > compareTotal) {
            return -1;
        } else if (thisTotal < compareTotal) {
            return 1;
        } else {
            return 0;
        }
    }

}
