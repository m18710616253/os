package cn.ctyun.oos.server.util;

import org.apache.commons.lang3.StringUtils;

import common.tuple.Pair;

public class HttpUtils {
    
    /**
     * 解析header里的range, 保证了返回的endrange一定不小于offset
     * bytes=x-y 从x位取到y位
     * bytes=-x 取后x位
     * bytes=x- 从x位开始取到结尾
     * x-y 从x位取到y位
     * 
     * @exception first: 起始位置0； second：终止位置为对象长度
     * 
     * @return first: 起始位置； second：终止位置，如果大于对象size，则返回对象size
     * */
    public static Pair<Long, Long> getRange(String range, long contentLength){
        Pair<Long, Long> p = new Pair<Long, Long>();
        long offset = 0, endRange = contentLength - 1;
        if (StringUtils.isBlank(range)) {
            p.first(offset);
            p.second(endRange);
            return p;
        }
        try {
            String bytesStr = "bytes=";
            if(range.startsWith(bytesStr))
                range = range.substring(range.indexOf(bytesStr) + bytesStr.length());
            int dash = range.indexOf('-');
            if (dash == 0)
                offset = Math.max(0, endRange
                        - Long.parseLong(range.substring(dash + 1)) + 1);
            else {
                offset = Long.parseLong(range.substring(0, dash));
                if (dash != range.length() - 1)
                    endRange = Long.parseLong(range.substring(dash + 1));
            }
        } catch (Exception e) {
            offset = 0;
            // 如果range不对，就返回整个object
            endRange = contentLength - 1;
        }
        if ((offset > endRange) || (offset < 0)
                || (offset > contentLength)) {
            offset = 0;
            endRange = contentLength - 1;
        }
        endRange = (endRange > contentLength - 1 ? contentLength - 1
                : endRange);
        p.first(offset);
        p.second(endRange);
        return p;
    }
    
    
    
}
