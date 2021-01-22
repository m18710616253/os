package cn.ctyun.oos.server.util;

import javax.servlet.http.HttpServletRequest;

import cn.ctyun.common.Consts;

public class HttpLimitParser {
    
    public static String[] getLimits(HttpServletRequest req) {
        String limitStr = req.getHeader(Consts.X_AMZ_LIMIT);
        if(null == limitStr)
            limitStr = req.getParameter(Consts.X_AMZ_LIMIT);
        if(null == limitStr)
            return null;
        if(limitStr.contains(","))
            return limitStr.split(",");
        return new String[] {limitStr};
    }
    
    public static String getLimit(HttpServletRequest req, String item) {
        String[] limits = getLimits(req);
        if(null == limits)
            return null;
        for(String str : limits) {
            if(str.trim().contains(item)) {
                String[] limit = str.split("=");
                //通用代码不校验参数正确性。
                if(limit[0].trim().equals(item))
                    return str;
            }
        }
        return null;
    }
}
