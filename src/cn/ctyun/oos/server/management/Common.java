package cn.ctyun.oos.server.management;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import cn.ctyun.common.BaseException;
import cn.ctyun.common.Consts;
import cn.ctyun.common.region.DataRegions;
import cn.ctyun.oos.common.Utils;

import com.amazonaws.services.s3.Headers;
import common.io.ArrayInputStream;
import common.io.ArrayOutputStream;

/**
 * @author: Cui Meng
 */
public class Common {
    private static final Log log = LogFactory.getLog(Common.class);
    
    //符合IPV4格式的正则表达式
    private static String ipRegExp = "^(1\\d{2}|2[0-4]\\d|25[0-5]|[1-9]\\d|[1-9])\\."  
            +"(00?\\d|1\\d{2}|2[0-4]\\d|25[0-5]|[1-9]\\d|\\d)\\."  
            +"(00?\\d|1\\d{2}|2[0-4]\\d|25[0-5]|[1-9]\\d|\\d)\\."  
            +"(00?\\d|1\\d{2}|2[0-4]\\d|25[0-5]|[1-9]\\d|\\d)$"; 

    //符合CIDR格式的IP地址段表达式
    private static String cidrRegExp = "^(1\\d{2}|2[0-4]\\d|25[0-5]|[1-9]\\d|[1-9])\\."  
                +"(00?\\d|1\\d{2}|2[0-4]\\d|25[0-5]|[1-9]\\d|\\d)\\."  
                +"(00?\\d|1\\d{2}|2[0-4]\\d|25[0-5]|[1-9]\\d|\\d)\\."  
                +"(00?\\d|1\\d{2}|2[0-4]\\d|25[0-5]|[1-9]\\d|\\d)/(\\d{1,2})$";
    
    //符合CIDR格式的IP地址段表达式
    private static String cidrStrictRegExp = "^(1\\d{2}|2[0-4]\\d|25[0-5]|[1-9]\\d|[1-9])\\."  
                +"(00?\\d|1\\d{2}|2[0-4]\\d|25[0-5]|[1-9]\\d|\\d)\\."  
                +"(00?\\d|1\\d{2}|2[0-4]\\d|25[0-5]|[1-9]\\d|\\d)\\."  
                +"(00?\\d|1\\d{2}|2[0-4]\\d|25[0-5]|[1-9]\\d|\\d)/(\\d|[1-2]\\d|3[0-2])$";
    
    public static void checkParameter(String parameterName)
            throws BaseException {
        if (parameterName == null || parameterName.trim().length() == 0)
            throw new BaseException();
    }
    
    public static void checkParameter(String parameterName, int maxLength)
            throws BaseException {
        if (parameterName == null || parameterName.trim().length() == 0
                || parameterName.trim().length() > maxLength)
            throw new BaseException();
    }
    
    public static void checkParameter(String parameterName, int maxLength,
            int minLength) throws BaseException {
        if (parameterName == null || parameterName.trim().length() == 0
                || parameterName.trim().length() > maxLength
                || parameterName.trim().length() < minLength)
            throw new BaseException();
    }
    
    public static void checkParameter(double parameterName)
            throws BaseException {
        if (parameterName == 0)
            throw new BaseException();
    }
    
    private static Pattern getPatternCompile(String strRegexp){
        return Pattern.compile(strRegexp);
        }    
        
    public static boolean isValidIPAddr(String ip){
        return getPatternCompile(ipRegExp).matcher(ip).matches();
    }
    
    public static boolean isValidCidrAddr(String ip){
        return getPatternCompile(cidrStrictRegExp).matcher(ip).matches();
    }
        
    public static void checkIPParameter(List<String> ipList)
            throws BaseException {
        List<String> noMatchList = ipList.stream().filter(new Predicate<String>(){
            @Override
            public boolean test(String t) {
                return  !isValidIPAddr(t) && !isValidCidrAddr(t) && !Utils.isValidIpv6Addr(t) && !Utils.isValidIpv6Cidr(t);
            }
        }).collect(Collectors.toList());
        
        if (noMatchList.size() > 0)
            throw new BaseException(403,"ipIsNotValid", StringUtils.join(noMatchList, "  |  "));
    }
    
    /**
     * 验证ipv4或ipv6合法性
     * @param ip
     * @throws BaseException
     */
    public static void checkIPParameter(String ip) throws BaseException {
        if (!isValidIPAddr(ip) && !isValidCidrAddr(ip) && !Utils.isValidIpv6Addr(ip) && !Utils.isValidIpv6Cidr(ip))
            throw new BaseException(403,"ipIsNotValid","ipIsNotValid");
    }
    
    public static void writeResponseEntity(HttpServletResponse resp, String body)
            throws IOException {
        resp.setCharacterEncoding(Consts.STR_UTF8);
        if (body != null) {
            resp.setIntHeader(Headers.CONTENT_LENGTH,
                    body.getBytes(Consts.CS_UTF8).length);
            resp.setHeader(Headers.CONTENT_TYPE, "application/xml");
            try {
                resp.getOutputStream().write(body.getBytes(Consts.STR_UTF8));
            } catch (Exception e) {
                log.error("catch write response error");
            }
        } else
            resp.setIntHeader(Headers.CONTENT_LENGTH, 0);
    }
    
    public static void writeJsonResponseEntity(HttpServletResponse resp, String body)
            throws IOException {
        resp.setCharacterEncoding(Consts.STR_UTF8);
        if (body != null) {
            resp.setIntHeader(Headers.CONTENT_LENGTH,
                    body.getBytes(Consts.CS_UTF8).length);
            resp.setHeader(Headers.CONTENT_TYPE, "application/json");
            try {
                resp.getOutputStream().write(body.getBytes(Consts.STR_UTF8));
            } catch (Exception e) {
                log.error("catch write response error");
            }
        } else
            resp.setIntHeader(Headers.CONTENT_LENGTH, 0);
    }
    
    public static void writeResponseEntity(HttpServletResponse resp, String body, String format, String file) throws IOException {
        resp.setCharacterEncoding(Consts.STR_UTF8);
        if (format!= null && format.equals("csv")) {
            resp.setHeader("Content-type", "application/octet-stream");
            resp.setHeader("Content-Disposition","attachment;filename="+file);
        }
        if (body != null) {
            resp.setIntHeader(Headers.CONTENT_LENGTH, body.getBytes(Consts.CS_UTF8).length);
            try {
                resp.getOutputStream().write(body.getBytes(Consts.STR_UTF8));
            } catch (Exception e) {
                log.error("catch write response error");
            }
        } else
            resp.setIntHeader(Headers.CONTENT_LENGTH, 0);
    }
    
    public static InputStream logInputStream(InputStream ip) throws IOException {
        InputStream is = null;
        InputStream is2 = null;
        ArrayOutputStream aos = null;
        try {
            aos = new ArrayOutputStream(Consts.DEFAULT_BUFFER_SIZE);
            int size = IOUtils.copy(ip, aos);
            is = new ArrayInputStream(aos.data(), 0, size);
            is2 = new ArrayInputStream(aos.data(), 0, size);
            log.info(IOUtils.toString(is2));
        } finally {
            if (ip != null) {
                try {
                    ip.close();
                } catch (Throwable e) {
                    log.error(e.getMessage(), e);
                }
            }
            if (is2 != null) {
                try {
                    is2.close();
                } catch (Throwable e) {
                    log.error(e.getMessage(), e);
                }
            }
            if (aos != null) {
                try {
                    aos.close();
                } catch (Throwable e) {
                    log.error(e.getMessage(), e);
                }
            }
        }
        return is;
    }

    public static List<String> getAllRegions() {
        List<String> regions = new ArrayList<>();
        regions.addAll(DataRegions.getAllRegions());
        regions.add(Consts.GLOBAL_DATA_REGION);
        return Collections.unmodifiableList(regions);
    }
    
    public static List<String> getAllRegionsExceptGlobalRegion() {
        List<String> regions = new ArrayList<>();
        regions.addAll(DataRegions.getAllRegions());
        return Collections.unmodifiableList(regions);
    }
}
