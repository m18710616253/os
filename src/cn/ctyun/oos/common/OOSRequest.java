package cn.ctyun.oos.common;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URLDecoder;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.Header;
import org.apache.http.client.methods.HttpRequestBase;

import com.amazonaws.DefaultRequest;
import com.amazonaws.http.HttpMethodName;

import cn.ctyun.common.BaseException;
import cn.ctyun.common.Consts;

public class OOSRequest<T> extends DefaultRequest<T> {
    private static final Log log = LogFactory.getLog(OOSRequest.class);
    
    public OOSRequest(String serviceName) {
        super(serviceName);
    }
    
    public OOSRequest(final HttpServletRequest req) throws BaseException {
        super(null);
        Enumeration<?> e = req.getHeaderNames();
        while (e.hasMoreElements()) {
            String k = (String) e.nextElement();
            // header有重名值，value以逗号分隔，如header1=value1，header1=value2，value为value1，value2
            Enumeration<String> multiV = req.getHeaders(k);
            StringBuilder sb = new StringBuilder();
            while (multiV.hasMoreElements()) {
                sb.append(multiV.nextElement().trim()).append(",");
            }
            String v = sb.subSequence(0, sb.length()-1).toString();
            //如果将x-ctyun-替换为空串，客户端在put object的时候使用了此请求头进行V4签名，而服务端在签名计算时，使用的是被替换成空前缀的header，这样会导致签名不通过。 
//            k = k.replace("x-ctyun-", "");
            this.getHeaders().put(k, v);
        }
        String query = req.getQueryString();
        String[] querys;
        if (query != null) {
            querys = query.split("&");
            for (int i = 0; i < querys.length; i++) {
                String[] tmp = querys[i].split("=", 2);
                String k = tmp[0];
                String v = null;
                if (tmp.length > 1)
                    v = tmp[1];
                if (v != null && v.equals(""))
                    v = null;
                if (k.equals("sessionId"))
                    continue;
                try {
                    if (v != null)
                        this.getParameters().put(k, URLDecoder.decode(v, Consts.STR_UTF8));
                    else
                        this.getParameters().put(k, v);
                } catch (UnsupportedEncodingException e1) {
                    log.error(e1.getMessage(), e1);
                    throw new BaseException();
                }
            }
        }
        this.setHttpMethod(HttpMethodName.valueOf(req.getMethod()));
        try {
            this.setContent(req.getInputStream());
        } catch (IOException e1) {
            log.error(e1.getMessage(), e1);
            throw new BaseException();
        }
    }
    
    public OOSRequest(final HttpRequestBase req) {
        super(null);
        Header[] header = req.getAllHeaders();
        for (int i = 0; i < header.length; i++) {
            String k = header[i].getName();
            String v = header[i].getValue();
            this.getHeaders().put(k, v);
        }
    }
  
    public OOSRequest(final HttpURLConnection req) throws BaseException {
        super(null);
        Map<String, List<String>> header = req.getRequestProperties();
        Iterator<String> keys = header.keySet().iterator();
        while (keys.hasNext()) {
            String k = keys.next();
            String v = header.get(k).get(0);
            k = k.replace("x-ctyun-", "");
            this.getHeaders().put(k, v);
        }
        String query = req.getURL().getQuery();
        String[] querys;
        if (query != null) {
            querys = query.split("&");
            for (int i = 0; i < querys.length; i++) {
                String[] tmp = querys[i].split("=");
                String k = tmp[0];
                String v = null;
                if (tmp.length > 1)
                    v = tmp[1];
                if (v == "")
                    v = null;
                if (k.equals("sessionId"))
                    continue;
                try {
                    if (v != null)
                        this.getParameters().put(k, URLDecoder.decode(v, Consts.STR_UTF8));
                    else
                        this.getParameters().put(k, v);
                } catch (UnsupportedEncodingException e1) {
                    log.error(e1.getMessage(), e1);
                    throw new BaseException();
                }
            }
        }
        this.setHttpMethod(HttpMethodName.valueOf(req.getRequestMethod()));
    }
}