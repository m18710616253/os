package cn.ctyun.oos.website;

import java.io.IOException;
import java.io.InputStream;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.eclipse.jetty.server.Request;
import org.json.JSONException;
import org.json.JSONObject;

import cn.ctyun.common.Consts;
import cn.ctyun.common.Session;
import cn.ctyun.oos.hbase.MetaClient;
import cn.ctyun.oos.metadata.OwnerMeta;

/**
 * @author: Cui Meng
 */
public class UDB {
    private static final Log log = LogFactory.getLog(UDB.class);
    public static CompositeConfiguration config;
    private static MetaClient client = MetaClient.getGlobalClient();
    public static void login(Request basereq, HttpServletRequest request,
            HttpServletResponse response, Session<String, String> session)
            throws Exception {
        if (request.getParameter("res_code") != null
                && request.getParameter("res_code").equals("0")) {
            String p_user_id = request.getParameter("p_user_id");
            String code = request.getParameter("code");
            if (p_user_id == null || p_user_id.trim().length() == 0
                    || code == null || code.trim().length() == 0)
                sendToErrorPage(response);
            String token = getToken(code, p_user_id);
            if (token != null) {
                OwnerMeta owner = new OwnerMeta(p_user_id);
                if (client.ownerSelect(owner)) {
                    String sessionId = Portal.generateSession(request,
                            response, session, p_user_id, owner.displayName);
                    response.sendRedirect(config.getString("website.webpages")
                            + "/v2/console.html?sid=" + sessionId + "&token="
                            + token);
                } else {
                    String sessionId = Portal.generateSession(request,
                            response, session, p_user_id, null);
                    String mobilePhone = getMobilePhoneFromUDB(
                            config.getString("udb.app_id"), token);
                    response.sendRedirect(config.getString("website.webpages")
                            + "/v1/udbreg.html?sid=" + sessionId
                            + "&mobilephone=" + mobilePhone + "&userName="
                            + p_user_id + "&token=" + token);
                }
            } else
                sendToErrorPage(response);
        } else
            sendToErrorPage(response);
    }
    
    private static String getMobilePhoneFromUDB(String appid, String token)
            throws ClientProtocolException, IOException {
        String host = config.getString("udb.apiHost");
        String uri = "https://" + host
                + config.getString("udb.phoneAndProvince") + "?app_id=" + appid
                + "&access_token=" + token + "&type=json";
        HttpGet httpRequest = new HttpGet(uri);
        HttpHost httpHost = new HttpHost(host);
        HttpClient client = new DefaultHttpClient();
        HttpResponse newResponse = client.execute(httpHost, httpRequest);
        String phone = "";
        InputStream ip = null;
        try {
            ip = newResponse.getEntity().getContent();
            JSONObject entity = new JSONObject(IOUtils.toString(ip));
            phone = entity.getString("cellphone");
            return phone;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return null;
        } finally {
            if (ip != null)
                ip.close();
        }
    }
    
    private static void sendToErrorPage(HttpServletResponse response)
            throws IOException {
        response.sendRedirect(config.getString("website.webpages")
                + "/error.html?msg="
                + URLEncoder.encode("登录失败", Consts.STR_UTF8));
    }
    
    public static void logout(String token) throws ClientProtocolException,
            IOException {
        HttpPost httpRequest = new HttpPost("https://"
                + config.getString("udb.authAPIHost")
                + config.getString("udb.logout"));
        List<NameValuePair> params = new ArrayList<NameValuePair>();
        params.add(new BasicNameValuePair("app_id", config
                .getString("udb.app_id")));
        params.add(new BasicNameValuePair("access_token", token));
        params.add(new BasicNameValuePair("redirect_uri", "http://"
                + config.getString("website.portalDomain") + "/logout"));
        httpRequest
                .setEntity(new UrlEncodedFormEntity(params, Consts.STR_UTF8));
        HttpHost httpHost = new HttpHost(config.getString("udb.authAPIHost"));
        HttpClient client = new DefaultHttpClient();
        HttpResponse newResponse = client.execute(httpHost, httpRequest);
        InputStream ip = null;
        try {
            ip = newResponse.getEntity().getContent();
        } finally {
            if (ip != null)
                ip.close();
        }
    }
    
    private static String getToken(String code, String p_user_id)
            throws ClientProtocolException, IOException, IllegalStateException,
            JSONException {
        HttpPost httpRequest = new HttpPost("https://"
                + config.getString("udb.authAPIHost")
                + config.getString("udb.accessToken"));
        List<NameValuePair> params = new ArrayList<NameValuePair>();
        params.add(new BasicNameValuePair("grant_type", "authorization_code"));
        params.add(new BasicNameValuePair("code", code));
        params.add(new BasicNameValuePair("app_id", config
                .getString("udb.app_id")));
        params.add(new BasicNameValuePair("app_secret", config
                .getString("udb.app_secret")));
        params.add(new BasicNameValuePair("redirect_uri", "http://"
                + config.getString("website.portalDomain")
                + config.getString("udb.udbLoginRedirect")));
        httpRequest
                .setEntity(new UrlEncodedFormEntity(params, Consts.STR_UTF8));
        HttpHost httpHost = new HttpHost(config.getString("udb.authAPIHost"));
        HttpClient client = new DefaultHttpClient();
        HttpResponse newResponse = client.execute(httpHost, httpRequest);
        if (newResponse.getStatusLine().getStatusCode() == 200) {
            InputStream ip = null;
            try {
                ip = newResponse.getEntity().getContent();
                JSONObject entity = new JSONObject(IOUtils.toString(ip));
                if (entity.getInt("res_code") == 0
                        && entity.getString("res_message").equals("Success")
                        && entity.getString("p_user_id").equals(p_user_id)) {
                    return entity.getString("access_token");
                } else
                    return null;
            } finally {
                if (ip != null)
                    ip.close();
            }
        } else
            return null;
    }
    
    public static void logoutFromUDB(HttpServletRequest request,
            HttpServletResponse response) {
        if (request.getParameter("res_code") != null
                && request.getParameter("res_code").equals("0")) {
            log.info("logout from udb success,token is:"
                    + request.getParameter("access_token"));
        }
    }
}
