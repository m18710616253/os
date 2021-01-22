package cn.ctyun.oos.tempo;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.Header;
import org.apache.http.client.HttpRequestRetryHandler;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.conn.ssl.X509HostnameVerifier;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.client.DefaultHttpRequestRetryHandler;
import org.apache.http.impl.conn.PoolingClientConnectionManager;
import org.apache.http.params.HttpParams;

import com.amazonaws.DefaultRequest;

import cn.ctyun.oos.server.signer.SignerUtils;
import cn.ctyun.oos.server.signer.V4Signer;
import common.threadlocal.ThreadLocalBytes;
import common.time.TimeStat;
import common.util.BlockingExecutor;
import common.util.GetOpt;
import common.util.HexUtils;
import common.util.JsonUtils;

class OOSRequest2<T> extends DefaultRequest<T> {
    public OOSRequest2(String serviceName) {
        super(serviceName);
    }
    
    public OOSRequest2(final HttpRequestBase req) {
        super(null);
        Header[] header = req.getAllHeaders();
        for (int i = 0; i < header.length; i++) {
            String k = header[i].getName();
            String v = header[i].getValue();
            k = k.replace("x-ctyun-", "");
            this.getHeaders().put(k, v);
        }
        HttpParams params = req.getParams();
        if (params.getParameter("public") != null)
            this.getParameters().put("public", "");
        if (params.getParameter("code") != null)
            this.getParameters().put("code", (String) params.getParameter("code"));
        if (params.getParameter("offset") != null)
            this.getParameters().put("offset", "");
    }
}

public class AdaptorClientV4 {
    static {
        System.setProperty("log4j.log.app", "adaptorclientv4");
    }
    private static final Log log = LogFactory.getLog(AdaptorClientV4.class);
    DefaultHttpClient client;
    static String uid;
    static String pool;
    static String secret;
    public static List<String> list = new ArrayList<String>();
    public static List<String> readingList = new LinkedList<String>();
    public static final String SCHEME = "AWS4";
    public static final String ALGORITHM = "HMAC-SHA256";
    public static final String TERMINATOR = "aws4_request";
    
    public static class Stats {
        String method = "";
        String path = null;
        /**
         * 所请求的对象大小，跟是否请求成功无关。 对于写操作，这个值将被赋值为需要发送的对象的大小。
         * 对于读操作，如果可以获得Content-Length，则为Content-Length； 否则，只有当请求成功后，才能被赋值。
         */
        int objSize = -1;
        int respCode = -1;
        /** HTTP Response中的Message */
        String respMsg = null;
        /** 当程序运行抛出异常时，会被赋值为Exception.getMessage(). */
        String exceptionMsg = null;
        protected long timeTransStart = -1;
        protected long timeConnectStart = -1;
        protected long timeConnectEnd = -1;
        protected long timeSentHeaders = -1;
        protected long timeSentLastByte = -1;
        protected long timeReadFirstByte = -1;
        protected long timeReadLastByte = -1;
        protected long timeTransEnd = -1;
        /** 实际读入或者写出的对象字节数。如果请求成功，则等于对象大小，否则小于对象大小，甚至为0 */
        int transferredBytes = 0;
        
        public String toJsonString() {
            // return "method=" + method + ", path=" + path + ", respCode=" +
            // respCode
            // + ", timeTransStart=" + timeTransStart
            // + ", timeConnectStart=" + timeConnectStart
            // + ", timeConnectEnd=" + timeConnectEnd
            // + ", timeSentHeaders=" + timeSentHeaders
            // + ", timeSentLastByte=" + timeSentLastByte
            // + ", timeReadFirstByte=" + timeReadFirstByte
            // + ", timeReadLastByte=" + timeReadLastByte
            // + ", timeTransEnd=" + timeTransEnd
            // + ", transferredBytes=" + transferredBytes;
            return JsonUtils.toJson(this);
        }
        
        public String toStringForCSV() {
            return method + ", " + path + ", " + objSize + ", " + respCode + ", " + respMsg + ", "
                    + timeTransStart + ", " + timeConnectStart + ", " + timeConnectEnd + ", "
                    + timeSentHeaders + ", " + timeSentLastByte + ", " + timeReadFirstByte + ", "
                    + timeReadLastByte + ", " + timeTransEnd + ", " + transferredBytes;
        }
    }
    static class TM implements javax.net.ssl.TrustManager, javax.net.ssl.X509TrustManager {
        public java.security.cert.X509Certificate[] getAcceptedIssuers() {
            return null;
        }
        
        public boolean isServerTrusted(java.security.cert.X509Certificate[] certs) {
            return true;
        }
        
        public boolean isClientTrusted(java.security.cert.X509Certificate[] certs) {
            return true;
        }
        
        public void checkServerTrusted(java.security.cert.X509Certificate[] certs, String authType)
                throws java.security.cert.CertificateException {
            return;
        }
        
        public void checkClientTrusted(java.security.cert.X509Certificate[] certs, String authType)
                throws java.security.cert.CertificateException {
            return;
        }
    }
    void init(int maxPerRoute, int port) throws Exception {
        // Step0 : 接受所有证书
        System.out.println("Accept all hostname when ssl handshaking.");
        HttpsURLConnection.setDefaultHostnameVerifier(new HostnameVerifier() {
            @Override
            public boolean verify(String hostname, SSLSession session) {
                return true;
            }
        });
        javax.net.ssl.TrustManager[] trustAllCerts = new javax.net.ssl.TrustManager[1];
        javax.net.ssl.TrustManager tm = new TM();
        trustAllCerts[0] = tm;
        javax.net.ssl.SSLContext sc = javax.net.ssl.SSLContext.getInstance("SSL");
        sc.init(null, trustAllCerts, null);
        javax.net.ssl.HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());
        // Step1: 配置HttpClient
        SSLContext sslContext = SSLContext.getInstance("SSL");
        // set up a TrustManager that trusts everything
        sslContext.init(null, new TrustManager[] { new X509TrustManager() {
            public X509Certificate[] getAcceptedIssuers() {
                return null;
            }
            
            public void checkClientTrusted(X509Certificate[] certs, String authType) {
            }
            
            public void checkServerTrusted(X509Certificate[] certs, String authType) {
            }
        } }, new SecureRandom());
        SSLSocketFactory sf = new SSLSocketFactory(sslContext, new X509HostnameVerifier() {
            public boolean verify(String hostname, SSLSession session) {
                return true;
            }
            
            public void verify(String host, SSLSocket ssl) throws IOException {
            }
            
            public void verify(String host, X509Certificate cert) throws SSLException {
            }
            
            public void verify(String host, String[] cns, String[] subjectAlts) throws SSLException {
            }
        });
        Scheme httpsScheme = new Scheme("https", port, sf);
        SchemeRegistry schemeRegistry = new SchemeRegistry();
        schemeRegistry.register(httpsScheme);
        PoolingClientConnectionManager ccm;
        // if (port == 443 || port == 8443)
        ccm = new PoolingClientConnectionManager(schemeRegistry);
        // else
        // ccm = new PoolingClientConnectionManager();
        ccm.setMaxTotal(maxPerRoute);
        ccm.setDefaultMaxPerRoute(maxPerRoute);
        client = new DefaultHttpClient(ccm);
        HttpRequestRetryHandler retryHandler = new DefaultHttpRequestRetryHandler(0, false);
        client.setHttpRequestRetryHandler(retryHandler);
    }
    
    static String generateFileName(Random rand) {
        byte[] buf = new byte[32];
        rand.nextBytes(buf);
        return "putget_" + HexUtils.toHexString(buf);
    }
    
    public static String getStorageClass(String storage) {
        String[] st = storage.split(",");
        int i = tlrand.get().nextInt(st.length);
        return st[i].toString();
    }
    
    void postV4(String prot, String hoststr, int port, String key,
            int postSize_kB, final int bandwidth_ms4kB, final Stats stats,
            String storage, String domain, String regionName,
            boolean payloadHashEnabled) throws Exception {
        stats.timeTransStart = System.currentTimeMillis();
        String method = stats.method = "PUT";
        stats.path = key;
        stats.objSize = postSize_kB;
        byte[] bytes = ThreadLocalBytes.current().get1KBytes();
        URL url = new URL(prot, hoststr, port, "/" + pool + "/" + key);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        RandomInputStream ris = new RandomInputStream(postSize_kB);
        {
            // Step3：构造V4签名！
            String contentHashString = V4Signer.UNSIGNED_PAYLOAD;
            if (payloadHashEnabled) {
                // 对内容做签名
                MessageDigest mDigest = MessageDigest.getInstance("SHA-256");
                DigestInputStream dis = new DigestInputStream(ris, mDigest);
                while((dis.read(bytes)) >0);
                byte[] contentHash = dis.getMessageDigest().digest();
                dis.close();
                ris.reset();
                contentHashString = SignerUtils.toHex(contentHash);
            }
            String storageClass = SimpleTest.getStorageClass(storage);
            String dateTimeStamp = SignerUtils.formatTimestamp(new Date().getTime());
            Map<String, String> headers = new HashMap<>();
            headers.put("Content-Type", "text/plain");
            headers.put("Content-Length", String.valueOf(postSize_kB));
            if (!storage.equals("all"))
                headers.put("x-amz-storage-class", storageClass);
            headers.put("x-amz-content-sha256", contentHashString);
            headers.put("Host", domain);
            String authorization = computeSignature(headers, null, contentHashString,  uid, secret, url, "PUT", "s3", regionName, dateTimeStamp);
            conn.setRequestProperty("Authorization", authorization);
            conn.setRequestProperty("Content-Type", "text/plain");
            if (!storage.equals("all"))
                conn.setRequestProperty("x-amz-storage-class", storageClass);
            conn.setRequestProperty("Date", dateTimeStamp);
            conn.setRequestProperty("Host", domain);
            conn.setRequestProperty("x-amz-content-sha256", contentHashString);
        }
        conn.setRequestMethod(method);
        conn.setDoOutput(true);
        conn.setFixedLengthStreamingMode(postSize_kB);
        conn.setConnectTimeout(30 * 1000);
        conn.setReadTimeout(120 * 1000);
        conn.setUseCaches(false);
        int sent = 0;
        try {
            stats.timeConnectStart = System.currentTimeMillis();
            conn.connect();
            stats.timeConnectEnd = System.currentTimeMillis();
            try (OutputStream out = conn.getOutputStream()) {
                stats.timeSentHeaders = System.currentTimeMillis();
                while (sent < postSize_kB) {
                    int remains = postSize_kB - sent;
                    int block = (remains < bytes.length) ? remains : bytes.length;
                    block = ris.read(bytes, 0, block);
                    out.write(bytes, 0, block);
                    sent += block;
                    /*
                     * 让其等待足够的时间 if(sent >= postSize_kB) break;
                     */
                    long nextTime = bandwidth_ms4kB * sent / BLOCK_SIZE;
                    long towait = stats.timeTransStart + nextTime - System.currentTimeMillis();
                    if (towait > 0)
                        Thread.sleep(towait);
                }
                stats.timeSentLastByte = System.currentTimeMillis();
                stats.respCode = conn.getResponseCode();
                stats.respMsg = conn.getResponseMessage();
                stats.timeReadFirstByte = stats.timeReadLastByte = System.currentTimeMillis();
            }
        } finally {
            stats.transferredBytes = sent;
            try {
                conn.disconnect();
            } finally {
                stats.timeTransEnd = System.currentTimeMillis();
            }
            // 打log,当前时间|格式化的时间|发送最后一个字节时间，到收到响应时间|响应码|contentLength
            long now = stats.timeTransEnd;
            log.info("minilog-post: " + now + ","
                    + new SimpleDateFormat("HH:mm:ss").format(new Date(now)) + ","
                    + String.valueOf(stats.timeReadFirstByte - stats.timeSentLastByte) + ","
                    + stats.respCode + "," + sent);
            log.info("minilog-post-all: " + stats.toJsonString() + " tranCost=" 
                    + (stats.timeTransEnd - stats.timeConnectStart) + " e2eCost="
                    + (stats.timeReadFirstByte - stats.timeSentLastByte));
        }
    }
    
    static class RandomInputStream extends InputStream {
        ThreadLocalRandom rand = ThreadLocalRandom.current();
        byte[] data;
        int offset = 0;
        int length = 0;
        RandomInputStream(int size) {
            data = new byte[size];
            rand.nextBytes(data);
            length = size;
        }
        public int read() throws IOException {
            throw new UnsupportedOperationException();
        }
        
        public int read(byte[] bb)  throws IOException {
            return read(bb, 0, bb.length);
        }
        
        public int read(byte[] bb, int off, int len) throws IOException {
            if (bb == null) {
                throw new NullPointerException();
            } else if (off < 0 || len < 0 || len > bb.length - off) {
                throw new IndexOutOfBoundsException();
            } else if (len == 0) {
                return 0;
            }
            if(offset == length)
                return -1;
            int n = Math.min(Math.min(bb.length - off, len), length - offset);
            System.arraycopy(data, offset, bb, off, n);
            offset +=n;
            return n;
        }
        
        public void reset() {
            offset = 0;
            length = data.length;
        }
    }
    
    public static String computeSignature(Map<String, String> headers,
            Map<String, String> queryParameters, String bodyHash,
            String accessKey, String secretKey, URL endpointUrl,
            String httpMethod, String serviceName, String regionName,
            String dateTimeStamp) {
        String canonicalizedHeaderNames = getCanonicalizeHeaderNames(headers);
        String canonicalizedHeaders = getCanonicalizedHeaderString(headers);

        // if any query string parameters have been supplied, canonicalize them
        String canonicalizedQueryParameters = getCanonicalizedQueryString(queryParameters);

        // canonicalize the various components of the request
        String canonicalRequest = getCanonicalRequest(endpointUrl, httpMethod,
                canonicalizedQueryParameters, canonicalizedHeaderNames,
                canonicalizedHeaders, bodyHash);
//        System.out.println("--------- Canonical request --------");
//        System.out.println(canonicalRequest);
//        System.out.println("------------------------------------");

        headers.put("Date", dateTimeStamp);
        String dateStamp = dateTimeStamp.split("T")[0];
        String scope = dateStamp + "/" + regionName + "/" + serviceName + "/" + TERMINATOR;
        String stringToSign = getStringToSign(SCHEME, ALGORITHM, dateTimeStamp, scope, canonicalRequest);
//        System.out.println("--------- String to sign -----------");
//        System.out.println(stringToSign);
//        System.out.println("------------------------------------");

        // compute the signing key
        byte[] kSecret = (SCHEME + secretKey).getBytes();
        byte[] kDate = sign(dateStamp, kSecret, "HmacSHA256");
        byte[] kRegion = sign(regionName, kDate, "HmacSHA256");
        byte[] kService = sign(serviceName, kRegion, "HmacSHA256");
        byte[] kSigning = sign(TERMINATOR, kService, "HmacSHA256");
        byte[] signature = sign(stringToSign, kSigning, "HmacSHA256");
        

        String credentialsAuthorizationHeader = "Credential=" + accessKey + "/" + scope;
        String signedHeadersAuthorizationHeader = "SignedHeaders=" + canonicalizedHeaderNames;
        String signatureAuthorizationHeader = "Signature=" + SignerUtils.toHex(signature);

         String authorizationHeader = SCHEME + "-" + ALGORITHM + " "
         + credentialsAuthorizationHeader + ", "
         + signedHeadersAuthorizationHeader + ", "
         + signatureAuthorizationHeader;

        return authorizationHeader;
    }
    
    public static String getCanonicalizeHeaderNames(Map<String, String> headers) {
        List<String> sortedHeaders = new ArrayList<String>();
        sortedHeaders.addAll(headers.keySet());
        Collections.sort(sortedHeaders, String.CASE_INSENSITIVE_ORDER);

        StringBuilder buffer = new StringBuilder();
        for (String header : sortedHeaders) {
            if (buffer.length() > 0) buffer.append(";");
            buffer.append(header.toLowerCase());
        }

        return buffer.toString();
    }
    
    public static String getCanonicalizedHeaderString(Map<String, String> headers) {
        if ( headers == null || headers.isEmpty() ) {
            return "";
        }
        
        // step1: sort the headers by case-insensitive order
        List<String> sortedHeaders = new ArrayList<String>();
        sortedHeaders.addAll(headers.keySet());
        Collections.sort(sortedHeaders, String.CASE_INSENSITIVE_ORDER);

        // step2: form the canonical header:value entries in sorted order. 
        // Multiple white spaces in the values should be compressed to a single 
        // space.
        StringBuilder buffer = new StringBuilder();
        for (String key : sortedHeaders) {
            buffer.append(key.toLowerCase().replaceAll("\\s+", " ") + ":" + headers.get(key).replaceAll("\\s+", " "));
            buffer.append("\n");
        }

        return buffer.toString();
    }
    
    public static String getCanonicalizedQueryString(Map<String, String> parameters) {
        if ( parameters == null || parameters.isEmpty() ) {
            return "";
        }
        
        SortedMap<String, String> sorted = new TreeMap<String, String>();

        Iterator<Map.Entry<String, String>> pairs = parameters.entrySet().iterator();
        while (pairs.hasNext()) {
            Map.Entry<String, String> pair = pairs.next();
            String key = pair.getKey();
            String value = pair.getValue();
            sorted.put(urlEncode(key, false), urlEncode(value, false));
        }

        StringBuilder builder = new StringBuilder();
        pairs = sorted.entrySet().iterator();
        while (pairs.hasNext()) {
            Map.Entry<String, String> pair = pairs.next();
            builder.append(pair.getKey());
            builder.append("=");
            builder.append(pair.getValue());
            if (pairs.hasNext()) {
                builder.append("&");
            }
        }

        return builder.toString();
    }
    
    public static String getCanonicalRequest(URL endpoint, String httpMethod,
            String queryParameters, String canonicalizedHeaderNames,
            String canonicalizedHeaders, String bodyHash) {
        String canonicalRequest = httpMethod + "\n"
                + getCanonicalizedResourcePath(endpoint) + "\n"
                + queryParameters + "\n" + canonicalizedHeaders + "\n"
                + canonicalizedHeaderNames + "\n" + bodyHash;
        return canonicalRequest;
    }
    
    public static String getCanonicalizedResourcePath(URL endpoint) {
        if ( endpoint == null ) {
            return "/";
        }
        String path = endpoint.getPath();
        if ( path == null || path.isEmpty() ) {
            return "/";
        }
        
        String encodedPath = urlEncode(path, true);
        if (encodedPath.startsWith("/")) {
            return encodedPath;
        } else {
            return "/".concat(encodedPath);
        }
    }
    
    public static String urlEncode(String url, boolean keepPathSlash) {
        String encoded;
        try {
            encoded = URLEncoder.encode(url, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("UTF-8 encoding is not supported.", e);
        }
        if ( keepPathSlash ) {
            encoded = encoded.replace("%2F", "/");
        }
        return encoded;
    }
    
    public static String getStringToSign(String scheme, String algorithm, String dateTime, String scope, String canonicalRequest) {
        String stringToSign =
                        scheme + "-" + algorithm + "\n" +
                        dateTime + "\n" +
                        scope + "\n" +
                        SignerUtils.toHex(hash(canonicalRequest));
        return stringToSign;
    }
    
    public static byte[] hash(String text) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            md.update(text.getBytes("UTF-8"));
            return md.digest();
        } catch (Exception e) {
            throw new RuntimeException("Unable to compute hash while signing request: " + e.getMessage(), e);
        }
    }
    
    public static byte[] sign(String stringData, byte[] key, String algorithm) {
        try {
            byte[] data = stringData.getBytes("UTF-8");
            Mac mac = Mac.getInstance(algorithm);
            mac.init(new SecretKeySpec(key, algorithm));
            return mac.doFinal(data);
        } catch (Exception e) {
            throw new RuntimeException("Unable to calculate a request signature: " + e.getMessage(), e);
        }
    }
    
    void getV4(String prot, String hoststr, int port, String method, String path,
            final int bandwidth_ms4kB, final Stats stats,String domain,String regionName) throws Exception {
        stats.timeTransStart = System.currentTimeMillis();
        stats.method = method;
        stats.path = path;
        byte[] bytes = ThreadLocalBytes.current().get1KBytes();
        URL url = new URL(prot, hoststr, port, "/" + pool + "/" + path);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        {
            // Step3：构造签名！
            String contentHashString = V4Signer.UNSIGNED_PAYLOAD;
            String dateTimeStamp = SignerUtils.formatTimestamp(new Date().getTime());
            Map<String, String> headers = new HashMap<>();
            headers.put("Content-Type", "text/plain");
            headers.put("x-amz-content-sha256", contentHashString);
            headers.put("Host", domain);
            String authorization = computeSignature(headers, null, contentHashString,  uid, secret, url, "GET", "s3", regionName, dateTimeStamp);
            conn.setRequestProperty("Authorization", authorization);
            conn.setRequestProperty("Content-Type", "text/plain");
            conn.setRequestProperty("Date", dateTimeStamp);
            conn.setRequestProperty("Host", domain);
            conn.setRequestProperty("x-amz-content-sha256", contentHashString);
        }
        conn.setRequestMethod(method);
        conn.setDoInput(true);
        conn.setConnectTimeout(30 * 1000);
        conn.setReadTimeout(120 * 1000);
        conn.setUseCaches(false);
        InputStream in = null;
        int read = 0;
        try {
            stats.timeConnectStart = System.currentTimeMillis();
            conn.connect();
            stats.timeConnectEnd = System.currentTimeMillis();
            /*
             * It is bug! 当调用getInputStream()时，Request Headers are sent and
             * response headers are received.
             */
            stats.timeSentLastByte = stats.timeSentHeaders = System.currentTimeMillis();
            // 必须在getInputStream()前获得respCode，否则getInputStream()会抛异常
            stats.respCode = conn.getResponseCode();
            stats.respMsg = conn.getResponseMessage();
            stats.timeReadFirstByte = System.currentTimeMillis();
            in = conn.getInputStream();
            if (in == null)
                in = conn.getErrorStream();
            stats.objSize = conn.getContentLength();// may be -1.
            int len = 0;
            while ((len = in.read(bytes)) > 0) {
                if (len < 0)
                    break;
                read += len;
                /*
                 * 让其等待足够的时间 if (total >= 0 && read >= total) break;
                 */
                long nextTime = bandwidth_ms4kB * read / BLOCK_SIZE;
                long towait = stats.timeTransStart + nextTime - System.currentTimeMillis();
                if (towait > 0)
                    Thread.sleep(towait);
            }
            stats.timeReadLastByte = System.currentTimeMillis();
            if (stats.objSize != -1)
                assert (stats.objSize == read);
            stats.objSize = read;
            // return read;
        } finally {
            stats.transferredBytes = read;
            try {
                if (in != null)
                    in.close();
            } finally {
                try {
                    conn.disconnect();
                } finally {
                    stats.timeTransEnd = System.currentTimeMillis();
                }
                // 打log,当前时间|格式化的时间|发出请求到收到第一个字节时间|响应码|contentLength
                long now = stats.timeTransEnd;
                log.info("minilog-get: " + now + ","
                        + new SimpleDateFormat("HH:mm:ss").format(new Date(now)) + ","
                        + String.valueOf(stats.timeReadFirstByte - stats.timeSentLastByte) + ","
                        + stats.respCode + "," + read);
                log.info("minilog-get-all: " + stats.toJsonString() + " tranCost=" 
                        + (stats.timeTransEnd - stats.timeConnectStart) + " e2eCost="
                        + (stats.timeReadFirstByte - stats.timeSentLastByte));
            }
        }
    }
    
    void deleteV4(String prot, String hoststr, int port, String path, Stats stats,String domain,String regionName) throws Exception {
        stats.timeTransStart = System.currentTimeMillis();
        String method = stats.method = "DELETE";
        stats.path = path;
        byte[] bytes = ThreadLocalBytes.current().get1KBytes();
        URL url = new URL(prot, hoststr, port, "/" + pool + "/" + path);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        {
            // Step3：构造签名！
            String contentHashString = V4Signer.UNSIGNED_PAYLOAD;
            String dateTimeStamp = SignerUtils.formatTimestamp(new Date().getTime());
            Map<String, String> headers = new HashMap<>();
            headers.put("x-amz-content-sha256", contentHashString);
            headers.put("Host", domain);
            String authorization = computeSignature(headers, null, contentHashString,  uid, secret, url, "DELETE", "s3", regionName, dateTimeStamp);
            conn.setRequestProperty("Authorization", authorization);
            conn.setRequestProperty("Date", dateTimeStamp);
            conn.setRequestProperty("Host", domain);
            conn.setRequestProperty("x-amz-content-sha256", contentHashString);
        }
        int sent = 0;
        InputStream in = null;
        conn.setRequestMethod(method);
        conn.setConnectTimeout(30 * 1000);
        try {
            stats.timeConnectStart = System.currentTimeMillis();
            conn.connect();
            stats.timeConnectEnd = System.currentTimeMillis();
            stats.timeSentLastByte = stats.timeSentHeaders = System.currentTimeMillis();
            in = conn.getInputStream();
            if (in == null)
                in = conn.getErrorStream();
            stats.respCode = conn.getResponseCode();
            stats.respMsg = conn.getResponseMessage();
            stats.timeReadFirstByte = System.currentTimeMillis();
            int len = 0;
            while ((len = in.read(bytes)) > 0)
                ;
            sent += len;
            stats.timeReadLastByte = System.currentTimeMillis();
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        } finally {
            stats.transferredBytes = sent;
            try {
                if (null != in)
                    in.close();
            } finally {
                try {
                    if (null != conn)
                        conn.disconnect();
                } finally {
                    stats.timeTransEnd = System.currentTimeMillis();
                }
            }
            log.info("minilog-delete-all: " + stats.toJsonString() + " tranCost=" 
                    + (stats.timeTransEnd - stats.timeConnectStart) + " e2eCost="
                    + (stats.timeReadFirstByte - stats.timeSentLastByte));
        }
    }
    
    static int KB = 1024;
    static int MB = 1024 * 1024;
    static final int BLOCK_SIZE = 4 * KB;
    static Random rand = new Random(Long.MAX_VALUE);
    
    // 对于大流量，要去掉12，24MB，对于小流量，要加上
    private static int getLength() {
        int rint = rand.nextInt(100);
        int length;
        if (rint < 5)
            length = 4 * KB;
        else if (rint < 6)
            length = 8 * KB;
        else if (rint < 8)
            length = 16 * KB;
        else if (rint < 14)
            length = 32 * KB;
        else if (rint < 24)
            length = 64 * KB;
        else if (rint < 31)
            length = 128 * KB;
        else if (rint < 39)
            length = 256 * KB;
        else if (rint < 49)
            length = 512 * KB;
        else if (rint < 61)
            length = MB;
        else if (rint < 88)
            length = 2 * MB;
        else if (rint < 94)
            length = 4 * MB;
        else if (rint < 95)
            length = 12 * MB;
        else if (rint < 99)
            length = 24 * MB;
        else
            length = 2 * MB;
        return length;
    }
    
    public static void main(String[] args) throws Exception {
        System.setProperty("sun.net.http.allowRestrictedHeaders", "true");
        GetOpt opts = new GetOpt(
                "[help][ana]:[loglinestart]:[loglineoffset]:[logoutdir]:[capacity]:[prot]:h:[port]:[pool]:[storage]:u:p:c:r:n:s:b:w:d:t:[stc]:[domain]:[seed]:[region]:[payloadhashenabled]",
                args);
        if (opts.hasOpt("help")) {
            System.out
                    .println("-help -ana file"
                            + "-prot protocol -h hostname -port port "
                            + "-u uid -p secret -c max-conn-num -r ramp-up-period "
                            + "-n object-num -s object-size -b bandwidth(ms/4KB) -w writeRatio -d del-conn-num -t put-get-time");
            System.out.println("  -help : print this help.");
            System.out
                    .println("  -ana  : analysis from log file. If the log file is not specified, using stdin.");
            System.out.println("  -logoutdir : http or https. default is https.");
            System.out.println("  -loglinestart : the start line of log");
            System.out
                    .println("  -loglineoffset : the offset from the start line specified in argument loglinestart.");
            System.out.println("  -capacity : the maximume size of the comparator.");
            System.out.println("  -prot : http or https. default is https.");
            System.out.println("  -h : the hostname of atmos.");
            System.out.println("  -port : the port of atmos.");
            System.out.println("  -pool : the pool of atmos.");
            System.out.println("  -u : the uid of atmos.");
            System.out.println("  -p : the secret of atmos.");
            System.out.println("  -c : maxinum number of thread.");
            System.out.println("  -r (s): ramp up period(s) to start the threads set forth.");
            System.out.println("  -n : how many objects to post per connection.");
            System.out.println("  -s (KB) : the size of each object to post.");
            System.out.println("  -b (ms/4KB): bandwidth to post the object.");
            System.out.println("  -w : write/read ratio");
            System.out.println("  -d : maxinum number of delete thread.");
            System.out.println("  -t : time of put/get operation.");
            System.out.println("  -seed : set random seed");
            System.out.println("  -region : set region name for signature V4.");
            System.out.println("  -payloadhashenabled : payloadhashenabled or not for signature V4. default is enabled.");
            System.exit(0);
        }
        if (opts.hasOpt("ana")) {
            String logfile = opts.getOpt("ana");
            String logoutdir = opts.getOpt("logoutdir");
            int loglinestart = opts.getInt("loglinestart", 1);
            int loglineoffset = opts.getInt("loglineoffset", Integer.MAX_VALUE / 2);
            int capacity = opts.getInt("capacity", 100000);
            BufferedReader r = null;
            try {
                if (logoutdir == null) {
                    System.out.println("logoutdir must be specified!");
                    System.exit(1);
                }
                if (logfile == null) {
                    // read from stdin
                    r = new BufferedReader(new InputStreamReader(System.in, "UTF-8"));
                } else
                    r = new BufferedReader(new FileReader(logfile));
                // new BenchLogAnalysis(capacity).analysisLog(r, loglinestart,
                // loglineoffset,
                // logoutdir);
                r.close();
            } catch (Exception e) {
                log.error("Exception when analysis log.", e);
            } finally {
                System.exit(0);
            }
        }
        final String prot = opts.getOpt("prot", "http");
        final String[] hoststr = opts
                .getOpt("h", "api-las.atmosonline.com,api-las.atmosonline.com").split(",");
        final String[] port = opts.getOpt("port", "80").split(",");
        uid = opts.getOpt("u", "fb7ff589a61841ddbc74af35bdcf7add/A375822969bfde1f3d45");// "fb7ff589a61841ddbc74af35bdcf7add/A375822969bfde1f3d45");
        secret = opts.getOpt("p", "06Rb0VioCVeRIBeNSNPzHmrVEJQ=");// "06Rb0VioCVeRIBeNSNPzHmrVEJQ=");
        final int maxConnNum = opts.getInt("c", 2);
        System.out.println("maxConnNum = " + maxConnNum);
        pool = opts.getOpt("pool", "oos-mini-bench-client-pool");
        System.out.println("pool = " + pool);
        final int ramp_ms = opts.getInt("r", 1) * 1000;
        System.out.println("ramp_ms = " + ramp_ms);
        final int postCount = opts.getInt("n", 300);// 要是6的倍数，这样最后删除的时候，才能全部删除
        System.out.println("postCount = " + postCount);
        final int postSize_kB = opts.getInt("s", 4);
        System.out.println("postSize_kB = " + postSize_kB);
        final int bandwidth_ms4kB = opts.getInt("b", 0);
        System.out.println("bandwidth_ms4kB = " + bandwidth_ms4kB);
        final double ratio = opts.getDouble("w", 1);// w=1 写，w=0.05 读
        System.out.println("write ratio = " + ratio);
        final int delNum = opts.getInt("d", 2);
        System.out.println("delNum = " + delNum);
        final long time = opts.getLong("t", 10); // 单位秒
        System.out.println("time = " + time);
        final String stc = opts.getOpt("stc", "EC_2_0");
        System.out.println("storage = " + stc);
        final String domain = opts.getOpt("domain", "oos-hq-bj.ctyunapi.cn");
        System.out.println("domain = " + domain);
        // regionName，用于V4签名，如domain=oos-hz.ctyuanapi.cn，则regionName=hz
        final String regionName = opts.getOpt("region", "cn");
        System.out.println("regionName = " + regionName);
        // V4签名的时候，是否对内容做签名
        final boolean payloadHashEnabled = opts.hasOpt("payloadhashenabled") ? true : false;
        System.out.println("payloadhashenabled = " + payloadHashEnabled);
        final long seed = opts.getLong("seed", 0l);
        System.out.println("seed = " + seed);
        Random rand = new Random(seed);
        final AdaptorClientV4 adaptor = new AdaptorClientV4();
        for (String p : port)
            adaptor.init(maxConnNum, Integer.parseInt(p));
        final AtomicLong total = new AtomicLong();
        final AtomicLong writeSuc = new AtomicLong();
        final AtomicLong writeFail = new AtomicLong();
        final AtomicLong readSuc = new AtomicLong();
        final AtomicLong readFail = new AtomicLong();
        final AtomicLong deleteSuc = new AtomicLong();
        final AtomicLong deleteFail = new AtomicLong();
        final TimeStat stat = new TimeStat();
        Thread t = new Thread() {
            @Override
            public void run() {
                for (;;) {
                    synchronized (stat) {
                        log.info("Stat =" + stat);
                    }
                    log.info("Total = " + total.get());
                    log.info("write: Success=" + writeSuc.get() + "/Failure=" + writeFail.get());
                    log.info("readSuc = " + readSuc.get() + "/Failure=" + readFail.get());
                    log.info("deleteSuc = " + deleteSuc.get() + "/Failure=" + deleteFail.get());
                    try {
                        Thread.sleep(3000);
                    } catch (InterruptedException e) {
                    }
                }
            }
        };
        t.setDaemon(true);
        t.start();
        BlockingExecutor pool = new BlockingExecutor(maxConnNum, maxConnNum, maxConnNum, 5000,
                "Post-ThreadPool-");
        // final double[] binomial = Utils.getBinomial(maxConnNum * postCount,
        // ratio);
        long putStart = System.currentTimeMillis();
        for (int i = 0; i < maxConnNum; i++) {
            // 确保平均在ramp_ms内均匀的启动
            int r = tlrand.get().nextInt(hoststr.length);
            final String host = hoststr[r];
            r = tlrand.get().nextInt(port.length);
            final int p = Integer.parseInt(port[r]);
            pool.execute(new Runnable() {
                public void run() {
                    Random random = new Random();
                    try {
                        Thread.sleep(random.nextInt(ramp_ms));
                    } catch (InterruptedException e1) {
                    }
                    for (int i = 0; i < postCount; i++) {
                        boolean isWrite = random.nextDouble() <= ratio ? true : false;
                        boolean needSleep = false;
                        if (isWrite) {
                            String key;
                            synchronized (rand) {
                                key = generateFileName(rand);
                            }
                            String path = key;
                            Stats stats = new Stats();
                            try {
                                int size = 0;
                                if (postSize_kB == 0)
                                    size = getLength();
                                else
                                    size = postSize_kB * KB;
                                // 在外面控制等待的时间
                                long start = System.currentTimeMillis();
                                adaptor.postV4(prot, host, p, path, size, bandwidth_ms4kB, stats,
                                        stc, domain, regionName, payloadHashEnabled);
                                long supposedEnd = start
                                        + (bandwidth_ms4kB * stats.transferredBytes / BLOCK_SIZE);
                                long towait = supposedEnd - System.currentTimeMillis();
                                if (towait > 0)
                                    Thread.sleep(towait);
                            } catch (Exception e) {
                                stats.exceptionMsg = e.getMessage();
                                log.error("Post URL failed: " + path, e);
                            } finally {
                                log.info("JSON LOG: " + stats.toJsonString());
                                log.info("CSV LOG: " + stats.toStringForCSV());
                                synchronized (stat) {
                                    stat.record("PostTimeCost", stats.timeTransEnd
                                            - stats.timeTransStart);
                                }
                                total.incrementAndGet();
                                if (stats.respCode == 200) {
                                    writeSuc.incrementAndGet();
                                    synchronized (list) {
                                        list.add(key);
                                    }
                                } else
                                    writeFail.incrementAndGet();
                            }
                        } else {
                            String key = null;
                            synchronized (list) {
                                if (list.size() > 0) {
                                    int n = random.nextInt(list.size());
                                    key = list.get(n);
                                } else {
                                    i--;
                                    needSleep = true;
                                }
                            }
                            if (needSleep) {
                                try {
                                    Thread.sleep(1000);
                                } catch (InterruptedException e1) {
                                }
                                continue;
                            }
                            String path = key;
                            Stats stats = new Stats();
                            try {
                                synchronized (readingList) {
                                    readingList.add(key);
                                }
                                // 在外面控制等待的时间
                                long start = System.currentTimeMillis();
                                adaptor.getV4(prot, host, p, "GET", path, bandwidth_ms4kB, stats,domain,regionName);
                                long supposedEnd = start
                                        + (bandwidth_ms4kB * stats.transferredBytes / BLOCK_SIZE);
                                long towait = supposedEnd - System.currentTimeMillis();
                                if (towait > 0)
                                    Thread.sleep(towait);
                            } catch (Exception e) {
                                stats.exceptionMsg = e.getMessage();
                                log.error("Get URL failed: " + path, e);
                            } finally {
                                log.info("JSON LOG: " + stats.toJsonString());
                                log.info("CSV LOG: " + stats.toStringForCSV());
                                total.incrementAndGet();
                                synchronized (stat) {
                                    stat.record("GetTimeCost", stats.timeTransEnd
                                            - stats.timeTransStart);
                                }
                                if (stats.respCode == 200)
                                    readSuc.incrementAndGet();
                                else
                                    readFail.incrementAndGet();
                                synchronized (readingList) {
                                    readingList.remove(key);
                                }
                            }
                        }
                    }
                }
            });
        }
        while (true) {
            if (total.get() >= postCount * maxConnNum
                    || (System.currentTimeMillis() - putStart) > time * 1000)// 为了可以通过修改zk，来动态决定什么时候开始delete，借用一下这个zk配置
                break;
            else
                Thread.sleep(1000);
        }
        BlockingExecutor delPool = new BlockingExecutor(delNum, delNum, delNum, 5000,
                "Delete-ThreadPool-");
        long putEnd = System.currentTimeMillis();
        int size = list.size();
        final List<String> list2 = new ArrayList<String>();
        for (int j = 0; j < size; j++) {
            list2.add(list.get(j));
        }
        for (int j = 0; j < size; j++) {
            final int j2 = j;
            int r = tlrand.get().nextInt(hoststr.length);
            final String host = hoststr[r];
            r = tlrand.get().nextInt(port.length);
            final int p = Integer.parseInt(port[r]);
            delPool.execute(new Runnable() {
                public void run() {
                    Stats stats = new Stats();
                    String key = list2.get(j2);
                    String path = key;
                    synchronized (list) {
                        if (!readingList.contains(key))
                            list.remove(key);
                        else
                            return;
                    }
                    try {
                        adaptor.deleteV4(prot, host, p, path, stats,domain,regionName);
                    } catch (Exception e) {
                        log.error("DELETE URL failed: " + stats.path, e);
                    } finally {
                        log.info("JSON LOG: " + stats.toJsonString());
                        log.info("CSV LOG: " + stats.toStringForCSV());
                        total.incrementAndGet();
                        synchronized (stat) {
                            stat.record("DelTimeCost", System.currentTimeMillis()
                                    - stats.timeTransStart);
                        }
                        if (stats.respCode == 204) {
                            deleteSuc.incrementAndGet();
                        } else
                            deleteFail.incrementAndGet();
                    }
                }
            });
        }
        long deleteEnd = System.currentTimeMillis();
        delPool.shutdown();
        delPool.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
        if (delPool.isShutdown())
            pool.shutdownNow();
        // pool.shutdown();
        // pool.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
        synchronized (stat) {
            log.info("Stat =" + stat);
        }
        log.info("Total = " + total.get());
        log.info("writeSuc = " + writeSuc.get());
        log.info("readSuc = " + readSuc.get());
        log.info("deleteSuc = " + deleteSuc.get());
        log.info("put time:" + String.valueOf(putEnd - putStart) + " delete time:"
                + String.valueOf(deleteEnd - putEnd));
    }
    
    private static ThreadLocal<SecureRandom> tlrand = new ThreadLocal<SecureRandom>() {
        @Override
        protected SecureRandom initialValue() {
            return new SecureRandom();
        }
    };
}