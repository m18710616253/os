package cn.ctyun.oos.tempo;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.security.MessageDigest;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.IOUtils;
import org.json.JSONObject;

import com.amazonaws.AmazonClientException;
import com.amazonaws.DefaultRequest;
import com.amazonaws.auth.SigningAlgorithm;
import com.amazonaws.services.s3.Headers;
import com.amazonaws.services.s3.internal.RestUtils;
import com.amazonaws.util.BinaryUtils;

import common.time.TimeUtils;

public class PutObjectForCDN {
    static String ak = "09ad52b4916fc9fddddf";
    static String sk = "a438d57963709c9727e472dce9c417ef62cd9419";
    // user access key
    static String userAk = "51d06c609213af7d3770";
    static String host = "oos-gz.ctyunapi.cn";
    static int port = 9080;
    // your local file
    static File file = new File("d:\\test\\client_in.txt");
    // your bucket
    static String bucket = "test3";
    // your object
    static String object = "1.txt";

    public static void main(String[] args) throws Exception {
        PutObjectForCDN demo = new PutObjectForCDN();
        demo.putObject();
    }

    private void putObject() throws Exception {
        URL url = new URL("http", host, 80, "/" + bucket + "/" + object);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setFixedLengthStreamingMode(file.length());
        conn.setRequestProperty("Date", TimeUtils.toGMTFormat(new Date()));
        String resourcePath = "/" + bucket + "/" + object;
        @SuppressWarnings("unchecked")
        String canonicalString = RestUtils.makeS3CanonicalString("PUT", resourcePath,
                new Request(conn), null);
        String authorization = "AWS " + userAk + ":" + makeSignature(canonicalString);
        conn.setRequestProperty("Authorization", authorization);
        conn.setConnectTimeout(10 * 1000);
        conn.setReadTimeout(10 * 1000);
        conn.setDoOutput(true);
        conn.setRequestMethod("PUT");
        conn.connect();
        try (OutputStream out = conn.getOutputStream()) {
            byte[] buff = new byte[1024];
            int c;
            try (FileInputStream fileIn = new FileInputStream(file)) {
                while ((c = fileIn.read(buff)) != -1) {
                    out.write(buff, 0, c);
                }
            }
            out.flush();
        }
        int code = conn.getResponseCode();
        System.out.println("put object response code:" + code);
    }

    private String makeSignature(String stringToSign) throws Exception {
        String path = "makeSignature";
        URL url = new URL("http", host, port, "/" + path);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestProperty("Date", TimeUtils.toGMTFormat(new Date()));
        conn.setRequestProperty("Content-Type", "text/plain");
        conn.setRequestProperty("x-amz-accessKey", userAk);
        MessageDigest md5 = MessageDigest.getInstance("MD5");
        md5.update(stringToSign.getBytes());
        conn.setRequestProperty(Headers.CONTENT_MD5, BinaryUtils.toBase64(md5.digest()));
        conn.setFixedLengthStreamingMode(stringToSign.length());
        conn.setRequestMethod("PUT");
        conn.setDoOutput(true);
        conn.setConnectTimeout(10 * 1000);
        conn.setReadTimeout(10 * 1000);
        String resourcePath = "/" + path;
        @SuppressWarnings("unchecked")
        String canonicalString = RestUtils.makeS3CanonicalString("PUT", resourcePath,
                new Request(conn), null);
        String signature = sign(canonicalString, sk, SigningAlgorithm.HmacSHA1);
        String authorization = "AWS " + ak + ":" + signature;
        conn.setRequestProperty("Authorization", authorization);
        conn.connect();
        try (OutputStream out = conn.getOutputStream()) {
            out.write(stringToSign.getBytes());
        }
        int code = conn.getResponseCode();
        System.out.println("make signature response code:" + code);
        if (code == 200)
            try (InputStream input = conn.getInputStream()) {
                String res = IOUtils.toString(input);
                JSONObject json = new JSONObject(res);
                return json.getString("signature");
            }
        else
            return null;
    }

    private String sign(String data, String key, SigningAlgorithm algorithm) {
        // refer to com.amazonaws.services.s3.internal.S3Signer
        try {
            Mac mac = Mac.getInstance(algorithm.toString());
            mac.init(new SecretKeySpec(key.getBytes("UTF-8"), algorithm.toString()));
            byte[] bs = mac.doFinal(data.getBytes("UTF-8"));
            return new String(Base64.encodeBase64(bs), "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new AmazonClientException(
                    "Unable to calculate a request signature: " + e.getMessage(), e);
        } catch (Exception e) {
            throw new AmazonClientException(
                    "Unable to calculate a request signature: " + e.getMessage(), e);
        }
    }
}

class Request<T> extends DefaultRequest<T> {
    public Request(String serviceName) {
        super(serviceName);
    }

    public Request(final HttpURLConnection req) {
        super(null);
        Map<String, List<String>> header = req.getRequestProperties();
        Iterator<String> keys = header.keySet().iterator();
        while (keys.hasNext()) {
            String k = keys.next();
            String v = header.get(k).get(0);
            k = k.replace("x-ctyun-", "");
            this.getHeaders().put(k, v);
        }
    }
}