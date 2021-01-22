package cn.ctyun.oos.website;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.security.MessageDigest;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.codec.binary.Base64;
import org.json.JSONObject;

import common.util.MD5Hash;

public class ConnectBss {
    
    static String[] accountIdList = new String[] {"000195494100432c8366fe5d0ea5c599",
    "0003c9447685478e96d4e6478400767d",
    "000b4df0a86a431e8e18760733c877ac",
    "000e95e869a444b2b6bb0ceb2d7e7070",
    "0011f2d83bf547c69e259253669cec88",
    "001c25bb9aa8452babaeb45642cfebbf",
    "001c63570a844f11ba08e1460e3cebc1",
    "0030e0ba398744d0b9b192b954334d45",
    "00388a901c024e21bc6e33189d7a6ab1",
    "003a9e2048e7469db1a530c69fe9d103",
    "0053a3c77f834a21a4869b50905d747d",
    "00567d90399740ec89527cbdf2e4ecb2",
    "005831bde1b14d2ea4c9304535384fe5",
    "005dee534aed44be9ad64a6aa13f0d2c",
    "0068723d2c9c4f98a8fc31bacc5572f3",
    "006e07a0e93f4cab8ec8cf79c5c411d9",
    "00732f3059834078a05dab846ece5ed6",
    "007447705ba74f298c604b71968c7d9e",
    "007d92ca021f43a9b7c6b6f9ae5ed48c",
    "008277fd51694a39a1eceb46b9ab4e8b",
    "0098fbe1147a42279b47e47021d4cdaf",
    "00990e05a3db420db34ea4719e3f5f70",
    "009b3486320f415ea26c7e1c45389f1c",
    "00ae4b72062d4176914490d2086922c1",
    "00b075a7c26c47c3b939d49b191ba66c",
    "00c1ce0fc84349cc85330b38b92643d8",
    "00c30b669a13463b865d1b1c43f148a4",
    "00d1e8d9d5214f028716e11145828d8a",
    "00d7925d1e5d45bc9bae0b0e6f11fe7b",
    "00e0ad4527334f839c65cbd74c5d9bbb"};
    
    public static void main(String[] args) throws Exception {
          System.out.println(URLEncoder.encode("https://cda.ctyun.cn/"));
          String str = "http://www.ctyun.cn/login?service=https%3A%2F%2Foos-cn.ctyun.cn%2Foos%2Fctyun%2FresourcePackage.html";
        for (String accountId: accountIdList) {
            JSONObject customInfo = new JSONObject();
            customInfo.put("type", 2);
            JSONObject identity = new JSONObject();
            identity.put("accountId", accountId);
            customInfo.put("identity", identity);
            URL url = new URL("https", "mock.yonyoucloud.com", 443, "/mock/6432/apiproxy/v3/oos/resourceList");
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestProperty("host", "bss-acct.ctyun.cn");
            conn.setRequestProperty("customInfo", customInfo.toString());
            conn.setRequestProperty("accessKey", "5c73b8181b09473db3905813833b4b27");
            String contentMD5 = getMD5Base64("");
            // 注意：contentMD5Source 中多个参数以 \n 分割，参数拼接时需要使用 "\\n" !!!
            conn.setRequestProperty("contentMD5", contentMD5);
            Date date = new Date();
            String requestDate = new SimpleDateFormat("EEE, d MMM yyyy HH:mm:ss z", Locale.ENGLISH).format(date);
            conn.setRequestProperty("requestDate", requestDate);
            // 注意：calculateHMAC中待加密字符串 多个参数以 \n 分割，参数拼接时需要使用 "\n" !!!
            String hmac = sign(contentMD5+"\n"+requestDate+"\n"+"/apiproxy/v3/oos/resourceList", "65ebb6a694154478bbf5812c6400eaf3");
            conn.setRequestProperty("hmac", hmac);
            conn.setRequestProperty("platform", "3");
            
            conn.setRequestMethod("GET");
            conn.setUseCaches(false);
            conn.setDoInput(true);
            conn.connect();
            
            System.out.println(conn.getResponseCode());
            
            
            
            
            byte[] bytes = new byte[1024];
            StringBuilder sb = new StringBuilder();
            InputStream input = conn.getInputStream();
            int i;
            while((i = input.read(bytes, 0, 1024)) > -1) {
                sb.append(new String(bytes), 0, i);
            }
            System.out.println(sb);
        }
    }
    
    public static String getMD5Base64(String contentMD5Source) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.update(contentMD5Source.getBytes());
            byte[] digest = md.digest();
            String result = new String(Base64.encodeBase64(digest));
            return result;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
    
    public static String sign(String data, String key) throws Exception {
        // refer to com.amazonaws.services.s3.internal.S3Signer
        try {
            Mac mac = Mac.getInstance("HmacSHA1");
            mac.init(new SecretKeySpec(key.getBytes("UTF-8"), "HmacSHA1"));
            byte[] bs = mac.doFinal(data.getBytes("UTF-8"));
            return new String(Base64.encodeBase64(bs), "UTF-8");
        }  catch (Exception e) {
            throw new Exception("Unable to calculate a request signature: ");
        }
    }

}
