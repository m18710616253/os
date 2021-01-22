package cn.ctyun.oos.server.processor.image.antispam;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.http.client.HttpClient;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import cn.ctyun.common.Consts;
import cn.ctyun.oos.server.conf.ContentSecurityConfig;

public class NeteaseDun {
    private static final Log log = LogFactory.getLog(NeteaseDun.class);
    
    private final static int TYPE_IMAGE_URL = 1;
    private final static int TYPE_IMAGE_BASE64 = 2;
    
    private final static int LABEL_PORN_ID = 100;
    private final static int LABEL_SEXY_ID = 110;
    
    private final static String LABEL_PORN_STR = "porn";
    private final static String LABEL_SEXY_STR = "sexy";
    private final static String LABEL_NORMAL_STR = "normal";
    
    private final static int ACTION_NORMAL = 1;
    private final static int ACTION_TRASH = 2;
    private final static int ACTION_NOTSURE = 3;
    
    /** 实例化HttpClient，发送http请求使用，可根据需要自行调参 */
    private static HttpClient httpClient = HttpClient4Utils.createHttpClient(
            ContentSecurityConfig.socketTimeout,
            ContentSecurityConfig.connectTimeout,
            ContentSecurityConfig.connectionRequestTimeout);
    
    /**
    * 生成签名信息
    * @param secretKey 产品私钥
    * @param params 接口请求参数名和参数值map，不包括signature参数名
    * @return
    * @throws UnsupportedEncodingException
    */
    public static String genSignature(String secretKey, Map<String, String> params) throws UnsupportedEncodingException {
        // 1. 参数名按照ASCII码表升序排序
        String[] keys = params.keySet().toArray(new String[0]);
        Arrays.sort(keys);

        // 2. 按照排序拼接参数名与参数值
        StringBuffer paramBuffer = new StringBuffer();
        for (String key : keys) {
            paramBuffer.append(key).append(params.get(key) == null ? "" : params.get(key));
        }
        // 3. 将secretKey拼接到最后
        paramBuffer.append(secretKey);

        // 4. MD5是128位长度的摘要算法，用16进制表示，一个十六进制的字符能表示4个位，所以签名后的字符串长度固定为32个十六进制字符。
        return DigestUtils.md5Hex(paramBuffer.toString().getBytes(Consts.CS_UTF8));
    }
    /**
     * 对图片进行鉴黄
     * @param sectetId
     * @param businessId
     * @param secretKey
     * @param imageUrl
     * @return
     * @throws JSONException
     * @throws IOException
     */
    public static String decodeImage(String sectetId,String businessId,String secretKey,String key, InputStream input) throws JSONException, IOException{
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        IOUtils.copy(input, output);
        byte[] bts = output.toByteArray();
        
        JSONArray imageJsonArray = new JSONArray();
        JSONObject image1 = new JSONObject();
        image1.put("name", key);
        image1.put("type", TYPE_IMAGE_BASE64);
        image1.put("data", Base64.encodeBytes(bts));
        imageJsonArray.put(image1);
        return decodeImage(sectetId,businessId,secretKey,imageJsonArray);
    }
    public static String decodeImage(String sectetId,String businessId,String secretKey,String imageUrl) throws JSONException, IOException{
        return decodeImage(sectetId, businessId, secretKey, new String[]{imageUrl});
    }
    public static String decodeImage(String sectetId,String businessId,String secretKey,String[] imageUrls) throws JSONException, IOException{
        JSONArray imageJsonArray = new JSONArray();
        // 传图片url进行检测，name结构产品自行设计，用于唯一定位该图片数据
        for(String imageUrl:imageUrls){
            JSONObject image1 = new JSONObject();
            image1.put("name", imageUrl);
            image1.put("type", TYPE_IMAGE_URL);
            image1.put("data", imageUrl);
            imageJsonArray.put(image1);
        }
        return decodeImage(sectetId,businessId,secretKey,imageJsonArray);
    }
    public static String decodeImage(String sectetId,String businessId,String secretKey,JSONArray imageJsonArray) throws JSONException, IOException{
        long begin = System.currentTimeMillis();
        Map<String, String> params = new HashMap<String, String>();
        // 1.设置公共参数
        params.put("secretId", sectetId);
        params.put("businessId", businessId);
        params.put("version", "v3");
        params.put("timestamp", String.valueOf(System.currentTimeMillis()));
        params.put("nonce", String.valueOf(new Random().nextInt()));

        
        params.put("images", imageJsonArray.toString());
        //可选参数
//        params.put("account", ACCOUNT);
//        params.put("ip", IP);
        // 3.生成签名信息
        String signature = genSignature(secretKey, params);
        params.put("signature", signature);
        // 4.发送HTTP请求，这里使用的是HttpClient工具包，产品可自行选择自己熟悉的工具包发送请求
        String response = HttpClient4Utils.sendPost(httpClient, ContentSecurityConfig.imageApiUrlNeteaseDun, params, Consts.CS_UTF8);
        
        log.info("decodeImagePorn ret:"+response +" timemills:"+(System.currentTimeMillis() - begin));
        // 5.解析接口返回值
        JSONObject respJsonObject = new JSONObject(response);
        int code = respJsonObject.getInt("code");
        String msg = respJsonObject.getString("msg");
        
        JSONObject resultJsonObject = new JSONObject();
        resultJsonObject.put("code", code);
        if (code == 200) {
            resultJsonObject.put("msg", "OK");
            JSONArray respResultArray = respJsonObject.getJSONArray("result");
            JSONArray resultArray = new JSONArray();
            for(int i=0; i< respResultArray.length();i++){
                JSONObject jObject = respResultArray.getJSONObject(i); 
                String name = jObject.getString("name");
                String taskId = jObject.getString("taskId");
                JSONArray labelArray = jObject.getJSONArray("labels");
                log.info(String.format("taskId=%s，name=%s，labels：", taskId, name));
                for(int j=0; j< labelArray.length();j++){
                    JSONObject lObject = labelArray.getJSONObject(j); 
                    int label = lObject.getInt("label");
                    if(label == LABEL_PORN_ID || label == LABEL_SEXY_ID){
                        int level = lObject.getInt("level");
                        double rate = lObject.getDouble("rate");
                        if(rate > 0){
                            JSONObject resultObject = new JSONObject();
                            resultObject.put("rate", rate);
                            String labelStr = null;
                            if(label == LABEL_PORN_ID){
                                labelStr = LABEL_PORN_STR;
                            }else{
                                labelStr = LABEL_SEXY_STR;
                            }
                            resultObject.put("label", labelStr);
                            resultObject.put("review", level==1 ? true : false);
                            resultArray.put(resultObject);
                        }
                    }
                }
                if(resultArray.length()==0){
                    JSONObject resultObject = new JSONObject();
                    resultObject.put("rate", 1);
                    resultObject.put("label", LABEL_NORMAL_STR);
                    resultObject.put("review", false);
                    resultArray.put(resultObject);
                }
                resultJsonObject.put("result", resultArray);
            }
            
        } else {
            resultJsonObject.put("msg", msg);
        }
        return resultJsonObject.toString();
    }
    
    
    public static String decodeText(String sectetId,String businessId,String secretKey,String text) throws JSONException, IOException{
        String dataId = UUID.randomUUID().toString();
        long begin = System.currentTimeMillis();
        Map<String, String> params = new HashMap<String, String>();
        // 1.设置公共参数
        params.put("secretId", sectetId);
        params.put("businessId", businessId);
        params.put("version", "v3");
        params.put("timestamp", String.valueOf(System.currentTimeMillis()));
        params.put("nonce", String.valueOf(new Random().nextInt()));

        // 2.设置私有参数
        params.put("dataId", dataId);
        params.put("content", text);
        //可选参数
//        params.put("dataType", "1");
//        params.put("ip", "123.115.77.137");
//        params.put("account", "java@163.com");
//        params.put("deviceType", "4");
//        params.put("deviceId", "92B1E5AA-4C3D-4565-A8C2-86E297055088");
//        params.put("callback", "ebfcad1c-dba1-490c-b4de-e784c2691768");
//        params.put("publishTime", String.valueOf(System.currentTimeMillis()));

        // 3.生成签名信息
        String signature = genSignature(secretKey, params);
        params.put("signature", signature);

        // 4.发送HTTP请求，这里使用的是HttpClient工具包，产品可自行选择自己熟悉的工具包发送请求
        String response = HttpClient4Utils.sendPost(httpClient, ContentSecurityConfig.textApiUrlNeteaseDun, params, Consts.CS_UTF8);
        System.out.println(response);
        log.info("decodeText ret:"+response +" timemills:"+(System.currentTimeMillis() - begin));
        // 5.解析接口返回值
        JSONObject respJsonObject = new JSONObject(response);
        int code = respJsonObject.getInt("code");
        String msg = respJsonObject.getString("msg");
        
        JSONObject resultJsonObject = new JSONObject();
        resultJsonObject.put("code", code);
        if (code == 200) {
            resultJsonObject.put("msg", "OK");
            JSONObject respResultObject = respJsonObject.getJSONObject("result");
            int action = respResultObject.getInt("action");
//            String taskId = respResultObject.getString("taskId");
            JSONArray labelArray = respResultObject.getJSONArray("labels");
            
            int resultAction = -1;
            int maxLevel = 0;
            int labelId = -1;
            String resultLabel = "normal";
            if (action == 0) {
                resultAction = ACTION_NORMAL;
            } else if (action == 1) {
                resultAction = ACTION_NOTSURE;
            } else if (action == 2) {
                resultAction = ACTION_TRASH;
            }
            for(int j=0; j< labelArray.length();j++){
                JSONObject lObject = labelArray.getJSONObject(j); 
                int label = lObject.getInt("label");
                int level = lObject.getInt("level");
                if(level > maxLevel){
                    maxLevel = level;
                    labelId = label;
                }
            }
            //100：色情，200：广告，400：违禁，600：谩骂，700：灌水
            switch(labelId){
            case 100:
                resultLabel = "porn";
                break;
            case 200:
                resultLabel = "ad";
                break;
            case 400:
                resultLabel = "contraband";
                break;
            case 600:
                resultLabel = "abuse";
                break;
            case 700:
                resultLabel = "flood";
                break;
            }
            JSONObject jObject = new JSONObject();
            jObject.put("action", resultAction);
            jObject.put("label", resultLabel);
            resultJsonObject.put("result", jObject);
        } else {
            resultJsonObject.put("msg", msg);
        }
    
        return resultJsonObject.toString();
    }
    
    /**
     * 
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        /** 产品密钥ID，产品标识 */
        String SECRETID = "74a3f2ccf226c13b3bc10539c4b190a8";
        /** 产品私有密钥，服务端生成签名信息使用，请严格保管，避免泄露 */
        String SECRETKEY = "5bb50ec90865c9f23b2b93b536207e86";
        /** 业务ID，易盾根据产品业务特点分配 */
        String IMAGE_BUSINESSID = "c4a0f945121d2df6aa7ceba2c6443b8a";
        /** 易盾反垃圾云服务文本在线检测 业务ID */
        String TEXT_BUSINESSID = "f2e457b7a913c0de7788015dcabd409f";
//        String response = decodeImage(SECRETID, IMAGE_BUSINESSID, SECRETKEY, "http://nos.netease.com/yidun/2-0-0-4038669695e344a4addc546f772e90a5.jpg");
//        System.out.println(response);
        
        String textResponse = decodeText(SECRETID, TEXT_BUSINESSID, SECRETKEY, "一切皆有可能。李宁");
        System.out.println(textResponse);
    }
}
