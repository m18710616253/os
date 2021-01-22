package cn.ctyun.oos.server.formpost;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.Map;

import com.amazonaws.services.s3.internal.XmlWriter;

import cn.ctyun.common.BaseException;
import cn.ctyun.common.Consts;
import cn.ctyun.common.MultipartInputStream;
import cn.ctyun.oos.metadata.BucketMeta;
import cn.ctyun.oos.metadata.ObjectMeta;
import cn.ctyun.oos.server.formpost.exceptions.InvalidArgumentException;
import cn.ctyun.oos.server.formpost.exceptions.MetadataTooLargeException;

public class FormPostTool {

    /**
     * 从流中读取file前面的字段
     * 注意：同时将bucket也放入参数map中，方便进行policy校验  (S3验证了Bucket,即Policy文档中必须有bucket)
     * @param ip 输入流
     * @param formPostHeaderNotice 保存读取的字段
     * @param boundary  header里面指明的分割字符串
     * @return
     * @throws BaseException 如果字段超过长度限制抛出此异常
     * @throws IOException
     */
    public static InputStream readFormFields(InputStream ip,
            FormPostHeaderNotice formPostHeaderNotice, byte[] boundary)
                    throws BaseException, IOException {
        MultipartInputStream multipartInputStream = new MultipartInputStream(ip, boundary, formPostHeaderNotice);
        FormPostInputStream fos = new FormPostInputStream(multipartInputStream, formPostHeaderNotice);
        try{
            ip = new BufferedInputStream(fos);
            ip.mark(1);
            ip.read();
            ip.reset();
        }catch(IOException e){
            if(e instanceof MetadataTooLargeException){
                MetadataTooLargeException me = (MetadataTooLargeException)e;
                throw new BaseException(400,me.code,me.message);
            }else{
                throw e;
            }
        }
        return ip;
    }
    /**
     * 获取文件名称
     * 
     * @param formPostHeaderNotice
     * @return
     * @throws InvalidArgumentException 如果未找到对应的文件名，抛出参数错误异常
     */
    public static String getFileNameFormField(FormPostHeaderNotice formPostHeaderNotice) throws InvalidArgumentException{
        
        Map<String, FormField> formFields = formPostHeaderNotice.getFieldMap();
        FormField fileField = formFields.get("file");
        if (fileField == null) {
            throw new InvalidArgumentException(400,
                    "POST requires exactly one file upload per request.",
                    "file", "0");
        }
        return fileField.getFileName();
    }
    /**
     * 从参数中读取Key
     * FormPOST key不允许为空
     * 如果key 以 ${filename} 结尾，则使用 fileName进行替换
     * @param formPostHeaderNotice
     * @param fileName file字段的文件名称
     * @return
     * @throws InvalidArgumentException
     */
    public static String getKeyFromField(FormPostHeaderNotice formPostHeaderNotice,String fileName)
                    throws InvalidArgumentException {
        Map<String, FormField> formFields = formPostHeaderNotice.getFieldMap();
        String orignKey = null;
        FormField keyField = formFields.get("key");
        if (keyField == null) {
            throw new InvalidArgumentException(400,
                    "Bucket POST must contain a field named 'key'. If it is specified, please check the order of the fields.",
                    "key", "");
        }
        orignKey = keyField.getValue();
        if (orignKey == null || orignKey.trim().length()==0){
            throw new InvalidArgumentException(400,
                    "User key must have a length greater than 0.",
                    "key", "");
        }
        orignKey = orignKey.trim();
        //全部替换
        if (orignKey != null && orignKey.contains("${filename}")) {
            orignKey = orignKey.replaceAll("\\$\\{filename\\}", fileName);
            keyField.value = orignKey;
        }
        //只替换结尾
//        if (orignKey != null && orignKey.endsWith("${filename}")) {
//            orignKey = orignKey.substring(0,orignKey.length()-"${filename}".length()) + fileName;
//            keyField.value = orignKey;
//        }
        return orignKey;
    }
    /**
     * 获取重定向的URL
     * @param formFields
     * @param dbBucket
     * @param object
     * @return
     * @throws UnsupportedEncodingException
     */
    public static String getSuccessActionRedirectUrl(
            Map<String, FormField> formFields, BucketMeta dbBucket,
            ObjectMeta object) throws UnsupportedEncodingException {
        String successActionRedirect = null;
        FormField successActionRedirectField = formFields
                .get("success_action_redirect");
        if (successActionRedirectField == null) {
            successActionRedirectField = formFields.get("redirect");
        }
        if (successActionRedirectField != null
                && successActionRedirectField.getValue()!=null && successActionRedirectField.getValue().trim().length() > 0) {
            successActionRedirect = successActionRedirectField.getValue()
                    .trim();
            try {
                new URL(successActionRedirect);
            } catch (MalformedURLException e) {
                successActionRedirect = null;
            }
        }
        if (successActionRedirect != null) {
            StringBuilder sb = new StringBuilder(successActionRedirect);
            if (successActionRedirect.indexOf("?") > 0) {
                sb.append("&");
            } else {
                sb.append("?");
            }
//            sb.append("bucket=").append(dbBucket.name).append("&key=").append(object.name).append("&etag=").append("\"").append(object.etag).append("\"");
            sb.append("bucket=").append(URLEncoder.encode(dbBucket.name, Consts.STR_UTF8)).append("&key=")
                    .append(URLEncoder.encode(object.name, Consts.STR_UTF8)).append("&etag=").append(URLEncoder.encode("\""+object.etag+"\"", Consts.STR_UTF8));
            return sb.toString();
        }
        return successActionRedirect;
    }
    /**
     * 获取响应码，默认204
     * @param formFields
     * @return
     */
    public static int getSuccessActionStatusCode(Map<String, FormField> formFields){
        int responseCode = 204;
        FormField successActionStatusField = formFields
                .get("success_action_status");
        if (successActionStatusField != null
                && successActionStatusField.getValue().trim().length() > 0) {
            try {
                responseCode = Integer
                        .parseInt(successActionStatusField.getValue().trim());
            } catch (Exception e) {
            }
            //success_action_status校验，仅支持200 201 204，无效值将按照204处理。
            if(responseCode!=200 && responseCode!=201 && responseCode!=204){
                responseCode = 204;
            }
        }
        return responseCode;
    }

    public static XmlWriter getSuccessResponse(String location, String bucket,
            String key, String etag) throws UnsupportedEncodingException {
        XmlWriter xml = new XmlWriter();
        xml.start("PostResponse");
        xml.start("Location").value(location).end();
        xml.start("Bucket").value(bucket).end();
        xml.start("Key").value(key).end();
        xml.start("ETag").value("\""+etag+"\"").end();
        xml.end();
        return xml;
    }
    

}
