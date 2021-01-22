//package cn.ctyun.oos.website;
//
///**
// * 通用工具类-实现公共参数(请求头部分)的封装
// * @author fxc 
// * @date 2018-01-03
// * */
//
//import java.security.MessageDigest;
//import java.text.ParseException;
//import java.text.SimpleDateFormat;
//import java.util.Date;
//import java.util.HashMap;
//import java.util.Locale;
//import java.util.Map;
//
//import javax.crypto.Mac;
//import javax.crypto.spec.SecretKeySpec;
//
//import org.jasypt.contrib.org.apache.commons.codec_1_3.binary.Base64;
//
//import com.mysql.jdbc.StringUtils;
//
//
//public class VmHeaderUtils {
//	private static final String HMAC_SHA1_ALGORITHM = "HmacSHA1";
//	
//	/**
//	 * 1.accessKey *必填 HEADER 天翼云分配给用户的公钥 
//	 * 2.contentMD5 *必填 HEADER
//		 * 业务参数值的MD5摘要：para1\npara2\n...paraN-1\nparaN contentMD5的加密方式：
//		 * contentMD5内容为业务参数的MD5信息摘要，构成MD5原始信息格式为（参数的拼接顺序以API签名为准，错误的顺序将导致验证失败）：
//		 * para1\npara2\n...paraN-1\nparaN，即将每个业务参数通过字符“\n”进行连接。若所调用的接口没有参数直接传空字符串“”
//		 * 进行转换为md5 
//	 * 3.requestDate *必填 HEADER “EEE, d MMM yyyy HH:mm:ss z”格式的请求日期（只接收英文格式）
//	 * 4.hmac *必填 HEADER 使用HMAC算法生成的信息摘要。 HMAC原始信息中需要的字段：
//		 * 使用HMAC加密码时需要密钥和待加密的消息两部分内容。所以，系统中使用secretKey（用户密钥）作为加密密钥，待加密的消息由下面三部分构成：
//		 * contentMD5，requestDate，servicePath（REST服务名称，例如“/apiproxy/v3/order/
//		 * cancelOrder”），
//		 * 三部分信息通过“\n”进行连接（要注意连接的顺序）：contentMD5\nrequestDate\nservicePath。
//	 * 5.platform *必填 HEADER 平台类型，整数类型，现在默认传3，该参数不需要加密，后续该字段的具体值会补充。
//	 */
//
//	/**
//	 * 生产请求-需要的header
//	 * @param contentMD5Source 需要MD5校验的业务参数值
//	 * @param servicePath REST服务名称，例如“/apiproxy/v3/order/
//	 * @param accessKey 公钥
//	 * @param privateKey MD5校验需要的私钥
//	 * @return Map
//	 */
//	public static Map<String, String> getExfoVmHeader(String contentMD5Source, String servicePath,String accessKey,String privateKey) {
//		Map<String, String> httpHeader = new HashMap<String, String>();
////		httpHeader.put("Content-Type", "application/json;charset=UTF-8");
//		httpHeader.put("accessKey", accessKey);
//		String contentMD5=toMD5Base64(contentMD5Source);
////		注意：contentMD5Source 中多个参数以 \n 分割，参数拼接时需要使用 "\\n" !!!
//		System.out.println("contentMD5Source="+contentMD5Source);
//		httpHeader.put("contentMD5", contentMD5);
//		Date date = new Date();
////		测试时，可以默认成一个固定时间
////		String defaultDateStr="2018-01-01 01:00:00";
////		try {
////			date=new SimpleDateFormat("yyyy-MM-DD HH:mm:ss").parse(defaultDateStr);
////		} catch (ParseException e) {
////			e.printStackTrace();
////		}
//		String requestDate = new SimpleDateFormat("EEE, d MMM yyyy HH:mm:ss z", Locale.ENGLISH).format(date);
//		System.out.println("requestDate="+requestDate);
//		System.out.println("servicePath="+servicePath);
//		httpHeader.put("requestDate", requestDate);
////		注意：calculateHMAC中待加密字符串 多个参数以 \n 分割，参数拼接时需要使用 "\n" !!!
//		String hmac=calculateHMAC(privateKey,contentMD5+"\n"+requestDate+"\n"+servicePath);
//		httpHeader.put("hmac", hmac);
//		httpHeader.put("platform", "3");
//
//		return httpHeader;
//	}
//
//	/**
//	 * 将输入字符串转换为MD5 
//	 * @param contentMD5Source  目标字符串
//	 * @return contentMD5		MD5加密后的字符串
//	 */
//	public static String toMD5Base64(String contentMD5Source) {
//		try {
//			MessageDigest md = MessageDigest.getInstance("MD5");
//			md.update(contentMD5Source.getBytes());
//			byte[] digest = md.digest();
//			String result = new String(Base64.encodeBase64(digest));
//			return result;
//		} catch (Exception e) {
//			System.out.println("toMD5Base64-MD5转换报错-"+e.getMessage());
//			e.printStackTrace();
//			return null;
//		}
//	}
//
//	/**
//	 * 根据HmacSHA1算法生成HMAC信息摘要
//	 * @param secret 密钥
//	 * @param data 消息输入
//	 * @return 信息摘要
//	 */
//	public static String calculateHMAC(String secret,String data) {
//		try {
//			SecretKeySpec secretKeySpec=new SecretKeySpec(secret.getBytes(),
//					HMAC_SHA1_ALGORITHM);
//			Mac mac=Mac.getInstance(HMAC_SHA1_ALGORITHM);
//			mac.init(secretKeySpec);
//			byte[] rawHmac=mac.doFinal(data.getBytes());
//			String result=new String(Base64.encodeBase64(rawHmac));			
//			return result;
//		} catch (Exception e) {
//			System.out.println("calculateHMAC-生成HMAC信息摘要报错-"+e.getMessage());
//			e.printStackTrace();
//			throw new IllegalArgumentException();
//		}
//	}
//}
