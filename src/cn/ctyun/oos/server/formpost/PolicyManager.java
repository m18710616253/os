package cn.ctyun.oos.server.formpost;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TimeZone;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import cn.ctyun.common.BaseException;
import cn.ctyun.common.Consts;
import cn.ctyun.oos.server.formpost.exceptions.EntityTooLargeException;
import cn.ctyun.oos.server.formpost.exceptions.EntityTooSmallException;
import cn.ctyun.oos.server.formpost.exceptions.InvalidArgumentException;
import cn.ctyun.oos.server.signer.V4Signer;

/**
 * 策略管理器 负责：策略文件的读取和验证参数是否满足策略文件
 * 
 * @author zhaowentao
 *
 */
public class PolicyManager {
    public static final String CONDITION_CONTENT_LENGTH_RANGE = "content-length-range";
    public static final String CONDITION_ACTION_STATUS = "success_action_status";
    
    public static final String CONDITION_BUCKET = "bucket";
    // V4签名
    private static final Set<String> ignoreV4FieldNames = new HashSet<>();
    private static final Set<String> ignoreV4ConditionNames = new HashSet<>();
    
    private static final Log log = LogFactory.getLog(PolicyManager.class);
    // 过期时间的格式
    private static final SimpleDateFormat expirationDateFormater = new SimpleDateFormat(
            "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
    // 忽略的字段
    private static final Set<String> ignoreFieldNames = new HashSet<>();

    static {
        ignoreFieldNames.add("awsaccesskeyid");
        ignoreFieldNames.add("signature");
        ignoreFieldNames.add("policy");
        ignoreFieldNames.add("file");
        expirationDateFormater.setTimeZone(TimeZone.getTimeZone("UTC"));
        
        ignoreV4FieldNames.add(V4Signer.X_AMZ_SIGNATURE);
        ignoreV4FieldNames.add("policy");
        ignoreV4FieldNames.add("file");
        
        ignoreV4ConditionNames.add(V4Signer.X_AMZ_ALGORITHM);
        ignoreV4ConditionNames.add(V4Signer.X_AMZ_CREDENTIAL);
        ignoreV4ConditionNames.add(V4Signer.X_AMZ_DATE);
        ignoreV4ConditionNames.add("x-amz-security-token");
    }

    /**
     * 判断字段是否应该忽略，即不在Policy的check范围
     * 
     * @param fieldName
     * @return
     */
    public static boolean isIgnoreField(String fieldName) {
        if (fieldName == null) {
            return false;
        }
        String lowcaseName = fieldName.toLowerCase();
        if (ignoreFieldNames.contains(lowcaseName)) {
            return true;
        }
        if (lowcaseName.startsWith("x-ignore-")) {
            return true;
        }
        return false;
    }
    public static Policy readPolicy(String orignPolicy) throws BaseException {
        byte[] policyBytes = Base64.decodeBase64(orignPolicy);
        String policyStr = new String(policyBytes, Consts.CS_UTF8);
        // 替换 \$ 为 $
        policyStr = policyStr.replaceAll("\\\\\\$", "\\$");
        Policy policy = null;
        JSONObject jsonObject;
        try {
            jsonObject = new JSONObject(policyStr);
            policy = PolicyManager.readPolicy(jsonObject);
        } catch (JSONException e) {
            throw new InvalidPolicyDocumentException(
                    "Invalid Policy: JSON INVALID "+e.getMessage());
        }
        return policy;
    }
    /**
     * 读取Policy 从JSON字符串转换为Policy对象
     * 
     * @param json
     * @return
     * @throws BaseException
     */
    public static Policy readPolicy(JSONObject json) throws BaseException {
        
        Policy policy = new Policy();
        String expirationStr = json.optString("expiration");
        if (isEmpty(expirationStr)) {
            throw new InvalidPolicyDocumentException(
                    "Invalid Policy: Policy missing expiration.");
        }
        try {
            Date expirationDate = parseExpirationDate(expirationStr);
            policy.expiration = expirationDate;

        } catch (ParseException e1) {
            throw new InvalidPolicyDocumentException(
                    "Invalid Policy: Invalid 'expiration' value: '"
                            + expirationStr + "'");
        }
        if (policy.expiration.getTime() < System.currentTimeMillis()) {
            throw new AccessDeniedException("Invalid according to Policy: Policy expired.");
        }

        JSONArray conditionJsonArray = json.optJSONArray("conditions");
        if (conditionJsonArray == null) {
            throw new InvalidPolicyDocumentException(
                    "Invalid Policy: Policy missing conditions.");
        }
        try {
            for (int i = 0; i < conditionJsonArray.length(); i++) {
                Object obj = conditionJsonArray.get(i);
                if (obj instanceof JSONArray) {
                    JSONArray ja = (JSONArray) obj;
                    if (ja.length() != 3) {
                        throw new InvalidPolicyDocumentException(
                                "Invalid Policy: Invalid Condition, wrong number of arguments. Condition: '"+ja.toString()+"'");
                    }
                    String command = ja.getString(0);
                    if ("starts-with".equalsIgnoreCase(command)) {
                        String key = getConditionKey(ja.getString(1),ja.toString());
                        String value = checkConditionValue(ja.getString(2),ja.toString());
                        policy.conditions.put(key.toLowerCase(), new StartWithCondition(key, value));
                    } else if ("eq".equalsIgnoreCase(command)) {
                        String key = getConditionKey(ja.getString(1),ja.toString());
                        String value = checkConditionValue(ja.getString(2),ja.toString());
                        policy.conditions.put(key.toLowerCase(),
                                new EqualCondition(key, value));
                    } else if (CONDITION_CONTENT_LENGTH_RANGE
                            .equalsIgnoreCase(command)) {
                        try{
                            long start = ja.getLong(1);
                            long end = ja.getLong(2);
                            if(start<0 || end<start){
                                throw new InvalidPolicyDocumentException(
                                        "Invalid Policy: Invalid Condition, wrong range. Condition:'"+ja.toString()+"'");
                            }
                            policy.conditions.put(CONDITION_CONTENT_LENGTH_RANGE.toLowerCase(),
                                    new RangeCondition(command,
                                            start, end,true));
                        }catch(JSONException e){
                            throw new InvalidPolicyDocumentException(
                                    "Invalid Policy: Invalid Condition, Range need a long value. Condition:'"+ja.toString()+"'");
                        }
                    } else {
                        throw new InvalidPolicyDocumentException(
                                "Invalid Policy: unknown operation '" + command
                                        + "'.");
                    }
                } else if (obj instanceof JSONObject) {
                    JSONObject jo = (JSONObject) obj;
                    if (jo.length() != 1) {
                        throw new InvalidPolicyDocumentException(
                                "Invalid Policy: Invalid Simple-Condition: Simple-Conditions must have exactly one property specified.");
                    }
                    String key = jo.names().getString(0);
                    String value = checkConditionValue(jo.getString(key), jo.toString());
                    policy.conditions.put(key.toLowerCase(), new EqualCondition(key, value));
                    if (log.isDebugEnabled()) {
                        log.debug("JSONObject: " + i + " " + jo.names());
                    }
                }
            }
            ConditionBase actionStatusCondition = policy.conditions.get(CONDITION_ACTION_STATUS);
            if(actionStatusCondition!=null){
                if(!(actionStatusCondition instanceof EqualCondition)){
                    throw new InvalidPolicyDocumentException("Invalid Policy: '"+CONDITION_ACTION_STATUS+"' only support exact matching.");
                }
            }
            ConditionBase contentLengthRangeCondition = policy.conditions.get(CONDITION_CONTENT_LENGTH_RANGE);
            if(contentLengthRangeCondition!=null){
                if(!(contentLengthRangeCondition instanceof RangeCondition)){
                    throw new InvalidPolicyDocumentException("Invalid Policy: '"+CONDITION_CONTENT_LENGTH_RANGE+"' only support range matching.");
                }
            }
        } catch (JSONException e) {
            throw new InvalidPolicyDocumentException(
                    "Invalid Policy: " + e.getMessage());
        }
        return policy;
    }
    private static String getConditionKey(String conditionKey,String rawJson)
            throws JSONException, InvalidPolicyDocumentException {
        String key = conditionKey;
        //参数名称必须以$开头
        if (key.startsWith("$")) {
            key = key.substring(1);
        }else{
            throw new InvalidPolicyDocumentException(
                    "Invalid Policy: Invalid Condition, Condition Key Must start-with '$'. Condition:'"+rawJson+"'");
        }
        return key;
    }
   
    /**
     * 读取Value
     * 注意：["starts-with","$bucket",null] 
     *    会解析为：["starts-with","$bucket","null"]
     * @param conditionValue
     * @param rawJson
     * @return
     * @throws JSONException
     * @throws InvalidPolicyDocumentException
     */
    private static String checkConditionValue(String conditionValue,String rawJson)
            throws JSONException, InvalidPolicyDocumentException {
        String value = conditionValue;
        //参数名称必须以$开头
        if (value == null){
            throw new InvalidPolicyDocumentException(
                    "Invalid Policy: Invalid JSON: '"+rawJson+"'");
        }
        return value;
    }

    /**
     * 校验文件长度
     * 注：如果PublicWriteBucket policy可能为空
     * @param policy
     * @param fileLength
     * @throws BaseException
     */
    public static void checkFileLength(Policy policy, long fileLength)
            throws BaseException {
        if(policy!=null){
            ConditionBase sizeRangeCondition = policy.conditions
                    .get(PolicyManager.CONDITION_CONTENT_LENGTH_RANGE);
            if (sizeRangeCondition != null) {
                ((RangeCondition)sizeRangeCondition).check(fileLength);
            }
        }
    }
    /**
     * 检查文件数量，仅支持一个
     * @param formPostHeaderNotice
     * @throws InvalidArgumentException
     */
    public static void checkFileFieldCount(FormPostHeaderNotice formPostHeaderNotice) throws InvalidArgumentException{
        if(formPostHeaderNotice.fileCount!=1){
            throw new InvalidArgumentException(400,
                    "POST requires exactly one file upload per request.",
                    "file", ""+formPostHeaderNotice.fileCount);
        }
    }
    
    /**
     * 检查Bucket是否满足Policy要求
     * @param policy
     * @param bucketName
     * @throws BaseException
     */
    private static void checkBucket(Policy policy, String bucketName)
            throws BaseException {
        ConditionBase bucketCondition = policy.conditions
                .get(PolicyManager.CONDITION_BUCKET);
        if (bucketCondition != null) {
            bucketCondition.check(bucketName);
        }
    }
   
    

    /**
     * 读取并验证字段是否满足Policy文档的要求
     * 
     * @param policy
     * @param fields
     * @param bucketName
     * @throws BaseException
     * @throws InvalidPolicyDocumentException
     * @throws AccessDeniedException
     */
    public static void checkPolicy(Policy policy, Map<String, FormField> fields,
           String bucketName) throws BaseException,
                    InvalidPolicyDocumentException, AccessDeniedException {
        Iterator<Entry<String,FormField>> fieldIterator = fields.entrySet().iterator();
        while (fieldIterator.hasNext()) {
            Entry<String,FormField> entry = fieldIterator.next();
            String key = entry.getKey();
            FormField field = entry.getValue();
            if (PolicyManager.isIgnoreField(key)) {
                continue;
            }
            ConditionBase condition = policy.conditions.get(key);
            if (condition == null) {
                throw new InvalidPolicyDocumentException(
                        "Invalid according to Policy: Extra input fields: "
                                + field.getFieldName());
            }
            condition.check(field.getValue());
        }
        // 校验Bucket是否有效
        checkBucket(policy, bucketName);
        Iterator<ConditionBase> conditionIterator = policy.conditions.values()
                .iterator();
        while (conditionIterator.hasNext()) {
            ConditionBase condition = conditionIterator.next();
            if (!condition.isSatisfyed) {
                throw new AccessDeniedException(
                        "Invalid according to Policy: Policy Condition failed:"
                                + condition);
            }
        }
    }

    public static void checkPolicyV4(Policy policy,
            Map<String, FormField> fields, String bucketName)
            throws BaseException, InvalidPolicyDocumentException,
            AccessDeniedException {
         Iterator<Entry<String,FormField>> fieldIterator = fields.entrySet().iterator();
         while (fieldIterator.hasNext()) {
             Entry<String,FormField> entry = fieldIterator.next();
             String key = entry.getKey();
             FormField field = entry.getValue();
             if (PolicyManager.isIgnoreV4Field(key)) {
                 continue;
             }
             ConditionBase condition = policy.conditions.get(key);
             if (condition == null) {
                 throw new InvalidPolicyDocumentException(
                         "Invalid according to Policy: Extra input fields: "
                                 + field.getFieldName());
             }
             // policy里的参数项值是否与form filed中一致
             condition.check(field.getValue());
         }
         // 校验Bucket是否有效
         checkBucket(policy, bucketName);
         Iterator<ConditionBase> conditionIterator = policy.conditions.values().iterator();
         while (conditionIterator.hasNext()) {
             ConditionBase condition = conditionIterator.next();
             if (PolicyManager.isIgnoreV4Condition(condition.key))
                 continue;
             if (!condition.isSatisfyed) {
                 throw new AccessDeniedException(
                         "Invalid according to Policy: Policy Condition failed:"
                                 + condition);
             }
         }
     }
    
    /**
     * 判断字段是否应该忽略，即不在Policy的check范围
     * 
     * @param fieldName
     * @return
     */
    public static boolean isIgnoreV4Field(String fieldName) {
        if (fieldName == null) {
            return false;
        }
        String lowcaseName = fieldName.toLowerCase();
        if (ignoreV4FieldNames.contains(lowcaseName)) {
            return true;
        }
        if (lowcaseName.startsWith("x-ignore-")) {
            return true;
        }
        return false;
    }
    
    /**
     * 判断字段是否应该忽略，即不在Policy的check范围
     * 
     * @param fieldName
     * @return
     */
    public static boolean isIgnoreV4Condition(String fieldName) {
        if (fieldName == null) {
            return false;
        }
        String lowcaseName = fieldName.toLowerCase();
        if (ignoreV4ConditionNames.contains(lowcaseName)) {
            return true;
        }
        if (lowcaseName.startsWith("x-ignore-")) {
            return true;
        }
        return false;
    }

    private static boolean isEmpty(String expirationStr) {
        return expirationStr == null || expirationStr.length() == 0;
    }
    /**
     * 策略对象
     * @author zhaowentao
     *
     */
    public static class Policy {
        //策略的过期时间
        Date expiration;
        //校验条件
        Map<String, ConditionBase> conditions = new HashMap<>();;
    }
    /**
     * 条件基类
     * @author zhaowentao
     *
     */
    public static abstract class ConditionBase {
        public ConditionType type;
        public String key;
        public boolean isSatisfyed;

        public abstract void check(String testValue) throws BaseException;
        public abstract boolean isStatisfy(String testValue);
    }
    /**
     * 相等的条件
     * @author zhaowentao
     *
     */
    public static class EqualCondition extends ConditionBase {
        public String value;

        public EqualCondition(String key, String value) {
            super.type = ConditionType.EQUAL;
            // TODO check $
            super.key = key;
            this.value = value;
        }

        @Override
        public void check(String testValue) throws AccessDeniedException {
            isSatisfyed = value.equals(testValue);
            if (!isSatisfyed) {
                throw new AccessDeniedException(
                        "Invalid according to Policy: Policy Condition failed: "
                                + this);
            }
        }

        @Override
        public String toString() {
            return "[\"eq\", \"$" + key + "\", \"" + value + "\"]";
        }

        @Override
        public boolean isStatisfy(String testValue) {
            return value.equals(testValue);
        }
    }
    /**
     * start-with 条件
     * @author zhaowentao
     *
     */
    public static class StartWithCondition extends ConditionBase {
        public String value;

        public StartWithCondition(String key, String value) {
            super.type = ConditionType.START_WIDTH;
            super.key = key;
            this.value = value;
        }

        @Override
        public void check(String testValue) throws AccessDeniedException {
            isSatisfyed = testValue.startsWith(value);
            if (!isSatisfyed) {
                throw new AccessDeniedException(
                        "Invalid according to Policy: Policy Condition failed: "
                                + this);
            }
        }

        @Override
        public String toString() {
            return "[\"starts-with\", \"$" + key + "\", \"" + value + "\"]";
        }

        @Override
        public boolean isStatisfy(String testValue) {
            if(testValue == null){
                return false;
            }
            return testValue.startsWith(value);
        }
    }
    /**
     * 范围条件
     * @author zhaowentao
     *
     */
    public static class RangeCondition extends ConditionBase {
        public long rangeStart;
        public long rangeEnd;

        public RangeCondition(String key, long start, long end,boolean isSatisfyed) {
            super.key = key;
            this.rangeStart = start;
            this.rangeEnd = end;
            this.isSatisfyed = isSatisfyed;
        }

        @Override
        public void check(String testValue) throws BaseException {
            long testLong = 0;
            try {
                testLong = Long.parseLong(testValue);
            } catch (Exception e) {
                throw new InvalidPolicyDocumentException(
                        "Condition " + key + " need a long value.");
            }
            if(testLong < rangeStart){
                throw new EntityTooSmallException(400, "EntityTooSmall", testLong, rangeStart);
            }else if(testLong > rangeEnd){
                throw new EntityTooLargeException(400, "EntityTooLarge", testLong, rangeEnd);
            }
        }
        public void check(long testValue) throws BaseException {
            if(testValue < rangeStart){
                throw new EntityTooSmallException(400, "EntityTooSmall", testValue, rangeStart);
            }else if(testValue > rangeEnd){
                throw new EntityTooLargeException(400, "EntityTooLarge", testValue, rangeEnd);
            }
        }
        @Override
        public String toString() {
            return "[\"" + key + "," + rangeStart + ", " + rangeEnd + "]";
        }

        @Override
        public boolean isStatisfy(String testValue) {
            long testLong = 0;
            try {
                testLong = Long.parseLong(testValue);
            } catch (Exception e) {
                return false;
            }
            return testLong >= rangeStart && testLong <= rangeEnd;
        }

    }

    public static class AccessDeniedException extends BaseException {
        private static final long serialVersionUID = 8718981873819609155L;

        public AccessDeniedException(String message) {
            super(403, "AccessDenied", message);
        }
    }

    public static class InvalidPolicyDocumentException extends BaseException {
        private static final long serialVersionUID = -8884711897471592595L;

        public InvalidPolicyDocumentException(String message) {
            super(400, "InvalidPolicyDocument", message);
        }
    }
    public static Date parseExpirationDate(String dateStr) throws ParseException {
        synchronized(expirationDateFormater) {
            return expirationDateFormater.parse(dateStr);
        }
    }
    public static enum ConditionType {
        START_WIDTH, EQUAL, RANGE
    }
}
