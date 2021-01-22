package cn.ctyun.oos.server;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TimeZone;
import java.util.stream.Collectors;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOCase;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.net.util.SubnetUtils;
import org.apache.hadoop.hbase.util.Bytes;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.policy.Statement.Effect;
import com.amazonaws.auth.policy.conditions.IpAddressCondition.IpAddressComparisonType;
import com.amazonaws.auth.policy.conditions.StringCondition.StringComparisonType;
import com.amazonaws.http.HttpMethodName;
import com.amazonaws.services.s3.Headers;
import com.amazonaws.services.s3.internal.ServiceUtils;
import com.amazonaws.services.s3.internal.XmlWriter;
import com.amazonaws.services.s3.model.BucketCrossOriginConfiguration;
import com.amazonaws.services.s3.model.BucketLoggingConfiguration;
import com.amazonaws.services.s3.model.BucketVersioningConfiguration;
import com.amazonaws.services.s3.model.BucketWebsiteConfiguration;
import com.amazonaws.services.s3.model.CORSRule;
import com.amazonaws.services.s3.model.CORSRule.AllowedMethods;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.CtyunBucketDataLocation.CtyunBucketDataScheduleStrategy;
import com.amazonaws.services.s3.model.CtyunBucketDataLocation.CtyunBucketDataType;
import com.amazonaws.services.s3.model.CtyunGetBucketLocationResult;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.DeleteObjectsRequest.KeyVersion;
import com.amazonaws.services.s3.model.GroupGrantee;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.Permission;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.transform.XmlResponsesSaxParser;
import com.amazonaws.util.BinaryUtils;
import com.amazonaws.util.Md5Utils;
import com.amazonaws.util.json.JSONArray;
import com.amazonaws.util.json.JSONException;
import com.amazonaws.util.json.JSONObject;

import cn.ctyun.common.BaseException;
import cn.ctyun.common.Consts;
import cn.ctyun.common.conf.OOSConfig;
import cn.ctyun.common.conf.StorageClassConfig;
import cn.ctyun.common.model.BucketLifecycleConfiguration;
import cn.ctyun.common.model.BucketLifecycleConfiguration.Rule;
import cn.ctyun.common.model.ObjectLockConfiguration;
import cn.ctyun.common.region.MetaRegionMapping;
import cn.ctyun.oos.bssAdapter.server.BssAdapterClient;
import cn.ctyun.oos.common.AuthResult;
import cn.ctyun.oos.common.DeleteMultipleVersionObj;
import cn.ctyun.oos.common.DeleteMultipleVersionObjByPrefix;
import cn.ctyun.oos.common.ErrorMessage;
import cn.ctyun.oos.common.OOSActions;
import cn.ctyun.oos.common.SignableInputStream;
import cn.ctyun.oos.common.Utils;
import cn.ctyun.oos.common.XmlResponseSaxParser;
import cn.ctyun.oos.hbase.MetaClient;
import cn.ctyun.oos.iam.accesscontroller.AccessEffect;
import cn.ctyun.oos.iam.accesscontroller.policy.reader.JsonDocumentFields;
import cn.ctyun.oos.iam.accesscontroller.util.IAMStringUtils;
import cn.ctyun.oos.metadata.AccelerateMeta;
import cn.ctyun.oos.metadata.BucketMeta;
import cn.ctyun.oos.metadata.BucketOwnerConsistencyMeta.BucketOwnerConsistencyType;
import cn.ctyun.oos.metadata.InitialUploadMeta;
import cn.ctyun.oos.metadata.ObjectMeta;
import cn.ctyun.oos.metadata.OwnerMeta;
import cn.ctyun.oos.metadata.UploadMeta;
import cn.ctyun.oos.metadata.UserTypeMeta;
import cn.ctyun.oos.model.DeleteMultipleVersionResult;
import cn.ctyun.oos.server.backoff.Backoff;
import cn.ctyun.oos.server.conf.AccelerateXmlParser;
import cn.ctyun.oos.server.conf.BackoffConfig;
import cn.ctyun.oos.server.conf.BssAdapterConfig;
import cn.ctyun.oos.server.count.PinData;
import cn.ctyun.oos.server.log.BucketLog;
import cn.ctyun.oos.server.signer.V4Signer;
import cn.ctyun.oos.server.storage.EtagMaker;
import cn.ctyun.oos.server.util.AccessControlUtils;
import cn.ctyun.oos.server.util.ObjectLockUtils;
import common.io.ArrayInputStream;
import common.io.ArrayOutputStream;
import common.time.TimeUtils;
import common.tuple.Pair;
import common.tuple.Triple;

/**
 * @author: Cui Meng
 */
public class OpBucket {
    private static final Log log = LogFactory.getLog(OpBucket.class);
    private static MetaClient client = MetaClient.getGlobalClient();
    private final static InternalClient internalClient = InternalClient.getInstance();

    public static void getAclFromRequest(String acl, BucketMeta dbBucket)
            throws BaseException {
        if (acl == null)
            dbBucket.permission = BucketMeta.PERM_PRIVATE;
        else {
            if (!acl.equals(CannedAccessControlList.Private.toString())
                    && !acl.equals(CannedAccessControlList.PublicRead.toString())
                    && !acl.equals(CannedAccessControlList.PublicReadWrite.toString())) {
                throw new BaseException("invalid bucket acl. ", 400, ErrorMessage.ERROR_CODE_INVALID_ARGUMENT);
            }
            if (acl.equals(CannedAccessControlList.Private.toString()))
                dbBucket.permission = BucketMeta.PERM_PRIVATE;
            else if (acl.equals(CannedAccessControlList.PublicRead.toString()))
                dbBucket.permission = BucketMeta.PERM_PUBLIC_READ;
            else if (acl.equals(CannedAccessControlList.PublicReadWrite.toString()))
                dbBucket.permission = BucketMeta.PERM_PUBLIC_READ_WRITE;
        }
    }
    
    public static String listObjectsResult(ObjectListing ol) {
        XmlWriter xml = new XmlWriter();
        xml.start("ListBucketResult", "xmlns", Consts.XMLNS);
        xml.start("Name").value(ol.getBucketName()).end();
        if (ol.getPrefix() == null)
            xml.start("Prefix").value("").end();
        else
            xml.start("Prefix").value(ol.getPrefix()).end();
        if (ol.getMarker() == null)
            xml.start("Marker").value("").end();
        else
            xml.start("Marker").value(ol.getMarker()).end();
        if (ol.getNextMarker() != null)
            xml.start("NextMarker").value(ol.getNextMarker()).end();
        xml.start("MaxKeys").value(Integer.toString(ol.getMaxKeys())).end();
        if (ol.getDelimiter() != null)
            xml.start("Delimiter").value(ol.getDelimiter()).end();
        xml.start("IsTruncated").value(Boolean.toString(ol.isTruncated())).end();
        for (S3ObjectSummary objectSummary : ol.getObjectSummaries()) {
            xml.start("Contents");
            xml.start("Key").value(objectSummary.getKey()).end();
            xml.start("LastModified")
                    .value(ServiceUtils.formatIso8601Date(objectSummary.getLastModified())).end();
            xml.start("ETag").value("\"" + objectSummary.getETag() + "\"").end();
            xml.start("Size").value(Long.toString(objectSummary.getSize())).end();
            xml.start("StorageClass").value(objectSummary.getStorageClass()).end();
            xml.start("Owner");
            xml.start("ID").value("").end();
            xml.start("DisplayName").value("").end();
            xml.end();
            xml.end();
        }
        for (String commonPrefix : ol.getCommonPrefixes()) {
            xml.start("CommonPrefixes");
            xml.start("Prefix").value(commonPrefix).end();
            xml.end();
        }
        xml.end();
        String str = Consts.XML_HEADER + xml.toString();
        str = str.replaceAll("&quote;", "&quot;");
        if (log.isDebugEnabled())
            log.debug(xml.toString());
        return str;
    }
    
    public static String getBucketACLResult(OwnerMeta owner, BucketMeta dbBucket)
            throws BaseException, IOException {
        XmlWriter xml = new XmlWriter();
        xml.start("AccessControlPolicy");
        xml.start("Owner");
        xml.start("ID").value(owner.getName()).end();
        if (owner.displayName != null && owner.displayName.trim().length() != 0)
            xml.start("DisplayName").value(owner.displayName).end();
        else
            xml.start("DisplayName").value("").end();
        xml.end();
        xml.start("AccessControlList");
        xml.start("Grant");
        String[] attr2 = { "xsi:type", "xmlns:xsi" };
        String[] value2 = { "Group", "http://www.w3.org/2001/XMLSchema-instance" };
        xml.start("Grantee", attr2, value2);
        xml.start("URI").value(GroupGrantee.AllUsers.getIdentifier()).end();
        xml.end();
        switch (dbBucket.permission) {
        case BucketMeta.PERM_PRIVATE:
            xml.start("Permission").value("").end();
            break;
        case BucketMeta.PERM_PUBLIC_READ:
            xml.start("Permission").value(Permission.Read.toString()).end();
            break;
        case BucketMeta.PERM_PUBLIC_READ_WRITE:
            xml.start("Permission").value(Permission.FullControl.toString()).end();
            break;
        }
        xml.end();
        xml.end();
        xml.end();
        if (log.isDebugEnabled())
            log.debug(xml.toString());
        String str = Consts.XML_HEADER + xml.toString();
        str = str.replaceAll("&quote;", "&quot;");
        return str;
    }
    
    public static String getBucketWebSite(BucketMeta dbBucket) {
        XmlWriter xml = new XmlWriter();
        xml.start("WebsiteConfiguration", "xmlns", Consts.XMLNS);
        xml.start("IndexDocument");
        if (dbBucket.indexDocument != null && dbBucket.indexDocument.trim().length() != 0)
            xml.start("Suffix").value(dbBucket.indexDocument).end();
        else
            xml.start("Suffix").value("").end();
        xml.end();
        xml.start("ErrorDocument");
        if (dbBucket.errorDocument != null && dbBucket.errorDocument.trim().length() != 0)
            xml.start("Key").value(dbBucket.errorDocument).end();
        else
            xml.start("Key").value("").end();
        xml.end();
        xml.end();
        if (log.isDebugEnabled())
            log.debug(xml.toString());
        String str = Consts.XML_HEADER + xml.toString();
        return str;
    }

    public static void putWebSite(BucketMeta bucket, InputStream ip) throws BaseException,
            AmazonClientException, IOException {
        BucketWebsiteConfiguration website = new XmlResponseSaxParser()
                .parseWebsiteConfigurationResponse(ip).getConfiguration();
        V4Signer.checkContentSignatureV4(ip);
        if (website.getIndexDocumentSuffix() == null
                || website.getIndexDocumentSuffix().trim().length() == 0
                || website.getIndexDocumentSuffix().contains("/"))
            throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_ARGUMENT,
                    "The IndexDocument Suffix is not well formed");
        else
            bucket.indexDocument = website.getIndexDocumentSuffix();
        bucket.errorDocument = website.getErrorDocument();
        client.bucketUpdate(bucket);
    }
    
    private static void checkPolicyValid(JSONObject parentObject, String attribute,
            String[] validValues, boolean required) throws BaseException, JSONException {
        String attributeValue = null;
        if (parentObject.has(attribute)) {
            attributeValue = parentObject.getString(attribute);
        } else if (required) {
            throw new BaseException(400, "InvalidPolicyDocument",
                    "Policy is missing required element - " + attribute);
        } else
            return;
        boolean ref = false;
        for (int i = 0; i < validValues.length; i++)
            if (attributeValue.equals(validValues[i])) {
                ref = true;
                break;
            }
        if (!ref)
            throw new BaseException(400, "InvalidPolicyDocument",
                    "Policy element has invalid value - " + attribute + ":" + attributeValue);
    }
    
    private static void checkPolicyActionValid(JSONObject parentObject, String attribute,
            String[] validValues) throws BaseException, JSONException {
        JSONArray actions = null;
        if (parentObject.has(attribute)) {
            actions = getJSONArray(parentObject, attribute);
        } else {
            throw new BaseException(400, "InvalidPolicyDocument",
                    "Policy is missing required element - " + attribute);
        }
        if (actions.length() == 0) {
            throw new BaseException(400, "InvalidPolicyDocument",
                    "Policy is missing required element - Action content");
        }
        for (int j = 0; j < actions.length(); j++) {
            boolean allowed = false;
            for (int i = 0; i < validValues.length; i++)
                if (FilenameUtils.wildcardMatch(validValues[i], actions.getString(j), IOCase.INSENSITIVE)) {
                    allowed = true;
                    break;
                }
            if (!allowed)
                throw new BaseException(400, "InvalidPolicyDocument",
                        "Policy element has invalid value - " + attribute + ":" + actions.getString(j));
        }
    }

    /**
     * 检查策略中的principal是否合法
     * @param statement
     * @throws BaseException
     * @throws JSONException
     */
    private static void checkPolicyPrincipalValid(JSONObject statement) throws BaseException, JSONException {

        // 检查Principal是否存在
        if (!statement.has(JsonDocumentFields.PRINCIPAL)) {
            throw new BaseException(400, "InvalidPolicyDocument", "Policy is missing required element - Principal");
        }
        String principalStr = statement.getString(JsonDocumentFields.PRINCIPAL);
        // 字符*，直接通过验证
        if ("*".equals(principalStr)) {
            return;
        }

        // 其他情况principal为json对象
        JSONObject principalJson;
        try {
            principalJson = statement.getJSONObject(JsonDocumentFields.PRINCIPAL);
        } catch (JSONException e) {
            String error = "Policy element has invalid value - Principal" + ":" + principalStr;
            log.error(error, e);
            throw new BaseException(400, "InvalidPolicyDocument", error);
        }

        // 获取AWS或CTYUN的值
        JSONArray principals = getKeysJSONArray(principalJson, "AWS", "CTYUN");
        // 获取不到AWS或CTYUN对应的值
        if (principals == null) {
            throw new BaseException(400, "InvalidPolicyDocument", "Policy element has invalid value - Principal" + ":" + principalStr);
        }
        // 检查是否为*或者arn:ctyun:iam::、arn:aws:iam::开头
        for (int i = 0; i < principals.length(); i++) {
            String principal = principals.getString(i);
            if (!principal.equals("*") && !principal.startsWith("arn:ctyun:iam::") && !principal.startsWith("arn:aws:iam::")) {
                throw new BaseException(400, "InvalidPolicyDocument", "Policy element has invalid value - Principal" + ":" + principalStr);
            }
        }
    }

    /**
     * 检查策略
     * @param statement
     * @param dbBucket
     * @throws BaseException
     * @throws JSONException
     */
    private static void checkPolicyResourceValid(JSONObject statement, BucketMeta dbBucket) throws BaseException, JSONException {
        if (!statement.has("Resource")) {
            throw new BaseException(400, "InvalidPolicyDocument", "Policy is missing required element - Resource");
        }

        JSONArray resources = getJSONArray(statement, "Resource");
        if (resources.length() == 0) {
            throw new BaseException(400, "InvalidPolicyDocument",
                    "Policy is missing required element - Resource content");
        }
        for (int j = 0; j < resources.length(); j++) {
            String resource = resources.getString(j);
            if (resource != null) {
                if (resource.contains("/")) {
                    String[] resourceArr = resource.split("/");
                    if (resourceArr.length == 1) {
                        throw new BaseException(400, "InvalidPolicyDocument", "Policy has invalid resource - " + resource);
                    }
                    resource = resourceArr[0];
                }
                // 通过bucket获取账户ID
                String accountId = IAMStringUtils.getAccountId(dbBucket.getOwnerId());
                // 如果不与当前的bucket的ARN匹配，报错
                if (!resource.equals("arn:aws:s3:::" + dbBucket.getName())
                        && !resource.equals("arn:ctyun:oos:::" + dbBucket.getName())
                        && !resource.equals("arn:aws:s3::" + accountId + ":" + dbBucket.getName())
                        && !resource.equals("arn:ctyun:oos::" + accountId + ":" + dbBucket.getName())) {
                    throw new BaseException(400, "InvalidPolicyDocument", "Policy has invalid resource - " + resource);
                }
            }
        }
    }

    public static void putPolicy(BucketMeta dbBucket, InputStream ip, int length) throws BaseException,
            JSONException, IOException {
        JSONObject jsonObject = null;
        if (length > Consts.MAX_POLICY_JSON_SIZE) {
            throw new BaseException(400, "InvalidPolicyDocument",
                    "The maximum size of a policy document you provided is 20KB.");
        }
        try {
            dbBucket.policy = IOUtils.toString(ip);
            V4Signer.checkContentSignatureV4(ip);
            jsonObject = new JSONObject(dbBucket.policy);
        } catch (JSONException | IOException error) {
            log.error(error.getMessage(), error);
            throw new BaseException(400, "InvalidPolicyDocument",
                    "Policy could not be parsed as a valid JSON string");
        }
        checkPolicyValid(jsonObject, "Version", new String[] { Consts.POLICY_VERSION }, false);
        JSONArray statements = null;
        if (jsonObject.has("Statement")) {
            statements = jsonObject.getJSONArray("Statement");
        } else {
            throw new BaseException(400, "InvalidPolicyDocument",
                    "Policy is missing required element - Statement");
        }
        for (int m = 0; m < statements.length(); m++) {
            JSONObject statement = (JSONObject) statements.get(m);
            checkPolicyValid(statement, "Effect", new String[] { Effect.Allow.name(), Effect.Deny.name() }, true);
            checkPolicyActionValid(statement, "Action", POLICY_ALLOWED_ACTIONS);
            // 检查statement中的principal是否合法
            checkPolicyPrincipalValid(statement);
            // 检查statement中的resource是否合法
            checkPolicyResourceValid(statement, dbBucket);
            if (statement.has("Condition")) {
                JSONObject conditionBlock = statement.getJSONObject("Condition");
                String[] conditions = JSONObject.getNames(conditionBlock);
                if (conditions != null) {
                    for (int i = 0; i < conditions.length; i++) {
                        boolean ref = false;
                        for (StringComparisonType s : StringComparisonType.values()) {
                            if (conditions[i].equals(s.name())) {
                                JSONObject condition = conditionBlock.getJSONObject(conditions[i]);
                                String[] conditionKeys = JSONObject.getNames(condition);
                                if (conditionKeys == null)
                                    break;
                                for (int j = 0; j < conditionKeys.length; j++) {
                                    if (!conditionKeys[j].equals("aws:Referer") && !conditionKeys[j].equals("aws:UserAgent") &&
                                            !conditionKeys[j].equals("ctyun:Referer") && !conditionKeys[j].equals("ctyun:UserAgent"))
                                        throw new BaseException(400, "InvalidPolicyDocument",
                                                "Policy has an invalid condition key - " + conditionKeys[j]);
                                }
                                ref = true;
                                break;
                            }
                        }
                        for (IpAddressComparisonType s : IpAddressComparisonType.values()) {
                            if (conditions[i].equals(s.name())) {
                                JSONObject condition = conditionBlock.getJSONObject(conditions[i]);
                                String[] conditionKeys = JSONObject.getNames(condition);
                                if (conditionKeys == null)
                                    break;
                                for (int j = 0; j < conditionKeys.length; j++) {
                                    if (!conditionKeys[j].equals("aws:SourceIp") && !conditionKeys[j].equals("ctyun:SourceIp"))
                                        throw new BaseException(400, "InvalidPolicyDocument",
                                                "Policy has an invalid condition key - " + conditionKeys[j]);
                                }
                                ref = true;
                                break;
                            }
                        }
                        if (conditions[i].equals("Bool")) {
                            JSONObject condition = conditionBlock.getJSONObject(conditions[i]);
                            String[] conditionKeys = JSONObject.getNames(condition);
                            if (conditionKeys == null)
                                break;
                            for (int j = 0; j < conditionKeys.length; j++) {
                                if (!conditionKeys[j].equals("aws:SecureTransport") && !conditionKeys[j].equals("ctyun:SecureTransport"))
                                    throw new BaseException(400, "InvalidPolicyDocument",
                                            "Policy has an invalid condition key - " + conditionKeys[j]);
                            }
                            ref = true;
                            break;
                        }
                        if (!ref)
                            throw new BaseException(400, "InvalidPolicyDocument", "No such condition type - " + conditions[i]);
                    }
                }
            }
            combinationValidCheck(statement);
        }
        client.bucketUpdate(dbBucket);
    }
    
    /**
     * Action和Resource联合验证
     * @param statement
     * @throws JSONException
     * @throws BaseException
     */
    public static void combinationValidCheck(JSONObject statement) throws JSONException, BaseException {
        // Bucket级别的操作不需要最后的斜杠和{object_name}，Object级别的操作需要最后的斜杠和{object_name}
        JSONArray actions = getJSONArray(statement, "Action");
        for (int i = 0; i < actions.length(); i++) {
            String action = actions.getString(i);
            if (getActionLevel(action) == 0) {
                // Bucket级别的操作不需要最后的斜杠和{object_name}
                boolean valid = false;
                JSONArray resources = getJSONArray(statement, "Resource");
                for (int j = 0; j < resources.length(); j++) {
                    String resource = resources.getString(j);
                    if (!resource.contains("/")) {
                        valid = true;
                        break;
                    }
                }
                if (!valid) {
                    throw new BaseException(400, "InvalidPolicyDocument",
                            "Action does not apply to any resource(s) in statement");
                }
            } else if (getActionLevel(action) == 1) {
                // Object级别的操作需要最后的斜杠和{object_name}
                boolean valid = false;
                JSONArray resources = getJSONArray(statement, "Resource");
                for (int j = 0; j < resources.length(); j++) {
                    String resource = resources.getString(j);
                    if (resource.contains("/")) {
                        valid = true;
                        break;
                    }
                }
                if (!valid) {
                    throw new BaseException(400, "InvalidPolicyDocument",
                            "Action does not apply to any resource(s) in statement");
                }
            } else {
                continue;
            }
        }
    }

    /**
     * 获取action的操作级别
     * @param action
     * @return 0:action为Bucket级别的操作,1:action为Object级别的操作,-1:action为Bucket、object级别的操作
     */
    private static int getActionLevel(String action) {
        boolean bucketLevel = false;
        boolean objectLevel = false;
        for (int i = 0; i < POLICY_ALLOWED_BUCKET_LEVEL_ACTIONS.length; i++) {
            if (FilenameUtils.wildcardMatch(POLICY_ALLOWED_BUCKET_LEVEL_ACTIONS[i], action, IOCase.INSENSITIVE)) {
                bucketLevel = true;
                break;
            }
        }
        for (int i = 0; i < POLICY_ALLOWED_OBJECT_LEVEL_ACTIONS.length; i++) {
            if (FilenameUtils.wildcardMatch(POLICY_ALLOWED_OBJECT_LEVEL_ACTIONS[i], action, IOCase.INSENSITIVE)) {
                objectLevel = true;
                break;
            }
        }
        if (bucketLevel && !objectLevel) {
            return 0;
        } else if (objectLevel && !bucketLevel) {
            return 1;
        } else {
            return -1;
        }
    }

    /**
     * 检查请求的操作是否与statement中的Action匹配
     * @param statement
     * @param action
     * @return
     * @throws JSONException
     */
    private static boolean checkAction(JSONObject statement, String action) throws JSONException {
        JSONArray actions = getJSONArray(statement, "Action");
        for (int j = 0; j < actions.length(); j++) {
            if (FilenameUtils.wildcardMatch("s3:" + action, actions.getString(j), IOCase.INSENSITIVE)
                    || FilenameUtils.wildcardMatch("oos:" + action, actions.getString(j), IOCase.INSENSITIVE)) {
                return true;
            }
        }
        return false;
    }

    /**
     * 检查请求者身份是否与statement中的Principal匹配
     * @param statement
     * @param
     * @return
     * @throws JSONException
     */
    private static boolean checkPrincipal(JSONObject statement, AuthResult authResult) throws JSONException {

        String principalStr = statement.getString(JsonDocumentFields.PRINCIPAL);
        // 字符*，直接通过验证
        if ("*".equals(principalStr)) {
            return true;

        }
        // 其他情况principal为json对象
        JSONObject principalJson = statement.getJSONObject(JsonDocumentFields.PRINCIPAL);

        // 获取AWS或CTYUN的值
        JSONArray principals = getKeysJSONArray(principalJson, "AWS", "CTYUN");

        // 如果Principal中包含*，不做身份信息匹配，返回通过
        for (int i = 0; i < principals.length(); i++) {
            String principal = principals.getString(i);
            if ("*".equals(principal)) {
                return true;
            }
        }
        // 如果是匿名用户，返回不通过
        if (authResult == null || authResult.owner == null) {
            return false;
        }

        // 进行身份信息匹配
        // OOS格式用户ARN
        String userArn = authResult.getUserArn();
        // AWS格式用户ARN
        String userAWSArn = authResult.getUserAWSArn();
        // 判断是否有匹配的身份信息
        for (int i = 0; i < principals.length(); i++) {
            String principal = principals.getString(i);
            if (FilenameUtils.wildcardMatch(userArn, principal, IOCase.SENSITIVE)
                    || FilenameUtils.wildcardMatch(userAWSArn, principal, IOCase.SENSITIVE)) {
                return true;
            }
        }
        return false;
    }
    
    /**
     * 检查请求的资源是否与statement中的Resource匹配
     * @param statement
     * @param bucket
     * @param key
     * @return
     * @throws JSONException
     */
    private static boolean checkResource(JSONObject statement, BucketMeta bucket, String key) throws JSONException {
        JSONArray resources = getJSONArray(statement, "Resource");
        // 指向资源的名称
        String targetResourceName = bucket.name;
        // 如果objectName不为空
        if (StringUtils.isNotEmpty(key)) {
            targetResourceName = bucket.name + "/" + key;
        }
        String accountId = IAMStringUtils.getAccountId(bucket.getOwnerId());
        for (int i = 0; i < resources.length(); i++) {
            String resource = resources.getString(i);
            if (FilenameUtils.wildcardMatch("arn:aws:s3:::" + targetResourceName, resource, IOCase.SENSITIVE)
                    || FilenameUtils.wildcardMatch("arn:ctyun:oos:::" + targetResourceName, resource, IOCase.SENSITIVE)
                    || FilenameUtils.wildcardMatch("arn:aws:s3:::" + accountId + ":" + targetResourceName, resource, IOCase.SENSITIVE)
                    || FilenameUtils.wildcardMatch("arn:ctyun:oos::" + accountId + ":" + targetResourceName, resource, IOCase.SENSITIVE)) {
                return true;
            }
        }
        return false;
    }
    
    private static boolean checkCondition(JSONObject statement, HttpServletRequest req, String effect) throws JSONException, BaseException {
        if (!statement.has("Condition"))
            return true;
        JSONObject conditionBlock = statement.getJSONObject("Condition");
        String[] conditions = JSONObject.getNames(conditionBlock);
        boolean result = true;
        if (conditions == null)
            return true;
        for (int i = 0; i < conditions.length; i++) {
            JSONObject condition = conditionBlock.getJSONObject(conditions[i]);
            String reqUserAgent = req.getHeader("User-Agent");
            String reqReferer = req.getHeader("Referer");
            if (conditions[i].equals(StringComparisonType.StringEquals.name())) {
                result &= checkStringComp(result, condition, reqUserAgent, reqReferer, false, true, effect);
            } else if (conditions[i].equals(StringComparisonType.StringLike.name())) {
                result &= checkStringComp(result, condition, reqUserAgent, reqReferer,false, false, effect);
            } else if (conditions[i].equals(StringComparisonType.StringEqualsIgnoreCase.name())) {
                result &= checkStringComp(result, condition, reqUserAgent, reqReferer,true, true, effect);
            } else if (conditions[i].equals(StringComparisonType.StringNotEquals.name())) {
                result &= !checkStringComp(result, condition, reqUserAgent, reqReferer,false, true, effect);
            } else if (conditions[i].equals(StringComparisonType.StringNotLike.name())) {
                result &= !checkStringComp(result, condition, reqUserAgent, reqReferer,false, false, effect);
            } else if (conditions[i].equals(StringComparisonType.StringNotEqualsIgnoreCase.name())) {
                result &= !checkStringComp(result, condition, reqUserAgent, reqReferer,true, true, effect);
            } else if (conditions[i].equals("IpAddress")) {
                result &= checkIP(condition, Utils.getIpAddr(req), true);
            } else if (conditions[i].equals("NotIpAddress")) {
                result &= !checkIP(condition, Utils.getIpAddr(req), false);
            } else if (conditions[i].equals("Bool")) {
                // haproxy需要配置reqadd X-Forwarded-Proto，否则无法区分https
                boolean isSecure = "https".equalsIgnoreCase(req.getHeader("X-Forwarded-Proto")) || req.isSecure();
                result &= checkSecureTransport(condition, isSecure);
            } else
                throw new BaseException(400, "InvalidPolicyDocument");
            if (!result)
                return result;
        }
        return result;
    }

    private static boolean checkStringComp(boolean result, JSONObject condition,
            String reqUserAgent, String reqReferer, boolean isIgnoreCase, boolean isEqual, String effect) throws JSONException {
        result &= checkUserAgent(condition, reqUserAgent, isIgnoreCase, isEqual);
        result &= checkReferer(condition, reqReferer, isIgnoreCase, isEqual, effect);
        return result;
    }
    
    private static boolean checkIP(JSONObject condition, String ip,
            boolean isIPAddress) throws JSONException, BaseException {
        JSONArray sourceIp = getKeysJSONArray(condition, "aws:SourceIp", "ctyun:SourceIp");
        List<String> portalIP = OOSConfig.getProxyIp();
        Pair<List<String>,List<String>> sourceIps = Utils.getSourceIpSeparated(sourceIp);
        if (!ip.contains(":")) {
            // 请求ip为ipv4
            List<String> sourceIpv4 = sourceIps.first();
            for (int i = 0; i < sourceIpv4.size(); i++) {
                if (sourceIpv4.get(i).contains("/")) {
                    try {
                        SubnetUtils subnetUtils = new SubnetUtils((String) sourceIpv4.get(i));
                        subnetUtils.setInclusiveHostCount(true);
                        if (subnetUtils.getInfo().isInRange(ip))
                            return true;
                    } catch (IllegalArgumentException e) {
                        log.error("checkIP IllegalArgumentException. " + e.getMessage()
                        + "ip=" + ip + ", sourceIp=" + sourceIpv4.get(i));
                    }
                } else {
                    if (Utils.isValidIPv4Addr(sourceIpv4.get(i))) {
                        if (ip.equals(sourceIpv4.get(i))) {
                            return true;
                        }
                    } else {
                        log.error("checkIP error. ip invalid. sourceIp:"+ sourceIpv4.get(i));
                    }
                }
                if (portalIP.contains(ip)) {
                    if (isIPAddress)
                        return true;
                    else
                        return false;
                }
            }
            return false;
        } else {
            // 请求ip为ipv6
            List<String> sourceIpv6 = sourceIps.second();
            for (int i = 0; i < sourceIpv6.size(); i++) {
                try {
                    if (Utils.isInIpv6Range(ip, sourceIpv6.get(i))) {
                        return true;
                    }
                } catch (UnknownHostException e) {
                    log.error("checkIP UnknownHostException. " + e.getMessage()
                            + "ip=" + ip + ", sourceIp=" + sourceIpv6.get(i));
                }
                if (portalIP.contains(ip)) {
                    if (isIPAddress)
                        return true;
                    else
                        return false;
                }
            }
            return false;
        }
    }
    
    private static JSONArray getJSONArray(JSONObject object, String key) throws JSONException {
        if (object.getString(key).startsWith("["))
            return object.getJSONArray(key);
        else {
            JSONArray array = new JSONArray();
            array.put(object.getString(key));
            return array;
        }
    }
    
    private static JSONArray getKeysJSONArray(JSONObject object, String... keys) throws JSONException {
        for (String key : keys) {
            if (object.has(key)) {
                return getJSONArray(object, key);
            }
        }
        return null;
    }

    private static boolean checkReferer(JSONObject condition, String requestReferer,
            boolean isIgnoreCase, boolean isEqual, String effect) throws JSONException {

        JSONArray referers = getKeysJSONArray(condition, "aws:Referer", "ctyun:Referer");
        if (referers == null)
            return true;
        for (int i = 0; i < referers.length(); i++) {
            if ((requestReferer == null || requestReferer.trim().length() == 0)
                    && referers.get(i).toString().equals("")) {
                return true;
            }
            if (isEqual && !isIgnoreCase) {
                if (referers.get(i).toString().equals(requestReferer))
                    return true;
                else
                    continue;
            } else if (isEqual && isIgnoreCase && requestReferer != null) {
                if (referers.get(i).toString().toLowerCase().equals(requestReferer.toLowerCase()))
                    return true;
                else
                    continue;
            } else if (requestReferer != null && !referers.get(i).toString().equals("")) {
                if (FilenameUtils.wildcardMatch(requestReferer, referers.get(i).toString(), IOCase.SENSITIVE))
                    return true;
            }
        }
        // 如果请求来自于自服务门户且该条件键所属statement的effect为Allow，返回匹配
        if (requestReferer != null && requestReferer.equals("http://" + OOSConfig.getPortalDomain() + "/")
        		&& AccessEffect.Allow.toString().equals(effect)) {
        	return true;
        }
        return false;
    }
    
    private static boolean checkUserAgent(JSONObject condition, String requestStr,
            boolean isIgnoreCase, boolean isEqual) throws JSONException {
        JSONArray conditionValue = getKeysJSONArray(condition, "aws:UserAgent", "ctyun:UserAgent");
        if (conditionValue == null)
            return true;
        for (int i = 0; i < conditionValue.length(); i++) {
            if ((requestStr == null || requestStr.trim().length() == 0)
                    && conditionValue.get(i).toString().equals("")) {
                return true;
            }
            if (isEqual && !isIgnoreCase) {
                if (conditionValue.get(i).toString().equals(requestStr))
                    return true;
                else
                    continue;
            } else if (isEqual && isIgnoreCase && requestStr != null) {
                if (conditionValue.get(i).toString().toLowerCase().equals(requestStr.toLowerCase()))
                    return true;
                else
                    continue;
            } else if (requestStr != null && !conditionValue.get(i).toString().equals("")) {
                if (FilenameUtils.wildcardMatch(requestStr, conditionValue.get(i).toString(), IOCase.SENSITIVE))
                    return true;
            }
        }
        return false;
    }
    
    private static boolean checkSecureTransport(JSONObject condition, boolean requestSecure) throws JSONException {
        JSONArray conditionValue = getKeysJSONArray(condition, "aws:SecureTransport", "ctyun:SecureTransport");
        if (conditionValue == null)
            return true;
        for (int i = 0; i < conditionValue.length(); i++) {
            if (conditionValue.get(i).toString().equalsIgnoreCase(requestSecure + "")) {
                return true;
            }
        }
        return false;
    }

    public static final String[] POLICY_ALLOWED_ACTIONS = new String[] {
            "s3:ListBucket", "s3:ListBucketMultipartUploads",
            "s3:AbortMultipartUpload", "s3:DeleteObject", "s3:GetObject",
            "s3:ListMultipartUploadParts", "s3:PutObject",
            "oos:ListBucket", "oos:ListBucketMultipartUploads",
            "oos:AbortMultipartUpload", "oos:DeleteObject", "oos:GetObject",
            "oos:ListMultipartUploadParts", "oos:PutObject"};

    public static final String[] POLICY_ALLOWED_BUCKET_LEVEL_ACTIONS = new String[] {
            "s3:ListBucket", "s3:ListBucketMultipartUploads",
            "oos:ListBucket", "oos:ListBucketMultipartUploads"};

    public static final String[] POLICY_ALLOWED_OBJECT_LEVEL_ACTIONS = new String[] {
            "s3:AbortMultipartUpload", "s3:DeleteObject", "s3:GetObject",
            "s3:ListMultipartUploadParts", "s3:PutObject",
            "oos:AbortMultipartUpload", "oos:DeleteObject", "oos:GetObject",
            "oos:ListMultipartUploadParts", "oos:PutObject" };

    public static AccessEffect checkPolicy(HttpServletRequest req, BucketMeta dbBucket, String action,
            boolean acl, String key, AuthResult authResult) throws JSONException, BaseException {
        if (dbBucket.policy == null || dbBucket.policy.trim().length() == 0) {
            // 没有通过ACL，隐式拒绝，如果基于身份的访问控制是允许，最终是允许操作
            return acl ? AccessEffect.Allow : AccessEffect.ImplicitDeny;
        }
        JSONObject jsonObject = new JSONObject(dbBucket.policy);
        JSONArray statements = jsonObject.getJSONArray("Statement");
        // 检查是否匹配到显示拒绝
        if (explicitPolicy(req, dbBucket, key, action, statements, AccessEffect.Deny.toString(), authResult)) {
            return AccessEffect.Deny;
        } else {
            if (acl) {
                return AccessEffect.Allow;
            } else {
                // 检查是否匹配到显示允许
                if (explicitPolicy(req, dbBucket, key, action, statements, AccessEffect.Allow.toString(), authResult)) {
                    return AccessEffect.Allow;
                } else {
                    return AccessEffect.ImplicitDeny;
                }
            }
        }
    }

    /**
     * 与指定的Effect进行匹配
     * @param req
     * @param bucket
     * @param key
     * @param action
     * @param statements
     * @param expectEffect
     * @param authResult
     * @return
     * @throws JSONException
     * @throws BaseException
     */
    public static boolean explicitPolicy(HttpServletRequest req, BucketMeta bucket, String key, String action,
            JSONArray statements, String expectEffect, AuthResult authResult) throws JSONException, BaseException {
        for (int i = 0; i < statements.length(); i++) {
            JSONObject statement = (JSONObject) statements.get(i);
            String effect = statement.getString("Effect");
            // 如果statement中的Effect不是参数中指定的，匹配下一个statement
            if (!expectEffect.equals(effect)) {
                continue;
            }
            // 检查Action是否匹配
            if (!checkAction(statement, action)) {
                continue;
            }
            // 检查身份是否匹配
            if (!checkPrincipal(statement, authResult)) {
                continue;
            }
            // 检查资源是否匹配
            if (!checkResource(statement, bucket, key)) {
                continue;
            }
            // action、principal、resource都匹配后，condition也匹配，返回匹配
            if (checkCondition(statement, req, effect)) {
                return true;
            }
        }
        // 对所有的statement都不匹配，返回不匹配
        return false;
    }

    public static String listMultipartUploads(String metaRegion, String bucket, int maxUploads, String delimiter,
            String prefix, String keyMarker, String uploadIdMarker, String requestId) throws Exception {
        if (MetaRegionMapping.needMapping(metaRegion)) {
            String dataRegion = MetaRegionMapping.getMappedDataRegion(metaRegion);
            return internalClient.listUploads(dataRegion, metaRegion, bucket, maxUploads, delimiter, prefix, keyMarker, uploadIdMarker,
                    requestId);
        }
        List<InitialUploadMeta> uploads = client
                .initialListMultipartUploads(metaRegion, bucket, prefix, delimiter,
                        maxUploads + 1, keyMarker, uploadIdMarker);

        XmlWriter xml = new XmlWriter();
        xml.start("ListMultipartUploadsResult", "xmlns", Consts.XMLNS);
        xml.start("Bucket").value(bucket).end();
        if (StringUtils.isNotBlank(keyMarker))
            xml.start("KeyMarker").value(keyMarker).end();
        else
            xml.start("KeyMarker").value("").end();
        if (StringUtils.isNotBlank(uploadIdMarker))
            xml.start("UploadIdMarker").value(uploadIdMarker).end();
        else
            xml.start("UploadIdMarker").value("").end();
        if (uploads.size() > maxUploads) {
            xml.start("NextKeyMarker").value(uploads.get(maxUploads - 1).objectName).end();
            if (uploads.get(maxUploads - 1).uploadId != null)
                xml.start("NextUploadIdMarker").value(uploads.get(maxUploads - 1).uploadId).end();
        }
        xml.start("MaxUploads").value(String.valueOf(maxUploads)).end();
        if (StringUtils.isNotBlank(delimiter))
            xml.start("Delimiter").value(delimiter).end();
        if (StringUtils.isNotBlank(prefix))
            xml.start("Prefix").value(prefix).end();
        int m = 0;
        if (uploads.size() > maxUploads) {
            xml.start("IsTruncated").value("true").end();
            m = uploads.size() - 1;
        } else {
            xml.start("IsTruncated").value("false").end();
            m = uploads.size();
        }
        Set<String> list = new LinkedHashSet<String>();
        for (int i = 0; i < m; i++) {
            InitialUploadMeta o = uploads.get(i);
            if (StringUtils.isNotBlank(delimiter)) {
                if (o.objectName.endsWith(delimiter)) {
                    list.add(o.objectName);
                    continue;
                }
            }
            xml.start("Upload");
            xml.start("Key").value(o.objectName).end();
            xml.start("UploadId").value(o.uploadId).end();
            xml.start("Initiator");
            if (o.initiator != 0) {
                OwnerMeta iniOwner = new OwnerMeta(o.initiator);
                client.ownerSelectById(iniOwner);
                xml.start("ID").value(iniOwner.getName()).end();
                if (iniOwner.displayName != null)
                    xml.start("DisplayName").value(iniOwner.displayName).end();
                else
                    xml.start("DisplayName").value("").end();
            } else {
                xml.start("ID").value("").end();
                xml.start("DisplayName").value("").end();
            }
            xml.end();
            xml.start("Owner");
            xml.start("ID").value("").end();
            xml.start("DisplayName").value("").end();
            xml.end();
            InitialUploadMeta initialUpload = new InitialUploadMeta(o.metaRegion, o.bucketName, o.objectName,
                    null, false);
            client.initialSelectByObjectId(initialUpload);
            if (initialUpload.storageClass != null
                    && initialUpload.storageClass.trim().length() != 0)
                xml.start("StorageClass").value(initialUpload.storageClass).end();
            else
                xml.start("StorageClass").value(StorageClassConfig.getDefaultClass()).end();
            xml.start("Initiated").value(ServiceUtils.formatIso8601Date(new Date(o.initiated)))
                    .end();
            xml.end();
        }
        if (delimiter != null) {
            for (String p : list) {
                xml.start("CommonPrefixes");
                xml.start("Prefix").value(p).end();
                xml.end();
            }
        }
        xml.end();
        String str = Consts.XML_HEADER + xml.toString();
        str = str.replaceAll("&quote;", "&quot;");
        if (log.isDebugEnabled())
            log.debug(xml.toString());
        return str;
    }
    
    public static void putLogging(BucketMeta dbBucket, OwnerMeta owner, InputStream ip)
            throws BaseException, IOException {
        BucketLoggingConfiguration logging = new XmlResponseSaxParser().parsePutBucketLogging(ip)
                .getLogging();
        V4Signer.checkContentSignatureV4(ip);
        if (logging.getDestinationBucketName() == null && logging.getLogFilePrefix() != null)
            throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_ARGUMENT);
        if (logging.getDestinationBucketName() != null) {
            BucketMeta bucket = new BucketMeta(logging.getDestinationBucketName());
            if (!client.bucketSelect(bucket) || bucket.getOwnerId() != owner.getId())
                throw new BaseException(400, "InvalidTargetBucketForLogging",
                        "the target bucket name is: " + bucket.getName());
            if (logging.getLogFilePrefix() != null
                    && logging.getLogFilePrefix().length() > Consts.BUCKET_LOG_PREFIX_MAX_LENGTH)
                throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_ARGUMENT);
            dbBucket.logTargetBucket = bucket.getId();
            dbBucket.logTargetPrefix = logging.getLogFilePrefix();
        }
        if (logging.getDestinationBucketName() == null && logging.getLogFilePrefix() == null) {
            dbBucket.logTargetBucket = 0;
            dbBucket.logTargetPrefix = null;
        }
        client.bucketUpdate(dbBucket);
    }
    
    public static String getLogging(BucketMeta dbBucket) throws IOException {
        XmlWriter xml = new XmlWriter();
        BucketMeta targetBucket = new BucketMeta(dbBucket.logTargetBucket);
        boolean ref = client.bucketSelect(targetBucket);
        xml.start("BucketLoggingStatus", "xmlns", Consts.XMLNS);
        boolean res = false;
        if (dbBucket.logTargetBucket != 0 && dbBucket.logTargetPrefix != null) {
            if (!ref) {
                dbBucket.logTargetBucket = 0;
                dbBucket.logTargetPrefix = "";
                client.bucketUpdate(dbBucket);
            } else {
                xml.start("LoggingEnabled");
                res = true;
                xml.start("TargetBucket").value(targetBucket.getName()).end();
                xml.start("TargetPrefix").value(dbBucket.logTargetPrefix).end();
            }
        }
        if (res)
            xml.end();
        xml.end();
        if (log.isDebugEnabled())
            log.debug(xml.toString());
        String str = Consts.XML_HEADER + xml.toString();
        return str;
    }

    /**
     * 处理 http header中 x-amz-version-deletemode 不为空的请求
     *
     * @param req
     * @param dbBucket
     * @param ip
     * @param owner
     * @param bucketLog
     * @param pinDataList
     * @return
     * @throws BaseException
     */
    private static String deleteMultipleVersionObjects(HttpServletRequest req, BucketMeta dbBucket, InputStream ip,
            OwnerMeta owner, BucketLog bucketLog, List<PinData> pinDataList)
            throws BaseException {
        if (!Utils.ifCanWrite(owner, dbBucket)) {
            throw new BaseException(403, ErrorMessage.ERROR_MESSAGE_403);
        }
        InputStream is = null;
        try {
            is = checkMd5Sign(ip, req.getHeader(Headers.CONTENT_MD5));
            if (Consts.X_AMZ_VERSION_DELETEMODE_PREFIX
                    .equals(req.getHeader(Consts.X_AMZ_VERSION_DELETEMODE))) {
                return deleteMultipleObjectsByPrefix(dbBucket, is, bucketLog, pinDataList);
            } else if (Consts.X_AMZ_VERSION_DELETEMODE_COMMON
                    .equals(req.getHeader(Consts.X_AMZ_VERSION_DELETEMODE))) {
                return deleteMultipleObjectsByObjectKey(dbBucket, is, bucketLog, pinDataList);
            } else {
                throw new BaseException(ErrorMessage.ERROR_MESSAGE_INVALID_DELETE_MODE, 400,
                        "badRequest",
                        Consts.X_AMZ_VERSION_DELETEMODE + " header error");
            }
        } catch (Exception e) {
            if (e instanceof BaseException)
                throw (BaseException) e;
            log.error(e.getMessage(), e);
        } finally {
            Utils.streamClose(is);
        }
        return null;
    }

    /**
     * 根据指定key和版本批量删除对象
     *
     * @param dbBucket
     * @param ip
     * @param bucketLog
     * @param pinDataList
     * @return
     * @throws BaseException
     */
    private static String deleteMultipleObjectsByObjectKey(BucketMeta dbBucket, InputStream ip, BucketLog bucketLog,
            List<PinData> pinDataList) throws BaseException {
        DeleteMultipleVersionObj handle = null;
        try {
            handle = new XmlResponseSaxParser().parseDeleteMultipleVersionObjects(ip).getDeleteMultipleVersionObj();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new BaseException(400, ErrorMessage.ERROR_CODE_MALFORMEDXML);
        }

        //检查是否有空值存在
        handle.checkObjectsList();

        DeleteMultipleVersionResult result = new DeleteMultipleVersionResult(handle.isQuiet(),
                handle.isErrorContinue());

        int count = 0;
        //用来标记处理到的对象位置
        int index = 0;
        long startTime, endTime;
        for (DeleteMultipleVersionObj.KeyVersion kv : handle.getObjects()) {
            try {
                ObjectMeta object = new ObjectMeta(kv.getKey(), dbBucket.getName(), dbBucket.metaLocation);
                if (client.isBusy(object, OOSConfig.getMaxConcurrencyPerRegionServer())) {
                    Backoff.backoff();
                }
                //4.网关版本删除模式：指定版本N的情况下，
                //(1)对于>N的版本都保留
                //(2)对于≤N的版本，保留最大版本
                //(3)后续提到的指定版本号N均为上述含义
                startTime = System.currentTimeMillis();
                List<ObjectMeta> objects = client.objectGetRangeVersions(object, 0L, kv.getVersion());
                endTime = System.currentTimeMillis();
                log.info("get object version time cost: " + (endTime - startTime)+"; version count: "+objects.size());
                if (objects.size() <= 1) {
                    index++;
                    continue;
                }
                //保留<=N的最大版本
                objects.remove(objects.size() - 1);
                // 抽出方法。以抛异常的方式来决定是否退出外层循环。
                startTime = System.currentTimeMillis();
                int successNum = deleteMultiVersionObject(dbBucket, objects, bucketLog, pinDataList, result);
                endTime = System.currentTimeMillis();
                log.info("delete object time cost: " + (endTime - startTime)+"; object count: "+objects.size()+"; success count: "+successNum);
                //如果有失败的
                if (successNum != objects.size() && !result.getErrorContinue()) {
                    break;
                }
                index++;
                count += objects.size();
                if (count >= Consts.LIST_OBJECTS_MAX_KEY)
                    break;
            } catch (IOException ie) {
                log.error(ie.getMessage(), ie);
                index++;
                result.putUnexecuted(kv.getKey());
                if (!result.getErrorContinue())
                    break;
            } catch (Exception e) {
                log.error(e.getMessage(), e);
                index++;
                result.putErrorDetail(kv.getKey(), String.valueOf(kv.getVersion()), "SlowDown", "Service is busy.Please try again.");
                if (!result.getErrorContinue())
                    break;
            }
        }
        //将未执行删除的对象响应给客户端。
        if (index != handle.getObjects().size())
            result.putUnexecuted(handle.getObjects()
                    .subList(index, handle.getObjects().size()).stream()
                    .map(DeleteMultipleVersionObj.KeyVersion::getKey)
                    .collect(Collectors.toList()));

        //        System.out.println(result.toXmlString());
        return result.toXmlString();
    }

    /**
     * 根据前缀批量删除多版本对象
     * @param dbBucket
     * @param ip
     * @param bucketLog
     * @param pinDataList
     * @return
     * @throws BaseException
     * @throws IOException
     */
    private static String deleteMultipleObjectsByPrefix(BucketMeta dbBucket, InputStream ip, BucketLog bucketLog, List<PinData> pinDataList)
            throws BaseException, IOException {
        DeleteMultipleVersionObjByPrefix handle = null;
        try {
            handle = new XmlResponseSaxParser()
                    .parseDeleteMultipleVersionByPrefixObjects(ip)
                    .getDeleteMultipleVersionObjByPrefix();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new BaseException(400, ErrorMessage.ERROR_CODE_MALFORMEDXML);
        }
        // 检查数据合法性
        handle.checkParam();
        //遍历object计数
        int count = 0;
        //删除成功计数
        int totalSuccess = 0;
        DeleteMultipleVersionResult result = new DeleteMultipleVersionResult(handle.isQuiet(),
                handle.isErrorContinue(),handle.getPrefix(),handle.getVersionId() +"");
        boolean continueFlag = true;
        long startTime, endTime;
        while (continueFlag) {
            // 分批次查询符合条件的记录
            startTime = System.currentTimeMillis();
            List<List<ObjectMeta>> objects = client
                    .objectMetaList(dbBucket.metaLocation, dbBucket.name,
                            handle.getPrefix(), handle.getMarker(), null,
                            OOSConfig.getMultipleVersionDeleteScanNum(), 0, handle.getVersionId());
            endTime = System.currentTimeMillis();
            log.info("get objects time cost: " + (endTime - startTime)+"; object count: "+objects.size());
            if(objects.size() == 0) {
                return result.toXmlString();
            }
            if (objects.size() >= OOSConfig.getMultipleVersionDeleteScanNum()) {
                String marker = objects.get(objects.size() - 1).get(0).name;
                result.setNextKeyMarker(marker);
                handle.setMarker(marker);
            } else {
                continueFlag = false;
            }
            for(List<ObjectMeta> object : objects){
                //网关版本删除模式：指定版本N的情况下，
                //(1)对于>N的版本都保留
                //(2)对于≤N的版本，保留最大版本
                //(3)后续提到的指定版本号N均为上述含义
                if (object.size() <= 1) {
                    continue;
                }
                object.remove(object.size() - 1);
                //每个metas包含一个object的不同版本
                startTime = System.currentTimeMillis();
                int successNum = deleteMultiVersionObject(dbBucket, object, bucketLog, pinDataList, result);
                endTime = System.currentTimeMillis();
                log.info("delete object time cost: " + (endTime - startTime)+"; object count: "+object.size()+"; success count: "+successNum);
                totalSuccess += successNum;
                count += object.size();
                // 中断条件：
                // 1、删除出错，并且errorContinue=false
                // 2、处理对象数量超过Consts.LIST_OBJECTS_MAX_KEY限制
                // 3、成功数量>=参数中设置的限制
                if ((successNum != object.size() && !handle.isErrorContinue())
                        ||count >= Consts.LIST_OBJECTS_MAX_KEY
                        || totalSuccess >= handle.getMaxKey()) {
                    result.setNextKeyMarker(object.get(0).name);
                    continueFlag = false;
                    break;
                }
                log.info(
                        "successNum=" + successNum + ";object.size=" + object.size() + ";count=" + count
                                + ";totalSuccess=" + totalSuccess + ";errorContinue=" + handle.getErrorContinue()
                                + ";maxKey=" + handle.getMaxKey());
            }
        }
        return result.toXmlString();
    }

    /**
     * 删除集合中对象，并返回删除成功数量
     *
     * @param dbBucket
     * @param objectMetas
     * @param bucketLog
     * @param pinDataList
     * @param result
     * @return
     */
    private static int deleteMultiVersionObject(BucketMeta dbBucket, List<ObjectMeta> objectMetas,
            BucketLog bucketLog, List<PinData> pinDataList, DeleteMultipleVersionResult result) {
        int successNum = 0;
        boolean deleteSuccess;
        for(ObjectMeta object : objectMetas) {
            deleteSuccess = deleteSpecifyObjectVersion(dbBucket, object, bucketLog, pinDataList, result);
            if(deleteSuccess) {
                successNum++;
            }
            //如果失败了，并且不继续，则退出
            if(!result.getErrorContinue() && !deleteSuccess) {
                break;
            }
        }
        return successNum;
    }

    /**
     * 删除某个特定版本的object
     * @param dbBucket
     * @param object
     * @param bucketLog
     * @param pinDataList
     * @param result
     * @return
     */
    private static boolean deleteSpecifyObjectVersion(BucketMeta dbBucket, ObjectMeta object, BucketLog bucketLog,
            List<PinData> pinDataList, DeleteMultipleVersionResult result) {
        boolean success = true;
        String code = "";
        String msg = "Success";
        try {
            // if the object specified in the request is not found, Amazon S3 returns the result as deleted.
            if (!ObjectLockUtils.objectLockCheck(dbBucket, object)) {
                PinData pinData = new PinData();
                OpObject.deleteObject(dbBucket, object, bucketLog, pinData, true);
                pinDataList.add(pinData);
            } else {
                // 对象处于合规保留期内 不允许被操作
                code = ErrorMessage.ERROR_CODE_FILE_IMMUTABLE;
                msg = ErrorMessage.ERROR_MESSAGE_FILE_IMMUTABLE;
                success = false;
            }
        } catch (BaseException e) {
            log.error(e.getMessage(), e);
            if (e.status == 503) {
                code = "SlowDown";
                msg = "Service is busy.Please try again.";
            }else {
                code = "InternalError";
                msg = "We encountered an internal error.Please try again.";
            }
            success = false;
        }
        if (!success) {
            result.putErrorDetail(object.name, object.version, code, msg);
        }else {
            result.putSucessDetail(object.name, object.version);
        }
        return success;
    }


    public static String deleteMultipleObjectsStatic(HttpServletRequest req, BucketMeta dbBucket, InputStream ip,
            OwnerMeta owner, BucketLog bucketLog, List<PinData> pinDataList) throws IOException, BaseException {
        if(null != req.getHeader(Consts.X_AMZ_VERSION_DELETEMODE))
            return deleteMultipleVersionObjects(req,dbBucket,ip,owner,bucketLog,pinDataList);
        else
            return deleteMultipleObjects(req,dbBucket,ip,owner,bucketLog,pinDataList);
    }

    /**
     * 校验content摘要是否与头部md5签名一致
     * @param inputStream
     * @param signature
     * @return
     * @throws BaseException
     */
    private static ByteArrayInputStream checkMd5Sign(
            InputStream inputStream, String signature) throws BaseException {
        ArrayOutputStream aos = null;
        ByteArrayInputStream is;
        try {
            aos = new ArrayOutputStream(Consts.DEFAULT_BUFFER_SIZE);
            int size = IOUtils.copy(inputStream, aos);
            V4Signer.checkContentSignatureV4(inputStream);
            is = new ArrayInputStream(aos.data(), 0, size);
            byte[] md5 = Md5Utils.computeMD5Hash(is);
            is.reset();
            String md5Base64 = BinaryUtils.toBase64(md5);
            if (!md5Base64.equals(signature))
                throw new BaseException(400, ErrorMessage.ERROR_CODE_BAD_DIGEST);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new BaseException(400, ErrorMessage.ERROR_CODE_BAD_DIGEST);
        }finally {
            Utils.streamClose(aos);
        }
        return is;
    }

    public static String deleteMultipleObjects(HttpServletRequest req, BucketMeta dbBucket,
            InputStream ip, OwnerMeta owner, BucketLog bucketLog, List<PinData> pinDataList) throws IOException, BaseException {
        DeleteObjectsRequest objects = null;
        InputStream is2 = null;
        InputStream is = null;
        ArrayOutputStream aos = null;
        try {
            try {
                aos = new ArrayOutputStream(Consts.DEFAULT_BUFFER_SIZE);
                int size = IOUtils.copy(ip, aos);
                V4Signer.checkContentSignatureV4(ip);
                is = new ArrayInputStream(aos.data(), 0, size);
                is2 = new ArrayInputStream(aos.data(), 0, size);
                byte[] md5 = Md5Utils.computeMD5Hash(is);
                String md5Base64 = BinaryUtils.toBase64(md5);
                if (!md5Base64.equals(req.getHeader(Headers.CONTENT_MD5)))
                    throw new BaseException(400, ErrorMessage.ERROR_CODE_BAD_DIGEST);
            } catch (Exception e) {
                log.error(e.getMessage(), e);
                throw new BaseException(400, ErrorMessage.ERROR_CODE_BAD_DIGEST);
            }
            try {
                objects = new XmlResponseSaxParser().parseDeleteMultipleObjects(is2)
                        .getDeleteObjectsRequest();
            } catch (Throwable e) {
                log.error(e.getMessage(), e);
                throw new BaseException(400, ErrorMessage.ERROR_CODE_MALFORMEDXML);
            }
            boolean existsNull = objects.getKeys().contains(null);
            if (existsNull) {
                throw new BaseException(400, ErrorMessage.ERROR_CODE_MALFORMEDXML, ErrorMessage.ERROR_MESSAGE_MALFORMEDXML);
            }

        } finally {
            if (is2 != null)
                try {
                    is2.close();
                } catch (Throwable e) {
                    log.error(e.getMessage(), e);
                }
            if (is != null) {
                try {
                    is.close();
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
        if (objects.getKeys().size() > Consts.LIST_OBJECTS_MAX_KEY)
            throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_ARGUMENT, "the number of objects exceeds "
                    + Consts.LIST_OBJECTS_MAX_KEY);
        XmlWriter xml = new XmlWriter();
        xml.start("DeleteResult", "xmlns", Consts.XMLNS);
        for (KeyVersion keyVersion : objects.getKeys()) {
            PinData pinData = new PinData();
            ObjectMeta object = new ObjectMeta(keyVersion.getKey(), dbBucket.getName(), dbBucket.metaLocation);
            bucketLog.getMetaTime = System.currentTimeMillis();
            boolean objectExist;
            try{
                if(client.isBusy(object, OOSConfig.getMaxConcurrencyPerRegionServer()))
                    Backoff.backoff();
                objectExist = client.objectSelect(object);
            } finally {
                bucketLog.getMetaTime = System.currentTimeMillis() - bucketLog.getMetaTime;
            }
            String code = "Success";
            String msg = "Success";
            long objectSize = 0;
            long redundantSize = 0;
            long alinSize = 0;
            try {
                if (!Utils.ifCanWrite(owner, dbBucket)) {
                    code = "AccessDenied";
                    msg = "Access Denied";
                } else if (!objectExist) {
                    code = "Success";
                    msg = "Success";// if the object specified in
                    // the request is not found, Amazon S3 returns the result as
                    // deleted.
                } else if (ObjectLockUtils.objectLockCheck(dbBucket, object)) {
                    // 对象处于合规保留期内 不允许被操作
                    code = ErrorMessage.ERROR_CODE_FILE_IMMUTABLE;
                    msg = ErrorMessage.ERROR_MESSAGE_FILE_IMMUTABLE;
                } else {
                    Map<InitialUploadMeta, List<UploadMeta>> map = new LinkedHashMap<InitialUploadMeta, List<UploadMeta>>();
                    Triple<Long, Long, Long> triple = OpObject.deleteObjectMeta(dbBucket, object, map, bucketLog, false);
                    objectSize = triple.first();
                    redundantSize = triple.second();
                    alinSize = triple.third();
                    pinData.setSize(objectSize, redundantSize, alinSize, object.dataRegion, object.originalDataRegion, object.storageClassString, object.getLastCostTime());
                    //只有成功删除才更改统计量
                    pinDataList.add(pinData);
                    try {
                        OpObject.deleteObject(dbBucket, object, bucketLog, false, false, map);// ostor底部异步删除
                    } catch (Exception e) {
                        //只要deleteObjectMeta成功了就认为删除成功了，但是存在数据没有删除成功的情况。
                        log.warn("DeleteObjectMeta success, but deleteObjectData failed. ownerId : " + dbBucket.getOwnerId() + " MetaRegionName: " + object.metaRegionName
                                + ", bucketName: " + object.bucketName + ", objectName: " + object.name, e);
                    } 
                }
            } catch (Throwable e) {
                log.error(e.getMessage(), e);
                code = "InternalError";
                msg = "Internal Error";
            } finally {
                // 批量删除打印用户的详细请求结果，暂时用于定位是否存在重复删除同一个对象的情况
                log.info("OpBucket.deleteMultipleObjects delete ownerId : " + (Objects.nonNull(owner) ? owner.getId() : "null") + " bucket : " + dbBucket.getName() + " objectName : " + keyVersion.getKey() + " code : " + code + " objectSize : " + objectSize);
            }
            if (msg.equals("Success")) {
                if (!objects.getQuiet()) {
                    xml.start("Deleted");
                    xml.start("Key").value(object.name).end();
                    xml.end();
                }
            } else {
                xml.start("Error");
                xml.start("Key").value(keyVersion.getKey()).end();
                xml.start("Code").value(code).end();
                xml.start("Message").value(msg).end();
                xml.end();
            }
            
        }
        xml.end();
        if (log.isDebugEnabled())
            log.debug(xml.toString());
        return Consts.XML_HEADER + xml.toString();
    }
    
    public static String deleteMultiVersionRangeObjectStatics(HttpServletRequest req, BucketMeta dbBucket,
            List<ObjectMeta> objects, OwnerMeta owner, BucketLog bucketLog, List<PinData> pinDataList, PinData pinData) throws IOException, BaseException{
        boolean isPublicIp = Utils.isPublicIP(bucketLog.ipAddress);
        try {
            if (Backoff.checkPutGetDeleteOperationsBackoff(HttpMethodName.DELETE, isPublicIp)
                    && !BackoffConfig.getWhiteList().contains(dbBucket.name))
                Backoff.backoff();
            return deleteMultiVersionRangeObject(req,dbBucket,objects,owner,bucketLog,pinDataList, pinData);
        } finally {
            Backoff.decreasePutGetDeleteOperations(HttpMethodName.DELETE, isPublicIp);
        }
    }
    
    /**
     * 删除一个对象的指定版本范围的多个版本
     * @param req
     * @param dbBucket
     * @param objects
     * @param owner
     * @param bucketLog
     * @return
     * @throws IOException
     * @throws BaseException
     */
    public static String deleteMultiVersionRangeObject(HttpServletRequest req, BucketMeta dbBucket,
            List<ObjectMeta> objects, OwnerMeta owner, BucketLog bucketLog, List<PinData> pinDataList, PinData pinData) throws IOException, BaseException {
        if (objects.size() > Consts.LIST_OBJECTS_MAX_KEY)
            throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_ARGUMENT, "the number of objects exceeds "
                    + Consts.LIST_OBJECTS_MAX_KEY);
        XmlWriter xml = new XmlWriter();
        xml.start("DeleteResult", "xmlns", Consts.XMLNS);
        boolean gwMultiVersionEnabled = false;
        if(MultiVersionsController.gwMultiVersionEnabled(req, dbBucket)) {
            gwMultiVersionEnabled = true;
        }
        long totalDeleteSize = 0;
        for (ObjectMeta object : objects) {
            PinData data = new PinData();
//            client.objectSelect(object);
            String msg = "Success";
            long objectSize = 0;
            long redundantSize = 0;
            long alinSize = 0;
            try {
                Map<InitialUploadMeta, List<UploadMeta>> map = new LinkedHashMap<InitialUploadMeta, List<UploadMeta>>();
                Triple<Long, Long, Long> triple = OpObject.deleteObjectMeta(dbBucket, object, map, bucketLog, gwMultiVersionEnabled);
                objectSize = triple.first();
                redundantSize = triple.second();
                alinSize = triple.third();
                data.setSize(objectSize, redundantSize, alinSize, object.dataRegion, object.originalDataRegion, object.storageClassString, object.getLastCostTime());
                pinDataList.add(data);                         
                totalDeleteSize += objectSize;
                try {
                    OpObject.deleteObject(dbBucket, object, bucketLog, false, gwMultiVersionEnabled, map);// ostor底部异步删除
                } catch (Exception e) {
                    //只要deleteObjectMeta成功了就认为删除成功了，但是存在数据没有删除成功的情况。
                    log.warn("DeleteObjectMeta success, but deleteObjectData failed. ownerId : " + dbBucket.getOwnerId() + " MetaRegionName: " + object.metaRegionName
                            + ", bucketName: " + object.bucketName + ", objectName: " + object.name, e);
                }
            } catch (Throwable e) {
                log.error(e.getMessage(), e);
                msg = "InternalError";
            }
            if (!msg.equals("Success")) {
                xml.start("Error");
                xml.start("Key").value(object.name).end();
                xml.start("VersionId").value(object.version).end();
                xml.start("Code").value(msg).end();
                xml.start("Message").value(msg).end();
                xml.end();
            }
        }
        xml.end();
        if (log.isDebugEnabled())
            log.debug(xml.toString());        
        pinData.jettyAttributes.objectAttribute.objectSize = totalDeleteSize;
//        req.setAttribute(Consts.ATTRIBUTE_OBJECT_SIZE, totalDeleteSize);
        return Consts.XML_HEADER + xml.toString();
    }
  
    /**
     * 1 过期时间要大于转储（转换存储类型）开始时间</br>
            2 不能从任何存储类转换到STANDARD存储类。标准类型的 两种。</br>
            3    不能从任何存储类转换为REDUCED_REDUNDANCY存储类。</br>
            4    如果用户创建第二个相同前缀的生命周期策略，无法创建成功。 </br>
            5    每个Bucket以整个Bucket维度匹配的生命周期策略只能存在一个且不与其他生命周期策略共存
            是否允许某个目录已经创建了bucket级别，是否能put目录级别 /  和/A
     * */
    public static void putBucketLifecycle(BucketMeta dbBucket, InputStream ip, long length,
            String contentMD5) throws BaseException, IOException {
        if (contentMD5 == null || contentMD5.trim().length() == 0)
            throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_REQUEST,
                    "Missing required header for this request: Content-MD5");
        EtagMaker etagmaker = new EtagMaker();
        SignableInputStream sin = new SignableInputStream(ip, length, etagmaker);
        BucketLifecycleConfiguration lifeConf;
        try {
            lifeConf = new XmlResponseSaxParser().parseBucketLifecycleConfigurationResponse(sin).getConfiguration();
        } catch (AmazonClientException e) {
            throw new BaseException(400, ErrorMessage.ERROR_CODE_MALFORMEDXML, ErrorMessage.ERROR_MESSAGE_MALFORMEDXML);
        }
        V4Signer.checkContentSignatureV4(ip);
        Utils.checkMd5(contentMD5, etagmaker.digest());
        if (lifeConf.getRules() == null || lifeConf.getRules().isEmpty())
            throw new BaseException(lifeConf.toString(), 400, ErrorMessage.ERROR_CODE_MALFORMEDXML,
                    ErrorMessage.ERROR_MESSAGE_MALFORMEDXML);
        if (lifeConf.getRules().size() > Consts.LIFECYCLE_RULES_MAX_SIZE)
            throw new BaseException(lifeConf.toString(), 400, ErrorMessage.ERROR_CODE_MALFORMEDXML,
                    ErrorMessage.ERROR_MESSAGE_MALFORMEDXML);
        //prefix 区分转储和过期
        List<Pair<String, Object>> expirationPair = new ArrayList<>();
        List<Pair<String, Object>> transitionPair = new ArrayList<>();
        List<String> ids = new ArrayList<String>();
        for (Rule r : lifeConf.getRules()) {
            //rule id 长度必须小于255
            if (r.getId() != null && r.getId().length() > Consts.LIFECYCLE_RULEID_MAX_LENGTH)
                throw new BaseException(lifeConf.toString(), 400, ErrorMessage.ERROR_CODE_INVALID_ARGUMENT,
                        "ID length should not exceed allowed limit of "
                                + Consts.LIFECYCLE_RULEID_MAX_LENGTH);
            if (r.getPrefix() == null) {
                throw new BaseException(lifeConf.toString(), 400, ErrorMessage.ERROR_CODE_MALFORMEDXML, ErrorMessage.ERROR_MESSAGE_MALFORMEDXML);
            }
            //prefix 长度必须小于1024
            if (r.getPrefix() != null
                    && r.getPrefix().length() > Consts.LIFECYCLE_PREFIX_MAX_LENGTH)
                throw new BaseException(lifeConf.toString(), 400, ErrorMessage.ERROR_CODE_INVALID_REQUEST,
                        "The maximum size of a prefix is " + Consts.LIFECYCLE_PREFIX_MAX_LENGTH);
            //status 状态只能有enable 和disable两种之一
            if (r.getStatus() == null
                    || (!r.getStatus().equals(BucketLifecycleConfiguration.DISABLED)
                            && !r.getStatus().equals(BucketLifecycleConfiguration.ENABLED)))
                throw new BaseException(lifeConf.toString(), 400, ErrorMessage.ERROR_CODE_MALFORMEDXML,
                        ErrorMessage.ERROR_MESSAGE_MALFORMEDXML);
            //如果id为空，随机生成一个id
            if (r.getId() == null || r.getId().trim().length() == 0)
                r.setId(RandomStringUtils
                        .randomAlphanumeric(Consts.LIFECYCLE_RANDOM_RULEID_LENGTH));
            
            //策略没有配置过期时间
            if(r.getExpirationDate() == null && r.getExpirationInDays() == BucketLifecycleConfiguration.INITIAL_DAYS && (null == r.getTransition() ||
                     (r.getTransition().getDate() == null && r.getTransition().getDays() == BucketLifecycleConfiguration.INITIAL_DAYS))) {
                throw new BaseException(lifeConf.toString(), 400,
                        ErrorMessage.ERROR_CODE_INVALID_ARGUMENT,
                        "At least one action needs to be specified in a rule");
            }
            
            //校验时间格式
            if(null != r.getTransition() 
                    && (r.getTransition().getDate() != null || r.getTransition().getDays() != BucketLifecycleConfiguration.INITIAL_DAYS))
                checkLifecycleDateAndDays(r.getTransition().getDate(),r.getTransition().getDays(), "Transition");
            if(r.getExpirationDate() != null || r.getExpirationInDays() != BucketLifecycleConfiguration.INITIAL_DAYS)
                checkLifecycleDateAndDays(r.getExpirationDate(), r.getExpirationInDays(), "Expiration");
            
            //只有转储配置存在才需进行验证 本期只支持转低频
            if(null != r.getTransition() && !Consts.STORAGE_CLASS_STANDARD_IA.equals(r.getTransition().getStorageClass()))
                throw new BaseException(lifeConf.toString(), 400, ErrorMessage.ERROR_CODE_INVALID_ARGUMENT,
                        "Invalid target Storage class for Transition action");
            
            //id 不能重复
            if (ids.contains(r.getId()))
                throw new BaseException(lifeConf.toString(), 400, ErrorMessage.ERROR_CODE_INVALID_REQUEST,
                        "Rule ID must be unique. Found same ID for more than one rule");
            else
                ids.add(r.getId());
            
            //记录所有出现过的prefix
            String prefix = (r.getPrefix() != null && r.getPrefix().trim().length() > 0) 
                    ? r.getPrefix() : BucketLifecycleConfiguration.ROOT_PREFIX;
            if(null != r.getExpirationDate()) {
                expirationPair.add(new Pair<String, Object>(prefix, r.getExpirationDate()));
            }else if(r.getExpirationInDays() != BucketLifecycleConfiguration.INITIAL_DAYS){
                expirationPair.add(new Pair<String, Object>(prefix, r.getExpirationInDays()));
            }
            if(null != r.getTransition()) {
                if(null != r.getTransition().getDate())
                    transitionPair.add(new Pair<String, Object>(prefix, r.getTransition().getDate()));
                else if(r.getTransition().getDays() != BucketLifecycleConfiguration.INITIAL_DAYS) 
                    transitionPair.add(new Pair<String, Object>(prefix,  r.getTransition().getDays()));
            }
        }
        
        //不能出现两个相同prefix的配置。 不能出现两个prefix有包含关系的
        checkLifecyclePrefix(expirationPair, "Expiration", lifeConf);
        checkLifecyclePrefix(transitionPair, "Transition", lifeConf);
        checkLifecycleDaysAndDate(expirationPair, transitionPair, lifeConf);
        dbBucket.lifecycle = lifeConf;
        client.bucketInsert(dbBucket);
    }
    
    public static String getBucketLifecycle(BucketMeta bucket) throws IOException, BaseException {
        if (bucket.lifecycle == null)
            throw new BaseException(404, "NoSuchLifecycleConfiguration",
                    "The lifecycle configuration does not exist.");
        XmlWriter xml = new XmlWriter();
        xml.start("LifecycleConfiguration", "xmlns", Consts.XMLNS);
        for (Rule rule : bucket.lifecycle.getRules()) {
            xml.start("Rule");
            xml.start("ID").value(rule.getId()).end();
            xml.start("Prefix").value(rule.getPrefix()).end();
            xml.start("Status").value(rule.getStatus()).end();
            if(null != rule.getExpirationDate() ||
                    rule.getExpirationInDays() != BucketLifecycleConfiguration.INITIAL_DAYS) {
                xml.start("Expiration");
                if (rule.getExpirationDate() == null)
                    xml.start("Days").value(String.valueOf(rule.getExpirationInDays())).end();
                else
                    xml.start("Date").value(ServiceUtils
                            .formatIso8601Date(rule.getExpirationDate())).end();
                //Expiration end
                xml.end();
            }
            if(null != rule.getTransition()) {
                xml.start("Transition");
                if(rule.getTransition().getDate() == null) {
                    xml.start("Days").value(String.valueOf(rule.getTransition().getDays())).end();
                } else {
                    xml.start("Date").value(ServiceUtils
                            .formatIso8601Date(rule.getTransition().getDate())).end();
                }
                xml.start("StorageClass").value(rule.getTransition().getStorageClass()).end();
                //Transition end
                xml.end();
            }
            //Rule end
            xml.end();
        }
        //LifecycleConfiguration end
        xml.end();
        String str = Consts.XML_HEADER + xml.toString();
        return str;
    }
    
    public static void putAccelerate(BucketMeta dbBucket, InputStream ip) throws BaseException, IOException{
        if (dbBucket == null)
            throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_ARGUMENT);
        
        //bucket名不包含“.”
        boolean dotExits = dbBucket.name.indexOf('.') != -1;
        if (dotExits){
            throw new BaseException(404, "CanNotIncludespecificChar");
        }
        
        boolean bucketExists = client.bucketSelect(dbBucket);
        if (!bucketExists){
            throw new BaseException(404, ErrorMessage.ERROR_CODE_NO_SUCH_BUCKET);
        }
        
        //解析用户上传的XML文件
        AccelerateMeta acceMeta = AccelerateXmlParser.parseAccelerateConfiguration(dbBucket.name, ip);
        V4Signer.checkContentSignatureV4(ip);
        if (acceMeta == null){
            throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_ARGUMENT,
                    "AccelerateMeta can not be null");
        }
        
        if (acceMeta.status == null || acceMeta.status.length() ==0){
            throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_ARGUMENT,
                    "status can not be null");
        }
        
        if (acceMeta.ipWhiteList != null && acceMeta.ipWhiteList.size() > 5){
            throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_ARGUMENT,
                    "IP can not more than 5");
        }
        client.accelerateInsert(acceMeta);
    }
    
    public static String getAccelerate (String bucketName) throws IOException, BaseException {
        if (bucketName == null || bucketName.length() == 0)
            throw new BaseException(404, "NoSuchBucketConfiguration",
                    "The bucketName can not be null.");
        
        AccelerateMeta acceMeta = client.accelerateSelect(bucketName);
        
        XmlWriter xml = new XmlWriter();
        xml.start("AccelerateConfiguration", "xmlns", Consts.XMLNS);
               
        if ( (acceMeta != null) && (acceMeta.status != null) 
                && (acceMeta.status.length() > 0) ){
            xml.start("Status").value(acceMeta.status).end();
        }
        
        if (acceMeta != null && acceMeta.ipWhiteList != null && 
                acceMeta.ipWhiteList.size()>0){
            xml.start("IPWhiteLists");
            for(String ipStr : acceMeta.ipWhiteList) {
                xml.start("IP").value(ipStr).end();
            }
            xml.end();
        }
        xml.end();
        String str = Consts.XML_HEADER + xml.toString();
        return str;
    }

    public static void putBucketCors(BucketMeta dbBucket, InputStream ip, int length) throws BaseException, IOException {
        BucketCrossOriginConfiguration corsConfiguration = null;
        if (ip != null ) {
            if (length > Consts.MAX_CORS_XML_SIZE) {
                throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_BUCKET_CORS_CONFIGURE,
                        ErrorMessage.ERROR_MESSAGE_INVALID_BUCKET_CORS_XML_SIZE);
            }
            corsConfiguration = getBucketCorsConfiguration(ip);
            V4Signer.checkContentSignatureV4(ip);
        } else {
            throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_BUCKET_CORS_CONFIGURE,
                    ErrorMessage.ERROR_CODE_INVALID_BUCKET_CORS_CONFIGURE);
        }
        if (corsConfiguration != null) {
            List<CORSRule> rules = corsConfiguration.getRules();
            if (rules == null || rules.isEmpty())
                throw new BaseException(400, ErrorMessage.ERROR_CODE_MALFORMEDXML,
                        ErrorMessage.ERROR_MESSAGE_BUCKET_CORS_EMPTY);
            if (rules.size() > Consts.MAX_CORS_RULE_NUMBER) {
                throw new BaseException(400, ErrorMessage.ERROR_CODE_MALFORMEDXML,
                        ErrorMessage.ERROR_MESSAGE_INVALID_BUCKET_CORS_NUMBER);
            }
            for (CORSRule rule:rules) {
                checkCORSRuleValid(rule);
            }
            checkCORSRuleIdUnique(rules);
            dbBucket.cors = corsConfiguration;
            client.bucketUpdate(dbBucket);
        } else {
            throw new BaseException(400, ErrorMessage.ERROR_CODE_MALFORMEDXML,
                    ErrorMessage.ERROR_MESSAGE_BUCKET_CORS_EMPTY);
        }
    }
    
    /**
     * 校验CORSRule合法性
     * @param rule
     * @throws BaseException
     */
    public static void checkCORSRuleValid(CORSRule rule) throws BaseException {
        String id = rule.getId();
        if (id != null && !id.isEmpty() && id.length() > Consts.MAX_RULE_ID_SIZE) {
            throw new BaseException(400, ErrorMessage.ERROR_CODE_MALFORMEDXML,
                    ErrorMessage.ERROR_MESSAGE_BUCKET_CORS_ID_LENGTH);
        }
        List<String> origins = rule.getAllowedOrigins();
        if (origins == null || origins.isEmpty()) {
            throw new BaseException(400, ErrorMessage.ERROR_CODE_MALFORMEDXML,
                    ErrorMessage.ERROR_MESSAGE_INVALID_BUCKET_CORS_ALLOWEDORIGIN);
        }
        List<AllowedMethods> methods = rule.getAllowedMethods();
        if (methods == null || methods.isEmpty()) {
            throw new BaseException(400, ErrorMessage.ERROR_CODE_MALFORMEDXML,
                    ErrorMessage.ERROR_MESSAGE_INVALID_BUCKET_CORS_ALLOWEDMETHOD);
        }
        for (String origin:origins) {
            if (StringUtils.countMatches(origin, "*") > 1) {
                throw new BaseException(400, ErrorMessage.ERROR_CODE_MALFORMEDXML,
                        "AllowedOrigin "+'"'+origin+'"'+" can not have more than one wildcard.");
            }
        }
        List<String> headers = rule.getAllowedHeaders();
        if (headers != null && !headers.isEmpty()) {
            for (String header:headers) {
                if (StringUtils.countMatches(header, "*") > 1) {
                    throw new BaseException(400, ErrorMessage.ERROR_CODE_MALFORMEDXML,
                            "AllowedHeader "+'"'+header+'"'+" can not have more than one wildcard.");
                }
            }
        }
        List<String> exposeHeaders = rule.getExposedHeaders();
        if (exposeHeaders != null && !exposeHeaders.isEmpty()) {
            for (String exposeHeader:exposeHeaders) {
                if (exposeHeader.contains("*")) {
                    throw new BaseException(400, ErrorMessage.ERROR_CODE_MALFORMEDXML,
                            "ExposeHeader \""+exposeHeader+"\" contains wildcard. We currently do not support wildcard for ExposeHeader.");
                }
            }
        }
    }
    
    /**
     * 检查CORSRule ID唯一性
     * @param rules
     * @throws BaseException 
     */
    public static void checkCORSRuleIdUnique(List<CORSRule> rules) throws BaseException {
        if (rules == null || rules.isEmpty()) {
            return;
        }
        Set<String> s = new HashSet<String>();
        for (CORSRule rule : rules) {
            if (rule.getId() != null && !rule.getId().isEmpty()) {
                boolean unique = s.add(rule.getId());
                if (!unique) {
                    throw new BaseException(400, ErrorMessage.ERROR_CODE_MALFORMEDXML, ErrorMessage.ERROR_MESSAGE_BUCKET_CORS_ID_UNIQUE);
                }
            }
        }
    }
    
    /**
     * 获取xml中的bucket cors配置信息
     * @param ip
     * @return
     * @throws BaseException
     */
    private static BucketCrossOriginConfiguration getBucketCorsConfiguration(InputStream ip) throws BaseException {
        BucketCrossOriginConfiguration corsConfiguration = null;
        try {
            corsConfiguration = new XmlResponsesSaxParser().parseBucketCrossOriginConfigurationResponse(ip).getConfiguration();
        } catch (Exception e) {
            throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_BUCKET_CORS_CONFIGURE,
                    ErrorMessage.ERROR_MESSAGE_INVALID_BUCKET_CORS_XML);
        }
        return corsConfiguration;
    }

    private static CtyunGetBucketLocationResult getBucketLocation(InputStream ip)
            throws BaseException {
        CtyunGetBucketLocationResult location;
        try {
            location = new XmlResponsesSaxParser().parseBucketLocationResponse(ip);
        } catch (IllegalArgumentException e) {
            throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_LOCATION_CONSTRAINT,
                    ErrorMessage.ERROR_MESSAGE_INVALID_LOCATION_CONSTRAINT);
        }
        return location;
    }

    public static void put(BucketMeta bucket, InputStream ip, boolean bucketExists, OwnerMeta owner,
            String aclHeader, HttpServletResponse resp)
            throws BaseException, AmazonClientException, IOException {
        long bucketNumCeiling = 0;
        if (owner.bucketCeilingNum != -1) {
            // 用户信息中配置了存储桶上限，即单独为该用户配置过
            bucketNumCeiling = owner.bucketCeilingNum;
        } else {
            // 从UserType表中获取其类型的存储桶上限数据
            UserTypeMeta userTypeMeta = new UserTypeMeta(owner.userType);
            if(userTypeMeta.getUserType() == null)
                throw new BaseException(404, "NoSuchUserType");
            client.userTypeSelect(userTypeMeta);
            bucketNumCeiling = userTypeMeta.getBucketNumCeiling().ceiling;
        }
        if (!bucketExists && client.bucketList(owner.getId()).size() >= bucketNumCeiling) {
            throw new BaseException(400, "TooManyBuckets");
        }
        
        bucket.ownerId = owner.getId();
        OpBucket.getAclFromRequest(aclHeader, bucket);
        //获取用户的元数据域，数据域
        Pair<Set<String>, Set<String>> regions = client.getRegions(owner.getId());
        Set<String> metaRegions = regions.first();
        Set<String> dataRegions = regions.second();
        CtyunGetBucketLocationResult location = null;
        //获取请求中的bucket配置信息
        if (ip != null && ip.available() > 0) {
            location = getBucketLocation(ip);
            V4Signer.checkContentSignatureV4(ip);
        }
        if (dataRegions.size() == 1) {
            //用户的数据域只有一个，不能配置bucket数据域
            if (location != null && location.getDataLocation() != null)
                throw new BaseException(405, ErrorMessage.ERROR_CODE_405);
            bucket.dataLocation.setDataRegions(new ArrayList<String>(dataRegions));
            bucket.dataLocation.setStragegy(CtyunBucketDataScheduleStrategy.NotAllowed);
            bucket.dataLocation.setType(CtyunBucketDataType.Specified);
        } else {
            //根据用户的配置，设置bucket数据域
            if (location != null && location.getDataLocation() != null) {
                bucket.dataLocation.setType(location.getDataLocation().getType());
                bucket.dataLocation.setStragegy(location.getDataLocation().getStragegy());
                List<String> bucketDataRegions = location.getDataLocation().getDataRegions();
                if (bucket.dataLocation.getType().equals(CtyunBucketDataType.Specified)) {
                    if (bucketDataRegions == null || bucketDataRegions.size() == 0)
                        throw new BaseException(400,
                                ErrorMessage.ERROR_CODE_INVALID_LOCATION_CONSTRAINT,
                                ErrorMessage.ERROR_MESSAGE_INVALID_LOCATION_CONSTRAINT);
                    for (String dataRegion : bucketDataRegions) {
                        if (!dataRegions.contains(dataRegion)) {
                            log.error(ErrorMessage.ERROR_CODE_INVALID_LOCATION_CONSTRAINT + "::DataRegoins=" + dataRegions + ", user bucketDataRegions=" + bucketDataRegions);
                            throw new BaseException(400,
                                    ErrorMessage.ERROR_CODE_INVALID_LOCATION_CONSTRAINT,
                                    ErrorMessage.ERROR_MESSAGE_INVALID_LOCATION_CONSTRAINT);
                        }
                    }
                    // data region不能重复
                    Set<String> dataSet = new HashSet<String>();
                    dataSet.addAll(bucketDataRegions);
                    if (dataSet.size() != bucketDataRegions.size()) {
                        log.error(ErrorMessage.ERROR_CODE_INVALID_LOCATION_CONSTRAINT + "::DataRegoins=" + dataRegions + ", user bucketDataRegions=" + bucketDataRegions);
                        throw new BaseException(400,
                                ErrorMessage.ERROR_CODE_INVALID_LOCATION_CONSTRAINT,
                                ErrorMessage.ERROR_MESSAGE_INVALID_LOCATION_CONSTRAINT);
                    }
                    bucket.dataLocation.setDataRegions(location.getDataLocation().getDataRegions());
                }
                //这里判断下bucket 是否已经有datalocation 如果有了。 不赋默认值。 
            } else if(location == null && bucket.dataLocation.getDataRegions() == null)
                bucket.dataLocation.setType(CtyunBucketDataType.Local);
            if (bucket.dataLocation.getType().equals(CtyunBucketDataType.Local))
                bucket.dataLocation.setDataRegions(null);
        }
        if (metaRegions.size() == 1) {
            //用户的元数据域只有一个，不能配置bucket元数据域
            if (location != null && location.getMetaRegion() != null)
                throw new BaseException(405, ErrorMessage.ERROR_CODE_405);
            // 元数据域不能改
            if (!bucketExists) {
                bucket.metaLocation = (String) metaRegions.toArray()[0];
            }
        } else {
            if (bucketExists && location != null && location.getMetaRegion() != null)
                throw new BaseException(400,
                        ErrorMessage.ERROR_CODE_CAN_NOT_MODIFY_METADATA_LOCATION,
                        ErrorMessage.ERROR_MESSAGE_CAN_NOT_MODIFY_METADATA_LOCATION);
            if (location != null && location.getMetaRegion() != null) {
                if (!metaRegions.contains(location.getMetaRegion())) {
                    log.error(ErrorMessage.ERROR_CODE_INVALID_LOCATION_CONSTRAINT + "::Location.getMetaRegion=" + (location == null ? null : location.getMetaRegion()) + ", metaRegions=" + metaRegions);
                    throw new BaseException(400,
                            ErrorMessage.ERROR_CODE_INVALID_LOCATION_CONSTRAINT,
                            ErrorMessage.ERROR_MESSAGE_INVALID_LOCATION_CONSTRAINT);
                }
                bucket.metaLocation = location.getMetaRegion();
            }
        }
        if (bucket.metaLocation == null) {
            log.error(ErrorMessage.ERROR_CODE_INVALID_LOCATION_CONSTRAINT + "::Location.getMetaRegion=" + (location == null ? null : location.getMetaRegion()) + ", metaRegions=" + metaRegions);
            throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_LOCATION_CONSTRAINT,
                    ErrorMessage.ERROR_MESSAGE_INVALID_LOCATION_CONSTRAINT);
        }
        if (bucketExists)
            client.bucketUpdate(bucket);
        else {
            //Bucket存在性全局验证                    
            BssAdapterClient.create(bucket.name,
                                    BssAdapterConfig.localPool.ak,
                                    BssAdapterConfig.localPool.getSK(),
                                    BucketOwnerConsistencyType.BUCKET);
            if(!client.atomicBucketInsert(bucket)) {
                throw new BaseException(409, ErrorMessage.ERROR_CODE_BUCKET_ALREADY_EXIST, ErrorMessage.ERROR_MESSAGE_BUCKET_ALREADY_EXIST);
            }
        }    
        client.bucketSelect(bucket);
        resp.setStatus(200);
        resp.setHeader("Location", "/" + bucket.name);
    }

    public static String getBucketLocation(BucketMeta bucket) throws IOException, BaseException {
        Pair<Set<String>, Set<String>> res = client.getShowRegions(bucket.getOwnerId());
        XmlWriter xml = new XmlWriter();
        xml.start("BucketConfiguration", "xmlns", Consts.XMLNS);
        if (res.first().size() > 0) {
            xml.start("MetadataLocationConstraint");
            xml.start("Location").value(bucket.metaLocation).end();
            xml.end();
        }
        if (res.second().size() > 0) {
            xml.start("DataLocationConstraint");
            xml.start("Type").value(bucket.dataLocation.getType().name()).end();
            if (bucket.dataLocation.getDataRegions() != null) {
                xml.start("LocationList");
                for (String r : bucket.dataLocation.getDataRegions())
                    xml.start("Location").value(r).end();
                xml.end();
            }
            xml.start("ScheduleStrategy").value(bucket.dataLocation.getStragegy().name()).end();
            xml.end();
        }
        xml.end();
        String str = Consts.XML_HEADER + xml.toString();
        return str;
    }
    
    public static String getBucketCors(BucketMeta bucket) throws IOException, BaseException {
        if (bucket.cors == null)
            throw new BaseException(404, ErrorMessage.ERROR_CODE_NO_SUCH_BUCKET_CORS,
                    ErrorMessage.ERROR_MESSAGE_NO_SUCH_BUCKET_CORS);
        XmlWriter xml = new XmlWriter();
        xml.start("CORSConfiguration", "xmlns", Consts.XMLNS);
        if (bucket.cors.getRules() != null && !bucket.cors.getRules().isEmpty()) {
            for (CORSRule rule : bucket.cors.getRules()) {
                xml.start("CORSRule");
                if (rule.getId() != null && !rule.getId().isEmpty()) {
                    xml.start("ID").value(rule.getId()).end();
                }
                List<String> allowedOrigins = rule.getAllowedOrigins();
                if (allowedOrigins !=null && !allowedOrigins.isEmpty()) {
                    for (String allowedOrigin:allowedOrigins) {
                        xml.start("AllowedOrigin").value(allowedOrigin).end();
                    }
                }
                List<AllowedMethods> allowedMethods = rule.getAllowedMethods();
                if (allowedMethods !=null && !allowedMethods.isEmpty()) {
                    for (AllowedMethods allowedMethod:allowedMethods) {
                        xml.start("AllowedMethod").value(allowedMethod.toString()).end();
                    }
                }
                if (rule.getMaxAgeSeconds() != 0) {
                    xml.start("MaxAgeSeconds").value(String.valueOf(rule.getMaxAgeSeconds())).end();
                }
                List<String> allowedHeaders = rule.getAllowedHeaders();
                if (allowedHeaders !=null && !allowedHeaders.isEmpty()) {
                    for (String allowedHeader:allowedHeaders) {
                        xml.start("AllowedHeader").value(allowedHeader).end();
                    }
                }
                List<String> exposeHeaders = rule.getExposedHeaders();
                if (exposeHeaders !=null && !exposeHeaders.isEmpty()) {
                    for (String exposeHeader:exposeHeaders) {
                        xml.start("ExposeHeader").value(exposeHeader).end();
                    }
                }
                xml.end();
            }
        }
        xml.end();
        String str = Consts.XML_HEADER + xml.toString();
        return str;
    }
    
    /**
     * 处理简单请求和预检请求通过后的CORS请求，请求若没有通过授权验证，不处理
     * @param request
     * @param response
     * @param bucket
     */
    public static void handleSimpleAndActualCORS(HttpServletRequest request, HttpServletResponse response, BucketMeta bucket) {
        String origin = request.getHeader(Consts.REQUEST_HEADER_ORIGIN);
        String method = request.getMethod();
        if (bucket != null && bucket.cors != null && bucket.cors.getRules() != null && bucket.cors.getRules().size() > 0) {
            List<CORSRule> rules = bucket.cors.getRules();
            boolean isAllowed = false;
            int ruleIndex = -1;
            for (int i = 0; i < rules.size(); i++) {
                if (!isOriginAllowed(origin, rules.get(i))) {
                    continue;
                }
                if (!rules.get(i).getAllowedMethods().contains(AllowedMethods.fromValue(method))) {
                    continue;
                }
                isAllowed = true;
                ruleIndex = i;
            }
            if (isAllowed) {
                CORSRule rule = rules.get(ruleIndex);
                if (anyOriginAllowed(rule)) {
                    response.addHeader(Consts.RESPONSE_HEADER_ACCESS_CONTROL_ALLOW_ORIGIN, "*");
                } else {
                    response.addHeader(Consts.RESPONSE_HEADER_ACCESS_CONTROL_ALLOW_ORIGIN, origin);
                }
                List<AllowedMethods> methods = rule.getAllowedMethods();
                String[] array = new String[methods.size()];
                for (int i = 0; i < methods.size(); i++) {
                    array[i] = methods.get(i).toString();
                }
                String responseAllowMethods = StringUtils.join(array, ",");
                response.addHeader(Consts.RESPONSE_HEADER_ACCESS_CONTROL_ALLOW_METHODS, responseAllowMethods);
                if (rule.getMaxAgeSeconds() > 0) {
                    response.addHeader(Consts.RESPONSE_HEADER_ACCESS_CONTROL_MAX_AGE, String.valueOf(rule.getMaxAgeSeconds()));
                }
                List<String> exposeHeaders = rule.getExposedHeaders();
                if ((exposeHeaders != null) && (!exposeHeaders.isEmpty())) {
                    String[] exposeHeadersArr = exposeHeaders.toArray(new String[exposeHeaders.size()]);
                    String responseExposeHeaders = StringUtils.join(exposeHeadersArr, ",");
                    response.addHeader(Consts.RESPONSE_HEADER_ACCESS_CONTROL_EXPOSE_HEADERS, responseExposeHeaders);
                }
            }
            // 请求没有通过授权验证，响应头不返回跨域相关头，继续往下执行Object相关操作。
        }
    }
    
    /**
     * 处理预检请求，请求若没有通过授权验证，返回403
     * @param bucket
     * @param request
     * @param response
     * @throws BaseException
     */
    public static void handlePreflightCORS(BucketMeta bucket, HttpServletRequest request,
            HttpServletResponse response) throws BaseException {
        String origin = request.getHeader(Consts.REQUEST_HEADER_ORIGIN);
        String method = request.getHeader(Consts.REQUEST_HEADER_ACCESS_CONTROL_REQUEST_METHOD);
        if (method == null || (!HTTP_METHODS.contains(method.trim()))) {
            throw new BaseException(403, "Forbidden");
        } else {
            method = method.trim();
        }
        String accessControlRequestHeadersHeader = request.getHeader(Consts.REQUEST_HEADER_ACCESS_CONTROL_REQUEST_HEADERS);
        List<String> accessControlRequestHeaders = new LinkedList<String>();
        if (accessControlRequestHeadersHeader != null && !accessControlRequestHeadersHeader.trim().isEmpty()) {
            String[] headers = accessControlRequestHeadersHeader.trim().split(",");
            for (String header : headers) {
                accessControlRequestHeaders.add(header.trim().toLowerCase());
            }
        }
        if (bucket != null && bucket.cors != null && bucket.cors.getRules() != null && bucket.cors.getRules().size() > 0) {
            List<CORSRule> rules = bucket.cors.getRules();
            // 是否能够找到一条匹配的CORSRule
            boolean isAllowed = false;
            // 匹配的CORSRule index
            int ruleIndex = -1;
            // 依次检查是否匹配，直到找到一条匹配的CORSRule跳出循环。
            for (int i = 0; i < rules.size(); i++) {
                if (!isOriginAllowed(origin, rules.get(i))) {
                    continue;
                }
                if (!rules.get(i).getAllowedMethods().contains(AllowedMethods.fromValue(method))) {
                    continue;
                }
                if (!accessControlRequestHeaders.isEmpty()) {
                    boolean notAllowed = false;
                    for (String header : accessControlRequestHeaders) {
                        if (!isHeaderAllowed(header, rules.get(i))) {
                            notAllowed = true;
                            break;
                        }
                    }
                    if (notAllowed) {
                        continue;
                    }
                }
                // 找到匹配的CORSRule，跳出循环
                isAllowed = true;
                ruleIndex = i;
                break;
            }
            if (isAllowed) {
                // 根据匹配的CORSRule，装配跨域响应头
                CORSRule rule = rules.get(ruleIndex);
                // 若跨域配置AllowedOrigin为"*"，则响应头"Access-Control-Allow-Origin"为"*",否则响应头为请求origin值。
                if (anyOriginAllowed(rule)) {
                    response.addHeader( Consts.RESPONSE_HEADER_ACCESS_CONTROL_ALLOW_ORIGIN, "*");
                } else {
                    response.addHeader(Consts.RESPONSE_HEADER_ACCESS_CONTROL_ALLOW_ORIGIN, origin);
                }
                // 响应头"Access-Control-Allow-Methods"为匹配的CORSRule允许跨域的所有HTTP方法
                List<AllowedMethods> methods = rule.getAllowedMethods();
                String[] array = new String[methods.size()];
                for (int i = 0; i < methods.size(); i++) {
                    array[i] = methods.get(i).toString();
                }
                String responseAllowMethods = StringUtils.join(array, ",");
                response.addHeader(Consts.RESPONSE_HEADER_ACCESS_CONTROL_ALLOW_METHODS, responseAllowMethods);
                // 跨域配置MaxAgeSeconds>0，则装配该响应头"Access-Control-Max-Age"
                if (rule.getMaxAgeSeconds() > 0) {
                    response.addHeader(Consts.RESPONSE_HEADER_ACCESS_CONTROL_MAX_AGE,String.valueOf(rule.getMaxAgeSeconds()));
                }
                // 若请求头中有Access-Control-Request-Headers，返回响应头显示匹配后的小写形式，若跨域配置没有AllowedHeader，返回403
                // 若请求头中没有Access-Control-Request-Headers，响应头不显示该项
                if (accessControlRequestHeaders != null && accessControlRequestHeaders.size() > 0) {
                    List<String> allowedHeaders = rule.getAllowedHeaders();
                    if ((allowedHeaders != null) && (!allowedHeaders.isEmpty())) {
                        String[] allowedHeadersArr = accessControlRequestHeaders.toArray(new String[accessControlRequestHeaders.size()]);                       
                        String responseAllowHeaders = StringUtils.join(allowedHeadersArr, ",");
                        response.addHeader(Consts.RESPONSE_HEADER_ACCESS_CONTROL_ALLOW_HEADERS, responseAllowHeaders);
                    } else {
                        throw new BaseException(403, "Forbidden");
                    }
                }
                // 若跨域配置有ExposeHeader，响应头"Access-Control-Expose-Headers"为匹配的CORSRule允许跨域的所有ExposeHeader
                List<String> exposeHeaders = rule.getExposedHeaders();
                if ((exposeHeaders != null) && (!exposeHeaders.isEmpty())) {
                    String[] exposeHeadersArr = exposeHeaders.toArray(new String[exposeHeaders.size()]);
                    String responseExposeHeaders = StringUtils.join(exposeHeadersArr, ",");
                    response.addHeader(Consts.RESPONSE_HEADER_ACCESS_CONTROL_EXPOSE_HEADERS, responseExposeHeaders);
                }
            } else {
             // 预检请求与CORSRule规则依次匹配检查，均失败，即预检请求没有通过授权验证，返回403
                throw new BaseException(403, "Forbidden");
            }
        } else {
            // CORSRule规则为空，返回403
            throw new BaseException(403, "Forbidden");
        }
    }
    
    /**
     * CORSRule origin是否包含*，即允许所有特定源发出的请求
     * @param rule
     * @return
     */
    private static boolean anyOriginAllowed(CORSRule rule) {
        List<String> allowedOrigins = rule.getAllowedOrigins();
        for (String allowedOrigin:allowedOrigins) {
            if (allowedOrigin.equals("*")) {
                return true;
            }
        }
        return false;
    }
    
    /**
     * 检查请求origin是否允许
     * @param origin
     * @param rule
     * @return
     */
    private static boolean isOriginAllowed(String origin,CORSRule rule) {
        List<String> allowedOrigins = rule.getAllowedOrigins();
        for (String allowedOrigin:allowedOrigins) {
            if (allowedOrigin.equals("*")) {
                return true;
            } else if (!allowedOrigin.contains("*")){
                if (origin.equals(allowedOrigin)) {
                    return true;
                }
            } else {
                boolean match = FilenameUtils.wildcardMatch(origin, allowedOrigin, IOCase.SENSITIVE);
                if (match) {
                    return true;
                }
            }
        }
        return false;
    }
    
    private static boolean isHeaderAllowed(String header,CORSRule rule) {
        List<String> allowedHeaders = rule.getAllowedHeaders();
        if (allowedHeaders == null || allowedHeaders.size() == 0)
            return false;
        for (String allowedHeader : allowedHeaders) {
            if (allowedHeader.equals("*")) {
                return true;
            } else {
                boolean match = FilenameUtils.wildcardMatch(header, allowedHeader, IOCase.INSENSITIVE);
                if (match) {
                    return true;
                }
            }
        }
        return false;
    }
    
    /**
     * 判断请求的类型
     * @param request
     * @return
     */
    public static CORSRequestType checkRequestType(final HttpServletRequest request) {
        CORSRequestType requestType = CORSRequestType.INVALID_CORS;
        String originHeader = request.getHeader(Consts.REQUEST_HEADER_ORIGIN);
        if (originHeader != null) {
            if (originHeader.isEmpty()) {
                requestType = CORSRequestType.INVALID_CORS;
            } else if (!isValidOrigin(originHeader)) {
                requestType = CORSRequestType.INVALID_CORS;
            } else {
                String method = request.getMethod();
                if (method != null && HTTP_METHODS.contains(method)) {
                    if ("OPTIONS".equals(method)) {
                        String accessControlRequestMethodHeader =
                                request.getHeader(Consts.REQUEST_HEADER_ACCESS_CONTROL_REQUEST_METHOD);
                        if (accessControlRequestMethodHeader != null && !accessControlRequestMethodHeader.isEmpty()) {
                            requestType = CORSRequestType.PRE_FLIGHT;
                        } else if (accessControlRequestMethodHeader != null && accessControlRequestMethodHeader.isEmpty()) {
                            requestType = CORSRequestType.INVALID_CORS;
                        } else {
                            requestType = CORSRequestType.ACTUAL;
                        }
                    } else if ("GET".equals(method) || "HEAD".equals(method)) {
                        requestType = CORSRequestType.SIMPLE;
                    } else if ("POST".equals(method)) {
                        String contentType = request.getContentType();
                        if (contentType != null) {
                            contentType = contentType.toLowerCase().trim();
                            if (SIMPLE_HTTP_REQUEST_CONTENT_TYPE_VALUES.contains(contentType)) {
                                requestType = CORSRequestType.SIMPLE;
                            } else {
                                requestType = CORSRequestType.ACTUAL;
                            }
                        }
                    } else if (COMPLEX_HTTP_METHODS.contains(method)) {
                        requestType = CORSRequestType.ACTUAL;
                    }
                }
            }
        } else {
            requestType = CORSRequestType.NOT_CORS;
        }
        return requestType;
    }
    
    public static final Collection<String> SIMPLE_HTTP_REQUEST_CONTENT_TYPE_VALUES =
            new HashSet<String>(Arrays.asList("application/x-www-form-urlencoded", "multipart/form-data", "text/plain"));
    
    public static final Collection<String> COMPLEX_HTTP_METHODS =
            new HashSet<String>(Arrays.asList("PUT", "DELETE", "TRACE", "CONNECT"));
       
    public static enum CORSRequestType {
        /**
         * A simple HTTP request, i.e. it shouldn't be pre-flighted.
         */
        SIMPLE,
        /**
         * A HTTP request that needs to be pre-flighted.
         */
        ACTUAL,
        /**
         * A pre-flight CORS request, to get meta information, before a
         * non-simple HTTP request is sent.
         */
        PRE_FLIGHT,
        /**
         * Not a CORS request, but a normal request.
         */
        NOT_CORS,
        /**
         * An invalid CORS request, i.e. it qualifies to be a CORS request, but
         * fails to be a valid one.
         */
        INVALID_CORS
    }
    
    public static final Collection<String> HTTP_METHODS = new HashSet<String>(
            Arrays.asList("OPTIONS", "GET", "HEAD", "POST", "PUT", "DELETE", "TRACE", "CONNECT"));
    
    public static boolean isValidOrigin(String origin) {
        // Checks for encoded characters. Helps prevent CRLF injection.
        if (origin.contains("%")) {
            return false;
        }
        // "null" is a valid origin
        if ("null".equals(origin)) {
            return true;
        }
        // RFC6454, section 4. "If uri-scheme is file, the implementation MAY
        // return an implementation-defined value.". No limits are placed on
        // that value so treat all file URIs as valid origins.
        if (origin.startsWith("file://")) {
            return true;
        }
        URI originURI;
        try {
            originURI = new URI(origin);
        } catch (URISyntaxException e) {
            return false;
        }
        // If scheme for URI is null, return false. Return true otherwise.
        return originURI.getScheme() != null;
    }
    
    /**
     * 获取bucket多版本配置
     * @param bucket
     * @return
     * @throws IOException
     * @throws BaseException
     */
    public static String getBucketVersioning(BucketMeta bucket) throws IOException, BaseException {
        XmlWriter xml = new XmlWriter();
        xml.start("VersioningConfiguration", "xmlns", Consts.XMLNS);
        if (bucket.versioning != null && bucket.versioning.getStatus() != null) {
            String status = bucket.versioning.getStatus();
            xml.start("Status").value(status).end();
        }
        xml.end();
        String str = Consts.XML_HEADER + xml.toString();
        return str;
    }
    
    /**
     * 配置bucket多版本
     * @param dbBucket
     * @param ip
     * @param length
     * @throws BaseException
     * @throws IOException
     */
    public static void putBucketVersioning(BucketMeta dbBucket, InputStream ip, int length) throws BaseException, IOException {
        BucketVersioningConfiguration versioningConf = new XmlResponsesSaxParser().parseVersioningConfigurationResponse(ip).getConfiguration();
        if (versioningConf.getStatus() == null || versioningConf.getStatus().trim().length() == 0)
            throw new BaseException(400, ErrorMessage.ERROR_CODE_ILLEGAL_VERSIONING_CONFIGURATION, ErrorMessage.ERROR_MESSAGE_ILLEGAL_VERSIONING_CONFIGURATION);
        if (!versioningConf.getStatus().equals("Enabled") && !versioningConf.getStatus().equals("Suspended"))
            throw new BaseException(400, ErrorMessage.ERROR_CODE_MALFORMEDXML, ErrorMessage.ERROR_MESSAGE_MALFORMEDXML);
        dbBucket.versioning = versioningConf;
        client.bucketUpdate(dbBucket);
    }
    
    /**
     * 根据请求的query参数，返回bucket相应的属性
     * @param
     * @param dbBucket
     * @param req
     * @return
     * @throws BaseException
     * @throws IOException
     */
    public static String getBucketSubresource(AuthResult authResult, BucketMeta dbBucket, HttpServletRequest req, HttpServletResponse resp) throws BaseException, IOException {
        String value = req.getParameter("subresource");
        if (value == null || value.trim().isEmpty()) {
            throw new BaseException(400, "Invalid Argument", "Invalid Argument");
        }
        String[] valueArr = value.split(",");
        List<String> list = Arrays.asList(valueArr);
        boolean hasACL = false;
        XmlWriter xml = new XmlWriter();
        xml.start("BucketSubresource", "xmlns", Consts.XMLNS);
        if (list.contains("acl") && canAccess(authResult, OOSActions.Bucket_Get_Acl, dbBucket, req, "ACL", xml)) {
            hasACL = true;
            xml.start("ACL");
            xml.start("AccessControlPolicy");
            xml.start("Owner");
            OwnerMeta owner = authResult.owner;
            xml.start("ID").value(owner.getName()).end();
            if (owner.displayName != null && owner.displayName.trim().length() != 0)
                xml.start("DisplayName").value(owner.displayName).end();
            else
                xml.start("DisplayName").value("").end();
            xml.end();
            xml.start("AccessControlList");
            xml.start("Grant");
            String[] attr2 = { "xsi:type", "xmlns:xsi" };
            String[] value2 = { "Group", "http://www.w3.org/2001/XMLSchema-instance" };
            xml.start("Grantee", attr2, value2);
            xml.start("URI").value(GroupGrantee.AllUsers.getIdentifier()).end();
            xml.end();
            switch (dbBucket.permission) {
            case BucketMeta.PERM_PRIVATE:
                xml.start("Permission").value("").end();
                break;
            case BucketMeta.PERM_PUBLIC_READ:
                xml.start("Permission").value(Permission.Read.toString()).end();
                break;
            case BucketMeta.PERM_PUBLIC_READ_WRITE:
                xml.start("Permission").value(Permission.FullControl.toString()).end();
                break;
            }
            xml.end();
            xml.end();
            xml.end();
            xml.end();
        }
        if (list.contains("location") && canAccess(authResult, OOSActions.Bucket_Get_Location, dbBucket, req, "Location", xml)) {
            Pair<Set<String>, Set<String>> res = client.getShowRegions(dbBucket.getOwnerId());
            xml.start("Location");
            xml.start("BucketConfiguration");
            if (res.first().size() > 0) {
                xml.start("MetadataLocationConstraint");
                xml.start("Location").value(dbBucket.metaLocation).end();
                xml.end();
            }
            if (res.second().size() > 0) {
                xml.start("DataLocationConstraint");
                xml.start("Type").value(dbBucket.dataLocation.getType().name()).end();
                if (dbBucket.dataLocation.getDataRegions() != null) {
                    xml.start("LocationList");
                    for (String r : dbBucket.dataLocation.getDataRegions())
                        xml.start("Location").value(r).end();
                    xml.end();
                }
                xml.start("ScheduleStrategy").value(dbBucket.dataLocation.getStragegy().name()).end();
                xml.end();
            }
            xml.end();
            xml.end();
        }
        if (list.contains("policy") && canAccess(authResult, OOSActions.Bucket_Get_Policy, dbBucket, req, "Policy", xml)) {
            xml.start("Policy");
            if (dbBucket.policy != null && dbBucket.policy.trim().length() != 0) {
                xml.start("PolicyConfiguration").value(dbBucket.policy).end();
            }
            xml.end();
        }
        if (list.contains("website") && canAccess(authResult, OOSActions.Bucket_Get_WebSite, dbBucket, req, "Website", xml)) {
            xml.start("Website");
            xml.start("WebsiteConfiguration");
            xml.start("IndexDocument");
            if (dbBucket.indexDocument != null && dbBucket.indexDocument.trim().length() != 0)
                xml.start("Suffix").value(dbBucket.indexDocument).end();
            else
                xml.start("Suffix").value("").end();
            xml.end();
            xml.start("ErrorDocument");
            if (dbBucket.errorDocument != null && dbBucket.errorDocument.trim().length() != 0)
                xml.start("Key").value(dbBucket.errorDocument).end();
            else
                xml.start("Key").value("").end();
            xml.end();
            xml.end();
            xml.end();
        }
        if (list.contains("logging") && canAccess(authResult, OOSActions.Bucket_Get_logging, dbBucket, req, "Logging", xml)) {
            BucketMeta targetBucket = new BucketMeta(dbBucket.logTargetBucket);
            boolean ref = client.bucketSelect(targetBucket);
            xml.start("Logging");
            xml.start("BucketLoggingStatus");
            boolean res = false;
            if (dbBucket.logTargetBucket != 0 && dbBucket.logTargetPrefix != null) {
                if (!ref) {
                    dbBucket.logTargetBucket = 0;
                    dbBucket.logTargetPrefix = "";
                    client.bucketUpdate(dbBucket);
                } else {
                    xml.start("LoggingEnabled");
                    res = true;
                    xml.start("TargetBucket").value(targetBucket.getName()).end();
                    xml.start("TargetPrefix").value(dbBucket.logTargetPrefix).end();
                }
            }
            if (res)
                xml.end();
            xml.end();
            xml.end();
        }
        if (list.contains("accelerate") && canAccess(authResult, OOSActions.Bucket_Get_Accelerate, dbBucket, req, "Accelerate", xml)) {
            AccelerateMeta acceMeta = client.accelerateSelect(dbBucket.name);
            xml.start("Accelerate");
            xml.start("AccelerateConfiguration");
            if (acceMeta == null || acceMeta.status == null || acceMeta.status.length() == 0){
                xml.start("Status").value("Suspended").end();
            } else {
                xml.start("Status").value(acceMeta.status).end();
            }
            if (acceMeta != null && acceMeta.ipWhiteList != null && acceMeta.ipWhiteList.size()>0){
                xml.start("IPWhiteLists");
                for(String ipStr : acceMeta.ipWhiteList) {
                    xml.start("IP").value(ipStr).end();
                }
                xml.end();
            }
            xml.end();
            xml.end();
        }
        if (list.contains("lifecycle") && canAccess(authResult, OOSActions.Bucket_Get_Lifecycle, dbBucket, req, "Lifecycle", xml)) {
            xml.start("Lifecycle");
            xml.start("LifecycleConfiguration");
            if (dbBucket.lifecycle != null) {
                for (Rule rule : dbBucket.lifecycle.getRules()) {
                    xml.start("Rule");
                    xml.start("ID").value(rule.getId()).end();
                    xml.start("Prefix").value(rule.getPrefix()).end();
                    xml.start("Status").value(rule.getStatus()).end();
                    if(null != rule.getExpirationDate() ||
                            rule.getExpirationInDays() != BucketLifecycleConfiguration.INITIAL_DAYS) {
                        xml.start("Expiration");
                        if (rule.getExpirationDate() == null)
                            xml.start("Days").value(String.valueOf(rule.getExpirationInDays())).end();
                        else
                            xml.start("Date").value(ServiceUtils
                                    .formatIso8601Date(rule.getExpirationDate())).end();
                        //Expiration end
                        xml.end();
                    }
                    if(null != rule.getTransition()) {
                        xml.start("Transition");
                        if(rule.getTransition().getDate() == null) {
                            xml.start("Days").value(String.valueOf(rule.getTransition().getDays())).end();
                        } else {
                            xml.start("Date").value(ServiceUtils
                                    .formatIso8601Date(rule.getTransition().getDate())).end();
                        }
                        xml.start("StorageClass").value(rule.getTransition().getStorageClass()).end();
                        //Transition end
                        xml.end();
                    }
                    xml.end();
                }
            }
            xml.end();
            xml.end();
        }
        if (list.contains("cors") && canAccess(authResult, OOSActions.Bucket_Get_Cors, dbBucket, req, "CORS", xml)) {
            xml.start("CORS");
            xml.start("CORSConfiguration");
            if (dbBucket.cors != null && dbBucket.cors.getRules() != null && !dbBucket.cors.getRules().isEmpty()) {
                for (CORSRule rule : dbBucket.cors.getRules()) {
                    xml.start("CORSRule");
                    if (rule.getId() != null && !rule.getId().isEmpty()) {
                        xml.start("ID").value(rule.getId()).end();
                    }
                    List<String> allowedOrigins = rule.getAllowedOrigins();
                    if (allowedOrigins !=null && !allowedOrigins.isEmpty()) {
                        for (String allowedOrigin:allowedOrigins) {
                            xml.start("AllowedOrigin").value(allowedOrigin).end();
                        }
                    }
                    List<AllowedMethods> allowedMethods = rule.getAllowedMethods();
                    if (allowedMethods !=null && !allowedMethods.isEmpty()) {
                        for (AllowedMethods allowedMethod:allowedMethods) {
                            xml.start("AllowedMethod").value(allowedMethod.toString()).end();
                        }
                    }
                    if (rule.getMaxAgeSeconds() != 0) {
                        xml.start("MaxAgeSeconds").value(String.valueOf(rule.getMaxAgeSeconds())).end();
                    }
                    List<String> allowedHeaders = rule.getAllowedHeaders();
                    if (allowedHeaders !=null && !allowedHeaders.isEmpty()) {
                        for (String allowedHeader:allowedHeaders) {
                            xml.start("AllowedHeader").value(allowedHeader).end();
                        }
                    }
                    List<String> exposeHeaders = rule.getExposedHeaders();
                    if (exposeHeaders !=null && !exposeHeaders.isEmpty()) {
                        for (String exposeHeader:exposeHeaders) {
                            xml.start("ExposeHeader").value(exposeHeader).end();
                        }
                    }
                    xml.end();
                }
            }
            xml.end();
            xml.end();
        }
        if (list.contains("object-lock") && canAccess(authResult, OOSActions.Bucket_Get_ObjectLockConfiguration, dbBucket, req, "ObjectLock", xml)) {
            xml.start("ObjectLock");
            if (dbBucket.objectLockConfiguration != null) {
                dbBucket.objectLockConfiguration.appendToXml(xml);
            }
            xml.end();
        }

        xml.end();
        if (log.isDebugEnabled())
            log.debug(xml.toString());
        String str = Consts.XML_HEADER + xml.toString();
        str = str.replaceAll("&quote;", "&quot;");
        if (hasACL) {
            resp.setHeader(Headers.LAST_MODIFIED, TimeUtils.toGMTFormat(new Date(dbBucket.createDate)));
        }
        return str;
    }
    
    public static void deleteBucket(BucketMeta bucket) throws BaseException, IOException{
        if (!client.bucketIsEmpty(bucket.metaLocation, bucket.getName())) {
            throw new BaseException(409, "BucketNotEmpty");
        }
        for(int i = 0 ; i<3; i++) {
            try {
                client.bucketDelete(bucket);   
                BssAdapterClient.delete(bucket.name,
                        BssAdapterConfig.localPool.ak,
                        BssAdapterConfig.localPool.getSK());
                break;
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }
    }
    
    /**
     * 判断当前请求是否可以通过用户的访问控制
     * 如果不能访问，将访问错误信息写入xml中
     * @param authResult
     * @param oosAction
     * @param dbBucket
     * @param req
     * @param tagName
     * @param xml
     * @return
     * @throws IOException 
     * @throws BaseException 
     */
    private static boolean canAccess(AuthResult authResult, OOSActions oosAction, BucketMeta dbBucket, HttpServletRequest req, String tagName, XmlWriter xml) throws IOException {
        // 根用户可以访问
        if (authResult.isRoot()) {
            return true;
        }
        // 子用户进行访问控制
        try {
            AccessControlUtils.auth(req, oosAction, dbBucket, authResult);
        } catch (BaseException e) {
            xml.start(tagName);
            xml.start("Error");
            xml.start("Status").value(String.valueOf(e.status)).end();
            xml.start("Code").value(e.code).end();
            xml.start("Message").value(e.message).end();
            xml.end();
            xml.end();
            // 没有权限返回false
            return false;
        }
        return true;
    }
    
    /**
     * 检查lifecycle配置里的date和days是否符合规则。
     * */
    private static void checkLifecycleDateAndDays(Date expirationDate,
            int expirationInDays ,String type) throws BaseException {
        // 如果日期精度的过期时间为空，则天精度的过期时间不能是-1 。
        if (expirationDate == null) {
            if (expirationInDays <= 0)
                throw new BaseException("expirationInDays " + expirationInDays,400,
                        ErrorMessage.ERROR_CODE_INVALID_ARGUMENT,
                        "'Days' for " + type + " action must be a positive integer");
        } else {
            // 如果日期精度的时间不为空，天精度的时间必须为-1，从客户的行为来说，这两只能且必须存在一个
            if (expirationInDays != BucketLifecycleConfiguration.INITIAL_DAYS)
                throw new BaseException(400,
                        ErrorMessage.ERROR_CODE_INVALID_ARGUMENT,
                        "Only one action can be specified in the rule");
            Date d = expirationDate;
            Calendar calendar = Calendar
                    .getInstance(TimeZone.getTimeZone("GMT"), Locale.ENGLISH);
            calendar.setTime(d);
            if (calendar.get(Calendar.HOUR_OF_DAY) != 0
                    || calendar.get(Calendar.MINUTE) != 0
                    || calendar.get(Calendar.SECOND) != 0
                    || calendar.get(Calendar.MILLISECOND) != 0)
                throw new BaseException("expirationDate " + expirationDate.toString(), 400,
                        ErrorMessage.ERROR_CODE_INVALID_ARGUMENT,
                        "'Date' must be at midnight GMT");
        }
    }
    
    /**
     * 本方法做了两步校验: <br> 1 当prefix重叠时， 过期时间不能小于转储时间。<br>  2 过期时间和转储时间配置类型必须一致。 
     * */
    private static void checkLifecycleDaysAndDate(List<Pair<String, Object>> expirationList, 
            List<Pair<String, Object>> transitionList, BucketLifecycleConfiguration lifeConf) throws BaseException{
        for(Pair<String, Object> expirationPair : expirationList) {
            for(Pair<String, Object> transitionPair : transitionList) {
                //当prefix有重叠时。
                if(expirationPair.first().startsWith(transitionPair.first()) 
                        || transitionPair.first().startsWith(expirationPair.first())) {
                    //转储时间不能大于过期时间。day和day比较，date和date比较  
                    if(expirationPair.second() instanceof Integer && transitionPair.second() instanceof Integer) {
                        if((Integer) transitionPair.second() >= (Integer) expirationPair.second())
                            throw new BaseException(lifeConf.toString(), 400, ErrorMessage.ERROR_CODE_INVALID_ARGUMENT,
                                    "'Days' in the Expiration action for prefix '" + expirationPair.first() + "' must be greater than 'Days' in the Transition action" + (
                                            expirationPair.first().equals(transitionPair.first()) ?
                                                    "" :
                                                    " for prefix '" + transitionPair.first() + "'"));
                    } else if(expirationPair.second() instanceof Date && transitionPair.second() instanceof Date) {
                        if(!((Date)expirationPair.second()).after((Date)transitionPair.second())){
                            throw new BaseException(lifeConf.toString(), 400, ErrorMessage.ERROR_CODE_INVALID_ARGUMENT,
                                    "'Date' in the Expiration action for prefix '" + expirationPair.first() + "' must be later than 'Date' in the Transition action" + (
                                            expirationPair.first().equals(transitionPair.first()) ?
                                                    "" :
                                                    " for prefix '" + transitionPair.first() + "'"));
                        }
                    }
                    //过期时间和转储时间必须使用同一个类型的过期方式。 即都是date或days
                    if((expirationPair.second() instanceof Integer && transitionPair.second() instanceof Date) || (
                            transitionPair.second() instanceof Integer && expirationPair.second() instanceof Date)) {
                        throw new BaseException(lifeConf.toString(), 400, ErrorMessage.ERROR_CODE_INVALID_ARGUMENT,
                                "Found mixed 'Date' and 'Days' based Expiration and Transition actions in lifecycle rule for prefixes "
                                + "'"+expirationPair.first()+"' and '"+transitionPair.first()+"'");
                    }
                }
            }
        }
    }
    
    /**
     * 检查lifecycle的prefix是否有重叠
     * */
    private static void checkLifecyclePrefix(List<Pair<String, Object>> list, String type,
            BucketLifecycleConfiguration lifeConf) throws BaseException{
        int noprefixNum = 0;
        for (int i = 0; i < list.size(); i++) {
            //如果有多个规则，其中一个没有prefix则报错。
            if(list.get(i).first().equals(BucketLifecycleConfiguration.ROOT_PREFIX))
                noprefixNum++;
            for (int j = 0; j < list.size(); j++) {
                if (list.get(i).first().equals(list.get(j).first()) && i != j)
                    throw new BaseException(lifeConf.toString(), 400, ErrorMessage.ERROR_CODE_INVALID_REQUEST,
                            "Found two rules with same prefix '" + list.get(i).first() + "' for same action type '"+type+"'");
                if (list.get(i).first().startsWith(list.get(j).first()) && i != j)
                    throw new BaseException(lifeConf.toString(), 400, ErrorMessage.ERROR_CODE_INVALID_REQUEST,
                            "Found overlapping prefixes '" + list.get(i).first() + "' and '"
                                    + list.get(j).first() + "' for same action type '"+type+"'");
            }
        }
        //如果所有规则都没有配置prefix 。 则不报错。
        if(noprefixNum != 0 && noprefixNum != list.size())
            throw new BaseException(lifeConf.toString(), 400, ErrorMessage.ERROR_CODE_INVALID_REQUEST,
                    "Found overlapping prefixes '" + list.stream().filter(
                            e -> !e.first().equals(BucketLifecycleConfiguration.ROOT_PREFIX)).findFirst().get().first() 
                                + "' and '' for same action type '"+type+"'");
    }

    /**
     * 添加修改合规保留策略
     *
     * @param dbBucket
     * @param ip                  请求体的inputstream
     * @param req
     * @param ifRecordManageEvent 是否记录管理事件
     * @throws BaseException
     * @throws IOException
     */
    public static void putObjectLockConfiguration(BucketMeta dbBucket, InputStream ip, HttpServletRequest req, PinData pinData, boolean ifRecordManageEvent) throws BaseException, IOException, org.json.JSONException {
        // 必须参数校验
        String contentMd5 = req.getHeader(Headers.CONTENT_MD5);
        if (contentMd5 == null || contentMd5.trim().length() == 0) {
            throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_REQUEST,
                    ErrorMessage.ERROR_MESSAGE_MISSING_MD5);
        }
        if (req.getHeader(Headers.CONTENT_LENGTH) == null) {
            throw new BaseException(411, ErrorMessage.ERROR_CODE_MISSING_CONTENT_LENGTH, ErrorMessage.ERROR_MESSAGE_MISSING_CONTENT_LENGTH);
        }
        long contentLength = Long.parseLong(req.getHeader(Headers.CONTENT_LENGTH));

        EtagMaker etagmaker = new EtagMaker();
        SignableInputStream sin = new SignableInputStream(ip, contentLength, etagmaker);
        // xml转化为configuration对象
        ObjectLockConfiguration objectLockConfiguration = null;
        try {
            objectLockConfiguration = new XmlResponseSaxParser().parseObjectLockConfigurationResponse(sin).getObjectLockConfiguration();
        } catch (AmazonClientException e) {
            // 解析中如果出现解析days years为非数字 转换为详细的异常信息抛出
            if (e.getCause() instanceof NumberFormatException) {
                throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_ARGUMENT, ErrorMessage.ERROR_MESSAGE_INVALID_ARGUMENT_FOR_INVALID_VALUE_OF_DAY_OR_YEAR);
            }
            throw e;
        }finally {
            // 记录管理事件
            if (ifRecordManageEvent && Objects.nonNull(objectLockConfiguration)) {
                pinData.jettyAttributes.manageEventAttribute.reqContent = objectLockConfiguration.toManagerEventJson();
            }
        }

        V4Signer.checkContentSignatureV4(ip);
        Utils.checkMd5(contentMd5, etagmaker.digest());
        // 对配置进行参数校验
        ObjectLockUtils.validCheckObjectLockConfiguration(objectLockConfiguration);

        ObjectLockConfiguration oldConfiguration = dbBucket.objectLockConfiguration;
        if (oldConfiguration != null && ObjectLockConfiguration.ObjectLockStatus.ENABLED.toString().equals(oldConfiguration.objectLockEnabled)) {
            log.info("OpBucket.putObjectLockConfiguration bucket : " + Bytes.toString(dbBucket.getRow()) + " old conf : " + oldConfiguration + " new conf :" + objectLockConfiguration);
            // 已经启用的配置校验
            if (!ObjectLockConfiguration.ObjectLockStatus.ENABLED.toString().equals(objectLockConfiguration.objectLockEnabled)) {
                // 已经开启的规则不能关闭
                throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_ARGUMENT, ErrorMessage.ERROR_MESSAGE_CONF_HAS_BEEN_ENABLED);
            }
            if (objectLockConfiguration.isLockDayLessThan(oldConfiguration)) {
                // 新设置的保留时长 比 旧配置的短
                throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_ARGUMENT, ErrorMessage.ERROR_MESSAGE_INVALID_ARGUMENT_FOR_PERIOD_VALUE);
            }
        }
        dbBucket.objectLockConfiguration = objectLockConfiguration;
        client.bucketUpdate(dbBucket);
    }

    /**
     * 获取合规保留配置
     *
     * @param dbBucket
     * @return
     * @throws BaseException
     */
    public static String getObjectLockConfiguration(BucketMeta dbBucket) throws BaseException {
        ObjectLockConfiguration objectLockConf = dbBucket.objectLockConfiguration;
        if (objectLockConf == null) {
            //配置不存在
            throw new BaseException(404, ErrorMessage.ERROR_CODE_OBJECT_LOCK_CONFIGURATION_NOT_FOUND, ErrorMessage.ERROR_MESSAGE_OBJECT_LOCK_CONFIGURATION_NOT_FOUND);
        }
        XmlWriter xml = objectLockConf.toXmlObject();
        String str = Consts.XML_HEADER + xml.toString();
        str = str.replaceAll("&quote;", "&quot;");
        return str;
    }

    /**
     * 删除合规保留配置
     *
     * @param dbBucket
     * @throws IOException
     * @throws BaseException
     */
    public static void deleteObjectLockConfiguration(BucketMeta dbBucket) throws IOException,BaseException {
        if (dbBucket.objectLockConfiguration == null) {
            //配置不存在
            throw new BaseException(404, ErrorMessage.ERROR_CODE_OBJECT_LOCK_CONFIGURATION_NOT_FOUND, ErrorMessage.ERROR_MESSAGE_OBJECT_LOCK_CONFIGURATION_NOT_FOUND);
        }
        if (Objects.equals(ObjectLockConfiguration.ObjectLockStatus.ENABLED.toString(),
                dbBucket.objectLockConfiguration.objectLockEnabled)) {
            // 配置已经启用不能删除
            throw new BaseException(400, ErrorMessage.ERROR_CODE_CAN_NOT_DELETE_OBJECTLOCKCONFIGURATION, ErrorMessage.ERROR_MESSAGE_CAN_NOT_DELETE_OBJECTLOCKCONFIGURATION);
        }
        dbBucket.objectLockConfiguration = null;
        client.bucketUpdate(dbBucket);
    }
}