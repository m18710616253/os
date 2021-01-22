package cn.ctyun.oos.server.util;

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang3.StringUtils;

import cn.ctyun.common.BaseException;
import cn.ctyun.oos.common.AuthResult;
import cn.ctyun.oos.common.ErrorMessage;
import cn.ctyun.oos.common.OOSActions;
import cn.ctyun.oos.hbase.MetaClient;
import cn.ctyun.oos.iam.accesscontroller.AccessController;
import cn.ctyun.oos.iam.accesscontroller.AccessEffect;
import cn.ctyun.oos.iam.accesscontroller.RequestInfo;
import cn.ctyun.oos.iam.accesscontroller.util.IAMStringUtils;
import cn.ctyun.oos.metadata.AkSkMeta;
import cn.ctyun.oos.metadata.BucketMeta;
import cn.ctyun.oos.metadata.OwnerMeta;

/**
 * 基于身份的访问控制工具
 * @author wangduo
 *
 */
public class AccessControlUtils {
    
    /** 访问控制类 */
    private static AccessController accessController = new AccessController();
    
    private static MetaClient metaClient = MetaClient.getGlobalClient();
    
    /**
     * 获取访问控制使用的请求信息
     * @param req
     * @param dbBucket
     * @param key
     * @param authResult
     * @return
     */
    private static RequestInfo getRequestInfo(HttpServletRequest req, String actionName, BucketMeta dbBucket, String key, AuthResult authResult) {
        String accountId = authResult.owner.getAccountId();
        // 如果是涉及bucket的请求，使用bucket的ownerId作为accountId
        if (dbBucket != null && dbBucket.ownerId != 0) {
            accountId = IAMStringUtils.getAccountId(dbBucket.ownerId);
        }
        String resource = "arn:ctyun:oos::"+ accountId + ":";
        if (null == dbBucket && null == key) {
            resource += "*";
        } else {
            resource = StringUtils.isBlank(key) ?  resource + dbBucket.name : resource + dbBucket.name + "/"+ key;
        }
        String action = "oos:" + actionName;
        return new RequestInfo(action, resource, accountId, authResult.accessKey.userId, authResult.accessKey.userName, req);
    }
    
    /**
     * 基于身份的访问控制
     * @param req
     * @param dbBucket 存储桶
     * @param key 对象名
     * @param authResult 签名认证结果
     * @param bucketPolicyEffect 基于资源的访问控制的结果
     * @throws BaseException
     * @throws IOException
     */
    public static void auth(HttpServletRequest req, String action, BucketMeta dbBucket, String key, AuthResult authResult, AccessEffect bucketPolicyEffect) throws BaseException, IOException {
        
        // 匿名访问，没有经过基于资源的访问控制，不允许操作
        if ((authResult == null || authResult.owner == null) && bucketPolicyEffect == null) {
            throw new BaseException(403, ErrorMessage.ERROR_CODE_403, ErrorMessage.ERROR_MESSAGE_403);
        }
        
        // 基于资源的访问控制
        if (bucketPolicyEffect != null) {
            // 基于资源访问控制是deny的情况
            if (bucketPolicyEffect == AccessEffect.Deny) {
                throw new BaseException(403, ErrorMessage.ERROR_CODE_403, ErrorMessage.ERROR_MESSAGE_403);
            }
            // 来自匿名用户或其他账户的用户的访问，只需要进行基于资源的访问控制
            if (authResult == null || authResult.owner == null || authResult.owner.getId() != dbBucket.getOwnerId()) {
                // 隐式拒绝
                if (bucketPolicyEffect == AccessEffect.ImplicitDeny) {
                    throw new BaseException(403, ErrorMessage.ERROR_CODE_403, ErrorMessage.ERROR_MESSAGE_403);
                } else if (bucketPolicyEffect == AccessEffect.Allow) {
                    // 允许操作
                    return;
                }
            }
        }

        // 如果是根用户不做基于身份的访问控制
        if (authResult.isRoot()) {
            return;
        }
        // 获取基于身份的访问控制的请求信息
        RequestInfo requestInfo = getRequestInfo(req, action, dbBucket, key, authResult);
        // 基于身份的访问控制
        AccessEffect identityEffect = accessController.allow(requestInfo);
        if (identityEffect == AccessEffect.Deny) {
            throw new BaseException(403, ErrorMessage.ERROR_CODE_403, requestInfo.getErrorMessage());
        }
        // 基于身份的访问控制或基于资源的访问控制是允许，允许操作
        if (bucketPolicyEffect == AccessEffect.Allow  || identityEffect == AccessEffect.Allow) {
            return;
        }
        // 隐式拒绝
        throw new BaseException(403, ErrorMessage.ERROR_CODE_403, requestInfo.getErrorMessage());
    }
    
    /**
     * 基于身份的访问控制
     */
    public static void auth(HttpServletRequest req, OOSActions action, BucketMeta dbBucket, AuthResult authResult) throws BaseException, IOException {
        auth(req, action.actionName, dbBucket, null, authResult, null);
    }
    
    /**
     * 基于身份的访问控制
     */
    public static void auth(HttpServletRequest req, OOSActions action, AuthResult authResult) throws BaseException, IOException {
        auth(req, action.actionName, null, null, authResult, null);
    }
    
    public static void auth(HttpServletRequest req, OOSActions action, BucketMeta dbBucket, String key, AuthResult authResult, AccessEffect bucketPolicyEffect) throws BaseException, IOException {
        auth(req, action.actionName, dbBucket, key, authResult, bucketPolicyEffect);
    }
    
    /**
     * 判断object请求的权限（包括bucket acl、bucket policy和子用户权限）
     * 无权限会抛出403的BaseException
     * @param req
     * @param bucket 
     * @param objectName
     * @param action
     * @param httpMethod
     * @param accessKeyId
     * @throws Exception
     */
    public static void checkObjectAllow(HttpServletRequest req, BucketMeta bucket, String objectName, OOSActions action, 
            String httpMethod, String accessKeyId) throws Exception {
        // 获取子用户的AK
        AkSkMeta accessKey = new AkSkMeta(accessKeyId);
        metaClient.akskSelect(accessKey);
        OwnerMeta owner = new OwnerMeta(accessKey.ownerId);
        metaClient.ownerSelectById(owner);
        // 获取owner信息
        AuthResult authResult = new AuthResult();
        authResult.accessKey = accessKey;
        authResult.owner = owner;
        
        // bucket policy权限校验
        AccessEffect accessEffect = ResourceAccessControlUtils.checkObjectACLsAndPolicy(req, bucket, objectName, 
                authResult, action.actionName, httpMethod);
        // iam 子用户权限校验
        AccessControlUtils.auth(req, action, bucket, objectName, authResult, accessEffect);
    }

    /**
     * 匹配基于身份的策略
     * @param req
     * @param action
     * @param dbBucket
     * @param key
     * @param authResult
     * @return
     * @throws BaseException
     * @throws IOException
     */
    public static AccessEffect checkIamPolicy(HttpServletRequest req, String action, BucketMeta dbBucket, String key, AuthResult authResult) throws BaseException, IOException {
        // 匿名访问，没有经过基于资源的访问控制，不允许操作
        if ((authResult == null || authResult.owner == null)) {
            return AccessEffect.Deny;
        }
        // 如果是根用户不做基于身份的访问控制
        if (authResult.isRoot()) {
            return AccessEffect.Allow;
        }
        // 获取基于身份的访问控制的请求信息
        RequestInfo requestInfo = getRequestInfo(req, action, dbBucket, key, authResult);
        // 基于身份的访问控制
        return accessController.allow(requestInfo);
    }
    
}
