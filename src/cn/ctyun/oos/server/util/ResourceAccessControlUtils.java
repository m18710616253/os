package cn.ctyun.oos.server.util;

import javax.servlet.http.HttpServletRequest;

import com.amazonaws.HttpMethod;
import com.amazonaws.util.json.JSONException;

import cn.ctyun.common.BaseException;
import cn.ctyun.oos.common.AuthResult;
import cn.ctyun.oos.common.ErrorMessage;
import cn.ctyun.oos.iam.accesscontroller.AccessEffect;
import cn.ctyun.oos.metadata.BucketMeta;
import cn.ctyun.oos.server.OpBucket;

/**
 * 基于资源的访问控制工具
 */
public class ResourceAccessControlUtils {
    
    /**
     * 检查bucket的ACL和Policy权限，若没有权限则拒绝bucket访问
     * @param req
     * @param dbBucket
     * @param owner
     * @param action
     * @throws JSONException
     * @throws BaseException
     */
    public static AccessEffect checkBucketACLsAndPolicy(HttpServletRequest req, BucketMeta dbBucket, 
            AuthResult authResult, String action) throws JSONException, BaseException {
        // 如果是根用户且账户拥有bucket，返回允许
        if (authResult != null && authResult.isRoot() && authResult.owner.getId() == dbBucket.getOwnerId()) {
            return AccessEffect.Allow;
        } else {
            boolean acl = false;
            if (dbBucket.permission == BucketMeta.PERM_PUBLIC_READ || dbBucket.permission == BucketMeta.PERM_PUBLIC_READ_WRITE) {
                acl = true;
            }
            return OpBucket.checkPolicy(req, dbBucket, action, acl, null, authResult);
        }
    }
    
    /**
     * 检查bucket的ACL和Policy权限，若没有权限则拒绝对象访问
     * @param req
     * @param resp
     * @param dbBucket
     * @param key
     * @param authResult
     * @throws Exception
     */
    public static AccessEffect checkObjectACLsAndPolicy(HttpServletRequest req, BucketMeta dbBucket, String key,
            AuthResult authResult,String action, String method) throws Exception {
        AccessEffect accessEffect = null;
        // 如果是根用户且账户拥有bucket，返回允许
        if (authResult != null && authResult.isRoot() && authResult.owner.getId() == dbBucket.getOwnerId()) {
            accessEffect = AccessEffect.Allow;
        } else {
            boolean acl = true;
            if (dbBucket.permission == BucketMeta.PERM_PRIVATE) {
                acl = false;
            } else if ((dbBucket.permission == BucketMeta.PERM_PUBLIC_READ) && (method.equalsIgnoreCase(HttpMethod.PUT.toString())
                    || (method.equalsIgnoreCase(HttpMethod.DELETE.toString()) || method.equalsIgnoreCase(HttpMethod.POST.toString())))) {
                acl = false;
            }
            if (action != null) {
                accessEffect = OpBucket.checkPolicy(req, dbBucket, action, acl, key, authResult);
                if (accessEffect == AccessEffect.Deny) {
                    throw new BaseException(403, ErrorMessage.ERROR_CODE_403, ErrorMessage.ERROR_MESSAGE_403);
                }
            }
        }
        return accessEffect;
    }
}
