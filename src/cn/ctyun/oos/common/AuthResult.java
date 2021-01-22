package cn.ctyun.oos.common;

import java.io.InputStream;

import cn.ctyun.oos.iam.accesscontroller.util.ARNUtils;
import cn.ctyun.oos.metadata.AkSkMeta;
import cn.ctyun.oos.metadata.OwnerMeta;
import cn.ctyun.oos.metadata.TokenMeta;

/**
 * 签名后返回信息
 * 包括签名后获取到的owner、accessKey，请求数据
 * @author wangduo
 *
 */
public class AuthResult {

    /** 请求的账户信息 */
    public OwnerMeta owner;
    
    /** 请求使用的accessKey */
    public AkSkMeta accessKey;
    
    /** sts使用的tokenMeta */
    public TokenMeta tokenMeta;
    
    /** 请求数据 */
    public InputStream inputStream;
    
    /** 是否是临时授权 */
    public boolean isSts;
    
    /**
     * 判断当前用户是否是根用户
     * @return
     */
    public boolean isRoot() {
        // 如果使用根用户AK访问
        if (accessKey != null && accessKey.isRoot == 1) {
            return true;
        }
        // 临时授权为根用户
        if (isSts) {
            return true;
        }
        return false;
    }
    
    /**
     * 获取用户的ARN
     * @return
     */
    public String getUserArn() {
        if (isRoot()) {
            return ARNUtils.generateArn(owner.getAccountId(), "root");
        } else {
            return ARNUtils.generateUserArn(owner.getAccountId(), accessKey.userName);
        }
    }
    
    /**
     * 获取用户的AWS格式ARN
     * @return
     */
    public String getUserAWSArn() {
        if (isRoot()) {
            return ARNUtils.generateAWSArn(owner.getAccountId(), "root");
        } else {
            return ARNUtils.generateUserAWSArn(owner.getAccountId(), accessKey.userName);
        }
    }
    
}
