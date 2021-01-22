package cn.ctyun.oos.server;

import org.json.JSONObject;

import cn.ctyun.oos.common.OOSActions;
import cn.ctyun.oos.hbase.MetaClient;
import cn.ctyun.oos.metadata.OwnerMeta;

/**
 * 
 * 需要保留记录的key-value值
 * @author peijingj
 *
 */
public class JettyAttributes {

    public OOSActions action;
    public BucketOwnerAttribute bucketOwnerAttribute;
    public ManageEventAttribute manageEventAttribute;
    public ObjectAttribute objectAttribute;
    public long reqHeadFlow = 0;
    public long respHeadFlow = 0;
    
    public JettyAttributes() {
        this.bucketOwnerAttribute = new BucketOwnerAttribute();
        this.manageEventAttribute = new ManageEventAttribute();
        this.objectAttribute = new ObjectAttribute();
    }    
    
    public class ObjectAttribute {
        public long objectSize;
        public long length;
        public long storageId = -1;
    }

    public class BucketOwnerAttribute {
        public String bucketOwnerName;
        public long bucketOwnerId = -1;
        public String bucketOwnerDisplayName;
        
        public void setBucketOwner(OwnerMeta owner) {
            this.bucketOwnerId = owner.getId();
            this.bucketOwnerName = owner.getName();
            this.bucketOwnerDisplayName = owner.displayName;
        }
        
        public OwnerMeta getBucketOwnerFromAttribute(long ownerId) throws Exception {
            if (bucketOwnerId == -1) {
                OwnerMeta owner = new OwnerMeta(ownerId);
                MetaClient client = MetaClient.getGlobalClient();
                client.ownerSelectById(owner);
                return owner;
            }
            OwnerMeta owner = new OwnerMeta(bucketOwnerId);
            owner.name = bucketOwnerName;
            owner.displayName = bucketOwnerDisplayName;
            return owner;
        }
    }
    
    public class ManageEventAttribute {
        public long reqRecieveTime;
        public String bucketName;
        public String userName;
        public String userType;
        public String principleId;
        public long ownerId = -1;
        public boolean ifRecordManageEvent;
        public String bucketAccountId;
        public long bucketOwnerId = -1;
        public JSONObject reqContent;
        public String userArn;
        public String accessKeyId;
    }   
}
