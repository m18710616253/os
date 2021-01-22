package cn.ctyun.oos.common;


public enum OOSActions{  
    Regions_Get("GetRegions"), // OOS特有
    Service_Get("ListAllMyBucket"),   
    Bucket_Put("PutBucket"),
    Bucket_Delete("DeleteBucket"),  
    Bucket_Get("ListBucket"),        
    Bucket_Get_Acl("GetBucketAcl"),   
    Bucket_Put_Acl("PutBucket"),   
    Bucket_Get_Policy("GetBucketPolicy"),   
    Bucket_Put_Policy("PutBucketPolicy"),    
    Bucket_Delete_Policy("DeleteBucketPolicy"),
    Bucket_Get_WebSite("GetBucketWebsite"),   
    Bucket_Put_WebSite("PutBucketWebsite"),    
    Bucket_Delete_WebSite("DeleteBucketWebsite"),
    Bucket_Put_Logging("PutBucketLogging"),
    Bucket_Get_logging("GetBucketLogging"),
    Bucket_Get_MultipartUploads("ListBucketMultipartUploads"),
    Bucket_Put_Lifecycle("PutLifecycleConfiguration"),
    Bucket_Get_Lifecycle("GetLifecycleConfiguration"),
    Bucket_Delete_Lifecycle("PutLifecycleConfiguration"),
    Bucket_Put_Accelerate("PutAccelerateConfiguration"),
    Bucket_Get_Accelerate("GetBucketAccelerate"),
    Bucket_Get_Cors("GetBucketCORS"),
    Bucket_Put_Cors("PutBucketCORS"),
    Bucket_Delete_Cors("PutBucketCORS"),
    Bucket_Put_Versioning("PutBucketVersioning"), // version相关不进行访问控制
    Bucket_Get_Versioning("GetBucketVersioning"), // version相关不进行访问控制
    Bucket_Head("ListBucket"), 
    Bucket_Delete_MultipleObjects("DeleteMultipleObjects"),   
    Bucket_Get_Location("GetBucketLocation"),
    Bucket_Get_Subresource(""), // 内部根据具体的逻辑做访问控制 GetBucketAcl GetBucketPolicy GetBucketWebsite GetBucketLogging GetBucketAccelerate GetLifecycleConfiguration GetBucketCORS
    Bucket_Put_ObjectLockConfiguration("PutBucketObjectLockConfiguration"), // 添加修改对象锁定配置
    Bucket_Get_ObjectLockConfiguration("GetBucketObjectLockConfiguration"), // 获取对象锁定配置
    Bucket_Delete_ObjectLockConfiguration("DeleteBucketObjectLockConfiguration"), // 删除对象锁定配置
    Object_Get("GetObject"),   
    Object_Put("PutObject"),   
    Object_Delete("DeleteObject"), 
    Object_Get_ListParts("ListMultipartUploadParts"),
    Object_Get_ACL("GetObjectAcl"),
    Object_Put_UploadPart("PutObject"),
    Object_Put_CopyPart("PutObject"), 
    Object_Put_Copy("PutObject"), 
    Object_Delete_AbortMultipartUpload("AbortMultipartUpload"),
    Object_Head("GetObject"), 
    Object_Post_InitiateMultipartUpload("PutObject"),
    Object_Post_CompleteMultipartUpload("PutObject"),
    Object_Post("PutObject"),
    Object_Options(""); // 不做访问控制
    
    public String actionName;
    
    private OOSActions(String actionName) {  
        this.actionName = actionName;  
    }
}