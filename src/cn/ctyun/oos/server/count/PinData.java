package cn.ctyun.oos.server.count;

import cn.ctyun.common.Consts;
import cn.ctyun.oos.server.JettyAttributes;
import cn.ctyun.oos.server.util.Misc;

/**
 * @author: Cui Meng
 */
public class PinData {
    private long ownerId;
    private String bucketName;
    /* 是否是互联网ip */
    private boolean isInternetIP;
    /* 请求方法 */
    private String methodType;
    /* 归属地容量 */
    private long totalSize;
    /* 应属地容量 */
    private long originalTotalSize;
    /* 直接上传流量 */
    private long upload = 0;
    /* 直接下载流量 */
    private long flow = 0;
    /* 漫游上传流量 */
    private long roamUpload = 0;
    /* 漫游下载流量 */
    private long roamFlow = 0;
    /* 应属地region */
    private String originalRegion;
    /* 归属地region */
    private String dataRegion;
    /* 直接上传地region，directRegion与roamRegion中只能有一个不为空 */
    private String directRegion;
    /* 漫游上传地region，directRegion与roamRegion中只能有一个不为空 */
    private String roamRegion;
    /* 冗余容量 */
    private long redundantSize;
    /* 对齐后容量 */
    private long alinSize = 0;
    /* copy对象时数据取回的量和dataRegion以及存储类型和bucketName，只能是低频或者归档*/
    private long restore = 0;
    private String sourceDataRegion;
    private String sourceStorageType;
    private String sourceBucket;
    /* 存储类型 */
    private String storageType = Consts.STORAGE_CLASS_STANDARD;
    /* 用于判断是否是分段对象，如果是分段对象本期不计算大小补齐 */
    private boolean isMultipartUpload = false;
    
    public JettyAttributes jettyAttributes = new JettyAttributes();
    
    /* 最后修改时间 */
    private long lastModify = 0;
    /* 覆盖上传时的源object信息 */
    public SourceObject sourceObject = null;

    public void setSize(long totalSize, long redundantSize, long alinSize, String dataRegion,
            String originalRegion, String storageType, long lastModify) {
        this.totalSize = totalSize;
        this.originalTotalSize = totalSize;
        this.dataRegion = dataRegion;
        this.originalRegion = originalRegion;
        this.redundantSize = redundantSize;
        this.alinSize = alinSize;
        this.storageType = storageType;
        this.lastModify = lastModify;
    }
    

    public void setDirectUpload(long upload, String directRegion, String storageType) {
        this.upload = upload;
        this.directRegion = directRegion;
        this.storageType = storageType;
    }

    public void setDirectFlow(long transfer, String directRegion, String storageType) {
        this.flow = transfer;
        this.directRegion = directRegion;
        this.storageType = storageType;
    }

    public void setRoamUpload(long roamUpload, String roamRegion, String storageType) {
        this.roamUpload = roamUpload;
        this.roamRegion = roamRegion;
        this.storageType = storageType;
    }

    public void setRoamFlow(long roamFlow, String roamRegion, String storageType) {
        this.roamFlow = roamFlow;
        this.roamRegion = roamRegion;
        this.storageType = storageType;
    }
    
    public void setRequestInfo(long ownerId, String bucketName, boolean isInternetIP, String methodType) {
        this.ownerId = ownerId;
        this.bucketName = bucketName;
        this.isInternetIP = isInternetIP;
        this.methodType = methodType;
    }
    
    public void setRestore(long restore, String sourceDataRegion, String sourceStorageType, String bucketName) {
        this.restore = restore;
        this.sourceDataRegion = sourceDataRegion;
        this.sourceStorageType = sourceStorageType;
        this.sourceBucket = bucketName;
    }

    public long getTotalSize() {
        return totalSize;
    }

    public long getRedundantSize() {
        return redundantSize;
    }

    public long getAlinSize() {
        return alinSize;
    }

    public String getOriginalRegion() {
        return originalRegion;
    }

    public String getDataRegion() {
        return dataRegion;
    }

    public long getUpload() {
        return upload;
    }

    public String getDirectRegion() {
        return directRegion;
    }

    public long getOriginalTotalSize() {
        return originalTotalSize;
    }

    public long getFlow() {
        return flow;
    }

    public long getRoamUpload() {
        return roamUpload;
    }

    public long getRoamFlow() {
        return roamFlow;
    }
    
    public long getRestore() {
        return restore;
    }
    
    public String getSourceDataRegion() {
        return sourceDataRegion;
    }

    public String getSourceStorageType() {
        return sourceStorageType;
    }
    
    public String getSourceBucket() {
        return sourceBucket;
    }
    
    public String getRoamRegion() {
        return roamRegion;
    }
   
    public long getOwnerId() {
        return ownerId;
    }

    public String getBucketName() {
        return bucketName;
    }

    public boolean getIsInternetIp() {
        return isInternetIP;
    }

    public String getMethodType() {
        return methodType;
    }
    
    public void setStorageType(String storageType) {
        this.storageType = storageType;
    }
    
    public String getStorageType() {
        return storageType;
    }
    
    public void setIsMultipartUploadTrue() {
        this.isMultipartUpload = true;
    }
    
    public boolean getIsMultipartUpload() {
        return isMultipartUpload;
    }
    
    public long getLastModify() {
        return lastModify;
    }
    
    public boolean needRestoreAndCompelete() {
        return storageType.equals(Consts.STORAGE_CLASS_STANDARD_IA);
    }
    
    
    public class SourceObject {
        private long sourceSize = 0;
        private long sourceAlinSize = 0;
        private long sourceRedundantSize = 0;
        private long lastModify = 0;
        private String storageType = Consts.STORAGE_CLASS_STANDARD;
        private String dataRegion;
        private String originalRegion;
        /* 用于判断是否是分段对象，如果是分段对象本期不计算大小补齐 */
        private boolean isMultipartUpload = false;
        
        public SourceObject(long sourceSize, long sourceAlinSize, long sourceRedundantSize, long lastModify,
                String storageType, String dataRegion, String originalRegion) {
            this.sourceSize = sourceSize;
            this.sourceAlinSize = sourceAlinSize;
            this.sourceRedundantSize = sourceRedundantSize;
            this.lastModify = lastModify;
            this.storageType = storageType;
            this.dataRegion = dataRegion;
            this.originalRegion = originalRegion;
        }

        public long getSourceSize() {
            return sourceSize;
        }

        public void setSourceSize(long sourceSize) {
            this.sourceSize = sourceSize;
        }

        public long getSourceAlinSize() {
            return sourceAlinSize;
        }

        public void setSourceAlinSize(long sourceAlinSize) {
            this.sourceAlinSize = sourceAlinSize;
        }

        public long getSourceRedundantSize() {
            return sourceRedundantSize;
        }

        public void setSourceRedundantSize(long sourceRedundantSize) {
            this.sourceRedundantSize = sourceRedundantSize;
        }

        public long getLastModify() {
            return lastModify;
        }

        public void setLastModify(long lastModify) {
            this.lastModify = lastModify;
        }

        public String getStorageType() {
            return storageType;
        }
        
        public String getDataRegion() {
            return dataRegion;
        }
        
        public String getOriginalRegion() {
            return originalRegion;
        }

        public void setStorageType(String storageType) {
            this.storageType = storageType;
        }    
        
        public boolean needRestoreAndCompelete() {
            return storageType.equals(Consts.STORAGE_CLASS_STANDARD_IA);
        }
        
        public void setIsMultipartUploadTrue() {
            this.isMultipartUpload = true;
        }
        
        public boolean getIsMultipartUpload() {
            return isMultipartUpload;
        }
    }
}
