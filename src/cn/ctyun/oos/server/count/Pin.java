package cn.ctyun.oos.server.count;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import cn.ctyun.common.Consts;
import cn.ctyun.oos.server.usage.FiveMinuteUsage;

/**
 * @author Dong chk 探针
 */
public class Pin {
    private static Log log = LogFactory.getLog(Pin.class);

    /**TODO fieldName和minutesUsageMeta变量名是否需要对应
     * 根据返回码确定增量字段
     * @param status
     * @return
     */
    private String getStatusCode(int status) {
        String statusString = "";
        if (status >= 200 && status <300) {
            if (status == 200 || status == 204 || status == 206)
                statusString = String.valueOf(status);
            else
                statusString = "2_n";
        } else if (status >= 400 && status < 500) {
            if (status == 403 || status == 404)
                statusString = String.valueOf(status);
            else
                statusString = "4_n";
        } else if (status >= 500 && status < 600) {
            if (status == 500 || status == 503)
                statusString = String.valueOf(status);
            else
                statusString = "5_n";
        }
        return statusString;
    }

    public void increStorage(long usrId, long size, String region, String bucketName, String storageType) {
        if(size == 0)
            return;
        try {
            FiveMinuteUsage.addAndGet(region, usrId, null, "size_total", size, storageType);
            FiveMinuteUsage.addAndGet(region, usrId, bucketName, "size_total", size, storageType);
        } catch (SecurityException e) {
        }
    }

    public void increRedundantStorage(long usrId, long size,String region, String bucketName, String storageType) {
        if(size == 0)
            return;
        try {
            FiveMinuteUsage.addAndGet(region, usrId, null, "size_redundant", size, storageType);
            FiveMinuteUsage.addAndGet(region, usrId, bucketName, "size_redundant", size, storageType);
        } catch (SecurityException e) {
        }
    }
    
    public void increAlignStorage(long usrId, long size,String region, String bucketName, String storageType) {
        if(size == 0)
            return;
        try {
            FiveMinuteUsage.addAndGet(region, usrId, null, "size_alin", size, storageType);
            FiveMinuteUsage.addAndGet(region, usrId, bucketName, "size_alin", size, storageType);
        } catch (SecurityException e) {
        }
    }
    
    public void increChangeSize(long usrId, long size, String region, String bucketName, String storageType) {
        if(size == 0)
            return;
        try {
            FiveMinuteUsage.addAndGet(region, usrId, null, "size_changeSize", size, storageType);
            FiveMinuteUsage.addAndGet(region, usrId, bucketName, "size_changeSize", size, storageType);
        } catch (SecurityException e) {
        }
    }
    
    public void increPreChangeSize(long usrId, long size, String region, String bucketName, String storageType) {
        if(size == 0)
            return;
        try {
            FiveMinuteUsage.addAndGet(region, usrId, null, "size_preChangeSize", size, storageType);
            FiveMinuteUsage.addAndGet(region, usrId, bucketName, "size_preChangeSize", size, storageType);
        } catch (SecurityException e) {
        }
    }
    
    public void increPreChangeCompelete(long usrId, long size, String region, String bucketName, String storageType) {
        if(size == 0)
            return;
        try {
            FiveMinuteUsage.addAndGet(region, usrId, null, "size_preChangeComplete", size, storageType);
            FiveMinuteUsage.addAndGet(region, usrId, bucketName, "size_preChangeComplete", size, storageType);
        } catch (SecurityException e) {
        }
    }
    
    public void increPreDelete(long usrId, long size, String region, String bucketName, String storageType) {
        if(size == 0)
            return;
        try {
            FiveMinuteUsage.addAndGet(region, usrId, null, "size_preDeleteSize", size, storageType);
            FiveMinuteUsage.addAndGet(region, usrId, bucketName, "size_preDeleteSize", size, storageType);
        } catch (SecurityException e) {
        }
    }
    
    public void increPreDeleteCompelete(long usrId, long size, String region, String bucketName, String storageType) {
        if(size == 0)
            return;
        try {
            FiveMinuteUsage.addAndGet(region, usrId, null, "size_preDeleteComplete", size, storageType);
            FiveMinuteUsage.addAndGet(region, usrId, bucketName, "size_preDeleteComplete", size, storageType);
        } catch (SecurityException e) {
        }
    }
    
    public void increSizeRestore(long usrId, long size, String region, String bucketName, String storageType) {
        if(size == 0)
            return;
        try {
            FiveMinuteUsage.addAndGet(region, usrId, null, "size_restore", size, storageType);
            FiveMinuteUsage.addAndGet(region, usrId, bucketName, "size_restore", size, storageType);
        } catch (SecurityException e) {
        }
    }
    
    public void increSizeCompelete(long usrId, long size, String region, String bucketName, String storageType) {
        if(size == 0)
            return;
        try {
            FiveMinuteUsage.addAndGet(region, usrId, null, "size_completeSize", size, storageType);
            FiveMinuteUsage.addAndGet(region, usrId, bucketName, "size_completeSize", size, storageType);
        } catch (SecurityException e) {
        }
    }
    
    public void increUpload(long usrId, long size, String region, String bucketName, String storageType) {
        if(size == 0)
            return;
        try {
            FiveMinuteUsage.addAndGet(region, usrId, null, "flow_upload", size, storageType);
            if (bucketName != null) {
                FiveMinuteUsage.addAndGet(region, usrId, bucketName, "flow_upload", size, storageType);
            }
        } catch (SecurityException e) {
        }
    }        
    
    public void increNoNetUpload(long usrId, long size, String region, String bucketName, String storageType) {
        if(size == 0)
            return;
        try {
            FiveMinuteUsage.addAndGet(region, usrId, null, "flow_noNetUpload", size, storageType);
            if (bucketName != null) {
                FiveMinuteUsage.addAndGet(region, usrId, bucketName, "flow_noNetUpload", size, storageType);
            }
        } catch (SecurityException e) {
        }
    }

    /**
     * 用户删除object成功后调用
     * 
     * @param usrId
     *            ：用户id
     * @param size
     *            ：用户删除的object大小，单位是字节
     */
    public void decreStorage(long usrId, long size, String region, String bucketName, String storageType) {
        increStorage(usrId, -size, region, bucketName, storageType);
    }

    public void decreRedundantStorage(long usrId, long size, String region, String bucketName, String storageType) {
        increRedundantStorage(usrId, -size, region, bucketName, storageType);
    }

    public void decreAlignStorage(long usrId, long size, String region, String bucketName, String storageType) {
        increAlignStorage(usrId, -size, region, bucketName, storageType);
    }
   
    public void decreOriginalStorage(long usrId, long size, String region, String bucketName, String storageType) {
        increOriginalStorage(usrId, -size, region, bucketName, storageType);
    }

    /**
     * 用户成功读取object后调用
     * 
     * @param usrId
     *            ：用户id
     * @param size
     *            ：用户读取的object大小，单位是字节
     */
    public void increTransfer(long usrId, long size, String region, String bucketName, String storageType) {
        if(size == 0)
            return;
        try { 
            FiveMinuteUsage.addAndGet(region, usrId, null, "flow_download", size, storageType);
            if (bucketName != null) {
                FiveMinuteUsage.addAndGet(region, usrId, bucketName, "flow_download", size, storageType);
            }
        } catch (SecurityException e) {
        }
    }

    /**
     * 用户成功读取object后，并且请求ip是非互联网ip时调用
     * 
     * @param usrId
     *            ：用户id
     * @param size
     *            ：用户读取的object大小，单位是字节
     */
    public void increNoNetTransfer(long usrId, long size, String region, String bucketName, String storageType) {
        if(size == 0)
            return;
        try {
            FiveMinuteUsage.addAndGet(region, usrId, null, "flow_noNetDownload", size, storageType);
            if (bucketName != null) {
                FiveMinuteUsage.addAndGet(region, usrId, bucketName, "flow_noNetDownload", size, storageType);
            }
        } catch (SecurityException e) {
        }
    }

    /**
     * 用户发出一个HTTP Get Method请求,并且请求ip属于互联网时调用,用于计量
     * @param usrId
     *            ：用户id
     * @param region
     * @param bucketName
     */
    public void increGetMethodRequest(long usrId, String region, String bucketName, int status, String storageType) {
        try {
            FiveMinuteUsage.addAndGet(region, usrId, null, "req_get", 1, storageType);
            String statusString = getStatusCode(status);
            if (statusString.length() != 0) {
                FiveMinuteUsage.addAndGet(region, usrId, null, "code_get" + statusString, 1, storageType);
            }
            if (bucketName != null && !bucketName.trim().isEmpty()) {
                FiveMinuteUsage.addAndGet(region, usrId, bucketName, "req_get", 1, storageType);
                if (statusString.length() != 0) {
                    FiveMinuteUsage.addAndGet(region, usrId, bucketName, "code_get" + statusString, 1, storageType);
                }
            }
        } catch (SecurityException e) {
        }
    }
    
    /**
     * 用户发出一个HTTP Get Method请求时,并且请求ip属于非互联网时调用,用于计量
     * @param usrId
     *            ：用户id
     * @param region
     * @param bucketName
     */
    public void increNoNetGetMethodReq(long usrId, String region, String bucketName, int status, String storageType) {
        try {
            FiveMinuteUsage.addAndGet(region, usrId, null, "req_noNetGet", 1, storageType);
            String statusString = getStatusCode(status);
            if (statusString.length() != 0) {
                FiveMinuteUsage.addAndGet(region, usrId, null, "code_noNetGet" + statusString, 1, storageType);
            }
            if (bucketName != null && !bucketName.trim().isEmpty()) {
                FiveMinuteUsage.addAndGet(region, usrId, bucketName, "req_noNetGet", 1, storageType);

                if (statusString.length() != 0) {
                    FiveMinuteUsage.addAndGet(region, usrId, bucketName, "code_noNetGet" + statusString, 1, storageType);
                }
            }
        } catch (SecurityException e) {
        }
    }

    /**
     * 用户发出一个HTTP Head Method请求,并且请求ip属于互联网时调用,用于计量
     * @param usrId
     *            ：用户id
     * @param region
     * @param bucketName
     */
    public void increHeadMethodRequest(long usrId, String region, String bucketName, int status, String storageType) {
        try {
            FiveMinuteUsage.addAndGet(region, usrId, null, "req_head", 1, storageType);
            String statusString = getStatusCode(status);
            if (statusString.length() != 0) {
                FiveMinuteUsage.addAndGet(region, usrId, null, "code_head" + statusString, 1, storageType);
            }
            if (bucketName != null && !bucketName.trim().isEmpty()) {
                FiveMinuteUsage.addAndGet(region, usrId, bucketName, "req_head", 1, storageType);
                if (statusString.length() != 0) {
                    FiveMinuteUsage.addAndGet(region, usrId, bucketName, "code_head" + statusString, 1, storageType);
                }
            }
        } catch (SecurityException e) {
        }
    }

    /**
     * 用户发出一个HTTP Head Method请求时,并且请求ip属于非互联网时调用,用于计量
     * @param usrId
     *            ：用户id
     * @param region
     * @param bucketName
     */
    public void increNoNetHeadMethodReq(long usrId, String region, String bucketName, int status, String storageType) {
        try {
            FiveMinuteUsage.addAndGet(region, usrId, null, "req_noNetHead", 1, storageType);
            String statusString = getStatusCode(status);
            if (statusString.length() != 0) {
                FiveMinuteUsage.addAndGet(region, usrId, null, "code_noNetHead" + statusString, 1, storageType);
            }
            if (bucketName != null && !bucketName.trim().isEmpty()) {
                FiveMinuteUsage.addAndGet(region, usrId, bucketName, "req_noNetHead", 1, storageType);
                if (statusString.length() != 0) {
                    FiveMinuteUsage.addAndGet(region, usrId, bucketName, "code_noNetHead" + statusString, 1, storageType);
                }
            }
        } catch (SecurityException e) {
        }
    }
    
    /**
     * 用户发出一个HTTP Put Method请求时,并且请求ip属于互联网时调用,用于计量
     * @param usrId
     * @param region
     * @param bucketName
     */
    public void increPutMethodRequest(long usrId, String region, String bucketName, int status, String storageType) {
        try {
            FiveMinuteUsage.addAndGet(region, usrId, null, "req_put", 1, storageType);
            String statusString = getStatusCode(status);
            if (statusString.length() != 0) {
                FiveMinuteUsage.addAndGet(region, usrId, null, "code_put" + statusString, 1, storageType);
            }
            if (bucketName != null && !bucketName.trim().isEmpty()) {
                FiveMinuteUsage.addAndGet(region, usrId, bucketName, "req_put", 1, storageType);
                if (statusString.length() != 0) {
                    FiveMinuteUsage.addAndGet(region, usrId, bucketName, "code_put" + statusString, 1, storageType);
                }
            }
        } catch (SecurityException e) {
        }
    }
    
    /**
     * 用户发出一个HTTP Put Method请求时,并且请求ip属于非互联网时调用,用于计量
     * @param usrId
     * @param region
     * @param bucketName
     */
    public void increNoNetPutMethodReq(long usrId, String region, String bucketName, int status, String storageType) {
        try {
            FiveMinuteUsage.addAndGet(region, usrId, null, "req_noNetPut", 1, storageType);
            String statusString = getStatusCode(status);
            if (statusString.length() != 0) { 
                FiveMinuteUsage.addAndGet(region, usrId, null, "code_noNetPut" + statusString, 1, storageType);
            }
            if (bucketName != null && !bucketName.trim().isEmpty()) {
                FiveMinuteUsage.addAndGet(region, usrId, bucketName, "req_noNetPut", 1, storageType);

                if (statusString.length() != 0) {
                    FiveMinuteUsage.addAndGet(region, usrId, bucketName, "code_noNetPut" + statusString, 1, storageType);
                }
            }
        } catch (SecurityException e) {
        }
    }
    
    /**
     * 用户发出一个HTTP Post Method请求时,并且请求ip属于互联网时调用,用于计量
     * @param usrId
     * @param region
     * @param bucketName
     */
    public void increPostMethodRequest(long usrId, String region, String bucketName, int status, String storageType) {
        try {
            FiveMinuteUsage.addAndGet(region, usrId, null, "req_post", 1, storageType);
            String statusString = getStatusCode(status);
            if (statusString.length() != 0) {
                FiveMinuteUsage.addAndGet(region, usrId, null, "code_post" + statusString, 1, storageType);
            }
            if (bucketName != null && !bucketName.trim().isEmpty()) {
                FiveMinuteUsage.addAndGet(region, usrId, bucketName, "req_post", 1, storageType);

                if (statusString.length() != 0) {
                    FiveMinuteUsage.addAndGet(region, usrId, bucketName, "code_post" + statusString, 1, storageType);
                }
            }
        } catch (SecurityException e) {
        }
    }
    
    /**
     * 用户发出一个HTTP Post Method请求时,并且请求ip属于非互联网时调用,用于计量
     * @param usrId
     * @param region
     * @param bucketName
     */
    public void increNoNetPostMethodReq(long usrId, String region, String bucketName, int status, String storageType) {
        try {
            FiveMinuteUsage.addAndGet(region, usrId, null, "req_noNetPost", 1, storageType);
            String statusString = getStatusCode(status);
            if (statusString.length() != 0) {
                FiveMinuteUsage.addAndGet(region, usrId, null, "code_noNetPost" + statusString, 1, storageType);
            }
            if (bucketName != null && !bucketName.trim().isEmpty()) {
                FiveMinuteUsage.addAndGet(region, usrId, bucketName, "req_noNetPost", 1, storageType);
                if (statusString.length() != 0) {
                    FiveMinuteUsage.addAndGet(region, usrId, bucketName, "code_noNetPost" + statusString, 1, storageType);
                }
            }
        } catch (SecurityException e) {
        }
    }
    
    /**
     * 用户发出一个HTTP Delete Method请求时,并且请求ip属于互联网时调用,用于计量
     * @param usrId
     * @param region
     * @param bucketName
     */
    public void increDeleteMethodRequest(long usrId, String region, String bucketName, int status, String storageType) {
        try {
            FiveMinuteUsage.addAndGet(region, usrId, null, "req_delete", 1, storageType);
            String statusString = getStatusCode(status);
            if (statusString.length() != 0) {
                FiveMinuteUsage.addAndGet(region, usrId, null, "code_delete" + statusString, 1, storageType);
            }
            if (bucketName != null && !bucketName.trim().isEmpty()) {
                FiveMinuteUsage.addAndGet(region, usrId, bucketName, "req_delete", 1, storageType);
                if (statusString.length() != 0) {
                    FiveMinuteUsage.addAndGet(region, usrId, bucketName, "code_delete" + statusString, 1, storageType);
                }
            }
        } catch (SecurityException e) {
        }
    }
    
    /**
     * 用户发出一个HTTP Delete Method请求时,并且请求ip属于非互联网时调用,用于计量
     * @param usrId
     * @param region
     * @param bucketName
     */
    public void increNoNetDeleteMethodReq(long usrId, String region, String bucketName, int status, String storageType) {
        try {
            FiveMinuteUsage.addAndGet(region, usrId, null, "req_noNetDelete", 1, storageType);
            String statusString = getStatusCode(status);
            if (statusString.length() != 0) {
                FiveMinuteUsage.addAndGet(region, usrId, null, "code_noNetDelete" + statusString, 1, storageType);
            }
            if (bucketName != null && !bucketName.trim().isEmpty()) {
                FiveMinuteUsage.addAndGet(region, usrId, bucketName, "req_noNetDelete", 1, storageType);
                if (statusString.length() != 0) {
                    FiveMinuteUsage.addAndGet(region, usrId, bucketName, "code_noNetDelete" + statusString, 1, storageType);
                }
            }
        } catch (SecurityException e) {
        }
    }
    
    /**
     * 用户发出一个HTTP Other Method(Options)请求时,并且请求ip属于互联网时调用,用于计量
     * @param usrId
     * @param region
     * @param bucketName
     */
    public void increOtherMethodRequest(long usrId, String region, String bucketName, int status, String storageType) {
        try {
            FiveMinuteUsage.addAndGet(region, usrId, null, "otherRequest", 1, storageType);
            String statusString = getStatusCode(status);
            if (statusString.length() != 0) {
                FiveMinuteUsage.addAndGet(region, usrId, null, "code_other" + statusString, 1, storageType);
            }
            if (bucketName != null && !bucketName.trim().isEmpty()) {
                FiveMinuteUsage.addAndGet(region, usrId, bucketName, "otherRequest", 1, storageType);
                if (statusString.length() != 0) {
                    FiveMinuteUsage.addAndGet(region, usrId, bucketName, "code_other" + statusString, 1, storageType);
                }
            }
        } catch (SecurityException e) {
        }
    }
    
    /**
     * 用户发出一个HTTP Other Method(Options)请求时,并且请求ip属于非互联网时调用,用于计量
     * @param usrId
     * @param region
     * @param bucketName
     */
    public void increNoNetOtherMethodReq(long usrId, String region, String bucketName, int status, String storageType) {
        try {
            FiveMinuteUsage.addAndGet(region, usrId, null, "noNetOtherReq", 1, storageType);
            String statusString = getStatusCode(status);
            if (statusString.length() != 0) {
                FiveMinuteUsage.addAndGet(region, usrId, null, "code_noNetOther" + statusString, 1, storageType);
            }
            if (bucketName != null && !bucketName.trim().isEmpty()) {
                FiveMinuteUsage.addAndGet(region, usrId, bucketName, "noNetOtherReq", 1, storageType);
                if (statusString.length() != 0) {
                    FiveMinuteUsage.addAndGet(region, usrId, bucketName, "code_noNetOther" + statusString, 1, storageType);
                }
            }
        } catch (SecurityException e) {
        }
    }
    
    /**
     * 用户发出文本反垃圾请求时调用
     * @param usrId
     * @param region
     */
    public void increSpamRequest(long usrId, String region, String bucketName, String storageType) {
        try {
            FiveMinuteUsage.addAndGet(region, usrId, null, "req_spam", 1, storageType);
            FiveMinuteUsage.addAndGet(region, usrId, bucketName, "req_spam", 1, storageType);
        } catch (SecurityException e) {
        }
    }
    
    /**
     * 用户发出图片鉴黄请求返回review结果为false，即算法确定部分
     * @param usrId
     * @param region
     */
    public void increPornReviewFalseRequset(long usrId, String region, String bucketName, String storageType) {
        try {
            FiveMinuteUsage.addAndGet(region, usrId, null, "req_pornReviewFalse", 1, storageType);
            FiveMinuteUsage.addAndGet(region, usrId, bucketName, "req_pornReviewFalse", 1, storageType);
        } catch (SecurityException e) {
        }
    }
    
    /**
     * 用户发出图片鉴黄请求返回review结果为true，即待用户确认部分
     * @param usrId
     * @param region
     */
    public void increPornReviewTrueRequset(long usrId, String region, String bucketName, String storageType) {
        try {
            FiveMinuteUsage.addAndGet(region, usrId, null, "req_pornReviewTrue", 1, storageType);
            FiveMinuteUsage.addAndGet(region, usrId, bucketName, "req_pornReviewTrue", 1, storageType);
        } catch (SecurityException e) {
        }
    }

    public void increOriginalStorage(long usrId, long size, String region, String bucketName, String storageType) {
        if(size == 0)
            return;
        try {
            FiveMinuteUsage.addAndGet(region, usrId, null, "size_originalTotal", size, storageType);
            FiveMinuteUsage.addAndGet(region, usrId, bucketName, "size_originalTotal", size, storageType);
        } catch (SecurityException e) {
        }
    }

    public void increRoamUpload(long usrId, long size, String region, String bucketName, String storageType) {
        if(size == 0)
            return;
        try {
            FiveMinuteUsage.addAndGet(region, usrId, null, "flow_roamUpload", size, storageType);
            if (bucketName != null) {
                FiveMinuteUsage.addAndGet(region, usrId, bucketName, "flow_roamUpload", size, storageType);
            }
        } catch (SecurityException e) {
        }
    }

    public void increNoNetRoamUpload(long usrId, long size, String region, String bucketName, String storageType) {
        if(size == 0)
            return;
        try {
            FiveMinuteUsage.addAndGet(region, usrId, null, "flow_noNetRoamUpload", size, storageType);
            if (bucketName != null) {
                FiveMinuteUsage.addAndGet(region, usrId, bucketName, "flow_noNetRoamUpload", size, storageType);
            }
        } catch (SecurityException e) {
        }
    }

    public void increRoamFlow(long usrId, long size, String region, String bucketName, String storageType) {
        if(size == 0)
            return;
        try {
            FiveMinuteUsage.addAndGet(region, usrId, null, "flow_roamDownload", size, storageType);
            if (bucketName != null) {
                FiveMinuteUsage.addAndGet(region, usrId, bucketName, "flow_roamDownload", size, storageType);
            }
        } catch (SecurityException e) {
        }
    }
    
    public void increNoNetRoamFlow(long usrId, long size, String region, String bucketName, String storageType) {
        if(size == 0)
            return;
        try {
            FiveMinuteUsage.addAndGet(region, usrId, null, "flow_noNetRoamDownload", 1, storageType);
            if (bucketName != null) {
                FiveMinuteUsage.addAndGet(region, usrId, bucketName, "flow_noNetRoamDownload", 1, storageType);
            }
        } catch (SecurityException e) {
        }
    }

    public void deleteFlow(long usrId, long size, String region, String bucketName, String storageType) {
        if(size == 0)
            return;
        try {
            FiveMinuteUsage.addAndGet(region, usrId, null, "flow_delete", size, storageType);
            FiveMinuteUsage.addAndGet(region, usrId, bucketName, "flow_delete", size, storageType);
        } catch (SecurityException e) {
        }
    }
    
    public void noNetDeleteFlow(long usrId, long size, String region, String bucketName, String storageType) {
        if(size == 0)
            return;
        try {
            FiveMinuteUsage.addAndGet(region, usrId, null, "flow_noNetDelete", size, storageType);
            FiveMinuteUsage.addAndGet(region, usrId, bucketName, "flow_noNetDelete", size, storageType);
        } catch (SecurityException e) {
        }
    }
    
    //restore 数据取回
    public void restoreSize(long usrId, long size, String region, String bucketName, String storageType) {
        if(size == 0)
            return;
        try {
            FiveMinuteUsage.addAndGet(region, usrId, null, "size_restore", size, storageType);
            FiveMinuteUsage.addAndGet(region, usrId, bucketName, "size_restore", size, storageType);
        } catch (SecurityException e) {
        }
    }
    
    //TODO 变更量，提前变更量，提前删除量，提前删除补齐，提前变更补齐，大小补齐
}