package cn.ctyun.oos.common;

import org.apache.commons.lang3.StringUtils;

import cn.ctyun.common.BaseException;

public class DeleteMultipleVersionObjByPrefix {
    private String bucketName;
    private long versionId;
    private String marker;
    private String prefix;
    private int maxKey;
    private boolean quiet = false;
    private boolean errorContinue = true;
    
    public void checkParam() throws BaseException {
        if(versionId == 0) {
            throw new BaseException("delete multiple version objects xml error ",
                    400, ErrorMessage.ERROR_CODE_MALFORMEDXML, "VersionId need not null");
        }
        if (StringUtils.isBlank(prefix)) {
            throw new BaseException("delete multiple version objects xml error ",
                    400, ErrorMessage.ERROR_CODE_MALFORMEDXML, "Prefix can't be empty");
        }
        if (StringUtils.isBlank(marker)) {
            throw new BaseException("delete multiple version objects xml error ",
                    400, ErrorMessage.ERROR_CODE_MALFORMEDXML, "Marker can't be empty");
        }
        if (maxKey <= 0) {
            throw new BaseException("delete multiple version objects xml error ",
                    400, ErrorMessage.ERROR_CODE_MALFORMEDXML, "Max-keys must be greater than zero");
        }
    }
    
    public String getBucketName() {
        return bucketName;
    }
    public void setBucketName(String bucketName) {
        this.bucketName = bucketName;
    }
    public long getVersionId() {
        return versionId;
    }
    public void setVersionId(long versionId) {
        this.versionId = versionId;
    }
    public String getMarker() {
        return marker;
    }
    public void setMarker(String marker) {
        this.marker = marker;
    }
    public String getPrefix() {
        return prefix;
    }
    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }
    public boolean isQuiet() {
        return quiet;
    }
    public void setQuiet(boolean quiet) {
        this.quiet = quiet;
    }
    public boolean isErrorContinue() {
        return errorContinue;
    }
    public boolean getErrorContinue() {
        return this.errorContinue;
    }
    public void setErrorContinue(boolean errorContinue) {
        this.errorContinue = errorContinue;
    }
    public int getMaxKey() {
        return maxKey;
    }
    public void setMaxKey(int maxKey) {
        this.maxKey = maxKey;
    }
    
}
