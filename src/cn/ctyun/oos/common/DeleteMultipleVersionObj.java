package cn.ctyun.oos.common;

import java.util.List;

import cn.ctyun.common.BaseException;
import cn.ctyun.common.Consts;

public class DeleteMultipleVersionObj {

    /**
     * 简明信息模式来返回响应
     */
    private boolean quiet = false;
    private boolean errorContinue = true;
    private List<KeyVersion> objects;

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

    public List<KeyVersion> getObjects() {
        return objects;
    }

    public void setObjects(List<KeyVersion> objects) {
        this.objects = objects;
    }
    
    public void checkObjectsList() throws BaseException {
        if(null == objects)
            throw new BaseException(400, ErrorMessage.ERROR_CODE_MALFORMEDXML);
        
        if(objects.size() > Consts.LIST_OBJECTS_MAX_KEY) {
            throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_ARGUMENT, "the number of objects exceeds "
                    + Consts.LIST_OBJECTS_MAX_KEY);
        }
        // 与产品确认 版本号可能会等于零
        for(KeyVersion kv : objects) {
            if(null == kv || null == kv.getKey() || null == kv.getVersion()) {
                throw new BaseException(
                        "delete multiple version objects xml error ", 400,
                        ErrorMessage.ERROR_CODE_MALFORMEDXML,
                        "XML Contains a null value");
            }
            if (0L > kv.getVersion()) {
                throw new BaseException(
                        "delete multiple version objects xml error ", 400,
                        ErrorMessage.ERROR_CODE_MALFORMEDXML,
                        "Object version can not less than zero");
            }
        }
    }

    public static class KeyVersion {
        String key;
        Long version;
        
        public KeyVersion() {}
        
        public String getKey() {
            return key;
        }
        public void setKey(String key) {
            this.key = key;
        }
        public Long getVersion() {
            return version;
        }
        public void setVersion(Long version) {
            this.version = version;
        }
    }
}
