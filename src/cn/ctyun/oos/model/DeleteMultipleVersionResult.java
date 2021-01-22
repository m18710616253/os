package cn.ctyun.oos.model;

import cn.ctyun.common.Consts;
import cn.ctyun.oos.common.DeleteMultipleVersionObjectsConstant;
import com.amazonaws.services.s3.internal.XmlWriter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 多版本对象删除结果类。
 *
 * @author wangxs
 */
public class DeleteMultipleVersionResult {

    private Boolean quiet;
    private Boolean errorContinue;
    private String versionId;
    private String marker;
    private String maxKeys;
    private String prefix;
    private String nextKeyMarker;
    private List<String> unexecuted = new ArrayList<String>();
    private List<DeleteFailedDetail> errorList = new ArrayList<DeleteFailedDetail>();
    private Map<String, List<DeleteSuccessDetail>> successMap = new HashMap<String, List<DeleteSuccessDetail>>();

    public DeleteMultipleVersionResult() {
    }

    public DeleteMultipleVersionResult(Boolean quiet, Boolean errorContinue) {
        this.quiet = quiet;
        this.errorContinue = errorContinue;
    }

    public DeleteMultipleVersionResult(Boolean quiet, Boolean errorContinue,
                                       String prefix, String versionId) {
        this.quiet = quiet;
        this.errorContinue = errorContinue;
        this.prefix = prefix;
        this.versionId = versionId;
    }

    public void putSucessDetail(String objectName, String versionId) {
        if (null == this.successMap.get(objectName))
            this.successMap.put(objectName,
                    new ArrayList<DeleteSuccessDetail>());

        DeleteSuccessDetail detail = new DeleteSuccessDetail(objectName,
                versionId);
        this.successMap.get(objectName).add(detail);
    }

    public void putErrorDetail(String objectName, String versionId, String code,
                               String message) {
        this.errorList.add(
                new DeleteFailedDetail(objectName, versionId, code, message));
    }

    public void putUnexecuted(String objectName) {
        unexecuted.add(objectName);
    }

    public void putUnexecuted(List<String> list) {
        unexecuted.addAll(list);
    }

    public String toXmlString() {
        XmlWriter xml = new XmlWriter();
        xml.start(DeleteMultipleVersionObjectsConstant.DELETE_RESULT, "xmlns",
                Consts.XMLNS);
        if (null != errorContinue)
            xml.start(DeleteMultipleVersionObjectsConstant.ERROR_CONTINUE)
                    .value(errorContinue + "").end();
        if (null != prefix)
            xml.start(DeleteMultipleVersionObjectsConstant.PREFIX).value(prefix).end();
        if (null != nextKeyMarker)
            xml.start(DeleteMultipleVersionObjectsConstant.NEXT_KEY_MARKER)
                    .value(nextKeyMarker).end();

        //成功数大于0，并且是非简明信息。
        if (successMap.size() != 0 && (null != quiet && !quiet)) {
            buildSuccessXml(xml);
        }

        if (errorList.size() != 0) {
            buildFaildXml(xml);
        }

        if (unexecuted.size() != 0)
            buildUnexecutedXml(xml);

        // xmlns 的end
        xml.end();
        return Consts.XML_HEADER + xml.toString();
    }

    private void buildUnexecutedXml(XmlWriter xml) {
        xml.start(DeleteMultipleVersionObjectsConstant.UN_EXECUTED);
        for (String str : unexecuted) {
            xml.start(DeleteMultipleVersionObjectsConstant.KEY).value(str)
                    .end();
        }
        xml.end();
    }

    private void buildSuccessXml(XmlWriter xml) {
        for (Map.Entry<String, List<DeleteSuccessDetail>> entry : successMap
                .entrySet()) {
            xml.start(DeleteMultipleVersionObjectsConstant.DELETED);
            xml.start(DeleteMultipleVersionObjectsConstant.KEY)
                    .value(entry.getKey()).end();
            xml.start(DeleteMultipleVersionObjectsConstant.VERSION);
            for (DeleteSuccessDetail d : entry.getValue()) {
                xml.start(DeleteMultipleVersionObjectsConstant.VERSIONID)
                        .value(d.getVersionId()).end();
            }
            xml.end();
            xml.end();
        }
    }

    private void buildFaildXml(XmlWriter xml) {
        xml.start(DeleteMultipleVersionObjectsConstant.ERROR);
        for (DeleteFailedDetail d : errorList) {
            xml.start(DeleteMultipleVersionObjectsConstant.KEY)
                    .value(d.getKey()).end();
            xml.start(DeleteMultipleVersionObjectsConstant.VERSIONID)
                    .value(d.getVersionId()).end();
            xml.start(DeleteMultipleVersionObjectsConstant.CODE)
                    .value(d.getCode()).end();
            xml.start(DeleteMultipleVersionObjectsConstant.MESSAGE)
                    .value(d.getMessage()).end();
        }
        xml.end();
    }

    public Boolean getQuiet() {
        return quiet;
    }

    public void setQuiet(Boolean quiet) {
        this.quiet = quiet;
    }

    public Boolean getErrorContinue() {
        return errorContinue;
    }

    public void setErrorContinue(Boolean errorContinue) {
        this.errorContinue = errorContinue;
    }

    public String getVersionId() {
        return versionId;
    }

    public void setVersionId(String versionId) {
        this.versionId = versionId;
    }

    public String getMarker() {
        return marker;
    }

    public void setMarker(String marker) {
        this.marker = marker;
    }

    public String getMaxKeys() {
        return maxKeys;
    }

    public void setMaxKeys(String maxKeys) {
        this.maxKeys = maxKeys;
    }

    public String getPrefix() {
        return prefix;
    }

    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }

    public String getNextKeyMarker() {
        return nextKeyMarker;
    }

    public void setNextKeyMarker(String nextKeyMarker) {
        this.nextKeyMarker = nextKeyMarker;
    }

    private class DeleteSuccessDetail {
        public DeleteSuccessDetail(String objectName, String versionId) {
            this.key = objectName;
            this.versionId = versionId;
        }

        private String key;
        private String versionId;

        public String getKey() {
            return key;
        }

        public void setKey(String key) {
            this.key = key;
        }

        public String getVersionId() {
            return versionId;
        }

        public void setVersionId(String versionId) {
            this.versionId = versionId;
        }
    }

    private class DeleteFailedDetail {

        public DeleteFailedDetail(String objectName, String versionId,
                                  String code, String message) {
            this.key = objectName;
            this.versionId = versionId;
            this.code = code;
            this.message = message;
        }

        private String key;
        private String versionId;
        private String code;
        private String message;

        public String getKey() {
            return key;
        }

        public void setKey(String key) {
            this.key = key;
        }

        public String getVersionId() {
            return versionId;
        }

        public void setVersionId(String versionId) {
            this.versionId = versionId;
        }

        public String getCode() {
            return code;
        }

        public void setCode(String code) {
            this.code = code;
        }

        public String getMessage() {
            return message;
        }

        public void setMessage(String message) {
            this.message = message;
        }
    }

}
