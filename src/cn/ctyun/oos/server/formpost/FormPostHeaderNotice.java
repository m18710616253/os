package cn.ctyun.oos.server.formpost;

import java.io.File;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import cn.ctyun.common.MultipartInputStream.HeaderNoticeable;
import cn.ctyun.oos.server.formpost.PolicyManager.Policy;


public class FormPostHeaderNotice implements HeaderNoticeable {
    private static final Log log = LogFactory.getLog(FormPostHeaderNotice.class);
    
    private Map<String, FormField> fieldMap = null;
    private List<FormField> fieldList = new LinkedList<>();
    private int boundaryLength=0;
    int fileCount = 0;
    FormField currentField;
    int currentId=0;
    long headerLength;
    
    public Policy policy;
    public long fileLength;
    
    public FormPostHeaderNotice(int boundaryLength){
        this.boundaryLength = boundaryLength;
    }
    
    @Override
    public void notice(byte[] header, int offset, int count) {
        currentId++;
        String headerStr = new String(header,offset,count);
        FormField field = new FormField();
        currentField = field;
        headerLength += (count+boundaryLength+4);//\r\n\r\n
        field.innerId =currentId;
        field.rawHead = headerStr;
        int namePos = headerStr.indexOf("name=\"");
        if (namePos < 0) {
            log.error("invalid form body.");
            return;
        }
        String afterName = headerStr.substring(namePos + "name=\"".length());
        int nameEndPos = afterName.indexOf("\"");
        if (nameEndPos < 0) {
            log.error("invalid form body.");
            return;
        }
        field.fieldName = afterName.substring(0, nameEndPos);
        String afterKey = afterName.substring(nameEndPos);
        int fileNamePos = afterKey.indexOf("; filename=\"");
        if (fileNamePos > 0) {
            int beginPos = fileNamePos + "; filename=\"".length();
            int endPos = afterKey.indexOf("\"", beginPos);
            if (endPos < 0) {
                log.error("invalid form body.");
                return;
            }
            field.fileName = new File(afterKey.substring(beginPos, endPos))
                    .getName();
            
        }
        fieldList.add(field);
        if("file".equalsIgnoreCase(field.fieldName)){
            fileCount++;
        }
    }
    
    
    /**
     * 获取FormBody除了fileContent的长度。
     * content-length = fileLength + getHeaderTotalLength
     * 注意：在读取完flieContent 后才能完整的读完Header,否则返回的是当前已经读取的长度，后面的字段没有读取到
     * 
     * @return
     */
    public long getHeaderTotalLength(){
        return headerLength +4;//--\r\n
    }

    public Map<String, FormField> getFieldMap() {
        if(fieldMap == null){
            fieldMap = new HashMap<>();
            //将list中的值放入map
            for(FormField f:fieldList){
                FormField oldField = fieldMap.put(f.fieldName.toLowerCase(), f);
                //多个相同名称的拼接起来
                if(oldField!=null){
                    f.value =oldField.getValue()+","+f.getValue();
                }
                if("file".equalsIgnoreCase(f.fieldName)){
                    break;
                }
            }
        }
        return fieldMap;
    }

    public List<FormField> getFieldList() {
        return fieldList;
    }
    
}
