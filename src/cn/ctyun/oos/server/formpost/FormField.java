package cn.ctyun.oos.server.formpost;
/**
 * 用于保存读取到的表单字段
 * @author zhaowentao
 *
 */
public class FormField {
    int innerId;
    //原始的内容，如：Content-Disposition: form-data; name="policy"
    String rawHead;
    //字段名称
    String fieldName;
    //文件名称，如果是文件，那么就是文件名称，否则留空
    String fileName;
    //字段的值，如果是文件留空
    String value="";
    public FormField() {
    }
    public FormField(String fieldName, String value) {
        this(fieldName, null, value);
    }
    public FormField(String fieldName, String fileName, String value) {
        super();
        this.fieldName = fieldName;
        this.fileName = fileName;
        this.value = value;
    }
    public boolean isFileField(){
//        return (fileName != null && fileName.trim().length()>0);
        return "file".equalsIgnoreCase(fieldName);
    }
    
    
    @Override
    public String toString() {
        return "FormField [fieldName=" + fieldName + ", fileName=" + fileName
                + ", value=" + value + ", innerId=" + innerId + ", rawHead=" + rawHead + "]";
    }

    public String getFieldName() {
        return fieldName;
    }
    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }
    public String getFileName() {
        return fileName;
    }
    public void setFileName(String fileName) {
        this.fileName = fileName;
    }
    public String getValue() {
        return value;
    }

}
