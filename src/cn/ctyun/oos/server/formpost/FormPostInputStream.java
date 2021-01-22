package cn.ctyun.oos.server.formpost;

import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.lang3.tuple.MutablePair;

import cn.ctyun.common.BaseException;
import cn.ctyun.common.Consts;
import cn.ctyun.common.MultipartInputStream;
import cn.ctyun.oos.server.formpost.exceptions.MetadataTooLargeException;
import common.threadlocal.ThreadLocalBytes;

public class FormPostInputStream  extends InputStream {
    MultipartInputStream input;
    FormPostHeaderNotice notice;
    byte[] headerBuffer = new byte[Consts.MAX_FORM_POST_FIELDS_LENGTH];
    public FormPostInputStream(MultipartInputStream input, FormPostHeaderNotice notice){
        this.input = input;
        this.notice = notice;
    }
//    public void readHeaders() throws IOException{
//        input.mark(1);
//        input.read();
//        input.reset();
//    }
    @Override
    public int read() throws IOException {
        int v = input.read();
        if(notice.currentField!=null && ! notice.currentField.isFileField()){
            while(! notice.currentField.isFileField() && !input.end){
                FormField formField = notice.currentField;
                MutablePair<String, Integer> pair = getValue(formField.innerId, v);
                formField.value=pair.left;
                v = pair.right;
            }
        }
        return v;
    }
    private MutablePair<String, Integer> getValue(int num, int v)
            throws IOException{
        int i = 0;
        MutablePair<String, Integer> p = new MutablePair<String, Integer>();
        while (!notice.currentField.isFileField() && notice.currentField.innerId == num) {
            if (v == -1)
                break;
            if(i>=headerBuffer.length){
                //单个字段长度超限
                throw new MetadataTooLargeException("MetadataTooLarge","The metadata size should be less than "+headerBuffer.length,headerBuffer.length);
            }
            headerBuffer[i] = (byte) v;
            i++;
            v = input.read();
        }
        notice.headerLength += i;
        if( notice.headerLength > Consts.MAX_FORM_POST_FIELDS_LENGTH){
            //文件前面字段总长度超限
            throw new MetadataTooLargeException("MaxPostPreDataLengthExceeded","Your POST request fields preceeding the upload file was too large. allowed max size is "+Consts.MAX_FORM_POST_FIELDS_LENGTH,Consts.MAX_FORM_POST_FIELDS_LENGTH);
        }
        p.left = new String(headerBuffer, 0, i, Consts.CS_UTF8);
        p.right = v;
        return p;
    }
}
