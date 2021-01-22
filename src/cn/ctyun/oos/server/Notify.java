package cn.ctyun.oos.server;

import java.util.LinkedList;

import com.amazonaws.services.s3.Headers;

import cn.ctyun.common.Consts;
import cn.ctyun.common.MultipartInputStream.HeaderNoticeable;

public class Notify implements HeaderNoticeable {
    public boolean status = false;
    public int name = 0;
    public static final int KEY = 1;
    public static final int CONTENTTYPE = 2;
    public static final int ACCESSKEYID = 3;
    public static final int SIGNATURE = 4;
    public static final int SUBMIT = 5;
    public static final int CACHE_CONTROL = 7;
    public static final int CONTENT_DISPOSITION = 8;
    public static final int CONTENT_ENCODING = 9;
    public static final int SUCCESS_ACTION_STATUS = 10;
    public static int METADATA = 11;
    public LinkedList<String> meta = new LinkedList<String>();
    
    @Override
    public void notice(byte[] header, int offset, int count) {
        String str = new String(header, offset, count, Consts.CS_UTF8);
        status = false;
        if (str.contains("name=\"Filename\"") || 
                str.contains("name=\"name\"")) {
            name = KEY;
        } else if (str.contains("name=\"" + Headers.CONTENT_TYPE + "\"")) {
            name = CONTENTTYPE;
        } else if (str.contains("name=\"AWSAccessKeyId\"")) {
            name = ACCESSKEYID;
        } else if (str.contains("name=\"Signature\"")) {
            name = SIGNATURE;
        } else if (str.contains("name=\"Upload\"")) {
            name = SUBMIT;
        } else if (str.contains("name=\"" + Headers.CACHE_CONTROL + "\"")) {
            name = CACHE_CONTROL;
        } else if (str.contains("name=\"" + Headers.CONTENT_DISPOSITION + "\"")) {
            name = CONTENT_DISPOSITION;
        } else if (str.contains("name=\"" + Headers.CONTENT_ENCODING + "\"")) {
            name = CONTENT_ENCODING;
        } else if (str.contains("name=\"" + Headers.S3_USER_METADATA_PREFIX)) {
            name = METADATA + 1;
            METADATA++;
            meta.add(str.split("\"")[1]);
        } else if (str.contains("name=\"success_action_status\"")) {
            name = SUCCESS_ACTION_STATUS;
        } else
            status = true;
    }
}