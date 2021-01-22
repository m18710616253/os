package cn.ctyun.oos.server;

import cn.ctyun.common.Consts;
import cn.ctyun.common.MultipartInputStream.HeaderNoticeable;

public class NotifyGW implements HeaderNoticeable {
    public boolean status = false;
    public int name = 0;
    public static final int FILENAME = 1;
    public static final int OFFSET = 2;
    public static final int LENGTH = 3;
    public static final int DATA = 4;
    public static final int SUBMIT = 5;
    
    @Override
    public void notice(byte[] header, int offset, int count) {
        String str = new String(header, offset, count, Consts.CS_UTF8);
        status = false;
        if (str.contains("name=\"Filename\"") || 
                str.contains("name=\"name\"")) {
            name = FILENAME;
            status = false;
        } else if (str.contains("name=\"Offset\"")) {
            name = OFFSET;
            status = false;
        } else if (str.contains("name=\"Length\"")) {
            name = LENGTH;
            status = false;
        } else if (str.contains("name=\"Upload\"")) {
            name = SUBMIT;
        } else
            status = true;
    }
}