package cn.ctyun.oos.server;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.commons.lang3.tuple.MutablePair;

import cn.ctyun.common.Consts;
import cn.ctyun.common.MultipartInputStream;

public class SinglepartInputStream extends InputStream {
    MultipartInputStream input;
    Notify notify;
    public String key;
    public String contentType;
    public String accessKeyId;
    public String signature;
    public String cacheControl;
    public String contentEncoding;
    public Properties metadata = new Properties();
    public String contentDisposition;
    public int status;
    private int metanum = 0;
    
    public SinglepartInputStream(MultipartInputStream input, Notify notify) {
        this.input = input;
        this.notify = notify;
    }
    
    private MutablePair<String, Integer> getValue(int num, int v)
            throws IOException {
        int i = 0;
        byte[] b = new byte[1024];
        int name = 0;
        MutablePair<String, Integer> p = new MutablePair<String, Integer>();
        name = notify.name;
        while (!notify.status && name == num) {
            if (v == -1)
                break;
            b[i] = (byte) v;
            i++;
            v = input.read();
            name = notify.name;
        }
        p.left = new String(b, 0, i, Consts.CS_UTF8);
        p.right = v;
        return p;
    }
    
    public int read() throws IOException {
        int v = 0;
        v = input.read();
        MutablePair<String, Integer> p = null;
        // 当status==true时，则不再处理数据，仅仅是把流里面的数据读完而已
        while (!notify.status) {
            if (notify.name == Notify.KEY) {
                p = getValue(notify.name, v);
                key = p.left;
            } else if (notify.name == Notify.CONTENTTYPE) {
                p = getValue(notify.name, p.right);
                contentType = p.left;
            } else if (notify.name == Notify.ACCESSKEYID) {
                p = getValue(notify.name, p.right);
                accessKeyId = p.left;
            } else if (notify.name == Notify.SIGNATURE) {
                p = getValue(notify.name, p.right);
                signature = p.left;
            } else if (notify.name == Notify.CACHE_CONTROL) {
                p = getValue(notify.name, p.right);
                cacheControl = p.left;
            } else if (notify.name == Notify.CONTENT_ENCODING) {
                p = getValue(notify.name, p.right);
                contentEncoding = p.left;
            } else if (notify.name == Notify.CONTENT_DISPOSITION) {
                p = getValue(notify.name, p.right);
                contentDisposition = p.left;
            } else if (notify.name == Notify.METADATA) {
                p = getValue(notify.name, p.right);
                metadata.setProperty(notify.meta.get(metanum), p.left);
                metanum++;
            } else if (notify.name == Notify.SUCCESS_ACTION_STATUS) {
                p = getValue(notify.name, p.right);
                status = Integer.parseInt(p.left);
            } else if (notify.name == Notify.SUBMIT) {
                p = getValue(notify.name, v);
            }
            if (p != null && p.right == -1)
                break;
        }
        if (p != null)
            return p.right;
        else
            return v;
    }
}