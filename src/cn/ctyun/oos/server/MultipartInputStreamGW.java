package cn.ctyun.oos.server;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;

import org.apache.commons.lang3.tuple.MutablePair;

import cn.ctyun.common.Consts;
import cn.ctyun.common.MultipartInputStream;

public class MultipartInputStreamGW extends InputStream {
    MultipartInputStream input;
    NotifyGW NotifyGW;
    public ArrayList<String> fileName = new ArrayList<String>();
    public ArrayList<String> offset = new ArrayList<String>();
    public ArrayList<String> length = new ArrayList<String>();
    public String data;
    public int status;
    
    public MultipartInputStreamGW(MultipartInputStream input, NotifyGW NotifyGW) {
        this.input = input;
        this.NotifyGW = NotifyGW;
    }
    
    private MutablePair<String, Integer> getValue(int num, int v)
            throws IOException {
        int i = 0;
        byte[] b = new byte[1024];
        int name = 0;
        MutablePair<String, Integer> p = new MutablePair<String, Integer>();
        name = NotifyGW.name;
        while (!NotifyGW.status && name == num) {
            if (v == -1)
                break;
            b[i] = (byte) v;
            i++;
            v = input.read();
            name = NotifyGW.name;
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
        while (!NotifyGW.status) {
            if (NotifyGW.name == NotifyGW.FILENAME) {
                p = getValue(NotifyGW.name, v);
                fileName.add(p.left);
            } else if (NotifyGW.name == NotifyGW.OFFSET) {
                p = getValue(NotifyGW.name, p.right);
                offset.add(p.left);
            } else if (NotifyGW.name == NotifyGW.LENGTH) {
                p = getValue(NotifyGW.name, p.right);
                length.add(p.left);
            } else if (NotifyGW.name == NotifyGW.SUBMIT) {
                p = getValue(NotifyGW.name, v);
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