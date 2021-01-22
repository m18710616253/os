package cn.ctyun.oos.common;

import java.io.IOException;
import java.io.InputStream;

public class BandWidthControlInputStream extends InputStream {
    private InputStream input;
    private String ownerName;
    private String type;
    
    public BandWidthControlInputStream(InputStream input, String ownerName, String type) {
        this.input = input;
        this.ownerName = ownerName;
        this.type = type;
    }

    @Override
    public int read() throws IOException {
        throw new IOException("not support read only a byte");
    }
    
    @Override
    public int read(byte[] b, int offset, int len) throws IOException {
        int res = 0;
        res = input.read(b, offset, len);
        UserBandWidthLimit.increaseFlow(ownerName, type, res);
        return res;
    }
    
    @Override
    public void close() throws IOException {
        input.close();
        UserBandWidthLimit.release(ownerName, type);
    }

}
