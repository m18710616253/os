package cn.ctyun.oos.common;

import java.io.IOException;
import java.io.InputStream;

import cn.ctyun.oos.server.storage.Signer;

public class SignableInputStream extends InputStream {
    private InputStream input;
    private long length;
    private Signer signer;
    public long count;

    public SignableInputStream(InputStream input, long length, Signer signer) {
        this.input = input;
        this.length = length;
        this.signer = signer;
    }

    @Override
    public int read() throws IOException {
        throw new IOException("not support read only a byte");
    }

    @Override
    public int read(byte[] b, int offset, int len) throws IOException {
        int res = 0;
        res = input.read(b, offset, len);
        if (res > 0 && signer != null)
            signer.sign(b, offset, res);
        count += res;
        return res;
    }

    @Override
    public void close() throws IOException {
        input.close();
    }

    public long getLength() {
        return length;
    }
}
