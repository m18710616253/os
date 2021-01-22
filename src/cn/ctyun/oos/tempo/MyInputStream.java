package cn.ctyun.oos.tempo;

import java.io.IOException;
import java.io.InputStream;
import java.util.Random;

class MyInputStream extends InputStream {
    int pos = 0;
    final Random r = new Random();
    long length = 0;

    MyInputStream(long length) {
        this.length = length;
    }

    @Override
    public int read() throws IOException {
        // return (pos++ >= length) ? -1
        // : (r.nextInt() & 0xFF);
        if (length <= 0)
            return 0;
        else
            return (pos++ >= length) ? -1 : 0;
    }
}
