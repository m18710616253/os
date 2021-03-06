package cn.ctyun.oos.server.storage;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;

/**
 * Wraps the HTTP input stream and connection object so that when the stream is
 * closed the connection can be closed as well.
 * 
 */
public class HttpInputStreamWrapper extends InputStream {
    private InputStream in;
    private HttpURLConnection con;
    
    public HttpInputStreamWrapper(InputStream in, HttpURLConnection con) {
        this.in = in;
        this.con = con;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see java.io.InputStream#available()
     */
    @Override
    public int available() throws IOException {
        return in.available();
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see java.io.InputStream#close()
     */
    @Override
    public void close() throws IOException {
        try {
            in.close();
        } finally {
            con.disconnect();
        }
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see java.io.InputStream#mark(int)
     */
    @Override
    public synchronized void mark(int readlimit) {
        in.mark(readlimit);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see java.io.InputStream#markSupported()
     */
    @Override
    public boolean markSupported() {
        return in.markSupported();
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see java.io.InputStream#read()
     */
    @Override
    public int read() throws IOException {
        return in.read();
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see java.io.InputStream#read(byte[], int, int)
     */
    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        return in.read(b, off, len);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see java.io.InputStream#read(byte[])
     */
    @Override
    public int read(byte[] b) throws IOException {
        return in.read(b);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see java.io.InputStream#reset()
     */
    @Override
    public synchronized void reset() throws IOException {
        in.reset();
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see java.io.InputStream#skip(long)
     */
    @Override
    public long skip(long n) throws IOException {
        return in.skip(n);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object obj) {
        return in.equals(obj);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        return in.hashCode();
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return in.toString();
    }
}
