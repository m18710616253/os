package cn.ctyun.oos.server.storage;

/**
 * @author: Jiang Feng
 */
public interface Signer {
    void sign(byte[] data, int offset, int len);
}
