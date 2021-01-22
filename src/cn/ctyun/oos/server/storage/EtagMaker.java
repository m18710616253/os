package cn.ctyun.oos.server.storage;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import common.util.HexUtils;

/**
 * @author: Jiang Feng
 */
public class EtagMaker implements Signer {
    MessageDigest DIGESTER;
    
    public EtagMaker() {
        try {
            DIGESTER = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }
    
    @Override
    public void sign(byte[] data, int offset, int len) {
        DIGESTER.update(data, offset, len);
        // System.out.println(HexUtils.toHexString(DIGESTER.digest()));
    }
    
    public String digest() {
        return HexUtils.toHexString(DIGESTER.digest());
    }
}
