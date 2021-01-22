package cn.ctyun.oos.tempo.disktest;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import common.time.TimeStat;

public class CRCUtils {
    private static Log log = LogFactory.getLog(CRCUtils.class);

    /**
     * 获取文件的CRC32校验和
     * 
     * @param file
     *            校验的文件
     * @return 校验和
     * @throws IOException
     */
    public static long getCRC32Checksum(File file) throws IOException {
        CheckedInputStream cis = null;
        long checksum = 0;
        try {
            cis = new CheckedInputStream(new FileInputStream(file), new CRC32());
            byte[] buffer = new byte[64];
            while (cis.read(buffer) != -1)
                ;
            checksum = cis.getChecksum().getValue();
        } finally {
            if (null != cis) {
                cis.close();
            }
        }
        return checksum;
    }

    /**
     * file的大小为pageSize(128k),不能太大
     * @param pageBytes
     * @param stat TODO
     * 
     * @return
     * @throws IOException
     */
    public static long getCRC32ChecksumAndContent(File file, byte[] pageBytes, TimeStat stat)
            throws IOException {
        CheckedInputStream cis = null;
        long checksum = 0;
        try {
            int length = (int) file.length();
            assert length <= pageBytes.length;
            long openS = System.currentTimeMillis();
            cis = new CheckedInputStream(new FileInputStream(file), new CRC32());
            stat.record("get_openfile", System.currentTimeMillis()-openS);
            int alreadyRead = 0;
            while (alreadyRead < length) {
                int r = cis.read(pageBytes, alreadyRead, length - alreadyRead);
                if (r == -1) {
                    break;
                }
                alreadyRead += r;
            }
            if (alreadyRead < length) {
                log.error("early eof when read object");
                throw new IOException("early eof when read object");
            }
            checksum = cis.getChecksum().getValue();
        } finally {
            if (null != cis) {
                cis.close();
            }
        }
        return checksum;
    }

    /**
     * file的大小为pageSize(128k),不能太大
     * @param pageBytes
     * @return
     * @throws IOException
     */
    public static long getCRC32ChecksumAndContent(File file, ByteBuffer pageBytes) throws IOException {
        CheckedInputStream cis = null;
        long checksum = 0;
        try {
            int length = (int) file.length();
            assert length <= pageBytes.limit();
            cis = new CheckedInputStream(new FileInputStream(file), new CRC32());
            int alreadyRead = 0;
            while(alreadyRead < length){
                int r = cis.read(pageBytes.array(), alreadyRead, length-alreadyRead);
                if(r == -1){
                    break;
                }
                alreadyRead += r;
            }
            if(alreadyRead < length){
                log.error("early eof when read object");
                throw new IOException("early eof when read object");
            }
            checksum = cis.getChecksum().getValue();
        } finally{
            if(null != cis){
                cis.close();
            }
        }
        return checksum;
    }
}