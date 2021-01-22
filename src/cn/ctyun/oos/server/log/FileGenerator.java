package cn.ctyun.oos.server.log;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import cn.ctyun.common.Consts;


/**
 * 文件生成类主要功能：
 * 1 按照指定大小进行输出，并添加_n的后缀
 * 2 先输出临时文件，最后close统一去掉tmp后缀，防止中途退出，残留文件不知道该如何处理。
 * @author xiaojun
 *
 */
public class FileGenerator {

    private static Log log = LogFactory.getLog(FileGenerator.class);
    FileOutputStream fos = null;  // 当前的文件输出流
    public static String MORE_SEP = "-";  // 临时文件后缀
    private String fileName = ""; // 原始文件名
    private File parentFile;          // 父目录
    private int fileSize = 0;         // 当前文件大小
    private int fileSizeLimit = 100*1024*1024;  // 文件切分大小，默认100M
    private int fileCount = 0;         // 文件数量
    /**
     * 
     * @param parent   父目录
     * @param child     文件名
     * @param cutSize  文件分割大小
     * @throws FileNotFoundException
     */
    public FileGenerator(File parent, String child, int cutSize) throws FileNotFoundException {
        fos = new FileOutputStream( new File(parent, child + Consts.SUFFIX_TMP));
        fileCount ++;
        this.fileName = child;
        parentFile = parent;
        if(cutSize >= 1 && cutSize <= 1024) { //1M 到 1G之间，默认100M
           this.fileSizeLimit = cutSize*1024*1024;
        }else {
            log.warn("file cut size is invliad. must in 1MB~1024MB. default 100MB. now size: " + String.valueOf(cutSize) + "MB");
        }
    }
    /**
     * @param b   写入byte数组
     * @throws IOException
     */
    public void write(byte b[]) throws IOException{
        if(fileSize + b.length > fileSizeLimit) {   // 如果超出限制，需要将之前的文件关闭
            closeCurrentChild();
            // 如果是第一个文件添加_1 后缀
            if(1 == fileCount){  // 修改第一个文件为_1
                File f = new File(parentFile, fileName + Consts.SUFFIX_TMP);
                if(!f.renameTo(new File(parentFile, fileName + MORE_SEP + fileCount +  Consts.SUFFIX_TMP))) {
                    log.error("FileGenerator failed to rename " + f.getAbsolutePath());
                }
            }
            // 打开新的文件输出流，使用_N+1
            fileCount ++;
            fos = new FileOutputStream(new File(parentFile, fileName + MORE_SEP +fileCount + Consts.SUFFIX_TMP));
            fileSize = 0;
        }
        fos.write(b);
        fileSize += b.length;
    }
    /**
     * @throws IOException
     */
    public void close() throws IOException{
        // 关闭当前流
        closeCurrentChild();
        // 去掉所有child文件tmp后缀
        if(1 == fileCount) {  // 只有一个child文件
            File f = new File(parentFile, fileName + Consts.SUFFIX_TMP);
            f.renameTo(new File(parentFile, fileName));       
        }else { // 多个child文件
            for(int i = 0; i < fileCount; i++) {
                File f = new File(parentFile, fileName + MORE_SEP + (i+1) + Consts.SUFFIX_TMP);
                if(!f.renameTo( new File(parentFile, fileName + MORE_SEP + (i+1) ))) {
                    log.error("FileGenerator failed to rename " + f.getAbsolutePath());
                }
            }
        }
    }
    /**
     * @throws IOException
     */
    private void closeCurrentChild() throws IOException {
        fos.close();
    }
   
    
     

}

