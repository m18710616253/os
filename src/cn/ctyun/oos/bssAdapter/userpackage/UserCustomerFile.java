package cn.ctyun.oos.bssAdapter.userpackage;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import common.tuple.Pair;

/**
 * 表示一个将要被实例化的话单文件，该文件内容不超过指定的行数和大小；
 */
public class UserCustomerFile {
    private static final Log log = LogFactory.getLog(UserCustomerFile.class);
    
    private String FILE_NAME_PRIFIX = "OOS_UserPackage_";
    private int MAX_FILE_SIZE = 1 * 1024 * 1024;
    private int MAX_FILE_ROW = 1 * 1000;
    private String dateSuffix = "";
    private List<Pair<String,Integer>> fileNameList = new ArrayList<Pair<String,Integer>>();
    private Object lock = new Object();
    
    private String fileName;
    private String root;
    private RandomAccessFile outputFile = null;
    
    // 当前文件长度;
    private AtomicLong currentFileSize = new AtomicLong(0);
    // 当前文件行数；
    private AtomicLong currentFileRow = new AtomicLong(1);

    private static DateTimeFormatter format = DateTimeFormatter.ofPattern("yyyyMMddHHmm");
    
    public UserCustomerFile init(String tmpDir,boolean clearFileList,int max_line,
            int max_size,String fileNamePrifix,String fileNameSuffix) throws IOException {
        this.FILE_NAME_PRIFIX = fileNamePrifix;
        this.MAX_FILE_ROW = max_line;
        this.MAX_FILE_SIZE = max_size;
        this.dateSuffix = fileNameSuffix.length()==0?"":"_"+fileNameSuffix;
        
        return this.init(tmpDir, clearFileList);
    }
    
    public UserCustomerFile init(String tmpDir,boolean clearFileList) throws IOException {
        if (outputFile != null) close();
        if (clearFileList) fileNameList.clear();
        
        root = tmpDir;
        renameFile();
        currentFileSize.set(0);
        currentFileRow.set(1);
        
        File file = new File(getFilePath());
        
        if (!file.getParentFile().exists()) {
            file.getParentFile().mkdirs();
        }
        
        while (file.exists()) {
            renameFile();
            file = new File(getFilePath());
        }
         
        outputFile = new RandomAccessFile(file, "rw");
        
        return this;
    }

    public synchronized void renameFile() {
        this.fileName = FILE_NAME_PRIFIX + LocalDateTime.now().format(format)
                + ThreadLocalRandom.current().nextInt(10, 99)+ this.dateSuffix + ".csv";
    }

    public synchronized String getFileName() {
        return fileName;
    }

    public String getFilePath() {
        if(root.endsWith(File.separator)) {
            return root+getFileName();
        }
        return root+File.separator+getFileName();
    }

    public boolean addMsg(String e) throws Exception {
        synchronized(lock) {
            if ((this.currentFileRow.get() > MAX_FILE_ROW)
                || ((this.currentFileSize.get()
                        + e.length()) >= MAX_FILE_SIZE)) {
                fileNameList.add(new Pair(this.getFilePath(),this.currentFileSize));
                this.init(this.root,false);
            }
    
            currentFileRow.incrementAndGet();
            currentFileSize.addAndGet(e.length());
            
            outputFile.writeBytes(e);
        }
        
        return true;
    }
    
    public boolean addMsgWhitFileName(String e) throws Exception {
        synchronized(lock) {
            if ((this.currentFileRow.get() > MAX_FILE_ROW)
                || ((this.currentFileSize.get()
                        + e.length()) >= MAX_FILE_SIZE)) {
                fileNameList.add(new Pair(this.getFilePath(),this.currentFileSize));
                this.init(this.root,false);
            }
            e = getFileName()+"-"+currentFileRow.get()+"|"+ e;
            
            currentFileRow.incrementAndGet();
            currentFileSize.addAndGet(e.length());
            outputFile.writeBytes(e);
        }
        
        return true;
    }

    public long getFileRows() {
        return this.currentFileRow.get();
    }

    public boolean isFull() {
        return this.currentFileRow.get() == MAX_FILE_ROW
                || this.currentFileSize.get() >= MAX_FILE_SIZE;
    } 
    
    public void close() throws IOException {
        if (outputFile != null) {
            outputFile.close();
            outputFile = null;
        }
    }
    
    //result:文件路径，文件大小
    public List<Pair<String,Integer>> getResultFileList(){
        if (!fileNameList.contains(this.getFilePath())) {
            fileNameList.add(new Pair(this.getFilePath(),this.currentFileSize));
        }
        return fileNameList;
    }
}
