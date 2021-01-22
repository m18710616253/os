package cn.ctyun.oos.server.processor;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import cn.ctyun.common.Consts;
import cn.ctyun.oos.server.conf.ContentSecurityConfig;
import cn.ctyun.oos.server.processor.Processor.ProcessorException;
import cn.ctyun.oos.server.processor.image.antispam.NeteaseDun;
import common.tuple.Pair;

public class TextProcessor {
    private static final Log log = LogFactory.getLog(TextProcessor.class);
    private static final long MAX_MESSAGE_LENGTH = 10000;
    private static final long MAX_FILE_SIZE = 1L*1024*1024;
    /**
     * 文本反垃圾方法
     * @param input
     * @param objectName
     * @param objSize
     * @param params
     * @return
     * @throws Exception
     */
    public static Pair<Long, InputStream> process2Stream(InputStream input, String objectName, long objSize,
            String params) throws Exception {
        if(objSize > MAX_FILE_SIZE)
            throw new ProcessorException(400, "Too big file. size:" + objSize);
        InputStream in = null;
        long size = 0;
        String text = convertStreamToString(input);
        String response = NeteaseDun.decodeText(ContentSecurityConfig.secretIdNeteaseDun, ContentSecurityConfig.textBusinessIdNeteaseDun, ContentSecurityConfig.getSecretKey(), text);
        
        byte[] b = response.getBytes(Consts.CS_UTF8);
        in = new ByteArrayInputStream(b);
        size = b.length;
        log.info("TextAntiSpam messageLenth:" + b.length+" message:"+response);
        
        return new Pair<>(size, in);
    }
    
    public static String convertStreamToString(InputStream is) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(is,Consts.CS_UTF8));
        StringBuilder sb = new StringBuilder();
        String line = null;
        try {
            while ((line = reader.readLine()) != null) {
                sb.append(line).append("\n");
                if(sb.length()>MAX_MESSAGE_LENGTH){
                    break;
                }
            }
        } finally {
            try {
                is.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return sb.toString();
    }
    
    public static void main(String[] args){
        File file = new File("d:\\temp\\antispam.txt");
        try {
            Pair<Long, InputStream> pair = TextProcessor.process2Stream(new FileInputStream(file), "antispam.txt", file.length(), "");
            System.out.println(pair.first());
            byte[] bts = new byte[8192];
            InputStream in = pair.second();
            int len = in.read(bts);
            System.out.println(new String(bts,0,len));
            in.close();
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
