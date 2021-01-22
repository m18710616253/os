package cn.ctyun.oos.server.processor.image.thumbnailator.watermark;

import java.awt.Font;
import java.awt.FontFormatException;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import cn.ctyun.oos.server.processor.image.ImageProcessorConsts;



public class SupportedFontNames {
    
    private static final Map<String,String> map = new HashMap<String,String>();
    private static final Log LOG = LogFactory.getLog(SupportedFontNames.class);
    private static final Map<String,Font> cache = new ConcurrentHashMap<String,Font>();
    /**
     *  wqy-zenhei   文泉驿正黑   d3F5LXplbmhlaQ
        wqy-microhei    文泉微米黑   d3F5LW1pY3JvaGVp
        fangzhengshusong    方正书宋    ZmFuZ3poZW5nc2h1c29uZw
        fangzhengkaiti  方正楷体    ZmFuZ3poZW5na2FpdGk
        fangzhengheiti  方正黑体    ZmFuZ3poZW5naGVpdGk
        fangzhengfangsong   方正仿宋    ZmFuZ3poZW5nZmFuZ3Nvbmc
        droidsansfallback   DroidSansFallback
     */
    static {
        //文泉驿正黑
        map.put("wqy-zenhei", "wqyzenhei.ttf");
        //文泉微米黑
        map.put("wqy-microhei", "wqymicrohei.ttc");
        //方正书宋
        map.put("fangzhengshusong", "fondershusongjianti.ttf");
        //方正楷体
        map.put("fangzhengkaiti", "fonderkaiti.ttf");
        //方正黑体
        map.put("fangzhengheiti", "fonderheiti.ttf");
        //方正仿宋
        map.put("fangzhengfangsong", "fonderfangsong.ttf");
        //DroidSansFallback
        map.put("droidsansfallback", "DroidSansFallback.ttf");
        //徐静蕾体
        //map.put("xjl", "xjlFont.fon");
    }
    
    public static boolean isSupport(String fontName){
        
        return map.containsKey(fontName);
    }
    
    public static Font loadFont(String  fontName,int style,int fontSize) throws IOException{
        
        String key = fontName;
        Font nf = null;
        if(cache.containsKey(key)){
            nf = cache.get(key);
        }else{
            //. 表示当前class目录
            String fileName = map.get(fontName);
            if(fileName ==null){
                return null;
            }
            String path = ImageProcessorConsts.DEFAULT_FONT_FOLDER+fileName;
            try {
                nf = Font.createFont(Font.TRUETYPE_FONT, new File(path));
                cache.put(fileName, nf);
            } catch (FontFormatException e) {
                LOG.error("load font fail! font name:"+fontName);
                return null;
            }
        }
        nf = nf.deriveFont(style, fontSize);
        return nf;
        
    }
}
