package cn.ctyun.oos.server.processor.image;

import java.awt.Color;

import cn.ctyun.oos.server.processor.image.thumbnailator.watermark.MixWatermark;

/**
 * 图片处理的常量
 * @author jiazg
 *
 */
public class ImageProcessorConsts {

    public static final int MAX_PIPELINE = 4;
    public static final int MAX_IMAGE_SIZE = 20 * 1024 * 1024;
    public static final int DEFAULT_POSITION = 9;
    public static final int DEFAULT_WATERMARK_IMAGE_TRANSPARENT = 100;
    public static final int DEFAULT_WATERMARK_FONT_SHADOW_TRANSPARENT = 50;
    public static final int DEFAULT_MARGIN_X = 10;
    public static final int DEFAULT_MARGIN_Y = 10;
    public static final int DEFAULT_VOFFSET = 0;
    //public static final Color DEFAULT_WATERMARK_FONT_COLOR = new Color(255,255,255,115);
    public static final Color DEFAULT_WATERMARK_FONT_COLOR = new Color(0,0,0,200);
    public static final Color DEFAULT_WATERMARK_FONT_SHADOW_COLOR = new Color(30, 30, 30);   //new Color(255, 0, 0);// new Color(170, 170, 170, 77);   
    public static final int DEFAULT_WATERMARK_FONT_SIZE = 40;   
    public static final MixWatermark.ORDER DEFAULT_IMAGE_TEXT_ORDER = MixWatermark.ORDER.ORDER_IMAGE_TEXT;   
    public static final MixWatermark.ALIGN DEFAULT_IMAGE_TEXT_ALIGN = MixWatermark.ALIGN.ALIGN_TOP;    
    public static final LargeMode DEFAULT_ENLARGE_MODE = LargeMode.Enlarge;
    public static final EdgeMode DEFAULT_EDGE_MODE = EdgeMode.NONE;
    public static final int DEFAULT_IMAGE_TEXT_INTERVAL = 10;
    public static final int MAX_EDGE_LENGTH = 4096;
    public final static int DEFAULT_PERCENTAGE = 100;
    public final static int DEFAULT_DEST_WIDTH_LENGTH= 0;
    public final static int DEFAULT_DEST_HEIGHT_LENGTH = 0;
    public final static int MAX_DES_EDGE_LENGHT = 4096*4;
    public final static int MAX_DES_SQUARE = 4096*4096;
    //默认字体目录
    public static final String DEFAULT_FONT_FOLDER = System.getenv("OOS_HOME") + "/resource/font/";
    //方正黑体
    public static final String DEFAULT_FONT_NAME = "fangzhengheiti";
    //水印最多支持31个文字
    public final static int MAX_WATERMARK_DECODED_TEXT_LENGTH = 64;
    public static final Color DEFAULT_TRANSPARENT_TRANS_COLOR = Color.BLACK;
    //GIF合并最多支持的图片数量
    public static final int MAX_MERGE_IMAGE_COUNT = 19;
    /**
     * 长度模式
     * @author zhaowentao
     *
     */
    public enum EdgeMode {
        //短边模式
        SHORT,
        //长边模式
        LONG,
        //强制按照指定的宽度和高度
        FORCE,
        //用户未显式指定
        NONE
    }
    /**
     * 是否扩大原图
     * @author zhaowentao
     *
     */
    public enum LargeMode {
        //如果缩放目标超过原图，保持原样
        Normal,
        //如果缩放目标超过原图，放大原图
        Enlarge,

    }
    /**
     * 是否执行自动裁剪
     * @author zhaowentao
     *
     */
    public enum CutMode{
        
        //不进行裁剪
        NOT_CUT,
        //执行自动裁剪
        DO_CUT,
        //未指定
        NONE;
    }
    /**
     * PNG转其他图片时透明颜色的处理方式
     * @author zhaowentao
     *
     */
    public enum TransparentTransMode{
        //透明部分转为黑色
        BLACK,
        //透明部分转为白色
        WHITE;
    }
    
    /**
     * 图像处理类型
     * @author jiazg
     *
     */
    public enum OPT_TYPE {
        IMAGE, INFO, WATER_MARK, PORN, MERGE_GIF;
    }
}
