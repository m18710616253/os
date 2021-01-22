package cn.ctyun.oos.server.processor.image.util;

import java.awt.Color;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import cn.ctyun.oos.server.processor.Processor.ProcessorException;
import cn.ctyun.oos.server.processor.image.ImageEvent;
import cn.ctyun.oos.server.processor.image.ImageEvent.ImageDetail;
import cn.ctyun.oos.server.processor.image.ImageProcessor;
import cn.ctyun.oos.server.processor.image.ImageProcessorConsts;
import cn.ctyun.oos.server.processor.image.ImageProcessorConsts.EdgeMode;
import cn.ctyun.oos.server.processor.image.ImageProcessorConsts.LargeMode;
import cn.ctyun.oos.server.processor.image.thumbnailator.watermark.MixWatermark;
import cn.ctyun.oos.server.processor.image.thumbnailator.watermark.SupportedFontNames;

/**
 * @author zhaowentao
 *
 */
public class ParameterUtils {
    
    public static int getWatermarkType(ImageEvent request)
            throws ProcessorException {
        int type = -1;
        Object str = request.getParamValue(ImageEvent.ImageParams.WATER_MARK);
        if (str == null) {
            throw new ProcessorException(400, "Missing argument:watermark");
        }

        type = (int) str;

        if (type < 1 || type > 3) {
            // 参数错误 watermark [1,3]
            throw new ProcessorException(400,
                    "The value: of parameter: watermark is invalid. ");
        }
        return type;
    }

    /**
     * 透明度, 如果是图片水印，就是让图片变得透明，如果是文字水印，就是让水印变透明。 默认值：100， 表示 100%（不透明） 取值范围:
     * [0-100]
     * 
     * @param request
     * @return
     * @throws ProcessorException
     */
    public static int getTransparent(ImageEvent request)
            throws ProcessorException {

        int t = ImageProcessorConsts.DEFAULT_WATERMARK_IMAGE_TRANSPARENT;
        Object obj = request.getParamValue(ImageEvent.ImageParams.TRANSPARENT);
        if (obj != null) {
            t = (int) obj;
            if (t < 0 || t > 100) {
                // 参数错误 t [0-100]
                throw new ProcessorException(400, String.format(
                        "The value: %d of parameter: t is invalid.", t));
            }
        }
        return t;
    }

    /**
     * 位置，水印打在图的位置，位置如成如下图。 默认值：9，表示在右下角打水印 取值范围：[1-9]
     * 
     * @param request
     * @return
     * @throws ProcessorException
     */
    public static int getPosition(ImageEvent request)
            throws ProcessorException {
        //
        int p = ImageProcessorConsts.DEFAULT_POSITION;
        Object obj = request.getParamValue(ImageEvent.ImageParams.POSITION);
        if (obj != null) {
            p = (int) obj;
            if (p < 1 || p > 9) {
                // "位置参数错误，有效值[1-9]"
                throw new ProcessorException(400, String.format(
                        "The value: %d of parameter: p is invalid.", p));
            }
        }
        return p;
    }

    /**
     * 水平边距, 就是距离图片边缘的水平距离， 这个参数只有当水印位置是左上，左中，左下， 右上，右中，右下才有意义 默认值：10 取值范围：[0 –
     * 4096] 单位：像素（px）
     * 
     * @param request
     * @return
     * @throws ProcessorException
     */
    public static int getMarginX(ImageEvent request) throws ProcessorException {
        int x = ImageProcessorConsts.DEFAULT_MARGIN_X;
        Object obj = request.getParamValue(ImageEvent.ImageParams.X_MARGIN);
        if (obj != null) {
            x = (int) obj;
            if (x < 0 || x > ImageProcessorConsts.MAX_EDGE_LENGTH) {
                throw new ProcessorException(400, String.format(
                        "The value: %d of parameter: x is invalid.", x));
            }
        }
        return x;
    }

    /**
     * 垂直边距, 就是距离图片边缘的垂直距离， 这个参数只有当水印位置是左上，中上， 右上，左下，中下，右下才有意义 默认值：10 取值范围：[0 –
     * 4096] 单位：像素(px)
     * 
     * @param request
     * @return
     * @throws ProcessorException
     */
    public static int getMarginY(ImageEvent request) throws ProcessorException {
        int y = ImageProcessorConsts.DEFAULT_MARGIN_Y;
        Object obj = request.getParamValue(ImageEvent.ImageParams.Y_MARGIN);
        if (obj != null) {
            y = (int) obj;
            if (y < 0 || y > ImageProcessorConsts.MAX_EDGE_LENGTH) {
                throw new ProcessorException(400, String.format(
                        "The value: %d of parameter: y is invalid.", y));
            }
        }
        return y;
    }

    /**
     * 中线垂直偏移，当水印位置在左中，中部，右中时，可以指定水印位置根据中线往上或者往下偏移。 默认值：0 取值范围：[-1000, 1000]
     * 单位：像素(px)
     * 
     * @param request
     * @return
     * @throws ProcessorException
     */
    public static int getVoffset(ImageEvent request) throws ProcessorException {

        int voffset = ImageProcessorConsts.DEFAULT_VOFFSET;
        Object str = request.getParamValue(ImageEvent.ImageParams.VOFFSET);
        if (str != null) {
            voffset = (int) str;
            if (voffset < -1000 || voffset > 1000) {
                throw new ProcessorException(400,
                        String.format(
                                "The value: %d of parameter: voffset is invalid.",
                                voffset));
            }
        }
        return voffset;
    }

    /**
     * text 表示文字水印的文字内容 EncodefontText = url_safe_base64_encode (fontText)
     * 最大长度为64个字符(即支持汉字最多20个左右)
     * 
     * @param request
     * @return
     * @throws ProcessorException
     */
    public static String getText(ImageEvent request) throws ProcessorException {
        //
        String text = null;
        Object str = request.getParamValue(ImageEvent.ImageParams.TEXT);
        if (str == null) {
            throw new ProcessorException(400, "Missing argument: text");
        }
        text=(String) request
        .getParamValue(ImageEvent.ImageParams.TEXT);
        int length = text.getBytes().length;
        if (length > ImageProcessorConsts.MAX_WATERMARK_DECODED_TEXT_LENGTH) {
            
            // 参数错误 text 超过长度限制(最大长度为64个字符)
            throw new ProcessorException(400,
                    "The value  of parameter: text is invalid.reason:font content is too large.");
        }
        //最大长度为64个字符(即支持汉字最多20个左右)
        //public final static int MAX_WATERMARK_ENCODED_TEXT_LENGTH = 64;

//        text = ProcessUtils.base64Decode(encodedText);
//        try {
//            text = ProcessUtils.base64Decode(encodedText);
//        } catch (Exception e) {
//            // TODO Auto-generated catch block
//            // "参数错误 text 不是 BASE64编码 "
//            throw new ProcessorException(400,
//                    "The value  of parameter: text is invalid.reason:Input is not base64 encoding.");
//        }
        return text;
    }

    /**
     * 参数意义：文字水印文字的颜色(必须编码) 注意：参数必须是Base64位编码 EncodeFontColor =
     * url_safe_base64_encode(fontColor) 参数的构成必须是：# + 六个十六进制数 如：#000000表示黑色。
     * #是表示前缀，000000每两位构成RGB颜色， #FFFFFF表示的是白色 默认值：#000000黑色
     * base64编码后值：IzAwMDAwMA 可选参数
     * 
     * @param request
     * @throws ProcessorException
     */
    public static Color getFontColor(ImageEvent request)
            throws ProcessorException {
        Color color = ImageProcessorConsts.DEFAULT_WATERMARK_FONT_COLOR;
        String colorText = null;
        try {
            Object obj = request.getParamValue(ImageEvent.ImageParams.COLOR);
            if (obj == null) {
                return color;
            }
            //colorText = ProcessUtils.base64Decode((String) obj);
            colorText = (String) obj;
            color = Color.decode(colorText);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            throw new ProcessorException(400,
                    "The value of parameter: color is invalid.");
        }
        return color;
    }

    /**
     * size 参数意义：文字水印文字大小(px) 取值范围：(0，1000] 默认值：40 可选参数
     * 
     * @param request
     * @return
     * @throws ProcessorException
     */
    public static int getFontSize(ImageEvent request)
            throws ProcessorException {
        //
        int size = ImageProcessorConsts.DEFAULT_WATERMARK_FONT_SIZE;
        Object str = request.getParamValue(ImageEvent.ImageParams.SIZE);
        if (str != null) {
            size = (int) str;
            if (size <= 0 || size > 1000) {
                // "参数错误 size (0 - 1000]"
                throw new ProcessorException(400, String.format(
                        "The value: %d of parameter: size is invalid.", size));
            }
        }
        return size;
    }

    /**
     * 参数意义：文字水印的阴影透明度 取值范围：(0,100]
     * 
     * @param request
     * @return
     * @throws ProcessorException
     */
    public static int getShadowTransparent(ImageEvent request)
            throws ProcessorException {
        int s = ImageProcessorConsts.DEFAULT_WATERMARK_FONT_SHADOW_TRANSPARENT;
        Object str = request.getParamValue(ImageEvent.ImageParams.SHADOW);
        if (str == null) {
            return s;
        }
        s = (int) str;
        if (s <= 0 || s > 100) {
            // "参数错误 s (0 - 100]"
            throw new ProcessorException(400, String
                    .format("The value: %d of parameter: s is invalid.", s));
        }
        return s;
    }

    /**
     * 参数意义：表示文字水印的文字类型(必须编码) 注意：必须是Base64编码 EncodeFontType =
     * url_safe_base64_encode (fontType) 取值范围：见下表（文字类型编码对应表） 默认值：wqy-zenhei (
     * 编码后的值：d3F5LXplbmhlaQ) 可选参数
     * 
     * @param request
     * @return
     * @throws ProcessorException
     */
    public static String getFontName(ImageEvent request)
            throws ProcessorException {
        // TODO Auto-generated method stub
        String fontName = ImageProcessorConsts.DEFAULT_FONT_NAME;
        try {
            Object str = request.getParamValue(ImageEvent.ImageParams.TYPE);
            if (str == null) {
                return fontName;
            }
            fontName = (String) str;
            if (StringUtils.isEmpty(fontName)) {
                return fontName;
            }

        } catch (Exception e) {
            // TODO Auto-generated catch block
            // "参数错误 type 不是 BASE64编码 "
            e.printStackTrace();
            throw new ProcessorException(400,
                    "The value  of parameter: type is invalid.reason:Input is not base64 encoding.");
        }
        if (!SupportedFontNames.isSupport(fontName)) {
            // "参数错误 type ,不支持的字体类型"
            throw new ProcessorException(400,
                    "The value of parameter type is invalid.");
        }
        return fontName;
    }

    /**
     * 参数意义： 水印图片的object名字(必须编码) 注意：内容必须是url 安全Base64编码 EncodedObject =
     * url_safe_base64_encode(object) 如object为”panda.png”, 编码过后的内容就是
     * “cGFuZGEucG5n”
     * 
     * @param request
     * @return
     * @throws Exception
     */
    public static ImageDetail getWatermarkBufferedImage(ImageEvent request)
            throws Exception {
        // TODO Auto-generated method stub
        Object object = request.getParamValue(ImageEvent.ImageParams.OBJECT);
        if (object == null) 
            throw new ProcessorException(400, "Missing argument: object");

        Object bucket = request
                .getParamValue(ImageEvent.ImageParams.BUCKET);
        if(null == bucket)
            throw new ProcessorException(403, "Access Denied.");
        
        ImageDetail imageDetail = request.getImageDetail();
        int sourceWidth = imageDetail.getWidth();
        int sourceHeight = imageDetail.getHeight();
        ImageDetail waterImage = ImageProcessor
                .processWaterMark((String) bucket, (String) object, sourceWidth, sourceHeight);
        if (waterImage == null) {
            throw new ProcessorException(400, "Watermark picture not found.");
        }
        return waterImage;
    }

    /**
     * 文字，图片水印前后顺序 取值范围：[0, 1] order = 0 图片在前(默认值)； order = 1 文字在前。 可选参数
     * 
     * @param request
     * @return
     * @throws ProcessorException
     */
    public static MixWatermark.ORDER getImageTextOrder(ImageEvent request)
            throws ProcessorException {
        MixWatermark.ORDER orderEnum = ImageProcessorConsts.DEFAULT_IMAGE_TEXT_ORDER;
        int order = 0;
        Object str = request.getParamValue(ImageEvent.ImageParams.ORDER);
        if (str != null) {
            order = (int) str;
            if (order == 0) {
                orderEnum = MixWatermark.ORDER.ORDER_IMAGE_TEXT;
            } else if (order == 1) {
                orderEnum = MixWatermark.ORDER.ORDER_TEXT_IMAGE;
            } else {
                throw new ProcessorException(400,
                        String.format(
                                "The value: %d of parameter: %s is invalid.",
                                order, ImageEvent.ImageParams.ORDER));
            }
        }
        return orderEnum;
    }

    /**
     * 文字、图片对齐方式 取值范围：[0, 1, 2] align = 0 上对齐(默认值) align = 1 中对齐 align = 2 下对齐
     * 可选参数
     * 
     * @param request
     * @return
     * @throws ProcessorException
     */
    public static MixWatermark.ALIGN getAlign(ImageEvent request)
            throws ProcessorException {
        MixWatermark.ALIGN align = ImageProcessorConsts.DEFAULT_IMAGE_TEXT_ALIGN;
        int s = 1;
        Object str = request.getParamValue(ImageEvent.ImageParams.ALIGN);
        if (str != null) {
            s = (int) str;
            if (s == 0) {
                align = MixWatermark.ALIGN.ALIGN_TOP;
            } else if (s == 1) {
                align = MixWatermark.ALIGN.ALIGN_CENTER;
            } else if (s == 2) {
                align = MixWatermark.ALIGN.ALIGN_BOTTOM;
            } else {
                // "参数错误 align {0,1,2}"
                throw new ProcessorException(400,
                        String.format(
                                "The value: %d of parameter: %s is invalid.",
                                align, ImageEvent.ImageParams.ALIGN));
            }
        }
        return align;
    }

    /**
     * 文字和图片间的间距 取值范围: [0, 1000] 可选参数
     * 
     * @param request
     * @return
     * @throws ProcessorException
     */
    public static int getInterval(ImageEvent request)
            throws ProcessorException {
        int s = ImageProcessorConsts.DEFAULT_IMAGE_TEXT_INTERVAL;

        Object xStr = request.getParamValue(ImageEvent.ImageParams.INTERVAL);
        if (xStr != null) {
            s = (int) xStr;
        }
        if (s < 0 || s > 1000) {
            // "参数错误 interval (0 - 1000]"
            throw new ProcessorException(400,
                    String.format("The value: %d of parameter: %s is invalid.",
                            s, ImageEvent.ImageParams.INTERVAL));
        }
        return s;

    }

    /**
     * 获取剪切起点 X 必填 >=0
     * 
     * @param request
     * @return
     * @throws ProcessorException
     */
    public static int getStartX(ImageEvent request) throws ProcessorException {
        // TODO Auto-generated method stub
        Object xStr = request.getParamValue(ImageEvent.ImageParams.X_MARGIN);
        if (xStr == null) {
            throw new ProcessorException(400, "Missing argument: x");
        }
        int x = (int) xStr;
        if (x < 0) {
            // "参数错误 x <0 "
            throw new ProcessorException(400,
                    String.format("The value: %d of parameter: %s is invalid.",
                            x, ImageEvent.ImageParams.X_MARGIN));
        }
        return x;
    }

    /**
     * 获取剪切起点 Y 必填 >=0
     * 
     * @param request
     * @return
     * @throws ProcessorException
     */
    public static int getStartY(ImageEvent request) throws ProcessorException {
        // TODO Auto-generated method stub
        Object str = request.getParamValue(ImageEvent.ImageParams.Y_MARGIN);
        if (str == null) {
            throw new ProcessorException(400, "Missing argument: y");
        }
        int y = (int) str;

        if (y < 0) {
            // "参数错误 y <0 "
            throw new ProcessorException(400,
                    String.format("The value: %d of parameter: %s is invalid.",
                            y, ImageEvent.ImageParams.Y_MARGIN));
        }
        return y;
    }

    /**
     * 解析宽度参数， 必填 [0,4096] 为0, 表示裁剪到图片的边缘
     * 
     * @param request
     * @return
     * @throws ProcessorException
     */
    public static int getWidth(ImageEvent request) throws ProcessorException {
        Object str = request.getParamValue(ImageEvent.ImageParams.WIDTH);
        if (str == null) {
            throw new ProcessorException(400, "Missing argument: width");
        }
        int width = (int) str;
        if (width < 0 || width > ImageProcessorConsts.MAX_EDGE_LENGTH) {
            // throw new ProcessorException(400, "参数错误 width [0-4096] ");
            throw new ProcessorException(400,
                    String.format("The value: %d of parameter: %s is invalid.",
                            width, ImageEvent.ImageParams.WIDTH));

        }
        return width;
    }

    /**
     * 解析高度参数， 必填 [0,4096] 为0, 表示裁剪到图片的边缘
     * 
     * @param request
     * @return
     * @throws ProcessorException
     */
    public static int getHeight(ImageEvent request) throws ProcessorException {
        // TODO Auto-generated method stub
        Object str = request.getParamValue(ImageEvent.ImageParams.HEIGHT);
        if (str == null) {
            throw new ProcessorException(400, "Missing argument: height");
        }
        int height = (int) str;
        if (height < 0 || height > ImageProcessorConsts.MAX_EDGE_LENGTH) {
            // throw new ProcessorException(400, "参数错误 heigth <0 ");
            throw new ProcessorException(400,
                    String.format("The value: %d of parameter: %s is invalid.",
                            height, ImageEvent.ImageParams.HEIGHT));
        }
        return height;
    }

    /**
     * 解析宽度参数， 选填 [0,4096],提供给缩放使用
     * 
     * @param request
     * @return
     * @throws ProcessorException
     */
    public static int getWidthForScale(ImageEvent request)
            throws ProcessorException {
        Object str = request.getParamValue(ImageEvent.ImageParams.WIDTH);
        int width = ImageProcessorConsts.DEFAULT_DEST_WIDTH_LENGTH;
        if (str != null) {
            width = (int) str;
            if (width <= 0) {
                throw new ProcessorException(400,
                        String.format(
                                "The value: %d of parameter: %s is invalid.",
                                width, ImageEvent.ImageParams.WIDTH));

            } else if (width > ImageProcessorConsts.MAX_EDGE_LENGTH) {
                throw new ProcessorException(400,
                        String.format(
                                "The value: %d of parameter: %s has exceeded the maximum value.",
                                width, ImageEvent.ImageParams.WIDTH));
            }
            return width;
        }
        return width;

    }

    /**
     * 解析高度参数， 必填 [0,4096] 为0, 表示裁剪到图片的边缘
     * 
     * @param request
     * @return
     * @throws ProcessorException
     */
    public static int getHeightForScale(ImageEvent request)
            throws ProcessorException {
        // TODO Auto-generated method stub
        Object str = request.getParamValue(ImageEvent.ImageParams.HEIGHT);
        int height = ImageProcessorConsts.DEFAULT_DEST_HEIGHT_LENGTH;
        if (str != null) {
            height = (int) str;
            if (height <= 0) {
                throw new ProcessorException(400,
                        String.format(
                                "The value: %d of parameter: %s is invalid.",
                                height, ImageEvent.ImageParams.HEIGHT));

            } else if (height > ImageProcessorConsts.MAX_EDGE_LENGTH) {
                throw new ProcessorException(400,
                        String.format(
                                "The value: %d of parameter: %s has exceeded the maximum value.",
                                height, ImageEvent.ImageParams.HEIGHT));
            }
            return height;
        }
        return height;
    }
    /**
     * 获取图片质量参数
     * @param request
     * @return
     * @throws ProcessorException
     */
    public static int getQuality(ImageEvent request) throws ProcessorException {
        int quality = getAbsoluteQuality(request);
        if (quality >= 0) {
            return quality;
        }
        quality = getRelativeQuality(request);

        return quality;
    }

    /**
     * 获取放大倍数
     * 
     * @param request
     * @return
     * @throws ProcessorException
     */
    public static int getPercentage(ImageEvent request)
            throws ProcessorException {
        // TODO Auto-generated method stub
        Object str = request.getParamValue(ImageEvent.ImageParams.ABSOLUTE_PERCENTAGE);
        int percentage = ImageProcessorConsts.DEFAULT_PERCENTAGE;
        if (str != null) {
            percentage = (int) str;
            if (percentage < 0) {
                throw new ProcessorException(400,
                        String.format(
                                "The value: %d of parameter: %s is invalid.",
                                percentage, ImageEvent.ImageParams.ABSOLUTE_PERCENTAGE));
            }
            return percentage;
        }
        return percentage;
    }

    /**
     * 解析相对质量变换
     * 
     * @param request
     * @return
     * @throws ProcessorException
     */
    private static int getRelativeQuality(ImageEvent request)
            throws ProcessorException {
        // TODO Auto-generated method stub
        Object str = request
                .getParamValue(ImageEvent.ImageParams.RELATIVE_QUALITY);
        
        int quality = -1;
        if (str == null) {
            return quality;
        }
        if(str instanceof Integer){
            quality = (int) str; 
        }else{
            throw new ProcessorException(400,
                    String.format("The value: %s of parameter: %s is invalid.",
                            str, ImageEvent.ImageParams.RELATIVE_QUALITY));
        }
        if (quality < 0 || quality > 100) {
            // throw new ProcessorException(400, "参数错误 heigth <0 ");
            throw new ProcessorException(400,
                    String.format("The value: %d of parameter: %s is invalid.",
                            quality, ImageEvent.ImageParams.RELATIVE_QUALITY));
        }
        return quality;
    }

    /**
     * 解析绝对质量变换
     * 
     * @param request
     * @return
     * @throws ProcessorException
     */
    private static int getAbsoluteQuality(ImageEvent request)
            throws ProcessorException {
        // TODO Auto-generated method stub
        Object str = request
                .getParamValue(ImageEvent.ImageParams.ABSOLUTE_QUALITY);
        int quality = -1;
        if (str == null) {
            return quality;
        }
        if(str instanceof Integer){
            quality = (int) str; 
        }else{
            throw new ProcessorException(400,
                    String.format("The value: %s of parameter: %s is invalid.",
                            str, ImageEvent.ImageParams.ABSOLUTE_QUALITY));
        }
        
        if (quality < 0 || quality > 100) {
            // throw new ProcessorException(400, "参数错误 heigth <0 ");
            throw new ProcessorException(400,
                    String.format("The value: %d of parameter: %s is invalid.",
                            quality, ImageEvent.ImageParams.ABSOLUTE_QUALITY));
        }
        return quality;
    }

    /**
     * 解析是否剪切
     * 
     * @param request
     * @return
     * @throws ProcessorException
     */
    public static ImageProcessorConsts.CutMode getIsCut(ImageEvent request)
            throws ProcessorException {
        // TODO Auto-generated method stub
        Object str = request.getParamValue(ImageEvent.ImageParams.IS_CUT);
        ImageProcessorConsts.CutMode isCut = ImageProcessorConsts.CutMode.NONE;
        if (str == null) {
            return ImageProcessorConsts.CutMode.NONE;
        }
        int isCutInt = (int) str;
        if (isCutInt < 0 || isCutInt > 1) {
            // throw new ProcessorException(400, "参数错误 heigth <0 ");
            throw new ProcessorException(400,
                    String.format("The value: %d of parameter: %s is invalid.",
                            isCutInt, ImageEvent.ImageParams.IS_CUT));
        }
        if (0 == isCutInt) {
            isCut = ImageProcessorConsts.CutMode.NOT_CUT;
        } else if (1 == isCutInt) {
            isCut = ImageProcessorConsts.CutMode.DO_CUT;
        }

        return isCut;
    }

    /**
     * 获取放大模式 如果原图小于目标图是否放大原图
     * 
     * @param request
     * @return
     * @throws ProcessorException
     */
    public static LargeMode getLargeMode(ImageEvent request)
            throws ProcessorException {
        LargeMode align = ImageProcessorConsts.DEFAULT_ENLARGE_MODE;
        int s = 0;
        Object str = request.getParamValue(
                ImageEvent.ImageParams.THUMBNAIL_GREATER_THAN_ORIGINAL);
        if (str != null) {
            s = (int) str;
            if (s == 0) {
                align = LargeMode.Enlarge;
            } else if (s == 1) {
                align = LargeMode.Normal;
            } else {
                // "参数错误 l {0,1}"
                throw new ProcessorException(400, String.format(
                        "The value: %d of parameter: %s is invalid.", align,
                        ImageEvent.ImageParams.THUMBNAIL_GREATER_THAN_ORIGINAL));
            }
        }
        return align;
    }

    /**
     * 获取边模式 默认0表示长边模式，1表示短边模式，2表示强制模式
     * 
     * @param request
     * @return
     * @throws ProcessorException
     */
    public static EdgeMode getEdgeMode(ImageEvent request)
            throws ProcessorException {
        EdgeMode align = ImageProcessorConsts.DEFAULT_EDGE_MODE;
        Object str = request
                .getParamValue(ImageEvent.ImageParams.THUMBNAIL_PRIOR_SIDE);
        if (str != null) {
            int s = (int) str;
            if (s == 0) {
                align = EdgeMode.LONG;
            } else if (s == 1) {
                align = EdgeMode.SHORT;
            } else if (s == 2) {
                align = EdgeMode.FORCE;
            } else {
                // "参数错误 e {0,1,2}"
                throw new ProcessorException(400, String.format(
                        "The value: %d of parameter: %s is invalid.", align,
                        ImageEvent.ImageParams.THUMBNAIL_PRIOR_SIDE));
            }
            return align;
        }
        return align;
    }
    /**
     * 获取 wh参数
     * png转其他格式图片是透明色的处理方式
     * @param request
     * @return
     * @throws ProcessorException
     */
    public static Color getTransparentTransColor(ImageEvent request)
            throws ProcessorException {
        Color bg = ImageProcessorConsts.DEFAULT_TRANSPARENT_TRANS_COLOR;
        Object str = request
                .getParamValue(ImageEvent.ImageParams.WHITE_FILL);
        if (str != null) {
            int s = (int) str;
            if (s == 0) {
                bg = Color.BLACK;
            } else if (s == 1) {
                bg = Color.WHITE;
            } else {
                // "参数错误 wh {0,1}"
                throw new ProcessorException(400, String.format(
                        "The value: %d of parameter: %s is invalid.", s,
                        ImageEvent.ImageParams.WHITE_FILL));
            }
        }
        return bg;
    }

    /**
     * 获取输出格式
     * 
     * @param request
     * @return
     * @throws ProcessorException
     */
    public static ImageFormat getFormat(ImageEvent request)
            throws ProcessorException {
        Object format = request
                .getParamValue(ImageEvent.ImageParams.DEST_IMAGE_FORMAT);
        if (null == format || !(format instanceof ImageFormat)) 
            return null;
        
        return (ImageFormat)format;
    }

    public static int[] getHighGradeCutValues(ImageEvent event)
            throws ProcessorException {
        // TODO Auto-generated method stub
        int[] values = new int[4];
        String valueStr = (String) event
                .getParamValue(ImageEvent.ImageParams.ADVANCED_CUT);
        if (valueStr == null) {
            throw new ProcessorException(400, "Missing argument: a");
        }
        if (!valueStr.matches("^\\d+-\\d+-\\d+-\\d+$")) {
            throw new ProcessorException(400,
                    String.format("The value: %s of parameter: %s is invalid.",
                            valueStr, "a"));
        }
        try {
            String[] params = valueStr.split("-");
            assert(params.length == 4);
            for (int i = 0; i < params.length; i++) {
               values[i] = Integer.parseInt(params[i]);
            }
        } catch (Exception e) {
            throw new ProcessorException(400,
                    String.format("The value: %s of parameter: %s is invalid.",
                            valueStr, "a"));
        }
        return values;
    }
    
    /**
     * 获取GIF图片的播放延时 
     * @param request
     * @return
     * @throws ProcessorException
     */
    public static int getDelayTime(ImageEvent request)
            throws ProcessorException {
        Object str = request.getParamValue(ImageEvent.ImageParams.GIF_DELAY_TIME);
        if (str == null) {
            throw new ProcessorException(400, "Missing argument: "+ImageEvent.ImageParams.GIF_DELAY_TIME);
        }
        try{
            int delay = Integer.parseInt((String)str);
            if (delay < 0 || delay > 5000) {
                throw new ProcessorException(400,
                        "The value: of parameter: "+ImageEvent.ImageParams.GIF_DELAY_TIME+" is invalid. ");
            }
            return delay;
        }catch(Exception e){
            throw new ProcessorException(400,
                    "The value: of parameter: "+ImageEvent.ImageParams.GIF_DELAY_TIME+" is invalid. ");
        }
        
    }
    /**
     * 获取GIF是否循环播放 
     * 是否循环播放，0是不循环，1是循环 0/1，默认是0
     * @param request
     * @return
     * @throws ProcessorException
     */
    public static boolean isLoop(ImageEvent request)
            throws ProcessorException {
        Object str = request.getParamValue(ImageEvent.ImageParams.GIF_LOOP);
        if (str == null) {
            return false;
        }
        try{
            int loop = Integer.parseInt((String)str);
            if (loop != 0 && loop != 1) {
                throw new ProcessorException(400,
                        "The value: of parameter: "+ImageEvent.ImageParams.GIF_LOOP+" is invalid. ");
            }
            return loop==1;
        }catch(Exception e){
            throw new ProcessorException(400,
                    "The value: of parameter: "+ImageEvent.ImageParams.GIF_LOOP+" is invalid. ");
        }
    }
    /**
     * 获取需要合并的对象列表
     * @param request
     * @return
     * @throws Exception 
     */
    public static List<ImageDetail> getMergeImages(ImageEvent request) throws Exception{
        List<ImageDetail> images = new ArrayList<ImageDetail>();
        Object list = request.getParamValue(ImageEvent.ImageParams.OBJECT);
        if(list == null){
            return images;
        }
        Object bucket = request.getParamValue(ImageEvent.ImageParams.SOURCE_BUCKET);
        if(null == bucket)
            throw new ProcessorException(403, "Access Denied.");
        
        List<String> urls = (List<String>)list;
        if(urls.size() > ImageProcessorConsts.MAX_MERGE_IMAGE_COUNT){
            throw new ProcessorException(400, "too many images. max count is:"+ImageProcessorConsts.MAX_MERGE_IMAGE_COUNT);
        }
        for(String url: urls){
            ImageDetail image = ImageProcessor.processImage((String) bucket, url);
            images.add(image);
            if (image == null) {
                throw new ProcessorException(400, "image not found.url:"+url );
            }
        }
        return images;
    }
}
