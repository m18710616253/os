package cn.ctyun.oos.server.processor.image.thumbnailator;

import java.awt.Color;
import java.awt.Font;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.util.List;

import cn.ctyun.oos.server.processor.EventBase;
import cn.ctyun.oos.server.processor.Processor;
import cn.ctyun.oos.server.processor.image.ImageEvent;
import cn.ctyun.oos.server.processor.image.ImageEvent.ImageDetail;
import cn.ctyun.oos.server.processor.image.ImageEvent.ImageParams;
import cn.ctyun.oos.server.processor.image.thumbnailator.watermark.MixWatermark;
import cn.ctyun.oos.server.processor.image.thumbnailator.watermark.SupportedFontNames;
import cn.ctyun.oos.server.processor.image.util.ParameterUtils;
import cn.ctyun.oos.server.processor.image.util.WatermarkHelper;
import net.coobird.thumbnailator.Thumbnails;
import net.coobird.thumbnailator.Thumbnails.Builder;

public class WatermarkProcessor extends Processor {
    public WatermarkProcessor(EventBase event) {
        super(event);
        // TODO Auto-generated constructor stub
    }

    // 文本水印
    private static final int PARAMS_WATERMARK_TYPE_TEXT = 2;
    // 图片水印
    private static final int PARAMS_WATERMARK_TYPE_IMAGE = 1;
    // 图文混合水印
    private static final int PARAMS_WATERMARK_TYPE_MIX = 3;

    /**
     * 判断处理类型 图片水印 @watermark=1&object=<encodedobject>&t=<transparency>&x=
     * <distanceX>&y=<distanceY>&p=<position>
     * 
     * 
     * 文字水印 @watermark=2&text=<encodeText>&type=<encodeType>&size=<size>&color=
     * <encode colr>&t=<t>&p=<p>&x=<x>&voffset=<offset>&y=<y>
     * 
     * 
     * 图文混合水印 @watermark=3&object=<encodeObject>&text=<encodeText>&type=<encodeType>&size=<size>&color=<encodecolor>&order=<order>&align=<align>&interval=<interval>&t=<t>&p=<p>&x=<x>&y=<y>
     * 
     * @throws Exception
     * 
     */
    @Override
    public void process() throws Exception {
        // TODO Auto-generated method stub
        ImageEvent imageEvent = (ImageEvent) event;
        // ====处理公共参数=====
        int watermarkType = ParameterUtils.getWatermarkType(imageEvent);

        // ====处理公共可选参数=====
        int transparent = ParameterUtils.getTransparent(imageEvent);
        int position = ParameterUtils.getPosition(imageEvent);

        int x = ParameterUtils.getMarginX(imageEvent);
        int y = ParameterUtils.getMarginY(imageEvent);

        int voffset = ParameterUtils.getVoffset(imageEvent);

        switch (watermarkType) {
        case PARAMS_WATERMARK_TYPE_TEXT:
            //处理文本水印
            processPrarmeterForTextWartermark(imageEvent, transparent, position,
                    x, y, voffset);
            break;
        case PARAMS_WATERMARK_TYPE_IMAGE:
            //处理图片水印
            processPrarmeterForImageWartermark(imageEvent, transparent,
                    position, x, y, voffset);
            break;
        case PARAMS_WATERMARK_TYPE_MIX:
            //处理图文混合水印
            processPrarmeterForMixWartermark(imageEvent, transparent, position,
                    x, y, voffset);
            break;

        }

    }

   

    /**
     * 处理文字水印参数
     * 
     * @param event
     * @param transparent
     * @param position
     * @param x
     * @param y
     * @param voffset
     * @throws ProcessorException
     * @throws IOException
     */
    private void processPrarmeterForTextWartermark(ImageEvent event,
            int transparent, int position, int x, int y, int voffset)
                    throws Exception {
        // ====处理必选参数=====
        String text = ParameterUtils.getText(event);
        // ====处理可选参数=====
        String fontName = ParameterUtils.getFontName(event);
        Color fontColor = ParameterUtils.getFontColor(event);
        int fontSize = ParameterUtils.getFontSize(event);
        int shadowTransparent = ParameterUtils.getShadowTransparent(event);

        // Font font = new Font(fontName, Font.CENTER_BASELINE, fontSize);
        Font font = SupportedFontNames.loadFont(fontName, Font.PLAIN, fontSize);
        if (font == null) {
            throw new ProcessorException(400, "font load fail...");
        }
        ImageDetail imageDetail = event.getImageDetail();
        Builder<?> builder = Thumbnails.of(imageDetail.bufferedImages);

        builder = WatermarkHelper.addTextWatermark(builder, text, font,
                fontColor, transparent, position, x, y, voffset,
                shadowTransparent);
        builder.scale(1);
        List<BufferedImage> bufferedImageList = builder.asBufferedImages();
        BufferedImage[] bufferedImages = bufferedImageList
                .toArray(new BufferedImage[bufferedImageList.size()]);
        imageDetail.bufferedImages = bufferedImages;
        event.setData(imageDetail);

    }

    /**
     * 处理图片水印参数
     * 
     * @param event
     * @param transparent
     * @param position
     * @param x
     * @param y
     * @param voffset
     * @throws Exception
     */
    private void processPrarmeterForImageWartermark(ImageEvent event,
            int transparent, int position, int x, int y, int voffset)
                    throws Exception {
        // ====处理必选参数=====
        ImageDetail wartermarkImageDetail = ParameterUtils
                .getWatermarkBufferedImage(event);
        BufferedImage watermarkImage = wartermarkImageDetail.bufferedImages[0];
        // ====处理可选参数=====
        // 无
        ImageDetail imageDetail = event.getImageDetail();
        Builder<?> builder = Thumbnails.of(imageDetail.bufferedImages);

        builder = WatermarkHelper.addImageWatermark(builder, watermarkImage,
                transparent, position, x, y, voffset);
        builder.scale(1);
        List<BufferedImage> bufferedImageList = builder.asBufferedImages();
        BufferedImage[] bufferedImages = bufferedImageList
                .toArray(new BufferedImage[bufferedImageList.size()]);
        imageDetail.bufferedImages = bufferedImages;
        event.setData(imageDetail);
    }
    /**
     * 处理图文混合水印
     * 
     * @param event
     * @param transparent
     * @param position
     * @param x
     * @param y
     * @param voffset
     * @throws Exception
     */
    private void processPrarmeterForMixWartermark(ImageEvent event,
            int transparent, int position, int x, int y, int voffset)
                    throws Exception {
        // TODO Auto-generated method stub
        // ====处理必选参数=====
        String text = ParameterUtils.getText(event);
        ImageDetail wartermarkImageDetail = ParameterUtils
                .getWatermarkBufferedImage(event);
        BufferedImage watermarkImage = wartermarkImageDetail.bufferedImages[0];
        // ====处理可选参数=====
        MixWatermark.ORDER order = ParameterUtils.getImageTextOrder(event);
        MixWatermark.ALIGN align = ParameterUtils.getAlign(event);
        int interval = ParameterUtils.getInterval(event);

        String fontName = ParameterUtils.getFontName(event);
        Color fontColor = ParameterUtils.getFontColor(event);
        int fontSize = ParameterUtils.getFontSize(event);
        int shadowTransparent = ParameterUtils.getShadowTransparent(event);

        // Font font = new Font(fontName, Font.CENTER_BASELINE, fontSize);

        Font font = SupportedFontNames.loadFont(fontName, Font.PLAIN, fontSize);
        if (font == null) {
            throw new ProcessorException(400, "font load fail...");
        }
        ImageDetail imageDetail = event.getImageDetail();
        Builder<?> builder = Thumbnails.of(imageDetail.bufferedImages);

        builder = WatermarkHelper.addMixWatermark(builder, text, watermarkImage,
                font, fontColor, transparent, position, x, y, voffset,
                shadowTransparent, order, align, interval);
        builder.scale(1);
        List<BufferedImage> bufferedImageList = builder.asBufferedImages();
        BufferedImage[] bufferedImages = bufferedImageList
                .toArray(new BufferedImage[bufferedImageList.size()]);
        imageDetail.bufferedImages = bufferedImages;
        event.setParamValue(ImageParams.DEST_IMAGE_FORMAT, imageDetail.format);
        event.setData(imageDetail);
    }
    @Override
    public Object getResult() {
        // TODO Auto-generated method stub
        return event.getData();
    }

}
