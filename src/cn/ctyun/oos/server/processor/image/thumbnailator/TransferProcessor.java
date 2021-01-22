package cn.ctyun.oos.server.processor.image.thumbnailator;

import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import cn.ctyun.oos.server.processor.EventBase;
import cn.ctyun.oos.server.processor.Processor;
import cn.ctyun.oos.server.processor.image.ImageEvent;
import cn.ctyun.oos.server.processor.image.ImageEvent.ImageDetail;
import cn.ctyun.oos.server.processor.image.ImageEvent.ImageParams;
import cn.ctyun.oos.server.processor.image.ImageProcessorConsts;
import cn.ctyun.oos.server.processor.image.ImageProcessorConsts.EdgeMode;
import cn.ctyun.oos.server.processor.image.ImageProcessorConsts.LargeMode;
import cn.ctyun.oos.server.processor.image.thumbnailator.watermark.TransparentColorFilter;
import cn.ctyun.oos.server.processor.image.util.ImageFormat;
import cn.ctyun.oos.server.processor.image.util.ParameterUtils;
import cn.ctyun.oos.server.processor.image.util.TransferHelper;
import net.coobird.thumbnailator.Thumbnails;
import net.coobird.thumbnailator.Thumbnails.Builder;

/**
 * 图片缩放和剪切处理器
 * 
 * @author zhaowentao
 *
 */
public class TransferProcessor extends Processor {
    private static Log log = LogFactory.getLog(TransferProcessor.class);

    public TransferProcessor(EventBase event) {
        super(event);
    }


    /**
     * 判断裁剪类型 自动裁剪 <height>h_<width>w_<mode>e_<cut>c[_<enlarge>l][.jpg] l参数可选
     * 高级裁剪 <x>-<y>-<width>-<height>a[.jpg] 区域裁剪 <width>x<height>-
     * <position>rc[.jpg] rc w h
     * 
     * @throws IOException
     */
    @Override
    public void process() throws Exception {
        ImageEvent imageEvent = (ImageEvent) event;
        ImageDetail imageDetail = imageEvent.getImageDetail();
        if (imageDetail == null) {
            throw new ProcessorException(500, "image is null");
        }
        
        boolean isChanged = false;
        Builder<?> builder = Thumbnails.of(imageDetail.bufferedImages);

        int sourceWidth = imageDetail.bufferedImages[0].getWidth();
        int sourceHeight = imageDetail.bufferedImages[0].getHeight();
        ImageProcessorConsts.CutMode isCut = ParameterUtils.getIsCut(imageEvent);
        /*
         * 判断逻辑：
         * a.如果 c =1 是自动裁剪
         * b.如果有a 参数认为是高级
         * c.其他认为是缩放
         */
        if (isCut == ImageProcessorConsts.CutMode.DO_CUT) {
            isChanged = processParameterForAutoCut(builder, imageEvent,
                    sourceWidth, sourceHeight);
        } else if (imageEvent
                .getParamValue(ImageEvent.ImageParams.ADVANCED_CUT) != null) {
            // 高级裁剪
            isChanged = processParameterForHighGradeCut(builder, imageEvent,
                    sourceWidth, sourceHeight);
        } else {
            // 缩放
            isChanged = processParameterForScale(builder, imageEvent,
                    sourceWidth, sourceHeight);
        }
        int quality = ParameterUtils.getQuality(imageEvent);
        if (quality > 0) {
            imageDetail.quality = quality;
            isChanged = true;
        }
        //用户指定了格式的后缀
        ImageFormat destFormat = ParameterUtils.getFormat(imageEvent);
        if(null != destFormat){
            if(imageDetail.format != destFormat){
                //png转其他格式时透明背景处理
                if (ImageFormat.PNG == imageDetail.format) {
                    if (!isChanged) {
                        builder.scale(1);
                    }
                    Color bgColor = ParameterUtils
                            .getTransparentTransColor(imageEvent);
                    builder.addFilter(new TransparentColorFilter(bgColor));
                }
                isChanged = true;
            }
            imageDetail.format = destFormat;
        }
        event.setParamValue(ImageParams.DEST_IMAGE_FORMAT, imageDetail.format);
        // 返回原图
        if (!isChanged) {
            event.setData(imageDetail);
            return;
        }
        List<BufferedImage> bufferedImageList = builder.asBufferedImages();
        imageDetail.bufferedImages = bufferedImageList
                .toArray(new BufferedImage[bufferedImageList.size()]);
        event.setData(imageDetail);
    }

    private boolean processParameterForScale(Builder<?> builder,
            ImageEvent event, int sourceWidth, int sourceHeight)
                    throws ProcessorException, IOException {
        ImageEvent ie = event;
        EdgeMode eMode = ParameterUtils.getEdgeMode(ie);
        LargeMode lMode = ParameterUtils.getLargeMode(ie);
        int w = ParameterUtils.getWidthForScale(ie);
        int h = ParameterUtils.getHeightForScale(ie);
        int percentage = ParameterUtils.getPercentage(ie);

        if (eMode != EdgeMode.NONE && w == 0 && h == 0) {
            throw new ProcessorException(400, "Width and Height can not both be 0");
        } else if (percentage != 100 && w == 0 && h == 0) {
            builder = TransferHelper.scale(builder, percentage * 0.01f,
                    sourceWidth, sourceHeight, lMode);
        } else if (w != 0 && h != 0) {
            w = (int) (w * percentage * 0.01f);
            h = (int) (h * percentage * 0.01f);
            builder = TransferHelper.scale(builder, w, h, sourceWidth,
                    sourceHeight, eMode, lMode);
        } else if (w != 0 || h != 0) {
            w = (int) (w * percentage * 0.01f);
            h = (int) (h * percentage * 0.01f);
            builder = TransferHelper.scale(builder, w, h, sourceWidth,
                    sourceHeight, lMode);
        } else {
            return false;
        }
        if (builder != null) {
            return true;
        }
        return false;

    }

    /**
     * 处理高级裁剪参数
     * @param builder
     * @param event
     * @param sourceWidth  源图像宽度
     * @param sourceHeight 源图像高度
     * @return 图像是否修改
     * @throws ProcessorException
     * @throws IOException
     */
    private boolean processParameterForHighGradeCut(Builder<?> builder,
            ImageEvent event, int sourceWidth, int sourceHeight)
                    throws ProcessorException, IOException {
        // TODO Auto-generated method stub
        int values[] = ParameterUtils.getHighGradeCutValues(event);
        int x = values[0];
        int y = values[1];
        int width = values[2];
        int height = values[3];
        // 高级裁剪
        if( (x >= sourceWidth) || (y >= sourceHeight)) {
            throw new ProcessorException(600, "Advance cut's position is out of image.");
        }
        builder = TransferHelper.crop(builder, x, y, width, height, sourceWidth,sourceHeight);

        if (builder == null) {
            return false;
        }

        return true;

    }

    /**
     * 处理自动裁剪
     * 
     * @param event
     * @param width
     * @param height
     * @throws ProcessorException
     * @throws IOException
     */
    private boolean processParameterForAutoCut(Builder<?> builder,
            ImageEvent event, int sourceWidth, int sourceHeight)
                    throws ProcessorException, IOException {
        // TODO Auto-generated method stub

        int width = ParameterUtils.getWidth((ImageEvent) event);
        int height = ParameterUtils.getHeight((ImageEvent) event);
        LargeMode enlargeMode = ParameterUtils.getLargeMode(event);
        // 自动裁剪

        builder = TransferHelper.crop(builder, width, height, enlargeMode,
                sourceWidth, sourceHeight);
        if (builder == null) {
            return false;
        }
        return true;
    }

  

    @Override
    public Object getResult() {
        // TODO Auto-generated method stub
        return event.getData();
    }

}
