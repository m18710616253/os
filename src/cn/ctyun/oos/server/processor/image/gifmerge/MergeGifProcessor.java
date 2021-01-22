package cn.ctyun.oos.server.processor.image.gifmerge;

import java.awt.Color;
import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.util.List;

import cn.ctyun.oos.server.processor.EventBase;
import cn.ctyun.oos.server.processor.Processor;
import cn.ctyun.oos.server.processor.image.ImageEvent;
import cn.ctyun.oos.server.processor.image.ImageEvent.ImageDetail;
import cn.ctyun.oos.server.processor.image.ImageEvent.ImageParams;
import cn.ctyun.oos.server.processor.image.util.ImageFormat;
import cn.ctyun.oos.server.processor.image.util.ParameterUtils;
import net.coobird.thumbnailator.Thumbnailator;
import net.coobird.thumbnailator.Thumbnails;
import net.coobird.thumbnailator.Thumbnails.Builder;

public class MergeGifProcessor extends Processor {
    
    public MergeGifProcessor(EventBase event) {
        super(event);
    }

    @Override
    public void process() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ImageEvent imageEvent = (ImageEvent) event;
        
        ImageDetail firstImageDetail = imageEvent.getImageDetail();
        if (firstImageDetail == null) {
            throw new ProcessorException(500, "image is null");
        }
        
        int delayTime = ParameterUtils.getDelayTime(imageEvent);
        boolean loop = ParameterUtils.isLoop(imageEvent);
        List<ImageDetail> images = ParameterUtils.getMergeImages(imageEvent);
        AnimatedGifEncoder encoder = new AnimatedGifEncoder();
        encoder.start(out);
        if(loop){
            encoder.setRepeat(0); 
        }else{
            encoder.setRepeat(1);  
        }
//        encoder.setQuality(5);
        if(isAllPng(firstImageDetail, images)){
            //设置要替换成透明的颜色  
            encoder.setTransparent(Color.WHITE,true);
        }
        int width = getMinWidth(firstImageDetail, images);
        int height = getMinHeight(firstImageDetail, images);
        for(BufferedImage image:firstImageDetail.bufferedImages){
            if(image.getWidth()!=width || image.getHeight()!=height){
                image = Thumbnails.of(image).forceSize(width, height).asBufferedImage();
            }
            encoder.setDelay(delayTime);
            encoder.addFrame(image);
        }
        for (int i = 0; i < images.size(); i++) {
            ImageDetail imageDetail = images.get(i);
            for(BufferedImage image:imageDetail.bufferedImages){
                if(image.getWidth()!=width || image.getHeight()!=height){
                    image = Thumbnails.of(image).forceSize(width, height).asBufferedImage();
                }
                encoder.setDelay(delayTime);
                encoder.addFrame(image);
            }
        }
        encoder.finish();
        event.setParamValue(ImageParams.DEST_IMAGE_FORMAT, ImageFormat.GIF);
        event.setData(out.toByteArray());
    }
    /**
     * 获取所有图片的最小宽度
     * @param firstImageDetail
     * @param images
     * @return
     */
    private int getMinWidth(ImageDetail firstImageDetail, List<ImageDetail> images) {
        int width = Integer.MAX_VALUE;
        for(BufferedImage image:firstImageDetail.bufferedImages){
            if(image.getWidth()<width){
                width = image.getWidth();
            }
        }
        for (int i = 0; i < images.size(); i++) {
            ImageDetail imageDetail = images.get(i);
            for(BufferedImage image:imageDetail.bufferedImages){
                if(image.getWidth()<width){
                    width = image.getWidth();
                }
            }
        }
        return width;
    }
    /**
     * 获取所有图片的最小高度
     * @param firstImageDetail
     * @param images
     * @return
     */
    private int getMinHeight(ImageDetail firstImageDetail, List<ImageDetail> images) {
        int height = Integer.MAX_VALUE;
        for(BufferedImage image:firstImageDetail.bufferedImages){
            if(image.getHeight()<height){
                height = image.getHeight();
            }
        }
        for (int i = 0; i < images.size(); i++) {
            ImageDetail imageDetail = images.get(i);
            for(BufferedImage image:imageDetail.bufferedImages){
                if(image.getHeight()<height){
                    height = image.getHeight();
                }
            }
        }
        return height;
    }
    /**
     * 判断要合并的图片是否全部是png
     * @param firstImageDetail
     * @param images
     * @return
     */
    private boolean isAllPng(ImageDetail firstImageDetail, List<ImageDetail> images) {
        if(firstImageDetail.format != ImageFormat.PNG){
            return false;
        }
        for(ImageDetail image:images){
            if(image.format != ImageFormat.PNG){
                return false;
            }
        }
        return true;
    }

    @Override
    public Object getResult() {
        return event.getData();
    }
    
}
