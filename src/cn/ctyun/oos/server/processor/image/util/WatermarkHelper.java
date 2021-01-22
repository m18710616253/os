package cn.ctyun.oos.server.processor.image.util;

import java.awt.Color;
import java.awt.Font;
import java.awt.Point;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;

import javax.imageio.ImageIO;

import cn.ctyun.oos.server.processor.image.thumbnailator.watermark.ImageWatermark;
import cn.ctyun.oos.server.processor.image.thumbnailator.watermark.MixWatermark;
import cn.ctyun.oos.server.processor.image.thumbnailator.watermark.TextWatermark;
import cn.ctyun.oos.server.processor.image.thumbnailator.watermark.MixWatermark.ALIGN;
import cn.ctyun.oos.server.processor.image.thumbnailator.watermark.MixWatermark.ORDER;
import net.coobird.thumbnailator.Thumbnails;
import net.coobird.thumbnailator.Thumbnails.Builder;
import net.coobird.thumbnailator.filters.Caption;
import net.coobird.thumbnailator.filters.Watermark;
import net.coobird.thumbnailator.geometry.Position;
import net.coobird.thumbnailator.geometry.Positions;

public class WatermarkHelper {
    /**
     * 增加图片水印
     * @param builder
     * @param watermarkImage
     * @param transparent
     * @param position
     * @param x
     * @param y
     * @param voffset
     * @return
     */
    public static Builder<?> addImageWatermark(Builder<?> builder,
            BufferedImage watermarkImage, int transparent, int position, int x,
            int y, int voffset) {
        // TODO Auto-generated method stub
        if(builder==null){
            return null;
        }
        ImageWatermark w = new ImageWatermark(  PositionHelper.getPosition(position),watermarkImage,transparent/100f,x,y,voffset);
        return builder.addFilter(w);
    }
    /**
     * 添加文字水印
     * @param builder
     * @param text
     * @param font
     * @param fontColor
     * @param transparent
     * @param position
     * @param x
     * @param y
     * @param voffset
     * @return
     */
    public static Builder<?> addTextWatermark(Builder<?> builder, String text,
            Font font, Color fontColor, int transparent, int position, int x,
            int y, int voffset,int shadowTransparent) {
        // TODO Auto-generated method stub
        if(builder==null){
            return null;
        }
        TextWatermark w = new TextWatermark( text,font,fontColor,transparent/100f,PositionHelper.getPosition(position),x,y,voffset,shadowTransparent/100f);
        return builder.addFilter(w);
        
    }
    /**
     * 增加图文混合水印
     * @param builder
     * @param text
     * @param watermarkImage
     * @param font
     * @param fontColor
     * @param transparent
     * @param position
     * @param x
     * @param y
     * @param voffset
     * @param shadowTransparent
     * @param order
     * @param align
     * @param interval
     * @return
     */
    public static Builder<?> addMixWatermark(Builder<?> builder, String text,
            BufferedImage watermarkImage, Font font, Color fontColor,
            int transparent, int position, int x, int y, int voffset,
            int shadowTransparent, ORDER order, ALIGN align, int interval) {
        // TODO Auto-generated method stub
        if(builder==null){
            return null;
        }
        MixWatermark mix = new MixWatermark(text, font, fontColor,transparent/100f, PositionHelper.getPosition(position), x, y,watermarkImage,align, order, interval,voffset,shadowTransparent/100f);
        return builder.addFilter(mix);

    }
    
    /**
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        // TODO Auto-generated method stub
        String srcPath = "D:/gm/images/name.bmp";
        //String srcPath = "D:/gm/images/gif.gif";
        //String srcPath = "D:/gm/images/cmyk.jpg";
        //String srcPath = "D:/gm/images/ipomoea.jpg";
        String desPath = "D:/gm/images/ipomoea_200w.jpg";
        
        String  logoPath =  "D:/gm/images/logo.png";
        
        Position position = Positions.TOP_RIGHT;
        
        Font font = new Font("黑体",Font.PLAIN,150);
        TextWatermark c = new TextWatermark("中国人民万岁！",font,Color.WHITE,1f,position,0,0,0,0.5f);
       // ImageWatermark w = new ImageWatermark( Positions.BOTTOM_RIGHT,ImageIO.read(new File(logoPath)),0.8f,0,0,0);
       // MixWatermark mix = new MixWatermark("云计算", font, Color.WHITE, 1f, position, 0, 0,ImageIO.read(new File(logoPath)),MixWatermark.ALIGN.ALIGN_TOP, MixWatermark.ORDER.ORDER_TEXT_IMAGE, 0,0,1);
        try {
            Thumbnails.of(srcPath).scale(0.2)
            //.addFilter(mix)
           // .addFilter(w)
            .addFilter(c)
           
            //.watermark( w  )
            
            .toFile(desPath);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    



    




}
