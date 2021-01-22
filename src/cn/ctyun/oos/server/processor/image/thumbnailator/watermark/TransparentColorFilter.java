package cn.ctyun.oos.server.processor.image.thumbnailator.watermark;

import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.Transparency;
import java.awt.image.BufferedImage;

import net.coobird.thumbnailator.builders.BufferedImageBuilder;
import net.coobird.thumbnailator.filters.ImageFilter;

/**
 * 透明色过滤器
 * 主要用于PNG图片转JPG图片时，透明颜色转换错误的问题。
 * @author zhaowentao
 *
 */
public class TransparentColorFilter implements ImageFilter {
    
    private final Color bgColor;
    
    public TransparentColorFilter(Color bgColor){
        this.bgColor = bgColor;
    }
    
    @Override
    public BufferedImage apply(BufferedImage img) {
        // TODO Auto-generated method stub
        //原图非透明，直接返回
        if(img.getTransparency() == Transparency.OPAQUE){
            return img;
        }
        int width = img.getWidth();
        int height = img.getHeight();
        int type = BufferedImage.TYPE_INT_RGB;
        BufferedImage bgImage = new BufferedImageBuilder(width, height,
                type).build();
       
        Graphics2D g = (Graphics2D) bgImage.getGraphics();
        
        // 方式一：先用背景色清除，然后绘制源图像
        // g.setBackground(this.bgColor);
        // g.clearRect(0, 0, width, height);
        // g.drawImage(img, 0, 0, null);
        //方式二：绘制时使用背景色代替透明色
        g.drawImage(img, 0, 0,width, height,this.bgColor, null);
        g.dispose();
        return bgImage;
    }

}
