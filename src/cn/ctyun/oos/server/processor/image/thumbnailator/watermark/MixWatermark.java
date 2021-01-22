package cn.ctyun.oos.server.processor.image.thumbnailator.watermark;

import java.awt.AlphaComposite;
import java.awt.Color;
import java.awt.Font;
import java.awt.Graphics2D;
import java.awt.Point;
import java.awt.font.FontRenderContext;
import java.awt.image.BufferedImage;

import cn.ctyun.oos.server.processor.image.ImageProcessorConsts;
import cn.ctyun.oos.server.processor.image.util.PositionHelper;
import cn.ctyun.oos.server.processor.image.util.PositionHelper.Padding;
import net.coobird.thumbnailator.filters.ImageFilter;
import net.coobird.thumbnailator.geometry.Position;

public class MixWatermark  implements ImageFilter
{
   

    
    public enum ALIGN{
        ALIGN_TOP,
        ALIGN_CENTER,
        ALIGN_BOTTOM
    }
    public enum ORDER{
        ORDER_IMAGE_TEXT,
        ORDER_TEXT_IMAGE,
    }
    
    /**
     * The text of the caption.
     */
    private final String caption;
    
    /**
     * The font of text to add.
     */
    private final Font font;
    
    /**
     * The color of the text to add.
     */
    private final Color c;
    
    /**
     * The opacity level of the text to add.
     * <p>
     * The value should be between {@code 0.0f} to {@code 1.0f}, where
     * {@code 0.0f} is completely transparent, and {@code 1.0f} is completely
     * opaque.
     */
    private final float alpha;
    
    /**
     * The position at which the text should be drawn.
     */
    private final Position position;
    
    /** 
     * The insets for the text to draw.
     */
    //private final int insets;
    /**
     * 水平方向移动像素
     */
    private final int x;
    /**
     * 垂直方向移动像素
     */
    private final int y;
    
    /**
     * 左中，中，右中垂直移动像素
     */
    private final int voffset;
    
    /**
     * 水印阴影透明度
     */
    private final float shadowTransparent;
   
    /**
     * 水印图片
     */
    private final BufferedImage watermarkImg;
 
    
    /**
     * 垂直方向移动像素
     */
    private final ALIGN align;
    
    /**
     * 垂直方向移动像素
     */
    private final ORDER order;
    /**
     * 垂直方向移动像素
     */
    private final int interval;
    /**
     * 
     * @param caption
     * @param font
     * @param c
     * @param alpha
     * @param position
     * @param x
     * @param y
     * @param watermarkImg      水印图片
     * @param align    文字，图片水印前后顺序     取值范围：[0, 1] order = 0 图片在前(默认值)； order = 1 文字在前。
     * @param order    参数意义：文字、图片对齐方式    取值范围：[0, 1, 2] align = 0 上对齐(默认值) align = 1 中对齐 align = 2 下对齐
     * @param interval 文字和图片间的间距 取值范围: [0, 1000]
     * @param voffset
     * @param shadowTransparent
     */
    public MixWatermark(String caption, Font font, Color c, float alpha,
            Position position, int x,int y,BufferedImage watermarkImg,ALIGN align,ORDER order,int interval,int voffset,float shadowTransparent)
    {
        this.caption = caption;
        this.font = font;
        this.c = c;
        this.alpha = alpha;
        this.position = position;
       // this.insets = insets;
        this.x=x;
        this.y=y;
        
        this.watermarkImg = watermarkImg;
        this.align = align;
        this.order = order;
        this.interval = interval;
        this.voffset = voffset;
        this.shadowTransparent = shadowTransparent;
    }

   

    public BufferedImage apply(BufferedImage img)
    {
        //BufferedImage newImage = BufferedImages.copy(img);
        
        Graphics2D g = img.createGraphics();
        g.setFont(font);
       
        
        int imageWidth = img.getWidth();
        int imageHeight = img.getHeight();
        
       
//        int captionWidth = g.getFontMetrics(font).stringWidth(caption);
//        int captionHeight = g.getFontMetrics(font).getHeight() ;/// 2;
//        int ascent = g.getFontMetrics(font).getAscent();
//        int desent = g.getFontMetrics(font).getDescent();
        FontRenderContext context = g.getFontRenderContext();
        int captionWidth = (int)Math.floor(font.getStringBounds(caption, context).getWidth());
        int captionHeight=(int)Math.floor(font.getStringBounds(caption, context).getHeight());
        
        //图片起始位置
        Point imagePoint = null;
        //文字起始位置
        Point textPoint = null;
        
        Padding textPadding = PositionHelper.getInstance().calculate(position, x, y);
        
        int totalWidth = captionWidth + watermarkImg.getWidth() + interval;
        int totalHeight = Math.max(captionHeight, watermarkImg.getHeight());
       
        if(order == ORDER.ORDER_IMAGE_TEXT){
            imagePoint = position.calculate(
                    imageWidth, imageHeight, totalWidth, totalHeight,
                    textPadding.getInsertLeft(), textPadding.getInsertRight(), textPadding.getInsertTop(), textPadding.getInsertBottom()
            );
            imagePoint = PositionHelper.getInstance().calculateVoffset(position,imagePoint,voffset);
            textPoint = new Point();
            textPoint.x = imagePoint.x + watermarkImg.getWidth() + interval;       
            textPoint.y = imagePoint.y;
            
        }else{
            textPoint = position.calculate(
                    imageWidth, imageHeight, totalWidth, totalHeight,
                    textPadding.getInsertLeft(), textPadding.getInsertRight(), textPadding.getInsertTop(), textPadding.getInsertBottom()
            );
            textPoint = PositionHelper.getInstance().calculateVoffset(position,textPoint,voffset);
            
            imagePoint = new Point();
            imagePoint.x = textPoint.x + captionWidth + interval;       
            imagePoint.y = textPoint.y;
        }
        
        int dis = Math.abs(captionHeight - watermarkImg.getHeight());
        int halfDis = dis/2;
        if(align == ALIGN.ALIGN_TOP){
            //do nothing...
        }else if(align ==  ALIGN.ALIGN_CENTER){
            if(captionHeight > watermarkImg.getHeight()){
                imagePoint.y = imagePoint.y+halfDis; 
            }else{
                textPoint.y = textPoint.y+halfDis;
            }
        }else if(align == ALIGN.ALIGN_BOTTOM){
            if(captionHeight > watermarkImg.getHeight()){
                imagePoint.y = imagePoint.y+dis; 
            }else{
                textPoint.y = textPoint.y+dis;
            }
        }
        
        //double yRatio = p.y / (double)img.getHeight();
        //int yOffset = (int)((1.0 - yRatio) * captionHeight);
        int yOffset =(int) Math.floor(captionHeight*0.8);
        
        Color fontShadowColor  = ImageProcessorConsts.DEFAULT_WATERMARK_FONT_SHADOW_COLOR;
        int fontsize = font.getSize();
        if (fontShadowColor != null) {
            g.setColor(fontShadowColor);
            g.setComposite(
                    AlphaComposite.getInstance(AlphaComposite.SRC_OVER, shadowTransparent)
            );
            int shadowTranslation = PositionHelper.getInstance().getShadowTranslation(fontsize);
            g.drawString(caption, textPoint.x + shadowTranslation,  textPoint.y + yOffset  + shadowTranslation);
        }
        
        g.setColor(c);
        g.setComposite(
                AlphaComposite.getInstance(AlphaComposite.SRC_OVER, alpha)
        );
        g.drawString(caption, textPoint.x, textPoint.y  + yOffset );
       // g.drawString(caption, textPoint.x, textPoint.y + font.getSize());
        
        g.drawImage(watermarkImg, imagePoint.x, imagePoint.y, null);
        g.dispose();
        
       
        
        return img;
    }
    
    
}