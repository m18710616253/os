package cn.ctyun.oos.server.processor.image.thumbnailator.watermark;

import java.awt.AlphaComposite;
import java.awt.Color;
import java.awt.Font;
import java.awt.Graphics2D;
import java.awt.Point;
import java.awt.RenderingHints;
import java.awt.font.FontRenderContext;
import java.awt.image.BufferedImage;

import cn.ctyun.oos.server.processor.image.ImageProcessorConsts;
import cn.ctyun.oos.server.processor.image.util.PositionHelper;
import cn.ctyun.oos.server.processor.image.util.PositionHelper.Padding;
import net.coobird.thumbnailator.filters.ImageFilter;
import net.coobird.thumbnailator.geometry.Position;

public class TextWatermark  implements ImageFilter
{
    
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
     * Instantiates a filter which adds a text caption to an image.
     * 
     * @param caption   The text of the caption.
     * @param font      The font of the caption.
     * @param c         The color of the caption.
     * @param alpha     The opacity level of caption.
     *                  <p>
     *                  The value should be between {@code 0.0f} and
     *                  {@code 1.0f}, where {@code 0.0f} is completely
     *                  transparent, and {@code 1.0f} is completely opaque.
     * @param position  The position of the caption.
     * @param insets    The inset size around the caption.
     */
    public TextWatermark(String caption, Font font, Color c, float alpha,
            Position position, int x,int y,int voffset,float shadowTransparent)
    {
        this.caption = caption;
        this.font = font;
        this.c = c;
        this.alpha = alpha;
        this.position = position;
       // this.insets = insets;
        this.x=x;
        this.y=y;
        this.voffset = voffset;
        this.shadowTransparent = shadowTransparent;
    }

    /**
     * Instantiates a filter which adds a text caption to an image.
     * <p>
     * The opacity of the caption will be 100% opaque.
     * 
     * @param caption   The text of the caption.
     * @param font      The font of the caption.
     * @param c         The color of the caption.
     * @param position  The position of the caption.
     * @param insets    The inset size around the caption.
     */
    public TextWatermark(String caption, Font font, Color c, Position position
            , int x,int y,int voffset,float shadowTransparent)
    {
        this.caption = caption;
        this.font = font;
        this.c = c;
        this.alpha = 1.0f;
        this.position = position;
        // this.insets = insets;
        this.x=x;
        this.y=y;
        this.voffset = voffset;
        this.shadowTransparent = shadowTransparent;
    }

    public BufferedImage apply(BufferedImage img)
    {
     //   BufferedImage newImage = BufferedImages.copy(img);
        Graphics2D g = img.createGraphics();
        
        g.setFont(font);
       
        g.setRenderingHint(
                RenderingHints.KEY_TEXT_ANTIALIASING,
                RenderingHints.VALUE_TEXT_ANTIALIAS_ON);
        g.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);

        int imageWidth = img.getWidth();
        int imageHeight = img.getHeight();
        
 //       int captionWidth = g.getFontMetrics().stringWidth(caption);
//        int captionHeight = g.getFontMetrics().getHeight();
        FontRenderContext context = g.getFontRenderContext();
        int captionWidth = (int)Math.floor(font.getStringBounds(caption, context).getWidth());
        int captionHeight=(int)Math.floor(font.getStringBounds(caption, context).getHeight());
        
        Padding padding = PositionHelper.getInstance().calculate(position, x, y);
        Point p = position.calculate(
                imageWidth, imageHeight, captionWidth, captionHeight,
                padding.getInsertLeft(), padding.getInsertRight(), padding.getInsertTop(), padding.getInsertBottom()
        );
      
        p = PositionHelper.getInstance().calculateVoffset(position,p,voffset);
        
        //double yRatio = p.y / (double)img.getHeight();
        //int yOffset = (int)((1.0 - yRatio) * captionHeight);
        int yOffset =(int) Math.floor(captionHeight*0.8);
        //yOffset = 0;
        Color fontShadowColor  =  ImageProcessorConsts.DEFAULT_WATERMARK_FONT_SHADOW_COLOR;;
        int fontsize = font.getSize();
        //写入水印
        if (fontShadowColor != null) {
            
            g.setColor(fontShadowColor);
            g.setComposite(
                    AlphaComposite.getInstance(AlphaComposite.SRC_OVER, shadowTransparent)
            );
            int shadowTranslation = PositionHelper.getInstance().getShadowTranslation(fontsize);
            g.drawString(caption, p.x + shadowTranslation,  p.y+yOffset  + shadowTranslation);
            
        }
        g.setColor(c);
        g.setComposite(
                AlphaComposite.getInstance(AlphaComposite.SRC_OVER, alpha)
        );
        g.drawString(caption, p.x, p.y +yOffset );
        g.dispose();
        
        return img;
    }
    
}