package cn.ctyun.oos.server.processor.image.thumbnailator.watermark;

import java.awt.AlphaComposite;
import java.awt.Graphics2D;
import java.awt.Point;
import java.awt.image.BufferedImage;

import cn.ctyun.oos.server.processor.image.util.PositionHelper;
import cn.ctyun.oos.server.processor.image.util.PositionHelper.Padding;
import net.coobird.thumbnailator.filters.ImageFilter;
import net.coobird.thumbnailator.geometry.Position;

public class ImageWatermark  implements ImageFilter
{
    /**
     * The position of the watermark.
     */
    private final Position position;
    
    /**
     * The watermark image.
     */
    private final BufferedImage watermarkImg;
    
    /**
     * The opacity of the watermark.
     */
    private final float opacity;
    /**
     * 水平移动像素
     */
    private final int x;
    /**
     * 垂直移动像素
     */
    private final int y;
    
    /**
     * 左中，中，右中垂直移动像素
     */
    private final int voffset;
    /**
     * Instantiates a filter which applies a watermark to an image.
     * 
     * @param position          The position of the watermark.
     * @param watermarkImg      The watermark image.
     * @param opacity           The opacity of the watermark.
     *                          <p>
     *                          The value should be between {@code 0.0f} and
     *                          {@code 1.0f}, where {@code 0.0f} is completely
     *                          transparent, and {@code 1.0f} is completely
     *                          opaque.
     */
    public ImageWatermark(Position position, BufferedImage watermarkImg,
            float opacity,int x,int y,int voffset)
    {
        if (position == null)
        {
            throw new NullPointerException("Position is null.");
        }
        if (watermarkImg == null)
        {
            throw new NullPointerException("Watermark image is null.");
        }
        if (opacity > 1.0f || opacity < 0.0f)
        {
            throw new IllegalArgumentException("Opacity is out of range of " +
                    "between 0.0f and 1.0f.");
        }
        
        this.position = position;
        this.watermarkImg = watermarkImg;
        this.opacity = opacity;
        this.x =x;
        this.y =y;
        this.voffset = voffset;
    }

    public BufferedImage apply(BufferedImage img)
    {
        int width = img.getWidth();
        int height = img.getHeight();

        //BufferedImage imgWithWatermark =
        //    new BufferedImageBuilder(width, height, type).build();
        
        int watermarkWidth = watermarkImg.getWidth();
        int watermarkHeight = watermarkImg.getHeight();
        Padding padding = PositionHelper.getInstance().calculate(position, x, y);
        Point p = position.calculate(
                width, height, watermarkWidth, watermarkHeight,
                padding.getInsertLeft(), padding.getInsertRight(), padding.getInsertTop(), padding.getInsertBottom()
        );
        p = PositionHelper.getInstance().calculateVoffset(position,p,voffset);
        Graphics2D g = img.createGraphics();
        
        // Draw the actual image.
        g.drawImage(img, 0, 0, null);
        
        // Draw the watermark on top.
        g.setComposite(
                AlphaComposite.getInstance(AlphaComposite.SRC_OVER, opacity)
        );
        
        g.drawImage(watermarkImg, p.x, p.y, null);
        
        g.dispose();

        return img;
    }
}