package cn.ctyun.oos.server.processor.image.util;

import java.io.IOException;

import cn.ctyun.oos.server.processor.Processor.ProcessorException;
import cn.ctyun.oos.server.processor.image.ImageProcessorConsts;
import cn.ctyun.oos.server.processor.image.ImageProcessorConsts.EdgeMode;
import cn.ctyun.oos.server.processor.image.ImageProcessorConsts.LargeMode;
import net.coobird.thumbnailator.Thumbnails.Builder;
import net.coobird.thumbnailator.geometry.Position;
import net.coobird.thumbnailator.geometry.Positions;

public class TransferHelper {


    /**
     * 实现强制按比例缩放功能
     * @param builder
     *          由InputStream构建的builder
     * @param percentage
     *          缩放比列
     * @return  设置后的builder       
     **/    
    public static  Builder<?> scale(Builder<?> builder, float ratio, int srcW, int srcH, LargeMode lMode) throws ProcessorException{
        if((ratio*srcW>ImageProcessorConsts.MAX_DES_EDGE_LENGHT)||(ratio*srcH>ImageProcessorConsts.MAX_DES_EDGE_LENGHT)||(ratio*srcW*ratio*srcH>ImageProcessorConsts.MAX_DES_SQUARE)){
            throw new ProcessorException(400, "The size of destination image has exceeded the maximum value!");
        }else{
            if(lMode==LargeMode.Normal&&ratio>1){
               return null;              
            }
            builder.scale(ratio);
            return builder;
        }
    }
    
    /**
     * 实现只指定宽度或者高度，按比例缩放
     * @param builder
     *          由InputStream构建的builder
     * @param percentage
     *          由指定的宽度或者高度计算得出的缩放比列
     * @param larger
     *          大于原图是否处理（默认0代表处理，1代表不处理）
     * @return  设置后的builder
     **/
    public  static Builder<?> scale(Builder<?> builder, int w, int h, int srcW, int srcH, LargeMode lMode) throws ProcessorException{
        if(w != ImageProcessorConsts.DEFAULT_DEST_WIDTH_LENGTH){
            float percentage = (float)w/(float)srcW;
            if((w>ImageProcessorConsts.MAX_DES_EDGE_LENGHT)||(w*srcH*percentage>ImageProcessorConsts.MAX_DES_SQUARE)){
                throw new ProcessorException(400, "The size of destination image has exceeded the maximum!");
            }else {
                if(lMode==LargeMode.Normal&&w>srcW){
                    return null;  
                }   
                builder.scale(percentage);
                return builder;
            }
        }else if(h != ImageProcessorConsts.DEFAULT_DEST_HEIGHT_LENGTH){
            float percentage = (float)h/(float)srcH;
            if((w>ImageProcessorConsts.MAX_DES_EDGE_LENGHT)||(w*srcH*percentage>ImageProcessorConsts.MAX_DES_SQUARE)){
                throw new ProcessorException(400, "The size of destination image has exceeded the maximum!");
            }else {
                if(lMode==LargeMode.Normal&&h>srcH){
                    return null;  
                }   
                builder.scale(percentage);
                return builder;
            }
        }
        return null;
    }
    
    /**
     * 同时指定宽度和高度，按比例或者强制缩放
     * @param builder
     *          输入图像对象
     * @param w
     *          要求的宽度
     * @param h
     *          要求的高度
     * @param ePriority
     *          指定缩放模式(默认0代表长边优先，1代表短边优先，2代表强制按指定高度和宽度缩放)
     * @param larger
     *          大于原图是否处理（默认0代表处理，1代表不处理）
     * @return  被缩放处理后的图像
     **/
    public  static Builder<?> scale(Builder<?> builder, int w, int h, int srcW, int srcH, EdgeMode eMode, LargeMode lMode) throws ProcessorException{
        if((w>ImageProcessorConsts.MAX_DES_EDGE_LENGHT)||(h>ImageProcessorConsts.MAX_DES_EDGE_LENGHT)||(w*h>ImageProcessorConsts.MAX_DES_SQUARE)){
            throw new ProcessorException(400, "The size of destination image has exceeded the maximum!");
        }else if(eMode==EdgeMode.FORCE){//不按原图比例强制缩放
            if(lMode==LargeMode.Normal&&(w>srcW||h>srcH)){
                return null;  
            }
            builder.forceSize(w, h);
            return builder; 
                
        }else if(eMode==EdgeMode.SHORT){//按原图比列，进行短边优先缩放
            float ratio = ((float)w/(float)srcW)>((float)h/(float)srcH)?((float)w/(float)srcW):((float)h/(float)srcH);
            if(lMode==LargeMode.Normal&&ratio>1f){
                return null;  
            }
            builder.scale(ratio);
            return builder; 
        }else if(eMode==EdgeMode.LONG || eMode==EdgeMode.NONE){//按原图比例，进行长边优先缩放
            float ratio =((float)w/(float)srcW)<((float)h/(float)srcH)?((float)w/(float)srcW):((float)h/(float)srcH);
            if(lMode==LargeMode.Normal&&ratio>1f){
                return null;  
            }
            builder.scale(ratio);
            return builder; 
        }
        return null;
    }
    
	/**
	 * 自动裁剪从按短边优先缩略的图中间进行裁剪
	 * @param builder
	 * @param width
	 * @param height
	 * @param e 如果目标缩略图大于原图是否处理，值是1, 即不处理，是0，表示处理  0/1, 默认是0
	 * @return 修改返回builder ,不修改返回null
	 * @throws IOException
	 */
	public static Builder<?> crop(Builder<?> builder,int width,int height,LargeMode e,int srcWidth,int srcHeight) throws IOException{
		if(builder==null){
			return null;
		}
		
		int w = srcWidth;
		int h = srcHeight;
		//如果目标缩略图大于原图不处理
		if(e == LargeMode.Normal){
			if(w<width || h<height){
				return null;
			}
		}
		//.crop(Positions.TOP_LEFT)  4096x4096 
		return builder.size(width, height).crop(Positions.CENTER);
		
	}
	
	/**
	 * 区域裁剪
	 * @param image
	 * @param width
	 * @param height
	 * @param pos
	 * @return
	 * @throws IOException
	 */
	public static Builder<?> crop(Builder<?> builder,int width,int height,int pos,int srcWidth,int srcHeight) throws IOException{
		if(builder==null){
			return null;
		}
		
		int w = srcWidth;
		int h = srcHeight;		
		
		if(height==0){
			height = h;
		}
		
		if(width==0){
			width = w;
		}
		Position position = PositionHelper.getPosition(pos);
		if(position == null){
			//超出范围
			throw new RuntimeException("Position is invalid.");
		}
		
		
		return builder.scale(1).sourceRegion(position, width, height);
		
		
		
	}
	/**
	 * 高级裁剪
	 * @param image
	 * @param x
	 * @param y
	 * @param height
	 * @param width
	 * @return
	 * @throws IOException
	 */
	public static Builder<?>  crop(Builder<?> builder,int x,int y,int height,int width,int srcWidth,int srcHeight) throws IOException{
		if(builder==null){
			return null;
		}
		
		int w = srcWidth;
		int h = srcHeight;

		if(height==0){
			height = h-y;
		}
		
		if(width==0){
			width = w-x;
		}
		//裁剪图与原图大小相同，不修改
		if(x==0 && y ==0 && width==w && height == h){
		    return null;
		}
		return builder.sourceRegion( x, y,width,height).scale(1);
		
		
		
	}
	

}
