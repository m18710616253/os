package cn.ctyun.oos.server.processor.image;

import java.awt.image.BufferedImage;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import javax.imageio.ImageIO;
import javax.imageio.ImageReadParam;
import javax.imageio.ImageReader;
import javax.imageio.metadata.IIOMetadata;
import javax.imageio.stream.ImageInputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.w3c.dom.Node;

import cn.ctyun.oos.server.processor.EventBase;
import cn.ctyun.oos.server.processor.Processor.ProcessorException;
import cn.ctyun.oos.server.processor.image.util.ImageFormat;
import cn.ctyun.oos.server.processor.image.util.NodeUtils;


public class ImageEvent extends EventBase{
    private static final Log log = LogFactory.getLog(ImageEvent.class);
    /** 流长度（文件大小） */
    protected long size;
    
    public ImageEvent() {
        super();
    }
    
    public ImageEvent(HashMap<String, Object> params, Object data, long size){
        super(params, data);
        this.size = size;
    }
    public long getSize(){
        return this.size;
    }
    public InputStream getInputStream(){
        if(null!=super.data && super.data instanceof InputStream){
            return (InputStream)super.data;
        }
        return null;
    }
    
    public ImageDetail getImageDetail() throws Exception{
        if(null == data)
            return null;
        ImageDetail detail = new ImageDetail();
        if(data instanceof InputStream){
            ImageInputStream imageStream = ImageIO.createImageInputStream((InputStream)data);
            Iterator<ImageReader> readers = ImageIO.getImageReaders(imageStream);
            ImageReader reader = null;
            if(!readers.hasNext()){
                throw new ProcessorException(400, "Invalid image.");
            }
            reader = readers.next();

            List<Node> imnodes = new ArrayList<Node>();
            List<BufferedImage> list = new ArrayList<>();
            try{
                reader.setInput(imageStream, true, true);
                ImageReadParam param = reader.getDefaultReadParam();
                ImageFormat format = ImageFormat.getImageFormat(reader.getFormatName());
                if (null == format){
                    throw new ProcessorException(400, "Invalid image format. format:" + format);
                }
                detail.format = format;
                if(format != ImageFormat.GIF){
                    list.add(reader.read(0, param));
                }else{
                    for(int i = 0; ; i++){
                        try {
                            list.add(reader.read(i, param));
                            imnodes.add(reader.getImageMetadata(i)
                                    .getAsTree(ImageDetail.GIF_IMAGE_METADATA_NAME));
                        } catch (IndexOutOfBoundsException e) {
                            break;
                        } catch (IOException e){
                            log.error("error image format."+e.getMessage(),e);
                        }
                    }
                }
            } finally {
                reader.dispose();
            }
            if(list.isEmpty()){
                throw new ProcessorException(400, "Invalid image.");
            }
            
            detail.bufferedImages = list.toArray(new BufferedImage[list.size()]);
            setData(detail);
            
            if(detail.format == ImageFormat.GIF){
                IIOMetadata streamMetadata = reader.getStreamMetadata();
                Node streamNode =  streamMetadata.getAsTree(ImageDetail.GIF_STREAM_METADATA_NAME);
                detail.gifMetaStream = streamNode;
                detail.gifMetaDatas = imnodes.toArray(new Node[imnodes.size()]);
            }
            
            
        }else if(data instanceof ImageDetail){
            detail =(ImageDetail)data;
        }
        return detail;
    }
    
    
    public void setBufferedImage(BufferedImage data) {
        this.data = data;
    }
    
    public static class ImageDetail{
        public static final String GIF_IMAGE_METADATA_NAME = "javax_imageio_gif_image_1.0";
        public static final String GIF_STREAM_METADATA_NAME = "javax_imageio_gif_stream_1.0";
        
        
        public BufferedImage[] bufferedImages;
        public float quality;
        public ImageFormat format;
        public Node gifMetaStream;
        public Node[] gifMetaDatas;
        //是不是合并
        public boolean mergeGif = false;
        
        /**
         * 如果是除GIF以外的图片，getWidth()与getWidth(0)等价，既返回第一张图片的宽度 如果是GIF，则读取GIF的元信息来获取图片宽度，这个值不一定和getWidth(0)相等
         * 
         * @return 图片宽度
         */
        public int getWidth() {
            if (ImageFormat.GIF == format && gifMetaStream != null) {
                Node screenDescNode = NodeUtils.getChild(gifMetaStream, "LogicalScreenDescriptor");
                if (screenDescNode != null) {
                    return NodeUtils.getIntAttr(screenDescNode, "logicalScreenWidth");
                }
            }

            return getWidth(0);
        }

        /**
         * 如果是除GIF以外的图片，getHeight()与getHeight(0)等价，既返回第一张图片的宽度 如果是GIF，则读取GIF的元信息来获取图片高度，这个值不一定和getHeight(0)相等
         * 
         * @return 图片高度
         */
        public int getHeight() {
            if (ImageFormat.GIF == format && gifMetaStream != null) {
                Node screenDescNode = NodeUtils.getChild(gifMetaStream, "LogicalScreenDescriptor");
                if (screenDescNode != null) {
                    return NodeUtils.getIntAttr(screenDescNode, "logicalScreenHeight");
                }
            }

            return getHeight(0);
        }

        public int getWidth(int index) {
            if (index < 0 || index >= bufferedImages.length) {
                throw new IndexOutOfBoundsException("Just totally have " + bufferedImages.length + " images");
            }

            return bufferedImages[index].getWidth();
        }

        public int getHeight(int index) {
            if (index < 0 || index >= bufferedImages.length) {
                throw new IndexOutOfBoundsException("Just totally have " + bufferedImages.length + " images");
            }

            return bufferedImages[index].getHeight();
        }
    }
    
    public static class ImageParams {
        public static final String DELIMITER = "@oosImage|";
        public static final String WIDTH = "w";
        public static final String HEIGHT = "h";
        public static final String THUMBNAIL_GREATER_THAN_ORIGINAL = "l";
        public static final String THUMBNAIL_PRIOR_SIDE = "e";
        public static final String IS_CUT = "c";
        public static final String ABSOLUTE_PERCENTAGE = "p";
        public static final String ADVANCED_CUT = "a";
        public static final String RELATIVE_QUALITY = "q";
        public static final String ABSOLUTE_QUALITY = "Q";
        public static final String WHITE_FILL = "wh";
        public static final String INFO_EXIF = "infoexif";
        public static final String TRANSPARENT = "t";
        public static final String POSITION = "p";
        public static final String X_MARGIN = "x";
        public static final String Y_MARGIN = "y";
        public static final String VOFFSET = "voffset";
        public static final String WATER_MARK = "watermark";
        public static final String OBJECT = "object";
        public static final String RELATIVE_PERCENTAGE = "P";
        public static final String TEXT = "text";
        public static final String TYPE = "type";
        public static final String COLOR = "color";
        public static final String SIZE = "size";
        public static final String SHADOW = "s";
        public static final String FORMAT_SRC = "src";
        public static final String ORDER = "order";
        public static final String ALIGN = "align";
        public static final String INTERVAL = "interval";
//        public static final String SRC_IMAGE_FORMAT = "srcImageFormat";
        public static final String DEST_IMAGE_FORMAT = "destImageFormat";
        public static final String BUCKET = "bucket";
        public static final String URL = "url";
        public static final String OBJECT_NAME = "objectName";
        public static final String SRC_WIDTH = "sw";
        public static final String SRC_HEIGHT = "sh";
        //鉴黄
        public static final String PORN = "porn";
        //GIF 合并
        public static final String MERGE_GIF = "mergegif";
        public static final String GIF_DELAY_TIME = "delayTime";
        public static final String GIF_LOOP = "loop";
        //源图片所在bucket,用于确定合并的图片所在bucket
        public static final String SOURCE_BUCKET = "sourceBucket";
        
   }

}
