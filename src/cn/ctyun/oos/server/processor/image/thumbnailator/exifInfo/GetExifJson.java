package cn.ctyun.oos.server.processor.image.thumbnailator.exifInfo;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import org.json.JSONException;
import org.json.JSONObject;

import com.drew.imaging.ImageMetadataReader;
import com.drew.imaging.ImageProcessingException;
import com.drew.metadata.Directory;
import com.drew.metadata.Metadata;
import com.drew.metadata.Tag;

import cn.ctyun.oos.server.processor.Processor.ProcessorException;

/**
 * @ivy org="com.drewnoakes" name="metadata-extractor" rev="2.8.1"<BR/>
 * 
 * @单独生成JSON的例子
 * @author liHX
 * 
 * */

public class GetExifJson {

	/**
	 * 获取JSON的静态方法，当不需要生成图像时，可以单独调用
	 * 
	 * @throws JSONException
	 * 
	 * 
	 * @例子 GetMetaJson.getJsonNotCreatImg(new FileInputStream("img.jpg"))
	 * 
	 * */
	public static JSONObject getJsonNotCreatImg(InputStream in, long length)
			throws ProcessorException {
		Metadata metadata = null;
		try {
			metadata = ImageMetadataReader.readMetadata(in);
		} catch (ImageProcessingException e) {
			throw new ProcessorException(500, "readding image error", e);
		} catch (IOException e) {
			throw new ProcessorException(500, "InputStream  error", e);
		}
	
		if (metadata == null) {
			return null;
		}
		ImageInfo imageInfo = readImageTags(metadata);
		imageInfo.FileSize = Long.toString(length);
		/*JSONObject jo = new JSONObject();
		try {
			for (Directory d : metadata.getDirectories()) {
				JSONObject jo2 = new JSONObject();
				for (Tag t : d.getTags()) {
					jo2.put(t.getTagName(), t.getDescription());
				}
				jo.put(d.getName(), jo2);
			}
		} catch (JSONException e) {
			throw new ProcessorException("creat exifJson error", e);
		}*/
		try{
		    return imageInfo.toJsonObj();
		}catch (JSONException e) {
            throw new ProcessorException(500, "creat exifJson error", e);
        }
	}
	
	private static ImageInfo readImageTags(Metadata metadata)
    {
	    ImageInfo imgInfoBean = new ImageInfo ();
        for (Directory directory : metadata.getDirectories()) {
            for (Tag tag : directory.getTags()) {
                String tagName = tag.getTagName();
                String desc = tag.getDescription();
//                System.out.println("[tagName]"+tagName+"[value]:"+desc);
                if (tagName.equals("Date/Time Original")) {
                    //拍摄时间
                    imgInfoBean.DateTime = desc;
                } else if (tagName.equals("File Size")) {
                    //图片大小
                    imgInfoBean.FileSize = desc;
                } else if (tagName.equals("Expected File Name Extension")) {
                    //图片格式
//                    tagName：Detected File Type Name desc:JPEG
//                    tagName：Detected File Type Long Name desc:Joint Photographic Experts Group
//                    tagName：Detected MIME Type desc:image/jpeg
//                    tagName：Expected File Name Extension desc:jpg
//                    tagName：File Name desc:zhaopian_gps.jpg
                    imgInfoBean.Format = desc;
                } else if (tagName.equals("Image Height")) {
                    //图片高度
                    imgInfoBean.ImageHeight = desc;
                } else if (tagName.equals("Image Width")) {
                    //图片宽度
                    imgInfoBean.ImageWidth = desc;
                } /*else if (tagName.equals("GPS Altitude")) {
                    //海拔
                } */else if (tagName.equals("GPS Latitude")) {
                    //纬度
                    imgInfoBean.GPSLatitude = pointToLatlong(desc);
                } else if (tagName.equals("GPS Latitude Ref")) {
                    //纬度
                    imgInfoBean.GPSLatitudeRef = desc;
                } else if (tagName.equals("GPS Longitude")) {
                    //经度
                    imgInfoBean.GPSLongitude = pointToLatlong(desc);
                } else if (tagName.equals("GPS Longitude Ref")) {
                    //经度
                    imgInfoBean.GPSLongitudeRef = desc;
                } else if (tagName.equals("Orientation")) {
                    //
//                    tagName：Orientation desc:Right side, top (Rotate 90 CW)
                    imgInfoBean.Orientation = desc;
                }
            }
        }
        return imgInfoBean;
    }
    /**
     * 经纬度转换  度分秒转换
     * @param point 坐标点
     * @return
     */
    public static String pointToLatlong(String point ) {
        Double du = Double.parseDouble(point.substring(0, point.indexOf("°")).trim());
        Double fen = Double.parseDouble(point.substring(point.indexOf("°")+1, point.indexOf("'")).trim());
        Double miao = Double.parseDouble(point.substring(point.indexOf("'")+1, point.indexOf("\"")).trim());
        Double duStr = du + fen / 60 + miao / 60 / 60 ;
        return duStr.toString();
    }
	public static class ImageInfo {
	   
	    public String FileSize = "";
	    public String Format = "";
	    public String ImageHeight = "";
	    public String ImageWidth = "";
	    
	    public String DateTime= "";
	    public String GPSLatitude= "";
	    public String GPSLatitudeRef= "";
	    public String GPSLongitude= "";
	    public String GPSLongitudeRef= "";
	    public String Orientation= "";
	    
	    public JSONObject toJsonObj() throws JSONException{
	        JSONObject obj = new JSONObject();
	        obj.put("FileSize", new JSONObject().put("value", FileSize));
	        obj.put("Format", new JSONObject().put("value", Format));
	        obj.put("ImageHeight", new JSONObject().put("value", ImageHeight));
	        obj.put("ImageWidth", new JSONObject().put("value", ImageWidth));
	        if(DateTime.length()!=0 || GPSLatitude.length()!=0 || GPSLongitude.length()!=0 || Orientation.length()!=0){
	            obj.put("DateTime", new JSONObject().put("value", DateTime));
	            obj.put("GPSLatitude", new JSONObject().put("value", GPSLatitude));
	            obj.put("GPSLatitudeRef", new JSONObject().put("value", GPSLatitudeRef));
	            obj.put("GPSLongitude", new JSONObject().put("value", GPSLongitude));
	            obj.put("GPSLongitudeRef", new JSONObject().put("value", GPSLongitudeRef));
	            obj.put("Orientation", new JSONObject().put("value", Orientation));
	        }
	        return obj;
	    }
	    
	}
	
	public static void main(String[] args){
//	    ImgInfoBean imgInfoBean = new TestMetadata().parseImgInfo("D:\\gm\\zhaopian_gps.jpg");
	    
	    try {
	        File file = new File("D:\\gm\\zhaopian.jpg");
            JSONObject obj = GetExifJson.getJsonNotCreatImg(new FileInputStream(file),file.length()) ;
            System.out.println(obj.toString());
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (ProcessorException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
	}
	/*
	 * [tagName]Compression Type[value]:Baseline
[tagName]Data Precision[value]:8 bits
[tagName]Image Height[value]:2448 pixels
[tagName]Image Width[value]:3264 pixels
[tagName]Number of Components[value]:3
[tagName]Component 1[value]:Y component: Quantization table 0, Sampling factors 2 horiz/1 vert
[tagName]Component 2[value]:Cb component: Quantization table 1, Sampling factors 1 horiz/1 vert
[tagName]Component 3[value]:Cr component: Quantization table 1, Sampling factors 1 horiz/1 vert
[tagName]ISO Speed Ratings[value]:160
[tagName]Orientation[value]:Right side, top (Rotate 90 CW)
Right side, top (Rotate 90 CW)
[tagName]Model[value]:SCH-N719
[tagName]Date/Time Original[value]:2014:03:10 11:44:23
[tagName]Metering Mode[value]:Center weighted average
[tagName]White Balance Mode[value]:Auto white balance
[tagName]Image Height[value]:2448 pixels
[tagName]Focal Length[value]:3.7 mm
[tagName]Exposure Program[value]:Aperture priority
[tagName]Flash[value]:Flash did not fire
[tagName]White Balance[value]:Unknown
[tagName]Date/Time[value]:2014:03:10 11:44:14
[tagName]Exposure Time[value]:0.03 sec
[tagName]User Comment[value]:?;
[tagName]F-Number[value]:f/2.6
[tagName]Image Width[value]:3264 pixels
[tagName]Make[value]:SAMSUNG
[tagName]GPS Latitude Ref[value]:N
[tagName]GPS Longitude[value]:111° 39' 57"
[tagName]GPS Longitude Ref[value]:E
[tagName]GPS Latitude[value]:40° 47' 55"
[tagName]Unknown tag (0x0201)[value]:474
[tagName]Unknown tag (0x0202)[value]:11446
[tagName]Number of Tables[value]:4 Huffman tables
[tagName]Detected File Type Name[value]:JPEG
[tagName]Detected File Type Long Name[value]:Joint Photographic Experts Group
[tagName]Detected MIME Type[value]:image/jpeg
[tagName]Expected File Name Extension[value]:jpg
	 */
}
