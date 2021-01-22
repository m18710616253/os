package cn.ctyun.oos.server.processor.image.util;


public enum ImageFormat {
    JPEG, PNG, BMP, GIF, WEBP;
    
    public static ImageFormat getImageFormat(String suffix){
        if(suffix == null || "".equals(suffix)){
            return null;
        }
        if("JPEG".equalsIgnoreCase(suffix)){
            return JPEG;
        }else if("JPG".equalsIgnoreCase(suffix)){
            return JPEG;
        }else if("BMP".equalsIgnoreCase(suffix)){
            return BMP;
        }else if("GIF".equalsIgnoreCase(suffix)){
            return GIF;
        }else if("PNG".equalsIgnoreCase(suffix)){
            return PNG;
        }else if("WEBP".equalsIgnoreCase(suffix)){
            return WEBP;
        }else{
            return null;
        }
    }
    
    public static String getDesc(ImageFormat format){
        if(JPEG == format){
            return "JPEG";
        }else if(BMP == format){
            return "BMP";
        }else if(GIF == format){
            return "GIF";
        }else if(PNG == format){
            return "PNG";
        }else if(WEBP == format){
            return "WEBP";
        }else{
            return "UNKNOWN";
        }
    }
    
    public String getDesc() {
        return ImageFormat.getDesc(this);
    }
}
