package cn.ctyun.oos.server.processor.image;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.Bytes;

import cn.ctyun.common.Consts;
import cn.ctyun.common.conf.OOSConfig;
import cn.ctyun.oos.hbase.MetaClient;
import cn.ctyun.oos.metadata.BucketMeta;
import cn.ctyun.oos.metadata.ObjectMeta;
import cn.ctyun.oos.server.backoff.Backoff;
import cn.ctyun.oos.server.processor.ChainProcessor;
import cn.ctyun.oos.server.processor.Processor;
import cn.ctyun.oos.server.processor.Processor.ProcessorException;
import cn.ctyun.oos.server.processor.image.ImageEvent.ImageDetail;
import cn.ctyun.oos.server.processor.image.ImageEvent.ImageParams;
import cn.ctyun.oos.server.processor.image.ImageProcessorConsts.OPT_TYPE;
import cn.ctyun.oos.server.processor.image.antispam.PornProcessor;
import cn.ctyun.oos.server.processor.image.gifmerge.MergeGifProcessor;
import cn.ctyun.oos.server.processor.image.thumbnailator.ExifProcessor;
import cn.ctyun.oos.server.processor.image.thumbnailator.TransferProcessor;
import cn.ctyun.oos.server.processor.image.thumbnailator.WatermarkProcessor;
import cn.ctyun.oos.server.processor.image.util.ImageFormat;
import cn.ctyun.oos.server.processor.image.util.ImageWriteUtils;
import cn.ctyun.oos.server.processor.image.util.ProcessUtils;
import cn.ctyun.oos.server.storage.Storage;
import common.tuple.Pair;

public class ImageProcessor {

    private static Log log = LogFactory.getLog(ImageProcessor.class);
    private static MetaClient client = MetaClient.getGlobalClient();

    public ImageProcessor(){}
    
    /**
     * 获取图片处理流
     * @param input      输入流
     * @param bucket     图片对象所在bucket
     * @param objectName 图片对象的名字
     * @param objSize  源文件大小
     * @param params     一个url由objectName@oosImage|kv_kv...，这里的params只包括kv_kv，不包括@oosImage
     * @return
     * @throws Exception
     */
    public static Pair<Long, InputStream> process2Stream(InputStream input,
            String bucket, String objectName, long objSize, String params) throws Exception{
        InputStream in = null;
        long size = 0;
        Map<String, Object> map = new HashMap<>();
        map.put(ImageParams.URL, params);
       
        
        Pair<Object, ImageFormat> p = process(bucket, objectName, map, input, objSize);
        Object object = p.first();
        if(object instanceof ImageDetail){
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            ImageWriteUtils.write((ImageDetail)object, output);
            byte[] b = output.toByteArray();
            size = b.length;
            in = new ByteArrayInputStream(b);
            log.info("ImageDetail object size:" + size);
        }else if(object instanceof byte[]){
            byte[] data = (byte[])object;
            in = new ByteArrayInputStream(data);
            size = data.length;
            log.info("ImageByte messageLenth:" + data.length);
        }else if(object instanceof String){
            byte[] b = ((String)object).getBytes(Consts.CS_UTF8);
            in = new ByteArrayInputStream(b);
            size = b.length;
            log.info("ImageInfo messageLenth:" + b.length);
        }
        return new Pair<>(size, in);
    }
    
    /**
     * 获取图片处理流
     * @param input      输入流
     * @param output     输出流
     * @param bucket     图片对象所在bucket
     * @param objectName 图片对象的名字
     * @param params
     * @throws Exception
     */
    public static void process2Stream(InputStream input, OutputStream output,
            String bucket, String objectName, String params,long objSize) throws Exception{
        
        Map<String, Object> map = new HashMap<>();
        map.put(ImageParams.URL, params);
        Pair<Object, ImageFormat> p = process(bucket, objectName, map, input,objSize);
        Object object = p.first();
        if(object instanceof ImageDetail){
            ImageWriteUtils.write((ImageDetail)object, output);
        }else if(object instanceof String){
            output.write(Bytes.toBytes((String)object));
        }
    }
    
    
    /**
     * 1. 从<tt>params<tt>中找出对象名和命令集<br>
     * 2. 检查管道个数，并分别检查各个管道内命令类型、校验各个参数的正确性<br>
     * 3. 把各个管道命令封装到并注册到<tt>ChainProcessor<tt>上<br>
     * 4. 得到最后一个管道的处理结果，返回给客户
     * @param bucketName
     *            Bucket名字
     * @param objectName
     *            对象名字
     * @param params
     *            参数串
     * @param input
     *            输入流
     * @param objSize 流长度（文件大小）
     * @return key是返回对象，如果key返回的对象是图片流，则value表示图片格式
     * @throws Exception 
     */
    public static Pair<Object, ImageFormat> process(String bucketName, String objectName, Map<String, Object> params, 
            InputStream input,long objSize) throws Exception {
        log.info("Start process image. Bucket:"+bucketName+" ObjectName:"+objectName+" Params:" + params);
        long begin = System.currentTimeMillis();
        String url = null;
        if(null == params || null == (url = (String)params.get(ImageParams.URL)))
            throw new ProcessorException(401, "Invalid request.");
        
        if(objSize > ImageProcessorConsts.MAX_IMAGE_SIZE)
            throw new ProcessorException(401, "Too big image. size:" + objSize);
        ImageFormat destImageFormat = null;
//      int index = objectName.lastIndexOf(".");
//      if(index == -1 || null == (srcFormat = ImageFormat.getImageFormat(
//              objectName.substring(index + 1, objectName.length()))))
//          throw new ProcessorException(401, "Invalid image format. params:" + params);
        if(url.startsWith("|")){
            url = url.substring(1);
        }
        String[] commonds = url.split("\\|");
        if (commonds.length > ImageProcessorConsts.MAX_PIPELINE)
            throw new ProcessorException(401,
                    "Too much pipelines. Allowed to process up to "
                            + ImageProcessorConsts.MAX_PIPELINE + " pipelines. params:" + params);

        ChainProcessor chain = new ChainProcessor();
        Processor processor = null;
        
        if(url.equals(ImageParams.INFO_EXIF)){
            HashMap<String, Object> map = new HashMap<>(1);
            map.put(url, "");
            processor = getProcessor(OPT_TYPE.INFO, map, input, objSize);
            chain.register(url, processor);
        }else{
            for (int i = 0; i < commonds.length; i++) {
                String cmd = commonds[i];
                if(cmd.trim().length()==0){
                    //空的跳过
                    continue;
                } 
                
                Pair<OPT_TYPE, HashMap<String, Object>> p = filterCommond(cmd);
                HashMap<String, Object> map = p.second();
                map.put(ImageParams.SOURCE_BUCKET, bucketName);
                map.put(ImageParams.OBJECT_NAME, objectName);
                if(map.containsKey(ImageParams.RELATIVE_PERCENTAGE) &&
                        params.containsKey(ImageParams.SRC_WIDTH) &&
                        params.containsKey(ImageParams.SRC_HEIGHT)){
                    int percentage = (Integer)map.get(ImageParams.RELATIVE_PERCENTAGE);
                    int width = (Integer)params.get(ImageParams.SRC_WIDTH);
                    int height = (Integer)params.get(ImageParams.SRC_HEIGHT);
                    width = width * percentage / 100;
                    height = height * percentage / 100;
                    map.put(ImageParams.WIDTH, width);
                    map.put(ImageParams.HEIGHT, height);
                }
                processor = getProcessor(p.first(), map, input, objSize);
                chain.register(cmd, processor);
                destImageFormat = (ImageFormat)map.get(ImageParams.DEST_IMAGE_FORMAT);
                if(p.first() == OPT_TYPE.PORN){
                    //porn后面内容不处理
                    break;
                }else if(p.first() == OPT_TYPE.MERGE_GIF){
                    //mergegif后面内容不处理
                    break;
                }
            }
        }
        if(processor == null){
            throw new ProcessorException(401, "Invalid request.argument is empty.");
        }
        chain.start();
        Object result = processor.getResult();
        log.info("End process image. Bucket:"+bucketName+" ObjectName:"+objectName+" Params:" + params + " Use(ms):"+(System.currentTimeMillis()-begin));
        return new Pair<>(result, destImageFormat);
    }

    /**
     * 处理水印
     * @param bucketName
     * @param params
     * @param sourceWidth
     * @param sourceHeight
     * @return
     * @throws Exception
     */
    public static ImageDetail processWaterMark(String bucketName, String params, 
            int sourceWidth, int sourceHeight) 
            throws Exception {
        
        if (null == params)
            throw new ProcessorException(400, "Invalid arguments.");
        String url = null;
        String[] items = params.split(ImageParams.DELIMITER.replace("|", "\\|"));
        String objectName = items[0];
        if(items.length == 1)
            url = "." + ImageFormat.getDesc(ImageFormat.JPEG);
        else
            url = items[1];
        
        BucketMeta dbBucket = new BucketMeta(bucketName);
        client.bucketSelect(dbBucket);
        ObjectMeta meta = new ObjectMeta(objectName, bucketName, dbBucket.metaLocation);
        if(client.isBusy(meta, OOSConfig.getMaxConcurrencyPerRegionServer()))
            Backoff.backoff();
        client.objectSelect(meta);
        if(null == meta || null == meta.storageId)
            throw new ProcessorException(400, "Watermark image not exist.");
        
        if(meta.size > ImageProcessorConsts.MAX_IMAGE_SIZE)
            throw new ProcessorException(404, "Too big image. size:" + meta.size 
                    + ", ostorKey:" + meta.storageId);        
        Storage storage = new Storage(null, meta.ostorId);
        String storageId = Storage.getStorageId(meta.storageId);        
        InputStream input = storage.read(storageId, 0, meta.size, objectName, meta.ostorPageSize, meta.storageClass);
        ImageDetail result = null;
        try {
            Map<String, Object> map = new HashMap<>();
            map.put(ImageParams.URL, url);
            map.put(ImageParams.SRC_WIDTH, sourceWidth);
            map.put(ImageParams.SRC_HEIGHT, sourceHeight);
            Pair<Object, ImageFormat> p = process(bucketName, objectName, map, input,meta.size);
            Object o = p.first();
            if(o instanceof ImageDetail)
                result = (ImageDetail)o;
        } finally {
            if (null != input)
                try {
                    input.close();
                } catch (IOException e) {}
        }
        return result;
    }

    /**
     * 根据图像的操作类型，来实例化不同的Processor
     * @param type
     * @param params
     * @param data
     * @param size 流长度（文件大小）
     * @return
     */
    private static Processor getProcessor(OPT_TYPE type, 
            HashMap<String, Object> params, Object data, long size){
        
        Processor p = null;
        ImageEvent event = new ImageEvent(params, data, size);
        if(type == OPT_TYPE.IMAGE){
            p = new TransferProcessor(event);
        }else if(type == OPT_TYPE.INFO){
            p = new ExifProcessor(event);
        }else if(type == OPT_TYPE.WATER_MARK){
            p = new WatermarkProcessor(event);
        }else if(type == OPT_TYPE.PORN){
            p = new PornProcessor(event);
        }else if(type == OPT_TYPE.MERGE_GIF){
            p = new MergeGifProcessor(event);
        }
        return p;
    }
    
    /**
     * 过滤命令参数
     * @param commond
     * @return
     * @throws ProcessorException
     */
    private static Pair<OPT_TYPE, HashMap<String, Object>> filterCommond(
            String commond) throws ProcessorException {

        HashMap<String, Object> map = new HashMap<>();
        OPT_TYPE optType = null;
        if(commond.equals(ImageParams.PORN)){
            optType = OPT_TYPE.PORN;
        }else if(commond.startsWith(ImageParams.MERGE_GIF)){
            //GIF 合并
            optType = OPT_TYPE.MERGE_GIF;
            String[] commonds = commond.split("&");
            List<String> objectUrls = new ArrayList<>();
            for (int i=1;i<commonds.length;i++) {
                String cmd = commonds[i];
                String[] pair = cmd.split("=");
                if(pair.length!=2){
                    log.info("ignore error parm:"+cmd);
                    continue;
                }
                String key = pair[0];
                String value = pair[1];
                if(key.equals(ImageParams.OBJECT)){
                    objectUrls.add(ProcessUtils.base64Decode(value));
                } else {
                    map.put(key, value);
                }
            }
            map.put(ImageParams.OBJECT, objectUrls);
        } else if (commond.contains(ImageParams.WATER_MARK)) {
            // 水印处理
            optType = OPT_TYPE.WATER_MARK;
            String[] commonds = commond.split("&");
            for (String cmd : commonds) {
                String[] pair = cmd.split("=");
                assert (pair.length == 2);
                String key = pair[0];
                String value = pair[1];
                if(key.equals(ImageParams.OBJECT) || key.equals(ImageParams.TEXT)
                        || key.equals(ImageParams.TYPE)
                        || key.equals(ImageParams.COLOR)){
                    map.put(key, ProcessUtils.base64Decode(value));
                }else if(value.matches("[0-9]+")){
                    map.put(key, Integer.valueOf(value));
                }else{
                    map.put(key, value);
                }
            }
            // 图像处理
        }else {
            optType = OPT_TYPE.IMAGE;
            Pattern p = Pattern.compile(ProcessUtils.getImageRegex());
            Matcher m = p.matcher(commond);
            if (!m.matches())
                throw new ProcessorException(400, "Invalid arguments. commond:"
                        + commond);
            String destFormatStr = null;
            int index = commond.lastIndexOf(".");
            if (index != -1) {
                destFormatStr = commond.substring(index + 1);
                commond = commond.substring(0, index);
            }
            String[] commonds = commond.split("_");
            for (String cmd : commonds) {
                int position = -1;
                String key, value;
                for (int i = cmd.length() - 1; i >= 0; i--) {
                    if (Character.isDigit(cmd.charAt(i))) {
                        position = i;
                        break;
                    }
                }
                if (position == -1) {
                    key = cmd;
                    value = "";
                } else {
                    key = cmd.substring(position + 1);
                    value = cmd.substring(0, position + 1);
                }
                if(!ProcessUtils.getImageOpts().contains(key))
                    continue;
                if(value.matches("[0-9]+"))
                    map.put(key, Integer.valueOf(value));
                else
                    map.put(key, value);
            }
            if(null != destFormatStr){
                ImageFormat format = ImageFormat.getImageFormat(destFormatStr);
                if(null == format)
                    throw new ProcessorException(400, "Invalid image format. format:" + destFormatStr);
                map.put(ImageParams.DEST_IMAGE_FORMAT, format);
            }
        }
        return new Pair<>(optType, map);
    }
    public static void saveAs() {}

    public static ImageDetail processImage(String bucketName, String params) throws Exception {
        if (null == params)
            throw new ProcessorException(400, "Invalid arguments.");
        String url = null;
        String[] items = params.split(ImageParams.DELIMITER.replace("|", "\\|"));
        String objectName = items[0];
        if(items.length == 1)
            url = "." + ImageFormat.getDesc(ImageFormat.JPEG);
        else
            url = items[1];
        
        BucketMeta dbBucket = new BucketMeta(bucketName);
        client.bucketSelect(dbBucket);
        ObjectMeta meta = new ObjectMeta(objectName, bucketName, dbBucket.metaLocation);
        if(client.isBusy(meta, OOSConfig.getMaxConcurrencyPerRegionServer()))
            Backoff.backoff();
        boolean exist = client.objectSelect(meta);
        if(!exist)
            throw new ProcessorException(400, "gif image not exist. bucket:"+bucketName+", object:"+objectName);
        
        if(meta.size > ImageProcessorConsts.MAX_IMAGE_SIZE)
            throw new ProcessorException(404, "Too big image. size:" + meta.size +" bucket:"+bucketName+", object:"+objectName
                    + ", ostorKey:" + meta.storageId);
        Storage storage = new Storage(null, meta.ostorId);
        String storageId = Storage.getStorageId(meta.storageId);        
        InputStream input = storage.read(storageId, 0, meta.size, objectName, meta.ostorPageSize, meta.storageClass);
        ImageDetail result = null;
        try {
            Map<String, Object> map = new HashMap<>();
            map.put(ImageParams.URL, url);
            Pair<Object, ImageFormat> p = process(bucketName, objectName, map, input,meta.size);
            Object o = p.first();
            if(o instanceof ImageDetail)
                result = (ImageDetail)o;
        } finally {
            if (null != input)
                try {
                    input.close();
                } catch (IOException e) {}
        }
        return result;
    }
}
