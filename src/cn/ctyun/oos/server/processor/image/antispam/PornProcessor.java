package cn.ctyun.oos.server.processor.image.antispam;

import cn.ctyun.oos.server.conf.ContentSecurityConfig;
import cn.ctyun.oos.server.processor.EventBase;
import cn.ctyun.oos.server.processor.Processor;
import cn.ctyun.oos.server.processor.image.ImageEvent;
import cn.ctyun.oos.server.processor.image.ImageEvent.ImageParams;

public class PornProcessor extends Processor {

    
    private String response="";
    public PornProcessor(EventBase event) {
        super(event);
    }

    @Override
    public void process() throws Exception {
        ImageEvent imageEvent = (ImageEvent) event;
        String key = (String)imageEvent.getParamValue(ImageParams.OBJECT_NAME);
        response = NeteaseDun.decodeImage(ContentSecurityConfig.secretIdNeteaseDun, ContentSecurityConfig.imageBusinessIdNeteaseDun, ContentSecurityConfig.getSecretKey(), key, imageEvent.getInputStream());
    }
    
    @Override
    public Object getResult() {
        return response;
    }
    
}
