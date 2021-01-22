package cn.ctyun.oos.server.processor.image.thumbnailator;

import org.json.JSONObject;

import cn.ctyun.oos.server.processor.EventBase;
import cn.ctyun.oos.server.processor.Processor;
import cn.ctyun.oos.server.processor.image.ImageEvent;
import cn.ctyun.oos.server.processor.image.thumbnailator.exifInfo.GetExifJson;

/**
 * 获取EXIF信息
 * 
 * @author liHX
 * */
public class ExifProcessor extends Processor {
	private JSONObject json;

	public ExifProcessor(EventBase event) {
		super(event);
	}

	@Override
	public void process() throws ProcessorException {
	    ImageEvent imageEvent = (ImageEvent) event;
		json = GetExifJson.getJsonNotCreatImg(imageEvent
				.getInputStream(),imageEvent.getSize());
	}

	@Override
	public Object getResult() {
		if (null == json)
			return "";
		return json.toString();
	}

}
