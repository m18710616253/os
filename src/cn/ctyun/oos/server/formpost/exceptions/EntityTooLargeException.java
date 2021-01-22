package cn.ctyun.oos.server.formpost.exceptions;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.s3.internal.XmlWriter;

import cn.ctyun.common.BaseException;
import cn.ctyun.common.Consts;
import cn.ctyun.common.conf.OOSConfig;

public class EntityTooLargeException  extends BaseException{

    private static final long serialVersionUID = 6124741689564504724L;

    public long proposedSize = 0;
    public long maxSizeAllowed = 0;
    
    public EntityTooLargeException(int status, String message, long proposedSize, long maxSizeAllowed) {
        this.code = "EntityTooLarge";
        this.status = status;
        this.message = message;
        this.proposedSize = proposedSize;
        this.maxSizeAllowed = maxSizeAllowed;
    }
    
    public EntityTooLargeException(int status, String message, String reqId, String resource, long proposedSize, long maxSizeAllowed) {
        this.code = "EntityTooLarge";
        this.status = status;
        this.message = message;
        this.reqId = reqId;
        this.resource = resource;
        this.proposedSize = proposedSize;
        this.maxSizeAllowed = maxSizeAllowed;
    }
    
    public XmlWriter toXmlWriter() throws UnsupportedEncodingException {
        XmlWriter xml = new XmlWriter();
        xml.start("Error");
        xml.start("Code").value(code).end();
        xml.start("Message").value(message).end();
        xml.start("ProposedSize").value(""+proposedSize).end();
        xml.start("MaxSizeAllowed").value(""+maxSizeAllowed).end();
        //TODO: 没有Resource
        xml.start("Resource").value(URLDecoder.decode(resource, Consts.STR_UTF8)).end();
        xml.start("RequestId").value(reqId).end();
        xml.start("HostId").value(domain).end();
        xml.end();
        return xml;
    }
}
