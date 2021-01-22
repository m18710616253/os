package cn.ctyun.oos.server.formpost.exceptions;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.s3.internal.XmlWriter;

import cn.ctyun.common.BaseException;
import cn.ctyun.common.Consts;
import cn.ctyun.common.conf.OOSConfig;

public class EntityTooSmallException  extends BaseException{
    private static final long serialVersionUID = 1216109383374688474L;
    public long proposedSize = 0;
    public long minSizeAllowed = 0;
    
    public EntityTooSmallException(int status, String message, long proposedSize, long minSizeAllowed) {
        this.code = "EntityTooSmall";
        this.status = status;
        this.message = message;
        this.proposedSize = proposedSize;
        this.minSizeAllowed = minSizeAllowed;
    }
    
    public EntityTooSmallException(int status, String message, String reqId, String resource, long proposedSize, long minSizeAllowed) {
        this.code = "EntityTooSmall";
        this.status = status;
        this.message = message;
        this.reqId = reqId;
        this.resource = resource;
        this.proposedSize = proposedSize;
        this.minSizeAllowed = minSizeAllowed;
    }
    
    public XmlWriter toXmlWriter() throws UnsupportedEncodingException {
        XmlWriter xml = new XmlWriter();
        xml.start("Error");
        xml.start("Code").value(code).end();
        xml.start("Message").value(message).end();
        xml.start("ProposedSize").value(""+proposedSize).end();
        xml.start("MinSizeAllowed").value(""+minSizeAllowed).end();
        //TODO: 没有Resource
        xml.start("Resource").value(URLDecoder.decode(resource, Consts.STR_UTF8)).end();
        xml.start("RequestId").value(reqId).end();
        xml.start("HostId").value(domain).end();
        xml.end();
        return xml;
    }
}
