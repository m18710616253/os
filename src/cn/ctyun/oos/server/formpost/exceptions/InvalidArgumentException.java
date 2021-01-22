package cn.ctyun.oos.server.formpost.exceptions;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.s3.internal.XmlWriter;

import cn.ctyun.common.BaseException;
import cn.ctyun.common.Consts;
import cn.ctyun.common.conf.OOSConfig;

public class InvalidArgumentException  extends BaseException{
    private static final long serialVersionUID = -3975058122395535975L;
    public String argumentName = "";
    public String argumentValue = "";
    
    public InvalidArgumentException(int status, String message, String argumentName, String argumentValue) {
        this.code = "InvalidArgument";
        this.status = status;
        this.message = message;
        this.argumentName = argumentName;
        this.argumentValue = argumentValue;
    }
    
    public InvalidArgumentException(int status, String message, String reqId, String resource, String argumentName, String argumentValue) {
        this.code = "InvalidArgument";
        this.status = status;
        this.message = message;
        this.reqId = reqId;
        this.resource = resource;
        this.argumentName = argumentName;
        this.argumentValue = argumentValue;
    }
    
    public XmlWriter toXmlWriter() throws UnsupportedEncodingException {
        XmlWriter xml = new XmlWriter();
        xml.start("Error");
        xml.start("Code").value(code).end();
        xml.start("Message").value(message).end();
        xml.start("ArgumentName").value(argumentName).end();
        xml.start("ArgumentValue").value(argumentValue).end();
        //TODO: 没有Resource
        xml.start("Resource").value(URLDecoder.decode(resource, Consts.STR_UTF8)).end();
        xml.start("RequestId").value(reqId).end();
        xml.start("HostId").value(domain).end();
        xml.end();
        return xml;
    }
}
