package cn.ctyun.oos.server.conf;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.input.SAXBuilder;

import cn.ctyun.common.BaseException;
import cn.ctyun.oos.common.Utils;
import cn.ctyun.oos.metadata.AccelerateMeta;

public class AccelerateXmlParser {
    private static SAXBuilder xmlBuilder  = new SAXBuilder();    
    
    //符合IPV4格式的正则表达式
    private static String ipRegExp = "^(1\\d{2}|2[0-4]\\d|25[0-5]|[1-9]\\d|[1-9])\\."  
            +"(00?\\d|1\\d{2}|2[0-4]\\d|25[0-5]|[1-9]\\d|\\d)\\."  
            +"(00?\\d|1\\d{2}|2[0-4]\\d|25[0-5]|[1-9]\\d|\\d)\\."  
            +"(00?\\d|1\\d{2}|2[0-4]\\d|25[0-5]|[1-9]\\d|\\d)$"; 

    //符合CIDR格式的IP地址段表达式
    private static String cidrRegExp = "^(1\\d{2}|2[0-4]\\d|25[0-5]|[1-9]\\d|[1-9])\\."  
                +"(00?\\d|1\\d{2}|2[0-4]\\d|25[0-5]|[1-9]\\d|\\d)\\."  
                +"(00?\\d|1\\d{2}|2[0-4]\\d|25[0-5]|[1-9]\\d|\\d)\\."  
                +"(00?\\d|1\\d{2}|2[0-4]\\d|25[0-5]|[1-9]\\d|\\d)/(\\d{1,2})$";

    //符合CIDR格式的IPv4地址段表达式
    private static String cidrStrictRegExp = "^(1\\d{2}|2[0-4]\\d|25[0-5]|[1-9]\\d|[1-9])\\."
                +"(00?\\d|1\\d{2}|2[0-4]\\d|25[0-5]|[1-9]\\d|\\d)\\."
                +"(00?\\d|1\\d{2}|2[0-4]\\d|25[0-5]|[1-9]\\d|\\d)\\."
                +"(00?\\d|1\\d{2}|2[0-4]\\d|25[0-5]|[1-9]\\d|\\d)/(\\d|[1-2]\\d|3[0-2])$";
       
    public static Pattern getPatternCompile(String strRegexp){
    return Pattern.compile(strRegexp);
    }    
    
    public static boolean isValidIPAddr(String ip){
        return getPatternCompile(ipRegExp).matcher(ip).matches();
    }
    
    public static boolean isValidCidrAddr(String ip){
        return getPatternCompile(cidrRegExp).matcher(ip).matches();
    }
    
    public static boolean isValidCidrAddr2(String ip){
        return getPatternCompile(cidrStrictRegExp).matcher(ip).matches();
    }
    
    /**
     * 用于解析用户上传的XML文件，以AccelerateConfiguration为root，子节点分别为：
     *      Status:是否开启CDN加速功能，Enabled/Suspended
     *      IPWhiteLists：用户自定义的IP白名单，IPV4/IPV6格式或者CIDR格式的IP地址段，每个
     *                    IP地址使用<IP></IP>标签进行标注；
     * @param bucketName 
     * @param ip 用户提交的XML Stream
     * @return
     * @throws BaseException
     */
    public synchronized static AccelerateMeta parseAccelerateConfiguration
                (String bucketName, InputStream ip) throws BaseException{
        int iCount = 0;
        Document document;
        String status = null;
        ArrayList<String> ipList = null;
        
        try {
            document = xmlBuilder.build(ip);
            Element root = document.getRootElement();
            
            for (Iterator iterator = root.getChildren().iterator(); iterator
                    .hasNext();) {
                Element child = (Element) iterator.next();
                if (child.getName().equals("Status")){
                    if (!"enabled".equals(child.getText().toLowerCase()) && 
                            !"suspended".equals(child.getText().toLowerCase())) {
                            throw new BaseException(400, "InvalidStatusParam",
                                    "Input xml file is not valid");
                        }
                    status = child.getText();
                } else if (child.getName().equals("IPWhiteLists")){
                    ipList = new ArrayList<String>();
                    
                    for (@SuppressWarnings("unchecked")
                    Iterator<Element> items = child.getChildren().iterator(); items.hasNext();) {
                        Element ipNode = items.next();
                        
                        if (!ipNode.getName().toLowerCase().equals("ip")) continue;
                        
                        String ipAddress = ipNode.getText().trim();
                        if (ipAddress == null || ipAddress.equals("")) continue;
                        
                        //IP地址和IP地址段的验证
                        if (!isValidIPAddr(ipAddress) && !isValidCidrAddr2(ipAddress) && !Utils.isValidIpv6Addr(ipAddress) && !Utils.isValidIpv6Cidr(ipAddress)) {
                            throw new BaseException(400, "InvalidIpAddress:"+ipAddress,
                                "Input xml file is not valid");
                        }
                        ipList.add(ipAddress);
                    }
                    
                    //去重
                    ipList = (ArrayList<String>) ipList.stream().distinct().collect(Collectors.toList());
                    
                    //不能超过五个IP地址
                    if (ipList.size()>5){
                        throw new BaseException(400, "NotMoreThan5IPSections",
                                "Input xml file is not valid");
                    }
                }
            }
              
            return new AccelerateMeta(bucketName, status, ipList);
        } catch (JDOMException | IOException e) {
            throw new BaseException(400, "InvalidAccelerateParam",
                    "Input xml file is not valid");
        }    
    }
}
