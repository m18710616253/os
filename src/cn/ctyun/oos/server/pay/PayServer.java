package cn.ctyun.oos.server.pay;

import java.io.IOException;
import java.util.Date;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.server.nio.SelectChannelConnector;

import com.amazonaws.HttpMethod;
import com.amazonaws.services.s3.internal.ServiceUtils;

import cn.ctyun.common.Consts;
import cn.ctyun.common.Program;
import cn.ctyun.common.conf.OOSConfig;
import cn.ctyun.oos.server.db.dbpay.DbResponse;

public class PayServer implements Program {
    public static void main(String[] args) throws Exception {
        new PayServer().exec(args);
    }
    
    @Override
    public String usage() {
        return "usage /n";
    }
    
    @Override
    public void exec(String[] args) throws Exception {
        System.setProperty("log4j.log.app", "pay");
        Server server = new Server();
        server.setSendServerVersion(false);
        SelectChannelConnector connector = new SelectChannelConnector();
        connector.setPort(9095);
        connector.setMaxIdleTime(OOSConfig.getSocketlIdleTimeout());
        connector.setRequestHeaderSize(Consts.CONNECTOR_REQUEST_HEADER_SIZE);
        server.setConnectors(new Connector[] { connector });
        server.setHandler(new HttpHandler());
        server.start();
        server.join();
    }
}

class HttpHandler extends AbstractHandler {
    private static final Log log = LogFactory.getLog(HttpHandler.class);
    
    public void handle(String target, Request baseRequest, HttpServletRequest request,
            HttpServletResponse response) throws IOException, ServletException {
        String method = request.getMethod();
        if ((request.getParameter("SIGN") != null) && (method.equals(HttpMethod.POST.toString()))) {
            log.info("sign = " + request.getParameter("SIGN"));
            DbResponse dr = new DbResponse();
            dr.ATTACH = request.getParameter("ATTACH");
            if (request.getParameter("ATTACHAMOUNT") != null)
                dr.ATTACHAMOUNT = Integer.parseInt(request.getParameter("ATTACHAMOUNT"));
            dr.BANKID = request.getParameter("BANKID");
            dr.CURTYPE = request.getParameter("CURTYPE");
            dr.ENCODETYPE = Integer.parseInt(request.getParameter("ENCODETYPE"));
            dr.ORDERAMOUNT = Integer.parseInt(request.getParameter("ORDERAMOUNT"));
            dr.ORDERREQTRANSEQ = request.getParameter("ORDERREQTRANSEQ");
            dr.ORDERSEQ = request.getParameter("ORDERSEQ");
            dr.PRODUCTAMOUNT = Integer.parseInt(request.getParameter("PRODUCTAMOUNT"));
            dr.RETNCODE = request.getParameter("RETNCODE");
            dr.RETNINFO = request.getParameter("RETNINFO");
            dr.SIGN = request.getParameter("SIGN");
            dr.TRANDATE = Integer.parseInt(request.getParameter("TRANDATE"));
            dr.UPTRANSEQ = request.getParameter("UPTRANSEQ");
            try {
                String st = ResponseBill.checkPayment(dr);
                if (st != null) {
                    response.setContentType("text/html;charset=utf-8");
                    response.setStatus(HttpServletResponse.SC_OK);
                    baseRequest.setHandled(true);
                    response.getWriter().write(st);
                }
            } catch (Exception e) {
                log.error(ServiceUtils.formatIso8601Date(new Date()) + " " + e.getMessage());
            }
        }
    }
}