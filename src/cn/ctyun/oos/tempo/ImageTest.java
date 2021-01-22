package cn.ctyun.oos.tempo;

import java.io.IOException;

import javax.imageio.ImageIO;
import javax.imageio.stream.ImageOutputStream;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang3.RandomStringUtils;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.server.nio.SelectChannelConnector;

import cn.ctyun.common.Consts;
import cn.ctyun.common.Program;
import cn.ctyun.common.conf.OOSConfig;
import common.util.VerifyImage;

public class ImageTest implements Program {
    class HttpHandler extends AbstractHandler {
        @Override
        public void handle(String target, Request baseRequest,
                HttpServletRequest request, HttpServletResponse resp)
                throws IOException, ServletException {
            String randomStr = RandomStringUtils.randomAlphanumeric(4);
            resp.setHeader("Cache-Control", "no-cache");
            resp.setDateHeader("Expires", 0);
            resp.setContentType("image/png");
            ImageOutputStream stream = ImageIO.createImageOutputStream(resp
                    .getOutputStream());
            try {
                ImageIO.write(VerifyImage.create(Consts.VERIFY_IMAGE_WIDTH,
                        Consts.VERIFY_IMAGE_HEIGHT, randomStr), "PNG", stream);
            } finally {
                stream.close();
            }
            
        }
    }

    public static void main(String[] args) throws Exception {
        new ImageTest().exec(args);
    }

    @Override
    public void exec(String[] args) throws Exception {
        SelectChannelConnector connector0 = new SelectChannelConnector();
        connector0.setPort(6060);
        connector0.setMaxIdleTime(OOSConfig.getSocketlIdleTimeout());
        connector0.setRequestHeaderSize(Consts.CONNECTOR_REQUEST_HEADER_SIZE);
        Server server = new Server();
        server.setConnectors(new Connector[] { connector0 });
        server.setHandler(new HttpHandler());
        server.start();
        server.join();
    }

    @Override
    public String usage() {
        return null;
    }
}
