package cn.ctyun.oos.tempo;

import java.io.IOException;
import java.io.OutputStream;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.server.nio.SelectChannelConnector;
import org.eclipse.jetty.util.thread.QueuedThreadPool;

import cn.ctyun.common.Consts;
import cn.ctyun.common.Program;
import cn.ctyun.common.conf.OOSConfig;


public class JettyBench implements Program {

    public static void main(String[] args) throws Exception {
        new JettyBench().exec(args);
    }
    
    @Override
    public String usage() {
        return "Usage: \n";
    }

    @Override
    public void exec(String[] args) throws Exception {
        QueuedThreadPool pool = new QueuedThreadPool();
        int maxThreads = 10000;
        pool.setMaxQueued(maxThreads);
        pool.setMaxThreads(maxThreads);
        /* http://wiki.eclipse.org/Jetty/Tutorial/Embedding_Jetty */
        SelectChannelConnector connector0 = new SelectChannelConnector();
        connector0.setAcceptQueueSize(1024);
        connector0.setThreadPool(pool);
        connector0.setPort(7272);
        connector0.setMaxIdleTime(OOSConfig.getSocketlIdleTimeout());
        connector0.setRequestHeaderSize(Consts.CONNECTOR_REQUEST_HEADER_SIZE);
        Server server = new Server();
        server.setSendServerVersion(false);
        server.setConnectors(new Connector[] { connector0/*, ssl_connector*/ });
        server.setHandler(new HttpHandler());
        server.start();
        server.join();
    }

    class HttpHandler extends AbstractHandler {
        
        private final byte[] BUF = new byte[]{0x00, 0x01, 0x02, 0x03,
                0x04, 0x05, 0x06, 0x07};
        
        @Override
        public void handle(String target, Request baseRequest,
                HttpServletRequest request, HttpServletResponse response)
                throws IOException, ServletException {
            response.setStatus(200);
            OutputStream os = response.getOutputStream();;
            try {
                for(int i=0; i<1024*1024; i++)
                    os.write(BUF);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                os.close();
                baseRequest.setHandled(true);
            }
        }
    }
}
