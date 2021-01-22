package cn.ctyun.oos.tempo;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

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
import org.eclipse.jetty.util.thread.QueuedThreadPool;

import com.amazonaws.services.s3.Headers;
import com.amazonaws.services.s3.internal.ServiceUtils;

import cn.ctyun.common.Consts;
import cn.ctyun.common.Program;
import cn.ctyun.common.conf.OOSConfig;

public class JettyServerBench implements Program {

    private int maxThreads = 10000;
    private long length = 0;
    private int port = 8080;
    private static final Log l4j = LogFactory.getLog(JettyServerBench.class);

    public static void main(String[] args) throws Exception {
        new JettyServerBench().exec(args);
    }

    @Override
    public String usage() {
        return "Usage: \n";
    }

    JettyServerBench() throws FileNotFoundException {
        InputStream is = null;

        Properties p = new Properties();
        is = new FileInputStream(System.getProperty("user.dir")
                + "/conf/jettyBench.conf");
        try {
            p.load(is);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            l4j.error(e);
        } finally{
            try {
                is.close();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                l4j.error(e);
            }
        }
        maxThreads = Integer.parseInt(p.getProperty("server.threadPool.maxThreads"));
        length = parseLength(p.getProperty("length"));
        port = Integer.parseInt(p.getProperty("port"));
    }

    private long parseLength(String length) {
        Character c = length.charAt(length.length() - 1);
        long result = 0;
        if (Character.isLetter(c)) {
            if (length.endsWith("k") || length.endsWith("K")) {
                result = 1024 * Integer.parseInt(length.substring(0,
                        length.length() - 1));
            } else if (length.endsWith("m") || length.endsWith("M")) {
                result = 1024 * 1024 * Integer.parseInt(length.substring(0,
                        length.length() - 1));
            } else if (length.endsWith("g") || length.endsWith("G")) {
                result = 1024 * 1024 * 1024 * Integer.parseInt(length
                        .substring(0, length.length() - 1));
            }
        } else {
            result = Integer.parseInt(length);
        }
        return result;
    }

    @Override
    public void exec(String[] args) throws Exception {
        QueuedThreadPool pool = new QueuedThreadPool();
        pool.setMaxQueued(maxThreads);
        pool.setMaxThreads(maxThreads);
        /* http://wiki.eclipse.org/Jetty/Tutorial/Embedding_Jetty */
        SelectChannelConnector connector0 = new SelectChannelConnector();
        connector0.setAcceptQueueSize(maxThreads);
        connector0.setThreadPool(pool);
        connector0.setPort(port);
        connector0.setMaxIdleTime(OOSConfig.getSocketlIdleTimeout());
        connector0.setRequestHeaderSize(Consts.CONNECTOR_REQUEST_HEADER_SIZE);
        Server server = new Server();
        server.setSendServerVersion(false);
        server.setConnectors(new Connector[] { connector0 /* , ssl_connector */});
        server.setHandler(new HttpHandler(length));
        server.start();
        server.join();
    }

    class HttpHandler extends AbstractHandler {

        private final byte[] BUF = new byte[] { 0x00, 0x01, 0x02, 0x03, 0x04,
                0x05, 0x06, 0x07 };
        private long length = 0;
        private final AtomicInteger reqNum = new AtomicInteger();

        HttpHandler(long length) {
            this.length = length;
        }

        @Override
        public void handle(String target, Request baseRequest,
                HttpServletRequest request, HttpServletResponse response)
                throws IOException, ServletException {
            l4j.info("handle new request " + reqNum.addAndGet(1));
            response.setStatus(200);
            response.setHeader(Headers.CONTENT_TYPE, "application/octet-stream");
            response.setHeader(Headers.DATE,
                    ServiceUtils.formatRfc822Date(new Date()));
//            response.setHeader(Headers.SERVER, Consts.OOS_SERVICE_NAME);
            response.setHeader("Connection", "close");
            OutputStream os = response.getOutputStream();
            try {
                for (int i = 0; i < length; i++)
                    os.write(BUF);
            } catch (Exception e) {
                l4j.info("os close exception", e);
            } finally {
                if (os != null) {
                    try{
                    os.close();
                    } catch (Exception e){
                        l4j.info("os close exception", e);
                    }
                }
                baseRequest.setHandled(true);
            }
        }
    }
}
