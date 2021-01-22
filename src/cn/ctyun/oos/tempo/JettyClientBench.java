package cn.ctyun.oos.tempo;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.ProtocolException;
import java.net.URI;
import java.net.URL;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jdom.Document;
import org.jdom.input.SAXBuilder;

public class JettyClientBench {

    private static long length = 0;
    private static int port = 8080;
    private static String host = "oos.ctyun.cn";
    private static int threadNum = 1000;
    private static int reqRate = 500;
    private static final Log l4j = LogFactory.getLog(JettyClientBench.class);
    private static final AtomicInteger getSuccess = new AtomicInteger();
    private static final AtomicInteger getFail = new AtomicInteger();
    private static final AtomicInteger totalGet = new AtomicInteger();

    public static void main(String[] args) throws Exception {
        new JettyClientBench().start();
    }
    
    public void start() {


        Thread ts[] = new Thread[threadNum];
        for (int i = 0; i < ts.length; i++) {
            Thread t = new Thread() {
                @Override
                public void run() {
                    l4j.info("I am " + Thread.currentThread().getId());
                    Random r = new Random();
                    while (true) {
                        try {
                            Thread.sleep(r.nextInt(2 * 1000 * threadNum / reqRate));
                            sendGetRequest(host, port);
                        } catch (Exception e) {
                            // TODO Auto-generated catch block
                            l4j.error(e);
                        }
                    }
                }
            };
            t.start();
        }
    
    }

    JettyClientBench() throws FileNotFoundException {
        InputStream is = null;

        Properties p = new Properties();
        is = new FileInputStream(System.getProperty("user.dir")
                + "/conf/jettyBench.conf");
        try {
            p.load(is);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        length = parseLength(p.getProperty("length"));
        host = p.getProperty("host");
        port = Integer.parseInt(p.getProperty("port"));
        threadNum = Integer.parseInt(p.getProperty("client.threadNum"));
        reqRate = Integer.parseInt(p.getProperty("client.reqRate"));
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

    public static void sendPutRequest(String host, int port, InputStream data,
            long length, String mimeType) {
        assert (data != null);
        HttpURLConnection con = null;
        OutputStream out = null;
        try {
            String resource = "/objects";
            URI uri = new URI("http", null, host, port, resource, null, null);
            URL u = new URL(uri.toString());
            con = (HttpURLConnection) u.openConnection();
            Map<String, String> headers = new HashMap<String, String>();
            mimeType = mimeType == null ? "application/octet-stream" : mimeType;
            headers.put("Content-Type", mimeType);
            con.setFixedLengthStreamingMode((int) length);
            con.setDoOutput(true);
            headers.put("Date", getDateHeader());
            con.connect();

            // post data
            byte[] buffer = new byte[4 * 1024];
            int read = 0;
            out = con.getOutputStream();
            while (read < length) {
                int c = data.read(buffer);
                out.write(buffer, 0, c);
                read += c;
            }
            // Check response
            if (con.getResponseCode() > 299) {
                handleError(con);
            }

        } catch (Exception e) {
            l4j.error("sendPutRequest exception", e);
        } finally {
            if (out != null) {
                try {
                    out.close();
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
            if (con != null) {
                try {
                con.disconnect();
                } catch (Exception e) {
                    
                }
            }
            if (data != null) {
                try {
                    data.close();
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }

        }
    }

    public static void sendGetRequest(String host, int port) {
        l4j.info("total get " + totalGet.addAndGet(1));
        HttpURLConnection con = null;
        InputStream in = null;
        try {
            String resource = "/objects";
            URI uri = new URI("http", null, host, port, resource, null, null);
            URL u = new URL(uri.toASCIIString());
            con = (HttpURLConnection) u.openConnection();
            Map<String, String> headers = new HashMap<String, String>();
            headers.put("Date", getDateHeader());
            configureRequest(con, "GET", headers);
            con.connect();

            // Check response
            if (con.getResponseCode() > 299) {
                handleError(con);
                l4j.info("Get fail num: " + getFail.addAndGet(1));
            } else {
                in = con.getInputStream();
                while (in.read() >= 0)
                    ;
                l4j.info("Get sucess num: " + getSuccess.addAndGet(1));
            }

        } catch (Exception e) {
            l4j.info("Get fail num: " + getFail.addAndGet(1));
            l4j.error("sendGetRequest exception", e);
        }
    }

    private static void configureRequest(HttpURLConnection con, String method,
            Map<String, String> headers) throws ProtocolException,
            UnsupportedEncodingException {
        // Can set all the headers, etc now.
        for (Iterator<String> i = headers.keySet().iterator(); i.hasNext();) {
            String name = i.next();
            con.setRequestProperty(name, headers.get(name));
        }
        // Set the method.
        con.setRequestMethod(method);
    }

    private static byte[] readResponse(HttpURLConnection con, byte[] buffer)
            throws IOException {
        InputStream in = null;
        if (con.getResponseCode() > 299) {
            in = con.getErrorStream();
            if (in == null) {
                in = con.getInputStream();
            }
        } else {
            in = con.getInputStream();
        }
        if (in == null) {
            // could not get stream
            return new byte[0];
        }
        try {
            byte[] output;
            int contentLength = con.getContentLength();
            // If we know the content length, read it directly into a buffer.
            if (contentLength != -1) {
                if (buffer != null && buffer.length < con.getContentLength()) {
                    throw new IOException(
                            "The response buffer was not long enough to hold the response: "
                                    + buffer.length + "<"
                                    + con.getContentLength());
                }
                if (buffer != null) {
                    output = buffer;
                } else {
                    output = new byte[con.getContentLength()];
                }

                int c = 0;
                while (c < contentLength) {
                    int read = in.read(output, c, contentLength - c);
                    if (read == -1) {
                        // EOF!
                        throw new EOFException(
                                "EOF reading response at position " + c
                                        + " size " + (contentLength - c));
                    }
                    c += read;
                }

                return output;
            } else {
                if (buffer == null) {
                    buffer = new byte[4096];
                }
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                try {
                    int c = 0;
                    while ((c = in.read(buffer)) != -1) {
                        baos.write(buffer, 0, c);
                    }
                } finally {
                    baos.close();
                }

                return baos.toByteArray();
            }
        } finally {
            if (in != null) {
                in.close();
            }
        }
    }

    private static void handleError(HttpURLConnection con) {
        int http_code = 0;
        // Try and read the response body.
        try {
            http_code = con.getResponseCode();
            byte[] response = readResponse(con, null);
            SAXBuilder sb = new SAXBuilder();
            Document d = sb.build(new ByteArrayInputStream(response));
            String code = d.getRootElement().getChildText("Code");
            String message = d.getRootElement().getChildText("Message");
            l4j.warn("response code: " + code + ", message: " + message);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private static String getDateHeader() {
        DateFormat HEADER_FORMAT = new SimpleDateFormat(
                "EEE, d MMM yyyy HH:mm:ss z", Locale.ENGLISH);
        TimeZone tz = TimeZone.getTimeZone("GMT");
        HEADER_FORMAT.setTimeZone(tz);
        String dateHeader = HEADER_FORMAT.format(new Date(System
                .currentTimeMillis()));
        return dateHeader;
    }
}
