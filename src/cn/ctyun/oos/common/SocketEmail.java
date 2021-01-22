package cn.ctyun.oos.common;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.util.ArrayList;

import javax.mail.internet.MimeUtility;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import cn.ctyun.common.Consts;
import cn.ctyun.common.conf.OOSConfig;

public class SocketEmail implements Runnable {
    private static final Log log = LogFactory.getLog(SocketEmail.class);
    private Email email;
    private static ArrayList<SocketEmail> runningThreads = new ArrayList<SocketEmail>();
    
    public SocketEmail(Email email) {
        this.email = email;
    }
    
    public static void sendEmail(Email email) {
        new Thread(new SocketEmail(email)).start();
    }
    
    // 将输入流转换成字符串
    public static String transStreamToString(InputStream is, int maxSize) {
        if (is == null) {
            throw new NullPointerException("inPutStream can not be null!............");
        }
        String data = null;
        byte[] buffer = new byte[maxSize];
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
            int actualSize = is.read(buffer);
            baos.write(buffer, 0, actualSize);
            data = new String(baos.toByteArray(), Consts.STR_UTF8);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                baos.close();
            } catch (Throwable e) {
                log.error(e.getMessage(), e);
            }
        }
        return data;
    }
    
    @Override
    public void run() {
        regist(this);
        boolean isSend = false;
        Socket sc = null;
        InputStream is = null;
        OutputStream os = null;
        final String END_TAG = "\r\n";
        StringBuffer request = new StringBuffer();
        StringBuffer response = new StringBuffer();
        String username;
        try {
            username = Base64.encodeBase64String(email.getFrom().getBytes(Consts.STR_UTF8));
            String pwd = Base64.encodeBase64String(email.getPassword().getBytes(Consts.STR_UTF8));
            String subject = Base64
                    .encodeBase64String(email.getSubject().getBytes(Consts.STR_UTF8));
            String content = Base64.encodeBase64String(email.getBody().getBytes(Consts.STR_UTF8));
            String senderEmail = email.getFrom();
            sc = new Socket(InetAddress.getByName(email.getSmtp()), email.getEmailPort());
            sc.setSoTimeout(OOSConfig.getSocketlIdleTimeout());// 设置超时
            is = sc.getInputStream();
            os = sc.getOutputStream();
            response = response.replace(0, response.length(),
                    transStreamToString(is, Consts.DEFAULT_BUFFER_SIZE));// 读取服务器返回数据
            if (response.indexOf("220") >= 0) {// 连接smtp服务器成功！
                // 打招呼
                request.replace(0, request.length(), "ehlo ")
                        .append(senderEmail.substring(0, senderEmail.indexOf("@"))).append(END_TAG);// 发送命令到服务器
                os.write(request.toString().getBytes(Consts.STR_UTF8));
                response = response.replace(0, response.length(),
                        transStreamToString(is, Consts.DEFAULT_BUFFER_SIZE));
                if (response.indexOf("250") >= 0) {// 打招呼成功!
                    // 使用login方式登录
                    request.replace(0, request.length(), "auth login\r\n");
                    os.write(request.toString().getBytes(Consts.STR_UTF8));
                    response = response.replace(0, response.length(),
                            transStreamToString(is, Consts.DEFAULT_BUFFER_SIZE));
                    // 输入BASE64编码的用户名
                    request.replace(0, request.length(), username.replace(END_TAG, "")).append(
                            END_TAG);
                    os.write(request.toString().getBytes(Consts.STR_UTF8));
                    response = response.replace(0, response.length(),
                            transStreamToString(is, Consts.DEFAULT_BUFFER_SIZE));
                    // 输入BASE64编码的密码
                    request.replace(0, request.length(), pwd.replace(END_TAG, "")).append(END_TAG);
                    os.write(request.toString().getBytes(Consts.STR_UTF8));
                    response = response.replace(0, response.length(),
                            transStreamToString(is, Consts.DEFAULT_BUFFER_SIZE));
                    if (response.indexOf("235") >= 0) {// 登录成功
                        // 填写发件人
                        request.replace(0, request.length(), "mail from:<").append(senderEmail)
                                .append(">").append(END_TAG);
                        os.write(request.toString().getBytes(Consts.STR_UTF8));
                        response = response.replace(0, response.length(),
                                transStreamToString(is, Consts.DEFAULT_BUFFER_SIZE));
                        if (response.indexOf("250") >= 0) {// 发件人被smtp服务器接受
                            // 填写收件人
                            String[] to = email.getTo().split(",");
                            for (int i = 0; i < to.length; i++) {
                                request.replace(0, request.length(), "rcpt to:<").append(to[i])
                                        .append(">").append(END_TAG);
                                os.write(request.toString().getBytes(Consts.STR_UTF8));
                            }
                            response = response.replace(0, response.length(),
                                    transStreamToString(is, Consts.DEFAULT_BUFFER_SIZE));
                            if (response.indexOf("250") >= 0) {// 收件人被smtp服务器接受
                                // 开始写邮件
                                request.replace(0, request.length(), "data").append(END_TAG);
                                os.write(request.toString().getBytes(Consts.STR_UTF8));
                                response = response.replace(0, response.length(),
                                        transStreamToString(is, Consts.DEFAULT_BUFFER_SIZE));
                                // 写邮件发件人
                                request.replace(
                                        0,
                                        request.length(),
                                        "from:\""
                                                + MimeUtility.encodeText(email.getNick(),
                                                        Consts.STR_UTF8, "B") + "\"<"
                                                + email.getFrom() + ">").append(END_TAG);
                                os.write(request.toString().getBytes(Consts.STR_UTF8));
                                // 写邮件收件人
                                request.replace(0, request.length(), "to:").append(email.getTo())
                                        .append(END_TAG);
                                os.write(request.toString().getBytes(Consts.STR_UTF8));
                                // 写邮件主题
                                request.replace(0, request.length(), "subject:")
                                        .append("=?UTF-8?B?" + subject.replace(END_TAG, "") + "?=")
                                        .append(END_TAG);
                                ;
                                os.write(request.toString().getBytes(Consts.STR_UTF8));
                                // 写邮件正文
                                request.replace(0, request.length(), "MIME-Version:").append("1.0")
                                        .append(END_TAG);
                                os.write(request.toString().getBytes(Consts.STR_UTF8));
                                request.replace(0, request.length(), "Content-Type:")
                                        .append("text/html;charset=UTF-8").append(END_TAG);
                                os.write(request.toString().getBytes(Consts.STR_UTF8));
                                request.replace(0, request.length(), "Content-Transfer-Encoding:")
                                        .append("base64").append(END_TAG);
                                os.write(request.toString().getBytes(Consts.STR_UTF8));
                                request.replace(0, request.length(), END_TAG).append(content)
                                        .append(END_TAG).append(".").append(END_TAG);
                                os.write(request.toString().getBytes(Consts.STR_UTF8));
                                response = response.replace(0, response.length(),
                                        transStreamToString(is, Consts.DEFAULT_BUFFER_SIZE));
                                if (response.indexOf("250") >= 0) {// 发送成功
                                    isSend = true;
                                }
                                // 退出
                                os.write("quit\r\n".getBytes(Consts.STR_UTF8));
                                response = response.replace(0, response.length(),
                                        transStreamToString(is, Consts.DEFAULT_BUFFER_SIZE));
                                request.delete(0, request.length());
                                response.delete(0, response.length());
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        } finally {
            if (sc != null) {
                try {
                    sc.close();
                } catch (IOException e) {
                    log.error(e.getMessage(), e);
                }
            }
        }
        if (isSend) {
            log.info("send email to " + email.getTo() + " success");
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                log.error(e.getMessage(), e);
            }
        } else
            log.info("send email to " + email.getTo() + " error");
        unRegist(this);
    }
    
    public void regist(SocketEmail socketEmail) {
        synchronized (runningThreads) {
            runningThreads.add(socketEmail);
        }
    }
    
    public void unRegist(SocketEmail socketEmail) {
        synchronized (runningThreads) {
            runningThreads.remove(socketEmail);
        }
    }
    
    public static boolean hasThreadRunning() {
        return (runningThreads.size() > 0);
    }
}
