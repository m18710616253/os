package cn.ctyun.oos.monitor;

import java.io.InputStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.mail.Email;
import org.apache.commons.mail.EmailException;
import org.apache.commons.mail.HtmlEmail;

import common.time.TimeUtils;

import cn.ctyun.oos.monitor.OOSMonitor.Config;

public class Utils {
    private static Log log = LogFactory.getLog(Utils.class);
    private static DateFormat format = new SimpleDateFormat(
        "yyyy-MM-dd HH:mm:ss");
    
    public static String buildMessageBody(int type, String nick, long time,
            int total, int failure, int bdFailure) {
        StringBuilder sb = new StringBuilder(
                "<table width=\"358\" border=\"0\" cellspacing=\"5\" style=\"font-size:14px\">")
                .append("<tr>")
                .append("<td width=\"225\" align=\"left\" valign=\"middle\">监控源 ：</td>")
                .append("<td width=\"137\" align=\"left\" valign=\"middle\">");
        if (type == 0) {
            sb.append(nick).append("</td>").append("</tr>");
            sb.append("<tr>")
                    .append("<td>报警级别：</td>")
                    .append("<td bgcolor=\"#FFFF33\"><strong style=\"color:#C63\">致命</strong></td>")
                    .append("</tr>");
            sb.append("<tr>").append("<td>失败率：</td>")
                    .append("<td>超过百分之八十</td>").append("</tr>");
            sb.append("<tr>").append("<td>耗时：</td>").append("<td>")
                    .append(time).append("s</td>").append("</tr>");
            sb.append("<tr>").append("<td>尝试次数：</td>").append("<td>")
                    .append(total).append("</td>").append("</tr>");
            sb.append("<tr>").append("<td>失败次数：</td>").append("<td>")
                    .append(failure).append("</td>").append("</tr>");
            sb.append("<tr>").append("<td>测试机本身网络问题导致的失败次数：</td>")
                    .append("<td>").append(bdFailure).append("</td>")
                    .append("</tr>");
        } else if (type == 1) {
            sb.append(nick).append("</td>").append("</tr>");
            sb.append("<tr>")
                    .append("<td>报警级别：</td>")
                    .append("<td bgcolor=\"#FFFF33\"><strong>严重</strong></td>")
                    .append("</tr>");
            sb.append("<tr>").append("<td>失败率：</td>")
                    .append("<td>超过百分之五十</td>").append("</tr>");
            sb.append("<tr>").append("<td>耗时：</td>").append("<td>")
                    .append(time).append("s</td>").append("</tr>");
            sb.append("<tr>").append("<td>尝试次数：</td>").append("<td>")
                    .append(total).append("</td>").append("</tr>");
            sb.append("<tr>").append("<td>失败次数：</td>").append("<td>")
                    .append(failure).append("</td>").append("</tr>");
            sb.append("<tr>").append("<td>测试机本身网络问题导致的失败次数：</td>")
                    .append("<td>").append(bdFailure).append("</td>")
                    .append("</tr>");
        } else if (type == 2) {
            sb.append(nick).append("</td>").append("</tr>");
            sb.append("<tr>")
                    .append("<td>报警级别：</td>")
                    .append("<td bgcolor=\"#FFFF33\"><strong>警告</strong></td>")
                    .append("</tr>");
            sb.append("<tr>").append("<td>失败率：</td>")
                    .append("<td>超过百分之二十</td>").append("</tr>");
            sb.append("<tr>").append("<td>耗时：</td>").append("<td>")
                    .append(time).append("s</td>").append("</tr>");
            sb.append("<tr>").append("<td>尝试次数：</td>").append("<td>")
                    .append(total).append("</td>").append("</tr>");
            sb.append("<tr>").append("<td>失败次数：</td>").append("<td>")
                    .append(failure).append("</td>").append("</tr>");
            sb.append("<tr>").append("<td>测试机本身网络问题导致的失败次数：</td>")
                    .append("<td>").append(bdFailure).append("</td>")
                    .append("</tr>");
        } else if (type == 3) {
            sb.append(nick).append("</td>").append("</tr>");
            sb.append("<tr>").append("<td>报警级别：</td>")
                    .append("<td><strong>恢复正常</strong></td>")
                    .append("</tr>");
            sb.append("<tr>").append("<td>失败率：</td>")
                    .append("<td>不超过百分之二十</td>").append("</tr>");
            sb.append("<tr>").append("<td>耗时：</td>").append("<td>")
                    .append(time).append("s</td>").append("</tr>");
            sb.append("<tr>").append("<td>尝试次数：</td>").append("<td>")
                    .append(total).append("</td>").append("</tr>");
            sb.append("<tr>").append("<td>失败次数：</td>").append("<td>")
                    .append(failure).append("</td>").append("</tr>");
            sb.append("<tr>").append("<td>测试机本身网络问题导致的失败次数：</td>")
                    .append("<td>").append(bdFailure).append("</td>")
                    .append("</tr>");
        } else if (type == 4) {
            sb.append(nick).append("</td>").append("</tr>");
            sb.append("<tr>").append("<td>报警级别：</td>")
                    .append("<td><strong>正常</strong></td>").append("</tr>");
            sb.append("<tr>").append("<td>持续时间：</td>")
                    .append("<td>过去的24小时</td>").append("</tr>");
        }
        sb.append("<tr>").append("<td>邮件发送时间：</td>").append("<td>")
                .append(format.format(new Date())).append("</td>")
                .append("</tr>");

        sb.append("</table>");
        sb.append("<p><em>注：</em></p>");
        sb.append(
                "<table width=\"653\" height=\"284\" border=\"1\" style=\"font-size:13px\">")
                .append("<tr>")
                .append("<td width=\"80\" rowspan=\"6\">报警级别 </td>")
                .append("<td width=\"563\">正常/恢复正常 &lt; 警告 &lt; 严重 &lt; 致命</td>")
                .append("</tr>")
                .append("<tr>")
                .append("<td>正常（在过去的24小时中，OOS的状态都是正常的）</td>")
                .append("</tr>")
                .append("<tr>")
                .append("<td>恢复正常（在上次监控的失败率 &gt; 20%情况下，本次监控的失败率 &lt; 20%）</td>")
                .append("</tr>")
                .append("<tr>")
                .append("<td>警告（失败率 &gt; 20%）</td>")
                .append("</tr>")
                .append("<tr>")
                .append("<td>严重（失败率 &gt; 50%）</td>")
                .append("</tr>")
                .append("<tr>")
                .append("<td>致命（失败率 &gt; 80%）</td>")
                .append("</tr>")
                .append("<tr>")
                .append("<td>监控原则</td>")
                .append("<td><p>&nbsp;&nbsp;&nbsp;&nbsp;监控程序")
                .append("每隔")
                .append(Config.runInter / 1000)
                .append("秒运行一次，每次监控都并发访问")
                .append(Config.optNum)
                .append("次服务，每次服务包括put/get/delete object，然后进行统计分析，并根据不同的失败率发送不同级别的报警邮件；每次发送报警邮件之前都会判断上次发送报警邮件的时间是否已经超过十分钟，如果超过则发送报警邮件，否则不发送；如果监控程序已经超过24小时未出现失败率 &lt; 10%的情况，则会发送一封正常邮件。</p><p>&nbsp;&nbsp;&nbsp;&nbsp;在发送警告/严重/致命邮件之前，都会判断上次发送此类级别邮件的时间是否已经超过10分钟，只有时间间隔超过10分钟，才会再次发送此类级别的报警邮件。</p></td>")
                .append("</tr>").append("</table>");

        return sb.toString();
    }
    
    private static Runtime exec = Runtime.getRuntime();
    public static String writeToGanglia(String cmd) {
        try {
            Process p = exec.exec(cmd);
            InputStream is = p.getErrorStream();
            if(is.available()>0)
                try{
                    byte[] error = new byte[1024];
                    int size = is.read(error);
                    return new String(error, 0, size, "utf-8");
                }finally{
                    is.close();
                }
        } catch (Exception e) {
            return e.getMessage();
        } 
        return null;
    }
    
    public static class SMS {
        public static String prefix = "java -jar ./lib/sendsms.jar ";

        public static boolean sendSMS(String message, String phone) {
            String command = prefix + phone + " " + message;
            Process process = null;
            try {
                process = Runtime.getRuntime().exec(command);
                InputStream is = process.getInputStream();
                try {
                    byte[] buff = new byte[1024];
                    int len = 0;
                    while ((len = is.read(buff)) != -1) {
                        String info = new String(buff, 0, len);
                        if (info.contains("Status:0")) {
                            log.info("Send message to " + phone
                                    + " success.");
                            return true;
                        }
                    }
                    InputStream errorIs = process.getErrorStream();
                    try {
                        buff = new byte[1024];
                        len = 0;
                        StringBuilder error = new StringBuilder();
                        while ((len = errorIs.read(buff)) != -1) {
                            error.append(new String(buff, 0, len)).append("\n");
                        }
                        log.info("Send message to " + phone
                                + " failed.\n" + error.toString());
                    } finally {
                        errorIs.close();
                    }
                } finally {
                    is.close();
                }
            } catch (Exception e) {
                log.error(e.getMessage());
            }
            return false;
        }
    }
    
    public static class MailServer {
        private String smtpHost;
        private int smtpPort;
        private String userName;
        private String pwd;

        public MailServer(String smtpHost, int smtpPort, String userName, String pwd) {
            this.smtpHost = smtpHost;
            this.smtpPort = smtpPort;
            this.userName = userName;
            this.pwd = pwd;
        }

        public Mail createMail(String from, String to, String subject, String nick,
                String body) {
            Email email = new HtmlEmail();
            email.setCharset("utf-8");
            email.setHostName(smtpHost);
            email.setSmtpPort(smtpPort);
            email.setAuthentication(userName, pwd);
            if(from == null || from.length() == 0)
                throw new IllegalArgumentException("InvalidArgument: Email sender should not be null or \"\"");
            if(to == null || to.length() == 0)
                throw new IllegalArgumentException("InvalidArgument: Recipients should not be null or \"\"");
            return new Mail(email, from, to.split(","), subject, nick, body);
        }
        
        public class Mail {
            private String from;
            private String[] to;
            private String subject;
            private String nick;
            private String body;
            private Email email;

            private Log log = LogFactory.getLog(Mail.class);

            Mail(Email email, String from, String[] to, String subject,
                    String nick, String body) {
                this.email = email;
                this.from = from;
                this.to = to;
                this.subject = subject;
                this.nick = nick;
                this.body = body;
            }

            public boolean sendMail() {
                try {
                    email.setSubject(subject);
                    email.setMsg(body);
                    email.setFrom(from, nick);
                    StringBuilder recipients = new StringBuilder();
                    for(String ss : to){
                        if(ss.length() == 0){
                            log.warn("Recipient should not be \"\".");
                            continue;
                        }else if(!ss.contains("@")){
                            log.warn("Recipient: " + ss + " is not valid.");
                            continue;
                        }
                        recipients.append(ss).append(",");
                        email.addTo(ss);
                    }
                    recipients.substring(0, recipients.lastIndexOf(","));

                    String result = email.send();
                    if (result != null) {
                        log.info(TimeUtils.toYYYYMMddHHmmss(new Date())
                                + ": Send Email to " + recipients.toString() + " SUCCESS");
                    }
                    return true;
                } catch (EmailException e) {
                    log.error("Send mail error: ", e);
                }
                return false;
            }

        }

    }
}
