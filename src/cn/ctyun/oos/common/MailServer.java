package cn.ctyun.oos.common;

import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.mail.Email;
import org.apache.commons.mail.EmailException;
import org.apache.commons.mail.HtmlEmail;

import common.time.TimeUtils;

/**
 * @author Dongchk
 *
 */
public class MailServer {
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
