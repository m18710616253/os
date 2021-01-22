package cn.ctyun.oos.server.pay;

import java.io.File;
import java.io.IOException;
import java.util.Date;

import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import cn.ctyun.oos.common.Email;
import cn.ctyun.oos.common.SocketEmail;
import cn.ctyun.oos.server.util.Misc;

import com.amazonaws.services.s3.internal.ServiceUtils;
import common.util.ConfigUtils;

public class MailRemind {
    private static Email emailConfig;
    public static CompositeConfiguration config = null;
    private static final Log log = LogFactory.getLog(MailRemind.class);
    static {
        File[] xmlConfs = {
                new File(System.getenv("OOS_HOME")
                        + "/conf/pay-online.xml"),
                new File(System.getenv("OOS_HOME") + "/conf/oos.xml") };
        try {
            config = (CompositeConfiguration) ConfigUtils
                    .loadXmlConfig(xmlConfs);
        } catch (Exception e) {
            log.error(ServiceUtils.formatIso8601Date(new Date()) + " "
                    + e.getMessage());
        }
        try {
            emailConfig = new Email(config);
        } catch (IOException e) {
            log.error(ServiceUtils.formatIso8601Date(new Date()) + " "
                    + e.getMessage());
        }
    }

    public static void sendMail(String to, byte sign) {
        try {
            // sign is 0 means need to send a pay mail, others means send a
            // frozen mail
            if (sign == 0) {
                if (!sendPayMail(to)) {
                    log.error(ServiceUtils.formatIso8601Date(new Date())
                            + " Sendmail in pay process failed.");
                }
            } else {
                if (!sendFrozenMail(to)) {
                    log.error(ServiceUtils.formatIso8601Date(new Date())
                            + " Sendmail in frozen process failed.");
                }
            }
        } catch (IOException e) {
            log.error(ServiceUtils.formatIso8601Date(new Date()) + " "
                    + e.getMessage());
        }
    }

    private static boolean sendPayMail(String to) throws IOException {
        Email email = new Email(null);
        String body = emailConfig.getPayBody();
        body = body.replace("DATE", Misc.formatYearMonthDate(new Date()));
        email.setTo(to);
        email.setFrom(emailConfig.getFrom189());
        email.setSmtp(emailConfig.getSmtp189());
        email.setSubject(emailConfig.getPaySubject());
        email.setBody(body);
        email.setNick(emailConfig.getFindPasswordNick());
        email.setPassword(emailConfig.getPassword());
        email.setEmailPort(emailConfig.getEmailPort());
        SocketEmail.sendEmail(email);
        return true;
    }

    private static boolean sendFrozenMail(String to) throws IOException {
        Email email = new Email(null);
        String body = emailConfig.getFrozenBody();
        body = body.replace("DATE", Misc.formatYearMonthDate(new Date()));
        email.setTo(to);
        email.setBody(body);
        email.setFrom(emailConfig.getFrom189());
        email.setSmtp(emailConfig.getSmtp189());
        email.setSubject(emailConfig.getFrozenSubject());
        email.setNick(emailConfig.getFindPasswordNick());
        email.setPassword(emailConfig.getPassword());
        email.setEmailPort(emailConfig.getEmailPort());
        SocketEmail.sendEmail(email);
        return true;
    }
}
