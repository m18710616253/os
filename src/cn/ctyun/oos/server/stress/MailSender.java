package cn.ctyun.oos.server.stress;

import java.io.File;
import java.util.Properties;

import javax.mail.Message;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import common.util.ConfigUtils;

public class MailSender {
	private static final Log logger = LogFactory.getLog(MailSender.class);

	private static String smtpHost;
	private static String userName;
	private static String pwd;
	private static String from;
	private static String to;
	private static String[] mails;

	static {
		try {
			CompositeConfiguration config = null;
			File[] xmlConfs = { new File(System.getProperty("user.dir")
					+ "/conf/email-config.xml") };
			config = (CompositeConfiguration) ConfigUtils.loadXmlConfig(xmlConfs);
			smtpHost = config.getString("smtp.host");
			userName = config.getString("username");
			pwd = config.getString("password");
			from = config.getString("from");
			to = config.getString("to");
			mails = to.split(";");
			System.out.println(smtpHost);
		} catch (Exception e) {
			logger.error("Loading the mail config error: ", e);
		}
	}
	
	public static void sendMail(String subject, String nick, String msgBody) {
		Properties props = new Properties();
        props.put("mail.smtp.host", smtpHost);
        props.put("mail.smtp.auth", "true"); 
        Session session = Session.getInstance(props, new javax.mail.Authenticator() {
            protected PasswordAuthentication getPasswordAuthentication() {
                return new PasswordAuthentication(userName, pwd);
            }});
        session.setDebug(false);
        try {
            Message msg = new MimeMessage(session);
            msg.setFrom(new InternetAddress(from));
            for(String toMail : mails)
            	msg.addRecipient(Message.RecipientType.TO,
                             new InternetAddress(toMail));
            msg.setSubject(subject);
            msg.setText(msgBody);
            Transport.send(msg);
            logger.info("Send Email to " + to + " SUCCESS!");
        }catch (Exception e) {
			logger.error("Send mail error: ", e);
		}

	}
	
}
