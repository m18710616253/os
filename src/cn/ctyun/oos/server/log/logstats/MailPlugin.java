package cn.ctyun.oos.server.log.logstats;

import java.util.concurrent.Executor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import cn.ctyun.common.conf.OOSConfig;
import cn.ctyun.oos.common.MailServer;
import cn.ctyun.oos.common.MailServer.Mail;
import common.util.BlockingExecutor;

public class MailPlugin{
    private static Log log = LogFactory.getLog(MailPlugin.class);
    private static Executor mailExecutor = new BlockingExecutor(4, 10, 10, 1000, "sendmail");
    private static MailServer mailServer = new MailServer(OOSConfig.getSmtpHost(), OOSConfig.getEmailPort(), OOSConfig.getFromEmail(), OOSConfig.getEmailPassword());

    public static void sendMail(final String mailSubject, final String mailBody){
        mailExecutor.execute(new Runnable(){
            @Override
            public void run() {
                Mail mail = mailServer.createMail(OOSConfig.getFromEmail(),OOSConfig.getToEmail(), mailSubject, "Performance Monitor", mailBody);
                if(mail.sendMail()){
                    log.info("SendMail OK.Mail Body is: " + mailBody);
                }else{
                    log.error("SendMail Failed.Mail Body is: " + mailBody);
                }                
            }
            
        });
    }
}
