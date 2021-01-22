package cn.ctyun.oos.common;

import java.io.File;
import java.io.IOException;

import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.io.FileUtils;

import cn.ctyun.common.Consts;
import cn.ctyun.common.conf.OOSConfig;

/**
 * @author: Cui Meng
 */
public class Email {
    private String registerSubject;
    private String registerBody;
    private String registerNick;
    private String findPasswordSubject;
    private String findPasswordBody;
    private String findPasswordNick;
    private String paySubject;
    private String payBody;
    private String frozenSubject;
    private String frozenBody;
    private String subject;
    private String body;
    private String nick;
    private String from189;
    private String smtp189;
    private String from;
    private String smtp;
    private String domainSuffix;
    private String password;
    private String to;
    private int emailPort;
    private boolean sendEmail;
    private String registerSubjectFromYunPortal;
    private String registerBodyFromYunPortal;
    private String registerBodyFromUDB;
    private static CompositeConfiguration config;
    public String adminRegisterSalesBody;
    public String verifySalesBody;
    public String registerSubjectEnglish;
    public String registerBodyEnglish;
    public String registerNickEnglish;
    public String findPasswordBodyEnglish;
    public String findPasswordSubjectEnglish;
    
    public Email(CompositeConfiguration config) throws IOException {
        if (config != null) {
            Email.config = config;
            this.smtp189 = OOSConfig.getSmtpHost();
            this.from189 = OOSConfig.getFromEmail();
            this.to = OOSConfig.getToEmail().toString()
                    .substring(1, OOSConfig.getToEmail().toString().length() - 1)
                    .replaceAll(" ", "");
            this.domainSuffix = config.getString("website.portalDomain");
            this.password = OOSConfig.getEmailPassword();
            this.emailPort = OOSConfig.getEmailPort();
            this.sendEmail = config.getBoolean("email.sendEmailToMarket");
            if (sendEmail) {
                this.registerSubject = config.getString("email.registerEmailSubject");
                System.out.println(this.registerSubject);
                this.registerNick = config.getString("email.emailNick");
                this.registerBody = FileUtils.readFileToString(new File(System.getenv("OOS_HOME")
                        + "/conf/registerEmail.html"), Consts.STR_UTF8);
            } else {
                this.registerSubject = config.getString("email.registerEmailSubjectSelf");
                this.registerNick = config.getString("email.emailNickSelf");
                this.registerBody = FileUtils.readFileToString(new File(System.getenv("OOS_HOME")
                        + "/conf/registerEmailSelf.html"), Consts.STR_UTF8);
                this.registerSubjectEnglish = config
                        .getString("email.registerEmailSubjectSelfEnglish");
                this.registerNickEnglish = config.getString("email.emailNickSelfEnglish");
                this.registerBodyEnglish = FileUtils
                        .readFileToString(new File(System.getenv("OOS_HOME")
                                + "/conf/registerEmailSelfEnglish.html"), Consts.STR_UTF8);
            }
            this.findPasswordSubject = config.getString("email.findPasswordEmailSubject");
            this.findPasswordSubjectEnglish = config
                    .getString("email.findPasswordEmailSubjectEnglish");
            this.findPasswordNick = config.getString("email.emailNickSelf");
            this.findPasswordBody = FileUtils.readFileToString(new File(System.getenv("OOS_HOME")
                    + "/conf/findPasswordEmail.html"), Consts.STR_UTF8);
            this.findPasswordBodyEnglish = FileUtils.readFileToString(
                    new File(System.getenv("OOS_HOME") + "/conf/findPasswordEmailEnglish.html"),
                    Consts.STR_UTF8);
            this.paySubject = config.getString("email.PayRemindEmailSubject");
            this.payBody = FileUtils.readFileToString(new File(System.getenv("OOS_HOME")
                    + "/conf/payRemindEmail.html"), Consts.STR_UTF8);
            this.frozenSubject = config.getString("email.FrozenRemindEmailSubject");
            this.frozenBody = FileUtils.readFileToString(new File(System.getenv("OOS_HOME")
                    + "/conf/frozenRemindEmail.html"), Consts.STR_UTF8);
            this.registerSubjectFromYunPortal = config
                    .getString("email.RegisterSubjectFromYunPortal");
            this.registerBodyFromYunPortal = FileUtils.readFileToString(
                    new File(System.getenv("OOS_HOME") + "/conf/registerFromYunPortalEmail.html"),
                    Consts.STR_UTF8);
            this.registerBodyFromUDB = FileUtils.readFileToString(
                    new File(System.getenv("OOS_HOME") + "/conf/registerFromUDBEmail.html"),
                    Consts.STR_UTF8);
            this.adminRegisterSalesBody = FileUtils.readFileToString(
                    new File(System.getenv("OOS_HOME") + "/conf/registerSalesManagerEmaill.html"),
                    Consts.STR_UTF8);
            this.verifySalesBody = FileUtils.readFileToString(new File(System.getenv("OOS_HOME")
                    + "/conf/verifySalesmanEmail.html"), Consts.STR_UTF8);
        }
    }
    
    public void setInnerRegisterEmail() throws IOException {
        this.registerSubject = config.getString("email.registerEmailSubject");
        this.registerNick = config.getString("email.emailNick");
        this.registerBody = FileUtils.readFileToString(new File(System.getenv("OOS_HOME")
                + "/conf/registerEmail.html"), Consts.STR_UTF8);
        this.to = config.getList("email.toEmail").toString()
                .substring(1, config.getList("email.toEmail").toString().length() - 1)
                .replaceAll(" ", "");
    }
    
    public String getRegisterBodyFromUDB() {
        return registerBodyFromUDB;
    }
    
    public void setRegisterBodyFromUDB(String registerBodyFromUDB) {
        this.registerBodyFromUDB = registerBodyFromUDB;
    }
    
    public String getRegisterSubjectFromYunPortal() {
        return registerSubjectFromYunPortal;
    }
    
    public void setRegisterSubjectFromYunPortal(String registerSubjectFromYunPortal) {
        this.registerSubjectFromYunPortal = registerSubjectFromYunPortal;
    }
    
    public String getRegisterBodyFromYunPortal() {
        return registerBodyFromYunPortal;
    }
    
    public void setRegisterBodyFromYunPortal(String registerBodyFromYunPortal) {
        this.registerBodyFromYunPortal = registerBodyFromYunPortal;
    }
    
    public String getFrom189() {
        return from189;
    }
    
    public void setFrom189(String from189) {
        this.from189 = from189;
    }
    
    public String getSmtp189() {
        return smtp189;
    }
    
    public void setSmtp189(String smtp189) {
        this.smtp189 = smtp189;
    }
    
    public boolean isSendEmail() {
        return sendEmail;
    }
    
    public void setSendEmail(boolean sendEmail) {
        this.sendEmail = sendEmail;
    }
    
    public int getEmailPort() {
        return emailPort;
    }
    
    public void setEmailPort(int emailPort) {
        this.emailPort = emailPort;
    }
    
    public String getPassword() {
        return password;
    }
    
    public void setPassword(String password) {
        this.password = password;
    }
    
    public void setRegisterSubject(String subject) {
        this.registerSubject = subject;
    }
    
    public String getRegisterBody() {
        return registerBody;
    }
    
    public String getDomainSuffix() {
        return domainSuffix;
    }
    
    public void setDomainSuffix(String domainSuffix) {
        this.domainSuffix = domainSuffix;
    }
    
    public String getEmailNick() {
        return registerNick;
    }
    
    public void setEmailNick(String emailNick) {
        this.registerNick = emailNick;
    }
    
    public void setRegisterBody(String body) {
        this.registerBody = body;
    }
    
    public String getFrom() {
        return from;
    }
    
    public void setFrom(String from) {
        this.from = from;
    }
    
    public String getSmtp() {
        return smtp;
    }
    
    public void setSmtp(String smtp) {
        this.smtp = smtp;
    }
    
    public String getNick() {
        return nick;
    }
    
    public void setNick(String nick) {
        this.nick = nick;
    }
    
    public String getFindPasswordNick() {
        return findPasswordNick;
    }
    
    public void setFindPasswordNick(String findPasswordNick) {
        this.findPasswordNick = findPasswordNick;
    }
    
    public String getSubject() {
        return subject;
    }
    
    public void setSubject(String subject) {
        this.subject = subject;
    }
    
    public String getBody() {
        return body;
    }
    
    public void setBody(String body) {
        this.body = body;
    }
    
    public String getFindPasswordSubject() {
        return findPasswordSubject;
    }
    
    public void setFindPasswordSubject(String findPasswordSubject) {
        this.findPasswordSubject = findPasswordSubject;
    }
    
    public String getFindPasswordBody() {
        return findPasswordBody;
    }
    
    public void setFindPasswordBody(String findPasswordBody) {
        this.findPasswordBody = findPasswordBody;
    }
    
    public String getTo() {
        return to;
    }
    
    public void setTo(String to) {
        this.to = to;
    }
    
    public String getRegisterSubject() {
        return registerSubject;
    }
    
    public String getPaySubject() {
        return paySubject;
    }
    
    public void setPaySubject(String paySubject) {
        this.paySubject = paySubject;
    }
    
    public String getPayBody() {
        return payBody;
    }
    
    public void setPayBody(String payBody) {
        this.payBody = payBody;
    }
    
    public String getFrozenSubject() {
        return frozenSubject;
    }
    
    public void setFrozenSubject(String frozenSubject) {
        this.frozenSubject = frozenSubject;
    }
    
    public String getFrozenBody() {
        return frozenBody;
    }
    
    public void setFrozenBody(String frozenBody) {
        this.frozenBody = frozenBody;
    }
}
