package cn.ctyun.oos.server.db.dbpay;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.s3.internal.ServiceUtils;

import cn.ctyun.common.Consts;
import cn.ctyun.common.db.DbSource;
import cn.ctyun.oos.hbase.MetaClient;
import cn.ctyun.oos.metadata.MinutesUsageMeta;
import cn.ctyun.oos.metadata.OwnerMeta;
import cn.ctyun.oos.server.db.dbprice.DbUserPackage;
import cn.ctyun.oos.server.pay.MailRemind;
import common.time.TimeUtils;

public class DBPay {
    
    private static Log log = LogFactory.getLog(DBPay.class);
    
    private static DBPay THIS = new DBPay();
    private static MetaClient client = MetaClient.getGlobalClient();

    public static DBPay getInstance() {
        return THIS;
    }

    public static void main(String[] args) throws Exception {
        // final DBPay dbPay = DBPay.getInstance();
        // dbPay.uninit();
        // dbPay.init();
        // BlockingExecutor exec = new
        // BlockingExecutor(OOSConfig.getSmallCoreThreadsNum(),
        // OOSConfig.getSmallMaxThreadsNum(), OOSConfig.getSmallQueueSize(),
        // OOSConfig.getSmallAliveTime(), "dbclient-emulator");
        // final AtomicInteger count = new AtomicInteger(0);
        // for (int i = 1; i < 50; i++) {
        // exec.execute(new Runnable() {
        // @Override
        // public void run() {
        // int i = count.getAndIncrement();
        // try {
        // DbPrice dbPrice = new DbPrice(i);
        // dbPrice.des = "for test";
        // dbPrice.ghReqPerTenThous = 0.6;
        // dbPrice.otherReqPerThous = 0.6;
        // dbPrice.outFlowPerGB = 0.6;
        // dbPrice.storagePerGB = 0.8;
        // dbPrice.strategy = 0;
        // dbPay.priceInsert(dbPrice);
        // System.out.println("done: " + i);
        // } catch (SQLException e) {
        // e.printStackTrace();
        // }
        // }
        // });
        // }
        // exec.shutdown();
        // exec.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        // Connection conn = new DBPay().dataSource.getConnection();
        // DbUsageCurrent.drop(conn);
        // DbUsageCurrent.create(conn);
    }

    public void init() throws SQLException {
        // Connection conn = dataSource.getConnection();
        // try {
        // DbPrice.create(conn);
        // DbUsageHistory.create(conn);
        // DbAccount.create(conn);
        // DbOrder.create(conn);
        // DbCRMBill.create(conn);
        // DbPackage.create(conn);
        // } finally {
        // conn.close();
        // }
    }

    /**
     * Purpose: during develop period, clear the last data
     *
     * @throws SQLException
     */
    public void uninit() throws SQLException {
        // Connection conn = dataSource.getConnection();
        // try {
        // DbPrice.drop(conn);
        // DbUsageHistory.drop(conn);
        // DbAccount.drop(conn);
        // DbOrder.drop(conn);
        // DbCRMBill.drop(conn);
        // DbPackage.drop(conn);
        // } finally {
        // conn.close();
        // }
    }

    public void create() throws SQLException {
        Connection conn = null;
        String create = "CREATE DATABASE IF NOT EXISTS oos DEFAULT CHARACTER SET utf8";
        try {
            conn = DbSource.getConnection();
            Statement st = conn.createStatement();
            try {
                st.execute(create);
            } finally {
                st.close();
            }
        } finally {
            if (conn != null)
                conn.close();
        }
    }

    public boolean historySelectById(DbUsageHistory dbHistory) throws SQLException {
        Connection conn = null;
        try {
            conn = DbSource.getConnection();
            return dbHistory.selectById(conn);
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }

    public boolean historySelectByDate(DbUsageHistory dbHistory) throws SQLException {
        Connection conn = null;
        try {
            conn = DbSource.getConnection();
            return dbHistory.selectByDate(conn);
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }

    public void historySelectActivity(DbOrder dbOrder) throws SQLException {
        Connection conn = null;
        try {
            conn = DbSource.getConnection();
            dbOrder.selectActivity(conn);
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }

    public void historySelectSum(DbUsageHistory dbHistory) throws SQLException {
        Connection conn = null;
        try {
            conn = DbSource.getConnection();
            dbHistory.selectSum(conn);
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }

    public void historySelectUsrIds(DbUsageHistory dbHistory) throws SQLException {
        Connection conn = null;
        try {
            conn = DbSource.getConnection();
            dbHistory.selectUsrIds(conn);
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }

    public void historyInsert(DbUsageHistory dbHistory) throws SQLException {
        Connection conn = null;
        try {
            conn = DbSource.getConnection();
            dbHistory.insert(conn);
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }
    
    public void historyUpdate(DbUsageHistory dbHistory) throws SQLException {
        Connection conn = null;
        try {
            conn = DbSource.getConnection();
            dbHistory.update(conn);
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }

    public void historyDelete(DbUsageHistory dbHistory) throws SQLException {
        Connection conn = null;
        try {
            conn = DbSource.getConnection();
            dbHistory.delete(conn);
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }
    /**
     * 更新用量信息，如果用户有套餐更新用户套餐，初始化用户的usageCurrent数据
     * @param usage global的前一天用量
     * @throws Exception
     */
    public void updateUsage(MinutesUsageMeta usage, DbUserPackage dbUserPackage) throws Exception {
        Connection conn = null;
        try {
            conn = DbSource.getConnection();
            conn.setAutoCommit(false);
            Savepoint point = conn.setSavepoint();
            try {
                // 更新用户套餐的用量
                updateUserPackage(usage, conn, dbUserPackage);
                conn.commit();
            } catch (SQLException e) {
                conn.rollback(point);
                throw e;
            } finally {
                conn.releaseSavepoint(point);
            }
        } finally {
            if (conn != null) {
                conn.setAutoCommit(true);
                conn.close();
            }
        }
    }
    
    /**
     * 获取所有ownerid
     * @throws Exception 
     */
    public List<Long> getAllUsrIdFromUserPackage() throws Exception{
        Connection conn = null;
        try {
            conn = DbSource.getConnection();
            return getAllUsrIdFromUserPackage(conn);
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }

    /**
     * 更新用户套餐
     */
    private void updateUserPackage(MinutesUsageMeta usage, Connection conn, DbUserPackage dbUserPackage) throws Exception {
        // 减少用户的套餐的用量
        boolean needFrozen = reduceUserPackage(usage, dbUserPackage);
        // 更新用户套餐
        dbUserPackage.update(conn);
        // 用量不足，需要冻结
        if (needFrozen) {
            frozenUser(usage.getUsrId());
        }
    }
    
    
    /**
     * 获取所有ownerid
     * @throws Exception 
     */
    private List<Long> getAllUsrIdFromUserPackage(Connection conn) throws Exception{
        DbUserPackage dbUserPackage = new DbUserPackage();
        return dbUserPackage.selectAllOwnerId(conn);
    }

    /**
     * 冻结用户
     * @param userId
     * @throws Exception 
     */
    public void frozenUser(long userId) throws Exception {
        // 获取owner信息
        OwnerMeta ownerMeta = new OwnerMeta(userId);
        if(client.ownerSelectById(ownerMeta)) {
            // 如果用户之前没有被冻结
            if (StringUtils.isEmpty(ownerMeta.frozenDate)) {
                // 设置冻结时间
                ownerMeta.frozenDate = ServiceUtils.formatIso8601Date(new Date());
                client.ownerUpdate(ownerMeta);
                log.info("frozen user [" + ownerMeta.getId() + " " + ownerMeta.getName() + "], frozenDate [" + ownerMeta.frozenDate + "].");
                // 发送邮件
                MailRemind.sendMail(ownerMeta.email, (byte)1);
            }
        }else {
            log.info("no such user:" + userId);
        }
    }
    
    /**
     * 减少用户的套餐的用量
     * @param usage
     * @param dbUserPackage
     * @return 套餐不足，需要冻结用户
     * @throws Exception
     */
    private boolean reduceUserPackage(MinutesUsageMeta usage, DbUserPackage dbUserPackage) throws Exception {
        // 是否需要冻结
        boolean needFrozen = false;
        // 超过容量处理，使用峰值容量判断
        if (dbUserPackage.storage < convertToGB(usage.sizeStats.size_peak)) {
            needFrozen = true;
        }
        // 流量
        dbUserPackage.flow -= convertToGB(usage.flowStats.flow_download);
        if (dbUserPackage.flow < 0) {
            dbUserPackage.flow = 0;
            needFrozen = true;
        }
        // 非互联网下行流量
        dbUserPackage.noNetFlow -= convertToGB(usage.flowStats.flow_noNetDownload);
        if (dbUserPackage.noNetFlow < 0) {
            dbUserPackage.noNetFlow = 0;
            needFrozen = true;
        }
        // 互联网 get head 请求次数
        dbUserPackage.ghRequest -= convertToThousand(usage.requestStats.req_get + usage.requestStats.req_head 
                - usage.codeRequestStats.getNetGHRequestNon2xxNum());
        if (dbUserPackage.ghRequest < 0) {
            dbUserPackage.ghRequest = 0;
            needFrozen = true;
        }
        // 非互联网 get head 请求次数
        dbUserPackage.noNetGHReq -= convertToThousand(usage.requestStats.req_noNetGet + usage.requestStats.req_noNetHead
                - usage.codeRequestStats.getNoNetGHRequestNon2xxNum());
        if (dbUserPackage.noNetGHReq < 0) {
            dbUserPackage.noNetGHReq = 0;
            needFrozen = true;
        }
        // 互联网 other 请求次数
        dbUserPackage.otherRequest -= convertToThousand(usage.requestStats.req_other + usage.requestStats.req_put
                + usage.requestStats.req_delete + usage.requestStats.req_post
                - usage.codeRequestStats.getNetOthersRequestNon2xxNum());
        if (dbUserPackage.otherRequest < 0) {
            dbUserPackage.otherRequest = 0;
            needFrozen = true;
        }
        // 非互联网 other 请求次数
        dbUserPackage.noNetOtherReq -= convertToThousand(usage.requestStats.req_noNetOther + usage.requestStats.req_noNetPut
                + usage.requestStats.req_noNetDelete + usage.requestStats.req_noNetPost
                - usage.codeRequestStats.getNoNetOthersRequestNon2xxNum());
        if (dbUserPackage.noNetOtherReq < 0) {
            dbUserPackage.noNetOtherReq = 0;
            needFrozen = true;
        }
        // 漫游下行流量
        dbUserPackage.roamFlow -= convertToGB(usage.flowStats.flow_roamDownload);
        if (dbUserPackage.roamFlow < 0) {
            dbUserPackage.roamFlow = 0;
            needFrozen = true;
        }
        // 漫游上行流量
        dbUserPackage.roamUpload -= convertToGB(usage.flowStats.flow_roamUpload);
        if (dbUserPackage.roamUpload < 0) {
            dbUserPackage.roamUpload = 0;
            needFrozen = true;
        }
        // 非互联网漫游下行流量
        dbUserPackage.noNetRoamFlow -= convertToGB(usage.flowStats.flow_noNetRoamDownload);
        if (dbUserPackage.noNetRoamFlow < 0) {
            dbUserPackage.noNetRoamFlow = 0;
            needFrozen = true;
        }
        // 非互联网漫游上行流量
        dbUserPackage.noNetRoamUpload -= convertToGB(usage.flowStats.flow_noNetRoamUpload);
        if (dbUserPackage.noNetRoamUpload < 0) {
            dbUserPackage.noNetRoamUpload = 0;
            needFrozen = true;
        }
        // 反垃圾请求次数
        dbUserPackage.spamRequest -= convertToThousand(usage.requestStats.req_spam);
        if (dbUserPackage.spamRequest < 0) {
            dbUserPackage.spamRequest = 0;
            needFrozen = true;
        }
        // 图片鉴黄请求次数
        dbUserPackage.porn -= convertToThousand(usage.requestStats.req_pornReviewFalse + usage.requestStats.req_pornReviewTrue);
        if (dbUserPackage.porn < 0) {
            dbUserPackage.porn = 0;
            needFrozen = true;
        }
        return needFrozen;
    }
    
    /**
     * 将单位Byte转换为GB
     * @return
     */
    private double convertToGB(long number) {
         double result = (double) number / Consts.GB;
         // 由于数据库中的小数精度为三位，这里保证用量小于1M不会作为1M被计入
         BigDecimal bigDecimal = new BigDecimal(result);
         return bigDecimal.setScale(3, BigDecimal.ROUND_DOWN).doubleValue();
    }
    /**
     * 将数字转换为以千为单位
     * @return
     */
    private double convertToThousand(long number) {
        return (double) number / Consts.THOUSAND;
    }
    
    public void accountInsertCRMBill(long ownerId) throws SQLException {
        Connection conn = null;
        try {
            conn = DbSource.getConnection();
            conn.setAutoCommit(false);
            Savepoint point = conn.setSavepoint();
            try {
                DbAccount dbAccount = new DbAccount(ownerId);
                if (dbAccount.select(conn)) {
                    dbAccount.isLast = 0;
                    dbAccount.update(conn);
                }
                dbAccount.orderSeq = "";
                dbAccount.tradeDate = TimeUtils.toYYYYMMddHHmmss(new Date());
                dbAccount.deposit = -dbAccount.balance;
                dbAccount.balance = 0;
                dbAccount.withdraw = 0;
                dbAccount.isLast = 1;
                dbAccount.insert(conn);
                conn.commit();
            } catch (SQLException e) {
                conn.rollback(point);
                throw e;
            } finally {
                conn.releaseSavepoint(point);
            }
        } finally {
            if (conn != null) {
                conn.setAutoCommit(true);
                conn.close();
            }
        }
    }

    public boolean orderSelectById(DbOrder dbOrder) throws SQLException {
        Connection conn = null;
        try {
            conn = DbSource.getConnection();
            return dbOrder.selectById(conn);
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }

    public boolean orderSelect(DbOrder dbOrder) throws SQLException {
        Connection conn = null;
        try {
            conn = DbSource.getConnection();
            return dbOrder.select(conn);
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }

    /**
     * 查看用户详单时调用，第一次生成详单时需要调用orderSelect(DbUsageHistory dbHistory,DbOrder
     * dbOrder,OwnerMeta OwnerMeta)方法。
     *
     * @param dbOrder
     *            ：指定ownerId和起止时间
     * @throws SQLException
     */
    public void orderSelectRange(DbOrder dbOrder) throws SQLException {
        Connection conn = null;
        try {
            conn = DbSource.getConnection();
            dbOrder.select(conn);
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }

    public void orderSelectSum(DbOrder dbOrder) throws SQLException {
        Connection conn = null;
        try {
            conn = DbSource.getConnection();
            dbOrder.selectSum(conn);
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }

    public void orderSelectSumByDay(DbOrder dbOrder) throws SQLException {
        Connection conn = null;
        try {
            conn = DbSource.getConnection();
            dbOrder.selectSumByDay(conn);
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }

    public void orderInsert(DbOrder dbOrder) throws SQLException {
        Connection conn = null;
        try {
            conn = DbSource.getConnection();
            dbOrder.insert(conn);
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }

    /**
     * 删除指定时间内的用户订单信息
     *
     * @param dbOrder
     *            ：指定ownerId，和起止时间
     * @throws SQLException
     */
    public void orderDelete(DbOrder dbOrder) throws SQLException {
        Connection conn = null;
        try {
            conn = DbSource.getConnection();
            dbOrder.delete(conn);
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }

    /**
     * 查询最新的账户信息
     *
     * @param dbAccount
     *            ：指定ownerId
     * @throws SQLException
     */
    public void accountSelect(DbAccount dbAccount) throws SQLException {
        Connection conn = null;
        try {
            conn = DbSource.getConnection();
            dbAccount.select(conn);
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }

    public void accountSelectAllBalance(DbAccount dbAccount) throws SQLException {
        Connection conn = null;
        try {
            conn = DbSource.getConnection();
            dbAccount.selectAllBalance(conn);
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }

    /**
     * @param dbAccount
     *            ：指定待查询记录的id，非ownerId
     * @return
     * @throws SQLException
     */
    public boolean accountSelectById(DbAccount dbAccount) throws SQLException {
        Connection conn = null;
        try {
            conn = DbSource.getConnection();
            return dbAccount.selectById(conn);
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }

    /**
     * 查询一定时间内用户的账户信息
     *
     * @param dbAccount
     *            ：指定ownerId和时间范围
     * @throws SQLException
     */
    public void accountSelectRange(DbAccount dbAccount) throws SQLException {
        Connection conn = null;
        try {
            conn = DbSource.getConnection();
            dbAccount.selectRange(conn);
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }

    /**
     * 用户充值成功后插入一条数据
     *
     * @param dbAccount
     *            ：需要指定要插入的ownerId，以及订单号
     * @param
     *            ：用户充值金额
     * @throws SQLException
     */
    public void accountInsert(DbAccount dbAccount, DbResponse dbResponse) throws Exception {
        double deposit = (double) dbResponse.ORDERAMOUNT / 100;// use Yuan as
                                                               // basic unit
        Connection conn = null;
        try {
            conn = DbSource.getConnection();
            conn.setAutoCommit(false);
            Savepoint point = conn.setSavepoint();
            try {
                if (dbAccount.select(conn)) {
                    dbAccount.isLast = 0;
                    dbAccount.update(conn);
                }
                dbAccount.orderSeq = dbResponse.ORDERSEQ;
                dbAccount.tradeDate = TimeUtils.toYYYYMMddHHmmss(new Date());
                dbAccount.withdraw = 0;
                dbAccount.deposit = deposit;
                dbAccount.balance += deposit;
                dbAccount.isLast = 1;
                dbAccount.insert(conn);
                dbResponse.insert(conn);
                DbBill dbBill = new DbBill();
                dbBill.setORDERSEQ(dbResponse.ORDERSEQ);
                dbBill.tag = 1;
                dbBill.update(conn);
                OwnerMeta ownerMeta = new OwnerMeta(dbAccount.getOwnerId());
                client.ownerSelectById(ownerMeta);
                if (dbAccount.balance < ownerMeta.credit) {
                    if ((ownerMeta.frozenDate == "") || (ownerMeta.frozenDate == null)
                            || (ownerMeta.frozenDate.equals(""))) {
                        ownerMeta.frozenDate = ServiceUtils.formatIso8601Date(new Date());
                        client.ownerUpdate(ownerMeta);
                    }
                } else {
                    ownerMeta.frozenDate = "";
                    client.ownerUpdate(ownerMeta);
                }
                conn.commit();
            } catch (SQLException e) {
                conn.rollback(point);
                throw e;
            } finally {
                conn.releaseSavepoint(point);
            }
        } finally {
            if (conn != null) {
                conn.setAutoCommit(true);
                conn.close();
            }
        }
    }

    /**
     *
     * @param dbAccount
     * @param deposit
     *            单位元
     * @throws SQLException
     */
    public void accountInsert(DbAccount dbAccount, OwnerMeta OwnerMeta, double deposit,
            String orderSeq) throws Exception {
        Connection conn = null;
        try {
            conn = DbSource.getConnection();
            conn.setAutoCommit(false);
            Savepoint point = conn.setSavepoint();
            try {
                if (dbAccount.select(conn)) {
                    dbAccount.isLast = 0;
                    dbAccount.update(conn);
                }
                dbAccount.orderSeq = orderSeq;
                dbAccount.tradeDate = TimeUtils.toYYYYMMddHHmmss(new Date());
                dbAccount.withdraw = 0;
                dbAccount.deposit = deposit;
                dbAccount.balance += deposit;
                dbAccount.isLast = 1;
                dbAccount.insert(conn);
                DbBill dbBill = new DbBill();
                dbBill.setORDERSEQ(orderSeq);
                dbBill.tag = 1;
                dbBill.update(conn);
                if (dbAccount.balance < OwnerMeta.credit) {
                    if ((OwnerMeta.frozenDate == "") || (OwnerMeta.frozenDate == null)
                            || (OwnerMeta.frozenDate.equals(""))) {
                        OwnerMeta.frozenDate = ServiceUtils.formatIso8601Date(new Date());
                        client.ownerUpdate(OwnerMeta);
                    }
                } else {
                    OwnerMeta.frozenDate = "";
                    client.ownerUpdate(OwnerMeta);
                }
                conn.commit();
            } catch (SQLException e) {
                conn.rollback(point);
                throw e;
            } finally {
                conn.releaseSavepoint(point);
            }
        } finally {
            if (conn != null) {
                conn.setAutoCommit(true);
                conn.close();
            }
        }
    }

    /**
     *
     * @param dbAccount
     * @param deposit
     *            充值金额，单位元
     * @throws SQLException
     */
    public void accountInsertFromManagement(DbAccount dbAccount, double deposit) throws Exception {
        Connection conn = null;
        try {
            conn = DbSource.getConnection();
            conn.setAutoCommit(false);
            Savepoint point = conn.setSavepoint();
            try {
                if (dbAccount.select(conn)) {
                    dbAccount.isLast = 0;
                    dbAccount.update(conn);
                }
                dbAccount.orderSeq = "";
                dbAccount.tradeDate = TimeUtils.toYYYYMMddHHmmss(new Date());
                dbAccount.withdraw = 0;
                dbAccount.deposit = deposit;
                dbAccount.balance += deposit;
                dbAccount.isLast = 1;
                dbAccount.insert(conn);
                OwnerMeta ownerMeta = new OwnerMeta(dbAccount.getOwnerId());
                client.ownerSelectById(ownerMeta);
                if (dbAccount.balance < ownerMeta.credit) {
                    if (ownerMeta.frozenDate == null || ownerMeta.frozenDate.trim().length() == 0) {
                        ownerMeta.frozenDate = ServiceUtils.formatIso8601Date(new Date());
                        client.ownerUpdate(ownerMeta);
                    }
                } else {
                    ownerMeta.frozenDate = "";
                    client.ownerUpdate(ownerMeta);
                }
                conn.commit();
            } catch (SQLException e) {
                conn.rollback(point);
                throw e;
            } finally {
                conn.releaseSavepoint(point);
            }
        } finally {
            if (conn != null) {
                conn.setAutoCommit(true);
                conn.close();
            }
        }
    }

    /**
     * 用户从云门户充值
     *
     * @param dbAccount
     *            ：需要指定要插入的ownerId，以及订单号
     * @param
     *            ：用户充值金额
     * @throws SQLException
     */
    public void accountInsertFromYunPortal(DbAccount dbAccount) throws Exception {
        Connection conn = null;
        try {
            conn = DbSource.getConnection();
            conn.setAutoCommit(false);
            Savepoint point = conn.setSavepoint();
            try {
                if (dbAccount.select(conn)) {
                    dbAccount.isLast = 0;
                    dbAccount.update(conn);
                }
                dbAccount.orderSeq = "";
                dbAccount.tradeDate = TimeUtils.toYYYYMMddHHmmss(new Date());
                dbAccount.withdraw = 0;
                dbAccount.balance += dbAccount.deposit;
                dbAccount.isLast = 1;
                dbAccount.insert(conn);
                OwnerMeta ownerMeta = new OwnerMeta(dbAccount.getOwnerId());
                client.ownerSelectById(ownerMeta);
                if (dbAccount.balance >= ownerMeta.credit) {
                    ownerMeta.frozenDate = "";
                    client.ownerUpdate(ownerMeta);
                }
                conn.commit();
            } catch (SQLException e) {
                conn.rollback(point);
                throw e;
            } finally {
                conn.releaseSavepoint(point);
            }
        } finally {
            if (conn != null) {
                conn.setAutoCommit(true);
                conn.close();
            }
        }
    }

    public void accountInsertFromYunPortalAfterSearchBalance(DbAccount dbAccount)
            throws SQLException {
        Connection conn = null;
        try {
            conn = DbSource.getConnection();
            conn.setAutoCommit(false);
            Savepoint point = conn.setSavepoint();
            try {
                if (dbAccount.select(conn)) {
                    dbAccount.isLast = 0;
                    dbAccount.update(conn);
                }
                dbAccount.orderSeq = "";
                dbAccount.tradeDate = TimeUtils.toYYYYMMddHHmmss(new Date());
                dbAccount.withdraw = 0;
                dbAccount.deposit = -dbAccount.balance;
                dbAccount.balance = 0;
                dbAccount.isLast = 1;
                dbAccount.insert(conn);
                conn.commit();
            } catch (SQLException e) {
                conn.rollback(point);
                throw e;
            } finally {
                conn.releaseSavepoint(point);
            }
        } finally {
            if (conn != null) {
                conn.setAutoCommit(true);
                conn.close();
            }
        }
    }

    /**
     * @param dbAccount
     *            : 需要事先指定要删除的ownerId和时间范围
     * @throws SQLException
     */
    public void accountDelete(DbAccount dbAccount) throws SQLException {
        Connection conn = null;
        try {
            conn = DbSource.getConnection();
            dbAccount.delete(conn);
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }

    // ops of price
    public void priceInsert(DbPrice dbPrice) throws SQLException {
        Connection conn = null;
        try {
            conn = DbSource.getConnection();
            dbPrice.insert(conn);
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }

    // ops of price
    public void priceSelectAll(DbPrice dbPrice) throws SQLException {
        Connection conn = null;
        try {
            conn = DbSource.getConnection();
            dbPrice.selectAll(conn);
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }

    // ops of price
    public boolean priceSelect(DbPrice dbPrice) throws SQLException {
        Connection conn = null;
        try {
            conn = DbSource.getConnection();
            return dbPrice.select(conn);
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }

    // ops of price
    public void priceUpdate(DbPrice dbPrice) throws SQLException {
        Connection conn = null;
        try {
            conn = DbSource.getConnection();
            dbPrice.update(conn);
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }

    // ops of price
    public void priceDelete(DbPrice dbPrice) throws SQLException {
        Connection conn = null;
        try {
            conn = DbSource.getConnection();
            dbPrice.delete(conn);
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }

    // ops of bill
    public void billInsert(DbBill dbBill) throws SQLException {
        Connection conn = null;
        try {
            conn = DbSource.getConnection();
            dbBill.insert(conn);
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }

    public boolean billSelect(DbBill dbBill) throws SQLException {
        Connection conn = null;
        try {
            conn = DbSource.getConnection();
            return dbBill.select(conn);
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }

    public void billSelectByRange(DbBill dbBill) throws SQLException {
        Connection conn = null;
        try {
            conn = DbSource.getConnection();
            dbBill.selectByRange(conn);
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }

    public void billUpdate(DbBill dbBill) throws SQLException {
        Connection conn = null;
        try {
            conn = DbSource.getConnection();
            dbBill.update(conn);
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }
    
    public int deleteBillByOwnerId(DbBill dbBill) throws SQLException {
        Connection conn = null;
        try {
            conn = DbSource.getConnection();
            return dbBill.deleteBillByOwnerId(conn);
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }

    // ops of response bill
    public void responseInsert(DbResponse dbResponse) throws SQLException {
        Connection conn = null;
        try {
            conn = DbSource.getConnection();
            dbResponse.insert(conn);
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }

    // ops of crmBill
    public void crmBillInsert(DbCRMBill dbCrmBill) throws SQLException {
        Connection conn = null;
        try {
            conn = DbSource.getConnection();
            dbCrmBill.insert(conn);
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }

    public boolean crmBillSelect(DbCRMBill dbCrmBill) throws SQLException {
        Connection conn = null;
        try {
            conn = DbSource.getConnection();
            return dbCrmBill.select(conn);
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }

    public boolean crmBillSelectByOwnerId(DbCRMBill dbCrmBill) throws SQLException {
        Connection conn = null;
        try {
            conn = DbSource.getConnection();
            return dbCrmBill.selectByOwnerId(conn);
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }
    
    public int crmBillBatchDelete(DbCRMBill dbCrmBill) throws SQLException {
        Connection conn = null;
        try {
            conn = DbSource.getConnection();
            return dbCrmBill.batchDelete(conn);
        }finally {
            if(conn != null) {
                conn.close();
            }
        }
    }

    public String orderSelectMonthSum(DbOrder dbOrder) throws SQLException {
        Connection conn = null;
        try {
            conn = DbSource.getConnection();
            return dbOrder.selectMonthSum(conn);
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }
}