package cn.ctyun.oos.server.db.dbprice;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Savepoint;

import cn.ctyun.common.db.DbSource;
import cn.ctyun.oos.server.db.dbpay.DbBill;

public class DBPrice {
    private static DBPrice THIS = new DBPrice();
    
    private DBPrice() {
    }
    
    public static DBPrice getInstance() {
        return THIS;
    }
    
    public void init() throws SQLException {
        Connection conn = DbSource.getConnection();
        try {
             DbUserPackage.create(conn);
             DbUserTieredDiscount.create(conn);
             DbOrganization.create(conn);
             DbSalesman.create(conn);
        } finally {
            conn.close();
        }
    }
    
    /**
     * Purpose: during develop period, clear the last data
     * 
     * @throws SQLException
     */
    public void uninit() throws SQLException {
        Connection conn = DbSource.getConnection();
        try {
             DbUserPackage.drop(conn);
             DbUserTieredDiscount.drop(conn);
             DbOrganization.drop(conn);
             DbSalesman.drop(conn);
        } finally {
            conn.close();
        }
    }
    
    public boolean tieredPriceSelect(DbTieredPrice dbTieredPrice)
            throws SQLException {
        Connection conn = null;
        try {
            conn = DbSource.getConnection();
            return dbTieredPrice.select(conn);
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }
    
    public void tieredPriceSelectAll(DbTieredPrice dbTieredPrice)
            throws SQLException {
        Connection conn = null;
        try {
            conn = DbSource.getConnection();
            dbTieredPrice.selectAll(conn);
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }
    
    public boolean tieredPriceSelectByRange(DbTieredPrice dbTieredPrice)
            throws SQLException {
        Connection conn = null;
        try {
            conn = DbSource.getConnection();
            return dbTieredPrice.selectByRange(conn);
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }
    
    public void tieredPriceInsert(DbTieredPrice dbTieredPrice)
            throws SQLException {
        Connection conn = null;
        try {
            conn = DbSource.getConnection();
            dbTieredPrice.insert(conn);
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }
    
    public void tieredPriceUpdate(DbTieredPrice dbTieredPrice)
            throws SQLException {
        Connection conn = null;
        try {
            conn = DbSource.getConnection();
            dbTieredPrice.update(conn);
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }
    
    public void tieredPriceUpdateDiscount(DbTieredPrice dbTieredPrice)
            throws SQLException {
        Connection conn = null;
        try {
            conn = DbSource.getConnection();
            dbTieredPrice.updateDiscount(conn);
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }
    
    public void tieredPriceUpdateExpiryDate(DbTieredPrice dbTieredPrice)
            throws SQLException {
        Connection conn = null;
        try {
            conn = DbSource.getConnection();
            dbTieredPrice.updateExpiryDate(conn);
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }
    
    public void tieredPriceDelete(DbTieredPrice dbTieredPrice)
            throws SQLException {
        Connection conn = null;
        try {
            conn = DbSource.getConnection();
            dbTieredPrice.delete(conn);
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }
    
    public boolean packageSelect(DbPackage dbPackage) throws SQLException {
        Connection conn = null;
        try {
            conn = DbSource.getConnection();
            return dbPackage.select(conn);
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }
    
    public void packageSelectAll(DbPackage dbPackage, boolean isManager)
            throws SQLException {
        Connection conn = null;
        try {
            conn = DbSource.getConnection();
            dbPackage.selectAll(conn, isManager);
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }
    
    public void packageInsert(DbPackage dbPackage) throws SQLException {
        Connection conn = null;
        try {
            conn = DbSource.getConnection();
            dbPackage.insert(conn);
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }
    
    public void packageUpdate(DbPackage dbPackage) throws SQLException {
        Connection conn = null;
        try {
            conn = DbSource.getConnection();
            dbPackage.update(conn);
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }
    
    public void packageDelete(DbPackage dbPackage) throws SQLException {
        Connection conn = null;
        try {
            conn = DbSource.getConnection();
            dbPackage.delete(conn);
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }
    
    public boolean userPackageSelect(DbUserPackage dbUser) throws SQLException {
        Connection conn = null;
        try {
            conn = DbSource.getConnection();
            return dbUser.select(conn);
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }
    
    public int userPackageExpireCount(DbUserPackage dbUser) throws SQLException {
        Connection conn = null;
        try {
            conn = DbSource.getConnection();
            return dbUser.countExpirePackage(conn);
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }
    
    public boolean userPackageSelectInRange(DbUserPackage dbUser)
            throws SQLException {
        Connection conn = null;
        try {
            conn = DbSource.getConnection();
            return dbUser.selectInRange(conn);
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }
    
    public void userPackageSelectByOwnerId(DbUserPackage dbUser)
            throws SQLException {
        Connection conn = null;
        try {
            conn = DbSource.getConnection();
            dbUser.selectByUsrId(conn);
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }
    
    public void userPackageSelectByPackageId(DbUserPackage dbUser)
            throws SQLException {
        Connection conn = null;
        try {
            conn = DbSource.getConnection();
            dbUser.selectByPackageId(conn);
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }
    
    public boolean userPackageSelectByOrderId(DbUserPackage dbUser)
            throws SQLException {
        Connection conn = null;
        try {
            conn = DbSource.getConnection();
            return dbUser.selectByOrderId(conn);
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }
    
    public void userPackageInsert(DbUserPackage dbUser) throws SQLException {
        Connection conn = null;
        try {
            conn = DbSource.getConnection();
            dbUser.insert(conn);
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }
    
    public void userPackageUpdate(DbUserPackage dbUser) throws SQLException {
        Connection conn = null;
        try {
            conn = DbSource.getConnection();
            dbUser.update(conn);
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }
    
    public void userPackageUpdateAll(DbUserPackage dbUser) throws SQLException {
        Connection conn = null;
        try {
            conn = DbSource.getConnection();
            dbUser.updateAll(conn);
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }

    public void userPackageCloseUserAll(DbUserPackage dbUser) throws SQLException {
        Connection conn = null;
        try {
            conn = DbSource.getConnection();
            dbUser.closeUserAllPackage(conn);
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }
    
    public void userPackageBillUpdate(DbUserPackage dbUserPackage)
            throws SQLException {
        Connection conn = null;
        try {
            conn = DbSource.getConnection();
            conn.setAutoCommit(false);
            Savepoint point = conn.setSavepoint();
            try {
                dbUserPackage.isPaid = 1;
                dbUserPackage.update(conn);
                DbBill dbBill = new DbBill();
                dbBill.setORDERSEQ(dbUserPackage.orderId);
                dbBill.tag = 1;
                dbBill.update(conn);
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
    
    public void userPackageDelete(DbUserPackage dbUser) throws SQLException {
        Connection conn = null;
        try {
            conn = DbSource.getConnection();
            dbUser.delete(conn);
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }
    
    public void userPackageDeleteByOwnerId(DbUserPackage dbUser) throws SQLException {
        Connection conn = null;
        try {
            conn = DbSource.getConnection();
            dbUser.deleteByOwnerId(conn);
        }finally {
            if(conn != null) {
                conn.close();
            }
        }
    }
    
    public boolean userTieredDiscountSelect(DbUserTieredDiscount dbUser)
            throws SQLException {
        Connection conn = null;
        try {
            conn = DbSource.getConnection();
            return dbUser.select(conn);
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }
    
    public boolean userTieredDiscountSelectByOrderId(DbUserTieredDiscount dbUser)
            throws SQLException {
        Connection conn = null;
        try {
            conn = DbSource.getConnection();
            return dbUser.selectByOrderId(conn);
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }
    
    public void userTieredDiscountInsert(DbUserTieredDiscount dbUser)
            throws SQLException {
        Connection conn = null;
        try {
            conn = DbSource.getConnection();
            dbUser.insert(conn);
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }
    
    public void userTieredDiscountUpdate(DbUserTieredDiscount dbUser)
            throws SQLException {
        Connection conn = null;
        try {
            conn = DbSource.getConnection();
            dbUser.update(conn);
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }
    
    public void userTieredDiscountDelete(DbUserTieredDiscount dbUser)
            throws SQLException {
        Connection conn = null;
        try {
            conn = DbSource.getConnection();
            dbUser.delete(conn);
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }
    
    public void userTieredDiscountDeleteByOwnerId(DbUserTieredDiscount dbUser)
            throws SQLException {
        Connection conn = null;
        try {
            conn = DbSource.getConnection();
            dbUser.deleteByOwnerId(conn);
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }
    
    public boolean organizationSelect(DbOrganization dbOrganization)
            throws SQLException {
        Connection conn = null;
        try {
            conn = DbSource.getConnection();
            return dbOrganization.select(conn);
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }
    
    public void organizationSelectAll(DbOrganization dbOrganization)
            throws SQLException {
        Connection conn = null;
        try {
            conn = DbSource.getConnection();
            dbOrganization.selectAll(conn);
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }
    
    public void organizationInsert(DbOrganization dbOrganization)
            throws SQLException {
        Connection conn = null;
        try {
            conn = DbSource.getConnection();
            dbOrganization.insert(conn);
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }
    
    public void organizationUpdate(DbOrganization dbOrganization)
            throws SQLException {
        Connection conn = null;
        try {
            conn = DbSource.getConnection();
            dbOrganization.update(conn);
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }
    
    public void organizationDelete(DbOrganization dbOrganization)
            throws SQLException {
        Connection conn = null;
        try {
            conn = DbSource.getConnection();
            dbOrganization.delete(conn);
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }
    
    /**
     * 查询某销售经理下所有的客户
     * 
     * @param dbSalerUser
     * @return
     * @throws SQLException
     */
    public boolean salerUserSelect(DbSalerUser dbSalerUser) throws SQLException {
        Connection conn = null;
        try {
            conn = DbSource.getConnection();
            return dbSalerUser.select(conn);
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }
    
    /**
     * 查询某客户所对应的销售经理
     * 
     * @param dbSalerUser
     * @return
     * @throws SQLException
     */
    public boolean salerUserSelectByUser(DbSalerUser dbSalerUser)
            throws SQLException {
        Connection conn = null;
        try {
            conn = DbSource.getConnection();
            return dbSalerUser.selectByUser(conn);
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }
    
    public void salerUserInsert(DbSalerUser dbSalerUser) throws SQLException {
        Connection conn = null;
        try {
            conn = DbSource.getConnection();
            dbSalerUser.insert(conn);
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }
    
    public void salerUserUpdate(DbSalerUser dbSalerUser) throws SQLException {
        Connection conn = null;
        try {
            conn = DbSource.getConnection();
            dbSalerUser.update(conn);
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }
    
    /**
     * 删除某销售经理属下的某客户
     * 
     * @param dbSalerUser
     * @throws SQLException
     */
    public void salerUserDelete(DbSalerUser dbSalerUser) throws SQLException {
        Connection conn = null;
        try {
            conn = DbSource.getConnection();
            dbSalerUser.delete(conn);
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }
    
    public boolean salesmanSelect(DbSalesman dbSalesman) throws SQLException {
        Connection conn = null;
        try {
            conn = DbSource.getConnection();
            return dbSalesman.select(conn);
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }
    
    public boolean salesmanSelectById(DbSalesman dbSalesman)
            throws SQLException {
        Connection conn = null;
        try {
            conn = DbSource.getConnection();
            return dbSalesman.selectById(conn);
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }
    
    public boolean salesmanSelectByVerify(DbSalesman dbSalesman)
            throws SQLException {
        Connection conn = null;
        try {
            conn = DbSource.getConnection();
            return dbSalesman.selectByVerify(conn);
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }
    
    public void salesmanSelectByOrganizationId(DbSalesman dbSalesman,
            boolean isManager) throws SQLException {
        Connection conn = null;
        try {
            conn = DbSource.getConnection();
            dbSalesman.selectByOrganizationId(conn, isManager);
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }
    
    public void salesmanInsert(DbSalesman dbSalesman) throws SQLException {
        Connection conn = null;
        try {
            conn = DbSource.getConnection();
            dbSalesman.insert(conn);
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }
    
    public void salesmanUpdate(DbSalesman dbSalesman) throws SQLException {
        Connection conn = null;
        try {
            conn = DbSource.getConnection();
            dbSalesman.update(conn);
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }
    
    public void salesmanDelete(DbSalesman dbSalesman) throws SQLException {
        Connection conn = null;
        try {
            conn = DbSource.getConnection();
            dbSalesman.delete(conn);
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }
}