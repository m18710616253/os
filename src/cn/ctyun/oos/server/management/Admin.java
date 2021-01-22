package cn.ctyun.oos.server.management;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.URLDecoder;
import java.net.UnknownHostException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.CharEncoding;
import org.apache.commons.lang3.EnumUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.amazonaws.AmazonClientException;
import com.amazonaws.HttpMethod;
import com.amazonaws.services.s3.internal.ServiceUtils;
import com.amazonaws.services.s3.internal.XmlWriter;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import cn.ctyun.common.BaseException;
import cn.ctyun.common.Consts;
import cn.ctyun.common.Session;
import cn.ctyun.common.conf.GlobalHHZConfig;
import cn.ctyun.common.dsync.ZKClient;
import cn.ctyun.common.node.OosZKNode;
import cn.ctyun.common.region.DataRegions;
import cn.ctyun.oos.bssAdapter.server.BssAdapterClient;
import cn.ctyun.oos.bssAdapter.server.SourceTYPE;
import cn.ctyun.oos.bssAdapter.server.TaskType;
import cn.ctyun.oos.bssAdapter.server.UpdateType;
import cn.ctyun.oos.common.Email;
import cn.ctyun.oos.common.ErrorMessage;
import cn.ctyun.oos.common.Parameters;
import cn.ctyun.oos.common.SocketEmail;
import cn.ctyun.oos.common.Utils;
import cn.ctyun.oos.common.WebsiteHeader;
import cn.ctyun.oos.hbase.ManagementUser;
import cn.ctyun.oos.hbase.MetaClient;
import cn.ctyun.oos.metadata.BucketMeta;
import cn.ctyun.oos.metadata.BucketOwnerConsistencyMeta;
import cn.ctyun.oos.metadata.BucketOwnerConsistencyMeta.BucketOwnerConsistencyType;
import cn.ctyun.oos.metadata.MinutesUsageMeta;
import cn.ctyun.oos.metadata.MinutesUsageMeta.UsageMetaType;
import cn.ctyun.oos.metadata.NoNetIPMeta;
import cn.ctyun.oos.metadata.OwnerMeta;
import cn.ctyun.oos.metadata.RoleMeta;
import cn.ctyun.oos.metadata.RoleMeta.RolePermission;
import cn.ctyun.oos.metadata.UserOstorWeightMeta;
import cn.ctyun.oos.metadata.UserToRoleMeta;
import cn.ctyun.oos.server.conf.BssAdapterConfig;
import cn.ctyun.oos.server.db.DB;
import cn.ctyun.oos.server.db.DBSelect;
import cn.ctyun.oos.server.db.DbBucketWebsite;
import cn.ctyun.oos.server.db.dbprice.DBPrice;
import cn.ctyun.oos.server.db.dbprice.DbOrganization;
import cn.ctyun.oos.server.db.dbprice.DbPackage;
import cn.ctyun.oos.server.db.dbprice.DbSalesman;
import cn.ctyun.oos.server.db.dbprice.DbTieredPrice;
import cn.ctyun.oos.server.db.dbprice.DbUserPackage;
import cn.ctyun.oos.server.util.DateParser;
import cn.ctyun.oos.server.util.Misc;
import common.io.StreamUtils;
import common.time.TimeUtils;
import common.tuple.Pair;
import common.util.ConfigUtils;

/**
 * @author: Cui Meng
 */
public class Admin {
    private static final Log log = LogFactory.getLog(Admin.class);
    public static CompositeConfiguration config;
    public static Session<String, String> session;
    public static CompositeConfiguration oosConfig;
    private static MetaClient client = MetaClient.getGlobalClient();
    private static ZKClient zkClient;
    static {
        File[] xmlConfs = { new File(System.getenv("OOS_HOME") + "/conf/management-online.xml"),
                new File(System.getenv("OOS_HOME") + "/conf/management.xml") };
        try {
            config = (CompositeConfiguration) ConfigUtils.loadXmlConfig(xmlConfs);
            File[] oosXmlConfs = { new File(System.getenv("OOS_HOME") + "/conf/oos-online.xml"),
                    new File(System.getenv("OOS_HOME") + "/conf/oos.xml") };
            oosConfig = (CompositeConfiguration) ConfigUtils.loadXmlConfig(oosXmlConfs);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        try {
            zkClient = new ZKClient(GlobalHHZConfig.getQuorumServers(), GlobalHHZConfig.getSessionTimeout());
        } catch (IOException e) {
            log.error("zk error", e);
        }
    }
    
    public static void registerOrganization(DbOrganization dbOrganization) throws BaseException,
            SQLException {
        DBPrice dbPrice = DBPrice.getInstance();
        if (dbPrice.organizationSelect(dbOrganization))
            dbPrice.organizationUpdate(dbOrganization);
        else
            dbPrice.organizationInsert(dbOrganization);
    }

    public static String getOrganization() throws SQLException {
        DbOrganization dbOrganization = new DbOrganization();
        DBPrice dbPrice = DBPrice.getInstance();
        dbPrice.organizationSelectAll(dbOrganization);
        XmlWriter xml = new XmlWriter();
        xml.start("Organizations");
        for (DbOrganization o : dbOrganization.orgs) {
            xml.start("Organization");
            xml.start("OrganizationId").value(o.organizationId).end();
            xml.start("OrganizationName").value(o.name).end();
            xml.end();
        }
        xml.end();
        return xml.toString();
    }
    
    public static void registerSalesManager(DbSalesman dbSalesman, boolean sendEmailToSalesman)
            throws BaseException, SQLException, IOException {
        DBPrice dbPrice = DBPrice.getInstance();
        if (dbPrice.salesmanSelect(dbSalesman))
            throw new BaseException(400, "SalesmanAlreadyExists");
        Email email = new Email(oosConfig);
        if (sendEmailToSalesman) {
            String password = RandomStringUtils.randomAlphanumeric(6);
            dbSalesman.setPwd(password);
            email.setSubject(config.getString("email.registerSalesSubject"));
            String body = email.adminRegisterSalesBody;
            body = body.replace("USERNAME", StringEscapeUtils.escapeHtml4(dbSalesman.email));
            body = body.replace("PASSWORD", StringEscapeUtils.escapeHtml4(dbSalesman.getPwd()));
            body = body.replace("DATE", Misc.formatYearMonthDate(new Date()));
            email.setBody(body);
            email.setTo(dbSalesman.email);
        } else {
            dbSalesman.verify = UUID.randomUUID().toString();
            email.setSubject(config.getString("email.registerSalesSubject"));
            String body = email.verifySalesBody;
            body = body.replace("NAME", StringEscapeUtils.escapeHtml4(dbSalesman.name));
            body = body.replace("ID", dbSalesman.id);
            body = body.replace("EMAIL", dbSalesman.email);
            body = body.replace("PHONE", dbSalesman.phone);
            body = body.replace("LINK",
                    "http://" + email.getDomainSuffix() + ":" + config.getString("port")
                            + "/?verifySalesman=" + dbSalesman.verify);
            body = body.replace("DATE", Misc.formatYearMonthDate(new Date()));
            email.setBody(body);
            DbSalesman manager = new DbSalesman();
            manager.organizationId = dbSalesman.organizationId;
            DbOrganization dbo = new DbOrganization(dbSalesman.organizationId);
            dbPrice.salesmanSelectByOrganizationId(manager, true);
            if (manager.sms.size() == 0)
                throw new BaseException(400, "NoSalesManager");
            if (!dbPrice.organizationSelect(dbo))
                throw new BaseException(400, "NoSuchOrganization");
            String to = "";
            for (DbSalesman m : manager.sms) {
                to += m.email + ",";
            }
            to = to.substring(0, to.length() - 1);
            email.setTo(to);
        }
        email.setNick(email.getEmailNick());
        email.setFrom(email.getFrom189());
        email.setSmtp(email.getSmtp189());
        SocketEmail.sendEmail(email);
        dbPrice.salesmanInsert(dbSalesman);
    }
    
    private static void addPackage(DbPackage dbPackage) throws SQLException, BaseException {
        checkPackageParameter(dbPackage, true);
        DBPrice dbPrice = DBPrice.getInstance();
        dbPrice.packageInsert(dbPackage);
        log.info("Admin add package success, the package name is:" + dbPackage.name);
    }
    
    public static boolean isAdmin(HttpServletRequest req) throws Exception {
        String userName = session.get(Utils.getSessionId(req));
        ManagementUser user=new ManagementUser(userName);
        return client.managementUserGet(user);
    }
    
    public static void handleAdminRequest(HttpServletRequest req, HttpServletResponse resp)
            throws Exception {
        if (!isAdmin(req))
            throw new BaseException(403, "NotAdmin");
        if (req.getParameter(ManagementHeader.ADMIN) != null
                && req.getParameter(ManagementHeader.ADMIN).equals("selectSQL")
                || req.getHeader(ManagementHeader.ADMIN).equals("selectSQL")) {
            handleSelectSQL(req, resp);
            return;
        }
        if (req.getHeader(ManagementHeader.ADMIN).equals("organization")) {
            handleOrganization(req, resp);
            return;
        }
        if (req.getHeader(ManagementHeader.ADMIN).equals("salesman")) {
            handleSalesManager(req, resp);
            return;
        }
        if (req.getHeader(ManagementHeader.ADMIN).equals("package")) {
            handlePackage(req, resp);
            return;
        }
        if (req.getHeader(ManagementHeader.ADMIN).equals("tieredPrice")) {
            handleTieredPrice(req, resp);
            return;
        }
        if (req.getHeader(ManagementHeader.ADMIN).equals("activity")) {
            handleActivity(req, resp);
            return;
        }
        if (req.getHeader(ManagementHeader.ADMIN).equals("accessKey")) {
            handleAccessKey(req, resp);
            return;
        }
        if (req.getHeader(ManagementHeader.ADMIN).equals("user")) {
            handleUser(req, resp);
            return;
        }
        if (req.getHeader(ManagementHeader.ADMIN).equals("bucketWebsite")) {
            handleBucketWebsite(req, resp);
            return;
        }
        // 非互联网IP配置
        if (req.getHeader(ManagementHeader.ADMIN).equals("noninternetIP")) {
            handleNoninternetIP(req, resp);
            return;
        }
        //处理role
        if (req.getHeader(ManagementHeader.ADMIN).equals("role")) {
            handleRole(req, resp);
            return;
        }
        //处理usertorole
        if (req.getHeader(ManagementHeader.ADMIN).equals("userToRole")) {
            handleUserToRole(req, resp);
            return;
        }
        // 查询所有数据域
        if (req.getHeader(ManagementHeader.ADMIN).equals("allDataRegions")) {
            handleValidDataRegions(resp);
            return;
        }
        // 维护 用户集群权重
        if (req.getHeader(ManagementHeader.ADMIN).equals("userToRegionOstor")) {
            handleUserOstorWeight(req, resp);
            return;
        }
    }
    
    /**
     * 处理角色相关请求
     * @param req
     * @param resp
     * @return
     * @throws Exception
     * @author wushuang
     */
    private static boolean handleRole(HttpServletRequest req, HttpServletResponse resp)
            throws Exception {
        if (req.getMethod().equalsIgnoreCase(HttpMethod.PUT.toString())) {
            if (req.getParameter(Parameters.ROLE_ID).equals("0")) { // roleID=0为新建角色
                RoleMeta roleMeta = createRoleMetaFromRequest(req);
                if (client.roleSelect(roleMeta))
                    throw new BaseException(400, "RoleAlreadyExist");// 该角色已存在
                // 检查权限是否合法且不为空
                checkRolePermission(roleMeta);
                
                BssAdapterClient.modifyRole(BssAdapterConfig.localPool.ak, BssAdapterConfig.localPool.getSK(), 
                        TaskType.UPDATEROLE_REG, "0",roleMeta,new String[]{BssAdapterConfig.localPool.name},
                        (code)->{
                            if (code==200) {
                                if (!BssAdapterConfig.localPool.name.equals(BssAdapterConfig.name)) {
                                    client.roleInsert(roleMeta);
                                }                                
                            } else {
                                throw new BaseException(500, "InsertRoleFail");
                            }
                        });
            } else { // roleID ！= 0 为修改角色(提交过来的roleID是旧ID)
                long roleID = Long.parseLong(req.getParameter(Parameters.ROLE_ID));
                RoleMeta oldRoleMeta = new RoleMeta(roleID);
                if(!client.roleSelect(oldRoleMeta))
                    throw new BaseException(404, "NoSuchRole");
                
                RoleMeta roleMeta = createRoleMetaFromRequest(req);
                if (roleID != roleMeta.getId()) {
                    throw new BaseException(400, "InvalidRoleNameWithOldId"); //当新旧角色名不同 (规定不可修改角色名)
                }
                
                // 检查权限是否合法且不为空
                checkRolePermission(roleMeta);
                BssAdapterClient.modifyRole(BssAdapterConfig.localPool.ak, BssAdapterConfig.localPool.getSK(), 
                        TaskType.UPDATEROLE_UDP,roleID+"",roleMeta,new String[]{BssAdapterConfig.localPool.name},
                        (code)->{
                            if (code==200) {
                                if (!BssAdapterConfig.localPool.name.equals(BssAdapterConfig.name)) {
                                    client.roleInsert(roleMeta);
                                }                                
                            } else {
                                throw new BaseException(500, "UpdateRoleFail");
                            }
                        });
            }
        } else if (req.getMethod().equalsIgnoreCase(HttpMethod.GET.toString())) {
            if (req.getParameter(Parameters.ROLE_NAME) != null) {
                if (req.getParameter(Parameters.ROLE_BIND) != null) { // 获取已绑定该角色的用户
                    String roleName = req.getParameter(Parameters.ROLE_NAME);
                    RoleMeta roleMeta = new RoleMeta(roleName);
                    if (!client.roleSelect(roleMeta))
                        throw new BaseException(404, "NoSuchRole");
                    List<UserToRoleMeta> userToRoleMetas = new ArrayList<>();
                    userToRoleMetas = client.userToRoleScanByRole(roleMeta.getId(), 0);
                    if (userToRoleMetas.size() == 0)
                        throw new BaseException(404, "RoleNoBindUser");

                    XmlWriter xml = new XmlWriter();
                    xml.start("Role");
                    xml.start("RoleBoundUsers");
                    for (UserToRoleMeta userToRoleMeta : userToRoleMetas) {
                        OwnerMeta owner = new OwnerMeta(userToRoleMeta.getOwnerId());
                        client.ownerSelectById(owner);
                        xml.start("RoleBoundUser").value(owner.getName()).end();
                    }
                    xml.end();
                    xml.end();
                    Common.writeResponseEntity(resp, xml.toString());

                } else { // 获取該角色
                    String roleName = req.getParameter(Parameters.ROLE_NAME);
                    RoleMeta roleMeta = new RoleMeta(roleName);
                    // roleMeta.getIdByName(roleName);
                    if (!client.roleSelect(roleMeta))
                        throw new BaseException(404, "NoSuchRole");

                    XmlWriter xml = new XmlWriter();
                    xml.start("Role");
                    xml.start("RoleId").value(String.valueOf(roleMeta.getId())).end();
                    xml.start("RoleName").value(roleMeta.getName()).end();
                    xml.start("Pools");
                    for (Entry<RolePermission, List<String>> permission : roleMeta.getPools().entrySet()) {
                        xml.start("RolePermission");
                        xml.start("PermissionType").value(permission.getKey().toString()).end();
                        xml.start("PermissionChName").value(permission.getKey().getCNName()).end();
                        for (String poolName : permission.getValue()) {
                            xml.start("PermissionScope").value(poolName).end();
                        }
                        xml.end();
                    }
                    xml.end();
                    xml.start("Regions");
                    for (Entry<RolePermission, List<String>> permission : roleMeta.getRegions().entrySet()) {
                        xml.start("RolePermission");
                        xml.start("PermissionType").value(permission.getKey().toString()).end();
                        xml.start("PermissionChName").value(permission.getKey().getCNName()).end();
                        for (String regionName : permission.getValue()) {
                            xml.start("PermissionScope").value(regionName).end();
                        }
                        xml.end();
                    }
                    xml.end();
                    xml.start("RoleDescription").value(roleMeta.getDescription()).end();
                    xml.end();
                    Common.writeResponseEntity(resp, xml.toString());
                }
            } else { // 获取所有角色
                List<RoleMeta> roleMetas = new ArrayList<RoleMeta>();
                roleMetas = client.roleSelectAll();

                XmlWriter xml = new XmlWriter();
                xml.start("Roles");
                for (RoleMeta roleMeta : roleMetas) {
                    xml.start("Role");
                    xml.start("RoleId").value(String.valueOf(roleMeta.getId())).end();
                    xml.start("RoleName").value(roleMeta.getName()).end();
                    xml.start("Pools");
                    for (Entry<RolePermission, List<String>> permission : roleMeta.getPools().entrySet()) {
                        xml.start("RolePermission");
                        xml.start("PermissionType").value(permission.getKey().toString()).end();
                        xml.start("PermissionChName").value(permission.getKey().getCNName()).end();
                        for (String poolName : permission.getValue()) {
                            xml.start("PermissionScope").value(poolName).end();
                        }
                        xml.end();
                    }
                    xml.end();
                    xml.start("Regions");
                    for (Entry<RolePermission, List<String>> permission : roleMeta.getRegions().entrySet()) {
                        xml.start("RolePermission");
                        xml.start("PermissionType").value(permission.getKey().toString()).end();
                        xml.start("PermissionChName").value(permission.getKey().getCNName()).end();
                        for (String regionName : permission.getValue()) {
                            xml.start("PermissionScope").value(regionName).end();
                        }
                        xml.end();
                    }
                    xml.end();
                    xml.start("RoleDescription").value(roleMeta.getDescription()).end();
                    xml.end();
                }
                xml.end();
                Common.writeResponseEntity(resp, xml.toString());
            }

        } else if (req.getMethod().equalsIgnoreCase(HttpMethod.DELETE.toString())) { // 删除角色
            Common.checkParameter(req.getParameter(Parameters.ROLE_NAME));
            String roleName = req.getParameter(Parameters.ROLE_NAME);
            RoleMeta roleMeta = new RoleMeta(roleName);
            if (client.userToRoleScanByRole(roleMeta.getId(), 1).size() > 0)
                throw new BaseException(400, "RoleAlreadyUsed");

            BssAdapterClient.modifyRole(BssAdapterConfig.localPool.ak, BssAdapterConfig.localPool.getSK(), 
                    TaskType.UPDATEROLE_DEL,String.valueOf(roleMeta.getId()),roleMeta,
                    new String[]{BssAdapterConfig.localPool.name},
                    (code)->{
                        if (code==200) {
                            if (!BssAdapterConfig.localPool.name.equals(BssAdapterConfig.name)) {
                                client.roleDelete(roleMeta);
                            }                                
                        } else {
                            throw new BaseException(500, "UpdateRoleFail");
                        }
                    });
        }
        return true;
    }
    
    private static RoleMeta createRoleMetaFromRequest(HttpServletRequest req) throws Exception {
        RoleMeta roleMeta = new RoleMeta();
        // 获取角色json
        roleMeta.read(IOUtils.toByteArray(req.getInputStream()));
        // 检查角色命名是否合法
        if (roleMeta.getName() == null || roleMeta.getName().equals("") || 
                roleMeta.getName().length() > Consts.ROLE_NAME_MAX_LENGTH)
            throw new BaseException(400, "InvalidRoleName");
        roleMeta.setIdByName();
        
        return roleMeta;
    }

    private static void checkRolePermission(RoleMeta roleMeta) throws IOException, BaseException {
        if (roleMeta.getRegions() != null && !roleMeta.getRegions().isEmpty()) {
            for (Entry<RolePermission, List<String>> permission : roleMeta.getRegions().entrySet()) {
                RolePermission type = permission.getKey();
                List<String> scope = permission.getValue();
                if (type.equals(RolePermission.PERMISSION_AVAIL_BW)) {
                    if (scope != null && !scope.isEmpty()) {
                        for (String region : scope) {
                            if (!DataRegions.getAllRegions().contains(region)) {
                                throw new BaseException(400, "ABWPInvalidRegion");// 可用带宽权限中数据域不合法
                            }
                        }
                    } else {
                        throw new BaseException(400, type + " NoRegions");// 可用带宽权限中没有数据域
                    }
                } else if (EnumUtils.isValidEnum(RolePermission.class, type.toString()) == false) {
                    throw new BaseException(400, "InvalidPermissions");// 权限不合法
                }
            }
        } else {
            throw new BaseException(400, "InvalidPermissions");
        }
    }

    /**
     * 处理用户角色相关请求
     * @param req
     * @param resp
     * @return
     * @throws Exception 
     * @author wushuang
     */
    private static boolean handleUserToRole(HttpServletRequest req, HttpServletResponse resp) throws Exception {
        if (req.getMethod().equalsIgnoreCase(HttpMethod.PUT.toString())) { // 绑定用户角色
            Common.checkParameter(req.getParameter(Parameters.USER_NAME));
            String userName = req.getParameter(Parameters.USER_NAME);
            OwnerMeta owner = new OwnerMeta(userName);
            if (!client.ownerSelect(owner))
                throw new BaseException(404, "NoSuchUser");
            Common.checkParameter(req.getParameter(Parameters.ROLE_NAME));
            String roleName = req.getParameter(Parameters.ROLE_NAME);
            RoleMeta role = new RoleMeta(roleName);
            // 检查角色合法性
            if (!client.roleSelect(role))
                throw new BaseException(404, "NoSuchRole");
            // 检查是否已经绑定该角色
            UserToRoleMeta userToRole = new UserToRoleMeta(owner.getId());
            client.userToRoleSelect(userToRole);
            List<Long> roleIDs = userToRole.getRoleID(); // 当前用户所有角色id
            if (roleIDs.contains(role.getId()))
                throw new BaseException(400, "RoleAlreadyBind");// 角色已被绑定
            userToRole.getRoleID().add(role.getId());
            
            BssAdapterClient.modifyUserToRole(BssAdapterConfig.localPool.ak, BssAdapterConfig.localPool.getSK(), 
                    userToRole,TaskType.UPDATEUSERROLE_REG,new String[]{BssAdapterConfig.localPool.name},
                    (code)->{
                        if (code==200) {
                            if (!BssAdapterConfig.localPool.name.equals(BssAdapterConfig.name)) {
                                UserToRoleMeta userToRoleInsert = new UserToRoleMeta(owner.getId()); // 插入该角色
                                userToRoleInsert.getRoleID().add(role.getId());
                                client.userToRoleInsert(userToRoleInsert);
                            }
                        } else {
                            throw new BaseException(500, "InsertUserToRoleFail");
                        }
                    });
        } else if (req.getMethod().equalsIgnoreCase(HttpMethod.GET.toString())) { // 获取用户所有角色
            Common.checkParameter(req.getParameter(Parameters.USER_NAME));
            String userName = req.getParameter(Parameters.USER_NAME);
            OwnerMeta owner = new OwnerMeta(userName);
            if (!client.ownerSelect(owner))
                throw new BaseException(404, "NoSuchUser");                   
            UserToRoleMeta userToRole = new UserToRoleMeta(owner.getId());                    
            client.userToRoleSelect(userToRole);                    
            List<Long> roleIDs = userToRole.getRoleID(); // 当前用户所有角色id                   
            if (roleIDs.isEmpty())                    
                throw new BaseException(404, "UserNoRoles"); // 用户没有绑定角色     
            List<RoleMeta> roles = new ArrayList<>();
            for (Long roleID : roleIDs) {                      
                RoleMeta roleMeta = new RoleMeta(roleID);                     
                client.roleSelect(roleMeta);                    
                roles.add(roleMeta);
            }
                
            XmlWriter xml = new XmlWriter();
            xml.start("UserToRole");
            xml.start("Roles");
            for (RoleMeta roleMeta : roles) {
                xml.start("Role");
                xml.start("RoleId").value(String.valueOf(roleMeta.getId())).end();
                xml.start("RoleName").value(roleMeta.getName()).end();
                xml.start("Pools");
                for (Entry<RolePermission, List<String>> permission : roleMeta.getPools().entrySet()) {
                    xml.start("RolePermission");
                    xml.start("PermissionType").value(permission.getKey().toString()).end();
                    xml.start("PermissionChName").value(permission.getKey().getCNName()).end();
                  for (String regions : permission.getValue()) {
                      xml.start("PermissionScope").value(regions).end();
                  }
                  xml.end();
                }
                xml.end();
                xml.start("Regions");
                for (Entry<RolePermission, List<String>> permission : roleMeta.getRegions().entrySet()) {
                    xml.start("RolePermission");
                    xml.start("PermissionType").value(permission.getKey().toString()).end();
                    xml.start("PermissionChName").value(permission.getKey().getCNName()).end();
                  for (String regions : permission.getValue()) {
                      xml.start("PermissionScope").value(regions).end();
                  }
                  xml.end();
                }
                xml.end();
                xml.start("RoleDescription").value(roleMeta.getDescription()).end();
                xml.end();
            }
            xml.end();
            xml.end();
            Common.writeResponseEntity(resp, xml.toString());
        } else if (req.getMethod().equalsIgnoreCase(HttpMethod.DELETE.toString())) { // 删除用户已绑定角色
            Common.checkParameter(req.getParameter(Parameters.USER_NAME));
            String userName = req.getParameter(Parameters.USER_NAME);
            Common.checkParameter(req.getParameter(Parameters.ROLE_NAME));
            String roleName = req.getParameter(Parameters.ROLE_NAME);
            RoleMeta roleMeta = new RoleMeta(roleName);
            OwnerMeta owner = new OwnerMeta(userName);
            UserToRoleMeta userToRole = new UserToRoleMeta(owner.getId());
            
            client.userToRoleSelect(userToRole);
            List<Long> roleIDs = userToRole.getRoleID(); // 当前用户所有角色id
            roleIDs.remove(roleMeta.getId());
            BssAdapterClient.modifyUserToRole(BssAdapterConfig.localPool.ak, BssAdapterConfig.localPool.getSK(), 
                    userToRole,TaskType.UPDATEUSERROLE_DEL,new String[]{BssAdapterConfig.localPool.name},
                    (code)->{
                        if (code==200) {
                            if (!BssAdapterConfig.localPool.name.equals(BssAdapterConfig.name)) {
                                client.userToRoleDeleteColumn(userToRole, String.valueOf(roleMeta.getId()));
                            }
                        } else {
                            throw new BaseException(500, "DeleteUserToRoleFail");
                        }
                    });
        }
        return true;
    }

    public static boolean isLengthRight(String para, int minLength, int maxLength) {
        if (para == null)
            return false;
        if (para.length() == 0 || para.length() > maxLength || para.length() < minLength)
            return false;
        return true;
    }
    
    private static void handleUser(HttpServletRequest req, HttpServletResponse resp)
            throws Exception {
        if (req.getMethod().equals(HttpMethod.PUT.toString())) {
            String source = req.getParameter(Parameters.REGISTER_SOURCE_TYPE);
            String comment = req.getParameter("comment");
            String operation = req.getParameter("operation");
            String ownerName = req.getParameter("ownerName");
            
            if (!Arrays.toString(SourceTYPE.values()).contains(source)) {
                throw new BaseException(405, "InvalidSourceType");
            }
            //接受本地业务管理平台的请求
            if (SourceTYPE.valueOf(source) == SourceTYPE.MANAGEMENT) {
                UpdateType updateType = null;
                
                Common.checkParameter(ownerName);
                OwnerMeta owner = new OwnerMeta(ownerName);
                if (!client.ownerSelect(owner))
                    throw new BaseException(404, "NoSuchUser");
                //1. 修改本地池用户信息
                if (comment != null) {
                    //增加备注是一个单独的功能，目前与其他功能互斥
                    if (comment.length() > MConsts.OWNER_COMMENT_MAX_LENGTH)
                        throw new BaseException();
                    owner.comment = comment;
                    updateType = UpdateType.COMMENT;
                } else if (operation != null) {
                    switch (operation) {
                    case "active":
                        owner.verify = null;
                        updateType = UpdateType.ACTIVE;
                        break;
                    case "frozen":
                        owner.frozenDate = ServiceUtils.formatIso8601Date(DateUtils.addDays(new Date(),
                                -1));
                        updateType = UpdateType.FREEZE;
                        break;
                    case "notFrozen":
                        owner.frozenDate = "";
                        updateType = UpdateType.UNFREEZE;
                        break;
                    case "delete":
                        log.info("delete user:" + owner.name);
                        updateType = UpdateType.DELETE;
                        break;
                    case "changePWD":
                        String newPwd = req.getHeader(WebsiteHeader.NEW_PASSWORD);
                        if (!isLengthRight(newPwd, Consts.PASSWORD_MIN_LENGTH, Consts.PASSWORD_MAX_LENGTH))
                            throw new BaseException(400, "InvalidLength");
                        if (!Utils.isValidPassword(newPwd))
                            throw new BaseException(400, "InvalidPassword");
                        
                        owner.setPwd(newPwd);
                        updateType = UpdateType.PASSWORD;
                        break;
                    case "changeUserType":
                        //业务管理平台修改用户类型，改为广播
                        int bssUserType = 3; 
                        try{
                            bssUserType = Integer.parseInt(req.getHeader(WebsiteHeader.BSS_USER_TYPE));
                        }catch (NumberFormatException e) {
                            throw new BaseException(400, "InvalidUserType");
                        }
                      owner.userType = bssUserType;
                      updateType = UpdateType.CHANGEUSERTYPE;
                      break;
                    }
                }
                
                //2. 发送请求到bss中心广播修改其它资源池用户       且中心bssAdapterserver广播的时候会排除申请广播的资源池
                if (!BssAdapterClient.modifyUser(BssAdapterConfig.localPool.ak, BssAdapterConfig.localPool.getSK(),
                        owner, SourceTYPE.valueOf(source), updateType,new String[]{BssAdapterConfig.localPool.name})){
                    throw new BaseException(500, "UpdateUserFail");
                }
                //3.以下逻辑是非中心池申请中心池广播后，若没有抛错则修改自身资源池的数据库。
                if (!BssAdapterConfig.localPool.name.equals(BssAdapterConfig.name)) {
                  //只有中心池localPool.name=BssAdapterConfig.name
                    OwnerMeta ownerDB = new OwnerMeta(owner.name);

                    if (!client.ownerSelect(ownerDB)) {
                        throw new BaseException(404, "NoSuchUser");
                    }
                    switch (updateType) {
                    case PASSWORD:
                        ownerDB.setPwd(owner.getPwd());
                        client.ownerUpdate(ownerDB);
                        break;
                    case FREEZE:
                        ownerDB.frozenDate = owner.frozenDate;
                        client.ownerUpdate(ownerDB);
                        break;
                    case UNFREEZE:
                        ownerDB.frozenDate = "";
                        client.ownerUpdate(ownerDB);
                        break;
                    case ACTIVE:
                        ownerDB.verify = null;
                        client.ownerDeleteColumn(ownerDB, "verify");
                        break;
                    case COMMENT:
                        ownerDB.comment = owner.comment;
                        client.ownerUpdate(ownerDB);
                        break;
                    case CHANGEUSERTYPE:
                        ownerDB.userType = owner.userType;
                        client.ownerUpdate(ownerDB);
                        break;
                    case DELETE:
                        client.ownerDelete(ownerDB);

                        //Delete global user existence meta
                        BucketOwnerConsistencyMeta meta = new BucketOwnerConsistencyMeta(ownerDB.name,
                                BucketOwnerConsistencyType.OWNER, BssAdapterConfig.name);
                        client.bucketOwnerConsistencyDelete(meta);
                        break;
                    }
                }
            }
        } else if (req.getMethod().equals(HttpMethod.POST.toString())) {
            String operation = req.getParameter("operation");
            String ownerName = req.getParameter("ownerName");
            if (req.getHeader("x-ctyun-batchUserType") != null && 
                    req.getInputStream() != null) {
                BufferedReader br = null;
                String line;
                try {
                   br = new BufferedReader(new InputStreamReader(
                            req.getInputStream()));
                   while((line = br.readLine()) != null){
                       if (line.trim().length() == 0) continue;
                    
                       try {
                           String[] fields = line.split("\t");
                           
                           if  (fields.length>2) continue;
                        
                           String userName = fields[0];
                           int type = Integer.parseInt(fields[1]);
                           
                           OwnerMeta meta = new OwnerMeta(userName);
                           if (client.ownerSelect(meta) && meta.userType != type) {
                               meta.userType = type;
                               client.ownerUpdate(meta);
                           }
                       } catch (Exception e1) {
                           log.error("line format error:"+line);
                           continue;
                       }
                   }  
                }finally {
                    try {
                        br.close();
                    } catch (IOException e) {}
                }
            } else if ("terminateAllPackage".equals(operation)) {
                Common.checkParameter(ownerName);
                OwnerMeta owner = new OwnerMeta(ownerName);
                if (!client.ownerSelect(owner))
                    throw new BaseException(404, "NoSuchUser");
                DbUserPackage dbUserPackage = new DbUserPackage(owner.getId());
                DBPrice.getInstance().userPackageCloseUserAll(dbUserPackage);
            }
        }
    }
    
    private static void handleAccessKey(HttpServletRequest req, HttpServletResponse resp)
            throws Exception {
        if (req.getMethod().equals(HttpMethod.PUT.toString())) {
            String ownerName = req.getParameter("ownerName");
            if (req.getParameter("maxAKNum") == null)
                throw new BaseException();
            int maxAKNum;
            try {
                maxAKNum = Integer.parseInt(req.getParameter("maxAKNum"));
            } catch (Exception e) {
                log.error(e.getMessage(), e);
                throw new BaseException();
            }
            Common.checkParameter(ownerName);
            OwnerMeta owner = new OwnerMeta(ownerName);
            if (!client.ownerSelect(owner))
                throw new BaseException(404, "NoSuchUser");
            owner.maxAKNum = maxAKNum;
            client.ownerUpdate(owner);
        }
    }
    
    private static void handleBucketWebsite(HttpServletRequest req, HttpServletResponse resp)
            throws Exception {
        if (req.getMethod().equals(HttpMethod.PUT.toString())) {
            String bucketName = req.getParameter("bucketName");
            Common.checkParameter(bucketName);
            BucketMeta bucket = new BucketMeta(bucketName);
            if (!client.bucketSelect(bucket))
                throw new BaseException(404, ErrorMessage.ERROR_CODE_NO_SUCH_BUCKET);
            DB db = DB.getInstance();
            DbBucketWebsite bucketWebsite = new DbBucketWebsite(bucket.getId());
            if (db.bucketWebsiteSelect(bucketWebsite))
                throw new BaseException(400, "BucketAlreadyInWebsite");
            db.bucketWebsiteInsert(bucketWebsite);
        }
    }

    private static void handleNoninternetIP(HttpServletRequest req,
            HttpServletResponse resp) throws Exception {
        // 增加非互联网IP
        if (req.getMethod().equals(HttpMethod.POST.toString())) {
            String ipSegment = req.getParameter("ipSegment");
            Common.checkParameter(ipSegment);
            Common.checkIPParameter(ipSegment);          
            NoNetIPMeta meta = new NoNetIPMeta(ipSegment);
            if (client.noNetIPSelect(meta))
                throw new BaseException(400, "IPSegmentAlreadyExist");
            checkNoNetIPRangeOverlap(ipSegment);
            client.noNetIPInsert(meta);
            return;
        }
        // 修改非互联网IP
        if (req.getMethod().equals(HttpMethod.PUT.toString())) {
            String oldIpSegment = req.getParameter("oldIpSegment");
            String newIpSegment = req.getParameter("newIpSegment");
            Common.checkParameter(oldIpSegment);
            Common.checkParameter(newIpSegment);
            Common.checkIPParameter(oldIpSegment);
            Common.checkIPParameter(newIpSegment);
            checkNoNetIPRangeOverlap2(newIpSegment,oldIpSegment);
            NoNetIPMeta oldMeta = new NoNetIPMeta(oldIpSegment);
            NoNetIPMeta newMeta = new NoNetIPMeta(newIpSegment);
            if (!client.noNetIPSelect(oldMeta))
                throw new BaseException(404, "NoSuchIPSegment");
            client.noNetIPUpdate(oldMeta, newMeta);
            return;
        }
        // 删除一条非互联网IP
        if (req.getMethod().equals(HttpMethod.DELETE.toString())) {
            String ipSegment = req.getParameter("ipSegment");
            Common.checkParameter(ipSegment);
            Common.checkIPParameter(ipSegment);
            NoNetIPMeta meta = new NoNetIPMeta(ipSegment);
            if (!client.noNetIPSelect(meta))
                throw new BaseException(404, "NoSuchIPSegment");
            try {
                client.noNetIPDelete(meta);
                log.info(
                        "Delete non-Internet IP segment success, the ipSegment is:"
                                + ipSegment);
            } catch (Exception e) {
                log.error(e.getMessage(), e);
                throw new BaseException(500, "DeleteNoNetIPError");
            }
            return;
        }
        if (req.getMethod().equals(HttpMethod.GET.toString())) {
            Common.writeResponseEntity(resp, getNoNetIPs());
            return;
        }
    }

    // 检查非互联网IP段是否已经被数据库中已有的IP段范围完全包含了
    public static void checkNoNetIPRangeOverlap(String ipSegment) throws BaseException, IOException {
        if (ipSegment.isEmpty()) {
            return;
        }
        List<String> keys = client.noNetIPSegmentList();
        if (keys.isEmpty()) {
            return;
        }     
        checkRequestIPIsContainedAtDB(ipSegment, keys);
        return;
    }
    
    // 检查非互联网IP段是否已经被数据库中已有的IP段(除了待修改的IP段)范围完全包含了
    public static void checkNoNetIPRangeOverlap2(String requestIp,String oldIp) throws BaseException, IOException {
        if (requestIp.isEmpty() || oldIp.isEmpty()) {
            return;
        }       
        List<String> keys = client.noNetIPSegmentList();
        if (keys.isEmpty()) {
            return;
        }
        keys.remove(oldIp);
        checkRequestIPIsContainedAtDB(requestIp, keys);
        return;
    }
    
    /**
     * 判断请求ip(单个ip或ip cidr)是否被数据库中的ip(单个ip或ip cidr)范围已经覆盖
     * @param requestIp
     * @param keys
     * @throws BaseException
     */
    private static void checkRequestIPIsContainedAtDB(String requestIp, List<String> keys) throws BaseException {
        boolean isOverlap = false;
        try {
            for (String key : keys) {
                if (!Common.isValidIPAddr(key) && !Common.isValidCidrAddr(key)
                        && !Utils.isValidIpv6Addr(key) && !Utils.isValidIpv6Cidr(key)) {
                    log.error("noninternet ip at db is invalid. " + key);
                    continue;
                }
                if (requestIp.contains("/")) {
                    // 请求ipSegment为ip段
                    if (!requestIp.contains(":")) {
                        // 请求ipSegment为ipv4 cidr
                        if (!key.contains(":")) {
                            if (key.contains("/")) {
                                // 数据库里也为ipv4 cidr
                                if (check2Ipv4CidrIsOverlap(requestIp, key)) {
                                    isOverlap = true;
                                    break;
                                }
                            } else {
                                // 数据库里为ipv4
                                if (requestIp.endsWith("/32")) {
                                    if (requestIp.substring(0, requestIp.length() - 3).equals(key)) {
                                        isOverlap = true;
                                        break;
                                    }
                                }
                            }
                        }
                    } else {
                        // 请求ipSegment为ipv6 cidr
                        if (key.contains(":")) {
                            if (key.contains("/")) {
                                // 数据库里也为ipv6 cidr
                                if (check2Ipv6CidrIsOverlap(requestIp, key)) {
                                    isOverlap = true;
                                    break;
                                }
                            } else {
                                // 数据库里为ipv6
                                if (requestIp.endsWith("/128")) {
                                    // 请求ip为ipv6 cidr,以/128结尾
                                    String requestIpTmp = requestIp.substring(0, requestIp.length() - 4);
                                    if (Utils.twoIpv6IsSame(requestIpTmp, key)) {
                                        isOverlap = true;
                                        break;
                                    }
                                }
                            }
                        }
                    }
                } else {
                    // 请求ipSegment为单个ip
                    if (Utils.ipExistsInRange(requestIp, key)) {
                        isOverlap = true;
                        break;
                    }
                }
            }
        } catch (Exception e) {
            log.error(e.getMessage());
        } finally {
            if (isOverlap)
                throw new BaseException(400, "IPRangeAlreadyExists");
        }
    }
    
    /**
     * 判断请求ipv4 cidr是否已经被数据库里ipv4 cidr范围覆盖
     * @param requestIp
     * @param dbIp
     * @return
     */
    private static boolean check2Ipv4CidrIsOverlap (String requestIp, String dbIp) {
        boolean flag = false;
        int dbIpNum = Integer.parseInt(dbIp.split("/")[1]);
        int reqIpNum = Integer.parseInt(requestIp.split("/")[1]);
        if ( dbIpNum <= reqIpNum) {
            String dbip2 = ipSegment2BinaryStr(dbIp);
            String reqip2 = ipSegment2BinaryStr(requestIp);
            if (dbip2.substring(0, dbIpNum-1).equals(reqip2.substring(0,dbIpNum-1)))
                flag = true;
        }
        return flag;
    }
    
    /**
     * 判断请求ipv6 cidr是否已经被数据库里ipv6 cidr范围覆盖
     * @param requestIp
     * @param dbIp
     * @return
     * @throws UnknownHostException 
     */
    private static boolean check2Ipv6CidrIsOverlap(String requestIp, String dbIp) throws UnknownHostException {
        Pair<InetAddress, InetAddress> requestIpPair = Utils.calculate(requestIp);
        if (Utils.isInIpv6Range(requestIpPair.first().getHostAddress(), dbIp)
                && Utils.isInIpv6Range(requestIpPair.second().getHostAddress(), dbIp)) {
            return true;
        }
        return false;
    }
    
    private static String ipSegment2BinaryStr(String ip) {
        String result = "";
        String[] items = ip.replaceAll("/.*", "").split("\\.");
        String[] res = new String[4];
        for (int i = 0; i <4; i++) {
            String tmp = Integer.toBinaryString(Integer.valueOf(items[i]));
            int tmp2 = Integer.parseInt(tmp);
            String tmp3 = String.format("%08d",tmp2);
            res[i] =tmp3;
        }
        result = res[0]+res[1]+res[2]+res[3];
        return result;
    }

    private static String getNoNetIPs() throws BaseException {
        List<String> ips = new ArrayList<String>();
        try {
            ips = client.noNetIPSegmentList();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new BaseException(500, "GetNoNetIPError");
        }

        XmlWriter xml = new XmlWriter();
        xml.start("NoninternetIP");
        for (String ip : ips) {
            xml.start("Data").value(ip).end();
        }
        xml.end();
        if (log.isDebugEnabled())
            log.debug(xml.toString());
        return xml.toString();
    }

    /** 活跃用户统计缓存 */
    private static LoadingCache<String, String> activityOwnerCache = CacheBuilder.newBuilder()
            .expireAfterWrite(1, TimeUnit.HOURS)
            .build(
                new CacheLoader<String, String>() {
                    @Override
                    public String load(String key) throws Exception {
                        return getActivityOwners();
                    }
                });
    /**
     * 活跃用户统计
     */
    private static void handleActivity(HttpServletRequest req, HttpServletResponse resp) throws Exception {
        String result = activityOwnerCache.get("");
        log.info(result);
        Common.writeResponseEntity(resp, result);
    }
    
    /**
     * 获取活跃用户统计的响应
     */
    public static String getActivityOwners() throws Exception {
        
        // 保存每个用户的用量，key为usrId
        Map<Long, ActivityOwner> activityOwnerMap = new HashMap<>();
        for (String region : DataRegions.getAllRegions()) {
            // key范围,6.8 rowkey改为region在前
            String startRow = region + "|" + UsageMetaType.DAY_OWNER.toString();
            String stopRow = region + "|" + UsageMetaType.DAY_OWNER.toString() + Character.MAX_VALUE;
            // 时间戳范围
            // 获取一个月前的数据
            long minStamp = DateUtils.addMonths(new Date(), -1).getTime();
            long maxStamp = System.currentTimeMillis();
            // 每次查询获取的数量
            int size = 5000;
            List<MinutesUsageMeta> metas = null;
            do {
                long start = System.currentTimeMillis();
                // 获取指定范围的DAY_OWNER数据
                metas = client.listMinutesUsage(startRow, stopRow, minStamp, maxStamp, size);
                log.info("get minutesUsage DAY_OWNER " + metas.size() + " datas, use time: " + (System.currentTimeMillis() - start) + "ms.");
                MinutesUsageMeta last = null;
                // 累加用量
                for (MinutesUsageMeta usage : metas) {
                    last = usage;
                    // 获取map中保存的活跃用户
                    ActivityOwner activityOwner = activityOwnerMap.get(usage.getUsrId());
                    if (activityOwner == null) {
                        activityOwner = new ActivityOwner();
                        activityOwner.ownerId = usage.getUsrId();
                        activityOwnerMap.put(usage.getUsrId(), activityOwner);
                    } 
                    // 添加用量
                    activityOwner.addUsage(usage);
                }
                // 设置下一个起始的key，用于下一次查询
                if (last != null) {
                    startRow = last.getKey() + Character.MIN_VALUE;
                }
            } while (metas != null && metas.size() == size);
        }       
        
        // 对统计结果进行排序
        List<ActivityOwner> activityOwnerList = new ArrayList<>(activityOwnerMap.values());
        Collections.sort(activityOwnerList);
        // 获取前100的用户
        if (activityOwnerList.size() > 100) {
            activityOwnerList =  activityOwnerList.subList(0, 100);
        }
        JSONObject jos = new JSONObject();
        for (ActivityOwner activityOwner : activityOwnerList) {
            // 获取用户名
            OwnerMeta owner = new OwnerMeta(activityOwner.ownerId);
            client.ownerSelectById(owner);
            JSONObject jo = new JSONObject();
            jo.put("ownerName", owner.getName());
            jo.put("totalSize", activityOwner.getTotalSize());
            jo.put("flow", activityOwner.flow);
            jo.put("ghRequest", activityOwner.ghRequest);
            jo.put("otherRequest", activityOwner.otherRequest);
            jo.put("total", activityOwner.getTotal());
            jos.append("Users", jo);
        }
        return jos.toString();
    }
    
    private static void handleSelectSQL(HttpServletRequest req, HttpServletResponse resp)
            throws BaseException, IOException {
        String sql = req.getParameter("sql");
        Common.checkParameter(sql);
        sql = URLDecoder.decode(sql, Consts.STR_UTF8);
        DBSelect db = DBSelect.getInstance();
        InputStream ip = null;
        OutputStream out = null;
        try {
            Pair<InputStream, Long> p = db.commonSelect(sql);
            ip = p.first();
            out = resp.getOutputStream();
            resp.setHeader("Content-Length", String.valueOf(p.second()));
            resp.setHeader("Content-Type", "text/plain");
            resp.setHeader("Content-Disposition", "attachment;filename=data.csv");
            StreamUtils.copy(ip, out, p.second());
        } catch (SQLException e) {
            log.error(e.getMessage(), e);
            throw new BaseException(400, "Bad Request", e.getMessage());
        } finally {
            if (ip != null) {
                try {
                    ip.close();
                } catch (Throwable e) {
                    log.error(e.getMessage(), e);
                }
            }
            if (out != null) {
                try {
                    out.close();
                } catch (Throwable e) {
                    log.error(e.getMessage(), e);
                }
            }
        }
    }
    
    private static void handleSalesManager(HttpServletRequest req, HttpServletResponse resp)
            throws BaseException, SQLException, IOException {
        if (req.getMethod().equals(HttpMethod.PUT.toString())) {
            String organizationId = req.getParameter("organizationId");
            String name = req.getParameter("name");
            String email = req.getParameter("email");
            String phone = req.getParameter("phone");
            String id = req.getParameter("id");
            String bestPayAccount = req.getParameter("bestPayAccount");
            Common.checkParameter(organizationId, MConsts.ORGANIZATION_ID_LENGTH);
            Common.checkParameter(name, MConsts.SALESMAN_NAME_LENGTH);
            Common.checkParameter(email, MConsts.SALESMAN_EMAIL_LENGTH);
            Common.checkParameter(phone, MConsts.SALESMAN_PHONE_LENGTH);
            Common.checkParameter(id, MConsts.SALESMAN_ID_LENGTH);
            Common.checkParameter(bestPayAccount, MConsts.SALESMAN_BESTPAYACCOUNT_LENGTH);
            boolean isManager = Boolean.parseBoolean(req.getParameter("isManager"));
            DbSalesman dbSalesman = new DbSalesman(email);
            dbSalesman.organizationId = organizationId;
            dbSalesman.name = name;
            dbSalesman.id = id;
            dbSalesman.phone = phone;
            dbSalesman.isManager = isManager ? 1 : 0;
            registerSalesManager(dbSalesman, true);
        }
    }
    
    private static void handleOrganization(HttpServletRequest req, HttpServletResponse resp)
            throws BaseException, SQLException, IOException {
        if (req.getMethod().equals(HttpMethod.PUT.toString())) {
            String organizationId = req.getParameter("organizationId");
            String organizationName = req.getParameter("organizationName");
            Common.checkParameter(organizationId);
            Common.checkParameter(organizationName);
            DbOrganization dbOrganization = new DbOrganization(organizationId);
            dbOrganization.name = organizationName;
            registerOrganization(dbOrganization);
            return;
        }
        if (req.getMethod().equals(HttpMethod.GET.toString())) {
            Common.writeResponseEntity(resp, getOrganization());
            return;
        }
        if (req.getMethod().equals(HttpMethod.DELETE.toString())) {
            String organizationId = req.getParameter("organizationId");
            Common.checkParameter(organizationId);
            deleteOrganization(organizationId);
            return;
        }
    }
    
    public static void deleteOrganization(String organizationId) throws BaseException, SQLException {
        DBPrice dbPrice = DBPrice.getInstance();
        DbOrganization dbOrganization = new DbOrganization(organizationId);
        if (!dbPrice.organizationSelect(dbOrganization))
            throw new BaseException(400, "NoSuchOrganization");
        dbPrice.organizationDelete(dbOrganization);
    }
    
    public static void handlePackage(HttpServletRequest req, HttpServletResponse resp)
            throws Exception {
        if (req.getMethod().equals(HttpMethod.PUT.toString())) {
            if (req.getParameter("orderId") != null) {
                closePackage(req.getParameter("userName"), req.getParameter("orderId"));
            } else if (req.getParameter("userName") != null) {
                orderPackage(req.getParameter("userName"), req.getParameter("packageId"),
                        req.getParameter("startTime"));
            } else {
                InputStream is = null;
                try {
                    is = Common.logInputStream(req.getInputStream());
                    DbPackage dbPackage = new XmlResponseSaxParser().parsePackage(is).getPackage();
                    if (dbPackage.isVisible == 0 && dbPackage.isValid == 1)
                        dbPackage.isValid = 2;
                    if (dbPackage.packageId == 0)
                        addPackage(dbPackage);
                    else if (modifyPackage(dbPackage))
                        Common.writeResponseEntity(resp, "UserUsed");
                } catch (AmazonClientException e) {
                    log.error(e.getMessage(), e);
                    throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_ARGUMENT);
                } finally {
                    if (is != null)
                        is.close();
                }
            }
            return;
        }
        if (req.getMethod().equals(HttpMethod.DELETE.toString())) {
            deletePackage(req.getParameter("packageId"));
            return;
        }
        if (req.getMethod().equals(HttpMethod.GET.toString())
                && req.getParameter("userName") == null) {
            Common.writeResponseEntity(resp, getPackage());
            return;
        }
        if (req.getMethod().equals(HttpMethod.GET.toString())
                && req.getParameter("userName") != null) {
            OwnerMeta owner = new OwnerMeta(req.getParameter("userName"));
            if (!client.ownerSelect(owner))
                throw new BaseException(400, "NoSuchUser");
            Common.writeResponseEntity(resp, getPackage(owner.getId()));
            return;
        }
    }
    
    private static String getPackage(long ownerId) throws SQLException {
        DBPrice dbPrice = DBPrice.getInstance();
        DbUserPackage up = new DbUserPackage(ownerId);
        dbPrice.userPackageSelectByOwnerId(up);
        XmlWriter xml = new XmlWriter();
        xml.start("Packages");
        for (DbUserPackage p : up.ups) {
            DbPackage dbPackage = new DbPackage(p.packageId);
            dbPrice.packageSelect(dbPackage);
            xml.start("Package");
            xml.start("OrderId").value(String.valueOf(p.orderId)).end();
            xml.start("ID").value(String.valueOf(p.packageId)).end();
            xml.start("Name").value(dbPackage.name).end();
            xml.start("CostPrice").value(String.valueOf(dbPackage.costPrice)).end();
            xml.start("PackagePrice").value(String.valueOf(dbPackage.packagePrice)).end();
            xml.start("PackageDiscount").value(String.valueOf(p.packageDiscount)).end();
            xml.start("PackageStart").value(p.packageStart).end();
            xml.start("PackageEnd").value(p.packageEnd).end();
            xml.start("IsPaid").value(p.isPaid == 1 ? "Paid" : "NotPaid").end();
            xml.start("IsClose").value(p.isClose == 1 ? "Close" : "Open").end();
            xml.start("OrderId").value(p.orderId).end();
            xml.end();
        }
        xml.end();
        return xml.toString();
    }
    
    private static String getPackage() throws SQLException {
        DBPrice dbPrice = DBPrice.getInstance();
        DbPackage dbPackage = new DbPackage();
        dbPrice.packageSelectAll(dbPackage, true);
        XmlWriter xml = new XmlWriter();
        xml.start("Packages");
        for (DbPackage p : dbPackage.packages) {
            xml.start("Package");
            xml.start("ID").value(String.valueOf(p.packageId)).end();
            xml.start("Name").value(p.name).end();
            xml.start("Storage").value(String.valueOf(p.storage)).end();
            xml.start("NetFlow").value(String.valueOf(p.flow)).end();
            xml.start("NoNetFlow").value(String.valueOf(p.noNetFlow)).end();
            if (p.ghRequest == Long.MAX_VALUE)
                xml.start("GHRequest").value(String.valueOf("不限")).end();
            else
                xml.start("GHRequest").value(String.valueOf(p.ghRequest)).end();
            if (p.otherRequest == Long.MAX_VALUE)
                xml.start("OtherRequest").value(String.valueOf("不限")).end();
            else
                xml.start("OtherRequest").value(String.valueOf(p.otherRequest)).end();            
            if (p.noNetGHReq == Long.MAX_VALUE)
                xml.start("NoNetGHReq").value(String.valueOf("不限")).end();
            else
                xml.start("NoNetGHReq").value(String.valueOf(p.noNetGHReq)).end();
            if (p.noNetOtherReq == Long.MAX_VALUE)
                xml.start("NoNetOtherReq").value(String.valueOf("不限")).end();
            else
                xml.start("NoNetOtherReq").value(String.valueOf(p.noNetOtherReq)).end();
            xml.start("NetRoamFlow").value(String.valueOf(p.roamFlow)).end();
            xml.start("NetRoamUpload").value(String.valueOf(p.roamUpload)).end();
            xml.start("NoNetRoamFlow").value(String.valueOf(p.noNetRoamFlow)).end();
            xml.start("NoNetRoamUpload").value(String.valueOf(p.noNetRoamUpload)).end();
            if (p.spamRequest == Long.MAX_VALUE)
                xml.start("SpamRequest").value(String.valueOf("不限")).end();
            else
                xml.start("SpamRequest").value(String.valueOf(p.spamRequest)).end();
            if (p.porn == Long.MAX_VALUE)
                xml.start("Porn").value(String.valueOf("不限")).end();
            else
                xml.start("Porn").value(String.valueOf(p.porn)).end();
            xml.start("Duration").value(String.valueOf(p.duration)).end();
            xml.start("CostPrice").value(String.valueOf(p.costPrice)).end();
            xml.start("PackagePrice").value(String.valueOf(p.packagePrice)).end();
            xml.start("Discount").value(String.valueOf(p.discount)).end();
            if (p.isValid != 2) {
                xml.start("IsValid").value(String.valueOf(p.isValid)).end();
                xml.start("IsVisible").value(String.valueOf(p.isValid)).end();
            } else {
                xml.start("IsValid").value("1").end();
                xml.start("IsVisible").value("0").end();
            }
            xml.end();
        }
        xml.end();
        return xml.toString();
    }
    
    private static void deletePackage(String packageId) throws BaseException, SQLException {
        Common.checkParameter(packageId);
        DBPrice dbPrice = DBPrice.getInstance();
        DbPackage dbPackage = new DbPackage();
        dbPackage.packageId = Integer.parseInt(packageId);
        DbUserPackage dup = new DbUserPackage();
        dup.packageId = dbPackage.packageId;
        dbPrice.userPackageSelectByPackageId(dup);
        if (dup.ups.size() > 0)
            throw new BaseException(403, "UserUsed");
        dbPrice.packageDelete(dbPackage);
        log.info("Admin delete package success, the package id is:" + packageId);
    }

    private static void checkPackageParameter(DbPackage dbPackage, boolean isAddPackage)
            throws BaseException {
        if (!isAddPackage)
            Common.checkParameter(dbPackage.packageId);
        Common.checkParameter(dbPackage.name);
        Common.checkParameter(dbPackage.storage);
        Common.checkParameter(dbPackage.flow);
        Common.checkParameter(dbPackage.ghRequest);
        Common.checkParameter(dbPackage.otherRequest);
        Common.checkParameter(dbPackage.duration);
//        Common.checkParameter(dbPackage.triggerStorage);
//        Common.checkParameter(dbPackage.triggerOtherRequest);
//        Common.checkParameter(dbPackage.roamFlow);
//        Common.checkParameter(dbPackage.roamUpload);
//        Common.checkParameter(dbPackage.noNetGHReq);
//        Common.checkParameter(dbPackage.noNetOtherReq);
//        Common.checkParameter(dbPackage.noNetFlow);
//        Common.checkParameter(dbPackage.noNetRoamFlow);
//        Common.checkParameter(dbPackage.noNetRoamUpload);
    }
    private static boolean modifyPackage(DbPackage dbPackage) throws BaseException, SQLException {
        checkPackageParameter(dbPackage, false);
        DBPrice dbPrice = DBPrice.getInstance();
        DbPackage t = new DbPackage();
        t.packageId = dbPackage.packageId;
        if (!dbPrice.packageSelect(t))
            throw new BaseException(400, "NoSuchPackage");
        dbPrice.packageUpdate(dbPackage);
        log.info("Admin modify package success, the package id is:" + dbPackage.packageId);
        DbUserPackage dup = new DbUserPackage();
        dup.packageId = dbPackage.packageId;
        dbPrice.userPackageSelectByPackageId(dup);
        return dup.ups.size() > 0;
    }
    
    private static void orderPackage(String userName, String packageId, String startTime)
            throws Exception {
        Common.checkParameter(userName);
        Common.checkParameter(packageId);
        Common.checkParameter(startTime);
        OwnerMeta owner = new OwnerMeta(userName);
        if (!client.ownerSelect(owner))
            throw new BaseException(400, "NoSuchUser");
        DbPackage dbPackage = new DbPackage(Integer.parseInt(packageId));
        DBPrice dbPrice = DBPrice.getInstance();
        if (!dbPrice.packageSelect(dbPackage))
            throw new BaseException(400, "NoSuchPackage");
        if (dbPackage.isValid == 0)
            throw new BaseException(400, "PackageNotValid");
        DbUserPackage dup = new DbUserPackage(owner.getId());
        dup.isPaid = 1;
        dup.date = startTime;
        if (dbPrice.userPackageSelect(dup))
            throw new BaseException(400, "PackageAlreadyExists");
        String endDate = TimeUtils.toYYYY_MM_dd(DateUtils.addMonths(
                DateParser.parseDate(startTime), (int) dbPackage.duration));
        dup.date = endDate;
        if (dbPrice.userPackageSelect(dup))
            throw new BaseException(400, "PackageAlreadyExists");
        dup.startDate = startTime;
        dup.endDate = endDate;
        if (dbPrice.userPackageSelectInRange(dup))
            throw new BaseException(400, "PackageAlreadyExists");
        dup.packageId = dbPackage.packageId;
        dup.orderId = String.valueOf(owner.getId()) + System.nanoTime();
        dup.packageDiscount = dbPackage.discount;
        dup.packageStart = startTime;
        dup.packageEnd = TimeUtils.toYYYY_MM_dd(DateUtils.addMonths(
                DateParser.parseDate(startTime), (int) dbPackage.duration));
        dup.storage = dbPackage.storage;
        dup.flow = dbPackage.flow;
        dup.ghRequest = dbPackage.ghRequest;
        dup.otherRequest = dbPackage.otherRequest;
        dup.roamFlow = dbPackage.roamFlow;
        dup.roamUpload = dbPackage.roamUpload;
        dup.noNetRoamFlow = dbPackage.noNetRoamFlow;
        dup.noNetRoamUpload = dbPackage.noNetRoamUpload;
        dup.noNetFlow = dbPackage.noNetFlow;
        dup.noNetGHReq = dbPackage.noNetGHReq;
        dup.noNetOtherReq = dbPackage.noNetOtherReq;
        dup.spamRequest = dbPackage.spamRequest;
        dup.porn = dbPackage.porn;
        dup.isPaid = 1;
        dup.salerId = "admin";
        dbPrice.userPackageInsert(dup);
        log.info("Admin order package success, the package id is:" + dbPackage.packageId
                + " and the userName is:" + userName);
    }
    
    public static void closePackage(String userName, String orderId) throws Exception {
        Common.checkParameter(userName);
        Common.checkParameter(orderId);
        OwnerMeta owner = new OwnerMeta(userName);
        if (!client.ownerSelect(owner))
            throw new BaseException(400, "NoSuchUser");
        DBPrice dbPrice = DBPrice.getInstance();
        DbUserPackage dbUserPackage = new DbUserPackage(orderId);
        if (!dbPrice.userPackageSelectByOrderId(dbUserPackage))
            throw new BaseException(400, "PackageNotExists");
        if (dbUserPackage.usrId != owner.getId())
            throw new BaseException(400, "PackageNotExists");
        if (dbUserPackage.isClose == 1)
            throw new BaseException(400, "PackageAlreadyClosed");
        dbUserPackage.isClose = 1;
        try {
            dbPrice.userPackageUpdate(dbUserPackage);
        } catch (SQLException e) {
            log.error(e.getMessage(), e);
            throw new BaseException(500, "CloseUserPackageError");
        }
    }
    
    public static void handleTieredPrice(HttpServletRequest req, HttpServletResponse resp)
            throws AmazonClientException, SQLException, BaseException, IOException {
        if (req.getMethod().equals(HttpMethod.PUT.toString())) {
            InputStream is = null;
            try {
                is = Common.logInputStream(req.getInputStream());
                DbTieredPrice dbtp = new XmlResponseSaxParser().parseTieredPrice(is)
                        .getTieredPrice();
                if (dbtp.id == 0)
                    addTieredPrice(dbtp, true);
                else
                    addTieredPrice(dbtp, false);
            } catch (AmazonClientException e) {
                log.error(e.getMessage(), e);
                throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_ARGUMENT);
            } finally {
                if (is != null)
                    is.close();
            }
            return;
        }
        if (req.getMethod().equals(HttpMethod.DELETE.toString())) {
            deleteTieredPrice(req.getParameter("tieredPriceId"));
            return;
        }
        if (req.getMethod().equals(HttpMethod.GET.toString())) {
            Common.writeResponseEntity(resp, getTieredPrice());
            return;
        }
    }

    private static String getTieredPrice() throws SQLException {
        DBPrice dbPrice = DBPrice.getInstance();
        DbTieredPrice dtp = new DbTieredPrice();
        dbPrice.tieredPriceSelectAll(dtp);
        XmlWriter xml = new XmlWriter();
        xml.start("TieredPrices");
        for (DbTieredPrice p : dtp.tps) {
            xml.start("TieredPrice");
            if (p.id <= DbTieredPrice.MAX_STORAGE_ID)
                xml.start("Type").value("storage").end();
            else if (p.id <= DbTieredPrice.MAX_FLOW_ID)
                xml.start("Type").value("flow").end();
            else if (p.id <= DbTieredPrice.MAX_GH_REQUEST_ID)
                xml.start("Type").value("ghRequest").end();
            else if (p.id <= DbTieredPrice.MAX_OTHER_REQUEST_ID)
                xml.start("Type").value("otherRequest").end();
            else if (p.id <= DbTieredPrice.MAX_ROAM_UPLOAD_ID)
                xml.start("Type").value("roamUpload").end();
            else if (p.id <= DbTieredPrice.MAX_ROAM_FLOW_ID)
                xml.start("Type").value("roamFlow").end();
            else if (p.id <= DbTieredPrice.MAX_NONET_GH_REQ_ID)
                xml.start("Type").value("noNetGHReq").end();
            else if (p.id <= DbTieredPrice.MAX_NONET_OTHER_REQ_ID)
                xml.start("Type").value("noNetOtherReq").end();
            else if (p.id <= DbTieredPrice.MAX_NONET_FLOW_ID)
                xml.start("Type").value("noNetFlow").end();
            else if (p.id <= DbTieredPrice.MAX_NONET_ROAM_FLOW_ID)
                xml.start("Type").value("noNetRoamFlow").end();
            else if (p.id <= DbTieredPrice.MAX_NONET_ROAM_UPLOAD_ID)
                xml.start("Type").value("noNetRoamUpload").end();
            else if (p.id <= DbTieredPrice.MAX_SPAM_REQUEST_ID)
                xml.start("Type").value("spamRequest").end();
            else if (p.id <= DbTieredPrice.MAX_PORN_REVIEW_FALSE_ID)
                xml.start("Type").value("pornReviewFalse").end();
            else if (p.id <= DbTieredPrice.MAX_PORN_REVIEW_TRUE_ID)
                xml.start("Type").value("pornReviewTrue").end();
            xml.start("PriceId").value(String.valueOf(p.id)).end();
            xml.start("Low").value(String.valueOf(p.low)).end();
            xml.start("Up").value(String.valueOf(p.up)).end();
            xml.start("Price").value(String.valueOf(p.price)).end();
            xml.start("Discount").value(String.valueOf(p.discount)).end();
            xml.end();
        }
        xml.end();
        return xml.toString();
    }

    private static void deleteTieredPrice(String tieredPriceId) throws BaseException, SQLException {
        Common.checkParameter(tieredPriceId);
        DBPrice dbPrice = DBPrice.getInstance();
        DbTieredPrice dtp = new DbTieredPrice();
        dtp.id = Integer.parseInt(tieredPriceId);
        dbPrice.tieredPriceDelete(dtp);
        log.info("Admin delete tiered price success, the tieredPriceId is:" + tieredPriceId);
    }
    
    private static void checkTieredPriceRange(int id, boolean isInsert, int start, int end,
            double value) throws SQLException, BaseException {
        DbTieredPrice tieredPrice = new DbTieredPrice();
        DBPrice dbPrice = DBPrice.getInstance();
        tieredPrice.idStart = start;
        tieredPrice.idEnd = end;
        tieredPrice.value = value;
        if (dbPrice.tieredPriceSelectByRange(tieredPrice)) {
            if (isInsert)
                throw new BaseException(400, "RangeAlreadyExists");
            else if (id != tieredPrice.id) {
                throw new BaseException(400, "RangeAlreadyExists");
            }
        }
    }

    private static void addTieredPrice(DbTieredPrice tieredPrice, boolean isInsert, int end)
            throws SQLException, BaseException {
        checkTieredPriceRange(tieredPrice.id, isInsert, end - 10, end, tieredPrice.low + 1);
        checkTieredPriceRange(tieredPrice.id, isInsert, end - 10, end, tieredPrice.up);
        if (isInsert)
            tieredPrice.id = getTieredPriceId(end - 10 + 1, end);
    }

    private static void addTieredPrice(DbTieredPrice tieredPrice, boolean isInsert)
            throws BaseException, SQLException {
        DBPrice dbPrice = DBPrice.getInstance();
        if (!isInsert) {
            DbTieredPrice t = new DbTieredPrice();
            t.id = tieredPrice.id;
            if (!dbPrice.tieredPriceSelect(t))
                throw new BaseException(400, "NoSuchTieredPrice");
        }
        Common.checkParameter(tieredPrice.type);
        switch (tieredPrice.type) {
        case "storage":
            addTieredPrice(tieredPrice, isInsert, DbTieredPrice.MAX_STORAGE_ID);
            break;
        case "flow":
            addTieredPrice(tieredPrice, isInsert, DbTieredPrice.MAX_FLOW_ID);
            break;
        case "ghRequest":
            addTieredPrice(tieredPrice, isInsert, DbTieredPrice.MAX_GH_REQUEST_ID);
            break;
        case "otherRequest":
            addTieredPrice(tieredPrice, isInsert, DbTieredPrice.MAX_OTHER_REQUEST_ID);
            break;
        case "roamUpload":
            addTieredPrice(tieredPrice, isInsert, DbTieredPrice.MAX_ROAM_UPLOAD_ID);
            break;
        case "roamFlow":
            addTieredPrice(tieredPrice, isInsert, DbTieredPrice.MAX_ROAM_FLOW_ID);
            break;        
        case "noNetGHReq":
            addTieredPrice(tieredPrice, isInsert, DbTieredPrice.MAX_NONET_GH_REQ_ID);
            break;
        case "noNetOtherReq":
            addTieredPrice(tieredPrice, isInsert, DbTieredPrice.MAX_NONET_OTHER_REQ_ID);
            break;
        case "noNetFlow":
            addTieredPrice(tieredPrice, isInsert, DbTieredPrice.MAX_NONET_FLOW_ID);
            break;
        case "noNetRoamFlow":
            addTieredPrice(tieredPrice, isInsert, DbTieredPrice.MAX_NONET_ROAM_FLOW_ID);
            break;
        case "noNetRoamUpload":
            addTieredPrice(tieredPrice, isInsert, DbTieredPrice.MAX_NONET_ROAM_UPLOAD_ID);
            break;
        case "spamRequest":
            addTieredPrice(tieredPrice, isInsert, DbTieredPrice.MAX_SPAM_REQUEST_ID);
            break;
        case "pornReviewFalse":
            addTieredPrice(tieredPrice, isInsert, DbTieredPrice.MAX_PORN_REVIEW_FALSE_ID);
            break;
        case "pornReviewTrue":
            addTieredPrice(tieredPrice, isInsert, DbTieredPrice.MAX_PORN_REVIEW_TRUE_ID);
            break;
        }
        if (isInsert)
            dbPrice.tieredPriceInsert(tieredPrice);
        else
            dbPrice.tieredPriceUpdate(tieredPrice);
        log.info(
                "Admin add or modify tiered price success, the tieredPriceId is:" + tieredPrice.id);
    }   

    private static int getTieredPriceId(int min, int max) throws SQLException, BaseException {
        DBPrice dbPrice = DBPrice.getInstance();
        DbTieredPrice dtp = new DbTieredPrice();
        dbPrice.tieredPriceSelectAll(dtp);
        int i = min;
        for (; i <= max; i++) {
            dtp.id = i;
            if (!dbPrice.tieredPriceSelect(dtp))
                break;
        }
        if (i == max + 1)
            throw new BaseException(400, "TooManyTieredPrice");
        return dtp.id;
    }

    public static void handleBssAdapterUserModify(HttpServletRequest req) 
            throws BaseException, Exception {
        String source = req.getParameter(Parameters.REGISTER_SOURCE_TYPE);
        String ownerName = req.getParameter("ownerName");
        String comment = req.getParameter("comment");
        String userType = req.getParameter("userType");
        String operation = req.getParameter("operation");
        
        if (source!=null && SourceTYPE.valueOf(source) == SourceTYPE.ADAPTERSERVER) {
            OwnerMeta owner = new OwnerMeta(ownerName);
            if (!client.ownerSelect(owner))
                throw new BaseException(404, "NoSuchUser");
            
            if (comment != null) {
                if (comment.length() > MConsts.OWNER_COMMENT_MAX_LENGTH)
                    throw new BaseException();
                owner.comment = comment;
                client.ownerUpdate(owner);
            }else if(userType != null) {
                owner.userType = Integer.parseInt(userType);
                client.ownerUpdate(owner);
            } else if (operation != null) {
                switch (operation) {
                case "base":
                    owner.displayName = URLDecoder.decode(req.getHeader(WebsiteHeader.NEW_DISPLAYNAME), "UTF-8");
                    owner.mobilePhone = req.getHeader(WebsiteHeader.NEW_MOBILEPHONE);
                    client.ownerUpdate(owner);
                    break;
                case "active":
                    owner.verify = null;
                    client.ownerDeleteColumn(owner, "verify");
                    break;
                case "frozen":
                    owner.frozenDate = ServiceUtils.formatIso8601Date(DateUtils.addDays(new Date(),
                            -1));
                    client.ownerUpdate(owner);
                    break;
                case "notFrozen":
                    owner.frozenDate = "";
                    client.ownerUpdate(owner);
                    break;
                case "delete":
                    log.info("delete user:" + owner.name);
                    client.ownerDelete(owner);
                    
                    //Delete global user existence meta 
                    BucketOwnerConsistencyMeta meta = new BucketOwnerConsistencyMeta(owner.name,
                            BucketOwnerConsistencyType.OWNER,"bssAdapter");
                    client.bucketOwnerConsistencyDelete(meta);
                    break;
                case "changePWD":
                    String newPwd = req.getHeader(WebsiteHeader.NEW_PASSWORD);
                    owner.setPwd(newPwd);
                    client.ownerUpdate(owner);
                    break;
                }
            }
        }
    }

    /**
     * 查询所有数据域
     * @param resp
     * @throws BaseException
     * @throws IOException
     */
    private static void handleValidDataRegions(HttpServletResponse resp) throws BaseException, IOException {
        List<String> dataRegions = DataRegions.getAllRegions();
        JSONObject jo = new JSONObject();
        JSONArray ja = new JSONArray();
        try {
            for (String r : dataRegions) {
                DataRegions.DataRegionInfo regionInfo = DataRegions.getRegionDataInfo(r);
                JSONObject o = new JSONObject();
                o.put("name", r);
                o.put("chName", regionInfo.getCHName());
                ja.put(o);
            }
            jo.put("dataRegions", ja);
        } catch (JSONException jsonException) {
            throw new BaseException();
        }
        Common.writeJsonResponseEntity(resp, jo.toString());
    }

    private static void handleUserOstorWeight(HttpServletRequest req, HttpServletResponse resp)
            throws BaseException, IOException {
        if (req.getMethod().equalsIgnoreCase(HttpMethod.GET.toString())) {
            // 用户集群权重列表
            listUserOstorWeight(req, resp);
        } else if (req.getMethod().equalsIgnoreCase(HttpMethod.PUT.toString())) {
            // 设置用户集群权重
            putUserOstorWeight(req, resp);
        } else if (req.getMethod().equalsIgnoreCase(HttpMethod.DELETE.toString())) {
            // 删除用户集群权重
            deleteUserOstorWeight(req, resp);
        } else {
            throw new BaseException(400, "BadRequest");
        }
    }

    /**
     * 获取用户集群权重列表
     * @param req
     * @param resp
     * @throws IOException
     * @throws BaseException
     */
    private static void listUserOstorWeight(HttpServletRequest req, HttpServletResponse resp)
            throws IOException, BaseException {
        int pageSize = 1000;
        if (req.getParameter("maxItem") != null) {
            pageSize = Integer.parseInt(req.getParameter("maxItem"));
            pageSize = Math.min(pageSize, 1000);
            pageSize = Math.max(pageSize, 1);
        }
        String marker = req.getParameter("marker");
        String ownerName = StringUtils.defaultString(req.getParameter("ownerName"));
        List<UserOstorWeightMeta> metaList = client.userOstorWeightMetaList(pageSize, ownerName, marker);
        JSONObject jo = new JSONObject();
        try {
            if (metaList.size() >= pageSize) {
                jo.put("IsTruncated", true);
                UserOstorWeightMeta last = metaList.get(metaList.size() - 1);
                jo.put("Marker", last.getRowString());
            } else {
                jo.put("IsTruncated", false);
                jo.put("Marker", "");
            }
            JSONArray ja = new JSONArray();
            for (UserOstorWeightMeta meta : metaList) {
                JSONObject o = new JSONObject();
                o.put("ownerName", meta.getOwnerName());
                o.put("dataRegion", meta.getDataRegion());
                o.put("dataRegionChName", meta.getDataRegionChName());
                o.put("ostorWeight", meta.getOstorWeight());
                ja.put(o);
            }
            jo.put("Data", ja);
        } catch (JSONException e) {
            throw new BaseException();
        }
        Common.writeJsonResponseEntity(resp, jo.toString());
    }

    /**
     * 设置用户集群权重
     * @param req
     * @param resp
     * @throws BaseException
     * @throws IOException
     */
    private static void putUserOstorWeight(HttpServletRequest req, HttpServletResponse resp)
            throws BaseException, IOException {
        Common.checkParameter(req.getParameter("ownerName"));
        Common.checkParameter(req.getParameter("dataRegion"));
        Common.checkParameter(req.getParameter("ostorWeight"));
        UserOstorWeightMeta userOstorWeightMeta = new UserOstorWeightMeta();
        OwnerMeta owner = new OwnerMeta(req.getParameter("ownerName"));
        try {
            if (!client.ownerSelect(owner)) {
                throw new BaseException(400, "NoSuchUser");
            }
        }catch (BaseException be){
            throw be;
        }catch (Exception e) {
            log.error("select owner error", e);
            throw new BaseException(500, "ServerError", "select owner failed.");
        }
        String dataRegion = req.getParameter("dataRegion");
        DataRegions.DataRegionInfo regionInfo = DataRegions.getRegionDataInfo(dataRegion);
        if (regionInfo == null) {
            throw new BaseException(400, "NoSuchDataRegion", "no such data region");
        }
        // 获取用户数据域O
        Set<String> ownerDataRegions = client.getDataRegions(owner.getId());
        if (!ownerDataRegions.contains(dataRegion)) {
            throw new BaseException(400, "InvalidDataRegion", "the dataRegion you set does't belong to the user");
        }
        userOstorWeightMeta.setUp(owner, regionInfo);
        // 读取zk获取数据域下所有ostor集群
        List<String> ostorIds = getRegionOstors(dataRegion);
        if (ostorIds == null || ostorIds.size() == 0) {
            throw new BaseException(400, "NoValidOstor");
        }
        Map<String, Integer> ostorWeightMap = new HashMap<>();
        try {
            long s = 0;
            String js = URLDecoder.decode(req.getParameter("ostorWeight"), CharEncoding.UTF_8);
            JSONObject jo = new JSONObject(js);
            Iterator iterator = jo.keys();
            while (iterator.hasNext()) {
                String key = (String) iterator.next();
                if (!ostorIds.contains(key)) {
                    throw new BaseException(400, "InvalidOstor", "no such ostor: " + key);
                }
                if (jo.getInt(key)<=0) {
                    throw new BaseException(400, "InvalidOstorWeight", "ostor: " + key + " weight must be larger than zero");
                }
                s += jo.getInt(key);
                ostorWeightMap.put(key, jo.getInt(key));
            }
            if (s > Integer.MAX_VALUE) {
                throw new BaseException(400, "InvalidOstorWeight", "ostor weight is too large");
            }
        } catch (JSONException e) {
            throw new BaseException(400, "InvalidJsonFormat", "json format error");
        }
        userOstorWeightMeta.setOstorWeight(ostorWeightMap);
        owner.addUserRegion(dataRegion);
        try {
            client.updateUserOstorWeight(owner, userOstorWeightMeta);
        } catch (Exception e) {
            log.error("save data error.", e);
            throw new BaseException(500, "ServerError", "Please try again later.");
        }
    }

    /**
     * 获取某数据域下所有ostor集群ID
     * @param dataRegion
     * @return
     * @throws BaseException
     */
    private static List<String> getRegionOstors(String dataRegion) throws BaseException {
        try {
            return zkClient.getChildren(OosZKNode.getDataRegionOstors(dataRegion),false);
        } catch (Exception e) {
            log.error("zk error.", e);
            throw new BaseException(500, "ServerError", "zk error");
        }
    }

    /**
     * 删除用户集群权重
     * @param req
     * @param resp
     * @throws BaseException
     * @throws IOException
     */
    private static void deleteUserOstorWeight(HttpServletRequest req, HttpServletResponse resp)
            throws BaseException {
        Common.checkParameter(req.getParameter("ownerName"));
        Common.checkParameter(req.getParameter("dataRegion"));
        String ownerName = String.valueOf(req.getParameter("ownerName"));
        String dataRegion = String.valueOf(req.getParameter("dataRegion"));
        OwnerMeta ownerMeta = new OwnerMeta(ownerName);
        try {
            if (!client.ownerSelect(ownerMeta)) {
                throw new BaseException(400, "NoSuchUser");
            }
            UserOstorWeightMeta ostorWeightMeta = new UserOstorWeightMeta(ownerName, dataRegion);
            ownerMeta.userRegions.remove(dataRegion);
            client.deleteUserOstorWeight(ownerMeta, ostorWeightMeta);
        } catch (Exception e) {
            log.error("error", e);
            throw new BaseException(500, ErrorMessage.ERROR_CODE_500);
        }
    }
}