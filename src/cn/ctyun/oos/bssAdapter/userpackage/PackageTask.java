package cn.ctyun.oos.bssAdapter.userpackage;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Callable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONObject;

import cn.ctyun.common.conf.OOSConfig;
import cn.ctyun.oos.common.Utils;
import cn.ctyun.oos.hbase.MetaClient;
import cn.ctyun.oos.metadata.OwnerMeta;
import cn.ctyun.oos.server.db.DB;
import cn.ctyun.oos.server.db.DbBSSUserPackage;
import common.util.MD5Hash;

public class PackageTask implements Callable<Boolean> {
    private static final Log log = LogFactory.getLog(PackageTask.class);
    private static MetaClient hClient = MetaClient.getGlobalClient();
    
    private int pageIndex;
    private UserCustomerFile file;

    public PackageTask(int pageIndex, UserCustomerFile file) {
        this.pageIndex = pageIndex;
        this.file = file;
    }

    /**
     * 获取响应的数据并且上传
     */
    @Override
    public Boolean call() throws Exception {
        DbBSSUserPackage userPackage = new DbBSSUserPackage(this.pageIndex);
        List<JSONObject> packageData = DB.getInstance().userePackageSelect(userPackage);
        
        if (packageData == null || packageData.size()==0) return null;

        StringBuilder sb = new StringBuilder();
        
        for(JSONObject jsonUser:packageData) {
            sb.setLength(0);
            OwnerMeta owner;
            
            try {
                owner = new OwnerMeta(Long.parseLong(jsonUser.getString("ownerId")));
                
                if (!hClient.ownerSelectById(owner)) continue;
                //按商用客户过滤
                if (owner.userType != 1) continue;
                
                Long hashPackageId = MD5Hash.digest(jsonUser.getInt("packageId") 
                        + OOSConfig.getPortalDomain() + owner.name).halfDigest();
                
                sb.append(owner.name).append("|")
                  .append(hashPackageId).append("|")    
                  .append(jsonUser.getLong("storage")).append("|")
                  .append(jsonUser.getLong("flow")).append("|")
                  .append(jsonUser.getLong("ghRequest")).append("|")
                  .append(jsonUser.getLong("otherRequest")).append("|")
                  .append(jsonUser.getLong("usedStorage")).append("|")
                  .append(jsonUser.getLong("usedFlow")).append("|")
                  .append(jsonUser.getLong("usedGhRequest")).append("|")
                  .append(jsonUser.getLong("usedOtherRequest")).append("|")
                  .append(jsonUser.getString("packageStart")).append("|")
                  .append(jsonUser.getString("packageEnd")).append("|");
                
                //套餐是否正常，0表示正常，1表示套餐不正常
                if (Utils.isFrozen(owner) ||   //冻结状态
                    jsonUser.getInt("isClose") == 1 ||  //套餐终止状态 
                    !checkDate(jsonUser.getString("packageStart"),
                            jsonUser.getString("packageEnd"),new Date()) //过期状态
                    ) {
                    sb.append("1").append("|"); 
                } else sb.append("0").append("|");
                sb.append(OOSConfig.getPortalDomain()).append("|");
                sb.append(owner.userType).append(System.lineSeparator());
                
                file.addMsg(sb.toString());                
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }
        
        return true;
    }

    private static boolean checkDate(String begin, String end,Date current) throws ParseException {
        if (ConcurrentDateUtil.parse(begin).after(current) || 
                ConcurrentDateUtil.parse(end).before(current) ){
            return false;
        }

        return true;
    }

}
