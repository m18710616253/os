package cn.ctyun.oos.bssAdapter.orderbill.sender;

import java.math.RoundingMode;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.Callable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONObject;

import cn.ctyun.common.Consts;
import cn.ctyun.oos.hbase.MetaClient;
import cn.ctyun.oos.metadata.MinutesUsageMeta;
import cn.ctyun.oos.server.conf.BssAdapterConfig;

public class LoadTask implements Callable<List<OrderMessage>> {
    private int pageIndex;
    private List<String> subRowKeyList;
    private HashMap<String,String> bssUsers;
    private Properties mappingProperty;
    private NumberFormat formater = NumberFormat.getNumberInstance();
    
    private static MetaClient client = MetaClient.getGlobalClient();
    private static final Log log = LogFactory.getLog(LoadTask.class);

    private void InitFormat() {
        formater.setMaximumFractionDigits(4);             //保留四位小数
        formater.setGroupingUsed(false);                  //不使用钱未付
        formater.setRoundingMode(RoundingMode.DOWN);      //不进行四舍五入
    }
    
    public LoadTask(int pageIndex, HashMap<String,String> bssUsers,List<String> subRowKeyList
            ,Properties mappingProperty) {
        this.pageIndex = pageIndex;
        this.subRowKeyList = subRowKeyList;
        this.bssUsers = bssUsers;
        this.mappingProperty = mappingProperty;
        InitFormat();
    }

    @Override
    public List<OrderMessage> call() throws Exception {
        Map<String, MinutesUsageMeta> result = client.minutesUsageBatchSelect(subRowKeyList);

        int count = 0;

        OrderMessage msg = new OrderMessage(new ArrayList<>(), BssAdapterConfig.asynchronizedRetryTimes);

        List<OrderMessage> returnValues = new ArrayList<>();
        for (Entry<String, MinutesUsageMeta> v : result.entrySet()) {
            count++;
            MinutesUsageMeta meta = v.getValue();
            
            //regionName + "|" DAY_OWNER + "|"+ usrId + "|" + time;
            JSONObject order = new JSONObject();
            if(meta.storageType.equals(Consts.STORAGE_CLASS_STANDARD_IA)) {//低频的话单
                order.put("accountId",  bssUsers.get(meta.getUsrId()+"").substring(0,bssUsers.get(meta.getUsrId()+"").indexOf(",")));
                order.put("ownerId",     meta.getUsrId());
                order.put("userId",     bssUsers.get(meta.getUsrId()+"").substring(bssUsers.get(meta.getUsrId()+"").indexOf(",")+1));
                order.put("date",       meta.time);
                order.put("peakSize",   formater.format((double)meta.sizeStats.size_peak/1024/1024/1024)); //G
                order.put("flow",       formater.format((double)meta.flowStats.flow_download/1024/1024/1024)); //G
                order.put("ghRequest", formater.format((
                        (double) ((meta.requestStats.req_get + meta.requestStats.req_head
                        + meta.requestStats.req_noNetGet + meta.requestStats.req_noNetHead)
                        - meta.codeRequestStats.getNetGHRequestNon2xxNum() - meta.codeRequestStats.getNoNetGHRequestNon2xxNum())) / 10000)); // 万次
                order.put("otherRequest",
                        formater.format(((double)(meta.requestStats.req_other + meta.requestStats.req_noNetOther + meta.requestStats.req_put
                                + meta.requestStats.req_noNetPut + meta.requestStats.req_post + meta.requestStats.req_noNetPost
                                + meta.requestStats.req_delete + meta.requestStats.req_noNetDelete
                                - meta.codeRequestStats.getNetOthersRequestNon2xxNum() - meta.codeRequestStats.getNoNetOthersRequestNon2xxNum())) / 10000)); // 万次，包含put、post、delete、other
                order.put("preChangeOfThreeSize", formater.format((double) (meta.sizeStats.size_preDeleteComplete + meta.sizeStats.size_preChangeComplete+meta.sizeStats.size_completePeak)/1024/1024/1024));//低频存储的提前变更量
                order.put("restore", formater.format((double)meta.sizeStats.size_restore/1024/1024/1024));//低频存储的数据取回值
                order.put("userType", meta.storageType);//用户类型，标记是否为低频
                order.put("regionName", mappingProperty.getProperty(meta.regionName, meta.regionName));
            }else {
                order.put("accountId",  bssUsers.get(meta.getUsrId()+"").substring(0,bssUsers.get(meta.getUsrId()+"").indexOf(",")));
                order.put("ownerId",     meta.getUsrId()+"");
                order.put("userId",     bssUsers.get(meta.getUsrId()+"").substring(bssUsers.get(meta.getUsrId()+"").indexOf(",")+1));
                order.put("date",       meta.time);
                order.put("peakSize",   formater.format((double)meta.sizeStats.size_peak/1024/1024/1024)); //G
                order.put("flow",       formater.format((double)meta.flowStats.flow_download/1024/1024/1024)); //G
                order.put("restore", formater.format(0/1024/1024/1024));
                order.put("ghRequest", formater.format((
                        (double) ((meta.requestStats.req_get + meta.requestStats.req_head
                        + meta.requestStats.req_noNetGet + meta.requestStats.req_noNetHead)
                        - meta.codeRequestStats.getNetGHRequestNon2xxNum() - meta.codeRequestStats.getNoNetGHRequestNon2xxNum())) / 10000)); // 万次
                order.put("otherRequest",
                        formater.format(((double)(meta.requestStats.req_other + meta.requestStats.req_noNetOther + meta.requestStats.req_put
                                + meta.requestStats.req_noNetPut + meta.requestStats.req_post + meta.requestStats.req_noNetPost
                                + meta.requestStats.req_delete + meta.requestStats.req_noNetDelete
                                - meta.codeRequestStats.getNetOthersRequestNon2xxNum() - meta.codeRequestStats.getNoNetOthersRequestNon2xxNum())) / 10000)); // 万次，包含put、post、delete、other
                order.put("regionName", mappingProperty.getProperty(meta.regionName, meta.regionName));
            }
            msg.getOrders().add(order);
            
            if ((count > 0 && count%500==0)) {
                returnValues.add(msg);
                msg = new OrderMessage(new ArrayList(),
                        BssAdapterConfig.asynchronizedRetryTimes);
            }
        }
        
        //剩余的数量
        if (msg.getOrders().size()>0) {
            returnValues.add(msg);
        }
        
        log.info("load page:" + this.pageIndex + " into app queue");
        
        return returnValues;
    }
}
