package cn.ctyun.oos.server.log.logstats;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import cn.ctyun.common.conf.DataRegion;
import cn.ctyun.common.conf.OOSConfig;
import cn.ctyun.common.ganglia.GangliaConsts;
import cn.ctyun.common.ganglia.GangliaMan;
import cn.ctyun.common.ganglia.GangliaPlugin;
import cn.ctyun.common.utils.HostUtils;
import cn.ctyun.oos.server.conf.LogConfig;
import cn.ctyun.oos.server.log.BucketLog;

//统计所有log的500个数,进行告警与恢复，并发送到Ganglia
public class SLAStat {
  private static Log log = LogFactory.getLog(SLAStat.class);
  
  AtomicInteger totalRespNum = new AtomicInteger(0);
  AtomicInteger putNum = new AtomicInteger(0);
  AtomicInteger getNum = new AtomicInteger(0);
  AtomicInteger deleteNum = new AtomicInteger(0);
  AtomicInteger postNum = new AtomicInteger(0);
  AtomicInteger headNum = new AtomicInteger(0);
  AtomicInteger expireNum = new AtomicInteger(0);
  
  String id = String.valueOf(LogConfig.getId(HostUtils.getHostName()));
  String gangliaFakeIp = GangliaMan.getFakeIpHost(GangliaMan.OOS_GROUP_NAME);
  
  public static String formKeyWithMethodResp(String method, int respCode){
      return method+"_"+respCode;
  }
  
  static class AlarmEntity{
      public int alarmValue;
      boolean hasAlarmed;
      long lastAlarmtime;
      public AlarmEntity(int alarmValue){
          this.alarmValue = alarmValue;
      }
  }
  
  private static Map<String, AtomicInteger> methodcode2Count = new ConcurrentHashMap<>();//put 201 countNum
  private static Map<String, AlarmEntity> methodcode2CountAlarm = new ConcurrentHashMap<>();//put 201 countNum
  static{
      parseConfigFile();
  }
  public SLAStat() {
  }
  
  public void clear() {//开始新的5分钟统计
      Iterator<AtomicInteger> iter = methodcode2Count.values().iterator();
      while(iter.hasNext()){
          AtomicInteger value = (AtomicInteger) iter.next();
          value.set(0);
      }
      totalRespNum.set(0);
      putNum.set(0);
      getNum.set(0);
      deleteNum.set(0);
      postNum.set(0);
      headNum.set(0);
      expireNum.set(0);
  }
  
  public void sendToGangliaAndAlarm() {
      Iterator<String> iter = methodcode2Count.keySet().iterator();
      while(iter.hasNext()){
          String methodRespcode = iter.next();
          int count = methodcode2Count.get(methodRespcode).get();
          
          /*ResponseCodeMeta meta = new ResponseCodeMeta();
          meta.code = methodRespcode.split("_")[0];
          meta.method = methodRespcode.split("_")[1];
          try {
            client.responseCodeInsert(meta);
          } catch (IOException e) {
            log.error(e.getMessage(),e);
          } */
          
          String gangliaName = GangliaConsts.RESPONSE_CODE_PREFIX + methodRespcode + "_" + id;
          GangliaPlugin.sendToGangliaWithFakeIp(gangliaName, String.valueOf(count), GangliaConsts.YTYPE_INT32, GangliaConsts.YNAME_NUM, GangliaConsts.GROUP_NAME_LOG_RESPONSE_CODE, "", gangliaFakeIp);
          
          double ratio= 0;
          int total = getMethodTotalnum(methodRespcode);
          assert total >= count;
          if(total != 0){
              ratio = count / (double)total * 100;
          }
          gangliaName = GangliaConsts.RESPONSE_RATIO_PREFIX + methodRespcode + "_" + id;
          GangliaPlugin.sendToGangliaWithFakeIp(gangliaName, String.valueOf(ratio), GangliaConsts.YTYPE_DOUBLE, GangliaConsts.YNAME_PERCENTAGE, GangliaConsts.GROUP_NAME_LOG_RESPONSE_CODE, "", gangliaFakeIp);
      }      
      if(LogConfig.writeRespcodeToLog() == 1){
          alarmRespCode();
      }
  }
  
  public int getMethodTotalnum(String methodRespcode){//根据get_200 得到get请求的总数
      int total = 0;
      String method = methodRespcode.split("_")[0];
      method = method.trim().toLowerCase();
      switch (method) {
      case "put":
          total = putNum.get();
          break;
      case "get":
          total = getNum.get();
          break;
      case "delete":
          total = deleteNum.get();
          break;
      case "post":
          total = postNum.get();
          break;
      case "head":
          total = headNum.get();
          break;
      case "expire":
          total = expireNum.get();
          break;
      default: 
          log.equals("unknown:" + methodRespcode);
     }
     return total;
  }
  
  public void alarmRespCode(){
      Iterator<String> iter = methodcode2CountAlarm.keySet().iterator();
      while(iter.hasNext()){
          String methodRespcode = iter.next();
          int value = 0;
          AtomicInteger valueObj = methodcode2Count.get(methodRespcode);
          if(valueObj != null){
              value = valueObj.get();
          }
          int total = getMethodTotalnum(methodRespcode);
          AlarmEntity alarmEntity = methodcode2CountAlarm.get(methodRespcode);
          if(value < alarmEntity.alarmValue && alarmEntity.hasAlarmed){//刚才告警现在恢复了
              alarmEntity.hasAlarmed = false;
              String body = String.format("Server Error Recovery.In the Past Five Minutes, %s LogServerNode %s Total %s Num is:%d, %s Num is:%d", DataRegion.getRegion().getName(), id, methodRespcode, total, methodRespcode, value);
              String info = String.format("%s: %s", "Response Code 500 Recovery", body);
              log.warn(info);
          }
          if(value >= alarmEntity.alarmValue && (System.currentTimeMillis() - alarmEntity.lastAlarmtime >= OOSConfig.getEmailInterval())){
              alarmEntity.lastAlarmtime = System.currentTimeMillis();
              alarmEntity.hasAlarmed = true;
              String body = String.format("Server Error Alarm.In the Past Five Minutes, %s LogServerNode %s Total %s Num is:%d, %s Num is:%d", DataRegion.getRegion().getName(), id, methodRespcode, total, methodRespcode, value);
              String info = String.format("%s: %s", "Response Code 500 Alarm", body);
              log.warn(info);
          }
      }
  }
  
  public void parseLogLine(BucketLog bucketLog) {
      String[] parts = bucketLog.operation.split("\\.");
      if(parts.length <= 1){
          return;
      }
      String method = parts[1].trim().toLowerCase();
      totalRespNum.incrementAndGet();
      switch(method){
      case "put": 
          putNum.incrementAndGet();
          break;
      case "get": 
          getNum.incrementAndGet();
          break;       
      case "delete": 
          deleteNum.incrementAndGet();
          break;
      case "post": 
          postNum.incrementAndGet();
          break;       
      case "head": 
          headNum.incrementAndGet();
          break;
      case "expire": 
          expireNum.incrementAndGet();
          break;
      }
      
      String key = formKeyWithMethodResp(method, bucketLog.status);
      if(methodcode2Count.containsKey(key)){//如果我们关心相应的响应码
          methodcode2Count.get(key).incrementAndGet();
      }
  }
  
  
  /**
   * 解析配置文件
   * <p>
    {
        "statistic":
            [
                {"method":"put", "resp":"400,201,403,404,500"},
                {"method":"get", "resp":"200,206,500"},
                {"method":"delete", "resp":"404,500"},
            ],
          "alarm":
            [
                {"method":"put", "resp":"500:1000,403:2000"},
                {"method":"get", "resp":"500:1000"},
                {"method":"delete", "resp":"500:1000"},  
            ]
    }</p>
   */
  private static void parseConfigFile(){
      try(InputStream in = new FileInputStream(new File(System.getenv("OOS_HOME") + "/conf/region/resp-code-statistic.txt"))){
//        try(InputStream in = new FileInputStream(new File("conf/global/resp-code-statistic.txt"))){
          String body = IOUtils.toString(in);
          JSONObject jo = new JSONObject(body);
          
          JSONArray statistic = jo.getJSONArray("statistic");
          for(int i=0; i<statistic.length(); i++){
              JSONObject j = statistic.getJSONObject(i);
              String method = j.getString("method").trim().toLowerCase();
              String respCode = j.getString("resp");
              String[] respCodes = respCode.split(",");
              for(String s : respCodes){
                  String key = formKeyWithMethodResp(method, Integer.parseInt(s));
                  AtomicInteger count = new AtomicInteger();
                  methodcode2Count.put(key, count);
              }
          }
          
          JSONArray alarm = jo.getJSONArray("alarm");
          for(int i=0; i<statistic.length(); i++){
              JSONObject j = alarm.getJSONObject(i);
              String method = j.getString("method").trim().toLowerCase();
              String respCode = j.getString("resp");
              String[] respCodes = respCode.split(",");
              for(String s : respCodes){
                  String[] respAlarm  = s.split(":");
                  if(respAlarm.length != 2){
                      throw new RuntimeException(respCodes.length+"!=2");
                  }
                  String key = formKeyWithMethodResp(method, Integer.parseInt(respAlarm[0]));
                  final AlarmEntity alarmEntity = new AlarmEntity(Integer.parseInt(respAlarm[1]));
                  if(!methodcode2Count.containsKey(key)){
                      throw new RuntimeException("config error");
                  }
                  methodcode2CountAlarm.put(key, alarmEntity);
              }
          }
//          dumpMap(methodcode2Count);
      } catch (IOException | JSONException e) {
          throw new RuntimeException(e);
    }
  }
}