package cn.ctyun.oos.server.log.logstats;

import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.HttpMethod;

import cn.ctyun.common.conf.OOSConfig;
import cn.ctyun.common.utils.HostUtils;
import cn.ctyun.oos.common.MailServer;
import cn.ctyun.oos.server.conf.LogConfig;
import cn.ctyun.oos.server.log.BucketLog;

//对SLA性能进行统计
public class OOSLogSLA {
  private static Log log = LogFactory.getLog(OOSLogSLA.class);
  private static MailServer mailServer = new MailServer(OOSConfig.getSmtpHost(), OOSConfig.getEmailPort(), OOSConfig.getFromEmail(), OOSConfig.getEmailPassword());
  private static String strPutObject = "REST." + HttpMethod.PUT.toString() + ".OBJECT";
  private static String strGetObject = "REST." + HttpMethod.GET.toString() + ".OBJECT";
  private static String strDeleteObject = "REST." + HttpMethod.DELETE.toString() + ".OBJECT";
  public static long logSlaStatIntervMs = 3*3600*1000; 
  //public static long size1M = 1024*1024L;
  private static double size1M = 1024.0*1024;
  //public static String resourcePool = "OOS-SH-OOSLOG-";
  public static int MAXTIME = 100000;//100s
  public static int GET_TIMEOUT = 5000;
  private Map<String, AtomicLong> slaStat = new HashMap<String, AtomicLong>();
  private long lastProcTimeMs;
  private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy年MM月dd日HH时mm分");

  String id = String.valueOf(LogConfig.getId(HostUtils.getHostName()));
  
  public OOSLogSLA(){
      this.lastProcTimeMs = System.currentTimeMillis();
  }
  public void parseLogLine(BucketLog logLine) {
      if(logLine.operation == null){
          return;
      }
      if(check4xx(logLine.status) || check3xx(logLine.status)){
          return;
      }
      if(logLine.operation.equalsIgnoreCase(strPutObject)){
          if(logLine.status >= 200 && logLine.status <=299){//get resp code 200-299之间算success
              long putObjMs = logLine.adapterResponseStartTime - logLine.clientRequestLastTime;//客户端接收到响应的时间-发送完请求的最后一个字节时间
              if(checkErrorTime(putObjMs)){
                  return;
              }
              if(logLine.exception == 3 ){//3.忽略分段
                  return;
              }
              String type = "PutNumAllSize";//所有成功次数
              handleOneType(type, 1);

              long len = parseContentLen(logLine.contentLength);
              if(len < size1M){
                  return;
              }
              type = "PutNum";//>1M的次数
              handleOneType(type, 1);
              
              type = "PutTime";
              int putObjMsPerM = (int) (putObjMs * size1M / len);//平均每M多少Ms
              handleOneType(type, (int) putObjMsPerM);
              if(putObjMsPerM < 150){
                  type = "Put150";
                  handleOneType(type, 1);
              }
              if(putObjMsPerM < 250){
                  type = "Put250";
                  handleOneType(type, 1);
              }
              if(putObjMsPerM < 300){
                  type = "Put300";
                  handleOneType(type, 1);
              }
              if(putObjMsPerM < 500){
                  type = "Put500";
                  handleOneType(type, 1);
              }
              if(putObjMsPerM < 1000){
                  type = "Put1000";
                  handleOneType(type, 1);
              }
              if(putObjMsPerM < 1500){
                  type = "Put1500";
                  handleOneType(type, 1);
              }
              if(putObjMsPerM < 2000){
                  type = "Put2000";
                  handleOneType(type, 1);
              }
              if(putObjMsPerM >= 2000){
                  type = "Put3000";
                  handleOneType(type, 1);
              }
          }else{
              String type = "PutError";
              handleOneType(type, 1);
          }
      }else if(logLine.operation.equalsIgnoreCase(strGetObject)){ 
          if(logLine.status >= 200 && logLine.status <=299){
              long getObjMs = logLine.adapterResponseStartTime;//adapterResponseStartTime已经改为与startTime的差值
              if(checkErrorTime(getObjMs)){
                  return;
              }
              if(logLine.exception == 1 || logLine.exception == 2){//1.忽略2xx的异常 2.忽略分段
                  return;
              }
              String type = "GetNumAllSize";
              handleOneType(type, 1);
              
              // oos服务端返回第一个字节的时间-接收客户端发送最后一个字节的时间差大于1秒，即为慢请求
              if(logLine.adapterResponseFirstTime > 1000) {
                  type = "SlowGet";
                  handleOneType(type, 1);
              }
              
              long len = parseContentLen(logLine.contentLength);
              if(len < size1M){
                  return;
              }
              type = "GetNum";
              handleOneType(type, 1);
              
              type = "GetTime";
              int getObjMsPerM = (int) (getObjMs * size1M / len);
              handleOneType(type, getObjMsPerM);
             
              if(getObjMsPerM < 150){
                  type = "Get150";
                  handleOneType(type, 1);
              }
              if(getObjMsPerM < 250){
                  type = "Get250";
                  handleOneType(type, 1);
              }
              if(getObjMsPerM < 300){
                  type = "Get300";
                  handleOneType(type, 1);
              }
              if(getObjMsPerM < 500){
                  type = "Get500";
                  handleOneType(type, 1);
              }
              if(getObjMsPerM < 1000){
                  type = "Get1000";
                  handleOneType(type, 1);
              }
              if(getObjMsPerM < 1500){
                  type = "Get1500";
                  handleOneType(type, 1);
              }
              if(getObjMsPerM < 2000){
                  type = "Get2000";
                  handleOneType(type, 1);
              }
              if(getObjMsPerM >= 2000){
                  type = "Get3000";
                  handleOneType(type, 1);
              }
          }else{
              String type = "GetError";
              handleOneType(type, 1);
          }
      }else if(logLine.operation.equalsIgnoreCase(strDeleteObject)){
          if(logLine.status >= 200 && logLine.status <=299){
              long deleteObjMs = logLine.adapterResponseStartTime;//adapterResponseStartTime已经改为与startTime的差值
              if(checkErrorTime(deleteObjMs)){
                  return;
              }
              if(logLine.exception == 4 ){//4.忽略分段
                  return;
              }
              String type = "DeleteNumAllSize";
              handleOneType(type, 1);
              
              long len = parseContentLen(logLine.objectSize);//取object size的大小
              if(len < size1M){
                  return;
              }
              type = "DeleteNum";
              handleOneType(type, 1);
              
              type = "DeleteTime";
              int deleteObjMsPerM = (int) (deleteObjMs * size1M / len);
              handleOneType(type, (int) deleteObjMsPerM);
              if(deleteObjMsPerM < 150){
                  type = "Delete150";
                  handleOneType(type, 1);
              }
              if(deleteObjMsPerM < 250){
                  type = "Delete250";
                  handleOneType(type, 1);
              }
              if(deleteObjMsPerM < 300){
                  type = "Delete300";
                  handleOneType(type, 1);
              }
              if(deleteObjMsPerM < 500){
                  type = "Delete500";
                  handleOneType(type, 1);
              }
              if(deleteObjMsPerM < 1000){
                  type = "Delete1000";
                  handleOneType(type, 1);
              }
              if(deleteObjMsPerM < 1500){
                  type = "Delete1500";
                  handleOneType(type, 1);
              }
              if(deleteObjMsPerM < 2000){
                  type = "Delete2000";
                  handleOneType(type, 1);
              }
              if(deleteObjMsPerM >= 2000){
                  type = "Delete3000";
                  handleOneType(type, 1);
              }
          }else{
              String type = "DeleteError";
              handleOneType(type, 1);
          }
      }
  }
  
  public void clear(){
      slaStat.clear();
  }
  
  private void handleOneType(String type, int delta){
      synchronized (slaStat) {
          AtomicLong v = slaStat.get(type);
          if(v != null){
              v.addAndGet(delta);
          }else{
              slaStat.put(type, new AtomicLong(delta));
          }
      }
  }
  
  public void writeRequestStatisLog(){
      try {          
          DecimalFormat df3dot = new DecimalFormat("#####0.000");
          DecimalFormat df8dot = new DecimalFormat("##0.00000000");
          String key = "PutNumAllSize";
          long putSuccAllSizeNum = slaStat.get(key) != null ? slaStat.get(key).get() : 0;
          key = "PutNum";
          long putSuccNum = slaStat.get(key) != null ? slaStat.get(key).get() : 0;
          key = "PutError";
          long putErrorNum = slaStat.get(key) != null ? slaStat.get(key).get() : 0;
          key = "GetNumAllSize";
          long getSuccAllSizeNum = slaStat.get(key) != null ? slaStat.get(key).get() : 0;
          key = "GetNum";
          long getSuccNum = slaStat.get(key) != null ? slaStat.get(key).get() : 0;
          key = "GetError";
          long getErrorNum = slaStat.get(key) != null ? slaStat.get(key).get() : 0;
          key = "DeleteNumAllSize";
          long deleteSuccAllSizeNum = slaStat.get(key) != null ? slaStat.get(key).get() : 0;
          key = "DeleteNum";
          long deleteSuccNum = slaStat.get(key) != null ? slaStat.get(key).get() : 0;
          key = "DeleteError";
          long deleteErrorNum = slaStat.get(key) != null ? slaStat.get(key).get() : 0;
          key = "SlowGet";
          long slowGetNum = slaStat.get(key) != null ? slaStat.get(key).get() : 0;
          
          String dateStr = dateFormat.format(new Date());
          long costTimeMs = System.currentTimeMillis() - lastProcTimeMs;
          char separator = '&';
          StringBuilder txtBody = new StringBuilder("TXT Body is -> 当前时间:");
          txtBody.append(dateStr).append(",统计时长:");
          txtBody.append(costTimeMs / 3600000).append("小时").append(costTimeMs % 3600000 / 60000).append("分").append(separator);
          txtBody.append("PUT总次数:").append(separator).append(putSuccAllSizeNum + putErrorNum).append(separator);
          txtBody.append("错误数:").append(separator).append(putErrorNum).append(separator);
          txtBody.append("GET总次数:").append(separator).append(getSuccAllSizeNum + getErrorNum).append(separator);
          txtBody.append("错误数:").append(separator).append(getErrorNum).append(separator);
          txtBody.append("DELETE总次数:").append(separator).append(deleteSuccAllSizeNum + deleteErrorNum).append(separator);
          txtBody.append("错误数:").append(separator).append(deleteErrorNum).append(separator);
          txtBody.append("SLOWGET总次数:").append(separator).append(slowGetNum).append(separator);
          log.info(txtBody.toString());
      } catch (Throwable t) {
          log.error("oos slaLog Error", t);
      }
  }
  
  /** 在每隔5分钟的处理log的后面，检查是否到了3小时，如果到了将过去3小时的结果分析并写日志*/
  public void checkTime(){
      long now = System.currentTimeMillis(); 
      if(now - lastProcTimeMs >= LogConfig.getLogSLAInterval() * 1000){
          writeRequestStatisLog();
          clear();
          lastProcTimeMs = now;
      }
  }
  
  public boolean checkErrorTime(long time){
      if(time < 0 || time >= MAXTIME){
          return true;
      }
      return false;
  }
  public boolean check4xx(int status){
      if(status >=400 && status <= 499){
          return true;
      }
      return false;
  }
  public boolean check3xx(int status){
      if(status >= 300 && status <= 399){
          return true;
      }
      return false;
  }
  
  public long parseContentLen(String conLen){
      long len = -1;
      if(conLen == null || conLen.equalsIgnoreCase("-")){
          return -1;
      }
      try{
          len = Long.parseLong(conLen);
      }catch(NumberFormatException e){
          log.error("parsingContenLen", e);
      }
      return len;
  }
}
