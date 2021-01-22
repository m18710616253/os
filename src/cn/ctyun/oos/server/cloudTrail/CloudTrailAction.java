package cn.ctyun.oos.server.cloudTrail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import cn.ctyun.common.BaseException;
import cn.ctyun.oos.common.Utils;
import cn.ctyun.oos.hbase.HBaseManageEvent;
import cn.ctyun.oos.hbase.MetaClient;
import cn.ctyun.oos.metadata.BucketMeta;
import cn.ctyun.oos.metadata.CloudTrailEvent;
import cn.ctyun.oos.metadata.CloudTrailEvent.Resources;
import cn.ctyun.oos.metadata.CloudTrailMeta;
import cn.ctyun.oos.metadata.CloudTrailMeta.CloudTrailReadWriteType;
import cn.ctyun.oos.metadata.CloudTrailMeta.EventSelectors;
import cn.ctyun.oos.metadata.ManageEventMeta;
import cn.ctyun.oos.metadata.OwnerMeta;
import common.tuple.Pair;

public class CloudTrailAction {
    private static MetaClient metaClient = MetaClient.getGlobalClient();
    private static String trailNameRegEx = "^[A-Za-z0-9_\\-\\.]+$";
    private static String trailNameRegExS = "^[A-Za-z0-9].+$";
    private static String trailNameRegExE = "^.+[A-Za-z0-9]$";
    private static long ONE_DAY_MILLSECOND = 24 * 60 * 60 * 1000;
    private static int lookupRange = 180;
    
    public static String createTrailWithManageEvent(OwnerMeta owner, String trailName, String targetBucket, String prefix,
            ManageEventMeta manageEvent) throws Exception {
        CloudTrailMeta meta = null;
        JSONObject respJo = new JSONObject();
        try {
            meta = CloudTrailAction.createTrail(owner, trailName, targetBucket, prefix);
        } finally {
            if (meta != null) {
                // 响应元素
                respJo.put("Name", meta.trailName);
                respJo.put("S3BucketName", meta.targetBucket);
                respJo.put("TrailARN", meta.getARN());
                if (meta.prefix != null)
                    respJo.put("S3KeyPrefix", meta.prefix);
                
                if (manageEvent != null) {
                    // 资源
                    List<Resources> resourcesList = new ArrayList<CloudTrailEvent.Resources>();
                    Resources r1 = new Resources();
                    r1.resourceARN = meta.getARN();
                    r1.resourceName = meta.trailName;
                    r1.resourceType = "CloudTrail Trail";
                    resourcesList.add(r1);
                    Resources r2 = new Resources();
                    r2.resourceARN = "arn:ctyun:oos::" + owner.getAccountId() + ":bucket/" + meta.targetBucket;
                    r2.resourceName = meta.targetBucket;
                    r2.resourceType = "OOS Bucket";
                    resourcesList.add(r2);
                    handleManageEvent(manageEvent, respJo.toString(), resourcesList);
                }
            }
        }
        return respJo.toString();
    }

    /**
     * 新建审计追踪规则
     * 
     * @param ownerMeta
     * @param trailName
     * @param targetBucket
     * @param prefix
     * @throws Exception
     */
    public static CloudTrailMeta createTrail(OwnerMeta owner, String trailName, String targetBucket, String prefix)
            throws Exception {
        int ownerCurrentTrailNum = metaClient.cloudTrailNum(owner.getId());
        // 判断是否超过数量限制
        if (ownerCurrentTrailNum >= owner.cloudTrailCeilingNum) {
            throw new BaseException(400, "MaximumNumberOfTrailsExceededException", "User: " + owner.getAccountId() + " already has 10 trails.");
        }
        // 判断是否存在该bucket
        BucketMeta bucket = new BucketMeta(targetBucket);
        if(!metaClient.bucketSelect(bucket))
            throw new BaseException(400, "S3BucketDoesNotExistException", "Bucket name does not exist:" + targetBucket);
        if(bucket.ownerId != owner.getId())
            throw new BaseException(403, "AccessDenied", "Can not access bucket " + targetBucket);
        CloudTrailMeta meta = new CloudTrailMeta(owner.getId(), trailName);
        meta.targetBucket = targetBucket;
        if (prefix != null) {
            meta.prefix = prefix;
        }
        if (!metaClient.cloudTrailInsert(meta))
            throw new BaseException(400, "TrailAlreadyExistsException", "Trail " + trailName + " already exists for customer: " + owner.getAccountId());
        return meta;
    }
    
    /**
     * 检查trailname是否符合要求
     * @param trailName
     * @throws BaseException
     */
    public static void checkTrailNameFormat(String trailName) throws BaseException {
        // check name
        // 不能为空
        if(trailName == null || trailName.length() == 0)
            throw new BaseException(400, "InvalidTrailNameException", "Trail name cannot be blank!");
        // 不能是ip地址格式
        if(Utils.isValidIpAddr(trailName))
            throw new BaseException(400, "InvalidTrailNameException", "Trail names must not be formatted as an IP address.");
        // 长度限制
        if(trailName.length() < 3)
            throw new BaseException(400, "InvalidTrailNameException", "Trail name too short. Minimum allowed length: 3 characters. Specified name length:" + trailName.length());
        if(trailName.length() > 128)
            throw new BaseException(400, "InvalidTrailNameException", "Trail name too long. Maximum allowed length: 128 characters. Specified name length:" + trailName.length());
        // 仅包含ASCII字母（a-z，A-Z），数字（0-9），句点（.），下划线（_）或短划线（ - ）
        if(!Pattern.compile(trailNameRegEx).matcher(trailName).matches())
            throw new BaseException(400, "InvalidTrailNameException", "Trail name or ARN can only contain uppercase letters, lowercase letters, numbers, periods (.), hyphens (-), and underscores (_).");
        // 以字母或数字开头，以字母或数字结尾
        if(!Pattern.compile(trailNameRegExS).matcher(trailName).matches())
            throw new BaseException(400, "InvalidTrailNameException", "Trail name must starts with a letter or number.");
        if(!Pattern.compile(trailNameRegExE).matcher(trailName).matches())
            throw new BaseException(400, "InvalidTrailNameException", "Trail name must ends with a letter or number.");
        // 不包含连续的.-_
        String[] ss = trailName.split("\\.|-|_");
        for (String s: ss) {
            if(s.length() == 0) {
                throw new BaseException(400, "InvalidTrailNameException", "Trail name or ARN cannot have adjacent periods (.), hyphens (-), or underscores (_).");
            }
        }
    }
    
    /**
     * 检查日志文件前缀是否符合要求
     * @param prefix
     * @throws BaseException
     */
    public static void checkTrailFilePrefix(String prefix) throws BaseException {
        if(prefix.length() > 200)
            throw new BaseException(400, "InvalidS3PrefixException", "S3 prefix is longer than maximum length:" + prefix.length());
    }

    public static void deleteTrailWithManageEvent(OwnerMeta owner, String trailName, ManageEventMeta manageEvent)
            throws IOException, JSONException, BaseException {
        CloudTrailMeta meta = null;
        try {
            meta = deleteTrail(owner, trailName);
        } finally {
            if (manageEvent != null && meta != null) {
                // 资源
                List<Resources> resourcesList = new ArrayList<CloudTrailEvent.Resources>();
                Resources r = new Resources();
                r.resourceARN = meta.getARN();
                r.resourceName = meta.trailName;
                r.resourceType = "CloudTrail Trail";
                resourcesList.add(r);
                handleManageEvent(manageEvent, null, resourcesList);
            }
        }
    }

    /**
     * 删除审计追踪规则
     * 
     * @param owner
     * @param trailName
     * @throws IOException
     * @throws JSONException
     * @throws BaseException
     */
    public static CloudTrailMeta deleteTrail(OwnerMeta owner, String trailName) throws IOException, JSONException, BaseException {
        CloudTrailMeta meta = new CloudTrailMeta(owner.getId(), trailName);
        // 是否存在该追踪
        if (!metaClient.cloudTrailSelect(meta))
            throw new BaseException(400, "TrailNotFoundException", "Unknown trail: arn:ctyun:CloudTrail:ctyun:userId:trail/" + trailName + " for the customer: " + owner.getAccountId());
        metaClient.cloudTrailDelete(meta);
        return meta;
    }

    public static String describeTrailsWithManageEvent(OwnerMeta owner, List<String> trailNames, ManageEventMeta manageEvent)
            throws JSONException, IOException {
        List<CloudTrailMeta> trailMetas = null;
        JSONObject respJo = new JSONObject();
        try {
            trailMetas = describeTrails(owner, trailNames);
        } finally {
            // 响应元素
            JSONArray respJa = new JSONArray();
            if (trailMetas != null) {
                for (CloudTrailMeta meta : trailMetas) {
                    JSONObject metaJo = new JSONObject();
                    metaJo.put("Name", meta.trailName);
                    metaJo.put("S3BucketName", meta.targetBucket);
                    if (meta.prefix != null)
                        metaJo.put("S3KeyPrefix", meta.prefix);
                    metaJo.put("TrailARN", meta.getARN());
                    respJa.put(metaJo);
                }
                respJo.put("trailList", respJa);
            }
        }
        return respJo.toString();
    }

    /**
     * 获取多个审计跟踪的设置信息
     * 
     * @param owner
     * @param trailNames
     * @return
     * @throws IOException
     * @throws JSONException
     */
    public static List<CloudTrailMeta> describeTrails(OwnerMeta owner, List<String> trailNames)
            throws IOException, JSONException {
        List<CloudTrailMeta> metas = null;
        if (trailNames != null && trailNames.size() > 0) {
            List<String> rowKeysToSelect = new LinkedList<String>();
            for (String name : trailNames) {
                String rowKey = owner.getId() + "|" + name;
                rowKeysToSelect.add(rowKey);
            }
            metas = metaClient.cloudTrailBatchSelect(rowKeysToSelect);
        } else {
            // 如果名称列表为空，返回所有审计跟踪
            metas = metaClient.cloudTrailList(owner.getId());
        }
        return metas;
    }

    public static String getTrailStatusWithManageEvent(OwnerMeta owner, String trailName, ManageEventMeta manageEvent)
            throws IOException, JSONException, BaseException {
        CloudTrailMeta meta = null;
        JSONObject respJo = new JSONObject();
        try {
            meta = getTrailStatus(owner, trailName);
        } finally {
            // 响应元素
            if (meta != null) {
                respJo.put("IsLogging", meta.status);
                if (meta.lastDeliveryTime > 0)
                    respJo.put("LatestDeliveryTime", meta.lastDeliveryTime);
                if (meta.startLoggingTime > 0)
                    respJo.put("StartLoggingTime", meta.startLoggingTime);
                if (meta.stopLoggingTime > 0)
                    respJo.put("StopLoggingTime", meta.stopLoggingTime);

                if (manageEvent != null) {
                    // 资源
                    List<Resources> resourcesList = new ArrayList<CloudTrailEvent.Resources>();
                    Resources r = new Resources();
                    r.resourceARN = meta.getARN();
                    r.resourceName = meta.trailName;
                    r.resourceType = "CloudTrail Trail";
                    resourcesList.add(r);
                    handleManageEvent(manageEvent, null, resourcesList);
                }
            }
        }
        return respJo.toString();
    }

    /**
     * 获取一个审计跟踪信息
     * 
     * @param owner
     * @param trailName
     * @return
     * @throws IOException
     * @throws JSONException
     * @throws BaseException
     */
    public static CloudTrailMeta getTrailStatus(OwnerMeta owner, String trailName)
            throws IOException, JSONException, BaseException {
        CloudTrailMeta meta = new CloudTrailMeta(owner.getId(), trailName);
        // 是否存在该追踪
        if (!metaClient.cloudTrailSelect(meta))
            throw new BaseException(400, "TrailNotFoundException", "Unknown trail: arn:ctyun:CloudTrail:ctyun:userId:trail/" + trailName + " for the customer: " + owner.getAccountId());
        return meta;
    }

    public static String putEventSelectorsWithManageEvent(OwnerMeta owner, String trailName, Boolean includeManagementEvents,
            String readWriteType, ManageEventMeta manageEvent) throws BaseException, IOException, JSONException {
        CloudTrailMeta meta = null;
        JSONObject respJo = new JSONObject();
        try {
            meta = putEventSelectors(owner, trailName, includeManagementEvents, readWriteType);
        } finally {
            // 响应元素
            if (meta != null) {
                JSONArray respJa = new JSONArray();
                for (EventSelectors es : meta.eventSelectors) {
                    JSONObject esJo = new JSONObject();
                    esJo.put("ReadWriteType", es.getReadWriteType().toString());
                    respJa.put(esJo);
                }
                respJo.put("EventSelectors", respJa);
                respJo.put("TrailARN", meta.getARN());

                if (manageEvent != null) {
                    // 资源
                    List<Resources> resourcesList = new ArrayList<CloudTrailEvent.Resources>();
                    Resources r = new Resources();
                    r.resourceARN = meta.getARN();
                    r.resourceName = meta.trailName;
                    r.resourceType = "CloudTrail Trail";
                    resourcesList.add(r);
                    handleManageEvent(manageEvent, respJo.toString(), resourcesList);
                }
            }
        }
        return respJo.toString();
    }

    /**
     * 配置审计跟踪的事件筛选器。默认情况下，没有设置事件筛选器的审计跟踪，会记录所有读和写的管理事件。每个追踪最多创建事件筛选器1个。
     * 
     * @param ownerId
     * @param trailName
     * @param includeManagementEvents
     * @param readWriteType
     * @throws BaseException
     * @throws IOException
     * @throws JSONException
     */
    public static CloudTrailMeta putEventSelectors(OwnerMeta owner, String trailName, Boolean includeManagementEvents,
            String readWriteType) throws BaseException, IOException, JSONException {
        CloudTrailMeta meta = new CloudTrailMeta(owner.getId(), trailName);
        // 是否存在该追踪
        if (!metaClient.cloudTrailSelect(meta))
            throw new BaseException(400, "TrailNotFoundException", "Unknown trail: arn:ctyun:CloudTrail:ctyun:userId:trail/" + trailName + " for the customer: " + owner.getAccountId());
        CloudTrailReadWriteType type;
        // 默认为true
        if (includeManagementEvents == null)
            includeManagementEvents = true;
        // 默认为ALL
        if (readWriteType == null)
            type = CloudTrailReadWriteType.ALL;
        else
            type = CloudTrailReadWriteType.getType(readWriteType);
        EventSelectors e = new EventSelectors(includeManagementEvents, type);
        List<EventSelectors> eventSelectors = new LinkedList<CloudTrailMeta.EventSelectors>();
        eventSelectors.add(e);
        meta.eventSelectors = eventSelectors;
        metaClient.cloudTrailUpdateCommon(meta);
        return meta;
    }

    public static String getEventSelectorsWithManageEvent(OwnerMeta owner, String trailName, ManageEventMeta manageEvent)
            throws JSONException, IOException, BaseException {
        CloudTrailMeta meta = null;
        JSONObject respJo = new JSONObject();
        try {
            meta = getEventSelectors(owner, trailName);
        } finally {
            // 响应元素
            if (meta != null) {
                JSONArray respJa = new JSONArray();
                for (EventSelectors es : meta.eventSelectors) {
                    JSONObject esJo = new JSONObject();
                    esJo.put("ReadWriteType", es.getReadWriteType().toString());
                    respJa.put(esJo);
                }
                respJo.put("EventSelectors", respJa);
                respJo.put("TrailARN", meta.getARN());

                if (manageEvent != null) {
                    // 资源
                    List<Resources> resourcesList = new ArrayList<CloudTrailEvent.Resources>();
                    Resources r = new Resources();
                    r.resourceARN = meta.getARN();
                    r.resourceName = meta.trailName;
                    r.resourceType = "CloudTrail Trail";
                    resourcesList.add(r);
                    handleManageEvent(manageEvent, null, resourcesList);
                }
            }
        }
        return respJo.toString();
    }

    /**
     * 获取审计跟踪的事件选择器设置信息
     * 
     * @param owner
     * @param trailName
     * @return
     * @throws IOException
     * @throws JSONException
     * @throws BaseException
     */
    public static CloudTrailMeta getEventSelectors(OwnerMeta owner, String trailName)
            throws IOException, JSONException, BaseException {
        CloudTrailMeta meta = new CloudTrailMeta(owner.getId(), trailName);
        // 是否存在该追踪
        if (!metaClient.cloudTrailSelect(meta))
            throw new BaseException(400, "TrailNotFoundException", "Unknown trail: arn:ctyun:CloudTrail:ctyun:userId:trail/" + trailName + " for the customer: " + owner.getAccountId());
        List<EventSelectors> eventSelectors = meta.eventSelectors;
        // 没有设置事件筛选器的审计跟踪，默认返回会记录所有读和写的管理事件
        if (eventSelectors == null || eventSelectors.isEmpty()) {
            eventSelectors = new ArrayList<CloudTrailMeta.EventSelectors>();
            EventSelectors es = new EventSelectors(true, CloudTrailReadWriteType.ALL);
            eventSelectors.add(es);
            meta.eventSelectors = eventSelectors;
        }
        return meta;
    }

    public static String updateTrailWithManageEvent(OwnerMeta owner, String trailName, String targetBucket, Object prefix,
            ManageEventMeta manageEvent) throws IOException, JSONException, BaseException {
        CloudTrailMeta meta = null;
        JSONObject respJo = new JSONObject();
        try {
            meta = updateTrail(owner, trailName, targetBucket, prefix);
        } finally {
            // 响应元素
            if (meta != null) {
                respJo.put("Name", meta.trailName);
                respJo.put("S3BucketName", meta.targetBucket);
                respJo.put("TrailARN", meta.getARN());
                if (meta.prefix != null)
                    respJo.put("S3KeyPrefix", meta.prefix);

                if (manageEvent != null) {
                    // 资源
                    List<Resources> resourcesList = new ArrayList<CloudTrailEvent.Resources>();
                    Resources r1 = new Resources();
                    r1.resourceARN = meta.getARN();
                    r1.resourceName = meta.trailName;
                    r1.resourceType = "CloudTrail Trail";
                    resourcesList.add(r1);
                    Resources r2 = new Resources();
                    r2.resourceARN = "arn:ctyun:oos::" + owner.getAccountId() + ":bucket/" + meta.targetBucket;
                    r2.resourceName = meta.targetBucket;
                    r2.resourceType = "OOS Bucket";
                    resourcesList.add(r2);
                    handleManageEvent(manageEvent, respJo.toString(), resourcesList);
                }
            }
        }
        return respJo.toString();
    }

    /**
     * 更新审计跟踪，指定将日志数据保存到OOS bucket中。更新trail信息时，不用先停止cloud trail
     * 
     * @param owner
     * @param trailName
     * @param targetBucket
     * @param prefix
     * @throws IOException
     * @throws JSONException
     * @throws BaseException
     */
    public static CloudTrailMeta updateTrail(OwnerMeta owner, String trailName, String targetBucket, Object prefix)
            throws IOException, JSONException, BaseException {
        CloudTrailMeta meta = new CloudTrailMeta(owner.getId(), trailName);
        // 是否存在该追踪
        if (!metaClient.cloudTrailSelect(meta))
            throw new BaseException(400, "TrailNotFoundException", "Unknown trail: arn:ctyun:CloudTrail:ctyun:userId:trail/" + trailName + " for the customer: " + owner.getAccountId());
        if (targetBucket != null) {
            BucketMeta bucket = new BucketMeta(targetBucket);
            if(!metaClient.bucketSelect(bucket))
                throw new BaseException(400, "S3BucketDoesNotExistException", "Bucket name does not exist:" + targetBucket);
            if(bucket.ownerId != owner.getId())
                throw new BaseException(403, "AccessDenied", "Can not access bucket " + targetBucket);
            meta.targetBucket = targetBucket;
        }
        if (prefix != null) {
            if (prefix.equals(null)) {
                meta.prefix = null;
            } else
                meta.prefix = prefix.toString();
        }        
        metaClient.cloudTrailUpdateCommon(meta);
        return meta;
    }

    public static void startLoggingWithManageEvent(OwnerMeta owner, String trailName, ManageEventMeta manageEvent)
            throws IOException, JSONException, BaseException {
        CloudTrailMeta meta = null;
        try {
            meta = startLogging(owner, trailName);
        } finally {
            if (manageEvent != null && meta != null) {
                // 资源
                List<Resources> resourcesList = new ArrayList<CloudTrailEvent.Resources>();
                Resources r = new Resources();
                r.resourceARN = meta.getARN();
                r.resourceName = meta.trailName;
                r.resourceType = "CloudTrail Trail";
                resourcesList.add(r);
                handleManageEvent(manageEvent, null, resourcesList);
            }
        }
    }

    /**
     * 开始追踪
     * 
     * @param ownerId
     * @param trailName
     * @throws IOException
     * @throws JSONException
     * @throws BaseException
     */
    public static CloudTrailMeta startLogging(OwnerMeta owner, String trailName)
            throws IOException, JSONException, BaseException {
        CloudTrailMeta meta = new CloudTrailMeta(owner.getId(), trailName);
        if (!metaClient.cloudTrailSelect(meta))
            throw new BaseException(400, "TrailNotFoundException", "Unknown trail: arn:ctyun:CloudTrail:ctyun:userId:trail/" + trailName + " for the customer: " + owner.getAccountId());
        meta.status = true;
        meta.startLoggingTime = System.currentTimeMillis();
        metaClient.cloudTrailUpdateStatus(meta);
        return meta;
    }

    public static void stopLoggingWithManageEvent(OwnerMeta owner, String trailName, ManageEventMeta manageEvent)
            throws IOException, JSONException, BaseException {
        CloudTrailMeta meta = null;
        try {
            meta = stopLogging(owner, trailName);
        } finally {
            if (manageEvent != null && meta != null) {
                // 资源
                List<Resources> resourcesList = new ArrayList<CloudTrailEvent.Resources>();
                Resources r = new Resources();
                r.resourceARN = meta.getARN();
                r.resourceName = meta.trailName;
                r.resourceType = "CloudTrail Trail";
                resourcesList.add(r);
                handleManageEvent(manageEvent, null, resourcesList);
            }
        }
    }

    /**
     * 停止追踪
     * 
     * @param ownerId
     * @param trailName
     * @throws IOException
     * @throws JSONException
     * @throws BaseException
     */
    public static CloudTrailMeta stopLogging(OwnerMeta owner, String trailName) throws IOException, JSONException, BaseException {
        CloudTrailMeta meta = new CloudTrailMeta(owner.getId(), trailName);
        if (!metaClient.cloudTrailSelect(meta))
            throw new BaseException(400, "TrailNotFoundException", "Unknown trail: arn:ctyun:CloudTrail:ctyun:userId:trail/" + trailName + " for the customer: " + owner.getAccountId());
        meta.status = false;
        meta.stopLoggingTime = System.currentTimeMillis();
        metaClient.cloudTrailUpdateStatus(meta);
        return meta;
    }

    public static String lookUpEventsWithManageEvent(OwnerMeta owner, long startTime, long stopTime,
            Map<String, String> attributes,  int maxAttributesNum, int maxResult, String nextToken, ManageEventMeta manageEvent)
            throws BaseException, IOException, JSONException {
        JSONObject respJo = new JSONObject();
        List<ManageEventMeta> eventMetas = null;
        try {
            eventMetas = lookUpEvents(owner, startTime, stopTime, attributes, maxAttributesNum,  maxResult, nextToken);
        } finally {
            // 响应元素
            if (eventMetas != null) {
                JSONArray eventsJa = new JSONArray();
                int num = 0;
                String nextTokenResp = null;
                for (ManageEventMeta manageEventMeta : eventMetas) {
                    if (num >= (maxResult - 1)) {
                        nextTokenResp = manageEventMeta.getRowKey(); // TODO 是否需要加密？直接返回了带ownerId的rowkey
                        break;
                    }
                    JSONObject mJo = new JSONObject();
                    mJo.put("CloudTrailEvent", manageEventMeta.getShowJson());
                    eventsJa.put(mJo);
                    num++;
                }
                respJo.put("Events", eventsJa);
                if (nextTokenResp != null)
                    respJo.put("NextToken", nextTokenResp);
            }
        }
        return respJo.toString();
    }

    public static List<ManageEventMeta> lookUpEvents(OwnerMeta owner, long startTime, long stopTime,
            Map<String, String> attributes, int maxAttributesNum, int maxResults, String nextToken) throws BaseException, IOException, JSONException {
        Pair<Long, Long> time = checkLookupTime(startTime, stopTime);
        startTime = time.first();
        stopTime = time.second();
        if(attributes.size() > maxAttributesNum)
            throw new BaseException(400, "InvalidLookupAttributesException", "Lookup attribute key can not be multiple.");
        // 如果包含eventId，可根据rowKey进行select
        if(attributes.keySet().contains("EventId")) {
            long eventId;
            try {
                eventId = Long.valueOf(attributes.get("EventId")); 
            } catch (NumberFormatException e) {
                throw new BaseException(400, "InvalidLookupAttributesException", "Lookup attribute value is not valid.");
            }
            List<ManageEventMeta> list = new ArrayList<ManageEventMeta>();
            ManageEventMeta meta = metaClient.manageEventSelectByEventId(owner.getId(), eventId);
           if(meta != null)
                list.add(meta);
            return list;
        }
        Map<String, Pair<byte[], String>> attributesCol = new HashMap<String, Pair<byte[],String>>();
        for (Entry<String, String> attribute : attributes.entrySet()) {
            byte[] columnName = getColumnNameFromAttributeKey(attribute.getKey());
            if (columnName == null)
                throw new BaseException(400, "InvalidLookupAttributesException", "Lookup attribute key is not valid.");
            if(attribute.getValue() == null || attribute.getValue().length() == 0)
                throw new BaseException(400, "InvalidLookupAttributesException", "Lookup attribute value cannot be null or empty");
            if(attribute.getKey().equals("ReadOnly") && !attribute.getValue().equals("true") && !attribute.getValue().equals("false"))
                throw new BaseException(400, "InvalidLookupAttributesException", "Lookup attribute value is not valid.");
                
            String value = attribute.getValue();
            if(attribute.getKey().equals("ResourceType"))
                value = "\"type\":\"" + value + "\"";
            else if(attribute.getKey().equals("ResourceName"))
                value = "\"name\":\"" + value;
            attributesCol.put(attribute.getKey(), new Pair<byte[], String>(columnName, value));
        }
        return metaClient.manageEventListByColumn(owner.getId(), startTime, stopTime, attributesCol, nextToken, maxResults);
    }
    
    /**
     * 检查lookup event时时间范围是否符合规则
     * @param startTime
     * @param stopTime
     * @return
     * @throws BaseException
     */
    public static Pair<Long, Long> checkLookupTime(long startTime, long stopTime) throws BaseException {
        if (startTime > 0 && stopTime > 0 && startTime > stopTime)
            throw new BaseException(400, "InvalidTimeRangeException", "The start time precedes the end time.");
        // 如果起始时间或者结束时间是早于n天之前的时间，则会返回数据的起始时间为180天前
        Date now = new Date();
        long rangeBegin = (now.getTime() - lookupRange * ONE_DAY_MILLSECOND) - (now.getTime() % ONE_DAY_MILLSECOND);
        if (startTime < rangeBegin)
            startTime = rangeBegin;
        if (stopTime != -1 && stopTime < rangeBegin)
            stopTime = rangeBegin + ONE_DAY_MILLSECOND;
        return new Pair<Long, Long>(startTime, stopTime);
    }

    /**
     * 处理管理事件中，与日志追踪相关的参数
     * 
     * @param manageEvent
     * @param eventType
     * @param reqParam
     * @param respParam
     */
    private static void handleManageEvent(ManageEventMeta manageEvent, String respParam, List<Resources> resourcesList) {
        if (manageEvent == null)
            return;
        CloudTrailEvent event = manageEvent.getEvent();
        event.responseElements = respParam;
        event.resources = resourcesList;
    }

    /**
     * 获取筛选时根据的列
     * 
     * @param key
     * @return
     */
    private static byte[] getColumnNameFromAttributeKey(String key) {
        switch (key) {
        case "EventName":
            return HBaseManageEvent.event_name_cl;
        case "ReadOnly":
            return HBaseManageEvent.readOnly_cl;
        case "UserName":
            return HBaseManageEvent.user_name_cl;
        case "ResourceType":
        case "ResourceName":
            return HBaseManageEvent.resource_cl;
        case "EventSource":
            return HBaseManageEvent.event_source_cl;
        case "AccessKeyId":
            return HBaseManageEvent.accessKey_id_cl;
        default:
            return null;
        }
    }
}
