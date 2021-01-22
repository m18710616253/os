package cn.ctyun.oos.server.util;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import cn.ctyun.common.Consts;
import cn.ctyun.common.conf.OOSConfig;
import cn.ctyun.oos.common.Utils;
import cn.ctyun.oos.metadata.MinutesUsageMeta;
import cn.ctyun.oos.metadata.MinutesUsageMeta.UsageMetaType;

public class CSVUtils {
    private static final Log log = LogFactory.getLog(CSVUtils.class);
    private static final int M = 1024 * 1024;

    public static final List<Object> PROXY_CAPACITY_HEAD = Arrays.asList("Date", "StorageClass", "SampleCapacity(Bytes)", "MaxCapacity(Bytes)", "AverageCapacity(Bytes)", "RemainderChargeStorageUsage(Bytes)", "RemainderChargeOfDuration(Bytes)", "RemainderChargeOfSize(Bytes)");
    // todo 临时兼容前端旧版本表头
    public static final List<Object> PROXY_CAPACITY_TEMP_HEAD = Arrays.asList("Date", "StorageClass", "SampleCapacity(Bytes)", "MaxCapacity(Bytes)", "AverageCapacity(Bytes)");

    public static final List<Object> PROXY_BILLED_STORAGE_HEAD = Arrays.asList("Date", "StorageClass", "BilledStorageUsage(Bytes)", "ActualMaxStorageUsage(Bytes)", "RemainderChargeStorageUsage(Bytes)", "RemainderChargeOfDuration(Bytes)", "RemainderChargeOfSize(Bytes)");
    public static final List<Object> PROXY_RESTORE_STORAGE_HEAD = Arrays.asList("Date", "StorageClass", "RestoreStorageUsage(Bytes)");
    public static final List<Object> PROXY_DELETE_STORAGE_HEAD = Arrays.asList("Date", "StorageClass", "DeleteStorageUsage(Bytes)");
    public static final List<Object> PROXY_FLOW_HEAD = Arrays.asList("Date", "StorageClass",
            "InternetDirectInbound(Bytes)", "InternetRoamInbound(Bytes)",
            "NonInternetDirectInbound(Bytes)", "NonInternetRoamInbound(Bytes)",
            "InternetDirectOutbound(Bytes)", "InternetRoamOutbound(Bytes)",
            "NonInternetDirectOutbound(Bytes)", "NonInternetRoamOutbound(Bytes)");
    public static final List<Object> PROXY_AVAIL_BANDWIDTH_HEAD = Arrays.asList(
            "Date", "Region", "InternetInboundAvailableBandwidth(Bytes/s)",
            "InternetOutboundAvailableBandwidth(Bytes/s)",
            "NonInternetInboundAvailableBandwidth(Bytes/s)",
            "NonInternetOutboundAvailableBandwidth(Bytes/s)");
    public static final List<Object> PROXY_REQUEST_ALL_HEAD = Arrays.asList("Date", "StorageClass", "Requests",
            "Response200", "Response204",
            "Response206", "Response403",
            "Response404", "Response4xx");
    public static final List<Object> PROXY_REQUEST_GET_HEAD = Arrays.asList("Date", "StorageClass", "Requests",
            "Response200",
            "Response206", "Response403",
            "Response404", "Response4xx");
    public static final List<Object> PROXY_REQUEST_HEAD_HEAD = Arrays.asList("Date", "StorageClass", "Requests",
            "Response200",
            "Response206", "Response403",
            "Response404", "Response4xx");
    public static final List<Object> PROXY_REQUEST_PUT_HEAD = Arrays.asList("Date", "StorageClass", "Requests",
            "Response200", "Response403",
            "Response404", "Response4xx");
    public static final List<Object> PROXY_REQUEST_POST_HEAD = Arrays.asList("Date", "StorageClass", "Requests",
            "Response200", "Response403",
            "Response404", "Response4xx");
    public static final List<Object> PROXY_REQUEST_DELETE_HEAD = Arrays.asList("Date", "StorageClass", "Requests",
            "Response200", "Response204",
            "Response403",
            "Response404", "Response4xx");
    public static final List<Object> PROXY_REQUEST_OTHER_HEAD = Arrays.asList("Date", "StorageClass", "Requests",
            "Response200", "Response403",
            "Response404", "Response4xx");
    public static final List<Object> PROXY_CONNECTIONS_HEAD = Arrays.asList(
            "Date", "StorageClass", "Connection", "InternetConnection", "NonInternetConnection");
    public static final List<Object> PROXY_AVAILABLE_RATE_HEAD = Arrays.asList(
            "Date", "StorageClass", "AvailableRate", "InternalServerErrorResponse", "ServiceUnavailableResponse", "Requests");
    public static final List<Object> PROXY_EFFECTIVE_RATE_HEAD = Arrays.asList(
            "Date", "StorageClass", "EffectiveRate", "SuccessResponse", "Requests");
    public static final List<Object> PROXY_FORBIDDEN_RATE_HEAD = Arrays.asList(
            "Date", "StorageClass", "ForbiddenRate", "ForbiddenResponse", "Requests");
    public static final List<Object> PROXY_NOTFOUND_RATE_HEAD = Arrays.asList(
            "Date", "StorageClass", "NotFoundRate", "NotFoundResponse", "Requests");
    public static final List<Object> PROXY_OTHERSERROR_RATE_HEAD = Arrays.asList(
            "Date", "StorageClass", "OthersErrorRate", "OthersErrorResponse", "Requests");

    public static final List<Object> CAPACITY_HEAD = Arrays.asList("Region", "BucketName", "Date", "StorageClass", "Capacity");
    public static final List<Object> BILLED_STORAGE_HEAD = Arrays.asList("Region", "BucketName", "Date", "StorageClass", "BilledStorageUsage(Bytes)", "ActualMaxStorageUsage(Bytes)", "RemainderChargeStorageUsage(Bytes)", "RemainderChargeOfDelete(Bytes)", "RemainderChargeOfUpdate(Bytes)", "RemainderChargeOfSize(Bytes)");
    public static final List<Object> RESTORE_STORAGE_HEAD = Arrays.asList("Region", "BucketName", "Date", "StorageClass", "RestoreStorageUsage(Bytes)");
    public static final List<Object> DELETE_STORAGE_HEAD = Arrays.asList("Region", "BucketName", "Date", "StorageClass", "DeleteStorageUsage(Bytes)");
    public static final List<Object> REDUNDANTSIZE_HEAD = Arrays.asList("Region", "BucketName", "Date", "StorageClass", "redundantSize");
    public static final List<Object> ALIGNSIZE_HEAD = Arrays.asList("Region", "BucketName", "Date", "StorageClass", "alignSize");
    public static final List<Object> ORIGINALTOTALSIZE_HEAD = Arrays.asList("Region", "BucketName", "Date", "StorageClass", "originalTotalSize");

    public static final List<Object> MAX_CAPACITY_HEAD = Arrays.asList("Region", "BucketName", "Date", "StorageClass", "Max_Capacity");
    public static final List<Object> MAX_ORIGINALPEAKSIZE_HEAD = Arrays.asList("Region", "BucketName", "Date", "StorageClass", "Max_OriginalPeakSize");

    public static final List<Object> AVG_CAPACITY_HEAD = Arrays.asList("Region", "BucketName", "Date", "StorageClass", "Avg_Capacity");
    public static final List<Object> AVG_REDUNDANTSIZE_HEAD = Arrays.asList("Region", "BucketName", "Date", "StorageClass", "AVG_RedundantSize");
    public static final List<Object> AVG_ALIGNSIZE_HEAD = Arrays.asList("Region", "BucketName", "Date", "StorageClass", "AVG_AlignSize");
    public static final List<Object> AVG_ORIGINALTOTALSIZE_HEAD = Arrays.asList("Region", "BucketName", "Date", "StorageClass", "AVG_OriginalTotalSize");

    public static final List<Object> FLOW_HEAD = Arrays.asList("Region",
            "BucketName", "Date", "StorageClass", "ExternalUploadFlow", "ExternalDownloadFlow",
            "ExternalDeleteFlow", "ExternalRoamUploadFlow",
            "ExternalRoamDownloadFlow", "IntranetUploadFlow",
            "IntranetDownloadFlow", "IntranetDeleteFlow",
            "IntranetRoamUploadFlow", "IntranetRoamDownloadFlow");
    public static final List<Object> REQUEST_HEAD = Arrays.asList("Region",
            "BucketName", "Date", "StorageClass", "ExternalGetRequest", "ExternalHeadRequest",
            "ExternalPutRequest", "ExternalPostRequest",
            "ExternalDeleteRequest", "ExternalOtherRequest",
            "IntranetGetRequest", "IntranetHeadRequest", "IntranetPutRequest",
            "IntranetPostRequest", "IntranetDeleteRequest",
            "IntranetOtherRequest");
    public static final List<Object> BANDWIDTH_HEAD = Arrays.asList("Region",
            "BucketName", "Date", "ExternalInBoundBandwidthUsed",
            "ExternalOutBoundBandwidthUsed", "IntranetInBoundBandwidthUsed",
            "IntranetOutBoundBandwidthUsed");
    public static final List<Object> AVAIL_BANDWIDTH_HEAD = Arrays.asList(
            "Date", "Region", "ExternalInBoundBandwidthAvailable",
            "ExternalOutBoundBandwidthAvailable",
            "IntranetInBoundBandwidthAvailable",
            "IntranetOutBoundBandwidthAvailable");
    public static final List<Object> CONNECTION_HEAD = Arrays.asList("Region",
            "BucketName", "Date", "Concurrent Connections");
    public static final List<Object> PEAK_BANDWIDTH_95_OWNER_HEAD = Arrays.asList("Region",
            "Date", "InternetInbound_bandwidth_95p(Mbps)", "InternetOutbound_bandwidth_95p(Mbps)");
    public static final List<Object> PEAK_BANDWIDTH_95_BUCKET_HEAD = Arrays.asList("Region",
            "Date", "BucketName", "InternetInbound_bandwidth_95p(Mbps)", "InternetOutbound_bandwidth_95p(Mbps)");

    /**
     * 将统计项转成csv格式字符串,与前端页面图形数据一致
     *
     * @param region2usage  数据
     * @param type          统计项类型
     * @param usageMetaType rowkey type
     * @return csv格式字符串
     */
    public static String createCSV(
            Map<String, List<MinutesUsageMeta>> region2usage,
            Map<String, List<MinutesUsageMeta>> region2AvailBandwidthUsage,
            UsageType type, UsageMetaType usageMetaType,
            String freq,
            boolean ifNeedAllRegionsUsage, String beginDate, String endDate) {
        // 表格头
        List<Object> headList = null;
        if (type.equals(UsageType.PROXY_CAPACITY)) {
            if (OOSConfig.getFrontEndVersion() > 6_7) {
                headList = PROXY_CAPACITY_HEAD;
            }else {
                headList = PROXY_CAPACITY_TEMP_HEAD;
            }
        } else if (type.equals(UsageType.PROXY_BILLED_STORAGE)) {
            headList = PROXY_BILLED_STORAGE_HEAD;
        } else if (type.equals(UsageType.PROXY_RESTORE_STORAGE)) {
            headList = PROXY_RESTORE_STORAGE_HEAD;
        } else if (type.equals(UsageType.PROXY_DELETE_STORAGE)) {
            headList = PROXY_DELETE_STORAGE_HEAD;
        } else if (type.equals(UsageType.PROXY_FLOW)) {
            headList = PROXY_FLOW_HEAD;
        } else if (type.equals(UsageType.PROXY_REQUEST_ALL)) {
            headList = PROXY_REQUEST_ALL_HEAD;
        } else if (type.equals(UsageType.PROXY_REQUEST_GET)) {
            headList = PROXY_REQUEST_GET_HEAD;
        } else if (type.equals(UsageType.PROXY_REQUEST_PUT)) {
            headList = PROXY_REQUEST_PUT_HEAD;
        } else if (type.equals(UsageType.PROXY_REQUEST_HEAD)) {
            headList = PROXY_REQUEST_HEAD_HEAD;
        } else if (type.equals(UsageType.PROXY_REQUEST_DELETE)) {
            headList = PROXY_REQUEST_DELETE_HEAD;
        } else if (type.equals(UsageType.PROXY_REQUEST_POST)) {
            headList = PROXY_REQUEST_POST_HEAD;
        } else if (type.equals(UsageType.PROXY_REQUEST_OTHER)) {
            headList = PROXY_REQUEST_OTHER_HEAD;
        } else if (type.equals(UsageType.PROXY_CONNECTIONS)) {
            headList = PROXY_CONNECTIONS_HEAD;
        } else if (type.equals(UsageType.PROXY_AVAILABLE_RATE)) {
            headList = PROXY_AVAILABLE_RATE_HEAD;
        } else if (type.equals(UsageType.PROXY_EFFECTIVE_RATE)) {
            headList = PROXY_EFFECTIVE_RATE_HEAD;
        } else if (type.equals(UsageType.PROXY_FORBIDDEN_RATE)) {
            headList = PROXY_FORBIDDEN_RATE_HEAD;
        } else if (type.equals(UsageType.PROXY_NOTFOUND_RATE)) {
            headList = PROXY_NOTFOUND_RATE_HEAD;
        } else if (type.equals(UsageType.PROXY_OTHERSERROR_RATE)) {
            headList = PROXY_OTHERSERROR_RATE_HEAD;
        } else if (type.equals(UsageType.PROXY_AVAIL_BANDWIDTH)) {
            headList = PROXY_AVAIL_BANDWIDTH_HEAD;
        }
        /** 容量采样值 */
        if (type.equals(UsageType.CAPACITY)) {
            headList = CAPACITY_HEAD;
        } else if (type.equals(UsageType.REDUNDANTSIZE)) {
            headList = REDUNDANTSIZE_HEAD;
        } else if (type.equals(UsageType.ALIGNSIZE)) {
            headList = ALIGNSIZE_HEAD;
        } else if (type.equals(UsageType.ORIGINALTOTALSIZE)) {
            headList = ORIGINALTOTALSIZE_HEAD;
        } else if (type.equals(UsageType.BILLED_STORAGE)) {
            headList = BILLED_STORAGE_HEAD;
        } else if (type.equals(UsageType.RESTORE_STORAGE)) {
            headList = RESTORE_STORAGE_HEAD;
        } else if (type.equals(UsageType.DELETE_STORAGE)) {
            headList = DELETE_STORAGE_HEAD;
        }
        /** 容量峰值 */
        else if (type.equals(UsageType.MAX_ORIGINALPEAKSIZE)) {
            headList = MAX_ORIGINALPEAKSIZE_HEAD;
        } else if (type.equals(UsageType.MAX_CAPACITY)) {
            headList = MAX_CAPACITY_HEAD;
        }
        /** 容量平均值 */
        else if (type.equals(UsageType.AVG_CAPACITY)) {
            headList = AVG_CAPACITY_HEAD;
        } else if (type.equals(UsageType.AVG_REDUNDANTSIZE)) {
            headList = AVG_REDUNDANTSIZE_HEAD;
        } else if (type.equals(UsageType.AVG_ALIGNSIZE)) {
            headList = AVG_ALIGNSIZE_HEAD;
        } else if (type.equals(UsageType.AVG_ORIGINALTOTALSIZE)) {
            headList = AVG_ORIGINALTOTALSIZE_HEAD;
        } else if (type.equals(UsageType.FLOW)) {
            headList = FLOW_HEAD;
        } else if (type.equals(UsageType.REQUEST)) {
            headList = REQUEST_HEAD;
        } else if (type.equals(UsageType.BANDWIDTH)) {
            headList = BANDWIDTH_HEAD;
        } else if (type.equals(UsageType.AVAIL_BANDWIDTH)) {
            headList = AVAIL_BANDWIDTH_HEAD;
        } else if (type.equals(UsageType.CONNECTIONS)) {
            headList = CONNECTION_HEAD;
        } else if (type.equals(UsageType.PEAK_BANDWIDTH_95) && usageMetaType.equals(UsageMetaType.MONTH_OWNER)) {
            headList = PEAK_BANDWIDTH_95_OWNER_HEAD;
        } else if (type.equals(UsageType.PEAK_BANDWIDTH_95) && usageMetaType.equals(UsageMetaType.MONTH_BUCKET)) {
            headList = PEAK_BANDWIDTH_95_BUCKET_HEAD;
        }

        processSumTotalValue(region2usage, type, usageMetaType, beginDate, endDate);

        // 数据
        List<List<Object>> dataList = new ArrayList<>();
        List<Object> rowList = null;
        if (type.equals(UsageType.AVAIL_BANDWIDTH) || type.equals(UsageType.PROXY_AVAIL_BANDWIDTH)) {
            for (String region : region2AvailBandwidthUsage.keySet()) {
                for (MinutesUsageMeta m : region2AvailBandwidthUsage.get(region)) {
                    rowList = new ArrayList<>();
                    rowList.add(m.time + "\t");// +\t防止数字较多时显示成缩略格式
                    rowList.add(m.regionName);
                    if (m.availableBandwidth != null) {
                        rowList.add(m.availableBandwidth.pubInBW);// ExternalInBoundBandwidthUsed
                        rowList.add(m.availableBandwidth.pubOutBW);// ExternalOutBoundBandwidthUsed
                        rowList.add(m.availableBandwidth.priInBW);// IntranetInBoundBandwidthUsed
                        rowList.add(m.availableBandwidth.priOutBW);// IntranetOutBoundBandwidthUsed
                    } else {
                        rowList.add(0);
                        rowList.add(0);
                        rowList.add(0);
                        rowList.add(0);
                    }
                    dataList.add(rowList);
                }
            }
        } else if (type.equals(UsageType.PEAK_BANDWIDTH_95)) {
            for (String region : region2usage.keySet()) {
                for (MinutesUsageMeta m : region2usage.get(region)) {
                    rowList = new ArrayList<>();
                    rowList.add(m.regionName);
                    rowList.add(m.time);
                    if (usageMetaType.equals(UsageMetaType.MONTH_BUCKET)) { //bucket级别
                        rowList.add(m.bucketName == null ? "" : m.bucketName);
                    }
                    if (m.peakBandWidth != null) {
                        //从Byte/s转换为Mbps
                        BigDecimal up = new BigDecimal(m.peakBandWidth.pubPeakUpBW);
                        BigDecimal down = new BigDecimal(m.peakBandWidth.pubPeakTfBW);
                        up = up.multiply(new BigDecimal(8)).divide(new BigDecimal(M), 2, BigDecimal.ROUND_HALF_UP);
                        down = down.multiply(new BigDecimal(8)).divide(new BigDecimal(M), 2, BigDecimal.ROUND_HALF_UP);
                        rowList.add(up.toString());// 上行带宽95峰值
                        rowList.add(down.toString());// 下行带宽95峰值
                    } else {
                        rowList.add(0);
                        rowList.add(0);
                    }
                    dataList.add(rowList);
                }
            }
        } else {
            if (!ifNeedAllRegionsUsage && region2usage.containsKey(Consts.GLOBAL_DATA_REGION)) {
                List<MinutesUsageMeta> global = region2usage.get(Consts.GLOBAL_DATA_REGION);
                region2usage.clear();
                region2usage.put(Consts.GLOBAL_DATA_REGION, global);
            }
            for (String region : region2usage.keySet()) {
                for (MinutesUsageMeta m : region2usage.get(region)) {
                    rowList = new ArrayList<>();
                    if (!type.toString().startsWith("PROXY")) {
                        rowList.add(m.regionName);
                        rowList.add(m.bucketName == null ? "all" : m.bucketName);
                    }
                    rowList.add(m.time + "\t");// +\t防止数字较多时显示成缩略格式
                    rowList.add(m.storageType); // 存储类型
                    if (type.equals(UsageType.CONNECTIONS)) {
                        if(m.connection != null) {
                            rowList.add(m.connection.noNet_connections + m.connection.connections);
                        } else {
                            rowList.add(0);
                        }
                    }
                    //性能分析，本期不上
//                    /* 自服务：服务可用性 */
//                    else if (type.equals(UsageType.PROXY_AVAILABLE_RATE)) {
//                        BigDecimal total5xx = new BigDecimal(m.codeRequestStats.getTotal5xxRequestNum());
//                        BigDecimal totalReq = new BigDecimal(m.requestStats.getTotalRequest());
//                        BigDecimal ratio = new BigDecimal(1).subtract(total5xx.divide(totalReq, 2, BigDecimal.ROUND_HALF_UP));
//                        rowList.add(ratio.doubleValue());
//                    }                    
//                    /* 自服务：有效请求 */
//                    else if (type.equals(UsageType.PROXY_EFFECTIVE_RATE)) {
//                        BigDecimal total2xx = new BigDecimal(m.codeRequestStats.getTotal2xxRequestNum());
//                        BigDecimal totalReq = new BigDecimal(m.requestStats.getTotalRequest());
//                        BigDecimal ratio = new BigDecimal(1).subtract(total2xx.divide(totalReq, 2, BigDecimal.ROUND_HALF_UP));
//                        rowList.add(ratio.doubleValue());
//                        rowList.add(total2xx.longValue());
//                        rowList.add(totalReq.longValue());
//                    } 
//                    /* 自服务：客户端授权错误 */
//                    else if (type.equals(UsageType.PROXY_FORBIDDEN_RATE)) {
//                        BigDecimal total403 = new BigDecimal(m.codeRequestStats.getTotal403RequestNum());
//                        BigDecimal totalReq = new BigDecimal(m.requestStats.getTotalRequest());
//                        BigDecimal ratio = new BigDecimal(1).subtract(total403.divide(totalReq, 2, BigDecimal.ROUND_HALF_UP));
//                        rowList.add(ratio.doubleValue());
//                        rowList.add(total403.longValue());
//                        rowList.add(totalReq.longValue());
//                    } 
//                    /* 自服务：客户端资源不存在错误 */
//                    else if (type.equals(UsageType.PROXY_NOTFOUND_RATE)) {
//                        BigDecimal total404 = new BigDecimal(m.codeRequestStats.getTotal404RequestNum());
//                        BigDecimal totalReq = new BigDecimal(m.requestStats.getTotalRequest());
//                        BigDecimal ratio = new BigDecimal(1).subtract(total404.divide(totalReq, 2, BigDecimal.ROUND_HALF_UP));
//                        rowList.add(ratio.doubleValue());
//                        rowList.add(total404.longValue());
//                        rowList.add(totalReq.longValue());
//                    } 
//                    /* 自服务：客户端其他错误 */
//                    else if (type.equals(UsageType.PROXY_OTHERSERROR_RATE)) {
//                        BigDecimal total4_n = new BigDecimal(m.codeRequestStats.getTotal4_nRequestNum());
//                        BigDecimal totalReq = new BigDecimal(m.requestStats.getTotalRequest());
//                        BigDecimal ratio = new BigDecimal(1).subtract(total4_n.divide(totalReq, 2, BigDecimal.ROUND_HALF_UP));
//                        rowList.add(ratio.doubleValue());
//                        rowList.add(total4_n.longValue());
//                        rowList.add(totalReq.longValue());
//                    }

                    /* 自服务：容量 */
                    else if (type.equals(UsageType.PROXY_CAPACITY)) {
                        if (m.sizeStats != null) {
                            rowList.add(m.sizeStats.size_total);
                            if (!freq.equals(Utils.BY_5_MIN)) {
                                rowList.add(m.sizeStats.size_peak);
                                rowList.add(m.sizeStats.size_avgTotal);
                            }else {
                                rowList.add(0);
                                rowList.add(0);
                            }
                            if (OOSConfig.getFrontEndVersion() > 6_7) {
                                rowList.add(m.sizeStats.size_preChangeComplete + m.sizeStats.size_preDeleteComplete + m.sizeStats.size_completePeak);
                                rowList.add(m.sizeStats.size_preChangeComplete + m.sizeStats.size_preDeleteComplete);
                                rowList.add(m.sizeStats.size_completePeak);
                            }
                        } else {
                            rowList.add(0);
                            rowList.add(0);
                            rowList.add(0);
                            if (OOSConfig.getFrontEndVersion() > 6_7) {
                                rowList.add(0);
                                rowList.add(0);
                                rowList.add(0);
                            }
                        }
                    }
                    /* 自服务：计费容量 */
                    else if (type.equals(UsageType.PROXY_BILLED_STORAGE)) {
                        MinutesUsageMeta.SizeStats sizeStats = Optional.ofNullable(m.sizeStats).orElse(new MinutesUsageMeta.SizeStats());
                        //BilledStorageUsage
                        rowList.add(sizeStats.size_peak + sizeStats.size_preChangeComplete + sizeStats.size_preDeleteComplete + sizeStats.size_completePeak);
                        //ActualMaxStorageUsage
                        rowList.add(sizeStats.size_total);
                        //RemainderChargeStorageUsage
                        rowList.add(sizeStats.size_preChangeComplete + sizeStats.size_preDeleteComplete + sizeStats.size_completePeak);
                        //RemainderChargeofDuration
                        rowList.add(sizeStats.size_preChangeComplete + sizeStats.size_preDeleteComplete);
                        //RemainderChargeofSize
                        rowList.add(sizeStats.size_completePeak);
                    }
                    /* 自服务：数据取回量 */
                    else if (type.equals(UsageType.PROXY_RESTORE_STORAGE)) {
                        MinutesUsageMeta.SizeStats sizeStats = Optional.ofNullable(m.sizeStats).orElse(new MinutesUsageMeta.SizeStats());
                        // RestoreStorageUsage
                        rowList.add(sizeStats.size_restore);
                    }
                    /* 自服务：数据删除量*/
                    else if (type.equals(UsageType.PROXY_DELETE_STORAGE)) {
                        MinutesUsageMeta.FlowStats flowStats = Optional.ofNullable(m.flowStats).orElse(new MinutesUsageMeta.FlowStats());
                        rowList.add(flowStats.flow_delete + flowStats.flow_noNetDelete);
                    }
                    /* 自服务：流量 */
                    else if (type.equals(UsageType.PROXY_FLOW)) {
//                        rowList.add(m.flowStats.getTotalFlow());
                        if (m.flowStats != null) {
//                            rowList.add(m.flowStats.getTotalUploadFlow());
//                            rowList.add(m.flowStats.getTotalDownloadFlow());
                            rowList.add(m.flowStats.flow_upload);
                            rowList.add(m.flowStats.flow_roamUpload);
                            rowList.add(m.flowStats.flow_noNetUpload);
                            rowList.add(m.flowStats.flow_noNetRoamUpload);
                            rowList.add(m.flowStats.flow_download);
                            rowList.add(m.flowStats.flow_roamDownload);
                            rowList.add(m.flowStats.flow_noNetDownload);
                            rowList.add(m.flowStats.flow_noNetRoamDownload);
                        } else {
                            rowList.add(0);
                            rowList.add(0);
                            rowList.add(0);
                            rowList.add(0);
                            rowList.add(0);
                            rowList.add(0);
                            rowList.add(0);
                            rowList.add(0);
                        }
                    }
                    /* 自服务：请求次数 */
                    else if (type.equals(UsageType.PROXY_REQUEST_ALL)) {
                        if (m.requestStats != null) {
                            rowList.add(m.requestStats.getTotalRequest());
                            rowList.add(m.codeRequestStats.getTotal200RequestNum());
                            rowList.add(m.codeRequestStats.getTotal204RequestNum());
                            rowList.add(m.codeRequestStats.getTotal206RequestNum());
                            rowList.add(m.codeRequestStats.getTotal403RequestNum());
                            rowList.add(m.codeRequestStats.getTotal404RequestNum());
                            rowList.add(m.codeRequestStats.getTotal4_nRequestNum());
                        } else {
                            rowList.add(0);
                            rowList.add(0);
                            rowList.add(0);
                            rowList.add(0);
                            rowList.add(0);
                            rowList.add(0);
                            rowList.add(0);
                        }
                    }
                    /* 自服务：按返回码区分的请求次数 get */
                    else if (type.equals(UsageType.PROXY_REQUEST_GET)) {
                        if (m.codeRequestStats != null) {
                            rowList.add(m.requestStats.req_get + m.requestStats.req_noNetGet);
                            rowList.add(m.codeRequestStats.code_get200 + m.codeRequestStats.code_noNetGet200);
                            rowList.add(m.codeRequestStats.code_get206 + m.codeRequestStats.code_noNetGet206);
                            rowList.add(m.codeRequestStats.code_get403 + m.codeRequestStats.code_noNetGet403);
                            rowList.add(m.codeRequestStats.code_get404 + m.codeRequestStats.code_noNetGet404);
                            rowList.add(m.codeRequestStats.code_get4_n + m.codeRequestStats.code_noNetGet4_n);
                        } else {
                            rowList.add(0);
                            rowList.add(0);
                            rowList.add(0);
                            rowList.add(0);
                            rowList.add(0);
                            rowList.add(0);
                        }
                    }
                    /* 自服务：按返回码区分的请求次数 head */
                    else if (type.equals(UsageType.PROXY_REQUEST_HEAD)) {
                        if (m.codeRequestStats != null) {
                            rowList.add(m.requestStats.req_head + m.requestStats.req_noNetHead);
                            rowList.add(m.codeRequestStats.code_head200 + m.codeRequestStats.code_noNetHead200);
                            rowList.add(m.codeRequestStats.code_head206 + m.codeRequestStats.code_noNetHead206);
                            rowList.add(m.codeRequestStats.code_head403 + m.codeRequestStats.code_noNetHead403);
                            rowList.add(m.codeRequestStats.code_head404 + m.codeRequestStats.code_noNetHead404);
                            rowList.add(m.codeRequestStats.code_head4_n + m.codeRequestStats.code_noNetHead4_n);
                        } else {
                            rowList.add(0);
                            rowList.add(0);
                            rowList.add(0);
                            rowList.add(0);
                            rowList.add(0);
                            rowList.add(0);
                        }
                    }
                    /* 自服务：按返回码区分的请求次数 put */
                    else if (type.equals(UsageType.PROXY_REQUEST_PUT)) {
                        if (m.codeRequestStats != null) {
                            rowList.add(m.requestStats.req_put + m.requestStats.req_noNetPut);
                            rowList.add(m.codeRequestStats.code_put200 + m.codeRequestStats.code_noNetPut200);
                            rowList.add(m.codeRequestStats.code_put403 + m.codeRequestStats.code_noNetPut403);
                            rowList.add(m.codeRequestStats.code_put404 + m.codeRequestStats.code_noNetPut404);
                            rowList.add(m.codeRequestStats.code_put4_n + m.codeRequestStats.code_noNetPut4_n);
                        } else {
                            rowList.add(0);
                            rowList.add(0);
                            rowList.add(0);
                            rowList.add(0);
                            rowList.add(0);
                        }
                    }
                    /* 自服务：按返回码区分的请求次数 post */
                    else if (type.equals(UsageType.PROXY_REQUEST_POST)) {
                        if (m.codeRequestStats != null) {
                            rowList.add(m.requestStats.req_post + m.requestStats.req_noNetPost);
                            rowList.add(m.codeRequestStats.code_post200 + m.codeRequestStats.code_noNetPost200);
                            rowList.add(m.codeRequestStats.code_post403 + m.codeRequestStats.code_noNetPost403);
                            rowList.add(m.codeRequestStats.code_post404 + m.codeRequestStats.code_noNetPost404);
                            rowList.add(m.codeRequestStats.code_post4_n + m.codeRequestStats.code_noNetPost4_n);
                        } else {
                            rowList.add(0);
                            rowList.add(0);
                            rowList.add(0);
                            rowList.add(0);
                            rowList.add(0);
                        }
                    }
                    /* 自服务：按返回码区分的请求次数 delete */
                    else if (type.equals(UsageType.PROXY_REQUEST_DELETE)) {
                        if (m.codeRequestStats != null) {
                            rowList.add(m.requestStats.req_delete + m.requestStats.req_noNetDelete);
                            rowList.add(m.codeRequestStats.code_delete200 + m.codeRequestStats.code_noNetDelete200);
                            rowList.add(m.codeRequestStats.code_delete204 + m.codeRequestStats.code_noNetDelete204);
                            rowList.add(m.codeRequestStats.code_delete403 + m.codeRequestStats.code_noNetDelete403);
                            rowList.add(m.codeRequestStats.code_delete404 + m.codeRequestStats.code_noNetDelete404);
                            rowList.add(m.codeRequestStats.code_delete4_n + m.codeRequestStats.code_noNetDelete4_n);
                        } else {
                            rowList.add(0);
                            rowList.add(0);
                            rowList.add(0);
                            rowList.add(0);
                            rowList.add(0);
                            rowList.add(0);
                        }
                    }
                    /* 自服务：按返回码区分的请求次数 other */
                    else if (type.equals(UsageType.PROXY_REQUEST_OTHER)) {
                        if (m.codeRequestStats != null) {
                            rowList.add(m.requestStats.req_other + m.requestStats.req_noNetOther);
                            rowList.add(m.codeRequestStats.code_other200 + m.codeRequestStats.code_noNetOther200);
                            rowList.add(m.codeRequestStats.code_other403 + m.codeRequestStats.code_noNetOther403);
                            rowList.add(m.codeRequestStats.code_other404 + m.codeRequestStats.code_noNetOther404);
                            rowList.add(m.codeRequestStats.code_other4_n + m.codeRequestStats.code_noNetOther4_n);
                        } else {
                            rowList.add(0);
                            rowList.add(0);
                            rowList.add(0);
                            rowList.add(0);
                            rowList.add(0);
                        }
                    }
                    /* 自服务：并发连接数 */
                    else if (type.equals(UsageType.PROXY_CONNECTIONS)) {
                        if(m.connection != null) {
                            rowList.add(m.connection.noNet_connections + m.connection.connections);
                            rowList.add(m.connection.connections);
                            rowList.add(m.connection.noNet_connections);
                        } else {
                            rowList.add(0);
                            rowList.add(0);
                            rowList.add(0);
                        }                        
                    }
                    /** 容量采样值 */
                    else if (type.equals(UsageType.CAPACITY)) {
                        if (m.sizeStats != null) {
                            rowList.add(m.sizeStats.size_total);//归属地已用容量
                        } else {
                            rowList.add(0);
                        }
                    } else if (type.equals(UsageType.REDUNDANTSIZE)) {
                        if (m.sizeStats != null) {
                            rowList.add(m.sizeStats.size_redundant);//归属地冗余容量
                        } else {
                            rowList.add(0);
                        }
                    } else if (type.equals(UsageType.ALIGNSIZE)) {
                        if (m.sizeStats != null) {
                            rowList.add(m.sizeStats.size_alin);//归属地裸容量
                        } else {
                            rowList.add(0);
                        }
                    } else if (type.equals(UsageType.ORIGINALTOTALSIZE)) {
                        if (m.sizeStats != null) {
                            rowList.add(m.sizeStats.size_originalTotal);//应属地已用容量
                        } else {
                            rowList.add(0);
                        }
                    } else if (type.equals(UsageType.BILLED_STORAGE)) {
                        MinutesUsageMeta.SizeStats sizeStats = Optional.ofNullable(m.sizeStats).orElse(new MinutesUsageMeta.SizeStats());
                        //BilledStorageUsage
                        rowList.add(sizeStats.size_peak + sizeStats.size_preChangeComplete + sizeStats.size_preDeleteComplete + sizeStats.size_completePeak);
                        //ActualMaxStorageUsage
                        rowList.add(sizeStats.size_peak);
                        //RemainderChargeStorageUsage
                        rowList.add(sizeStats.size_preChangeComplete + sizeStats.size_preDeleteComplete + sizeStats.size_completePeak);
                        //RemainderChargeOfDeleta
                        rowList.add(sizeStats.size_preDeleteComplete);
                        //RemainderChargeOfUpdate
                        rowList.add(sizeStats.size_preChangeComplete);
                        //RemainderChargeOfSize
                        rowList.add(sizeStats.size_completePeak);
                    } else if (type.equals(UsageType.RESTORE_STORAGE)) {
                        MinutesUsageMeta.SizeStats sizeStats = Optional.ofNullable(m.sizeStats).orElse(new MinutesUsageMeta.SizeStats());
                        // RestoreStorageUsage
                        rowList.add(sizeStats.size_restore);
                    } else if (type.equals(UsageType.DELETE_STORAGE)) {
                        MinutesUsageMeta.FlowStats flowStats = Optional.ofNullable(m.flowStats).orElse(new MinutesUsageMeta.FlowStats());
                        rowList.add(flowStats.flow_delete + flowStats.flow_noNetDelete);
                    }
                    /** 容量峰值 */
                    else if (type.equals(UsageType.MAX_CAPACITY)) {
                        if (m.sizeStats != null) {
                            rowList.add(m.sizeStats.size_peak); //归属地容量峰值
                        } else {
                            rowList.add(0);
                        }
                    } else if (type.equals(UsageType.MAX_ORIGINALPEAKSIZE)) {
                        if (m.sizeStats != null) {
                            rowList.add(m.sizeStats.size_originalPeak); //应属地容量峰值
                        } else {
                            rowList.add(0);
                        }
                    }
                    /** 容量平均值 */
                    else if (type.equals(UsageType.AVG_CAPACITY)) {
                        if (m.sizeStats != null) {
                            rowList.add(m.sizeStats.size_avgTotal); //平均容量
                        } else {
                            rowList.add(0);
                        }
                    } else if (type.equals(UsageType.AVG_REDUNDANTSIZE)) {
                        if (m.sizeStats != null) {
                            rowList.add(m.sizeStats.size_avgRedundant);//归属地平均冗余容量
                        } else {
                            rowList.add(0);
                        }
                    } else if (type.equals(UsageType.AVG_ALIGNSIZE)) {
                        if (m.sizeStats != null) {
                            rowList.add(m.sizeStats.size_avgAlin);//归属地平均裸容量
                        } else {
                            rowList.add(0);
                        }
                    } else if (type.equals(UsageType.AVG_ORIGINALTOTALSIZE)) {
                        if (m.sizeStats != null) {
                            rowList.add(m.sizeStats.size_avgOriginalTotal);//应属地平均已用容量
                        } else {
                            rowList.add(0);
                        }
                    } else if (type.equals(UsageType.FLOW)) {
                        if (m.flowStats != null) {
                            rowList.add(m.flowStats.flow_upload);
                            rowList.add(m.flowStats.flow_download);
                            rowList.add(m.flowStats.flow_delete);
                            rowList.add(m.flowStats.flow_roamUpload);
                            rowList.add(m.flowStats.flow_roamDownload);
                            rowList.add(m.flowStats.flow_noNetUpload);
                            rowList.add(m.flowStats.flow_noNetDownload);
                            rowList.add(m.flowStats.flow_noNetDelete);
                            rowList.add(m.flowStats.flow_noNetRoamUpload);
                            rowList.add(m.flowStats.flow_noNetRoamDownload);
                        } else {
                            rowList.add(0);
                            rowList.add(0);
                            rowList.add(0);
                            rowList.add(0);
                            rowList.add(0);
                            rowList.add(0);
                            rowList.add(0);
                            rowList.add(0);
                            rowList.add(0);
                            rowList.add(0);
                        }
                    } else if (type.equals(UsageType.BANDWIDTH)) {
                        if (m.bandwidthMeta != null) {
                            rowList.add(m.bandwidthMeta.upload + m.bandwidthMeta.roamUpload);//ExternalInBoundBandwidthUsed
                            rowList.add(m.bandwidthMeta.transfer + m.bandwidthMeta.roamFlow);//ExternalOutBoundBandwidthUsed
                            rowList.add(m.bandwidthMeta.noNetUpload + m.bandwidthMeta.noNetRoamUpload);//IntranetInBoundBandwidthUsed
                            rowList.add(m.bandwidthMeta.noNetTransfer + m.bandwidthMeta.noNetRoamFlow);//IntranetOutBoundBandwidthUsed
                        } else {
                            rowList.add(0);
                            rowList.add(0);
                            rowList.add(0);
                            rowList.add(0);
                        }
                    } else if (type.equals(UsageType.REQUEST)) {
                        if (m.requestStats != null) {
                            rowList.add(m.requestStats.req_get);
                            rowList.add(m.requestStats.req_head);
                            rowList.add(m.requestStats.req_put);
                            rowList.add(m.requestStats.req_post);
                            rowList.add(m.requestStats.req_delete);
                            rowList.add(m.requestStats.req_other);
                            rowList.add(m.requestStats.req_noNetGet);
                            rowList.add(m.requestStats.req_noNetHead);
                            rowList.add(m.requestStats.req_noNetPut);
                            rowList.add(m.requestStats.req_noNetPost);
                            rowList.add(m.requestStats.req_noNetDelete);
                            rowList.add(m.requestStats.req_noNetOther);
                        } else {
                            rowList.add(0);
                            rowList.add(0);
                            rowList.add(0);
                            rowList.add(0);
                            rowList.add(0);
                            rowList.add(0);
                            rowList.add(0);
                            rowList.add(0);
                            rowList.add(0);
                            rowList.add(0);
                            rowList.add(0);
                            rowList.add(0);
                        }
                    }
                    dataList.add(rowList);
                }
            }
        }
        StringBuilder sb = new StringBuilder();
        // 写入文件头部
        writeRow(headList, sb);
        // 写入文件内容
        for (List<Object> row : dataList) {
            writeRow(row, sb);
        }
        return sb.toString();
    }

    /**
     * 写一行数据
     * 
     * @param row
     *            数据列表
     * @param sb
     */
    private static void writeRow(List<Object> row, StringBuilder sb) {
        for (Object data : row) {
            sb.append(data).append(",");
        }
        sb.append("\n");
    }

    private static void processSumTotalValue(Map<String, List<MinutesUsageMeta>> region2usage,
                                             UsageType type, UsageMetaType usageMetaType,
                                             String beginDate, String endDate) {
        // 处理需要计算和值的数据
        for (Map.Entry<String, List<MinutesUsageMeta>> entry : region2usage.entrySet()) {
            String region = entry.getKey();
            List<MinutesUsageMeta> list = entry.getValue();
            if (CollectionUtils.isEmpty(list)) {
                continue;
            }
            long userId = list.get(0).getUsrId();
            String bucket = list.get(0).bucketName;
            String storageType = list.get(0).storageType;
            MinutesUsageMeta sumAllMeta = new MinutesUsageMeta(usageMetaType, region, userId, bucket, beginDate + "~" + endDate, storageType);
            sumAllMeta.initStatsExceptConnection();
            try {
                switch (type) {
                    case PROXY_FLOW:
                    case FLOW:
                        Field[] flowFields = MinutesUsageMeta.FlowStats.class.getDeclaredFields();
                        for (MinutesUsageMeta m : list) {
                            for (Field field : flowFields) {
                                field.setAccessible(true);
                                field.setLong(sumAllMeta.flowStats, field.getLong(sumAllMeta.flowStats) + field.getLong(m.flowStats));
                            }
                        }
                        list.add(sumAllMeta);
                        break;
                    case PROXY_RESTORE_STORAGE:
                    case RESTORE_STORAGE:
                        long restoreSize = 0L;
                        for (MinutesUsageMeta m : list) {
                            restoreSize += m.sizeStats.size_restore;
                        }
                        sumAllMeta.sizeStats.size_restore = restoreSize;
                        list.add(sumAllMeta);
                        break;
                    case PROXY_DELETE_STORAGE:
                    case DELETE_STORAGE:
                        long flowDelete = 0L;
                        long flowNotNetDelete = 0L;
                        for (MinutesUsageMeta m : list) {
                            flowDelete += m.flowStats.flow_delete;
                            flowNotNetDelete += m.flowStats.flow_noNetDelete;
                        }
                        sumAllMeta.flowStats.flow_delete = flowDelete;
                        sumAllMeta.flowStats.flow_noNetDelete = flowNotNetDelete;
                        list.add(sumAllMeta);
                        break;
                    case REQUEST:
                    case PROXY_REQUEST_ALL:
                    case PROXY_REQUEST_GET:
                    case PROXY_REQUEST_POST:
                    case PROXY_REQUEST_PUT:
                    case PROXY_REQUEST_DELETE:
                    case PROXY_REQUEST_HEAD:
                    case PROXY_REQUEST_OTHER:
                        Field[] requestFields = MinutesUsageMeta.RequestStats.class.getDeclaredFields();
                        Field[] requestCodeFields = MinutesUsageMeta.CodeRequestStats.class.getDeclaredFields();
                        for (MinutesUsageMeta m : list) {
                            for (Field field : requestFields) {
                                field.setAccessible(true);
                                field.setLong(sumAllMeta.requestStats, field.getLong(sumAllMeta.requestStats) + field.getLong(m.requestStats));
                            }
                            for (Field field : requestCodeFields) {
                                field.setAccessible(true);
                                field.setLong(sumAllMeta.codeRequestStats, field.getLong(sumAllMeta.codeRequestStats) + field.getLong(m.codeRequestStats));
                            }
                        }
                        list.add(sumAllMeta);
                        break;
                    default:
                        break;
                }
            } catch (Exception e) {
                //处理计算excel和值出现问题
                log.error("process excel sum value error!! type : " + type + " ", e);
            }
        }
    }

    public enum UsageType {
        CAPACITY, // 归属地采样容量
        BILLED_STORAGE, // 计费容量
        RESTORE_STORAGE, // 数据取回量
        DELETE_STORAGE, // 数据删除量
        REDUNDANTSIZE, //冗余容量
        ALIGNSIZE, //裸容量
        ORIGINALTOTALSIZE, //应属地采样容量
        MAX_CAPACITY, //归属地峰值容量
        MAX_ORIGINALPEAKSIZE, //应属地峰值容量
        AVG_CAPACITY, //平均容量
        AVG_REDUNDANTSIZE, //平均冗余容量
        AVG_ALIGNSIZE, //平均裸容量
        AVG_ORIGINALTOTALSIZE, //应属地平均容量
        FLOW, // 流量
        REQUEST,// 请求次数
        BANDWIDTH, // 已用带宽
        AVAIL_BANDWIDTH, // 可用带宽
        CONNECTIONS, // 连接数
        PEAK_BANDWIDTH_95, //带宽95峰值

        /* 自服务门户用量下载 */
        PROXY_CAPACITY, //容量字段，包括totalsize、avgtotalsize、peaksize
        PROXY_BILLED_STORAGE, // 计费容量
        PROXY_RESTORE_STORAGE, // 数据取回量
        PROXY_DELETE_STORAGE, // 数据删除量
        PROXY_FLOW, //流量
        PROXY_REQUEST_ALL, //请求次数
        PROXY_REQUEST_GET, //按返回码区分的请求次数
        PROXY_REQUEST_HEAD, //按返回码区分的请求次数
        PROXY_REQUEST_PUT, //按返回码区分的请求次数
        PROXY_REQUEST_POST, //按返回码区分的请求次数
        PROXY_REQUEST_DELETE, //按返回码区分的请求次数
        PROXY_REQUEST_OTHER, //按返回码区分的请求次数
        PROXY_CONNECTIONS, //并发连接数
        PROXY_AVAIL_BANDWIDTH, //可用带宽
        PROXY_AVAILABLE_RATE, //服务可用性
        PROXY_EFFECTIVE_RATE, //有效请求
        PROXY_FORBIDDEN_RATE, //客户端授权错误
        PROXY_NOTFOUND_RATE, //客户端资源不存在错误
        PROXY_OTHERSERROR_RATE,
        ; //客户端其他错误
    }
}
