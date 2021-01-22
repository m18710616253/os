package cn.ctyun.oos.website;

import cn.ctyun.common.Consts;
import cn.ctyun.common.conf.GlobalHHZConfig;
import cn.ctyun.oos.common.AuthResult;
import cn.ctyun.oos.common.Utils;
import cn.ctyun.oos.hbase.HBaseConnectionManager;
import cn.ctyun.oos.hbase.HBaseMinutesUsage;
import cn.ctyun.oos.hbase.MetaClient;
import cn.ctyun.oos.metadata.AkSkMeta;
import cn.ctyun.oos.metadata.MinutesUsageMeta;
import cn.ctyun.oos.metadata.OwnerMeta;
import cn.ctyun.oos.server.usage.UsageResult;
import cn.ctyun.oos.server.util.CSVUtils;
import cn.ctyun.oos.server.util.Misc;
import com.google.common.collect.Sets;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;

import java.lang.reflect.Field;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

/**
 * 自测使用，不要上传！！！
 *
 * @author Mirt Zhang
 * @date 2020/6/4.
 */
public class SelfTestMain {

    private static final Log log = LogFactory.getLog(SelfTestMain.class);

    private final static MetaClient CLIENT = MetaClient.getGlobalClient();

    private final static long userId = 1123; // 统一使用的userId
    private final static String bucketName = "mirt-demo";
    private final static AuthResult authResult = new AuthResult();

//    private final static MockHttpServletRequest req = new MockHttpServletRequest();

    private final static Random RANDOM = new Random(userId);

    private final static String beginTime = "2020-11-025";
    private final static String endTime = "2020-11-30";

    // 为前端创建假数据使用
    private final static long ownerId = 1193023600980285571L;
    private final static String fe_bucketName = "lllope";


    static {
        authResult.owner = new OwnerMeta();
        authResult.owner.name = "mirt";
        authResult.owner.setId(userId);

        authResult.accessKey = new AkSkMeta();
        authResult.accessKey.userName = "mirt";

//        req.setupAddParameter("Bucket", "");
//        req.setupAddParameter("Region", "huadong");
//        req.setupAddParameter("Freq", "");
//        req.setupAddParameter("StorageClass", "all");
//
//        req.setupAddParameter("InternetType", "");
//        req.setupAddParameter("RequestsType", "");
//        req.setupAddParameter("ResponseType", "");
//
//        req.setupAddParameter("InOutType", "");
//        req.setupAddParameter("TrafficsType", "");
    }

    public static void main(String[] args) throws Exception {

        System.out.println("========== START ==========");

        System.out.println(UsageResult.getSingleUsageInCsv(userId,beginTime, endTime, null, Utils.BY_HOUR, CSVUtils.UsageType.PROXY_FLOW, Sets.newHashSet("huadong"), Consts.STORAGE_CLASS_STANDARD_IA, true, false));
        System.out.println(UsageResult.getSingleUsageInJson(userId, beginTime, endTime, "billSize,totalSize,preDeleteComplete,preChangeComplete,completeSize", null, Utils.BY_HOUR, Sets.newHashSet("huadong"), Consts.STORAGE_CLASS_STANDARD_IA, true, false));

        System.out.println(UsageResult.getRegionUsageInJson(beginTime, endTime, "billSize", Utils.BY_DAY, Consts.ALL));

        System.out.println(UsageResult.getRegionUsageInCsv(beginTime, endTime, CSVUtils.UsageType.PROXY_DELETE_STORAGE, Utils.BY_DAY, Consts.STORAGE_CLASS_STANDARD_IA, false));

        System.out.println(UsageResult.getSingleCommonUsageStatsQuery(ownerId, LocalDate.now().format(Misc.format_uuuu_MM_dd), LocalDate.now().format(Misc.format_uuuu_MM_dd), null, Utils.BY_DAY, Sets.newHashSet("huadong"), MinutesUsageMeta.UsageMetaType.DAY_OWNER, "ALl", true).get("huadong").get(0).sizeStats.toJsonString());

        System.out.println(UsageResult.getSingleUsageStatsQuery(ownerId, beginTime, endTime, null, Utils.BY_DAY, Sets.newHashSet("huadong"), "ALL", MinutesUsageMeta.UsageMetaType.DAY_OWNER, false, true).first());

        //batchInsert();
        System.out.println("========== FINISH ==========");
        System.exit(1);
    }

    private static void createTable() throws Exception {
        Configuration globalConf = GlobalHHZConfig.getConfig();
        HConnection globalConn = HBaseConnectionManager.createConnection(globalConf);
        HBaseAdmin globalHbaseAdmin = new HBaseAdmin(globalConn);
        HBaseMinutesUsage.createTable(globalHbaseAdmin);
    }

    private static void truncateTable() {

    }

    private static void batchInsert() throws Exception {

        List<MinutesUsageMeta> datas = new ArrayList<>();
        LocalDateTime dateOfEnd = LocalDateTime.of(2020, 6, 15, 0, 0, 0);
        LocalDateTime dateOfBegin = dateOfEnd.minusDays(10);

        while (dateOfBegin.isBefore(dateOfEnd)) {
            String minutesTime = dateOfBegin.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm"));
            String dayTime = dateOfBegin.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
            long timeVersion = dateOfBegin.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
            MinutesUsageMeta m1 = new MinutesUsageMeta(MinutesUsageMeta.UsageMetaType.MINUTES_OWNER, "huadong", ownerId, minutesTime, Consts.STORAGE_CLASS_STANDARD);
            m1.timeVersion = timeVersion;
            MinutesUsageMeta m2 = new MinutesUsageMeta(MinutesUsageMeta.UsageMetaType.MINUTES_OWNER, "huadong", ownerId, minutesTime, Consts.STORAGE_CLASS_STANDARD_IA);
            m2.timeVersion = timeVersion;
            datas.add(m1);
            datas.add(m2);
            MinutesUsageMeta m3 = new MinutesUsageMeta(MinutesUsageMeta.UsageMetaType.MINUTES_BUCKET, "huadong", ownerId, fe_bucketName, minutesTime, Consts.STORAGE_CLASS_STANDARD);
            m3.timeVersion = timeVersion;
            MinutesUsageMeta m4 = new MinutesUsageMeta(MinutesUsageMeta.UsageMetaType.MINUTES_BUCKET, "huadong", ownerId, fe_bucketName, minutesTime, Consts.STORAGE_CLASS_STANDARD_IA);
            m4.timeVersion = timeVersion;
            datas.add(m3);
            datas.add(m4);

            MinutesUsageMeta m5 = new MinutesUsageMeta(MinutesUsageMeta.UsageMetaType.FIVE_MINUTES, "huadong", minutesTime, Consts.STORAGE_CLASS_STANDARD);
            m5.timeVersion = timeVersion;
            MinutesUsageMeta m6 = new MinutesUsageMeta(MinutesUsageMeta.UsageMetaType.FIVE_MINUTES, "huadong", minutesTime, Consts.STORAGE_CLASS_STANDARD_IA);
            m6.timeVersion = timeVersion;
            datas.add(m5);
            datas.add(m6);

            if (dateOfBegin.getMinute() == 0) {
                // hour
                MinutesUsageMeta h1 = new MinutesUsageMeta(MinutesUsageMeta.UsageMetaType.HOUR_OWNER, "huadong", ownerId, minutesTime, Consts.STORAGE_CLASS_STANDARD);
                h1.timeVersion = timeVersion;
                MinutesUsageMeta h2 = new MinutesUsageMeta(MinutesUsageMeta.UsageMetaType.HOUR_OWNER, "huadong", ownerId, minutesTime, Consts.STORAGE_CLASS_STANDARD_IA);
                h2.timeVersion = timeVersion;
                datas.add(h1);
                datas.add(h2);
                MinutesUsageMeta h3 = new MinutesUsageMeta(MinutesUsageMeta.UsageMetaType.HOUR_BUCKET, "huadong", ownerId, fe_bucketName, minutesTime, Consts.STORAGE_CLASS_STANDARD);
                h3.timeVersion = timeVersion;
                MinutesUsageMeta h4 = new MinutesUsageMeta(MinutesUsageMeta.UsageMetaType.HOUR_BUCKET, "huadong", ownerId, fe_bucketName, minutesTime, Consts.STORAGE_CLASS_STANDARD_IA);
                h4.timeVersion = timeVersion;
                datas.add(h3);
                datas.add(h4);
            }
            if (dateOfBegin.getHour() == 0) {
                // day
                MinutesUsageMeta d1 = new MinutesUsageMeta(MinutesUsageMeta.UsageMetaType.DAY_OWNER, "huadong", ownerId, dayTime, Consts.STORAGE_CLASS_STANDARD);
                d1.timeVersion = timeVersion;
                MinutesUsageMeta d2 = new MinutesUsageMeta(MinutesUsageMeta.UsageMetaType.DAY_OWNER, "huadong", ownerId, dayTime, Consts.STORAGE_CLASS_STANDARD_IA);
                d2.timeVersion = timeVersion;
                datas.add(d1);
                datas.add(d2);
                MinutesUsageMeta d3 = new MinutesUsageMeta(MinutesUsageMeta.UsageMetaType.DAY_BUCKET, "huadong", ownerId, fe_bucketName, dayTime, Consts.STORAGE_CLASS_STANDARD);
                d3.timeVersion = timeVersion;
                MinutesUsageMeta d4 = new MinutesUsageMeta(MinutesUsageMeta.UsageMetaType.DAY_BUCKET, "huadong", ownerId, fe_bucketName, dayTime, Consts.STORAGE_CLASS_STANDARD_IA);
                d4.timeVersion = timeVersion;
                datas.add(d3);
                datas.add(d4);

                MinutesUsageMeta d5 = new MinutesUsageMeta(MinutesUsageMeta.UsageMetaType.REGION_DAY, "huadong", dayTime, Consts.STORAGE_CLASS_STANDARD);
                d5.timeVersion = timeVersion;
                MinutesUsageMeta d6 = new MinutesUsageMeta(MinutesUsageMeta.UsageMetaType.REGION_DAY, "huadong", dayTime, Consts.STORAGE_CLASS_STANDARD_IA);
                d6.timeVersion = timeVersion;
                datas.add(d5);
                datas.add(d6);

            }

            dateOfBegin = dateOfBegin.plusMinutes(5);
        }

        datas.forEach(m -> {
            m.initCommonStats();
            try {
                Field[] sizeField = m.sizeStats.getClass().getDeclaredFields();
                for (Field field : sizeField) {
                    field.setAccessible(true);
                    field.setLong(m.sizeStats, Math.abs(RANDOM.nextLong()) % 1000);
                }
                Field[] flowField = m.flowStats.getClass().getDeclaredFields();
                for (Field field : flowField) {
                    field.setAccessible(true);
                    field.setLong(m.flowStats, Math.abs(RANDOM.nextLong()) % 1000);
                }
                Field[] codeField = m.codeRequestStats.getClass().getDeclaredFields();
                for (Field field : codeField) {
                    field.setAccessible(true);
                    field.setLong(m.codeRequestStats, Math.abs(RANDOM.nextLong()) % 1000);
                }
                Field[] requestField = m.requestStats.getClass().getDeclaredFields();
                for (Field field : requestField) {
                    field.setAccessible(true);
                    field.setLong(m.requestStats, Math.abs(RANDOM.nextLong()) % 1000);
                }
            } catch (Exception e) {
                log.error("SelfTestMain.batchInsert []", e);
            }
        });

        CLIENT.minutesUsageBatchInsert(datas);
        CLIENT.minutesUsageInsertLastPeriodTime(System.currentTimeMillis());

        System.out.println("insert into hbase : " + datas.stream().map(MinutesUsageMeta::getKey).collect(Collectors.toList()));
    }
}
