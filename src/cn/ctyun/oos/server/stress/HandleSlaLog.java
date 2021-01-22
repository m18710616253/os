package cn.ctyun.oos.server.stress;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class HandleSlaLog {
    
    static String[] opts = { "PutObject", "PutObject_num",
        "PutObjectError", "PutObject_0.3", "PutObject_0.5",
        "PutObject_1.0", "PutObject_1.5", "PutObject_2.0",
        "PutObject_5.0", "PutObject_10.0", "PutObject_11", "GetObject",
        "GetObject_num", "GetObjectError", "GetObject_0.3",
        "GetObject_0.5", "GetObject_1.0", "GetObject_1.5",
        "GetObject_2.0", "GetObject_5.0", "GetObject_10.0",
        "GetObject_11", "DeleteObject", "DeleteObject_num",
        "DeleteObjectError", "DeleteObject_0.3", "DeleteObject_0.5",
        "DeleteObject_1.0", "DeleteObject_1.5", "DeleteObject_2.0",
        "DeleteObject_5.0", "DeleteObject_10.0", "DeleteObject_11" };
    static DateFormat yyyyMMdd = new SimpleDateFormat("yyyy-MM-dd HH",
            Locale.ENGLISH);
    public HandleSlaLog() {
    }
    public static String handleLog(String fileName, long time, String[] hosts, String which, int objectSize) throws Exception {
        if(fileName == null || time <= 0 || hosts == null || hosts.length == 0 || which == null || objectSize <= 0)
            throw new RuntimeException("Illigel argument");
            String line;
            String[] parts;
            // 数量
            Map<String, Map<String, AtomicLong>> sum = new HashMap<String, Map<String, AtomicLong>>();
            // 值
            Map<String, Map<String, AtomicLong>> sum2 = new HashMap<String, Map<String, AtomicLong>>();
            // 数量
            Map<String, AtomicLong> total = new HashMap<String, AtomicLong>();
            // 值
            Map<String, AtomicLong> total2 = new HashMap<String, AtomicLong>();
            for (String hp : hosts) {
                HashMap<String, AtomicLong> map = new HashMap<String, AtomicLong>();
                for (String opt : opts) {
                    map.put(opt, new AtomicLong(0));
                }
                sum.put(hp, map);
            }
            for (String hp : hosts) {
                HashMap<String, AtomicLong> map = new HashMap<String, AtomicLong>();
                map.put("PutObject", new AtomicLong(0));
                map.put("GetObject", new AtomicLong(0));
                map.put("DeleteObject", new AtomicLong(0));
                sum2.put(hp, map);
            }
            for (String opt : opts) {
                total.put(opt, new AtomicLong(0));
                total2.put(opt, new AtomicLong(0));
            }
            BufferedReader br = new BufferedReader(new InputStreamReader(
                    new FileInputStream(fileName)));
            try {
                while ((line = br.readLine()) != null) {
                    parts = line.split(" ", -1);
                    String opt = parts[0];
                    String k = null;
                    long v = 0;
                    if (parts.length == 3) {
                        k = parts[2];
                        sum.get(k).get(opt).incrementAndGet();
                        // sum.get(k).get(opt.substring(0,
                        // opt.indexOf("Error")) +
                        // "_num").incrementAndGet();
                        total.get(opt).incrementAndGet();
                        // total.get(opt.substring(0, opt.indexOf("Error"))
                        // + "_num").incrementAndGet();
                        continue;
                    } else {
                        if (parts.length != 6)
                            continue;
                        k = parts[2];
                        v = Long.parseLong(parts[4]);
                    }

                    sum2.get(k).get(opt).addAndGet(v);
                    total2.get(opt).addAndGet(v);

                    if (v < 150) {
                        sum.get(k).get(opt + "_0.3").incrementAndGet();
                        total.get(opt + "_0.3").incrementAndGet();
                    }
                    if (v < 250) {
                        sum.get(k).get(opt + "_0.5").incrementAndGet();
                        total.get(opt + "_0.5").incrementAndGet();
                    }
                    if (v < 300) {
                        sum.get(k).get(opt + "_1.0").incrementAndGet();
                        total.get(opt + "_1.0").incrementAndGet();
                    }
                    if (v < 500) {
                        sum.get(k).get(opt + "_1.5").incrementAndGet();
                        total.get(opt + "_1.5").incrementAndGet();
                    }
                    if (v < 1000) {
                        sum.get(k).get(opt + "_2.0").incrementAndGet();
                        total.get(opt + "_2.0").incrementAndGet();
                    }
                    if (v < 1500) {
                        sum.get(k).get(opt + "_5.0").incrementAndGet();
                        total.get(opt + "_5.0").incrementAndGet();
                    }
                    if (v < 2000) {
                        sum.get(k).get(opt + "_10.0").incrementAndGet();
                        total.get(opt + "_10.0").incrementAndGet();
                    } else {
                        sum.get(k).get(opt + "_11").incrementAndGet();
                        total.get(opt + "_11").incrementAndGet();
                    }
                    sum.get(k).get(opt + "_num").incrementAndGet();
                    total.get(opt + "_num").incrementAndGet();
                }
            } finally {
                br.close();
            }

            DecimalFormat df1 = new DecimalFormat("00.000000");
            DecimalFormat df2 = new DecimalFormat("00.000000");
            StringBuilder body = new StringBuilder("邮件发送时间：");
            body.append(yyyyMMdd.format(new Date()) + "时").append("</br>");
            body.append("计时规则：")
                    .append("put操作计时是从发起请求到接收到响应的时间；get操作计时是从发出请求到接收完所有数据的时间；delete操作计时是从发出请求到接收到响应的时间。")
                    .append("</br>");
            body.append(objectSize / 1024 / 1024)
                    .append("MB文件，在过去的");
            body.append(time / 3600000).append("小时")
                    .append(time % 3600000 / 60000).append("分内，所有")
                    .append(which)
                    .append("节点put、get、delete操作的性能统计分析：").append("<br/>");
            body.append(hosts.length + "个")
                    .append(which).append("节点的整体性能分析：")
                    .append("<br/>");
            body.append("<table  border=\"1\">").append("<tr>")
                    .append("<td width=\"97\">&nbsp;</td>")
                    .append("<td width=\"64\">总次数</td>")
                    .append("<td width=\"64\">失败率</td>")
                    .append("<td width=\"64\">平均时间</td>")
                    .append("<td width=\"64\">&lt;150ms</td>")
                    .append("<td width=\"64\">&lt;250ms</td>")
                    .append("<td width=\"64\">&lt;300ms</td>")
                    .append("<td width=\"64\">&lt;500ms</td>")
                    .append("<td width=\"64\">&lt;1.0s</td>")
                    .append("<td width=\"64\">&lt;1.5s</td>")
                    .append("<td width=\"65\">&lt;2.0s</td>")
                    .append("<td width=\"64\">&gt;=2.0s</td>")
                    .append("</tr>");
            body.append("<tr>")
                    .append("<td>Put Object</td>")
                    .append("<td>")
                    .append(total.get("PutObject_num").get()
                            + total.get("PutObjectError").get())
                    .append("</td>")
                    .append("<td>")
                    .append(df2.format((double) total.get("PutObjectError")
                            .get()
                            / (total.get("PutObject_num").get() + total
                                    .get("PutObjectError").get()) * 100))
                    .append("%</td>")
                    .append("<td>");
                    if(total.get("PutObject_num").get() == 0)
                        body.append("N/A");
                    else
                        body.append(total2.get("PutObject").get()
                            / total.get("PutObject_num").get());
                    body.append("ms")
                    .append("</td>")
                    .append("<td>");
                    if(total.get("PutObject_num").get() == 0)
                        body.append("N/A");
                    else
                        body.append(df1
                            .format((double) total.get("PutObject_0.3")
                                    .get()
                                    / total.get("PutObject_num").get()
                                    * 100));
                    body.append("%</td>")
                    .append("<td>");
                    if(total.get("PutObject_num").get() == 0)
                        body.append("N/A");
                    else
                        body.append(df1.format((double) (total.get("PutObject_0.5")
                            .get())
                            / total.get("PutObject_num").get()
                            * 100));
                    body.append("%</td>")
                    .append("<td>");
                    if(total.get("PutObject_num").get() == 0)
                        body.append("N/A");
                    else
                        body.append(df1.format((double) (total.get("PutObject_1.0")
                            .get())
                            / total.get("PutObject_num").get()
                            * 100));
                    body.append("%</td>")
                    .append("<td>");
                    if(total.get("PutObject_num").get() == 0)
                        body.append("N/A");
                    else
                        body.append(df1.format((double) (total.get("PutObject_1.5")
                            .get())
                            / total.get("PutObject_num").get()
                            * 100));
                    body.append("%</td>")
                    .append("<td>");
                    if(total.get("PutObject_num").get() == 0)
                        body.append("N/A");
                    else
                        body.append(df1.format((double) (total.get("PutObject_2.0")
                            .get())
                            / total.get("PutObject_num").get()
                            * 100));
                    body.append("%</td>")
                    .append("<td>");
                    if(total.get("PutObject_num").get() == 0)
                        body.append("N/A");
                    else
                        body.append(df1.format((double) (total.get("PutObject_5.0")
                            .get())
                            / total.get("PutObject_num").get()
                            * 100));
                    body.append("%</td>")
                    .append("<td>");
                    if(total.get("PutObject_num").get() == 0)
                        body.append("N/A");
                    else
                        body.append(df1.format((double) (total
                            .get("PutObject_10.0").get())
                            / total.get("PutObject_num").get() * 100));
                    body.append("%</td>")
                    .append("<td>");
                    if(total.get("PutObject_num").get() == 0)
                        body.append("N/A");
                    else
                        body.append(df1.format((double) (total.get("PutObject_11")
                            .get())
                            / total.get("PutObject_num").get()
                            * 100));
                    body.append("%</td>").append("</tr>");

            body.append("<tr>")
                    .append("<td>Get Object</td>")
                    .append("<td>")
                    .append(total.get("GetObject_num").get()
                            + total.get("GetObjectError").get())
                    .append("</td>")
                    .append("<td>")
                    .append(df2.format((double) total.get("GetObjectError")
                            .get()
                            / (total.get("GetObject_num").get() + total
                                    .get("GetObjectError").get()) * 100))
                    .append("%</td>")
                    .append("<td>");
                    if(total.get("GetObject_num").get() == 0)
                        body.append("N/A");
                    else
                        body.append(total2.get("GetObject").get()
                            / total.get("GetObject_num").get());
                    body.append("ms")
                    .append("</td>")
                    .append("<td>");
                    if(total.get("GetObject_num").get() == 0)
                        body.append("N/A");
                    else
                        body.append(df1
                            .format((double) total.get("GetObject_0.3")
                                    .get()
                                    / total.get("GetObject_num").get()
                                    * 100));
                    body.append("%</td>")
                    .append("<td>");
                    if(total.get("GetObject_num").get() == 0)
                        body.append("N/A");
                    else
                        body.append(df1.format((double) (total.get("GetObject_0.5")
                            .get())
                            / total.get("GetObject_num").get()
                            * 100));
                    body.append("%</td>")
                    .append("<td>");
                    if(total.get("GetObject_num").get() == 0)
                        body.append("N/A");
                    else
                        body.append(df1.format((double) (total.get("GetObject_1.0")
                            .get())
                            / total.get("GetObject_num").get()
                            * 100));
                    body.append("%</td>")
                    .append("<td>");
                    if(total.get("GetObject_num").get() == 0)
                        body.append("N/A");
                    else
                        body.append(df1.format((double) (total.get("GetObject_1.5")
                            .get())
                            / total.get("GetObject_num").get()
                            * 100));
                    body.append("%</td>")
                    .append("<td>");
                    if(total.get("GetObject_num").get() == 0)
                        body.append("N/A");
                    else
                        body.append(df1.format((double) (total.get("GetObject_2.0")
                            .get())
                            / total.get("GetObject_num").get()
                            * 100));
                    body.append("%</td>")
                    .append("<td>");
                    if(total.get("GetObject_num").get() == 0)
                        body.append("N/A");
                    else
                        body.append(df1.format((double) (total.get("GetObject_5.0")
                            .get())
                            / total.get("GetObject_num").get()
                            * 100));
                    body.append("%</td>")
                    .append("<td>");
                    if(total.get("GetObject_num").get() == 0)
                        body.append("N/A");
                    else
                        body.append(df1.format((double) (total
                            .get("GetObject_10.0").get())
                            / total.get("GetObject_num").get() * 100));
                    body.append("%</td>")
                    .append("<td>");
                    if(total.get("GetObject_num").get() == 0)
                        body.append("N/A");
                    else
                        body.append(df1.format((double) (total.get("GetObject_11")
                            .get())
                            / total.get("GetObject_num").get()
                            * 100));
                    body.append("%</td>").append("</tr>");

            body.append("<tr>")
                    .append("<td>Delete Object</td>")
                    .append("<td>")
                    .append(total.get("DeleteObject_num").get()
                            + total.get("DeleteObjectError").get())
                    .append("</td>")
                    .append("<td>")
                    .append(df2.format((double) total.get(
                            "DeleteObjectError").get()
                            / (total.get("DeleteObject_num").get() + total
                                    .get("DeleteObjectError").get()) * 100))
                    .append("%</td>")
                    .append("<td>");
                    if(total.get("DeleteObject_num").get() == 0)
                        body.append("N/A");
                    else
                        body.append(total2.get("DeleteObject").get()
                            / total.get("DeleteObject_num").get());
                    body.append("ms")
                    .append("</td>")
                    .append("<td>");
                    if(total.get("DeleteObject_num").get() == 0)
                        body.append("N/A");
                    else
                        body.append(df1.format((double) total.get(
                            "DeleteObject_0.3").get()
                            / total.get("DeleteObject_num").get() * 100));
                    body.append("%</td>")
                    .append("<td>");
                    if(total.get("DeleteObject_num").get() == 0)
                        body.append("N/A");
                    else
                        body.append(df1.format((double) (total
                            .get("DeleteObject_0.5").get())
                            / total.get("DeleteObject_num").get() * 100));
                    body.append("%</td>")
                    .append("<td>");
                    if(total.get("DeleteObject_num").get() == 0)
                        body.append("N/A");
                    else
                        body.append(df1.format((double) (total
                            .get("DeleteObject_1.0").get())
                            / total.get("DeleteObject_num").get() * 100));
                    body.append("%</td>")
                    .append("<td>");
                    if(total.get("DeleteObject_num").get() == 0)
                        body.append("N/A");
                    else
                        body.append(df1.format((double) (total
                            .get("DeleteObject_1.5").get())
                            / total.get("DeleteObject_num").get() * 100));
                    body.append("%</td>")
                    .append("<td>");
                    if(total.get("DeleteObject_num").get() == 0)
                        body.append("N/A");
                    else
                        body.append(df1.format((double) (total
                            .get("DeleteObject_2.0").get())
                            / total.get("DeleteObject_num").get() * 100));
                    body.append("%</td>")
                    .append("<td>");
                    if(total.get("DeleteObject_num").get() == 0)
                        body.append("N/A");
                    else
                        body.append(df1.format((double) (total
                            .get("DeleteObject_5.0").get())
                            / total.get("DeleteObject_num").get() * 100));
                    body.append("%</td>")
                    .append("<td>");
                    if(total.get("DeleteObject_num").get() == 0)
                        body.append("N/A");
                    else
                        body.append(df1.format((double) (total
                            .get("DeleteObject_10.0").get())
                            / total.get("DeleteObject_num").get() * 100));
                    body.append("%</td>")
                    .append("<td>")
                    .append(df1.format((double) (total
                            .get("DeleteObject_11").get())
                            / total.get("DeleteObject_num").get() * 100));
                    body.append("%</td>").append("</tr>");
            body.append("</table>");

            for (String hp : hosts) {
                total = sum.get(hp);
                total2 = sum2.get(hp);
                body.append("</tr>").append(which).append("节点：")
                        .append(hp).append("的SLA").append("</tr>");
                body.append("<table  border=\"1\">").append("<tr>")
                        .append("<td width=\"97\">&nbsp;</td>")
                        .append("<td width=\"64\">总次数</td>")
                        .append("<td width=\"64\">失败率</td>")
                        .append("<td width=\"64\">平均时间</td>")
                        .append("<td width=\"64\">&lt;150ms</td>")
                        .append("<td width=\"64\">&lt;250ms</td>")
                        .append("<td width=\"64\">&lt;300ms</td>")
                        .append("<td width=\"64\">&lt;500ms</td>")
                        .append("<td width=\"64\">&lt;1.0s</td>")
                        .append("<td width=\"64\">&lt;1.5s</td>")
                        .append("<td width=\"65\">&lt;2.0s</td>")
                        .append("<td width=\"64\">&gt;=2.0s</td>")
                        .append("</tr>");
                body.append("<tr>")
                .append("<td>Put Object</td>")
                .append("<td>")
                .append(total.get("PutObject_num").get()
                        + total.get("PutObjectError").get())
                .append("</td>")
                .append("<td>")
                .append(df2.format((double) total.get("PutObjectError")
                        .get()
                        / (total.get("PutObject_num").get() + total
                                .get("PutObjectError").get()) * 100))
                .append("%</td>")
                .append("<td>");
                if(total.get("PutObject_num").get() == 0)
                    body.append("N/A");
                else
                    body.append(total2.get("PutObject").get()
                        / total.get("PutObject_num").get());
                body.append("ms")
                .append("</td>")
                .append("<td>");
                if(total.get("PutObject_num").get() == 0)
                    body.append("N/A");
                else
                    body.append(df1
                        .format((double) total.get("PutObject_0.3")
                                .get()
                                / total.get("PutObject_num").get()
                                * 100));
                body.append("%</td>")
                .append("<td>");
                if(total.get("PutObject_num").get() == 0)
                    body.append("N/A");
                else
                    body.append(df1.format((double) (total.get("PutObject_0.5")
                        .get())
                        / total.get("PutObject_num").get()
                        * 100));
                body.append("%</td>")
                .append("<td>");
                if(total.get("PutObject_num").get() == 0)
                    body.append("N/A");
                else
                    body.append(df1.format((double) (total.get("PutObject_1.0")
                        .get())
                        / total.get("PutObject_num").get()
                        * 100));
                body.append("%</td>")
                .append("<td>");
                if(total.get("PutObject_num").get() == 0)
                    body.append("N/A");
                else
                    body.append(df1.format((double) (total.get("PutObject_1.5")
                        .get())
                        / total.get("PutObject_num").get()
                        * 100));
                body.append("%</td>")
                .append("<td>");
                if(total.get("PutObject_num").get() == 0)
                    body.append("N/A");
                else
                    body.append(df1.format((double) (total.get("PutObject_2.0")
                        .get())
                        / total.get("PutObject_num").get()
                        * 100));
                body.append("%</td>")
                .append("<td>");
                if(total.get("PutObject_num").get() == 0)
                    body.append("N/A");
                else
                    body.append(df1.format((double) (total.get("PutObject_5.0")
                        .get())
                        / total.get("PutObject_num").get()
                        * 100));
                body.append("%</td>")
                .append("<td>");
                if(total.get("PutObject_num").get() == 0)
                    body.append("N/A");
                else
                    body.append(df1.format((double) (total
                        .get("PutObject_10.0").get())
                        / total.get("PutObject_num").get() * 100));
                body.append("%</td>")
                .append("<td>");
                if(total.get("PutObject_num").get() == 0)
                    body.append("N/A");
                else
                    body.append(df1.format((double) (total.get("PutObject_11")
                        .get())
                        / total.get("PutObject_num").get()
                        * 100));
                body.append("%</td>").append("</tr>");

                body.append("<tr>")
                .append("<td>Get Object</td>")
                .append("<td>")
                .append(total.get("GetObject_num").get()
                        + total.get("GetObjectError").get())
                .append("</td>")
                .append("<td>")
                .append(df2.format((double) total.get("GetObjectError")
                        .get()
                        / (total.get("GetObject_num").get() + total
                                .get("GetObjectError").get()) * 100))
                .append("%</td>")
                .append("<td>");
                if(total.get("GetObject_num").get() == 0)
                    body.append("N/A");
                else
                    body.append(total2.get("GetObject").get()
                        / total.get("GetObject_num").get());
                body.append("ms")
                .append("</td>")
                .append("<td>");
                if(total.get("GetObject_num").get() == 0)
                    body.append("N/A");
                else
                    body.append(df1
                        .format((double) total.get("GetObject_0.3")
                                .get()
                                / total.get("GetObject_num").get()
                                * 100));
                body.append("%</td>")
                .append("<td>");
                if(total.get("GetObject_num").get() == 0)
                    body.append("N/A");
                else
                    body.append(df1.format((double) (total.get("GetObject_0.5")
                        .get())
                        / total.get("GetObject_num").get()
                        * 100));
                body.append("%</td>")
                .append("<td>");
                if(total.get("GetObject_num").get() == 0)
                    body.append("N/A");
                else
                    body.append(df1.format((double) (total.get("GetObject_1.0")
                        .get())
                        / total.get("GetObject_num").get()
                        * 100));
                body.append("%</td>")
                .append("<td>");
                if(total.get("GetObject_num").get() == 0)
                    body.append("N/A");
                else
                    body.append(df1.format((double) (total.get("GetObject_1.5")
                        .get())
                        / total.get("GetObject_num").get()
                        * 100));
                body.append("%</td>")
                .append("<td>");
                if(total.get("GetObject_num").get() == 0)
                    body.append("N/A");
                else
                    body.append(df1.format((double) (total.get("GetObject_2.0")
                        .get())
                        / total.get("GetObject_num").get()
                        * 100));
                body.append("%</td>")
                .append("<td>");
                if(total.get("GetObject_num").get() == 0)
                    body.append("N/A");
                else
                    body.append(df1.format((double) (total.get("GetObject_5.0")
                        .get())
                        / total.get("GetObject_num").get()
                        * 100));
                body.append("%</td>")
                .append("<td>");
                if(total.get("GetObject_num").get() == 0)
                    body.append("N/A");
                else
                    body.append(df1.format((double) (total
                        .get("GetObject_10.0").get())
                        / total.get("GetObject_num").get() * 100));
                body.append("%</td>")
                .append("<td>");
                if(total.get("GetObject_num").get() == 0)
                    body.append("N/A");
                else
                    body.append(df1.format((double) (total.get("GetObject_11")
                        .get())
                        / total.get("GetObject_num").get()
                        * 100));
                body.append("%</td>").append("</tr>");

                body.append("<tr>")
                .append("<td>Delete Object</td>")
                .append("<td>")
                .append(total.get("DeleteObject_num").get()
                        + total.get("DeleteObjectError").get())
                .append("</td>")
                .append("<td>")
                .append(df2.format((double) total.get(
                        "DeleteObjectError").get()
                        / (total.get("DeleteObject_num").get() + total
                                .get("DeleteObjectError").get()) * 100))
                .append("%</td>")
                .append("<td>");
                if(total.get("DeleteObject_num").get() == 0)
                    body.append("N/A");
                else
                    body.append(total2.get("DeleteObject").get()
                        / total.get("DeleteObject_num").get());
                body.append("ms")
                .append("</td>")
                .append("<td>");
                if(total.get("DeleteObject_num").get() == 0)
                    body.append("N/A");
                else
                    body.append(df1.format((double) total.get(
                        "DeleteObject_0.3").get()
                        / total.get("DeleteObject_num").get() * 100));
                body.append("%</td>")
                .append("<td>");
                if(total.get("DeleteObject_num").get() == 0)
                    body.append("N/A");
                else
                    body.append(df1.format((double) (total
                        .get("DeleteObject_0.5").get())
                        / total.get("DeleteObject_num").get() * 100));
                body.append("%</td>")
                .append("<td>");
                if(total.get("DeleteObject_num").get() == 0)
                    body.append("N/A");
                else
                    body.append(df1.format((double) (total
                        .get("DeleteObject_1.0").get())
                        / total.get("DeleteObject_num").get() * 100));
                body.append("%</td>")
                .append("<td>");
                if(total.get("DeleteObject_num").get() == 0)
                    body.append("N/A");
                else
                    body.append(df1.format((double) (total
                        .get("DeleteObject_1.5").get())
                        / total.get("DeleteObject_num").get() * 100));
                body.append("%</td>")
                .append("<td>");
                if(total.get("DeleteObject_num").get() == 0)
                    body.append("N/A");
                else
                    body.append(df1.format((double) (total
                        .get("DeleteObject_2.0").get())
                        / total.get("DeleteObject_num").get() * 100));
                body.append("%</td>")
                .append("<td>");
                if(total.get("DeleteObject_num").get() == 0)
                    body.append("N/A");
                else
                    body.append(df1.format((double) (total
                        .get("DeleteObject_5.0").get())
                        / total.get("DeleteObject_num").get() * 100));
                body.append("%</td>")
                .append("<td>");
                if(total.get("DeleteObject_num").get() == 0)
                    body.append("N/A");
                else
                    body.append(df1.format((double) (total
                        .get("DeleteObject_10.0").get())
                        / total.get("DeleteObject_num").get() * 100));
                body.append("%</td>")
                .append("<td>")
                .append(df1.format((double) (total
                        .get("DeleteObject_11").get())
                        / total.get("DeleteObject_num").get() * 100));
                body.append("%</td>").append("</tr>");
                body.append("</table>");
            }
            body.append("</br>")
                    .append("注：所有的测试都是在没有错误重试的情况下进行的，错误率=错误次数/操作总次数；平均时间=成功操作总时间/成功操作总次数；<300ms的比率=（<300ms的总次数，其中不包含失败的操作）/（成功操作的总次数）；<500ms，<1.0s等等的计算方式与<300ms的计算方式相同。");
            return body.toString();

    }
    
}