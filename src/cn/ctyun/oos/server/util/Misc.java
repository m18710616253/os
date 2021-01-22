package cn.ctyun.oos.server.util;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.time.DateUtils;

/**
 * @author: Jiang Feng
 */
public class Misc {
    protected final static SimpleDateFormat format = new SimpleDateFormat("yyyy年MM月dd日");
    protected final static SimpleDateFormat formatstrftime = new SimpleDateFormat(
            "dd/MM/yyyy HH:mm:ss zz", Locale.ENGLISH);
    private static DateFormat gmtFormat = new SimpleDateFormat("EEE, d MMM yyyy HH:mm:ss 'GMT'",
            Locale.ENGLISH);
    private static DateFormat ZFormat = new SimpleDateFormat("EEE, d MMM yyyy HH:mm:ss Z",
            Locale.ENGLISH);
    private static DateFormat gmtZFormat = new SimpleDateFormat("EEE, d MMM yyyy HH:mm:ss 'GMT'Z",
            Locale.ENGLISH);
    private static DateFormat zFormat = new SimpleDateFormat("EEE, d MMM yyyy HH:mm:ss z",
            Locale.ENGLISH);
    private static DateFormat noEEEFormat = new SimpleDateFormat("d MMM yyyy HH:mm:ss 'GMT'",
            Locale.ENGLISH);
    // V4签名时间戳格式
    private static DateFormat ISO8601Format = new SimpleDateFormat("yyyyMMdd'T'HHmmss'Z'");
    // STS临时访问凭证时间戳
    private static SimpleDateFormat iso8601DateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
    private static TimeZone GMT = TimeZone.getTimeZone("GMT");
    private static TimeZone UTC = TimeZone.getTimeZone("UTC");
    // 统计项按月时间格式
    protected final static SimpleDateFormat formatyyyy_mm = new SimpleDateFormat("yyyy-MM");
    // 统计项按天时间格式
    protected final static SimpleDateFormat formatyyyy_mm_dd = new SimpleDateFormat("yyyy-MM-dd");
    // 统计项5分钟时间格式
    protected final static SimpleDateFormat formatyyyy_mm_dd_hh_mm = new SimpleDateFormat("yyyy-MM-dd HH:mm");
    // 连接数统计项时间格式
    protected final static SimpleDateFormat formatyyyy_mm_dd_hh_mm_ss = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    // 管理API的URL query时间参数，由于空格不能在URL里面，故使用此格式
    protected final static SimpleDateFormat formatyyyy_mm_dd_hh_mm2 = new SimpleDateFormat("yyyy-MM-dd-HH-mm");
    
    public static final DateTimeFormatter format_uuuu_MM_dd = DateTimeFormatter.ofPattern("uuuu-MM-dd");
    public static final DateTimeFormatter format_uuuu_MM_dd_HH_mm = DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm");
    public static final DateTimeFormatter format_uuuu_MM_dd_HH_mm_ss = DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss");

    public final static int FIVE_MINS = 5;
    private static final long HOUR_MILLSECONDS = 60 * 60 * 1000;
    //低频访问存储中需要用到的常量
    public final static long DAY_MILLSECONDS = 24 * HOUR_MILLSECONDS;
    public final static long THIRTY_DAY_MILLSECONDS = 30 * DAY_MILLSECONDS;
    public final static long MIN_STORAGE_IA = 64 * 1024;
    
    public static String formatYearMonthDate(Date date) {
        synchronized (format) {
            return format.format(date);
        }
    }
    
    public static String formatyyyymm(Date date) {
        synchronized (formatyyyy_mm) {
            return formatyyyy_mm.format(date);
        }
    }
    
    public static String formatyyyymmdd(Date date) {
        synchronized (formatyyyy_mm_dd) {
            return formatyyyy_mm_dd.format(date);
        }
    }
    
    public static String formatyyyymmddhhmm(Date date) {
        synchronized (formatyyyy_mm_dd_hh_mm) {
            return formatyyyy_mm_dd_hh_mm.format(date);
        }
    }
    
    public static Date formatyyyymm(String date) throws ParseException {
        synchronized (formatyyyy_mm) {
            return formatyyyy_mm.parse(date);
        }
    }
    
    public static Date formatyyyymmdd(String date) throws ParseException {
        synchronized (formatyyyy_mm_dd) {
            return formatyyyy_mm_dd.parse(date);
        }
    }
    
    public static Date formatyyyymmddhhmm(String date) throws ParseException {
        synchronized (formatyyyy_mm_dd_hh_mm) {
            return formatyyyy_mm_dd_hh_mm.parse(date);
        }
    }
    
    public static Date formatyyyymmddhhmmss(String date) throws ParseException {
        synchronized (formatyyyy_mm_dd_hh_mm_ss) {
            return formatyyyy_mm_dd_hh_mm_ss.parse(date);
        }
    }
    
    public static String formatyyyymmddhhmmss(Date date) {
        synchronized (formatyyyy_mm_dd_hh_mm_ss) {
            return formatyyyy_mm_dd_hh_mm_ss.format(date);
        }
    }
    
    public static Date formatyyyymmddhhmm2(String date) throws ParseException {
        synchronized (formatyyyy_mm_dd_hh_mm2) {
            return formatyyyy_mm_dd_hh_mm2.parse(date);
        }
    }
    
    public static String formatstrftime(Date date) {
        formatstrftime.setTimeZone(UTC);
        synchronized (formatstrftime) {
            return formatstrftime.format(date);
        }
    }
    
    public static String formatIso8601time(long time) {
        synchronized (iso8601DateFormat) {
            iso8601DateFormat.setTimeZone(UTC);
            return iso8601DateFormat.format(time);
        }
    }
    
    public static Date formatIso8601time(String time) throws ParseException {
        synchronized (iso8601DateFormat) {
            iso8601DateFormat.setTimeZone(UTC);
            return iso8601DateFormat.parse(time);
        }
    }
    
    public static String getUserIdFromAuthentication(String auth) {
        return auth.substring(auth.indexOf(' ') + 1, auth.indexOf(':'));
    }
    
    public static void validateBucketName(String bucketName, String[] domainName) {
        if (bucketName == null)
            throw new IllegalArgumentException("Bucket name cannot be null");
        if (bucketName.length() < 3 || bucketName.length() > 63)
            throw new IllegalArgumentException(
                    "Bucket name should be between 3 and 63 characters long");
        // 只能包含小写字母or数字or.or-
        // 不能是ip地址格式
        int digitNum = 0, dotNum = 0;
        if (bucketName.startsWith("3hub")) {
            for (int i = 0; i < bucketName.length(); i++) {
                char c = bucketName.charAt(i);
                if (Character.isDigit(c))
                    digitNum++;
                if (c == '.')
                    dotNum++;
            }
        } else {
            if (!bucketName.toLowerCase().equals(bucketName))
                throw new IllegalArgumentException(
                        "Bucket name should not contain uppercase characters");
            for (int i = 0; i < bucketName.length(); i++) {
                char c = bucketName.charAt(i);
                if (Character.isDigit(c))
                    digitNum++;
                if (c == '.')
                    dotNum++;
                if (!Character.isLowerCase(c) && !Character.isDigit(c) && !(c == '.')
                        && !(c == '-'))
                    throw new IllegalArgumentException("Bucket name contains illegal characters");
            }
        }
        if (digitNum != 0 && dotNum != 0 && (digitNum + dotNum) == bucketName.length())
            throw new IllegalArgumentException(
                    "Bucket names must not be formatted as an IP address");
        if (bucketName.endsWith("-") || bucketName.endsWith("."))
            throw new IllegalArgumentException("Bucket name should not end with '-' or '.'");
        if (bucketName.startsWith("-") || bucketName.startsWith("."))
            throw new IllegalArgumentException("Bucket name should not start with '-' or '.'");
        if (bucketName.contains(".."))
            throw new IllegalArgumentException(
                    "Bucket name should not contain two adjacent periods");
        if (bucketName.contains("-.") || bucketName.contains(".-"))
            throw new IllegalArgumentException(
                    "Bucket name should not contain dashes next to periods");
        for (String dn : domainName)
            if (bucketName.contains(dn))
                throw new IllegalArgumentException("Bucket names should not contain domain name");
    }
    
    public static String getMd5(String str) throws IOException, NoSuchAlgorithmException {
        MessageDigest messageDigest = MessageDigest.getInstance("MD5");
        messageDigest.reset();
        messageDigest.update(str.getBytes("UTF8"));
        final byte[] resultByte = messageDigest.digest();
        String result = new String(Hex.encodeHex(resultByte));
        return result;
    }
    
    public static Date parseDateFormat(String time) throws ParseException {
        DateFormat format;
        if (time.indexOf(",") == -1) {
            if(time.indexOf(":") == -1) {
                format = ISO8601Format;
                synchronized (format) {
                    format.setTimeZone(UTC);
                    Date d = format.parse(time);
                    return d;
                }
            } else {
                format = noEEEFormat;
                synchronized (format) {
                    format.setTimeZone(GMT);
                    Date d = format.parse(time);
                    return d;
                }
            }
            
        } else {
            String str = time.substring(26);
            TimeZone tz = TimeZone.getTimeZone(str);
            int length = str.length();
            if (length == 3)
                format = gmtFormat;
            else if (length == 5)
                format = ZFormat;
            else if (length == 8)
                format = gmtZFormat;
            else
                format = zFormat;
            synchronized (format) {
                format.setTimeZone(tz);
                Date d = format.parse(time);
                return d;
            }
        }
    }

    public static Date timeToNextMidnight(Date time) {
        Calendar calendar = Calendar.getInstance(GMT, Locale.ENGLISH);
        calendar.setTime(time);
        if (calendar.get(Calendar.HOUR_OF_DAY) >= 8 && !(calendar.get(Calendar.HOUR_OF_DAY) == 8
                && calendar.get(Calendar.MINUTE) == 0 && calendar.get(Calendar.SECOND) == 0
                && calendar.get(Calendar.MILLISECOND) == 0))
            calendar.setTime(DateUtils.addDays(time, 1));
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        return calendar.getTime();
    }
    
    /**
     * 获取最近一个小时之前的5分钟时刻,向下取整
     * 例如当前时间为2018-10-31 10:44:32，则返回2018-10-31 09:40:00
     * @param time
     * @return
     */
    public static Date getLastHourFiveMinutesDate(Date time) {
        Calendar currentTime = Calendar.getInstance();
        currentTime.setTime(time);
        int currentMin = currentTime.get(Calendar.MINUTE);
        int currentHour = currentTime.get(Calendar.HOUR_OF_DAY);
        currentMin -= currentMin % FIVE_MINS;
        currentTime.set(Calendar.HOUR_OF_DAY, currentHour - 1);
        currentTime.set(Calendar.MINUTE, currentMin);
        currentTime.set(Calendar.SECOND, 0);
        currentTime.set(Calendar.MILLISECOND, 0);
        return currentTime.getTime();
    }
    
    /**
     * 获取最近的5分钟时刻,向下取整
     * 例如当前时间为2018-10-31 10:44:32，则返回2018-10-31 10:40:00
     * @param time
     * @return
     */
    public static Date getLastFiveMinutesDate(Date time) {
        Calendar currentTime = Calendar.getInstance();
        currentTime.setTime(time);
        int currentMin = currentTime.get(Calendar.MINUTE);
        int currentHour = currentTime.get(Calendar.HOUR_OF_DAY);
        currentMin -= currentMin % FIVE_MINS;
        currentTime.set(Calendar.HOUR_OF_DAY, currentHour);
        currentTime.set(Calendar.MINUTE, currentMin);
        currentTime.set(Calendar.SECOND, 0);
        currentTime.set(Calendar.MILLISECOND, 0);
        return currentTime.getTime();
    }
    
    public static Date timeToMidnight(Date time) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(time);
        calendar.set(Calendar.HOUR_OF_DAY, 23);
        calendar.set(Calendar.MINUTE, 59);
        calendar.set(Calendar.SECOND, 59);
        return calendar.getTime();
    }

    /**
     * 获取当前整5分钟时刻，向上取整，
     * 例如当前时间为2018-10-31 10:44:32，则返回2018-10-31 10:45:00
     * @return
     */
    public static Date getCurrentFiveMinuteDateCeil() {
        return getFiveMinuteDateCeil(new Date());
    }
    
    public static Date getFiveMinuteDateCeil(Date date) {
        Calendar currentTime = Calendar.getInstance();
        currentTime.setTime(date);
        int currentMin = currentTime.get(Calendar.MINUTE);
        currentMin += FIVE_MINS - currentMin % FIVE_MINS;
        currentTime.set(Calendar.HOUR_OF_DAY, currentTime.get(Calendar.HOUR_OF_DAY));
        currentTime.set(Calendar.MINUTE, currentMin);
        currentTime.set(Calendar.SECOND, 0);
        currentTime.set(Calendar.MILLISECOND, 0);
        Date nextTime = currentTime.getTime();
        return nextTime;
    }
    
    /**
     * 获取当前整5分钟时刻，向下取整，
     * 例如当前时间为2018-10-31 10:44:32，则返回2018-10-31 10:40:00
     * @return
     */
    public static Date getCurrentFiveMinuteDateFloor() {
        Calendar currentTime = Calendar.getInstance();
        currentTime.setTime(new Date());
        int currentMin = currentTime.get(Calendar.MINUTE);
        currentMin -= currentMin % FIVE_MINS;
        currentTime.set(Calendar.HOUR_OF_DAY, currentTime.get(Calendar.HOUR_OF_DAY));
        currentTime.set(Calendar.MINUTE, currentMin);
        currentTime.set(Calendar.SECOND, 0);
        currentTime.set(Calendar.MILLISECOND, 0);
        Date nextTime = currentTime.getTime();
        return nextTime;
    }
    
    /**
     * 获取某天的第一个整5分钟时刻
     * 例如当天时间为2018-10-31，则返回2018-10-31 00:00:00
     * @return
     */
    public static Date getDayFirstFiveMinuteDate(Date date) {
        Calendar currentTime = Calendar.getInstance();
        currentTime.setTime(date);
        currentTime.set(Calendar.HOUR_OF_DAY, 0);
        currentTime.set(Calendar.MINUTE, 0);
        currentTime.set(Calendar.SECOND, 0);
        currentTime.set(Calendar.MILLISECOND, 0);
        Date nextTime = currentTime.getTime();
        return nextTime;
    }
    
    /**
     * 获取当前时刻向下取整5分钟时刻，若当前时刻为整5分钟时刻，则获取当前时刻减5分钟时刻
     * 例如当前时间为2018-10-31 10:44:32，则返回2018-10-31 10:40:00
     * 例如当前时间为2018-10-31 10:45:32，则返回2018-10-31 10:40:00
     * 例如当前时间为2018-10-31 10:46:32，则返回2018-10-31 10:45:00
     * @return
     */
    public static Date getCurrentFiveMinuteMinus5MinDateCeil() {
        Calendar currentTime = Calendar.getInstance();
        currentTime.setTime(new Date());
        int currentMin = currentTime.get(Calendar.MINUTE);
        currentMin -= currentMin % FIVE_MINS == 0 ? FIVE_MINS : currentMin % FIVE_MINS;
        currentTime.set(Calendar.HOUR_OF_DAY, currentTime.get(Calendar.HOUR_OF_DAY));
        currentTime.set(Calendar.MINUTE, currentMin);
        currentTime.set(Calendar.SECOND, 0);
        currentTime.set(Calendar.MILLISECOND, 0);
        Date nextTime = currentTime.getTime();
        return nextTime;
    }
    
    /**
     * 获取[dateBegin,dateEnd]之间的每5分钟时刻，格式为yyyy-MM-dd HH:mm
     * @param dateBegin
     * @param dateEnd
     * @return
     */
    public static List<String> getFiveMinuteDatesBetweenDays(String dateBegin, String dateEnd) {
        List<String> res = new LinkedList<String>();
        Date from = null;
        Date end = null;
        synchronized (formatyyyy_mm_dd) {
            try {
                from = formatyyyy_mm_dd.parse(dateBegin);
                end = formatyyyy_mm_dd.parse(dateEnd);
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }
            while (!from.after(end)) {
                List<String> dateList = getFiveMinuteDatesInOneDay(from);
                for (String date:dateList) {
                    res.add(date);
                }
                from = DateUtils.addDays(from, 1);
            }
        }
        return res;
    }
    
    /**
     * 获取一天的每5分钟整时刻，格式为yyyy-MM-dd HH:mm
     * @param date
     * @return
     */
    public static List<String> getFiveMinuteDatesInOneDay(Date date) {
        List<String> dateList = new ArrayList<String>();
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        Date next = calendar.getTime();
        Date nowRowDate = null;
        dateList.add(Misc.formatyyyymmddhhmm(next));
        if (DateUtils.isSameDay(new Date(), date)) {
            nowRowDate = Misc.getLastHourFiveMinutesDate(new Date());
        } else {
            nowRowDate = Misc.timeToMidnight(date);
        }       
        while (DateUtils.addMinutes(next, 5).before(nowRowDate)) {
            Date tmp = DateUtils.addMinutes(next, 5);
            String rowDate = Misc.formatyyyymmddhhmm(tmp);
            dateList.add(rowDate);
            next = tmp;
        }
        return dateList;
    }
    
    /**
     * 获取向上取整的整小时的时间
     * 如：输入11：00，返回11：00；输入11：05，返回12：00
     * @param mill
     */
    public static long getCeilHourStartTime(long mill) {
        if (mill % HOUR_MILLSECONDS == 0) {
            return mill;
        }
        return mill + (HOUR_MILLSECONDS - mill % HOUR_MILLSECONDS);
    }
    
    /**
     * 获取向下取整的整小时的时间
     * 如：输入11：00，返回10：00；输入11：05，返回11：00；输入00：00，返回前一天23：00
     * @param mill
     */
    public static long getfloorHourStartTime(long mill) {
        if (mill % HOUR_MILLSECONDS == 0) {
            return mill - HOUR_MILLSECONDS;
        }
        return mill - mill % HOUR_MILLSECONDS;
    }
}