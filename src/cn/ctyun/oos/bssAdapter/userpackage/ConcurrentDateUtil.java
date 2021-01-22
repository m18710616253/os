package cn.ctyun.oos.bssAdapter.userpackage;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class ConcurrentDateUtil {

    private static ThreadLocal<DateFormat> localDateFormat = new ThreadLocal<DateFormat>() {
        @Override
        protected DateFormat initialValue() {
            return new SimpleDateFormat("yyyy-MM-dd");
        }
    };

    public static Date parse(String dateStr) throws ParseException{
        return localDateFormat.get().parse(dateStr);
    }

    public static String format(Date date) {
        return localDateFormat.get().format(date);
    }
}