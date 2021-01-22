package cn.ctyun.oos.server.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Set;

import org.apache.commons.lang.time.DateUtils;

public class DateParser {
    private final static SimpleDateFormat format = new SimpleDateFormat(
            "yyyy-MM-dd", Locale.ENGLISH);

    public static Set<String> parseDate(String dateBegin, String dateEnd) {
        LinkedHashSet<String> set = new LinkedHashSet<String>();
        Date from = null;
        Date end = null;
        synchronized (format) {
            try {
                from = format.parse(dateBegin);
                end = format.parse(dateEnd);
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }
            while (!from.after(end)) {
                set.add(format.format(from));
                from = DateUtils.addDays(from, 1);
            }
        }
        return set;
    }

    public static Date parseDate(String date) throws ParseException {
        synchronized (format) {
            return format.parse(date);
        }
    }
}
