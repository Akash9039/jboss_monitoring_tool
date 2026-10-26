package com.rh.monitor.util;

import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import org.jboss.logging.Logger;

public final class TimeUtils {

    private static final Logger LOG = Logger.getLogger(TimeUtils.class);

    private static final DateTimeFormatter SPY_FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss,SSS");
    private static final DateTimeFormatter SPY_FMT_TIME_ONLY = DateTimeFormatter.ofPattern("HH:mm:ss,SSS");

    private TimeUtils() {}

    public static String msToHuman(long ms) {
        long seconds = ms / 1000;
        long days = seconds / 86400;
        long hours = (seconds % 86400) / 3600;
        long minutes = (seconds % 3600) / 60;
        long secs = seconds % 60;
        List<String> parts = new ArrayList<>();
        if (days > 0) parts.add(days + "d");
        if (hours > 0) parts.add(hours + "h");
        if (minutes > 0) parts.add(minutes + "m");
        parts.add(secs + "s");
        return String.join(" ", parts);
    }

    public static double spyTimeDiff(String start, String end) {
        try {
            boolean startHasDate = start.length() > 12 && start.charAt(4) == '-';
            boolean endHasDate = end.length() > 12 && end.charAt(4) == '-';
            if (startHasDate && endHasDate) {
                LocalDateTime t1 = LocalDateTime.parse(start, SPY_FMT);
                LocalDateTime t2 = LocalDateTime.parse(end, SPY_FMT);
                return Math.round(ChronoUnit.MILLIS.between(t1, t2) * 100.0) / 100.0;
            }
            LocalTime t1 = LocalTime.parse(startHasDate ? start.substring(11) : start, SPY_FMT_TIME_ONLY);
            LocalTime t2 = LocalTime.parse(endHasDate ? end.substring(11) : end, SPY_FMT_TIME_ONLY);
            return Math.round(ChronoUnit.MILLIS.between(t1, t2) * 100.0) / 100.0;
        } catch (Exception e) {
            return 0.0;
        }
    }

    public static String nowSpyFormat() {
        return LocalDateTime.now().format(SPY_FMT_TIME_ONLY);
    }

    public static int safeInt(Object val) {
        if (val == null) return 0;
        if (val instanceof Number) return ((Number) val).intValue();
        try {
            return Integer.parseInt(val.toString());
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    public static long safeLong(Object val) {
        if (val == null) return 0;
        if (val instanceof Number) return ((Number) val).longValue();
        try {
            return Long.parseLong(val.toString());
        } catch (NumberFormatException e) {
            return 0;
        }
    }
}
