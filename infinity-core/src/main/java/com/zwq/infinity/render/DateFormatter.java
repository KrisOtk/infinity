package com.zwq.infinity.render;

import com.zwq.infinity.util.Constants;
import lombok.extern.slf4j.Slf4j;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import java.util.Map;
import java.util.regex.Matcher;

@Slf4j
public class DateFormatter implements TemplateRender {
    private static final DateTimeFormatter ISOformatter = ISODateTimeFormat.dateTimeParser().withOffsetParsed();

    private final DateTimeZone timeZone;
    private final String format;

    DateFormatter(String format, String timezone) {
        this.format = format;
        this.timeZone = DateTimeZone.forID(timezone);
    }

    @Override
    public Object render(Map<String, Object> event) {
        Matcher matcher = Constants.INDEX_PATTERN.matcher(this.format);//test-%{+YYYY.MM.dd}
        StringBuffer stringBuffer = new StringBuffer();
        while (matcher.find()) {
            String match = matcher.group();//%{+YYYY.MM.dd}
            String key = match.substring(2, match.length() - 1);//+YYYY.MM.dd
            String replacement = getReplacementString(event, key);
            matcher.appendReplacement(stringBuffer, replacement);
        }
        matcher.appendTail(stringBuffer);
        return stringBuffer.toString();
    }

    private String getReplacementString(Map<String, Object> event, String key) {
        String replacement = null;
        if (key.startsWith("+")) {
            DateTimeFormatter formatter = DateTimeFormat.forPattern(key.substring(1)).withZone(this.timeZone);
            Object obj = event.get("@timestamp");
            if (obj != null) {
                if (obj instanceof DateTime) {
                    replacement = ((DateTime) obj).toString(formatter);
                } else if (obj instanceof Long) {
                    DateTime timestamp = new DateTime(obj);
                    replacement = timestamp.toString(formatter);
                } else if (obj instanceof String) {
                    DateTime timestamp = ISOformatter.parseDateTime((String) obj);
                    replacement = timestamp.toString(formatter);
                }
            } else {
                replacement = new DateTime().toString(formatter);
            }
        } else if ("+s".equalsIgnoreCase(key)) {
            Object obj = event.get("@timestamp");
            if (obj instanceof Long) {
                replacement = obj.toString();
            }
        } else if (event.containsKey(key)) {
            replacement = (String) event.get(key);
        }
        return replacement;
    }
}
