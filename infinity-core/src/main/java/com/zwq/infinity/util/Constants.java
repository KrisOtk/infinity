package com.zwq.infinity.util;

import java.util.regex.Pattern;

/**
 * @author Kris
 * @date 2019-08-05 14:14
 */
public class Constants {
    public static final Pattern MULTI_LEVEL_PATTERN = Pattern.compile("\\[(\\S+?)]+");//包含子属性,eg:[user][name]
    public static final Pattern INDEX_PATTERN = Pattern.compile("%\\{.*?}");//用于匹配index,eg: test-%{+YYYY.MM.dd}
}

