package com.zwq.infinity.plugin.filters.dateparser;

import org.joda.time.DateTime;

public interface DateParser {
	DateTime parse(String input);
}
