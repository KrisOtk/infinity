package com.zwq.infinity.plugin.filters;

import com.zwq.infinity.baseplugin.BaseFilter;
import com.zwq.infinity.baseplugin.BaseMetric;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;

@Slf4j
public class Rename extends BaseFilter {
	public Rename(Map<String, Object> config) {
		super(config);
	}
	public Rename(Map<String, Object> config, List<BaseMetric> metrics) {
		super(config, metrics);
	}

	private Map<String, String> fields;

	@Override
	protected void prepare() {
		this.fields = (Map<String, String>) config.get("fields");
	}

    @Override
	protected Map<String, Object> filter(final Map<String, Object> event) {
		fields.forEach((key,value)->{
			if (event.containsKey(key)) {
				event.put(value, event.remove(key));
			}
		});
		return event;
	}
}
