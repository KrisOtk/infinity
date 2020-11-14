package com.zwq.infinity.decoders;

import java.util.Map;

public class PlainDecoder implements Decode {

	@Override
	public Map<String, Object> decode(final String message) {
		return createDefaultEvent(message);
	}
}
