package org.springframework.batch.item.redis.support;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import io.lettuce.core.api.StatefulConnection;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Data;

@Data
@Builder
public class LiveReaderOptions {

	@Default
	private TransferOptions transferOptions = TransferOptions.builder().build();
	@Default
	private QueueOptions queueOptions = QueueOptions.builder().build();
	@Default
	private LiveKeyReaderOptions liveKeyReaderOptions = LiveKeyReaderOptions.builder().build();
	@Default
	private GenericObjectPoolConfig<StatefulConnection<String, String>> poolConfig = new GenericObjectPoolConfig<>();

}
