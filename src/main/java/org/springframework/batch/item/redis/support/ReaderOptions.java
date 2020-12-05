package org.springframework.batch.item.redis.support;

import lombok.Builder;
import lombok.Builder.Default;
import lombok.Data;

@Data
@Builder
public class ReaderOptions {

	@Default
	private TransferOptions transferOptions = TransferOptions.builder().build();
	@Default
	private QueueOptions queueOptions = QueueOptions.builder().build();
	@Default
	private KeyReaderOptions keyReaderOptions = KeyReaderOptions.builder().build();

}
