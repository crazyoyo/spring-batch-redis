package org.springframework.batch.item.redis.support;

public interface TransferTaskListener {

    void onUpdate(long count);

}
