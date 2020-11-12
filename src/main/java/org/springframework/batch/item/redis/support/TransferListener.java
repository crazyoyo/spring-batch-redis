package org.springframework.batch.item.redis.support;

public interface TransferListener {

    void onUpdate(long count);

}
