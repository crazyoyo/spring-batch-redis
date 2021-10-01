package org.springframework.batch.item.redis;

import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.Utils;
import com.redis.lettucemod.api.search.IndexInfo;
import com.redis.testcontainers.RedisModulesContainer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.batch.item.redis.test.Beer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
public class BeerTests {

    @Container
    private final static RedisModulesContainer REDIS = new RedisModulesContainer();

    @Test
    public void testBeerIndex() throws Throwable {
        RedisModulesClient client = RedisModulesClient.create(REDIS.getRedisURI());
        Beer.createIndex(client);
        IndexInfo indexInfo = Utils.indexInfo(client.connect().sync().indexInfo(Beer.INDEX));
        Assertions.assertEquals(18, indexInfo.getNumDocs());
    }
}
