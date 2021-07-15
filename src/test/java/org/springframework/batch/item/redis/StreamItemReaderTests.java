package org.springframework.batch.item.redis;

import com.redislabs.testcontainers.RedisClusterContainer;
import com.redislabs.testcontainers.RedisContainer;
import com.redislabs.testcontainers.RedisServer;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XReadArgs;
import io.lettuce.core.api.sync.RedisStreamCommands;
import io.lettuce.core.models.stream.PendingMessages;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.redis.support.DataStructure;
import org.springframework.batch.item.support.ListItemWriter;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.List;
import java.util.stream.Stream;

@Testcontainers
public class StreamItemReaderTests extends AbstractSpringBatchRedisTests {

    @Container
    private static final RedisContainer REDIS = new RedisContainer();

    @Container
    private static final RedisClusterContainer REDIS_CLUSTER = new RedisClusterContainer();

    @BeforeAll
    public static void setup() {
        add(REDIS, REDIS_CLUSTER);
    }

    static Stream<RedisServer> servers() {
        return Stream.of(REDIS, REDIS_CLUSTER);
    }

    private void assertMessageBody(List<? extends StreamMessage<String, String>> items) {
        for (StreamMessage<String, String> message : items) {
            Assertions.assertTrue(message.getBody().containsKey("field1"));
            Assertions.assertTrue(message.getBody().containsKey("field2"));
        }
    }

    @ParameterizedTest
    @MethodSource("servers")
    void testStreamReader(RedisServer server) throws Throwable {
        dataGenerator(server).dataTypes(DataStructure.STREAM).end(100).build().call();
        StreamItemReader reader = streamReader(server, XReadArgs.StreamOffset.from("stream:0", "0-0")).build();
        reader.open(new ExecutionContext());
        List<StreamMessage<String, String>> messages = reader.readMessages();
        Assertions.assertEquals(10, messages.size());
        assertMessageBody(messages);
    }

    @ParameterizedTest
    @MethodSource("servers")
    void testStreamReaderJob(RedisServer server) throws Throwable {
        dataGenerator(server).dataTypes(DataStructure.STREAM).end(100).build().call();
        StreamItemReader reader = streamReader(server, XReadArgs.StreamOffset.from("stream:0", "0-0")).build();
        ListItemWriter<StreamMessage<String, String>> writer = new ListItemWriter<>();
        JobExecution execution = executeFlushing(server, "stream-reader", reader, writer);
        awaitJobTermination(execution);
        Assertions.assertEquals(10, writer.getWrittenItems().size());
        List<? extends StreamMessage<String, String>> items = writer.getWrittenItems();
        assertMessageBody(items);
    }


    @ParameterizedTest
    @MethodSource("servers")
    void testMultipleStreamReaders(RedisServer server) throws Throwable {
        String stream = "stream:0";
        String consumerGroup = "consumerGroup";
        dataGenerator(server).dataTypes(DataStructure.STREAM).end(100).build().call();
        StreamItemReader reader1 = streamReader(server, XReadArgs.StreamOffset.from(stream, "0-0")).consumerGroup(consumerGroup).consumer("consumer1").ackPolicy(StreamItemReader.AckPolicy.MANUAL).build();
        StreamItemReader reader2 = streamReader(server, XReadArgs.StreamOffset.from(stream, "0-0")).consumerGroup(consumerGroup).consumer("consumer2").ackPolicy(StreamItemReader.AckPolicy.MANUAL).build();
        ListItemWriter<StreamMessage<String, String>> writer1 = new ListItemWriter<>();
        JobExecution execution1 = executeFlushing(server, "stream-reader-1", reader1, writer1);
        ListItemWriter<StreamMessage<String, String>> writer2 = new ListItemWriter<>();
        JobExecution execution2 = executeFlushing(server, "stream-reader-2", reader2, writer2);
        awaitJobTermination(execution1);
        awaitJobTermination(execution2);
        Assertions.assertEquals(10, writer1.getWrittenItems().size() + writer2.getWrittenItems().size());
        assertMessageBody(writer1.getWrittenItems());
        assertMessageBody(writer2.getWrittenItems());
        RedisStreamCommands<String, String> sync = sync(server);
        PendingMessages pendingMessages = sync.xpending(stream, consumerGroup);
        Assertions.assertEquals(10, pendingMessages.getCount());
        reader1.open(new ExecutionContext());
        reader1.ack(writer1.getWrittenItems());
        reader2.open(new ExecutionContext());
        reader2.ack(writer2.getWrittenItems());
        pendingMessages = sync.xpending(stream, consumerGroup);
        Assertions.assertEquals(0, pendingMessages.getCount());
    }

    private StreamItemReader.StreamItemReaderBuilder streamReader(RedisServer server, XReadArgs.StreamOffset<String> offset) {
        if (server.isCluster()) {
            return StreamItemReader.client(redisClusterClient(server)).offset(offset);
        }
        return StreamItemReader.client(redisClient(server)).offset(offset);
    }
}
