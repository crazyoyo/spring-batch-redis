package org.springframework.batch.item.redis;

import com.redis.testcontainers.RedisServer;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XReadArgs;
import io.lettuce.core.api.sync.RedisStreamCommands;
import io.lettuce.core.models.stream.PendingMessages;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.redis.support.DataStructure;
import org.springframework.batch.item.support.ListItemWriter;

import java.util.List;

public class StreamItemReaderTests extends AbstractRedisTestBase {

    private void assertMessageBody(List<? extends StreamMessage<String, String>> items) {
        for (StreamMessage<String, String> message : items) {
            Assertions.assertTrue(message.getBody().containsKey("field1"));
            Assertions.assertTrue(message.getBody().containsKey("field2"));
        }
    }

    @ParameterizedTest(name = "{displayName} - {index}: {0}")
    @MethodSource("servers")
    void testStreamReader(RedisServer server) throws Exception {
        dataGenerator(server).dataTypes(DataStructure.STREAM).end(100).build().call();
        StreamItemReader reader = streamReader(server, XReadArgs.StreamOffset.from("stream:0", "0-0")).build();
        reader.open(new ExecutionContext());
        List<StreamMessage<String, String>> messages = reader.readMessages();
        Assertions.assertEquals(10, messages.size());
        assertMessageBody(messages);
    }

    @ParameterizedTest(name = "{displayName} - {index}: {0}")
    @MethodSource("servers")
    void testStreamReaderJob(RedisServer redis) throws Exception {
        dataGenerator(redis).dataTypes(DataStructure.STREAM).end(100).build().call();
        StreamItemReader reader = streamReader(redis, XReadArgs.StreamOffset.from("stream:0", "0-0")).build();
        ListItemWriter<StreamMessage<String, String>> writer = new ListItemWriter<>();
        JobExecution execution = executeFlushing(name(redis, "stream-reader"), reader, writer);
        awaitTermination(execution);
        Assertions.assertEquals(10, writer.getWrittenItems().size());
        List<? extends StreamMessage<String, String>> items = writer.getWrittenItems();
        assertMessageBody(items);
    }

    @ParameterizedTest(name = "{displayName} - {index}: {0}")
    @MethodSource("servers")
    void testMultipleStreamReaders(RedisServer redis) throws Exception {
        String stream = "stream:0";
        String consumerGroup = "consumerGroup";
        dataGenerator(redis).dataTypes(DataStructure.STREAM).end(100).build().call();
        StreamItemReader reader1 = streamReader(redis, XReadArgs.StreamOffset.from(stream, "0-0")).consumerGroup(consumerGroup).consumer("consumer1").ackPolicy(StreamItemReader.AckPolicy.MANUAL).build();
        StreamItemReader reader2 = streamReader(redis, XReadArgs.StreamOffset.from(stream, "0-0")).consumerGroup(consumerGroup).consumer("consumer2").ackPolicy(StreamItemReader.AckPolicy.MANUAL).build();
        ListItemWriter<StreamMessage<String, String>> writer1 = new ListItemWriter<>();
        JobExecution execution1 = executeFlushing(name(redis, "stream-reader-1"), reader1, writer1);
        ListItemWriter<StreamMessage<String, String>> writer2 = new ListItemWriter<>();
        JobExecution execution2 = executeFlushing(name(redis, "stream-reader-2"), reader2, writer2);
        awaitTermination(execution1);
        awaitTermination(execution2);
        Assertions.assertEquals(10, writer1.getWrittenItems().size() + writer2.getWrittenItems().size());
        assertMessageBody(writer1.getWrittenItems());
        assertMessageBody(writer2.getWrittenItems());
        RedisStreamCommands<String, String> sync = sync(redis);
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
