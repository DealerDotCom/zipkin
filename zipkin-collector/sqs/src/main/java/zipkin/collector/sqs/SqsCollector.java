package zipkin.collector.sqs;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.*;
import zipkin.Codec;
import zipkin.collector.Collector;
import zipkin.collector.CollectorComponent;
import zipkin.collector.CollectorMetrics;
import zipkin.collector.CollectorSampler;
import zipkin.storage.Callback;
import zipkin.storage.InMemoryStorage;
import zipkin.storage.StorageComponent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by ddcbdevins on 6/10/16.
 */
public class SqsCollector implements CollectorComponent, Runnable {
    private final AtomicBoolean run = new AtomicBoolean(true);

    private Collector collector;
    private String sqsQueueUrl;
    private AmazonSQSClient client;

    public SqsCollector(Builder builder) {
        collector = builder.delegate.build();
        sqsQueueUrl = builder.sqsQueueUrl;
        client = builder.client;
    }

    @Override
    public CollectorComponent start() {
        new Thread(this).run();
        return this;
    }

    @Override
    public CheckResult check() {
        return CheckResult.OK;
    }

    @Override
    public void close() throws IOException {
        run.set(false);
    }

    @Override
    public void run() {
        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(sqsQueueUrl).withWaitTimeSeconds(1);
        while (run.get()) {
            ReceiveMessageResult result = client.receiveMessage(receiveMessageRequest);
            List<DeleteMessageBatchRequestEntry> deletes = new ArrayList<>();
            for (Message message : result.getMessages()) {
                System.out.println(message.getBody());
                collector.acceptSpans(Arrays.asList(message.getBody().getBytes()), Codec.JSON, Callback.NOOP);
                deletes.add(new DeleteMessageBatchRequestEntry(message.getMessageId(), message.getReceiptHandle()));
            }
            if (deletes.size() > 0) {
                client.deleteMessageBatch(sqsQueueUrl, deletes);
            }
        }
    }

    public static final class Builder implements CollectorComponent.Builder {
        Collector.Builder delegate = Collector.builder(SqsCollector.class);
        String sqsQueueUrl;
        AmazonSQSClient client;

        @Override
        public CollectorComponent.Builder storage(StorageComponent storage) {
            delegate.storage(storage);
            return this;
        }

        @Override
        public CollectorComponent.Builder metrics(CollectorMetrics metrics) {
            delegate.metrics(metrics);
            return this;
        }

        @Override
        public CollectorComponent.Builder sampler(CollectorSampler sampler) {
            delegate.sampler(sampler);
            return this;
        }

        public Builder sqsQueueUrl(String sqsQueueUrl) {
            this.sqsQueueUrl = sqsQueueUrl;
            return this;
        }

        public Builder client(AmazonSQSClient client) {
            this.client = client;
            return this;
        }

        public CollectorComponent build() {
            return new SqsCollector(this);
        }

        Builder() {
        }
    }
}
