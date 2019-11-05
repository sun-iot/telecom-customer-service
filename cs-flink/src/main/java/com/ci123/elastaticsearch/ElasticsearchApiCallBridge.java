package com.ci123.elastaticsearch;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.PublicEvolving;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;


@Internal
public interface ElasticsearchApiCallBridge<C extends AutoCloseable> extends Serializable {


    C createClient(Map<String, String> clientConfig) throws IOException;


    BulkProcessor.Builder createBulkProcessorBuilder(C client, BulkProcessor.Listener listener);

    @Nullable
    Throwable extractFailureCauseFromBulkItemResponse(BulkItemResponse bulkItemResponse);


    void configureBulkProcessorBackoff(
            BulkProcessor.Builder builder,
            @Nullable BulkFlushBackoffPolicy flushBackoffPolicy);


    RequestIndexer createBulkProcessorIndexer(BulkProcessor bulkProcessor);


    default void cleanup() {
        // nothing to cleanup by default
    }

    @PublicEvolving
    public enum FlushBackoffType {
        CONSTANT,
        EXPONENTIAL
    }


    public static class BulkFlushBackoffPolicy implements Serializable {

        private static final long serialVersionUID = -6022851996101826049L;

        // the default values follow the Elasticsearch default settings for BulkProcessor
        private FlushBackoffType backoffType = FlushBackoffType.EXPONENTIAL;
        private int maxRetryCount = 8;
        private long delayMillis = 50;

        public FlushBackoffType getBackoffType() {
            return backoffType;
        }

        public int getMaxRetryCount() {
            return maxRetryCount;
        }

        public long getDelayMillis() {
            return delayMillis;
        }

        public void setBackoffType(FlushBackoffType backoffType) {
            this.backoffType = checkNotNull(backoffType);
        }

        public void setMaxRetryCount(int maxRetryCount) {
            checkArgument(maxRetryCount >= 0);
            this.maxRetryCount = maxRetryCount;
        }

        public void setDelayMillis(long delayMillis) {
            checkArgument(delayMillis >= 0);
            this.delayMillis = delayMillis;
        }
    }

}
