package com.ci123.elastaticsearch;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.InstantiationUtil;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.rest.RestStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Copyright (c) 2018-2028 Corp-ci All Rights Reserved
 * <p>
 * Project: telecom-customer-service
 * Package: com.ci123.elasticsearch
 * Version: 1.0
 * <p>
 * @param <T> Type of the elements handled by this sink
 * @param <C> Type of the Elasticsearch client, which implements {@link AutoCloseable}
 * Created by SunYang on 2019/9/17 14:12
 */
@Internal
public abstract class AbstractElasticsearchOutputFormat<T, C extends AutoCloseable> extends
        RichOutputFormat<T> {

    private static final long serialVersionUID = -1007596293618451942L;

    private static final Logger LOG = LoggerFactory.getLogger(AbstractElasticsearchOutputFormat.class);

    // ------------------------------------------------------------------------
    //  Internal bulk processor configuration
    // ------------------------------------------------------------------------

    public static final String CONFIG_KEY_BULK_FLUSH_MAX_ACTIONS = "bulk.flush.max.actions";
    public static final String CONFIG_KEY_BULK_FLUSH_MAX_SIZE_MB = "bulk.flush.max.size.mb";
    public static final String CONFIG_KEY_BULK_FLUSH_INTERVAL_MS = "bulk.flush.interval.ms";
    public static final String CONFIG_KEY_BULK_FLUSH_BACKOFF_ENABLE = "bulk.flush.backoff.enable";
    public static final String CONFIG_KEY_BULK_FLUSH_BACKOFF_TYPE = "bulk.flush.backoff.type";
    public static final String CONFIG_KEY_BULK_FLUSH_BACKOFF_RETRIES = "bulk.flush.backoff.retries";
    public static final String CONFIG_KEY_BULK_FLUSH_BACKOFF_DELAY = "bulk.flush.backoff.delay";

    private final Integer bulkProcessorFlushMaxActions;
    private final Integer bulkProcessorFlushMaxSizeMb;
    private final Long bulkProcessorFlushIntervalMillis;
    private final ElasticsearchApiCallBridge.BulkFlushBackoffPolicy bulkProcessorFlushBackoffPolicy;

    // ------------------------------------------------------------------------
    //  User-facing API and configuration
    // ------------------------------------------------------------------------
    private final Map<String, String> userConfig;
    private final ElasticsearchSinkFunction<T> elasticsearchSinkFunction;
    private final DocWriteRequestFailureHandler failureHandler;

    /**
     * 通过 ElasticsearchSinkFunction 去添加 DocWriteRequests
     */
    private transient RequestIndexer requestIndexer;

    /**
     * 为不同的版本做一个兼容
     */
    private final ElasticsearchApiCallBridge<C> callBridge;

    /**
     * 使用调用桥创建的Elasticsearch客户端。
     */
    private transient C client;

    /**
     * 批量处理器缓冲并向使用客户端创建的Elasticsearch发送请求。
     */
    private transient BulkProcessor bulkProcessor;

    /**
     * 当输入或者关闭时进行错误检查
     */
    private final AtomicReference<Throwable> failureThrowable = new AtomicReference<>();

    public AbstractElasticsearchOutputFormat(
            ElasticsearchApiCallBridge callBridge,
            Map<String, String> userConfig,
            ElasticsearchSinkFunction<T> elasticsearchSinkFunction,
            DocWriteRequestFailureHandler failureHandler) {

        this.callBridge = checkNotNull(callBridge);
        this.elasticsearchSinkFunction = checkNotNull(elasticsearchSinkFunction);
        this.failureHandler = checkNotNull(failureHandler);

        // 序列化检查 elasticsearchSinkFunction
        checkArgument(InstantiationUtil.isSerializable(elasticsearchSinkFunction),
                "The implementation of the provided ElasticsearchSinkFunction is not serializable. " +
                        "The object probably contains or references non-serializable fields.");
        // 序列化检查 failureHandler
        checkArgument(InstantiationUtil.isSerializable(failureHandler),
                "The implementation of the provided DocWriteRequestFailureHandler is not serializable. " +
                        "The object probably contains or references non-serializable fields.");

        // 判断不为空
        checkNotNull(userConfig);

        // copy config so we can remove entries without side-effects
        userConfig = new HashMap<>(userConfig);

        ParameterTool params = ParameterTool.fromMap(userConfig);

        if (params.has(CONFIG_KEY_BULK_FLUSH_MAX_ACTIONS)) {
            bulkProcessorFlushMaxActions = params.getInt(CONFIG_KEY_BULK_FLUSH_MAX_ACTIONS);
            userConfig.remove(CONFIG_KEY_BULK_FLUSH_MAX_ACTIONS);
        } else {
            bulkProcessorFlushMaxActions = null;
        }

        if (params.has(CONFIG_KEY_BULK_FLUSH_MAX_SIZE_MB)) {
            bulkProcessorFlushMaxSizeMb = params.getInt(CONFIG_KEY_BULK_FLUSH_MAX_SIZE_MB);
            userConfig.remove(CONFIG_KEY_BULK_FLUSH_MAX_SIZE_MB);
        } else {
            bulkProcessorFlushMaxSizeMb = null;
        }

        if (params.has(CONFIG_KEY_BULK_FLUSH_INTERVAL_MS)) {
            bulkProcessorFlushIntervalMillis = params.getLong(CONFIG_KEY_BULK_FLUSH_INTERVAL_MS);
            userConfig.remove(CONFIG_KEY_BULK_FLUSH_INTERVAL_MS);
        } else {
            bulkProcessorFlushIntervalMillis = null;
        }

        boolean bulkProcessorFlushBackoffEnable = params.getBoolean(CONFIG_KEY_BULK_FLUSH_BACKOFF_ENABLE, true);
        userConfig.remove(CONFIG_KEY_BULK_FLUSH_BACKOFF_ENABLE);

        if (bulkProcessorFlushBackoffEnable) {
            this.bulkProcessorFlushBackoffPolicy = new ElasticsearchApiCallBridge.BulkFlushBackoffPolicy();

            if (params.has(CONFIG_KEY_BULK_FLUSH_BACKOFF_TYPE)) {
                bulkProcessorFlushBackoffPolicy.setBackoffType(ElasticsearchApiCallBridge.FlushBackoffType.valueOf(params.get(CONFIG_KEY_BULK_FLUSH_BACKOFF_TYPE)));
                userConfig.remove(CONFIG_KEY_BULK_FLUSH_BACKOFF_TYPE);
            }

            if (params.has(CONFIG_KEY_BULK_FLUSH_BACKOFF_RETRIES)) {
                bulkProcessorFlushBackoffPolicy.setMaxRetryCount(params.getInt(CONFIG_KEY_BULK_FLUSH_BACKOFF_RETRIES));
                userConfig.remove(CONFIG_KEY_BULK_FLUSH_BACKOFF_RETRIES);
            }

            if (params.has(CONFIG_KEY_BULK_FLUSH_BACKOFF_DELAY)) {
                bulkProcessorFlushBackoffPolicy.setDelayMillis(params.getLong(CONFIG_KEY_BULK_FLUSH_BACKOFF_DELAY));
                userConfig.remove(CONFIG_KEY_BULK_FLUSH_BACKOFF_DELAY);
            }

        } else {
            bulkProcessorFlushBackoffPolicy = null;
        }

        this.userConfig = userConfig;
    }

    @Override
    public void open(int taskNumber, int numberTasks) throws IOException {
        client = callBridge.createClient(userConfig);
        bulkProcessor = buildBulkProcessor(new BulkProcessorListener());
        requestIndexer = callBridge.createBulkProcessorIndexer(bulkProcessor);
    }

    @Override
    public void writeRecord(T value) throws IOException {
        // 如果有错误的就把错误的重新放入到队列中去
        checkErrorAndRethrow();
        elasticsearchSinkFunction.process(value, getRuntimeContext(), requestIndexer);
    }

    @Override
    public void close() throws IOException {
        if (bulkProcessor != null) {
            bulkProcessor.close();
            bulkProcessor = null;
        }

        if (client != null) {
            try {
                client.close();
            } catch (Exception e) {
                throw new RuntimeException("Fail to close client: " + client, e);
            }
            client = null;
        }

        callBridge.cleanup();

        // 开始回滚，对失败的重新开始
        checkErrorAndRethrow();
    }

    /**
     * <p> test process
     * @param listener
     * @return
     */
    @VisibleForTesting
    protected BulkProcessor buildBulkProcessor(BulkProcessor.Listener listener) {
        checkNotNull(listener);

        BulkProcessor.Builder bulkProcessorBuilder = callBridge.createBulkProcessorBuilder(client, listener);

        // This makes flush() blocking
        bulkProcessorBuilder.setConcurrentRequests(0);

        if (bulkProcessorFlushMaxActions != null) {
            bulkProcessorBuilder.setBulkActions(bulkProcessorFlushMaxActions);
        }

        if (bulkProcessorFlushMaxSizeMb != null) {
            bulkProcessorBuilder.setBulkSize(new ByteSizeValue(bulkProcessorFlushMaxSizeMb, ByteSizeUnit.MB));
        }

        if (bulkProcessorFlushIntervalMillis != null) {
            bulkProcessorBuilder.setFlushInterval(TimeValue.timeValueMillis(bulkProcessorFlushIntervalMillis));
        }

        // if backoff retrying is disabled, bulkProcessorFlushBackoffPolicy will be null
        callBridge.configureBulkProcessorBackoff(bulkProcessorBuilder, bulkProcessorFlushBackoffPolicy);

        return bulkProcessorBuilder.build();
    }

    private void checkErrorAndRethrow() {
        Throwable cause = failureThrowable.get();
        if (cause != null) {
            throw new RuntimeException("An error occurred in ElasticsearchSink.", cause);
        }
    }

    private class BulkProcessorListener implements BulkProcessor.Listener {
        @Override
        public void beforeBulk(long executionId, BulkRequest request) {
        }

        @Override
        public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
            if (response.hasFailures()) {
                BulkItemResponse itemResponse;
                Throwable failure;
                RestStatus restStatus;

                try {
                    for (int i = 0; i < response.getItems().length; i++) {
                        itemResponse = response.getItems()[i];
                        failure = callBridge.extractFailureCauseFromBulkItemResponse(itemResponse);
                        if (failure != null) {
                            LOG.error("Failed Elasticsearch item request: {}", itemResponse.getFailureMessage(), failure);

                            restStatus = itemResponse.getFailure().getStatus();
                            if (restStatus == null) {
                                failureHandler.onFailure(request.requests().get(i), failure, -1, requestIndexer);
                            } else {
                                failureHandler.onFailure(request.requests().get(i), failure, restStatus.getStatus(), requestIndexer);
                            }
                        }
                    }
                } catch (Throwable t) {
                    // fail the sink and skip the rest of the items
                    // if the failure handler decides to throw an exception
                    failureThrowable.compareAndSet(null, t);
                }
            }
        }

        @Override
        public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
            LOG.error("Failed Elasticsearch bulk request: {}", failure.getMessage(), failure.getCause());

            try {
                for (DocWriteRequest action : request.requests()) {
                    failureHandler.onFailure(action, failure, -1, requestIndexer);
                }
            } catch (Throwable t) {
                // fail the sink and skip the rest of the items
                // if the failure handler decides to throw an exception
                failureThrowable.compareAndSet(null, t);
            }
        }
    }

}
