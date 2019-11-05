package com.ci123.elastaticsearch;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Preconditions;
import org.apache.http.HttpHost;
import org.apache.log4j.Logger;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.client.RestHighLevelClient;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ElasticsearchOutputFormat<T> extends AbstractElasticsearchOutputFormat<T, RestHighLevelClient> {

  private static final Logger LOG = Logger.getLogger(ElasticsearchOutputFormat.class);
  private static final long serialVersionUID = 1L;

  private ElasticsearchOutputFormat(
      Map<String, String> bulkRequestsConfig,
      List<HttpHost> httpHosts,
      ElasticsearchSinkFunction<T> elasticsearchSinkFunction,
      DocWriteRequestFailureHandler failureHandler,
      RestClientFactory restClientFactory) {
    super(new Elasticsearch6ApiCallBridge(httpHosts, restClientFactory),  bulkRequestsConfig, elasticsearchSinkFunction, failureHandler);
  }

  @Override
  public void configure(Configuration configuration) {

  }


  @PublicEvolving
  public static class Builder<T> {

    private final List<HttpHost> httpHosts;
    private final ElasticsearchSinkFunction<T> elasticsearchSinkFunction;

    private Map<String, String> bulkRequestsConfig = new HashMap<>();
    private DocWriteRequestFailureHandler failureHandler = new NoOpFailureHandler();
    private RestClientFactory restClientFactory = restClientBuilder -> {};


    public Builder(List<HttpHost> httpHosts, ElasticsearchSinkFunction<T> elasticsearchSinkFunction) {
      this.httpHosts = Preconditions.checkNotNull(httpHosts);
      this.elasticsearchSinkFunction = Preconditions.checkNotNull(elasticsearchSinkFunction);
    }


    public Builder setBulkFlushMaxActions(int numMaxActions) {
      Preconditions.checkArgument(
          numMaxActions > 0,
          "Max number of buffered actions must be larger than 0.");
      LOG.info("Builder config: bulkFlushMaxActions=" + numMaxActions);
      this.bulkRequestsConfig.put(CONFIG_KEY_BULK_FLUSH_MAX_ACTIONS, String.valueOf(numMaxActions));
      return this;
    }


    public Builder setBulkFlushMaxSizeMb(int maxSizeMb) {
      Preconditions.checkArgument(
          maxSizeMb > 0,
          "Max size of buffered actions must be larger than 0.");
      LOG.info("Builder config: bulkFlushMaxSizeMb=" + maxSizeMb);
      this.bulkRequestsConfig.put(CONFIG_KEY_BULK_FLUSH_MAX_SIZE_MB, String.valueOf(maxSizeMb));
      return this;
    }


    public Builder setBulkFlushInterval(long intervalMillis) {
      Preconditions.checkArgument(
          intervalMillis >= 0,
          "Interval (in milliseconds) between each flush must be larger than or equal to 0.");
      LOG.info("Builder config: bulkFlushInterval=" + intervalMillis);
      this.bulkRequestsConfig.put(CONFIG_KEY_BULK_FLUSH_INTERVAL_MS, String.valueOf(intervalMillis));
      return this;
    }


    public Builder setBulkFlushBackoff(boolean enabled) {
      LOG.info("Builder config: bulkFlushBackoff=" + enabled);
      this.bulkRequestsConfig.put(CONFIG_KEY_BULK_FLUSH_BACKOFF_ENABLE, String.valueOf(enabled));
      return this;
    }


    public Builder setBulkFlushBackoffType(ElasticsearchApiCallBridge.FlushBackoffType flushBackoffType) {
      LOG.info("Builder config: bulkFlushBackoffType=" + flushBackoffType);
      this.bulkRequestsConfig.put(
          CONFIG_KEY_BULK_FLUSH_BACKOFF_TYPE,
          Preconditions.checkNotNull(flushBackoffType).toString());
      return this;
    }


    public Builder setBulkFlushBackoffRetries(int maxRetries) {
      Preconditions.checkArgument(
          maxRetries > 0,
          "Max number of backoff attempts must be larger than 0.");
      LOG.info("Builder config: bulkFlushBackoffRetries=" + maxRetries);
      this.bulkRequestsConfig.put(CONFIG_KEY_BULK_FLUSH_BACKOFF_RETRIES, String.valueOf(maxRetries));
      return this;
    }


    public Builder setBulkFlushBackoffDelay(long delayMillis) {
      Preconditions.checkArgument(
          delayMillis >= 0,
          "Delay (in milliseconds) between each backoff attempt must be larger than or equal to 0.");
      LOG.info("Builder config: bulkFlushBackoffDelay=" + delayMillis);
      this.bulkRequestsConfig.put(CONFIG_KEY_BULK_FLUSH_BACKOFF_DELAY, String.valueOf(delayMillis));
      return this;
    }


    public Builder setFailureHandler(DocWriteRequestFailureHandler failureHandler) {
      LOG.info("Builder config: failureHandler=" + failureHandler);
      this.failureHandler = Preconditions.checkNotNull(failureHandler);
      return this;
    }


    public Builder setRestClientFactory(RestClientFactory restClientFactory) {
      LOG.info("Builder config: restClientFactory=" + restClientFactory);
      this.restClientFactory = Preconditions.checkNotNull(restClientFactory);
      return this;
    }


    public ElasticsearchOutputFormat<T> build() {
      return new ElasticsearchOutputFormat<>(bulkRequestsConfig, httpHosts, elasticsearchSinkFunction, failureHandler, restClientFactory);
    }
  }


  @Internal
  private static class NoOpFailureHandler implements DocWriteRequestFailureHandler {

    private static final long serialVersionUID = 737941343410827885L;

    @Override
    public void onFailure(DocWriteRequest action, Throwable failure, int restStatusCode, RequestIndexer indexer) throws Throwable {
      // simply fail the sink
      throw failure;
    }

  }

}
