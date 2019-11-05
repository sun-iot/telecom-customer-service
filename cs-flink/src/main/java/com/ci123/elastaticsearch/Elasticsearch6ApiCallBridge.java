package com.ci123.elastaticsearch;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;
import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;
import java.util.Map;


@Internal
public class Elasticsearch6ApiCallBridge implements ElasticsearchApiCallBridge<RestHighLevelClient> {

	private static final long serialVersionUID = -5222683870097809633L;

	private static final Logger LOG = LoggerFactory.getLogger(Elasticsearch6ApiCallBridge.class);

	/**
	 *  HTTP Host.
	 */
	private final List<HttpHost> httpHosts;

	/**
	 *  rest client.
	 */
	private final RestClientFactory restClientFactory;

	Elasticsearch6ApiCallBridge(List<HttpHost> httpHosts, RestClientFactory restClientFactory) {
		Preconditions.checkArgument(httpHosts != null && !httpHosts.isEmpty());
		this.httpHosts = httpHosts;
		this.restClientFactory = Preconditions.checkNotNull(restClientFactory);
	}

	@Override
	public RestHighLevelClient createClient(Map<String, String> clientConfig) throws IOException {
		RestClientBuilder builder = RestClient.builder(httpHosts.toArray(new HttpHost[httpHosts.size()]));
		restClientFactory.configureRestClientBuilder(builder);

		RestHighLevelClient rhlClient = new RestHighLevelClient(builder);

		if (LOG.isInfoEnabled()) {
			LOG.info("Pinging Elasticsearch cluster via hosts {} ...", httpHosts);
		}

		if (!rhlClient.ping()) {
			throw new RuntimeException("There are no reachable Elasticsearch nodes!");
		}

		if (LOG.isInfoEnabled()) {
			LOG.info("Created Elasticsearch RestHighLevelClient connected to {}", httpHosts.toString());
		}

		return rhlClient;
	}

	@Override
	public BulkProcessor.Builder createBulkProcessorBuilder(RestHighLevelClient client, BulkProcessor.Listener listener) {
		return BulkProcessor.builder(client::bulkAsync, listener);
	}

	@Override
	public Throwable extractFailureCauseFromBulkItemResponse(BulkItemResponse bulkItemResponse) {
		if (!bulkItemResponse.isFailed()) {
			return null;
		} else {
			return bulkItemResponse.getFailure().getCause();
		}
	}

	@Override
	public void configureBulkProcessorBackoff(
		BulkProcessor.Builder builder,
		@Nullable BulkFlushBackoffPolicy flushBackoffPolicy) {

		BackoffPolicy backoffPolicy;
		if (flushBackoffPolicy != null) {
			switch (flushBackoffPolicy.getBackoffType()) {
				case CONSTANT:
					backoffPolicy = BackoffPolicy.constantBackoff(
						new TimeValue(flushBackoffPolicy.getDelayMillis()),
						flushBackoffPolicy.getMaxRetryCount());
					break;
				case EXPONENTIAL:
				default:
					backoffPolicy = BackoffPolicy.exponentialBackoff(
						new TimeValue(flushBackoffPolicy.getDelayMillis()),
						flushBackoffPolicy.getMaxRetryCount());
			}
		} else {
			backoffPolicy = BackoffPolicy.noBackoff();
		}

		builder.setBackoffPolicy(backoffPolicy);
	}

	@Override
	public RequestIndexer createBulkProcessorIndexer(BulkProcessor bulkProcessor) {
		return new Elasticsearch6BulkProcessorIndexer(bulkProcessor);
	}
}
