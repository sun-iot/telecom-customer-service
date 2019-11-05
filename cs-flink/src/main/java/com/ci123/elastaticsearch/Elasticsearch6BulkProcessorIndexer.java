package com.ci123.elastaticsearch;

import org.apache.flink.annotation.Internal;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;

import static org.apache.flink.util.Preconditions.checkNotNull;

@Internal
class Elasticsearch6BulkProcessorIndexer implements RequestIndexer {

	private final BulkProcessor bulkProcessor;

	Elasticsearch6BulkProcessorIndexer(BulkProcessor bulkProcessor) {
		this.bulkProcessor = checkNotNull(bulkProcessor);
	}

	@Override
	public void add(DeleteRequest... deleteRequests) {
		for (DeleteRequest deleteRequest : deleteRequests) {
			this.bulkProcessor.add(deleteRequest);
		}
	}

	@Override
	public void add(IndexRequest... indexRequests) {
		for (IndexRequest indexRequest : indexRequests) {
			this.bulkProcessor.add(indexRequest);
		}
	}

	@Override
	public void add(UpdateRequest... updateRequests) {
		for (UpdateRequest updateRequest : updateRequests) {
			this.bulkProcessor.add(updateRequest);
		}
	}
}
