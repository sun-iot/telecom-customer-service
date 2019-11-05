package com.ci123.elastaticsearch;

import org.apache.flink.annotation.PublicEvolving;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;


@PublicEvolving
public interface RequestIndexer {


    @Deprecated
    default void add(ActionRequest... actionRequests) {
        for (ActionRequest actionRequest : actionRequests) {
            if (actionRequest instanceof IndexRequest) {
                add((IndexRequest) actionRequest);
            } else if (actionRequest instanceof DeleteRequest) {
                add((DeleteRequest) actionRequest);
            } else if (actionRequest instanceof UpdateRequest) {
                add((UpdateRequest) actionRequest);
            } else {
                throw new IllegalArgumentException("RequestIndexer only supports Index, Delete and Update requests");
            }
        }
    }


    void add(DeleteRequest... deleteRequests);


    void add(IndexRequest... indexRequests);


    void add(UpdateRequest... updateRequests);
}
