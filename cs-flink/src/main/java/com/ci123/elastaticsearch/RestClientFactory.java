package com.ci123.elastaticsearch;

import org.apache.flink.annotation.PublicEvolving;
import org.elasticsearch.client.RestClientBuilder;

import java.io.Serializable;

@PublicEvolving
public interface RestClientFactory extends Serializable {

    void configureRestClientBuilder(RestClientBuilder restClientBuilder);

}
