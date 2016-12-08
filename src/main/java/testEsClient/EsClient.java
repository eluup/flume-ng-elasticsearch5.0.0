package testEsClient;

import org.apache.flume.Context;

import com.eluup.flume.sink.elasticsearch.ElasticSearchSink;

public class EsClient {
	public static void main(String[] args) throws Exception {

		
		Context context = new Context();
		context.put("hostNames", "10.207.9.235:9300");
		context.put("indexName", "esf_bi_access");
		context.put("indexType", "access");
		context.put("clusterName", "club");
		context.put("batchSize", "1100");
		context.put("ttl", "5d");
		context.put("client.transport.nodes_sampler_interval", "10s");
		
		
				
		ElasticSearchSink essink = new ElasticSearchSink();
		essink.configure(context);
		essink.start();
		essink.stop();
	}
}
