package testEsClient;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.net.InetAddress;
import java.util.Date;
import java.util.Iterator;

import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.xpack.client.PreBuiltXPackTransportClient;

import com.eluup.flume.sink.elasticsearch.client.PreBuiltTransportClient;

public class EsClient {
	
	private static PreBuiltXPackTransportClient transportClient;
	
	public static void main(String[] args) throws Exception {
		Settings settings = Settings.builder()
				.put("cluster.name", "club")
				.put("xpack.security.user", "elastic:123456") 
				.build();

		transportClient = new PreBuiltXPackTransportClient(settings);
		transportClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("host3"), 9300));
		transportClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("host1"), 9300));
		transportClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("host2"), 9300));
		
		
		try {
	        
			IndexResponse response = transportClient.prepareIndex("twitter123", "tweet", "1")
			        .setSource(jsonBuilder()
			                    .startObject()
			                        .field("user", "kimchy")
			                        .field("postDate", new Date())
			                        .field("message", "trying out Elasticsearch")
			                    .endObject()
			                  )
			        .get();
			System.out.println(response.getResult());
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
		
		try {
			
			
			System.out.println("=====================");
			System.out.println("=====================");
			System.out.println("=====================");
			System.out.println("=====================");
			
			
			BulkRequestBuilder bulkRequest = transportClient.prepareBulk();
			bulkRequest.add(transportClient.prepareIndex("esf_bi_access-123", "tweet", "2")
			        .setSource(jsonBuilder()
			                    .startObject()
			                        .field("user", "kimchy")
			                        .field("postDate", new Date())
			                        .field("message", "another post")
			                        .field("message2", "another post")
			                        .field("message3", "another post")
			                        .field("message4", "another post")
			                    .endObject()
			                  )
			        );
			System.out.println(bulkRequest.request().toString());
			BulkResponse bulkResponse = bulkRequest.get();
			for (Iterator<BulkItemResponse> iterator = bulkResponse.iterator(); iterator.hasNext();) {
		        BulkItemResponse r = iterator.next();
		        System.out.println(r.getResponse().getResult());
	        }
			System.out.println();
			if (bulkResponse.hasFailures()) {
			    // process failures by iterating through each bulk response item
				System.out.println(bulkResponse.toString());
				System.out.println(bulkResponse.buildFailureMessage());
			}
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
		
	}
}
