package sunrise.demo.source;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

/**
 * @author kuiqwang
 * @emai wqkenqingto@163.com
 * @time 2023/2/22
 * @desc
 */
public class ElasticsearchSource extends RichSourceFunction<String> {

    private final String index;
    private final String query;
    private final int interval;
    private boolean running;
    private RestHighLevelClient client;

    public ElasticsearchSource(String index, String query, int interval) {
        this.index = index;
        this.query = query;
        this.interval = interval;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        client = new RestHighLevelClient(RestClient.builder(new HttpHost("calculation02", 9200, "http")));
    }

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        running = true;
        while (running) {
            SearchRequest searchRequest = new SearchRequest(index);
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
//            searchSourceBuilder.query(QueryBuilders.matchQuery("message", query));
            searchSourceBuilder.query(QueryBuilders.matchAllQuery());
            searchRequest.source(searchSourceBuilder);

            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

            for (SearchHit hit : searchResponse.getHits().getHits()) {
                ctx.collect(hit.getSourceAsString());
            }

            Thread.sleep(interval);
        }
    }

    @Override
    public void cancel() {
        running = false;
        try {
            client.close();
        } catch (Exception e) {
            // Ignore exception
        }
    }
}
