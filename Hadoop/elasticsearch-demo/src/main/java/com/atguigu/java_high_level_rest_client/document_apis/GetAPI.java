package com.atguigu.java_high_level_rest_client.document_apis;

import org.apache.http.HttpHost;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;
import java.util.Map;

public class GetAPI {
    public static void main(String[] args) throws IOException {
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(new HttpHost("hadoop202", 9200, "http")));


        // 一、执行查询操作
        GetRequest getRequest = new GetRequest(
                "zyk_bq_jq_dd",
                "_doc",
                "13664489003");

        GetResponse getResponse = client.get(getRequest, RequestOptions.DEFAULT);

        String index = getResponse.getIndex();
        String type = getResponse.getType();
        String id = getResponse.getId();
        if (getResponse.isExists()) {
            long version = getResponse.getVersion();
            String sourceAsString = getResponse.getSourceAsString();
            //Map<String, Object> sourceAsMap = getResponse.getSourceAsMap();
            //byte[] sourceAsBytes = getResponse.getSourceAsBytes();

            System.out.println(version);
            System.out.println(sourceAsString);
        } else {
            System.out.println("指定 id 的文档不存在");
        }

        // 二、执行 Upsert 操作
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        builder.field("bjdh", "13765489823");
        builder.field("jjd_bh", "123456");
        builder.field("others", "cdef");
        builder.endObject();

        UpdateRequest updateRequest = new UpdateRequest("zyk_bq_jq_dd", "_doc", "13765489823").doc(builder);
        updateRequest.docAsUpsert(true);

        // 客户端发送请求，获取响应对象
        client.update(updateRequest, RequestOptions.DEFAULT);

        client.close();
    }
}
