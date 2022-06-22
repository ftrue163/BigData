package com.atguigu.sink;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.xcontent.XContentType;

import java.util.ArrayList;

public class Flink03_Sink_Es {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.从端口读取数据
        SingleOutputStreamOperator<WaterSensor> waterSensorDStream = env.socketTextStream("hadoop102", 9999)
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] split = value.split(" ");
                        return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                    }
                });

        SingleOutputStreamOperator<String> jsonStrDStream = waterSensorDStream.map(new MapFunction<WaterSensor, String>() {
            @Override
            public String map(WaterSensor value) throws Exception {
                return JSONObject.toJSONString(value);
            }
        });


        //将数据写入ES中
        ArrayList<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("hadoop102", 9200));
        httpHosts.add(new HttpHost("hadoop103", 9200));
        httpHosts.add(new HttpHost("hadoop104", 9200));

        // use a ElasticsearchSink.Builder to create an ElasticsearchSink
        ElasticsearchSink.Builder<String> esBuilder = new ElasticsearchSink.Builder<>(httpHosts, new ElasticsearchSinkFunction<String>() {
            @Override
            public void process(String element, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
                IndexRequest indexRequest = new IndexRequest("0625", "_doc", JSONObject.parseObject(element).getString("id"));

                indexRequest.source(element, XContentType.JSON);

                requestIndexer.add(indexRequest);
            }
        });

        //数据来一条写一条
        // configuration for the bulk requests; this instructs the sink to emit after every element, otherwise they would be buffered
        esBuilder.setBulkFlushMaxActions(1);

        // provide a RestClientFactory for custom configuration on the internally created REST client
        /*esBuilder.setRestClientFactory(
                restClientBuilder -> {
                    restClientBuilder.setDefaultHeaders(...)
                    restClientBuilder.setMaxRetryTimeoutMillis(...)
                    restClientBuilder.setPathPrefix(...)
                    restClientBuilder.setHttpClientConfigCallback(...)
                }
        );*/


        // finally, build and add the sink to the job's pipeline
        jsonStrDStream.addSink(esBuilder.build());

        env.execute();
    }
}
