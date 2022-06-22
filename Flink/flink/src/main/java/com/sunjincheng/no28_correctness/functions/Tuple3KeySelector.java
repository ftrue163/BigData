package com.sunjincheng.no28_correctness.functions;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 * 项目名称: Apache Flink 知其然，知其所以然 - khkw.correctness.functions
 * 作者： 张智奇

 */
public class Tuple3KeySelector implements KeySelector<Tuple3<String, Long, Long>, String> {
    @Override
    public String getKey(Tuple3<String, Long, Long> event) throws Exception {
        return event.f0;
    }
}
