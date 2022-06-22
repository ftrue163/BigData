package com.sunjincheng.no31_e2e_exactlyonce.functions;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;

public class Tuple3KeySelector_My implements KeySelector<Tuple3<String, Long, String>, String> {
    @Override
    public String getKey(Tuple3<String, Long, String> event) throws Exception {
        return event.f0;
    }
}
