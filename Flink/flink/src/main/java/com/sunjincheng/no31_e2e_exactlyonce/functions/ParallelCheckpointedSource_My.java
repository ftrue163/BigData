package com.sunjincheng.no31_e2e_exactlyonce.functions;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 自定义具有checkpoint功能的Source
 */
public class ParallelCheckpointedSource_My extends RichParallelSourceFunction<Tuple3<String, Long, String>> implements CheckpointedFunction {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(ParallelCheckpointedSource_My.class);

    //标示数据源一直在取数据
    protected volatile boolean running = true;

    //数据源的消费offset
    private transient long offset;

    //offsetState
    private transient ListState<Long> offsetState;

    //offsetState name
    private static final String OFFSETS_STATE_NAME = "offset-states";

    //当前任务实例的编号
    private transient int indexOfThisTask;

    private String name = "-";

    public ParallelCheckpointedSource_My(String name) {
        this.name = name;
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        if (!running) {
            LOG.error("snapshotState() called on closed source");
        } else {
            //清除上次的state
            this.offsetState.clear();
            //持久化最新的offset
            this.offsetState.add(offset);
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        indexOfThisTask = getRuntimeContext().getIndexOfThisSubtask();

        offsetState = context.getOperatorStateStore().getListState(new ListStateDescriptor<>(OFFSETS_STATE_NAME, Types.LONG));

        for (Long offsetValue : offsetState.get()) {
            offset = offsetValue;
            LOG.error(String.format("Current Source [%s] Restore from offset [%d]", name, offset));
        }
    }

    @Override
    public void run(SourceContext<Tuple3<String, Long, String>> ctx) throws Exception {
        while (true) {
            //这是必须添加的线程同步锁，主要是为了和snapshotState进行线程控制
            synchronized (ctx.getCheckpointLock()) {
                ctx.collect(new Tuple3<>("key", ++offset, this.name));
            }
            Thread.sleep(10L);
        }
    }

    @Override
    public void cancel() {

    }
}
