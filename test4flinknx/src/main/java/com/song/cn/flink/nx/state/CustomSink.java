package com.song.cn.flink.nx.state;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.ArrayList;
import java.util.List;

public class CustomSink implements SinkFunction<Tuple2<String, Long>>, CheckpointedFunction {

    /**
     * 用于缓存结果数据
     */
    private List<Tuple2<String, Long>> bufferedElements;

    /**
     * 表示内存中数据的大小阈值
     */
    private int threshold;
    /**
     * 用于保存内存中的状态值
     */
    private ListState<Tuple2<String, Long>> checkpointState;


    public CustomSink(int threshold) {
        this.threshold = threshold;
        bufferedElements = new ArrayList<>();
    }


    @Override
    public void invoke(Tuple2<String, Long> value, Context context) throws Exception {
        // 可以将接收到的每一条数据保存到任何的存储系统中
        bufferedElements.add(value);
        if (bufferedElements.size() == threshold) {
            // 简单打印
            System.out.println("自定义格式:" + bufferedElements);
            bufferedElements.clear();
        }
    }

    /**
     * 用于将内存中的数据保存到状态中
     *
     * @param context
     * @throws Exception
     */
    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        checkpointState.clear();
        for(Tuple2<String,Long> element:bufferedElements){
            checkpointState.add(element);
        }

    }

    /**
     * 用于在程序挥发的时候从状态中恢复数据到内存
     *
     * @param context
     * @throws Exception
     */
    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        ListStateDescriptor<Tuple2<String, Long>> descriptor = new ListStateDescriptor<>(
                "buffered-elements",
                TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {
                })
        );

        // 注册一个 operate state
        checkpointState = context.getOperatorStateStore().getListState(descriptor);

        if (context.isRestored()) {
            for (Tuple2<String, Long> element : checkpointState.get()) {
                bufferedElements.add(element);
            }
        }
    }
}
