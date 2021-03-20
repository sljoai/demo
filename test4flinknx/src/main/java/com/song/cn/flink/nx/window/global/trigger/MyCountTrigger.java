package com.song.cn.flink.nx.window.global.trigger;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;

public class MyCountTrigger extends Trigger<Tuple2<String,Integer>, GlobalWindow> {

    /**
     * 指定元素的最大数量
     */
    private int maxCount;

    /**
     * 用于存储每个key对应的count值
     */
    private ReducingStateDescriptor<Long> stateDescriptor = new ReducingStateDescriptor<Long>("count", new ReduceFunction<Long>() {
        @Override
        public Long reduce(Long aLong, Long t1) throws Exception {
            return aLong + t1;
        }
    },Long.class);

    @Override
    public TriggerResult onElement(Tuple2<String, Integer> element,
                                   long timestamp,
                                   GlobalWindow window,
                                   TriggerContext ctx) throws Exception {
        // 拿到当前key对应的state
        ReducingState<Long> partitionedState = ctx.getPartitionedState(stateDescriptor);
        // 累加1
        partitionedState.add(1L);
        // 当达到最大次数，触发window计算，清除数据
        if(partitionedState.get()==maxCount){
            partitionedState.clear();
            return TriggerResult.FIRE_AND_PURGE;
        }
        // 否则不做任何事情
        return TriggerResult.CONTINUE;
    }

    public MyCountTrigger(int maxCount){
        this.maxCount = maxCount;
    }

    @Override
    public TriggerResult onProcessingTime(long time, GlobalWindow window, TriggerContext ctx) throws Exception {
        // 写基于 Processing Time 的定时器器任务逻辑
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onEventTime(long time, GlobalWindow window, TriggerContext ctx) throws Exception {
        // 写基于 Event Time 的定时器器任务逻辑
        return TriggerResult.CONTINUE;
    }

    @Override
    public void clear(GlobalWindow window, TriggerContext ctx) throws Exception {
        // 清除状态
        ctx.getPartitionedState(stateDescriptor).clear();
    }
}
