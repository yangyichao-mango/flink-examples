/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package flink.examples.datastream._05._05_02;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.Window;

public class EarlyFireEventTimeTriggerV2<W extends Window> extends Trigger<Object, W> {

    // 窗口内提前触发的时间间隔
    private final long interval;

    // 将下一次要触发的时间戳记录在 State 中
    private final ReducingStateDescriptor<Long> stateDesc =
            new ReducingStateDescriptor<>("early-fire-time", new EarlyFireEventTimeTriggerV2.Min(), LongSerializer.INSTANCE);

    private EarlyFireEventTimeTriggerV2(long interval) {
        this.interval = interval;
    }

    @Override
    public TriggerResult onElement(Object element, long timestamp, W window, TriggerContext ctx)
            throws Exception {

        // 如果 Watermark 大于窗口最大时间戳，则直接触发计算，否则注册定时器
        if (window.maxTimestamp() <= ctx.getCurrentWatermark()) {
            return TriggerResult.FIRE;
        } else {
            ctx.registerEventTimeTimer(window.maxTimestamp());
        }

        // 获取到下一次要提前触发计算的时间
        ReducingState<Long> earlyFireTimestampState = ctx.getPartitionedState(stateDesc);
        if (earlyFireTimestampState.get() == null) {
            // 注册下一次要提前触发的定时器
            registerNextFireTimestamp(
                    timestamp - (timestamp % interval), window, ctx, earlyFireTimestampState);
        }

        return TriggerResult.CONTINUE;
    }

    // 定时器触发计算时，回调的方法
    @Override
    public TriggerResult onEventTime(long time, W window, TriggerContext ctx) throws Exception {

        if (time == window.maxTimestamp()) {
            return TriggerResult.FIRE;
        }

        ReducingState<Long> earlyFireTimestampState = ctx.getPartitionedState(stateDesc);

        Long earlyFireTimestamp = earlyFireTimestampState.get();

        // 如果定时器触发的回调的时间和 earlyFireTimestamp 相等，则说明可以进行触发计算：先注册下一次要提前触发的定时器，然后返回 FIRE 触发计算
        if (earlyFireTimestamp != null && earlyFireTimestamp == time) {
            earlyFireTimestampState.clear();
            registerNextFireTimestamp(time, window, ctx, earlyFireTimestampState);
            return TriggerResult.FIRE;
        }

        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onProcessingTime(long time, W window, TriggerContext ctx)
            throws Exception {
        return TriggerResult.CONTINUE;
    }

    @Override
    public void clear(W window, TriggerContext ctx) throws Exception {
        ReducingState<Long> fireTimestamp = ctx.getPartitionedState(stateDesc);
        Long timestamp = fireTimestamp.get();
        if (timestamp != null) {
            ctx.deleteEventTimeTimer(timestamp);
            fireTimestamp.clear();
        }
    }

    @Override
    public boolean canMerge() {
        return true;
    }

    // 合并窗口进行合并操作时调用
    @Override
    public void onMerge(W window, OnMergeContext ctx) throws Exception {
        // 合并多个窗口中记录的下一次要触发计算的时间戳，得到合并后新的下一次要出发计算的时间戳并注册定时器
        ctx.mergePartitionedState(stateDesc);
        Long nextFireTimestamp = ctx.getPartitionedState(stateDesc).get();
        if (nextFireTimestamp != null) {
            ctx.registerEventTimeTimer(nextFireTimestamp);
        }
    }

    public static <W extends Window> EarlyFireEventTimeTriggerV2<W> of(Time interval) {
        return new EarlyFireEventTimeTriggerV2<>(interval.toMilliseconds());
    }

    private static class Min implements ReduceFunction<Long> {
        private static final long serialVersionUID = 1L;

        @Override
        public Long reduce(Long value1, Long value2) throws Exception {
            return Math.min(value1, value2);
        }
    }

    private void registerNextFireTimestamp(
            long time, W window, TriggerContext ctx, ReducingState<Long> fireTimestampState)
            throws Exception {
        // 下一次要触发的时间戳肯定是在窗口内小于等于窗口最大时间戳的一个时间
        long nextFireTimestamp = Math.min(time + interval, window.maxTimestamp());
        fireTimestampState.add(nextFireTimestamp);
        ctx.registerEventTimeTimer(nextFireTimestamp);
    }
}
