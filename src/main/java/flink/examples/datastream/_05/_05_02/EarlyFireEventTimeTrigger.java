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

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.Window;

public class EarlyFireEventTimeTrigger<W extends Window> extends Trigger<Object, W> {

    // 在一个窗口内提前触发的时间间隔
    private final long interval;

    // 将下一次要触发的时间戳记录在 State 中
    private final ValueStateDescriptor<Long> earlyFireTimestampValueState =
            new ValueStateDescriptor<>("early-fire-timestamp", Long.class);

    private EarlyFireEventTimeTrigger(long interval) {
        this.interval = interval;
    }

    @Override
    public TriggerResult onElement(Object element, long timestamp, W window, TriggerContext ctx)
            throws Exception {

        if (window.maxTimestamp() <= ctx.getCurrentWatermark()) {
            return TriggerResult.FIRE;
        } else {
            ctx.registerEventTimeTimer(window.maxTimestamp());
        }
        ValueState<Long> earlyFireTimestampState = ctx.getPartitionedState(earlyFireTimestampValueState);
        if (earlyFireTimestampState.value() == null) {
            // 如果为是当前key下，当前窗口的第一条数据，则注册下一次要触发的定时器
            registerNextFireTimestamp(
                    timestamp - (timestamp % interval), window, ctx, earlyFireTimestampState);
        }
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onEventTime(long time, W window, TriggerContext ctx) throws Exception {

        if (time == window.maxTimestamp()) {
            return TriggerResult.FIRE;
        }

        ValueState<Long> earlyFireTimestampValue = ctx.getPartitionedState(earlyFireTimestampValueState);

        Long fireTimestamp = earlyFireTimestampValue.value();

        if (fireTimestamp != null && fireTimestamp == time) {
            // 如果状态中存储的要提前触发的时间戳和当前事件时间回调的定时器的时间戳一样，则需要将下次要触发的时间戳注册到定时器（Timer）中，并返回 FIRE 触发窗口的计算
            earlyFireTimestampValue.clear();
            registerNextFireTimestamp(time, window, ctx, earlyFireTimestampValue);
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
        ValueState<Long> fireTimestamp = ctx.getPartitionedState(earlyFireTimestampValueState);
        Long timestamp = fireTimestamp.value();
        if (timestamp != null) {
            ctx.deleteEventTimeTimer(timestamp);
            fireTimestamp.clear();
        }
    }

    @Override
    public boolean canMerge() {
        return true;
    }

    @Override
    public void onMerge(W window, OnMergeContext ctx) throws Exception {
        Long nextFireTimestamp = ctx.getPartitionedState(earlyFireTimestampValueState).value();
        if (nextFireTimestamp != null) {
            ctx.registerEventTimeTimer(nextFireTimestamp);
        }
    }

    @Override
    public String toString() {
        return "ContinuousEventTimeTrigger(" + interval + ")";
    }

    public static <W extends Window> EarlyFireEventTimeTrigger<W> of(Time interval) {
        return new EarlyFireEventTimeTrigger<>(interval.toMilliseconds());
    }

    private void registerNextFireTimestamp(
            long time, W window, TriggerContext ctx, ValueState<Long> earlyFireTimestampValue)
            throws Exception {
        long nextFireTimestamp = Math.min(time + interval, window.maxTimestamp());
        earlyFireTimestampValue.update(nextFireTimestamp);
        ctx.registerEventTimeTimer(nextFireTimestamp);
    }
}
