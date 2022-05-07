///*
// * Licensed to the Apache Software Foundation (ASF) under one
// * or more contributor license agreements.  See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership.  The ASF licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package flink.examples.datastream;
//
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.windowing.time.Time;
//import org.apache.flink.walkthrough.common.entity.Alert;
//import org.apache.flink.walkthrough.common.entity.Transaction;
//import org.apache.flink.walkthrough.common.sink.AlertSink;
//import org.apache.flink.walkthrough.common.source.TransactionSource;
//
///**
// * Skeleton code for the datastream walkthrough
// */
//public class FraudDetectionJob_3_3 {
//    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        env.setParallelism(2);
//        env.setMaxParallelism(2);
//
//        DataStream<Transaction> transactions = env
//                .addSource(new TransactionSource()) // TransactionSource 是一个 Function
//                .setParallelism(2)
//                .map(new AMapFunction()) // AMapFunction 是一个 Function
//                .setParallelism(2)
//                .setMaxParallelism()
//                .name("transactions");
//
//        DataStream<Alert> alerts = transactions
//                .keyBy(Transaction::getAccountId)
//                .timeWindow(Time.seconds(60))
//                .apply(new ApplyWindowFunction()) // ApplyWindowFunction 是一个 Function
//                .name("fraud-detector");
//
//        alerts
//                .addSink(new AlertSink()) // AlertSink 是一个 Function
//                .name("send-alerts");
//
//        env.execute("Fraud Detection");
//    }
//}
