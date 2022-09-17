package flink.examples.datastream._04._4_33.schema;

import org.apache.flink.api.common.serialization.SerializationSchema;

import flink.examples.datastream._04._3_4.protobuf.KafkaSourceModel;

public class ProtobufSerializationSchema implements SerializationSchema<KafkaSourceModel> {

    @Override
    public byte[] serialize(KafkaSourceModel element) {
        return element.toByteArray();
    }
}
