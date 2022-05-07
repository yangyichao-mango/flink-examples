package flink.examples.datastream._04._3_4.schema;

import java.io.IOException;

import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;

import flink.examples.datastream._04._3_4.protobuf.KafkaSourceModel;

public class ProtobufAbstractDeserializationSchema extends AbstractDeserializationSchema<KafkaSourceModel> {

    @Override
    public KafkaSourceModel deserialize(byte[] bytes) throws IOException {
        return KafkaSourceModel.parseFrom(bytes);
    }
}
