package flink.examples.datastream._04._3_4.schema;

import java.io.IOException;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import flink.examples.datastream._04._3_4.protobuf.KafkaSourceModel;

public class ProtobufDeserializationSchema implements DeserializationSchema<KafkaSourceModel> {
    @Override
    public KafkaSourceModel deserialize(byte[] bytes) throws IOException {
        return KafkaSourceModel.parseFrom(bytes);
    }

    @Override
    public boolean isEndOfStream(KafkaSourceModel kafkaSourceModel) {
        return false;
    }

    @Override
    public TypeInformation<KafkaSourceModel> getProducedType() {
        return TypeInformation.of(KafkaSourceModel.class);
    }
}
