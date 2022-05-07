// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: kafka_source.proto

package protobuf;

public interface KafkaSourceModelOrBuilder extends
    // @@protoc_insertion_point(interface_extends:flink.KafkaSourceModel)
    com.google.protobuf.MessageOrBuilder {

  /**
   * <code>optional string name = 1;</code>
   */
  String getName();
  /**
   * <code>optional string name = 1;</code>
   */
  com.google.protobuf.ByteString
      getNameBytes();

  /**
   * <code>repeated string names = 2;</code>
   */
  java.util.List<String>
      getNamesList();
  /**
   * <code>repeated string names = 2;</code>
   */
  int getNamesCount();
  /**
   * <code>repeated string names = 2;</code>
   */
  String getNames(int index);
  /**
   * <code>repeated string names = 2;</code>
   */
  com.google.protobuf.ByteString
      getNamesBytes(int index);

  /**
   * <code>map&lt;string, int32&gt; si_map = 7;</code>
   */
  int getSiMapCount();
  /**
   * <code>map&lt;string, int32&gt; si_map = 7;</code>
   */
  boolean containsSiMap(
          String key);
  /**
   * Use {@link #getSiMapMap()} instead.
   */
  @Deprecated
  java.util.Map<String, Integer>
  getSiMap();
  /**
   * <code>map&lt;string, int32&gt; si_map = 7;</code>
   */
  java.util.Map<String, Integer>
  getSiMapMap();
  /**
   * <code>map&lt;string, int32&gt; si_map = 7;</code>
   */

  int getSiMapOrDefault(
          String key,
          int defaultValue);
  /**
   * <code>map&lt;string, int32&gt; si_map = 7;</code>
   */

  int getSiMapOrThrow(
          String key);
}
