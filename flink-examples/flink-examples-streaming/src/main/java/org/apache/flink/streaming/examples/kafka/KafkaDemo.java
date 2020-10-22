package org.apache.flink.streaming.examples.kafka;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import com.esotericsoftware.minlog.Log;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * filebeat -> kafka -> flink -> kafka.
 */
public class KafkaDemo {
	private static final Logger LOG = LoggerFactory.getLogger(KafkaDemo.class);

	public static void main(String[] args) throws Exception {
		String inputTopic = "apache2-log";
		String outputTopic = "apache2-log-output";
//		String consumerGroup = "kafka-flink-group";
		String address = "10.10.10.97:9092";

		// Flink Stream execution
		StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, address);
//		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
		properties.setProperty(
				ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
				StringDeserializer.class.getName());
		properties.setProperty(
				ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
				StringDeserializer.class.getName());

		// Define input consumer
		FlinkKafkaConsumer<ObjectNode> consumer = new FlinkKafkaConsumer<>(
				inputTopic,
				new JSONKeyValueDeserializationSchema(false),
				properties);

		// Define output producer
		FlinkKafkaProducer<String> producer = new FlinkKafkaProducer<>(
				outputTopic,
				new SimpleStringSchema(),
				properties);

		// Attach consumer to input data stream
		DataStream<String> stringInputStream = environment
				.addSource(consumer)
				.map(new MapFunction<ObjectNode, String>() {
					@Override
					public String map(ObjectNode value) throws Exception {
						Log.info(value.toPrettyString());
						return value.get("value").get("message").asText();
//						return "ping";
					}
				});

		// Map process
		/*DataStream<Tuple2<String, Integer>> mapper = stringInputStream
				.map(value -> new Tuple2<>(value, Integer.valueOf(1)))
				.returns(Types.TUPLE(Types.STRING, Types.INT));

		// Reduce process
		DataStream<Tuple2<String, Integer>> reducer = mapper
				.keyBy(0)
				.sum(1);

		DataStream<String> output = reducer.map(tuple -> tuple.f0 + " -> " + tuple.f1);

		// Attach producer to output data stream
		output.addSink(producer);*/
		stringInputStream.addSink(producer);

		environment.execute("Kafka Flink");
	}
}
