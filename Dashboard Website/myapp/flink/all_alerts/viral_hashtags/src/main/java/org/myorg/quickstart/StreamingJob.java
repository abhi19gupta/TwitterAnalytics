package org.myorg.quickstart;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer08;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

// NOTE: System.out.print goes to the log file in Flink log folder flink-db1-taskmanager-0-db1-OptiPlex-3050.out
public class StreamingJob {

	public static long getTwitterDateMillis(String date) {

		final String TWITTER="EEE MMM dd HH:mm:ss  yyyy";
		System.out.println(date);
		SimpleDateFormat sf = new SimpleDateFormat(TWITTER,Locale.ENGLISH);
		sf.setLenient(true);
		try {
			System.out.print(sf.parse(date).getTime());
			return sf.parse(date).getTime();
		} catch (ParseException e) {
			System.out.println(0);
			System.out.println(e.toString());
			return 0;
		}
	}

	public static void main(String[] args) throws Exception {

		if (args.length != 1){
			System.err.println("Expected 1 command line argument: <alert_name>");
			return;
		}

		String jobName = args[0];

		ObjectMapper mapper = new ObjectMapper();

		Properties kafkaConsumerProperties = new Properties();
		kafkaConsumerProperties.setProperty("bootstrap.servers", "localhost:9092");
		kafkaConsumerProperties.setProperty("zookeeper.connect", "localhost:2181");
		kafkaConsumerProperties.setProperty("group.id", jobName);

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		DataStream<JsonNode> filtered_stream = env
				.addSource(new FlinkKafkaConsumer08<>("tweets_topic", new SimpleStringSchema(), kafkaConsumerProperties))
				// convert String to JsonNode type if possible and make it simpler, else leave out
				.flatMap(new FlatMapFunction<String, JsonNode>() {
					@Override
					public void flatMap(String value, Collector<JsonNode> out) {
						try {
							out.collect(mapper.readTree(value));
						} catch (Exception e) {}
					}
				})
				// filter Jsons specifying the conditions
				.filter(new FilterFunction<JsonNode>() {
					@Override
					public boolean filter(JsonNode jsonNode) {
						try {
							return true;
						} catch (Exception e) {
							return false;
						}
					}
				})
				// watermarking based on https://ci.apache.org/projects/flink/flink-docs-release-1.4/dev/event_timestamp_extractors.html
				.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<JsonNode>() {
					@Override
					public long extractAscendingTimestamp(JsonNode jsonNode) {
						// return getTwitterDateMillis(jsonNode.get("created_at").asText());
						return Long.parseLong(jsonNode.get("timestamp_ms").asText());
					}
				})
				// duplicate tweets into multiple tweets, separating the grouping entities
				.flatMap(new FlatMapFunction<JsonNode, JsonNode>() {
					@Override
					public void flatMap(JsonNode jsonNode, Collector<JsonNode> out) {
						try {
							List<String> hashtags = new ArrayList<>();
							for (final JsonNode node : jsonNode.get("entities").get("hashtags"))
								hashtags.add(node.get("text").asText());
							if (hashtags.isEmpty())
								hashtags.add("__NO_HASHTAG_FOUND__");

							for (final String hashtag : hashtags){
								ObjectNode newJsonNode = jsonNode.deepCopy();
								ObjectNode keyNode = newJsonNode.putObject("key");
								keyNode.put("hashtag",hashtag);
								out.collect(newJsonNode);
							}
						} catch (Exception e) {}
					}
				})
				.keyBy(new KeySelector<JsonNode, Object>() {
					@Override
					public Object getKey(JsonNode jsonNode) {
						try {
							return jsonNode.get("key");
						} catch (Exception e) {
							return null;
						}
					}
				})
				.window(SlidingEventTimeWindows.of(Time.seconds(60), Time.seconds(30)))
				.trigger(CountTrigger.of(3))
				.aggregate(new AggregateFunction<JsonNode, JsonNode, JsonNode>() {
					@Override
					public JsonNode createAccumulator() { return null; }

					@Override
					public JsonNode add(JsonNode jsonNode, JsonNode accumulator) {
						String tweet_id = jsonNode.get("id_str").asText();
						if (accumulator == null) {
							ObjectNode newAccumulator = JsonNodeFactory.instance.objectNode();
							newAccumulator.put("key", jsonNode.get("key").toString());
//							ObjectNode newAccumulator = jsonNode.get("key").deepCopy();
							newAccumulator.put("tweet_ids",tweet_id);
							return newAccumulator;
						} else {
							ObjectNode newAccumulator = accumulator.deepCopy();
							newAccumulator.put("tweet_ids",accumulator.get("tweet_ids").asText() + " " + tweet_id);
							return newAccumulator;
						}
					}

					@Override
					public JsonNode getResult(JsonNode accumulator) { return accumulator; }

					@Override
					public JsonNode merge(JsonNode a, JsonNode b) {
						if (a == null) return b;
						if (b == null) return a;
						ObjectNode newA = a.deepCopy();
						newA.put("tweet_ids", a.get("tweet_ids").asText() + " " + b.get("tweet_ids").asText());
						return newA;
					}
				}, new ProcessWindowFunction<JsonNode, JsonNode, Object, TimeWindow>() {
					@Override
					public void process(Object o, Context context, Iterable<JsonNode> accumulated, Collector<JsonNode> collector) throws Exception {
						ObjectNode accumulatedJsonNode = accumulated.iterator().next().deepCopy();
						accumulatedJsonNode.put("window_start", context.window().getStart());
						accumulatedJsonNode.put("window_end", context.window().getEnd());
						accumulatedJsonNode.put("alert_name", jobName);
						collector.collect(accumulatedJsonNode);
					}
				})
				;

		filtered_stream.addSink(new FlinkKafkaProducer08<>("localhost:9092", "alerts_topic", new SerializationSchema<JsonNode>() {
			@Override
			public byte[] serialize(JsonNode jsonNode) {
				try {
					return mapper.writeValueAsBytes(jsonNode);
				} catch (JsonProcessingException e) {
					return new byte[0];
				}
			}
		}));
		// filtered_stream.writeAsText(jobName+"_log.txt", FileSystem.WriteMode.OVERWRITE);

		// execute program
		env.execute(jobName);
	}
}