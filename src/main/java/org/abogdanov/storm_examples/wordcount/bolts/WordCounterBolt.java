package org.abogdanov.storm_examples.wordcount.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

public class WordCounterBolt extends BaseRichBolt {

	private Integer id;                 // Task ID
	private String name;                // Component ID
	private Map<String, Integer> counters;      // Map with word counters
	private OutputCollector collector;

	/**
	 * On create it fills the values
	 */
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.counters = new HashMap<>();
		this.collector = collector;
		this.id = context.getThisTaskId();
		this.name = context.getThisComponentId();
	}

	/**
	 * If map already contains current word then increment
	 * its counter in map value, else add to map with "1" as value.
	 */
	@Override
	public void execute(Tuple input) {
		/**
		 * Get the word from input tuple (index 0)
		 */
		String word = input.getString(0);
		if (!counters.containsKey(word)) {
			counters.put(word, 1);
		} else {
			Integer c = counters.get(word) + 1;
			counters.put(word, c);
		}
		// Acknowledge the input
		collector.ack(input);
	}

	/**
	 * At the end of the spout (when the cluster is shutdown
	 * We will show the word counters
	 */
	@Override
	public void cleanup() {
		System.out.println("-- Word Counter [" + name + "-" + id + "] --");
		for (Map.Entry<String, Integer> entry : counters.entrySet()) {
			System.out.println(entry.getKey() + " : " + entry.getValue());
		}
	}

	/**
	 * The bold emits no fields, left blank
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// No fields to emit, left blank.
	}

}