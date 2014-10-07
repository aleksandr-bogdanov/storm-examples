package org.abogdanov.storm_examples.wordcount.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class WordNormalizerBolt extends BaseRichBolt {

	private OutputCollector collector;

	/**
	 * Initialize the collector
	 */
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	/**
	 * Method execute() will process all input tuples.
	 * It will lowercase all letters in line and split it to separate words
	 */
	@Override
	public void execute(Tuple input) {
		/**
		 * Get sentences from input tuple (index 0), split to words,
		 * lowercase them and then emit.
		 */
		String sentence = input.getString(0);
		String[] words = sentence.split(" ");
		for (String word : words) {
			word = word.trim();
			if (!word.isEmpty()) {
				word = word.toLowerCase();
				// Emit the word
				List<Tuple> a = new ArrayList<>();
				a.add(input);
				collector.emit(a, new Values(word));
			}
		}
		// Acknowledge the input
		collector.ack(input);
	}

	/**
	 * Declare that the bolt will only emit the field "word"
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}

}