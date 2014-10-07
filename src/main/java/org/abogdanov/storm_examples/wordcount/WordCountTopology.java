package org.abogdanov.storm_examples.wordcount;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import org.abogdanov.storm_examples.wordcount.bolts.WordCounterBolt;
import org.abogdanov.storm_examples.wordcount.bolts.WordNormalizerBolt;
import org.abogdanov.storm_examples.wordcount.spouts.LocalFileReaderSpout;


public class WordCountTopology {

	public static void main(String[] args) throws Exception {

		if (args.length != 1) {
			System.err.println("Usage: WordCountTopology <inputFile>");
			System.exit(1);
		}

		// Config definition
		Config conf = new Config();
		conf.put("inputFile", args[0]);
		conf.setDebug(true);

		// Topology definition
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("local-file-reader-spout", new LocalFileReaderSpout());
		builder.setBolt("word-normalizer", new WordNormalizerBolt())
				.shuffleGrouping("local-file-reader-spout");
		builder.setBolt("word-counter", new WordCounterBolt())
				.shuffleGrouping("word-normalizer");

		// Run topology
		conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("WordCountTopology", conf, builder.createTopology());
		Thread.sleep(10000);
		cluster.shutdown();

	}

}