package org.abogdanov.storm_examples.wordcount.spouts;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

public class LocalFileReaderSpout extends BaseRichSpout {

	private SpoutOutputCollector collector;
	private FileReader fileReader;
	private boolean completed = false;
	private TopologyContext context;

	/**
	 * Method open() is called at the beginning.
	 * It reads the path to the input file from inputFile
	 * configuration variable and tries to open this file.
	 * Also it gets the collector object.
	 */
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		try {
			this.context = context;
			this.fileReader = new FileReader(conf.get("inputFile").toString());
		} catch (FileNotFoundException e) {
			throw new RuntimeException("Error reading file " + conf.get("inputFile"));
		}
		this.collector = collector;
	}

	/**
	 * Method nextTuple() is used to read each line
	 * from the input file and emit it to Storm.
	 */
	@Override
	public void nextTuple() {

		/**
		 * It should be called forever so if we reach the end of file
		 * we should wait and return
		 */
		if (completed) {
			try {
				Thread.sleep(10);
			} catch (InterruptedException ignored) {
				// Do nothing
			}
			return;
		}

		String str;

		// Open the reader
		BufferedReader reader = new BufferedReader(fileReader);
		try {
			// Read all lines
			while ((str = reader.readLine()) != null) {
				/**
				 * By each line emit a new value with the line as a their
				 */
				this.collector.emit(new Values(str));
			}
		} catch (Exception e) {
			throw new RuntimeException("Error reading tuple", e);
		} finally {
			/**
			 * Mark the file as completed
			 */
			completed = true;
		}
	}

	/**
	 * Declare that LocalFileReaderSpout is going to emit field "line"
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("line"));
	}

	/**
	 * Close an open file
	 */
	@Override
	public void close() {
		try {
			fileReader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Storm has determined that the tuple emitted by this
	 * spout with the msgId identifier has been fully processed.
	 */
	@Override
	public void ack(Object msgId) {
		System.out.println("OK:"+msgId);
	}

	/**
	 * The tuple emitted by this spout with the msgId
	 * identifier has failed to be fully processed.
	 */
	@Override
	public void fail(Object msgId) {
		System.out.println("FAIL:"+msgId);
	}

}