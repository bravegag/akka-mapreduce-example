package org.akka.essentials.wc.mapreduce.example.server;

import java.io.*;
import java.util.*;

import org.akka.essentials.wc.mapreduce.example.common.*;

import akka.actor.*;
import akka.event.*;

public class AggregateActor extends UntypedActor {
	final LoggingAdapter logger = Logging.getLogger(getContext().system(), this);

	private int completedTasksCount = 0;
	private TaskInfo taskInfo = null;
	private SortedMap<String, Integer> finalReducedMap = new TreeMap<String, Integer>();

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void onReceive(Object message) throws Exception {
		if (message instanceof Map) {
			completedTasksCount++;
			@SuppressWarnings("unchecked")
			Map<String, Integer> reducedList = (Map<String, Integer>) message;
			aggregateInMemoryReduce(reducedList);
		}
		else if (message instanceof TaskInfo) {
			taskInfo = (TaskInfo) message;
		}

		// final outcome
		logger.info("completedTasksCount=" + completedTasksCount);
		if (taskInfo != null)
			logger.info("taskInfo#numberOfTasks=" + taskInfo.getNumberOfTasks());
		if (taskInfo != null && completedTasksCount >= taskInfo.getNumberOfTasks()) {
			PrintStream out = null;
			try {
				out = new PrintStream(new FileOutputStream("finaloutput.log"));
				out.print(finalReducedMap.toString());
			}
			finally {
				if (out != null)
					out.close();
			}

			logger.info("*** now is really done!");
		}
	}

	private void aggregateInMemoryReduce(Map<String, Integer> reducedList) {
		Iterator<String> iter = reducedList.keySet().iterator();
		while (iter.hasNext()) {
			String key = iter.next();
			if (finalReducedMap.containsKey(key)) {
				Integer count = reducedList.get(key) + finalReducedMap.get(key);
				finalReducedMap.put(key, count);
			}
			else {
				finalReducedMap.put(key, reducedList.get(key));
			}

		}
	}
}
