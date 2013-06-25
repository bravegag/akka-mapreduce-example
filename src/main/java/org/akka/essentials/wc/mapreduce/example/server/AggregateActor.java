package org.akka.essentials.wc.mapreduce.example.server;

import java.io.*;
import java.util.*;

import org.akka.essentials.wc.mapreduce.example.common.*;

import akka.actor.*;

public class AggregateActor extends UntypedActor {
	private int completedTasksCount = 0;
	private TaskInfo taskInfo = null;
	private SortedMap<String, Integer> finalReducedMap = new TreeMap<String, Integer>();

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void onReceive(Object message) throws Exception {
		System.out.println("AggregateActor -> onReceive(" + message + ")");
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
		System.out.println("completedTasksCount=" + completedTasksCount);
		if (taskInfo != null)
			System.out.println("taskInfo#numberOfTasks=" + taskInfo.getNumberOfTasks());
		if (taskInfo != null && completedTasksCount >= taskInfo.getNumberOfTasks()) {
			PrintStream out = null;
			try {
				out = new PrintStream(new FileOutputStream(
						"/Users/bravegag/Downloads/finaloutput.log"));
				out.print(finalReducedMap.toString());
			}
			finally {
				if (out != null)
					out.close();
			}

			System.out.println("*** Now is really DONE!!!");
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
