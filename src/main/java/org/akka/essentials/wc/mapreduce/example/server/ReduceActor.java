package org.akka.essentials.wc.mapreduce.example.server;

import java.util.*;
import java.util.concurrent.*;

import akka.actor.*;

public class ReduceActor extends UntypedActor {
	private ActorRef aggregateActor = null;

	public ReduceActor(ActorRef aggregateActor) {
		this.aggregateActor = aggregateActor;
	}

	@Override
	public void onReceive(Object message) throws Exception {
		System.out.println("ReduceActor -> onReceive(" + message + ")");
		if (message instanceof List) {
			@SuppressWarnings("unchecked")
			List<Result> work = (List<Result>) message;

			// perform the work
			NavigableMap<String, Integer> reducedList = reduce(work);

			// reply with the result
			aggregateActor.tell(reducedList, getSelf());
		}
		else
			throw new IllegalArgumentException("Unknown message [" + message + "]");
	}

	private NavigableMap<String, Integer> reduce(List<Result> list) {
		NavigableMap<String, Integer> reducedMap = new ConcurrentSkipListMap<String, Integer>();

		Iterator<Result> iter = list.iterator();
		while (iter.hasNext()) {
			Result result = iter.next();
			if (reducedMap.containsKey(result.getWord())) {
				Integer value = (Integer) reducedMap.get(result.getWord());
				value++;
				reducedMap.put(result.getWord(), value);
			}
			else {
				reducedMap.put(result.getWord(), Integer.valueOf(1));
			}
		}
		return reducedMap;
	}
}
