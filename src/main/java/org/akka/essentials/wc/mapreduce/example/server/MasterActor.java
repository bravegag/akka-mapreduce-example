package org.akka.essentials.wc.mapreduce.example.server;

import org.akka.essentials.wc.mapreduce.example.common.*;

import akka.actor.*;
import akka.event.*;

public class MasterActor extends UntypedActor {
	final LoggingAdapter logger = Logging.getLogger(getContext().system(), this);

	private final ActorRef mapActor;
	private final ActorRef aggregateActor;
	private ActorRef remoteActor;

	public MasterActor(ActorRef aggregateActor, ActorRef mapActor) {
		this.mapActor = mapActor;
		this.aggregateActor = aggregateActor;
	}

	public void onReceive(Object message) {
		if (remoteActor == null) {
			remoteActor = getSender();
		}

		logger.info("MasterActor -> onReceive(" + message + ")");
		if (message instanceof TaskInfo) {
			aggregateActor.tell(message, getSelf());

			// kill remoteActor
			remoteActor.tell(new ShutdownInfo(), getSelf());

		} else
		if (message instanceof String) {
			mapActor.tell(message, getSelf());
		}
	}
}
