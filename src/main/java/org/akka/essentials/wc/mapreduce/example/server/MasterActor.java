package org.akka.essentials.wc.mapreduce.example.server;

import org.akka.essentials.wc.mapreduce.example.common.*;

import akka.actor.*;

public class MasterActor extends UntypedActor {
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

		System.out.println("MasterActor -> onReceive(" + message + ")");
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
