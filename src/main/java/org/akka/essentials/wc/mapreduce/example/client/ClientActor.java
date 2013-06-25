package org.akka.essentials.wc.mapreduce.example.client;

import java.util.*;

import org.akka.essentials.wc.mapreduce.example.common.*;

import akka.actor.*;

public class ClientActor extends UntypedActor {
	private ActorRef remoteServer = null;
	private final List<String> dataBuffer = new ArrayList<String>();
	private TaskInfo infoBuffer = null;
	private final ActorSystem system;
	@SuppressWarnings("unused")
	private ActorRef fileReadActor = null;
	private long start;

	/**
	 * Constructor
	 */
	public ClientActor(String remotePath, ActorSystem system) {
		this.system = system;
		sendIdentifyRequest(remotePath);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void onReceive(Object message) throws Exception {
		if (message instanceof ActorIdentity) {
			System.out.println("onReceive() actor identity received");
			remoteServer = ((ActorIdentity) message).getRef();

			// flush the buffers
			for (int i = dataBuffer.size() - 1; i >= 0; i--) {
				System.out.println("onReceive() sending to remote server");
				remoteServer.tell(dataBuffer.remove(i), getSelf());
			}
			if (infoBuffer != null) {
				System.out.println("onReceive() buffer sending info");
				remoteServer.tell(infoBuffer, getSelf());
				infoBuffer = null;
			}

		} else
		if (remoteServer == null) {
			if (message instanceof TaskInfo) {
				System.out.println("onReceive() info buffering");
				infoBuffer = (TaskInfo) message;

			} else {
				System.out.println("onReceive() data buffering");
				dataBuffer.add((String) message);
			}
		} else
		if (message instanceof TaskInfo) {
			System.out.println("onReceive() directly sending info");
			remoteServer.tell(message, getSelf());

		} else
		if (message instanceof ShutdownInfo) {
			System.out.println("onReceive() Shutdown signal received");
			system.shutdown();

		} else {
			System.out.println("onReceive() sending to remote server");
			remoteServer.tell(message, getSelf());
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void preStart() {
		start = System.currentTimeMillis();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void postStop() {
		// tell the world that the calculation is complete
		long timeSpent = (System.currentTimeMillis() - start) / 1000;
		System.out.println(String.format(
				"\n\tClientActor estimate: \t\t\n\tCalculation time: \t%s Secs", timeSpent));
	}

	private void sendIdentifyRequest(String remotePath) {
		getContext().actorSelection(remotePath).tell(new Identify(remotePath), getSelf());
	}
}
