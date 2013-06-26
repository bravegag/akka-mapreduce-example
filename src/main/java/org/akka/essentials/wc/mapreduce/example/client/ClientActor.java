package org.akka.essentials.wc.mapreduce.example.client;

import java.util.*;

import org.akka.essentials.wc.mapreduce.example.common.*;

import akka.actor.*;
import akka.event.*;

public class ClientActor extends UntypedActor {
	final LoggingAdapter logger = Logging.getLogger(getContext().system(), this);

	private ActorRef remoteServer = null;
	private final List<String> dataBuffer = new ArrayList<String>();
	private TaskInfo infoBuffer = null;
	@SuppressWarnings("unused")
	private ActorRef fileReadActor = null;
	private long start;

	/**
	 * Constructor
	 */
	public ClientActor(String remotePath) {
		sendIdentifyRequest(remotePath);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void onReceive(Object message) throws Exception {
		if (message instanceof ActorIdentity) {
			logger.info("actor identity received");
			remoteServer = ((ActorIdentity) message).getRef();

			// flush the buffers
			for (int i = dataBuffer.size() - 1; i >= 0; i--) {
				logger.info("sending to remote server");
				remoteServer.tell(dataBuffer.remove(i), getSelf());
			}
			if (infoBuffer != null) {
				logger.info("buffer sending info");
				remoteServer.tell(infoBuffer, getSelf());
				infoBuffer = null;
			}

		} else
		if (remoteServer == null) {
			if (message instanceof TaskInfo) {
				logger.info("info buffering");
				infoBuffer = (TaskInfo) message;

			} else {
				logger.info("data buffering");
				dataBuffer.add((String) message);
			}
		} else
		if (message instanceof TaskInfo) {
			logger.info("directly sending info");
			remoteServer.tell(message, getSelf());

		} else
		if (message instanceof ShutdownInfo) {
			logger.info("Shutdown signal received");
			getContext().system().shutdown();

		} else {
			logger.info("sending to remote server");
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
		logger.info(String.format(
				"\n\tClientActor estimate: \t\t\n\tCalculation time: \t%s Secs", timeSpent));
	}

	private void sendIdentifyRequest(String remotePath) {
		getContext().actorSelection(remotePath).tell(new Identify(remotePath), getSelf());
	}
}
