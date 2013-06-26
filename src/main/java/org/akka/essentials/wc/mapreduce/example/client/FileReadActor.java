package org.akka.essentials.wc.mapreduce.example.client;

import java.io.*;

import org.akka.essentials.wc.mapreduce.example.common.*;

import akka.actor.*;
import akka.event.*;

public class FileReadActor extends UntypedActor {
	final LoggingAdapter logger = Logging.getLogger(getContext().system(), this);

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void onReceive(Object message) throws Exception {
		if (message instanceof String) {
			String fileName = (String) message;
			try {
				BufferedReader reader = new BufferedReader(
						new InputStreamReader(Thread.currentThread().getContextClassLoader()
								.getResource(fileName).openStream()));
				String line = null;
				int numberOfTasks = 0;
				while ((line = reader.readLine()) != null) {
					getSender().tell(line, getSelf());
					numberOfTasks++;
				}
				logger.info("All lines send !");

				getSender().tell(new TaskInfo(numberOfTasks), getSelf());
			}
			catch (IOException x) {
				logger.error("IOException: %s%n", x);
			}
		}
		else
			throw new IllegalArgumentException("Unknown message [" + message + "]");
	}
}