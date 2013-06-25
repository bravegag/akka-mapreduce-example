package org.akka.essentials.wc.mapreduce.example.client;

import java.io.*;

import org.akka.essentials.wc.mapreduce.example.common.*;

import akka.actor.*;

public class FileReadActor extends UntypedActor {
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
				System.out.println("All lines send !");

				getSender().tell(new TaskInfo(numberOfTasks), getSelf());
			}
			catch (IOException x) {
				System.err.format("IOException: %s%n", x);
			}
		}
		else
			throw new IllegalArgumentException("Unknown message [" + message + "]");
	}
}