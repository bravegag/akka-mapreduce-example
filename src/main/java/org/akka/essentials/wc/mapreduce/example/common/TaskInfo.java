package org.akka.essentials.wc.mapreduce.example.common;

import java.io.*;

public class TaskInfo implements Serializable {
	/**
	 * Constructor
	 *
	 * @param numberOfTasks
	 */
	public TaskInfo(int numberOfTasks) {
		this.numberOfTasks = numberOfTasks;
	}

	/**
	 * Returns the numberOfTasks
	 *
	 * @return the numberOfTasks
	 */
	public final int getNumberOfTasks() {
		return numberOfTasks;
	}

	/**
	 * Sets the numberOfTasks
	 *
	 * @param numberOfTasks
	 *            the numberOfTasks to set
	 */
	public final void setNumberOfTasks(int numberOfTasks) {
		this.numberOfTasks = numberOfTasks;
	}

	private static final long serialVersionUID = -7830878298796956108L;
	private int numberOfTasks;
}
