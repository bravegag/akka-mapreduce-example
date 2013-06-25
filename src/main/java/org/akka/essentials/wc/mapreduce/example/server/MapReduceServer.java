package org.akka.essentials.wc.mapreduce.example.server;

import akka.actor.*;
import akka.kernel.*;
import akka.routing.*;

import com.typesafe.config.*;

public class MapReduceServer implements Bootable {
	/**
	 * Constructor
	 */
	public MapReduceServer() {
		ActorSystem system = ActorSystem.create("MapReduceApp",
				ConfigFactory.load().getConfig("MapReduceApp"));

		// create the aggregate Actor
		ActorRef aggregateActor = system.actorOf(Props.create(AggregateActor.class));

		// create the list of reduce Actors
		ActorRef reduceRouter = system.actorOf(Props.create(ReduceActor.class, aggregateActor)
				.withRouter(new FromConfig()), "reduceActor");

		// create the list of map Actors
		ActorRef mapRouter = system.actorOf(Props.create(MapActor.class, reduceRouter).
				withRouter(new FromConfig()), "mapActor");

		// create the overall Master Actor that acts as the remote actor for
		// clients
		@SuppressWarnings("unused")
		ActorRef masterActor = system.actorOf(
				Props.create(MasterActor.class, aggregateActor, mapRouter), "masterActor");
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		System.out.println("starting server ... ");
		new MapReduceServer();
		System.out.println("server started");
	}

	/**
	 * {@inheritDoc}
	 */
	public void shutdown() {
		// TODO Auto-generated method stub
	}

	/**
	 * {@inheritDoc}
	 */
	public void startup() {
		// TODO Auto-generated method stub
	}
}
