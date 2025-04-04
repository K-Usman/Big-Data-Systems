package de.ddm.actors;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.DispatcherSelector;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.cluster.typed.ClusterSingleton;
import akka.cluster.typed.SingletonActor;
import de.ddm.actors.patterns.Reaper;
import de.ddm.actors.profiling.DataProvider;
import de.ddm.actors.profiling.DependencyWorker;
import de.ddm.serialization.AkkaSerializable;
import de.ddm.singletons.SystemConfigurationSingleton;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

public class Worker extends AbstractBehavior<Worker.Message> {

	////////////////////
	// Actor Messages //
	////////////////////

	public interface Message extends AkkaSerializable {
	}

	@NoArgsConstructor
	public static class ShutdownMessage implements Message {
		private static final long serialVersionUID = 7516129288777469221L;
	}

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "worker";

	public static Behavior<Message> create() {
		return Behaviors.setup(Worker::new);
	}

	private Worker(ActorContext<Message> context) {
		super(context);

		Reaper.watchWithDefaultReaper(this.getContext().getSelf());
		getContext().getLog().info("this is spawned in:"+SystemConfigurationSingleton.get().getRole());
		this.role=SystemConfigurationSingleton.get().getRole();
		// Create a single DataProvider actor
		ActorRef<DataProvider.Message> providerProxy = context.spawn(
				DataProvider.create(role), // Passing null initially
				"DataProvider"
		);
		context.getLog().info("Single DataProvider actor created");
		final int numWorkers = SystemConfigurationSingleton.get().getNumWorkers();
		this.workers = new ArrayList<>(numWorkers);
		for (int id = 0; id < numWorkers; id++) {
			ActorRef<DependencyWorker.Message> dependencyWorkerRef = context.spawn(
					DependencyWorker.create(role),
					DependencyWorker.DEFAULT_NAME + "_" + id,
					DispatcherSelector.fromConfig("akka.worker-pool-dispatcher")
			);
			this.workers.add(dependencyWorkerRef);
//			ClusterSingleton singleton=ClusterSingleton.get(this.getContext().getSystem());
//			ActorRef<DataProvider.Message> providerProxy= singleton.init(SingletonActor.of(DataProvider.create(dependencyWorkerRef,role),"DataProvider"));
			;
			// Send DependencyWorker reference to the single DataProvider actor

			providerProxy.tell(new DataProvider.SetDependencyWorkerReference(dependencyWorkerRef));
			context.getLog().info("Sent DependencyWorker reference: {} to DataProvider", dependencyWorkerRef.path());
		}


	}

	/////////////////
	// Actor State //
	/////////////////

	final List<ActorRef<DependencyWorker.Message>> workers;
	private final String role;

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive<Message> createReceive() {
		return newReceiveBuilder()
				.onMessage(ShutdownMessage.class, this::handle)
				.build();
	}

	private Behavior<Message> handle(ShutdownMessage message) {
		// If we expect the system to still be active when the a ShutdownMessage is issued,
		// we should propagate this ShutdownMessage to all active child actors so that they
		// can end their protocols in a clean way. Simply stopping this actor also stops all
		// child actors, but in a hard way!
		return Behaviors.stopped();
	}
}