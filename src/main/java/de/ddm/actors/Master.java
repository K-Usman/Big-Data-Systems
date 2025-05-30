package de.ddm.actors;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.DispatcherSelector;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import de.ddm.actors.patterns.Reaper;
import de.ddm.actors.profiling.DependencyMiner;
import de.ddm.serialization.AkkaSerializable;
import lombok.NoArgsConstructor;

public class Master extends AbstractBehavior<Master.Message> {

	////////////////////
	// Actor Messages //
	////////////////////
	//Master.Message is the interface and we have start and shutdown message. Thats all this actor can do.
	// We have to figure out what it should with start and shutdown messages.


	public interface Message extends AkkaSerializable {
	}

	@NoArgsConstructor
	public static class StartMessage implements Message {
		private static final long serialVersionUID = -1963913294517850454L;
	}

	@NoArgsConstructor
	public static class ShutdownMessage implements Message {
		private static final long serialVersionUID = 7516129288777469221L;
	}

	////////////////////////
	// Actor Construction //
	////////////////////////
	//Every actor should define its own name how it would like to be spawned in the actor hierarchy


	public static final String DEFAULT_NAME = "master";
//Then we have this factory method,this method simply calls the constructor.

	public static Behavior<Message> create() {
		return Behaviors.setup(Master::new);
	}
	//We forwards the start message to the dependency miner to start some action.

	private Master(ActorContext<Message> context) {
		super(context);
		Reaper.watchWithDefaultReaper(this.getContext().getSelf());

		this.dependencyMiner = context.spawn(
				DependencyMiner.create(),
				DependencyMiner.DEFAULT_NAME,
				DispatcherSelector.fromConfig("akka.master-pinned-dispatcher"));
	}

	/////////////////
	// Actor State //
	/////////////////

	private final ActorRef<DependencyMiner.Message> dependencyMiner;

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive<Message> createReceive() {
		return newReceiveBuilder()
				.onMessage(StartMessage.class, this::handle)
				.onMessage(ShutdownMessage.class, this::handle)
				.build();
	}

	private Behavior<Message> handle(StartMessage message) {
		this.dependencyMiner.tell(new DependencyMiner.StartMessage());
		return this;
	}

	private Behavior<Message> handle(ShutdownMessage message) {
		// If we expect the system to still be active when a ShutdownMessage is issued,
		// we should propagate this ShutdownMessage to all active child actors so that they
		// can end their protocols in a clean way. Simply stopping this actor also stops all
		// child actors, but in a hard way!
		return Behaviors.stopped();
	}
}