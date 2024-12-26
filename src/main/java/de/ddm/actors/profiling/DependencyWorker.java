package de.ddm.actors.profiling;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.receptionist.Receptionist;
import de.ddm.actors.patterns.LargeMessageProxy;
import de.ddm.serialization.AkkaSerializable;
import de.ddm.singletons.InputConfigurationSingleton;
import de.ddm.structures.InclusionDependency;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.io.File;
import java.util.*;

public class DependencyWorker extends AbstractBehavior<DependencyWorker.Message> {

	////////////////////
	// Actor Messages //
	////////////////////

	public interface Message extends AkkaSerializable {
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class ReceptionistListingMessage implements Message {
		private static final long serialVersionUID = -5246338806092216222L;
		Receptionist.Listing listing;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class TaskMessage implements Message {
		private static final long serialVersionUID = -4667745204456518160L;
//		ActorRef<LargeMessageProxy.Message> dependencyMinerLargeMessageProxy;
		ActorRef<DependencyMiner.Message> dependencyMinerRef;
		String[][] header;
		List<List<String[]>> batches;
	}

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "dependencyWorker";

	public static Behavior<Message> create() {
		return Behaviors.setup(DependencyWorker::new);
	}

	private DependencyWorker(ActorContext<Message> context) {
		super(context);
		this.inputFiles = InputConfigurationSingleton.get().getInputFiles();
		/* from slide 37, same thing happening here. Worker is being registered to receptionist. here is connecting
		 to DependencyMiner not master */
		final ActorRef<Receptionist.Listing> listingResponseAdapter = context.messageAdapter(Receptionist.Listing.class, ReceptionistListingMessage::new);
		context.getSystem().receptionist().tell(Receptionist.subscribe(DependencyMiner.dependencyMinerService, listingResponseAdapter));

		this.largeMessageProxy = this.getContext().spawn(LargeMessageProxy.create(this.getContext().getSelf().unsafeUpcast()), LargeMessageProxy.DEFAULT_NAME);
	}

	/////////////////
	// Actor State //
	/////////////////

	private final ActorRef<LargeMessageProxy.Message> largeMessageProxy;
	private final File[] inputFiles;

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive<Message> createReceive() {
		return newReceiveBuilder()
				.onMessage(ReceptionistListingMessage.class, this::handle)
				.onMessage(TaskMessage.class, this::handle)
				.build();
	}

	//get a list of actor refs of actors registered to DependencyMiner service.
	private Behavior<Message> handle(ReceptionistListingMessage message) {
//		Set<ActorRef<DependencyMiner.Message>> dependencyMiners = message.getListing().getServiceInstances(DependencyMiner.dependencyMinerService);
//		for (ActorRef<DependencyMiner.Message> dependencyMiner : dependencyMiners)
			// this dependencyMiner(like Master) will then notify other registered actors about this new actors
			//availability
//			dependencyMiner.tell(new DependencyMiner.RegistrationMessage(this.getContext().getSelf()));
		return this;
	}

	private Behavior<Message> handle(TaskMessage message) {
		ActorRef<DependencyMiner.Message> dependencyMinerRef = message.getDependencyMinerRef();
		Random random = new Random();
		this.getContext().getLog().info("I am Working!");
		// I should probably know how to solve this task, but for now I just pretend some work...

		//simulating random stuff here. Here IND discovery may be performed.
		this.getContext().getLog().info("Processing data batch for task.");

		String[][] headers = message.getHeader();
		List<List<String[]>> batches = message.getBatches();
		List<InclusionDependency> inds = new ArrayList<>();

		for (int dependent = 0; dependent < this.inputFiles.length; dependent++) {
			File dependentFile = this.inputFiles[dependent];
			List<String[]> dependentBatch = batches.get(dependent);
			int dependentColumns = dependentBatch.stream().mapToInt(row -> row.length).max().orElse(0);

			for (int referenced = 0; referenced < this.inputFiles.length; referenced++) {
				File referencedFile = this.inputFiles[referenced];
				List<String[]> referencedBatch = batches.get(referenced);
				int referencedColumns = referencedBatch.stream().mapToInt(row -> row.length).max().orElse(0);

				// Check for inclusion dependencies
				for (int i = 0; i < dependentColumns; i++) {
					Set<String> dependentColumnValues = new HashSet<>();
					for (String[] row : dependentBatch) {
						if (i < row.length) {
							dependentColumnValues.add(row[i]);
						}
					}

					for (int j = 0; j < referencedColumns; j++) {
						// Skip self-dependencies when checking intra-file INDs
						if (dependent == referenced && i == j) {
							continue;
						}

						Set<String> referencedColumnValues = new HashSet<>();
						for (String[] row : referencedBatch) {
							if (j < row.length) {
								referencedColumnValues.add(row[j]);
							}
						}

						// Check for inclusion dependency
						if (!dependentColumnValues.isEmpty() && !referencedColumnValues.isEmpty()
								&& referencedColumnValues.containsAll(dependentColumnValues)) {
							String[] dependentAttributes = {headers[dependent][i]};
							String[] referencedAttributes = {headers[referenced][j]};

							inds.add(new InclusionDependency(dependentFile, dependentAttributes, referencedFile, referencedAttributes));

							if (dependent == referenced) {
								getContext().getLog().info("Intra-file IND found in file " + dependent +
										" between columns " + i + " and " + j);
							} else {
								getContext().getLog().info("Inter-file IND found between file " + dependent +
										" column " + i + " and file " + referenced + " column " + j);
							}
						}
					}
				}
			}
		}
		dependencyMinerRef.tell(new DependencyMiner.CompletionMessage(this.getContext().getSelf(), inds));
		return this;
	}
}