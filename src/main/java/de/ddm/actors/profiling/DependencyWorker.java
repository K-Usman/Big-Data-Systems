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
		List<File> files;
	}



	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "dependencyWorker";

	public static Behavior<Message> create(String role) {
		return Behaviors.setup(context -> new DependencyWorker(context, role));
	}

	private DependencyWorker(ActorContext<Message> context,String role) {
		super(context);
		this.role=role;
//		this.inputFiles = new File[0];
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
	private final String role;
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
		Set<ActorRef<DependencyMiner.Message>> dependencyMiners = message.getListing().getServiceInstances(DependencyMiner.dependencyMinerService);
		for (ActorRef<DependencyMiner.Message> dependencyMiner : dependencyMiners)
//			 this dependencyMiner(like Master) will then notify other registered actors about this new actors
//			availability
			dependencyMiner.tell(new DependencyMiner.GetWorkerRef(this.getContext().getSelf(),role));
			getContext().getLog().info("This dependencyWorker is spawned in {}",role);
			getContext().getLog().info("Dep Worker ref {} has been sent to DepMiner",this.getContext().getSelf().path());
		return this;
	}



	private Behavior<Message> handle(TaskMessage message) {
		ActorRef<DependencyMiner.Message> dependencyMinerRef = message.getDependencyMinerRef();
		String[][] headers = message.getHeader();
		getContext().getLog().info("Headers are {}", Arrays.deepToString(headers));
		List<List<String[]>> batches = message.getBatches();
		List<File> inputFiles = message.getFiles();
		getContext().getLog().info("Input files are {}", inputFiles);

		getContext().getLog().info("Batches in DW are : ");
		batches.forEach(batche -> {
			getContext().getLog().info("Batch : ");
			for (String[] batchItem : batche) {
				getContext().getLog().info("  " + String.join(", ", batchItem));
			}
		});
		List<InclusionDependency> inds = new ArrayList<>();

		if (!inputFiles.isEmpty() && headers.length == 2 && batches.size() == 2) {
			this.getContext().getLog().info("Start working");
			this.getContext().getLog().info("Number of Files received: {}", inputFiles.size());
			// Prepare dependent and referenced pairs
			File dependentFile = inputFiles.get(0);
			List<String[]> dependentBatch = batches.get(0);
			String[] dependentHeaders = headers[0];

			File referencedFile = inputFiles.get(1);
			List<String[]> referencedBatch = batches.get(1);
			String[] referencedHeaders = headers[1];

			int dependentColumns = dependentBatch.stream().mapToInt(row -> row.length).max().orElse(0);
			int referencedColumns = referencedBatch.stream().mapToInt(row -> row.length).max().orElse(0);

			// Check for inclusion dependencies between the pair
			for (int i = 0; i < dependentColumns; i++) {
				Set<String> dependentColumnValues = new HashSet<>();
				for (String[] row : dependentBatch) {
					if (i < row.length) {
						dependentColumnValues.add(row[i]);
					}
				}

				for (int j = 0; j < referencedColumns; j++) {
					if (dependentFile == referencedFile && i == j) {
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
						String[] dependentAttributes = {dependentHeaders[i]};
						String[] referencedAttributes = {referencedHeaders[j]};

						InclusionDependency newInd = new InclusionDependency(dependentFile, dependentAttributes, referencedFile, referencedAttributes);

						// Use the register method to ensure uniqueness
						if (newInd.register()) {
							inds.add(newInd);

							getContext().getLog().info("IND found: {} (Dependent from {}) -> {} (Referenced from {})",
									dependentAttributes[0], dependentFile.getName(),
									referencedAttributes[0], referencedFile.getName());
						} else {
							getContext().getLog().info("Duplicate IND found, not adding: {} (Dependent from {}) -> {} (Referenced from {})",
									dependentAttributes[0], dependentFile.getName(),
									referencedAttributes[0], referencedFile.getName());
						}
					}
				}
			}

			dependencyMinerRef.tell(new DependencyMiner.CompletionMessage(this.getContext().getSelf(), inds));
		} else {
			this.getContext().getLog().info("Invalid data received. Ensure exactly two pairs of headers, batches, and files.");
		}

		return this;
	}}


