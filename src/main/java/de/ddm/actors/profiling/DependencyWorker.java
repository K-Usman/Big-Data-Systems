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
		int dependent = 0;
		int referenced = 4;
		File dependentFile = this.inputFiles[dependent];
		File referencedFile = this.inputFiles[referenced];
//		String[] dependentAttributes = {headers[dependent][random.nextInt(headers[dependent].length)]};
//		String[] referencedAttributes = {headers[referenced][random.nextInt(headers[referenced].length)]};
		List<String[]> batch0 = batches.get(0); // Batch 0
		List<String[]> batch3 = batches.get(4); // Batch 3

		// Variables to store the resulting dependent and referenced indices
		int dependentIndex = 0;
		int referencedIndex = 0;

		// Find the number of columns in each batch
		int dependentColumns = batch0.stream().mapToInt(row -> row.length).max().orElse(0);
		int referencedColumns = batch3.stream().mapToInt(row -> row.length).max().orElse(0);

		for (int i = 0; i < dependentColumns; i++) {
			// Collect all values from the current column of batch0, ignoring missing columns
			Set<String> dependentColumnValues = new HashSet<>();
			for (String[] row : batch0) {
				if (i < row.length) { // Ensure column exists in the current row
					dependentColumnValues.add(row[i]);
				}
			}

			// Iterate over each column in the referenced file
			for (int j = 0; j < referencedColumns; j++) {
				// Collect all values from the current column of batch3, ignoring missing columns
				Set<String> referencedColumnValues = new HashSet<>();
				for (String[] row : batch3) {
					if (j < row.length) { // Ensure column exists in the current row
						referencedColumnValues.add(row[j]);
					}
				}

				getContext().getLog().info(dependentColumnValues.toString());
				getContext().getLog().info(referencedColumnValues.toString());
				// Check for inclusion dependency
				if (!dependentColumnValues.isEmpty() && dependentColumnValues.containsAll(referencedColumnValues)) {
					// Store the indices of the dependent and referenced columns
					dependentIndex = i;
					referencedIndex = j;
					break; // Exit the loop once a dependency is found
				}
			}
		}
		String[] dependentAttributes={headers[dependent][dependentIndex]};
		String[] referencedAttributes={headers[referenced][referencedIndex]};

		InclusionDependency ind = new InclusionDependency(dependentFile,dependentAttributes, referencedFile, referencedAttributes);
		List<InclusionDependency> inds = new ArrayList<>(1);
		inds.add(ind);
		dependencyMinerRef.tell(new DependencyMiner.CompletionMessage(this.getContext().getSelf(), inds));
		return this;
	}
}