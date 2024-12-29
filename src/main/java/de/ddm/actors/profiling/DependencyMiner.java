package de.ddm.actors.profiling;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.Terminated;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.receptionist.Receptionist;
import akka.actor.typed.receptionist.ServiceKey;
import de.ddm.actors.patterns.LargeMessageProxy;
import de.ddm.serialization.AkkaSerializable;
import de.ddm.singletons.InputConfigurationSingleton;
import de.ddm.singletons.SystemConfigurationSingleton;
import de.ddm.structures.InclusionDependency;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class DependencyMiner extends AbstractBehavior<DependencyMiner.Message> {

	////////////////////
	// Actor Messages //
	////////////////////

	public interface Message extends AkkaSerializable, LargeMessageProxy.LargeMessage {
	}

	@NoArgsConstructor
	public static class StartMessage implements Message {
		private static final long serialVersionUID = -1963913294517850454L;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class HeaderMessage implements Message {
		private static final long serialVersionUID = -5322425954432915838L;
		int id;
		String[] header;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class BatchMessage implements Message {
		private static final long serialVersionUID = 4591192372652568030L;
		int id;
		List<String[]> batch;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class RegistrationMessage implements Message {
		private static final long serialVersionUID = -4025238529984914107L;
		ActorRef<DataProvider.Message> dataProvider;
		String role;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class GetWorkerRef implements Message {
		private static final long serialVersionUID = 425842132825518251L;
		ActorRef<DependencyWorker.Message> dependencyWorkerRef;
		String role;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class CompletionMessage implements Message {
		private static final long serialVersionUID = -7642425159675583598L;
		ActorRef<DependencyWorker.Message> dependencyWorker;
		List<InclusionDependency> inclusionDependencies;
	}

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "dependencyMiner";

	public static final ServiceKey<DependencyMiner.Message> dependencyMinerService = ServiceKey.create(DependencyMiner.Message.class, DEFAULT_NAME + "Service");

	public static Behavior<Message> create() {
		return Behaviors.setup(DependencyMiner::new);
	}

	// with the creation of dependencyminer, other actors inputreader,resultcollector and largemessageproxy will be spawned.
	private DependencyMiner(ActorContext<Message> context) {
		super(context);
		this.role="";
		this.discoverNaryDependencies = SystemConfigurationSingleton.get().isHardMode();
		this.inputFiles = InputConfigurationSingleton.get().getInputFiles();
		this.headerLines = new String[this.inputFiles.length][];
		this.batchLines = new ArrayList<>(this.inputFiles.length);
		for (int i = 0; i < this.inputFiles.length; i++) {
			this.batchLines.add(new ArrayList<>()); // Initialize each batch list
		}

		this.inputReaders = new ArrayList<>(inputFiles.length);
		for (int id = 0; id < this.inputFiles.length; id++)
			this.inputReaders.add(context.spawn(InputReader.create(id, this.inputFiles[id]), InputReader.DEFAULT_NAME + "_" + id));
		this.resultCollector = context.spawn(ResultCollector.create(), ResultCollector.DEFAULT_NAME);
		this.largeMessageProxy = this.getContext().spawn(LargeMessageProxy.create(this.getContext().getSelf().unsafeUpcast()), LargeMessageProxy.DEFAULT_NAME);

		this.dataProviders = new ArrayList<>();
		this.dependencyWorkers = new ArrayList<>();

		context.getSystem().receptionist().tell(Receptionist.register(dependencyMinerService, context.getSelf()));
	}

	/////////////////
	// Actor State //
	/////////////////

	private long startTime;

	private final boolean discoverNaryDependencies;
	private final File[] inputFiles;
	private final String[][] headerLines;
	private final List<List<String[]>> batchLines;

	private final List<ActorRef<InputReader.Message>> inputReaders;
	private final ActorRef<ResultCollector.Message> resultCollector;
	private final ActorRef<LargeMessageProxy.Message> largeMessageProxy;
	private final String role;
	private final List<ActorRef<DataProvider.Message>> dataProviders;
	private final List<ActorRef<DependencyWorker.Message>> dependencyWorkers;


	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive<Message> createReceive() {
		return newReceiveBuilder()
				.onMessage(StartMessage.class, this::handle)
				.onMessage(BatchMessage.class, this::handle)
				.onMessage(HeaderMessage.class, this::handle)
				.onMessage(RegistrationMessage.class, this::handle)
				.onMessage(CompletionMessage.class, this::handle)
				.onMessage(GetWorkerRef.class, this::handle)
				.onSignal(Terminated.class, this::handle)
				.build();
	}

	/* sends ReaderHeaderMessage and ReadBatchMessage to input reader actors to start reading
	files and data in batches. this is happening in handle(ReadHeaderMessage/ReadBatchMessage message) */
	private Behavior<Message> handle(StartMessage message) {
		for (ActorRef<InputReader.Message> inputReader : this.inputReaders)
			inputReader.tell(new InputReader.ReadHeaderMessage(this.getContext().getSelf()));
		for (ActorRef<InputReader.Message> inputReader : this.inputReaders)
			inputReader.tell(new InputReader.ReadBatchMessage(this.getContext().getSelf(), 10000));
		this.startTime = System.currentTimeMillis();
		return this;
	}
	// here it will store the headers received from inputReader in response.
	private Behavior<Message> handle(HeaderMessage message) {
		this.headerLines[message.getId()] = message.getHeader();
		return this;
	}

	private Behavior<Message> handle(BatchMessage message) {
		// Ignoring batch content for now ... but I could do so much with it.
		int id = message.getId();
		List<String[]> batch = message.getBatch();
		if (id < batchLines.size()) {
			batchLines.get(id).addAll(batch); // Add all rows to the corresponding batch list
		} else {
			getContext().getLog().info("Received batch for invalid id: {}", id);
		}


//		System.out.println(MemoryUtils.byteSizeOf(message.getBatch()));
//		System.out.println(MemoryUtils.bytesMax() + "    " + MemoryUtils.bytesFree());

// here it will check if the current batch is empty then it will request a new batch.
		if (!message.getBatch().isEmpty())
			this.inputReaders.get(message.getId()).tell(new InputReader.ReadBatchMessage(this.getContext().getSelf(), 10000));
		return this;
	}
	//register or add the dependency worker to the list dependencyWorkers
	private Behavior<Message> handle(RegistrationMessage message) {
		ActorRef<DataProvider.Message> dataProvider = message.getDataProvider();
		String role=message.getRole();
		if (!this.dataProviders.contains(dataProvider)) {
			this.dataProviders.add(dataProvider);
			this.getContext().watch(dataProvider);
			// The worker should get some work ... let me send her something before I figure out what I actually want from her.
			// I probably need to idle the worker for a while, if I do not have work for it right now ... (see master/worker pattern)
			getContext().getLog().info("Number of registered Data Providers {}",String.valueOf(dataProviders.size()));
//			getContext().getLog().info(role);
			if(role.equals("worker")) {
				getContext().getLog().info("Sending data to Data Provider from Dep Miner");
				if (this.headerLines == null) {
					getContext().getLog().info("Header is null");
				}
				dataProvider.tell(new DataProvider.AssignHeadersMessage(this.getContext().getSelf(), this.headerLines));
				if (this.batchLines == null) {
					getContext().getLog().info("Batches are empty");
				}
				dataProvider.tell(new DataProvider.AssignBatchMessage(this.getContext().getSelf(), this.batchLines));
				if (this.inputFiles == null) {
					getContext().getLog().info("InputFiles are empty");
				}
				dataProvider.tell(new DataProvider.GetFiles(this.getContext().getSelf(), this.inputFiles));
			}else {
				getContext().getLog().info("Data Provider from worker actor system not yet joined");
			}
		}
		return this;
	}

	private Behavior<Message> handle(GetWorkerRef message){
		ActorRef<DependencyWorker.Message> dependencyWorker = message.getDependencyWorkerRef();
		String role=message.getRole();
		if (!this.dependencyWorkers.contains(dependencyWorker)) {
			this.dependencyWorkers.add(dependencyWorker);
			this.getContext().watch(dependencyWorker);
			getContext().getLog().info("Received DependencyWorker ref.");


		}
		return this;
	}
	private Behavior<Message> handle(CompletionMessage message) {
		ActorRef<DependencyWorker.Message> dependencyWorker = message.getDependencyWorker();
		// If this was a reasonable result, I would probably do something with it and potentially generate more work ... for now, let's just generate a random, binary IND.
		List<InclusionDependency> result= message.getInclusionDependencies();
		this.resultCollector.tell(new ResultCollector.ResultMessage(result));

		// I still don't know what task the worker could help me to solve ... but let me keep her busy.
		// Once I found all unary INDs, I could check if this.discoverNaryDependencies is set to true and try to detect n-ary INDs as well!

//		dependencyWorker.tell(new DependencyWorker.TaskMessage(this.largeMessageProxy, 42));

		// At some point, I am done with the discovery. That is when I should call my end method. Because I do not work on a completable task yet, I simply call it after some time.
		if(result.size()>55){
			this.end();
		}
		return this;
	}

	private void end() {
		this.resultCollector.tell(new ResultCollector.FinalizeMessage());
		long discoveryTime = System.currentTimeMillis() - this.startTime;
		this.getContext().getLog().info("Finished mining within {} ms!", discoveryTime);
	}

	private Behavior<Message> handle(Terminated signal) {
		ActorRef<DependencyWorker.Message> dependencyWorker = signal.getRef().unsafeUpcast();
		this.dataProviders.remove(dependencyWorker);
		return this;
	}
}