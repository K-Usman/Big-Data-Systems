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
import de.ddm.serialization.AkkaSerializable;
import de.ddm.singletons.SystemConfigurationSingleton;
import de.ddm.structures.InclusionDependency;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.io.File;
import java.util.*;

public class DependencyMiner extends AbstractBehavior<DependencyMiner.Message> {

	////////////////////
	// Actor Messages //
	////////////////////

	public interface Message extends AkkaSerializable {
	}

	@NoArgsConstructor
	public static class StartMessage implements Message {
		private static final long serialVersionUID = -1963913294517850454L;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class DataLoadingCompletedMessage implements Message {
		private static final long serialVersionUID = -4184858160945792991L;
		private File[] inputFiles;
		private String[][] headerLines;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class RegistrationMessage implements Message {
		private static final long serialVersionUID = -4025238529984914107L;
		ActorRef<DependencyWorker.Message> dependencyWorker;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class CompletionMessage implements Message {
		private static final long serialVersionUID = -7642425159675583598L;
		DependencyWorker.TaskMessage task;
		boolean validity;
	}

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "dependencyMiner";

	public static final ServiceKey<DependencyMiner.Message> dependencyMinerService = ServiceKey.create(DependencyMiner.Message.class, DEFAULT_NAME + "Service");

	public static Behavior<Message> create() {
		return Behaviors.setup(DependencyMiner::new);
	}

	private DependencyMiner(ActorContext<Message> context) {
		super(context);

		this.discoverNaryDependencies = SystemConfigurationSingleton.get().isHardMode();

		this.dataProvider = context.spawn(DataProvider.create(), DataProvider.DEFAULT_NAME);
		this.resultCollector = context.spawn(ResultCollector.create(), ResultCollector.DEFAULT_NAME);

		this.dependencyWorkers = new ArrayList<>();

		context.getSystem().receptionist().tell(Receptionist.register(dependencyMinerService, context.getSelf()));
	}

	/////////////////
	// Actor State //
	/////////////////

	private long startTime;

	private final boolean discoverNaryDependencies;

	private File[] inputFiles;
	private String[][] headerLines;

	private final List<List<DependencyWorker.TaskMessage>> inds = new ArrayList<>();
	private int arity = 0;

	private final ActorRef<DataProvider.Message> dataProvider;
	private final ActorRef<ResultCollector.Message> resultCollector;

	private final List<ActorRef<DependencyWorker.Message>> dependencyWorkers;

	private final Queue<DependencyWorker.TaskMessage> unassignedTasks = new LinkedList<>();
	private final Queue<ActorRef<DependencyWorker.Message>> idleWorkers = new LinkedList<>();
	private final Map<DependencyWorker.TaskMessage, ActorRef<DependencyWorker.Message>> task2WorkerAssignments = new HashMap<>();

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive<Message> createReceive() {
		return newReceiveBuilder()
				.onMessage(StartMessage.class, this::handle)
				.onMessage(DataLoadingCompletedMessage.class, this::handle)
				.onMessage(RegistrationMessage.class, this::handle)
				.onMessage(CompletionMessage.class, this::handle)
				.onSignal(Terminated.class, this::handle)
				.build();
	}

	private Behavior<Message> handle(StartMessage message) {
		this.getContext().getLog().info("Received " + message.toString());

		this.startTime = System.currentTimeMillis();
		this.dataProvider.tell(new DataProvider.LoadDataMessage(this.getContext().getSelf()));
		return this;
	}

	private Behavior<Message> handle(DataLoadingCompletedMessage message) {
		this.getContext().getLog().info("Received " + message.toString());

		this.inputFiles = message.getInputFiles();
		this.headerLines = message.getHeaderLines();

		for (ActorRef<DependencyWorker.Message> dependencyWorker : this.dependencyWorkers)
			dependencyWorker.tell(new DependencyWorker.WelcomeMessage(this.getContext().getSelf(), this.dataProvider));

		// Generate unary INDs
		this.generateCandidates();

		// Assign tasks
		this.idleWorkers.addAll(this.dependencyWorkers);
		this.assignTasks();

		return this;
	}

	private void assignTasks() {
		while (!this.idleWorkers.isEmpty() && !this.unassignedTasks.isEmpty()) {
			ActorRef<DependencyWorker.Message> worker = this.idleWorkers.remove();
			DependencyWorker.TaskMessage task = this.unassignedTasks.remove();
			this.task2WorkerAssignments.put(task, worker);
			worker.tell(task);
		}
	}

	private void assignTaskOrIdle(ActorRef<DependencyWorker.Message> worker) {
		if (this.unassignedTasks.isEmpty()) {
			this.idleWorkers.add(worker);
		}
		else {
			DependencyWorker.TaskMessage task = this.unassignedTasks.remove();
			this.task2WorkerAssignments.put(task, worker);
			worker.tell(task);
		}
	}

	private Behavior<Message> handle(RegistrationMessage message) {
		this.getContext().getLog().info("Received " + message.toString());

		ActorRef<DependencyWorker.Message> dependencyWorker = message.getDependencyWorker();
		if (!this.dependencyWorkers.contains(dependencyWorker)) {
			this.dependencyWorkers.add(dependencyWorker);
			this.getContext().watch(dependencyWorker);

			if (this.inputFiles != null) {
				dependencyWorker.tell(new DependencyWorker.WelcomeMessage(this.getContext().getSelf(), this.dataProvider));

				this.assignTaskOrIdle(dependencyWorker);
			}
		}
		return this;
	}

	private Behavior<Message> handle(CompletionMessage message) {
		this.getContext().getLog().info("Received " + message.toString());

		ActorRef<DependencyWorker.Message> dependencyWorker = this.task2WorkerAssignments.remove(message.getTask());
		this.assignTaskOrIdle(dependencyWorker);

		if (message.isValidity())
			this.inds.get(this.arity - 1).add(message.getTask());

		if (this.task2WorkerAssignments.isEmpty()) {
			if (this.discoverNaryDependencies) {
				this.generateCandidates();
				if (!this.unassignedTasks.isEmpty()) {
					this.assignTasks();
					return this;
				}
			}
			return this.end();
		}
		return this;
	}

	private Behavior<Message> end() {
		List<InclusionDependency> result = new ArrayList<>();
		for (List<DependencyWorker.TaskMessage> taskMessages : this.inds)
			for (DependencyWorker.TaskMessage taskMessage : taskMessages)
				result.add(new InclusionDependency(taskMessage, this.headerLines, this.inputFiles));
		this.resultCollector.tell(new ResultCollector.ResultMessage(result));
		this.resultCollector.tell(new ResultCollector.FinalizeMessage());

		long discoveryTime = System.currentTimeMillis() - this.startTime;
		this.getContext().getLog().info("Finished mining within {} ms!", discoveryTime);

		return this;
	}

	private void generateCandidates() {
		this.arity++;
		this.inds.add(new ArrayList<>());

		if (this.arity == 1)
			this.generateUnaryCandidates();
		else
			this.generateNaryCandidates();
	}

	private void generateUnaryCandidates() {
		for (int lhsFile = 0; lhsFile < this.headerLines.length; lhsFile++)
			for (int rhsFile = 0; rhsFile < this.headerLines.length; rhsFile++)
				for (int lhsAttr = 0; lhsAttr < this.headerLines[lhsFile].length; lhsAttr++)
					for (int rhsAttr = 0; rhsAttr < this.headerLines[rhsFile].length; rhsAttr++)
						if (lhsFile != rhsFile || lhsAttr != rhsAttr)
							this.unassignedTasks.add(new DependencyWorker.TaskMessage(lhsFile, new int[]{lhsAttr}, rhsFile, new int[]{rhsAttr}));
	}

	private void generateNaryCandidates() {
		List<DependencyWorker.TaskMessage> naryINDs = this.inds.get(this.arity - 2);

		int[] lhs1, lhs2, rhs1, rhs2;
		boolean match, overlap;
		int previousArity = this.arity - 1;
		int previousArityIndex = this.arity - 2;
		for (DependencyWorker.TaskMessage naryIND1 : naryINDs) {
			for (DependencyWorker.TaskMessage naryIND2 : naryINDs) {
				if (naryIND1.equals(naryIND2))
					continue;

				if (naryIND1.getLhsFile() != naryIND2.getLhsFile() || naryIND1.getRhsFile() != naryIND2.getRhsFile())
					continue;

				lhs1 = naryIND1.getLhsAttr();
				lhs2 = naryIND2.getLhsAttr();
				rhs1 = naryIND1.getRhsAttr();
				rhs2 = naryIND2.getRhsAttr();
				match = true;

				for (int i = 0; i < previousArity - 1; i++) {
					if (lhs1[i] != lhs2[i] || rhs1[i] != rhs2[i]) {
						match = false;
						break;
					}
				}

				if (lhs1[previousArityIndex] == lhs2[previousArityIndex] || rhs1[previousArityIndex] == rhs2[previousArityIndex])
					match = false;

				if (!match)
					continue;

				int[] newLhs = new int[this.arity];
				int[] newRhs = new int[this.arity];

				for (int i = 0; i < previousArity - 1; i++) {
					newLhs[i] = lhs1[i];
					newRhs[i] = rhs1[i];
				}

				if (lhs1[previousArityIndex] > lhs2[previousArityIndex])
					continue;

				newLhs[previousArityIndex] = lhs1[previousArityIndex];
				newLhs[previousArity] = lhs2[previousArityIndex];
				newRhs[previousArityIndex] = rhs1[previousArityIndex];
				newRhs[previousArity] = rhs2[previousArityIndex];

				overlap = false;
				if (naryIND1.getLhsFile() == naryIND1.getRhsFile())
					for (int lhsAttr : newLhs)
						for (int rhsAttr : newRhs)
							if (lhsAttr == rhsAttr)
								overlap = true;

				if (!overlap)
					this.unassignedTasks.add(new DependencyWorker.TaskMessage(naryIND1.getLhsFile(), newLhs, naryIND1.getRhsFile(), newRhs));
			}
		}
		this.getContext().getLog().info("Generated " + this.unassignedTasks.size() + " new n-ary IND candidates.");
	}

	private Behavior<Message> handle(Terminated signal) {
		this.getContext().getLog().info("Received " + signal.toString());

		ActorRef<DependencyWorker.Message> dependencyWorker = signal.getRef().unsafeUpcast();
		this.dependencyWorkers.remove(dependencyWorker);
		if (!this.idleWorkers.remove(dependencyWorker)) {
			DependencyWorker.TaskMessage task = null;
			for (Map.Entry<DependencyWorker.TaskMessage, ActorRef<DependencyWorker.Message>> entry : this.task2WorkerAssignments.entrySet()) {
				if (entry.getValue().equals(dependencyWorker)) {
					task = entry.getKey();
					break;
				}
			}
			this.task2WorkerAssignments.remove(task);
			this.unassignedTasks.add(task);
			this.assignTasks();
		}
		return this;
	}
}