package de.ddm.actors.profiling;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.receptionist.Receptionist;
import de.ddm.serialization.AkkaSerializable;
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
	public static class WelcomeMessage implements Message {
		private static final long serialVersionUID = -6140766679442014678L;
		ActorRef<DependencyMiner.Message> dependencyMiner;
		ActorRef<DataProvider.Message> dataProvider;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class DataMessage implements Message {
		private static final long serialVersionUID = 1395380056668200978L;
		File[] inputFiles;
		String[][] headerLines;
		String[][][] columns;
		List<List<Set<String>>> distinctColumns;;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class TaskMessage implements Message {
		private static final long serialVersionUID = -4667745204456518160L;
		int lhsFile;
		int[] lhsAttr;
		int rhsFile;
		int[] rhsAttr;

		@Override
		public String toString() {
			return "TaskMessage{" +
					"lhsFile=" + lhsFile +
					", rhsFile=" + rhsFile +
					", lhsAttr=" + Arrays.toString(lhsAttr) +
					", rhsAttr=" + Arrays.toString(rhsAttr) +
					'}';
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;
			TaskMessage that = (TaskMessage) o;
			return lhsFile == that.lhsFile &&
					rhsFile == that.rhsFile &&
					Arrays.equals(lhsAttr, that.lhsAttr) &&
					Arrays.equals(rhsAttr, that.rhsAttr);
		}

		@Override
		public int hashCode() {
			int result = Objects.hash(lhsFile, rhsFile);
			result = 31 * result + Arrays.hashCode(lhsAttr);
			result = 31 * result + Arrays.hashCode(rhsAttr);
			return result;
		}
	}

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "dependencyWorker";

	public static Behavior<Message> create(ActorRef<DataProviderProxy.Message> dataProviderProxy) {
		return Behaviors.setup(context -> new DependencyWorker(context, dataProviderProxy));
	}

	private DependencyWorker(ActorContext<Message> context, ActorRef<DataProviderProxy.Message> dataProviderProxy) {
		super(context);

		final ActorRef<Receptionist.Listing> listingResponseAdapter = context.messageAdapter(Receptionist.Listing.class, ReceptionistListingMessage::new);
		context.getSystem().receptionist().tell(Receptionist.subscribe(DependencyMiner.dependencyMinerService, listingResponseAdapter));

		this.dataProviderProxy = dataProviderProxy;
	}

	/////////////////
	// Actor State //
	/////////////////

	private ActorRef<DependencyMiner.Message> dependencyMiner;
	private final ActorRef<DataProviderProxy.Message> dataProviderProxy;

	private String[][][] columns;
	private List<List<Set<String>>> distinctColumns;

	private TaskMessage pendingTask;

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive<Message> createReceive() {
		return newReceiveBuilder()
				.onMessage(ReceptionistListingMessage.class, this::handle)
				.onMessage(WelcomeMessage.class, this::handle)
				.onMessage(DataMessage.class, this::handle)
				.onMessage(TaskMessage.class, this::handle)
				.build();
	}

	private Behavior<Message> handle(ReceptionistListingMessage message) {
		Set<ActorRef<DependencyMiner.Message>> dependencyMiners = message.getListing().getServiceInstances(DependencyMiner.dependencyMinerService);
		for (ActorRef<DependencyMiner.Message> dependencyMiner : dependencyMiners)
			dependencyMiner.tell(new DependencyMiner.RegistrationMessage(this.getContext().getSelf()));
		return this;
	}

	private Behavior<Message> handle(WelcomeMessage message) {
		this.dataProviderProxy.tell(new DataProviderProxy.GetDataMessage(this.getContext().getSelf(), message.getDataProvider()));
		this.dependencyMiner = message.getDependencyMiner();
		return this;
	}

	private Behavior<Message> handle(DataMessage message) {
		this.columns = message.getColumns();
		this.distinctColumns = message.getDistinctColumns();

		if (this.pendingTask != null) {
			TaskMessage pendingTask = this.pendingTask;
			this.pendingTask = null;
			return this.handle(pendingTask);
		}
		return this;
	}

	private Behavior<Message> handle(TaskMessage message) {
		if (this.distinctColumns == null) {
			this.pendingTask = message;
			return this;
		}

		boolean validity;
		if (message.getLhsAttr().length == 1) {
			// Validate unary IND candidate

			Set<String> lhs = this.distinctColumns.get(message.getLhsFile()).get(message.getLhsAttr()[0]);
			Set<String> rhs = this.distinctColumns.get(message.getRhsFile()).get(message.getRhsAttr()[0]);

			validity = rhs.containsAll(lhs);
		}
		else {
			// Validate n-ary IND candidate

			//validity = this.validateNaryBruteForce(message);
			validity = this.validateNaryWithListStrategy(message);
		}

		this.dependencyMiner.tell(new DependencyMiner.CompletionMessage(message, validity));
		return this;
	}

	private boolean validateNaryBruteForce(TaskMessage message) {
		String[][] lhsColumns = this.columns[message.getLhsFile()];
		String[][] rhsColumns = this.columns[message.getRhsFile()];

		boolean found, match;
		boolean validity = true;
		for (int lhsRecordIndex = 0; lhsRecordIndex < lhsColumns[0].length; lhsRecordIndex++) {
			found = false;
			for (int rhsRecordIndex = 0; rhsRecordIndex < rhsColumns[0].length; rhsRecordIndex++) {
				match = true;
				for (int attrIndex = 0; attrIndex < message.getLhsAttr().length; attrIndex++) {
					int lhsAttrIndex = message.getLhsAttr()[attrIndex];
					int rhsAttrIndex = message.getRhsAttr()[attrIndex];

					if (!lhsColumns[lhsAttrIndex][lhsRecordIndex].equals(rhsColumns[rhsAttrIndex][rhsRecordIndex])) {
						match = false;
						break;
					}
				}
				if (match) {
					found = true;
					break;
				}
			}
			if (!found) {
				validity = false;
				break;
			}
		}
		return validity;
	}

	private boolean validateNaryWithSetStrategy(TaskMessage message) {
		String[][] lhsColumns = this.columns[message.getLhsFile()];
		String[][] rhsColumns = this.columns[message.getRhsFile()];

		Set<String[]> lhsDistinctValues = new HashSet<>();
		Set<String[]> rhsDistinctValues = new HashSet<>();

		for (int lhsRecordIndex = 0; lhsRecordIndex < lhsColumns[0].length; lhsRecordIndex++) {
			String[] value = new String[lhsColumns.length];
			for (int attrIndex = 0; attrIndex < message.getLhsAttr().length; attrIndex++)
				value[attrIndex] = lhsColumns[attrIndex][lhsRecordIndex];
			lhsDistinctValues.add(value);
		}

		for (int rhsRecordIndex = 0; rhsRecordIndex < rhsColumns[0].length; rhsRecordIndex++) {
			String[] value = new String[rhsColumns.length];
			for (int attrIndex = 0; attrIndex < message.getRhsAttr().length; attrIndex++)
				value[attrIndex] = rhsColumns[attrIndex][rhsRecordIndex];
			rhsDistinctValues.add(value);
		}

		return rhsDistinctValues.containsAll(lhsDistinctValues);
	}

	private boolean validateNaryWithListStrategy(TaskMessage message) {
		String[][] lhsColumns = this.columns[message.getLhsFile()];
		String[][] rhsColumns = this.columns[message.getRhsFile()];

		String[][] lhsValues = new String[lhsColumns[0].length][];
		String[][] rhsValues = new String[rhsColumns[0].length][];

		for (int lhsRecordIndex = 0; lhsRecordIndex < lhsColumns[0].length; lhsRecordIndex++) {
			String[] value = new String[lhsColumns.length];
			for (int attrIndex = 0; attrIndex < message.getLhsAttr().length; attrIndex++)
				value[attrIndex] = lhsColumns[attrIndex][lhsRecordIndex];
			lhsValues[lhsRecordIndex] = value;
		}

		for (int rhsRecordIndex = 0; rhsRecordIndex < rhsColumns[0].length; rhsRecordIndex++) {
			String[] value = new String[rhsColumns.length];
			for (int attrIndex = 0; attrIndex < message.getRhsAttr().length; attrIndex++)
				value[attrIndex] = rhsColumns[attrIndex][rhsRecordIndex];
			rhsValues[rhsRecordIndex] = value;
		}

		Arrays.sort(lhsValues, (o1, o2) -> Arrays.compare(o1, o2));
		Arrays.sort(rhsValues, (o1, o2) -> Arrays.compare(o1, o2));

		int lhsIndex = 0;
		int rhsIndex = 0;
		while (lhsIndex < lhsValues.length) {
			String[] lhsValue = lhsValues[lhsIndex];

			while (lhsIndex + 1 < lhsValues.length && Arrays.equals(lhsValue, lhsValues[lhsIndex + 1]))
				lhsIndex++;

			boolean match = false;
			while (rhsIndex < rhsValues.length) {
				String[] rhsValue = lhsValues[lhsIndex];

				while (rhsIndex + 1 < rhsValues.length && Arrays.equals(rhsValue, rhsValues[rhsIndex + 1]))
					rhsIndex++;

				int compare = Arrays.compare(rhsValue, lhsValue);
				rhsIndex++;
				if (compare < 0) {
					continue;
				}
				else if (compare == 0) {
					match = true;
					break;
				}
				else {
					break;
				}
			}

			if (!match)
				return false;

			lhsIndex++;
		}
		return true;
	}
}
