package de.ddm.actors.profiling;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import de.ddm.actors.patterns.LargeMessageProxy;
import de.ddm.serialization.AkkaSerializable;
import de.ddm.singletons.InputConfigurationSingleton;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DataProvider extends AbstractBehavior<DataProvider.Message> {

	////////////////////
	// Actor Messages //
	////////////////////

	public interface Message extends AkkaSerializable, LargeMessageProxy.LargeMessage {
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class LoadDataMessage implements Message {
		private static final long serialVersionUID = 8565569801149974148L;
		ActorRef<DependencyMiner.Message> dependencyMiner;
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
	public static class GetDataRemoteMessage implements Message {
		private static final long serialVersionUID = 8146257395224871081L;
		ActorRef<LargeMessageProxy.Message> replyTo;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class GetDataLocalMessage implements Message {
		private static final long serialVersionUID = -6622192254872970559L;
		ActorRef<DataProviderProxy.Message> replyTo;
	}

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "dataProvider";

	public static Behavior<Message> create() {
		return Behaviors.setup(DataProvider::new);
	}

	private DataProvider(ActorContext<Message> context) {
		super(context);

		this.largeMessageProxy = this.getContext().spawn(LargeMessageProxy.create(this.getContext().getSelf().unsafeUpcast()), LargeMessageProxy.DEFAULT_NAME);
	}

	/////////////////
	// Actor State //
	/////////////////

	private final ActorRef<LargeMessageProxy.Message> largeMessageProxy;

	private File[] inputFiles;
	private String[][] headerLines;
	private String[][][] columns;

	private Map<Integer, List<List<String[]>>> batches;
	private List<ActorRef<InputReader.Message>> inputReaders;

	private ActorRef<DependencyMiner.Message> dependencyMiner;

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive<DataProvider.Message> createReceive() {
		return newReceiveBuilder()
				.onMessage(LoadDataMessage.class, this::handle)
				.onMessage(HeaderMessage.class, this::handle)
				.onMessage(BatchMessage.class, this::handle)
				.onMessage(GetDataLocalMessage.class, this::handle)
				.onMessage(GetDataRemoteMessage.class, this::handle)
				.build();
	}

	private Behavior<Message> handle(LoadDataMessage message) {
		this.dependencyMiner = message.getDependencyMiner();

		this.inputFiles = InputConfigurationSingleton.get().getInputFiles();
		this.headerLines = new String[this.inputFiles.length][];
		this.columns = new String[this.inputFiles.length][][];

		this.batches = new HashMap<>();
		this.inputReaders = new ArrayList<>(inputFiles.length);
		for (int id = 0; id < this.inputFiles.length; id++) {
			this.batches.put(Integer.valueOf(id), new ArrayList<>());
			this.inputReaders.add(this.getContext().spawn(InputReader.create(id, this.inputFiles[id]), InputReader.DEFAULT_NAME + "_" + id));
		}

		for (ActorRef<InputReader.Message> inputReader : this.inputReaders)
			inputReader.tell(new InputReader.ReadHeaderMessage(this.getContext().getSelf()));
		for (ActorRef<InputReader.Message> inputReader : this.inputReaders)
			inputReader.tell(new InputReader.ReadBatchMessage(this.getContext().getSelf()));
		return this;
	}

	private Behavior<Message> handle(HeaderMessage message) {
		this.headerLines[message.getId()] = message.getHeader();
		return this;
	}

	private Behavior<Message> handle(BatchMessage message) {
		if (message.getBatch().size() != 0) {
			this.inputReaders.get(message.getId()).tell(new InputReader.ReadBatchMessage(this.getContext().getSelf()));
			this.batches.get(message.getId()).add(message.getBatch());
			return this;
		}

		int fileId = message.getId();
		int numColumns = this.headerLines[fileId].length;
		int numRecords = this.batches.get(fileId).stream().map(a -> a.size()).reduce(0, (a, b) -> a + b);
		this.columns[fileId] = new String[numColumns][];

		for (int columnId = 0; columnId < numColumns; columnId++)
			this.columns[fileId][columnId] = new String[numRecords];

		int recordId = 0;
		for (List<String[]> batch : this.batches.get(fileId)) {
			for (String[] record : batch) {
				for (int columnId = 0; columnId < numColumns; columnId++)
					this.columns[fileId][columnId][recordId] = record[columnId];
				recordId++;
			}
		}
		this.batches.remove(fileId);

		if (this.isReadingDone()) {
			this.dependencyMiner.tell(new DependencyMiner.DataLoadingCompletedMessage(this.inputFiles, this.headerLines));
			this.dependencyMiner = null;
			this.batches = null;
			this.inputReaders = null;
		}
		return this;
	}

	private boolean isReadingDone() {
		for (int i = 0; i < this.inputFiles.length; i++) {
			if (this.headerLines[i] == null)
				return false;
			if (this.columns[i] == null)
				return false;
		}
		return true;
	}

	private Behavior<Message> handle(GetDataLocalMessage message) {
		DataProviderProxy.DataMessage dataMessage = new DataProviderProxy.DataMessage(this.inputFiles, this.headerLines, this.columns);
		message.getReplyTo().tell(dataMessage);
		return this;
	}

	private Behavior<Message> handle(GetDataRemoteMessage message) {
		LargeMessageProxy.LargeMessage dataMessage = new DataProviderProxy.DataMessage(this.inputFiles, this.headerLines, this.columns);
		this.largeMessageProxy.tell(new LargeMessageProxy.SendMessage(dataMessage, message.getReplyTo()));
		return this;
	}
}
