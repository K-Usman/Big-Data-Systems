package de.ddm.actors.profiling;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import de.ddm.actors.patterns.LargeMessageProxy;
import de.ddm.configuration.SystemConfiguration;
import de.ddm.serialization.AkkaSerializable;
import de.ddm.singletons.SystemConfigurationSingleton;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.io.File;
import java.util.*;

public class DataProviderProxy extends AbstractBehavior<DataProviderProxy.Message> {

	////////////////////
	// Actor Messages //
	////////////////////

	public interface Message extends AkkaSerializable, LargeMessageProxy.LargeMessage {
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class GetDataMessage implements Message {
		private static final long serialVersionUID = -3571780705578600223L;
		ActorRef<DependencyWorker.Message> replyTo;
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
	}

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "dataProviderProxy";

	public static Behavior<Message> create() {
		return Behaviors.setup(DataProviderProxy::new);
	}

	private DataProviderProxy(ActorContext<Message> context) {
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
	List<List<Set<String>>> distinctColumns;

	private boolean isDataRequested = false;
	private List<GetDataMessage> pendingRequests = new ArrayList<>();

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive<Message> createReceive() {
		return newReceiveBuilder()
				.onMessage(GetDataMessage.class, this::handle)
				.onMessage(DataMessage.class, this::handle)
				.build();
	}

	private Behavior<Message> handle(GetDataMessage message) {
		if (this.columns != null) {
			this.sendDataTo(message.getReplyTo());
			return this;
		}

		this.pendingRequests.add(message);
		if (!this.isDataRequested) {
			this.isDataRequested = true;

			if (SystemConfigurationSingleton.get().getRole().equals(SystemConfiguration.MASTER_ROLE))
				message.getDataProvider().tell(new DataProvider.GetDataLocalMessage(this.getContext().getSelf()));
			else
				message.getDataProvider().tell(new DataProvider.GetDataRemoteMessage(this.largeMessageProxy));
		}
		return this;
	}

	private void sendDataTo(ActorRef<DependencyWorker.Message> target) {
		target.tell(new DependencyWorker.DataMessage(
				this.inputFiles,
				this.headerLines,
				this.columns,
				this.distinctColumns));
	}

	private Behavior<Message> handle(DataMessage message) {
		this.inputFiles = message.getInputFiles();
		this.headerLines = message.getHeaderLines();
		this.columns = message.getColumns();

		this.distinctColumns = new ArrayList<>(this.inputFiles.length);
		for (int fileId = 0; fileId < this.columns.length; fileId++) {
			this.distinctColumns.add(new ArrayList<>(this.columns[fileId].length));
			for (int columnId = 0; columnId < this.columns[fileId].length; columnId++) {
				this.distinctColumns.get(fileId).add(new HashSet<>(Arrays.asList(this.columns[fileId][columnId])));
			}
		}

		for (GetDataMessage pendingMessage : this.pendingRequests)
			this.sendDataTo(pendingMessage.getReplyTo());

		this.pendingRequests = null;
		return this;
	}
}
