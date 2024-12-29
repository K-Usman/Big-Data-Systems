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
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.*;

public class DataProvider extends AbstractBehavior<DataProvider.Message> {


    ////////////////////
    // Actor Messages //
    ////////////////////

    public interface Message extends AkkaSerializable {
    }

    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ReceptionistListingMessage implements Message {
        private static final long serialVersionUID = -8536413915436820711L;
        Receptionist.Listing listing;
    }

    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class AssignHeadersMessage  implements Message {
        private static final long serialVersionUID = -2836164504241926323L;
//        ActorRef<LargeMessageProxy.Message> dependencyMinerLargeMessageProxy;
          ActorRef<DependencyMiner.Message> dependencyMinerRef;
          String[][] headers;
    }


    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class SetDependencyWorkerReference   implements Message {
        private static final long serialVersionUID = -4644621994959205733L;
         ActorRef<DependencyWorker.Message> dependencyWorkerRef;
    }

    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class AssignBatchMessage implements Message {
        private static final long serialVersionUID = -2913390488657018394L;
        ActorRef<DependencyMiner.Message> dependencyMinerRef;
        List<List<String[]>> batches;
    }

//    @Getter
//    @NoArgsConstructor
//    @AllArgsConstructor
//    public static class StealHeadersMessage implements Message {
//        private static final long serialVersionUID = -7656115153216584798L;
//        ActorRef<DependencyMiner.Message> dependencyMiner;
//    }


    ////////////////////////
    // Actor Construction //
    ////////////////////////

    public static final String DEFAULT_NAME = "dataProvider";

    public static Behavior<Message> create(String role) {
        return Behaviors.setup(context -> new DataProvider(context,role));
    }

    private DataProvider(ActorContext<Message> context,String role) {
        super(context);
//        this.dependencyWorkerRef = dependencyWorkerRef;
        this.role=role;
        this.dependencyWorkers = new ArrayList<>();


//        this.headers = new String[20][20];
        // Create a message adapter for the receptionist listing
        final ActorRef<Receptionist.Listing> listingResponseAdapter =
                context.messageAdapter(Receptionist.Listing.class, ReceptionistListingMessage::new);

        // Subscribe to DependencyMiner service
        context.getSystem().receptionist().tell(
                Receptionist.subscribe(DependencyMiner.dependencyMinerService, listingResponseAdapter)
        );
        this.largeMessageProxy = this.getContext().spawn(LargeMessageProxy.create(this.getContext().getSelf().unsafeUpcast()), LargeMessageProxy.DEFAULT_NAME);


        // At this point, dependencyMinerRef is not yet set. Workers are created after the reference is initialized.
    }

    /////////////////
    // Actor State //
    /////////////////

//    private final Map<Integer, String[]> assignedHeaders = new HashMap<>();
    private String[][] headers;
    private  List<List<String[]>> batchLines;
    private final Map<Integer, List<String[]>> assignedBatches = new HashMap<>();
    private final ActorRef<LargeMessageProxy.Message> largeMessageProxy;
    private ActorRef<DependencyMiner.Message> dependencyMinerRef; // Store DependencyMiner reference
    private final String role;
    private final List<ActorRef<DependencyWorker.Message>> dependencyWorkers;

    ////////////////////
    // Actor Behavior //
    ////////////////////

    @Override
    public Receive<Message> createReceive() {
        return newReceiveBuilder()
                .onMessage(ReceptionistListingMessage.class, this::handle)
                .onMessage(AssignHeadersMessage.class, this::handle)
                .onMessage(AssignBatchMessage.class, this::handle)
                .onMessage(SetDependencyWorkerReference.class, this::handle)
//                .onMessage(StealHeadersMessage.class, this::handle)
                .build();
    }

    //get a list of actor refs of actors registered to DependencyMiner service.
    private Behavior<Message> handle(ReceptionistListingMessage message) {
        Set<ActorRef<DependencyMiner.Message>> dependencyMiners = message.getListing().getServiceInstances(DependencyMiner.dependencyMinerService);
        for (ActorRef<DependencyMiner.Message> dependencyMiner : dependencyMiners)
            // this dependencyMiner(like Master) will then notify other registered actors about this new actors
            //availability
            dependencyMiner.tell(new DependencyMiner.RegistrationMessage(this.getContext().getSelf(),this.role));
            getContext().getLog().info("Data Prov ref sent to DepMiner");
        return this;
    }


    private Behavior<Message> handle(AssignHeadersMessage message) {
        getContext().getLog().info("Getting headers");
        this.headers = message.getHeaders();
        this.dependencyMinerRef = message.getDependencyMinerRef();
        getContext().getLog().info(String.valueOf(headers.length));
        sendTaskToWorker(); // Send both headers and batches if available
        return this;
    }


    private Behavior<Message> handle(AssignBatchMessage message) {
        getContext().getLog().info("Getting batches");
        this.batchLines = message.getBatches();
        this.dependencyMinerRef = message.getDependencyMinerRef();
        getContext().getLog().info(String.valueOf(batchLines.size()));
        sendTaskToWorker(); // Send both headers and batches if available
        return this;
    }

    private void sendTaskToWorker() {
        if (this.headers != null && this.batchLines != null && this.dependencyMinerRef != null) {
            getContext().getLog().info("Sending headers and batches to worker");
            ActorRef<DependencyWorker.Message> firstWorker = dependencyWorkers.get(0);
            firstWorker.tell(new DependencyWorker.TaskMessage(this.dependencyMinerRef, this.headers, this.batchLines));
        } else {
            getContext().getLog().info("Headers or batches are not yet fully received");
        }
    }

    private Behavior<Message> handle(SetDependencyWorkerReference message) {
        ActorRef<DependencyWorker.Message> dependencyWorker = message.getDependencyWorkerRef();
        if (!this.dependencyWorkers.contains(dependencyWorker)) {
            this.dependencyWorkers.add(dependencyWorker);
            this.getContext().watch(dependencyWorker);
            // The worker should get some work ... let me send her something before I figure out what I actually want from her.
            // I probably need to idle the worker for a while, if I do not have work for it right now ... (see master/worker pattern)
            getContext().getLog().info("Number of dep workers");
            getContext().getLog().info(String.valueOf(dependencyWorkers.size()));
        }
        return this;
    }}

//    private Behavior<Message> handle(StealHeadersMessage message) {
//        message.getDependencyMiner().tell(new DependencyMiner.RegistrationMessage(this.getContext().getSelf()));
//        return this;
//    }

