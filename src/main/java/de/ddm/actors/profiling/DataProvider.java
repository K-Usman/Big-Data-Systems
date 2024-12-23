package de.ddm.actors.profiling;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.DispatcherSelector;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.receptionist.Receptionist;
import de.ddm.actors.patterns.LargeMessageProxy;
import de.ddm.serialization.AkkaSerializable;
import de.ddm.singletons.SystemConfigurationSingleton;
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
          String[][] header;
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
        int id;
        List<String[]> batch;
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

    public static Behavior<Message> create(ActorRef<DependencyWorker.Message> dependencyWorkerRef) {
        return Behaviors.setup(context -> new DataProvider(context, dependencyWorkerRef));
    }

    private DataProvider(ActorContext<Message> context, ActorRef<DependencyWorker.Message> dependencyWorkerRef) {
        super(context);
        this.dependencyWorkerRef = dependencyWorkerRef;


        this.headers = new String[20][20];
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

    private final ActorRef<DependencyWorker.Message> dependencyWorkerRef;
//    private final Map<Integer, String[]> assignedHeaders = new HashMap<>();
    private String[][] headers;
    private final Map<Integer, List<String[]>> assignedBatches = new HashMap<>();
    private final ActorRef<LargeMessageProxy.Message> largeMessageProxy;
    private ActorRef<DependencyMiner.Message> dependencyMinerRef; // Store DependencyMiner reference

    ////////////////////
    // Actor Behavior //
    ////////////////////

    @Override
    public Receive<Message> createReceive() {
        return newReceiveBuilder()
                .onMessage(ReceptionistListingMessage.class, this::handle)
                .onMessage(AssignHeadersMessage.class, this::handle)
                .onMessage(AssignBatchMessage.class, this::handle)
//                .onMessage(StealHeadersMessage.class, this::handle)
                .build();
    }

    //get a list of actor refs of actors registered to DependencyMiner service.
    private Behavior<Message> handle(ReceptionistListingMessage message) {
        Set<ActorRef<DependencyMiner.Message>> dependencyMiners = message.getListing().getServiceInstances(DependencyMiner.dependencyMinerService);
        for (ActorRef<DependencyMiner.Message> dependencyMiner : dependencyMiners)
            // this dependencyMiner(like Master) will then notify other registered actors about this new actors
            //availability
            dependencyMiner.tell(new DependencyMiner.RegistrationMessage(this.getContext().getSelf()));
        return this;
    }


    private Behavior<Message> handle(AssignHeadersMessage message) {
        getContext().getLog().info("Getting headers");
        getContext().getLog().info("Sending headers to worker");
        this.headers=message.getHeader();
        this.dependencyMinerRef=message.dependencyMinerRef;
        if(dependencyMinerRef==null){
            getContext().getLog().info("This is null");
        }
        dependencyWorkerRef.tell(new DependencyWorker.TaskMessage(dependencyMinerRef,this.headers));
        return this;
    }


    private Behavior<Message> handle(AssignBatchMessage message) {

        return this;
    }

//    private Behavior<Message> handle(StealHeadersMessage message) {
//        message.getDependencyMiner().tell(new DependencyMiner.RegistrationMessage(this.getContext().getSelf()));
//        return this;
//    }

}
