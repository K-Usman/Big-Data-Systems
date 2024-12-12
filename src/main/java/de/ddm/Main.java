package de.ddm;

import akka.actor.typed.ActorSystem;
import de.ddm.actors.Guardian;
import de.ddm.configuration.Command;
import de.ddm.configuration.SystemConfiguration;
import de.ddm.singletons.SystemConfigurationSingleton;

import java.io.IOException;

public class Main {

	public static void main(String[] args) {
		Command.applyOn(args); //input parsing. the program takes some args so that we can config the inputs and parameter from outside.

		SystemConfiguration config = SystemConfigurationSingleton.get(); //this is the configuration that we will use to start our actor system
		final ActorSystem<Guardian.Message> guardian = ActorSystem.create(Guardian.create(), config.getActorSystemName(), config.toAkkaConfig());  //this is where actor system is created, we have to provide user guardian.


		if (config.getRole().equals(SystemConfiguration.MASTER_ROLE)) {
			if (config.isStartPaused())
				waitForInput(">>> Press ENTER to start <<<");

			guardian.tell(new Guardian.StartMessage());

//			waitForInput(">>> Press ENTER to exit <<<");
//
//			guardian.tell(new Guardian.ShutdownMessage());
		}
	} //if we end our program here, it would still be alive and wait for input.


	private static void waitForInput(String message) {
		try {
			System.out.println(message);
			System.in.read();
		} catch (IOException ignored) {
		}
	}
}
