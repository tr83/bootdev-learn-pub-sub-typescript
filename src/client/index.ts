import amqp from "amqplib";
import { clientWelcome, commandStatus, getInput, printClientHelp, printQuit } from "../internal/gamelogic/gamelogic.js";
import { declareAndBind, SimpleQueueType } from "../internal/pubsub/publish.js";
import { ExchangePerilDirect, PauseKey } from "../internal/routing/routing.js";
import { GameState } from "../internal/gamelogic/gamestate.js";
import { commandSpawn } from "../internal/gamelogic/spawn.js";
import { commandMove } from "../internal/gamelogic/move.js";

async function main() {
  console.log("Starting Peril client...");
  const rabbitConnString = "amqp://guest:guest@localhost:5672/";
  const conn = await amqp.connect(rabbitConnString);

  ["SIGINT", "SIGTERM"].forEach((signal) =>
    process.on(signal, async () => {
      try {
        await conn.close();
        console.log("RabbitMQ connection closed.");
      } catch (err) {
        console.error("Error closing RabbitMQ connection:", err);
      } finally {
        process.exit(0);
      }
    }),
  );

  try {
    const userName = await clientWelcome();

    await declareAndBind(
      conn,
      ExchangePerilDirect,
      `pause.${userName}`,
      PauseKey,
      SimpleQueueType.Transient,
    )

    const gs = new GameState(userName);

    while (true) {
      const input = await getInput();

      if (input.length === 0 || !input[0]) {
        continue;
      }

      const command = input[0].toLowerCase();

      switch (command) {
        case "spawn":
          try {
            console.log("Spawning a new unit...");
            commandSpawn(gs, input);
          } catch (err) {
            console.error("Error processing spawn command:", err);
          }
          break;
        case "move":
          try {
            console.log("Moving a unit...");
            const move = commandMove(gs, input);
            if (move) {
              console.log('Move succeeded!');
            }
          } catch (err) {
            console.error("Error processing move command:", err);
          }
          break;
        case "status":
          console.log("Getting game status...");
          commandStatus(gs);
          break;
        case "help":
          printClientHelp();
          break;
        case "spam":
          console.log("Spamming not allowed yet!");
          break;
        case "quit":
          printQuit();
          process.exit(0);
        default:
          console.log(`Unknown command: ${command}`);
          printClientHelp();
      }
    }
  } catch (err) {
    console.error("Error:", err);
  }
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
