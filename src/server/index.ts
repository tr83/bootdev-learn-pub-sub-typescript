import amqp from "amqplib";
import { publishJSON } from "../internal/pubsub/publish.js";
import { ExchangePerilDirect, PauseKey } from "../internal/routing/routing.js";
import { getInput, printServerHelp } from "../internal/gamelogic/gamelogic.js";

async function main() {
  const rabbitConnString = "amqp://guest:guest@localhost:5672/";
  const conn = await amqp.connect(rabbitConnString);
  console.log("Peril game server connected to RabbitMQ!");

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

  const publishCh = await conn.createConfirmChannel();
  printServerHelp();

  try {
    while(true) {
      const input = await getInput();
      
      if (input.length === 0 || !input[0]) {
        continue;
      }

      const command = input[0].toLowerCase();

      switch (command) {
        case "pause":
          console.log("Pausing the game...");
          await publishJSON(publishCh, ExchangePerilDirect, PauseKey, {
            isPaused: true,
          });
          break;
        case "resume":
          console.log("Resuming the game...");
          await publishJSON(publishCh, ExchangePerilDirect, PauseKey, {
            isPaused: false,
          });
          break;
        case "help":
          printServerHelp();
          break;
        case "quit":
          console.log("Shutting down the server...");
          await conn.close();
          process.exit(0);
        default:
          console.log(`Unknown command: ${command}`);
          printServerHelp();
      }
    }
  } catch (err) {
    console.error("Error publishing message:", err);
  }
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
