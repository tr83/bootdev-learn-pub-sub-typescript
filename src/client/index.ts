import amqp from "amqplib";
import { clientWelcome, commandStatus, getInput, getMaliciousLog, printClientHelp, printQuit } from "../internal/gamelogic/gamelogic.js";
import { SimpleQueueType, subscribeJSON } from "../internal/pubsub/consume.js";
import { ArmyMovesPrefix, ExchangePerilDirect, ExchangePerilTopic, GameLogSlug, PauseKey, WarRecognitionsPrefix } from "../internal/routing/routing.js";
import { GameState } from "../internal/gamelogic/gamestate.js";
import { commandSpawn } from "../internal/gamelogic/spawn.js";
import { commandMove } from "../internal/gamelogic/move.js";
import { type GameLog } from "../internal/gamelogic/logs.js";
import { handlerMove, handlerPause, handlerWar } from "./handlers.js";
import { publishJSON, publishMsgPack } from "../internal/pubsub/publish.js";

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
    const gs = new GameState(userName);
    const publishCh = await conn.createConfirmChannel();

    await subscribeJSON(
      conn,
      ExchangePerilDirect,
      `${PauseKey}.${userName}`,
      PauseKey,
      SimpleQueueType.Transient,
      handlerPause(gs)
    );

    await subscribeJSON(
      conn,
      ExchangePerilTopic,
      `${ArmyMovesPrefix}.${userName}`,
      `${ArmyMovesPrefix}.*`,
      SimpleQueueType.Transient,
      handlerMove(gs, publishCh)
    );

    await subscribeJSON(
      conn,
      ExchangePerilTopic,
      WarRecognitionsPrefix,
      `${WarRecognitionsPrefix}.*`,
      SimpleQueueType.Durable,
      handlerWar(gs, publishCh)
    );

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
            await publishJSON(
              publishCh,
              ExchangePerilTopic,
              `${ArmyMovesPrefix}.${userName}`,
              move
            );
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
          try {
            if (input.length !== 2 || typeof input[1] === "undefined" || input[1]?.length === 0) {
              console.log(`Spamming needs 1 argument, got ${input.length - 1}`);
            }
            const amount: number = parseInt(input[1] as string);

            for (let i = 0; i < amount; i++) {
              const logMessage = getMaliciousLog();
              publishGameLog(
                publishCh,
                ExchangePerilTopic,
                logMessage
              );
            }
          } catch (err) {
            console.error("Error processing spam command:", err);
          }
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

export function publishGameLog(channel: amqp.ConfirmChannel, username: string, message: string) {
  const gameLog: GameLog = {
    username: username,
    message: message,
    currentTime: new Date()
  }

  return publishMsgPack(
    channel,
    ExchangePerilTopic,
    `${GameLogSlug}.${username}`,
    gameLog
  );
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
