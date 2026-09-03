import ampq from "amqplib";

async function processOrder() {
  try {
    const connection = await ampq.connect({
      protocol: "amqp",
      hostname: "localhost",
      port: 5672,
      username: "admin",
      password: "securepassword123",
      vhost: "/",
    });

    const ch = await connection.createChannel();
    const exchangeName = "notification";
    const queueName = "order";
    await ch.assertExchange(exchangeName, "topic", { durable: true });

    await ch.assertQueue(queueName, { durable: true });
    await ch.bindQueue(queueName, exchangeName, "order.*");

    ch.consume(
      queueName,
      (msg) => {
        if (msg !== null) {
          const messageContent = JSON.parse(msg.content.toString());
          console.log("Received order notification:", messageContent);
          ch.ack(msg);
        }
      },
      { noAck: false },
    );
  } catch (err) {
    console.log("Error in processing order:", err);
  }
}

processOrder();