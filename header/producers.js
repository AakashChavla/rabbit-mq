import ampq from "amqplib";

async function sendQueue(header, message) {
  try {
    const conn = await ampq.connect({
      protocol: "amqp",
      hostname: "localhost",
      port: 5672,
      username: "admin",
      password: "securepassword123",
      vhost: "/",
    });

    const ch = await conn.createChannel();

    const exchangeName = "header-check";
    await ch.assertExchange(exchangeName, "headers", { durable: true });

    console.log("Sending notification:", message);

    ch.publish(exchangeName, "", Buffer.from(JSON.stringify(message)), {
      persistent: true,
      headers: header,
    });

    setTimeout(() => {
      ch.close();
      conn.close();
    }, 500);
  } catch (err) {
    console.log("Error in sending notification:", err);
  }
}

sendQueue(
  { "x-match": "all", type: "order", priority: "high" },
  {
    orderId: 123,
    userId: "order",
  },
);

sendQueue(
  { "x-match": "any", type: "payment", priority: "high" },
  {
    orderId: 123,
    userId: "payment",
  },
);
