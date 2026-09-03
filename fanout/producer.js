import ampq from "amqplib";

async function sendNotification(message) {
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
    const exchangeName = "notification2";

    await ch.assertExchange(exchangeName, "fanout", { durable: true });

    ch.publish(exchangeName, "", Buffer.from(JSON.stringify(message)), {
      persistent: true,
    });

    setTimeout(() => {
      ch.close();
      conn.close();
    }, 500);
  } catch (err) {
    console.log("Error in sending notification:", err);
  }
}

sendNotification({
  Title: " New Product Launch",
  ProductName: "Smartphone XYZ",
  Description:
    "Introducing the latest Smartphone XYZ with cutting-edge features.",
  LaunchDate: "2023-09-17",
});
