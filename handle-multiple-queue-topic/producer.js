import ampq from "amqplib";

async function sendQueue(routingKey, message) {
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
    await ch.assertExchange(exchangeName, "topic", { durable: true });
    console.log("Sending notification:", message);

    ch.publish(exchangeName, routingKey, Buffer.from(JSON.stringify(message)), {
      persistent: true,
    });

    setTimeout(() => {
      ch.close();
      connection.close();
    }, 500);
  } catch (err) {
    console.log("Error in sending notification:", err);
  }
}

sendQueue("order.confirm", {
  orderId: 123,
  userId: 122,
  userName: "John Doe",
});
sendQueue("payment.confirm", {
  paymentId: 897,
  orderId: 123,
  userId: 122,
});
