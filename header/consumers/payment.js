import ampq from "amqplib";

async function processPayment(){
    try{

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
        const queue  =  await ch.assertQueue("", { exclusive: true });
        await ch.bindQueue(queue.queue, exchangeName, "", {
            "x-match": "all",
            type: "payment",
            priority: "high",
        });

        ch.consume(
          queue.queue,
          (msg) => {
            if (msg !== null) {
              const messageContent = JSON.parse(msg.content.toString());
              console.log("Received order notification:", messageContent);
              ch.ack(msg);
            }
          },
          { noAck: false },
        );

    }catch(err){
        console.log("Error in processing order:", err);
    }
}

processPayment();