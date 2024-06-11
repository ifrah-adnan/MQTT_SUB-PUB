const mqtt = require("mqtt");
const { MongoClient } = require("mongodb");

const brokerUrl = "mqtt://localhost:1883";
const topicSubscribe = "data/#";
const topicPublishBase = "rpc/";

const mongoUrl =
  "mongodb+srv://aifrah:20242024@mqtt.ahb3azr.mongodb.net/?retryWrites=true&w=majority&appName=mqtt";
const dbName = "mqtt";
const collectionName = "messages";

const client = mqtt.connect(brokerUrl);
const mongoClient = new MongoClient(mongoUrl, {});

client.on("connect", () => {
  console.log("Connected to MQTT broker");
  client.subscribe(topicSubscribe, (err) => {
    if (err) {
      console.error("Subscription error:", err);
    } else {
      console.log(`Subscribed to topic: ${topicSubscribe}`);
    }
  });
});

client.on("message", async (topic, message) => {
  const id = topic.split("/")[1];
  const payload = message.toString();

  console.log(`Received message: ${payload} from topic: ${topic}`);

  try {
    await mongoClient.connect();
    const db = mongoClient.db(dbName);
    const collection = db.collection(collectionName);

    const doc = { id: id, message: payload, timestamp: new Date() };
    await collection.insertOne(doc);
    console.log(`Message stored in MongoDB: ${JSON.stringify(doc)}`);

    const publishTopic = `${topicPublishBase}${id}`;
    client.publish(publishTopic, payload, (err) => {
      if (err) {
        console.error("Publish error:", err);
      } else {
        console.log(`Published message to topic: ${publishTopic}`);
      }
    });
  } catch (err) {
    console.error("MongoDB error:", err);
  } finally {
    await mongoClient.close();
  }
});

client.on("error", (err) => {
  console.error("MQTT error:", err);
});
