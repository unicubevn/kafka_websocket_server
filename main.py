from fastapi import FastAPI, WebSocket, WebSocketDisconnect,Request
import asyncio

from kafka_consumer import KafkaConsumerManager

app = FastAPI()

class WebSocketServer:
    def __init__(self, kafka_consumer_manager):
        self.clients = {}  # topic -> set of websockets
        self.kafka_consumer_manager = kafka_consumer_manager

    async def connect(self, websocket: WebSocket, topic: str):
        await websocket.accept()
        if topic not in self.clients:
            self.clients[topic] = set()
        self.clients[topic].add(websocket)
        self.kafka_consumer_manager.create_consumer(topic, self)

    async def disconnect(self, websocket: WebSocket, topic: str):
        if topic in self.clients:
            self.clients[topic].remove(websocket)
            if not self.clients[topic]:  # No clients left for the topic
                del self.clients[topic]

    async def broadcast_message(self, topic: str, message: str):
        print(f"Broadcasting message to {topic}: {message}")
        if topic in self.clients:
            await asyncio.wait([client.send_text(message) for client in self.clients[topic]])

websocket_handler = WebSocketServer(KafkaConsumerManager(bootstrap_servers="localhost:9092"))

@app.websocket("/ws/{topic}")
async def websocket_endpoint(websocket: WebSocket, topic: str):
    await websocket_handler.connect(websocket, topic)
    try:
        while True:
            await websocket.receive_text()  # Expecting a keep-alive message or handle incoming if needed
    except WebSocketDisconnect:
        await websocket_handler.disconnect(websocket, topic)
@app.post("/send")
async def send_message(request: Request):
    data = await request.json()
    topic = data.get("topic")
    message = data.get("message")
    if topic and message:
        await websocket_handler.broadcast_message(topic, message)
        return {"status": "Message broadcast to topic"}
    return {"error": "Invalid request"}

