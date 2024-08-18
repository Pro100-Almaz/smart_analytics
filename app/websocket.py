import asyncio
from time import sleep

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, APIRouter
from fastapi.responses import HTMLResponse

from app.database import redis_database

router = APIRouter()

html = """
<!DOCTYPE html>
<html>
    <head>
        <title>Chat</title>
    </head>
    <body>
        <h1>WebSocket Chat</h1>
        <h2>Your ID: <span id="ws-id"></span></h2>
        <form action="" onsubmit="sendMessage(event)">
            <input type="text" id="messageText" autocomplete="off"/>
            <button>Send</button>
        </form>
        <ul id='messages'>
        </ul>
        <script>
            var client_id = Date.now()
            document.querySelector("#ws-id").textContent = client_id;
            var ws = new WebSocket(`wss://286c-87-255-216-104.ngrok-free.app/ws/top_5_fundings/${client_id}`);
            ws.onmessage = function(event) {
                console.log(event.data);
            };
            function sendMessage(event) {
                var input = document.getElementById("messageText")
                ws.send(input.value)
                input.value = ''
                event.preventDefault()
            }
        </script>
    </body>
</html>
"""


class ConnectionManager:
    def __init__(self):
        self.active_connections: list[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)

    async def broadcast(self, message: str):
        for connection in self.active_connections:
            await connection.send_text(message)


manager = ConnectionManager()


def get_merged_data():
    top_tickers = redis_database.get_top_5_tickers() or {}
    top_tickers_by_volume = redis_database.get_top_5_tickers_by_volume() or {}

    # Merge the two dictionaries
    merged_data = {
        "top_tickers": top_tickers,
        "top_tickers_by_volume": top_tickers_by_volume
    }

    return merged_data

@router.get("/test_websocket")
async def get():
    return HTMLResponse(html)


@router.websocket("/top_5_fundings/{client_id}")
async def websocket_endpoint(websocket: WebSocket, client_id: int):
    await manager.connect(websocket)
    print(manager.active_connections)
    try:
        while True:
            try:
                merged_data = get_merged_data()
                if websocket.client_state != websocket.client_state.DISCONNECTED:
                    await websocket.send_json(merged_data)

                await asyncio.sleep(60)

            except asyncio.TimeoutError:
                print("No data received within timeout duration. Closing connection.")
                await websocket.close()
                break

    except WebSocketDisconnect:
        print("Client disconnected")

    except Exception as e:
        print(f"Unexpected error: {e}")

        if websocket.client_state != websocket.client_state.DISCONNECTED:
            await websocket.close()

        manager.disconnect(websocket)

