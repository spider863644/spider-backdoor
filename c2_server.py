#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import asyncio
import json

HOST = "0.0.0.0"
PORT = 8765
END_MARKER = b"END_OF_DATA"

agents = {}       # agent_id -> writer
controllers = {}  # controller_id -> writer


async def send_json(writer, payload):
    try:
        writer.write(json.dumps(payload).encode("utf-8") + END_MARKER)
        await writer.drain()
    except Exception:
        pass


async def cleanup_writer(writer):
    dead_agents = [k for k, v in agents.items() if v == writer]
    dead_controllers = [k for k, v in controllers.items() if v == writer]

    for k in dead_agents:
        del agents[k]
    for k in dead_controllers:
        del controllers[k]

    try:
        writer.close()
        await writer.wait_closed()
    except Exception:
        pass


async def process_message(writer, payload):
    msg_type = payload.get("type")
    role = payload.get("role")

    if msg_type == "register":
        client_id = payload.get("id", "").strip()
        if not client_id:
            await send_json(writer, {"type": "error", "message": "Missing client ID"})
            return

        if role == "agent":
            agents[client_id] = writer
            await send_json(writer, {"type": "registered", "role": "agent", "id": client_id})
            print(f"[+] Agent registered: {client_id}")

        elif role == "controller":
            controllers[client_id] = writer
            await send_json(writer, {"type": "registered", "role": "controller", "id": client_id})
            print(f"[+] Controller registered: {client_id}")

        else:
            await send_json(writer, {"type": "error", "message": "Invalid role"})
            return

        # notify controllers of available agents
        await broadcast_agent_list()

    elif msg_type == "list_agents":
        await send_json(writer, {
            "type": "agent_list",
            "agents": sorted(list(agents.keys()))
        })

    elif msg_type == "forward_to_agent":
        target_agent = payload.get("target_agent")
        inner = payload.get("payload", {})
        agent_writer = agents.get(target_agent)

        if not agent_writer:
            await send_json(writer, {
                "type": "error",
                "message": f"Agent '{target_agent}' not connected"
            })
            return

        await send_json(agent_writer, inner)

    elif msg_type == "forward_to_controller":
        target_controller = payload.get("target_controller")
        inner = payload.get("payload", {})
        controller_writer = controllers.get(target_controller)

        if not controller_writer:
            return

        await send_json(controller_writer, inner)


async def broadcast_agent_list():
    payload = {
        "type": "agent_list",
        "agents": sorted(list(agents.keys()))
    }
    for _, writer in list(controllers.items()):
        await send_json(writer, payload)


async def handle_client(reader, writer):
    addr = writer.get_extra_info("peername")
    print(f"[*] Connection from {addr}")
    buffer = b""

    try:
        while True:
            chunk = await reader.read(4096)
            if not chunk:
                break

            buffer += chunk

            while END_MARKER in buffer:
                packet, buffer = buffer.split(END_MARKER, 1)
                if not packet.strip():
                    continue

                try:
                    payload = json.loads(packet.decode("utf-8", errors="ignore"))
                    await process_message(writer, payload)
                except json.JSONDecodeError:
                    await send_json(writer, {
                        "type": "error",
                        "message": "Invalid JSON received"
                    })
                except Exception as e:
                    await send_json(writer, {
                        "type": "error",
                        "message": f"Server error: {e}"
                    })

    except Exception as e:
        print(f"[-] Client error: {e}")

    finally:
        print(f"[-] Disconnect: {addr}")
        await cleanup_writer(writer)
        await broadcast_agent_list()


async def main():
    server = await asyncio.start_server(handle_client, HOST, PORT)
    print(f"[+] Relay server listening on {HOST}:{PORT}")

    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
