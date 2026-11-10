import asyncio
import websockets
import struct
import os

# Render automatically provides a dynamic port through the PORT environment variable
PORT = int(os.environ.get("PORT", 10000))

async def handle_client(websocket):
    print("[*] Client connected via WebSocket tunnel")
    stream_routing_table = {}

    try:
        async for message in websocket:
            if len(message) < 6:
                continue
            
            stream_id, payload_len = struct.unpack("!IH", message[:6])
            payload = message[6:6+payload_len]
            
            # Connection Initialization (New stream window setup)
            if payload_len > 0 and stream_id not in stream_routing_table:
                try:
                    atyp = struct.unpack("!H", message[4:6])
                    dst_port = struct.unpack("!H", payload[:2])
                    raw_host = payload[2:]
                    
                    target_host = raw_host.decode(errors='ignore') if atyp == 3 else ".".join(map(str, raw_host))
                    print(f"[Proxy] Stream #{stream_id} opening channel -> {target_host}:{dst_port}")
                    
                    tcp_reader, tcp_writer = await asyncio.open_connection(target_host, dst_port)
                    stream_routing_table[stream_id] = tcp_writer
                    
                    # Fork background pipe to funnel internet responses back down the WebSocket
                    asyncio.create_task(pipe_target_to_ws(tcp_reader, stream_id, websocket))
                except Exception as e:
                    print(f"[-] Target connection failed: {e}")
                continue

            # Route multiplexed streaming payload out to active web destination
            if stream_id in stream_routing_table:
                tcp_writer = stream_routing_table[stream_id]
                try:
                    tcp_writer.write(payload)
                    await tcp_writer.drain()
                except:
                    del stream_routing_table[stream_id]

    except websockets.exceptions.ConnectionClosed:
        print("[-] Client disconnected from tunnel")
    finally:
        for writer in stream_routing_table.values():
            writer.close()

async def pipe_target_to_ws(tcp_reader, stream_id, websocket):
    try:
        while True:
            data = await tcp_reader.read(4096)
            if not data:
                break
            response_header = struct.pack("!IH", stream_id, len(data))
            await websocket.send(response_header + data)
    except:
        pass

async def main():
    print(f"[*] Launching Multiplex Web Service on port {PORT}...")
    async with websockets.serve(handle_client, "0.0.0.0", PORT):
        await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())
