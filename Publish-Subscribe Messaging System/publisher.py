import argparse
import asyncio
import json
import uuid
from typing import Any, Dict, Optional

from protocol import colors_enabled, encode_frame, paint, time_tag, utc_now_iso


class PublisherClient:
    def __init__(
        self, client_id: str, host: str, port: int, use_color: Optional[bool] = None
    ) -> None:
        self.client_id = client_id
        self.host = host
        self.port = port
        self.color = colors_enabled() if use_color is None else use_color
        self.reader: Optional[asyncio.StreamReader] = None
        self.writer: Optional[asyncio.StreamWriter] = None

    def _log(self, msg: str, style: str = "yellow") -> None:
        ts = paint(f"[{time_tag()}]", "dim", enabled=self.color)
        tag = paint(f"[SENDER:{self.client_id}]", "bold", style, enabled=self.color)
        print(f"{ts} {tag} {msg}")

    async def _wait_for_frame(self, expected_type: str) -> Dict[str, Any]:
        assert self.reader is not None
        while True:
            line = await self.reader.readline()
            if not line:
                raise ConnectionError(f"Connection closed while waiting for {expected_type}")
            raw = line.decode("utf-8").strip()
            if not raw:
                continue
            frame = json.loads(raw)
            if frame.get("type") == expected_type:
                return frame

    async def connect(self) -> None:
        self.reader, self.writer = await asyncio.open_connection(self.host, self.port)
        self.writer.write(
            encode_frame({"type": "HELLO", "client_id": self.client_id, "role": "pub"})
        )
        await self.writer.drain()
        await self._wait_for_frame("HELLO_ACK")
        self._log("Connected to the store hub")

    async def publish(
        self, topic: str, payload: Dict[str, Any], msg_id: Optional[str] = None
    ) -> Dict[str, Any]:
        assert self.writer is not None and self.reader is not None
        mid = msg_id or str(uuid.uuid4())[:8]
        frame = {
            "type": "PUB",
            "msg_id": mid,
            "topic": topic,
            "payload": payload,
            "timestamp": utc_now_iso(),
        }
        self.writer.write(encode_frame(frame))
        await self.writer.drain()

        ack = await self._wait_for_frame("PUBACK")
        matched = ack.get("matched_queues", 0)
        payload_str = json.dumps(payload, separators=(",", ":"))
        self._log(
            f"Sent {paint(topic, 'bold', 'yellow', enabled=self.color)} "
            f"{paint(payload_str, 'dim', enabled=self.color)} "
            f"(id: {mid}) -> {paint(f'saved for {matched} listener(s)', 'green', enabled=self.color)}"
        )
        return ack

    async def close(self) -> None:
        if self.writer:
            self.writer.close()
            await self.writer.wait_closed()


def _build_payload_from_args(args: argparse.Namespace) -> Dict[str, Any]:
    if args.payload:
        raw = args.payload.strip()
        if raw.startswith("'") and raw.endswith("'"):
            raw = raw[1:-1]
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return {"raw": raw}

    data: Dict[str, Any] = {}
    if args.item is not None:
        data["item"] = args.item
    if args.packs is not None:
        data["packs_left"] = args.packs
    if args.cases is not None:
        data["cases_added"] = args.cases
    if args.amount is not None:
        data["amount_php"] = args.amount
    return data if data else {"item": "SkyFlakes", "packs_left": 2}


async def _cli_main(args: argparse.Namespace) -> None:
    use_color = False if args.no_color else None
    pub = PublisherClient(args.client_id, args.host, args.port, use_color=use_color)
    await pub.connect()
    try:
        payload = _build_payload_from_args(args)
        for i in range(args.count):
            data = dict(payload)
            if args.count > 1:
                data["seq"] = i + 1
            await pub.publish(args.topic, data)
            if i + 1 < args.count:
                await asyncio.sleep(args.interval)
    finally:
        await pub.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Sari-Sari Store Publisher Client")
    parser.add_argument("--client-id", required=True)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=5555)
    parser.add_argument("--topic", required=True)
    parser.add_argument("--payload", default=None, help="Optional raw JSON payload")
    parser.add_argument("--item", default=None, help="Shortcut: item name")
    parser.add_argument("--packs", type=int, default=None, help="Shortcut: packs_left")
    parser.add_argument("--cases", type=int, default=None, help="Shortcut: cases_added")
    parser.add_argument("--amount", type=float, default=None, help="Shortcut: amount_php")
    parser.add_argument("--count", type=int, default=1)
    parser.add_argument("--interval", type=float, default=1.0)
    parser.add_argument("--no-color", action="store_true", help="Disable ANSI terminal colors")
    args = parser.parse_args()
    asyncio.run(_cli_main(args))


if __name__ == "__main__":
    main()