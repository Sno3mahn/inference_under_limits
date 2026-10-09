"""Run the load generator against a running gateway.

  uv run python -m loadgen --rate 1200 --duration 60 --models model-a:0.6,model-b:0.4 \
      --batch-size 1 --tokens 1000

With --callback-port, a receiver is started too and batches carry its URL.
"""

import argparse
import asyncio
import json
import logging

from loadgen.generator import LoadGenerator, LoadSpec, drain
from loadgen.receiver import CallbackReceiver


def parse_models(text: str) -> dict[str, float]:
    out = {}
    for part in text.split(","):
        name, _, weight = part.partition(":")
        out[name.strip()] = float(weight or 1)
    return out


async def run(args) -> None:
    receiver = None
    if args.callback_port:
        receiver = CallbackReceiver("127.0.0.1", args.callback_port, fail_first=args.fail_first_callbacks)
        await receiver.start()
    spec = LoadSpec(
        rate=args.rate, duration_s=args.duration, models=parse_models(args.models),
        batch_size=args.batch_size, tokens=args.tokens, token_jitter=args.token_jitter,
        callback_url=receiver.url if receiver else None, seed=args.seed,
    )
    result = await LoadGenerator(args.gateway, spec).run()
    print(json.dumps({k: v for k, v in result.to_json().items() if k != "batches"}, indent=2))
    if args.drain:
        drained = await drain(args.gateway, args.drain, need_callbacks=receiver is not None)
        print("drained" if drained else f"still busy after {args.drain}s")
    if receiver is not None:
        print(f"callbacks delivered: {len(receiver.delivered())} "
              f"(deliveries incl. failures: {len(receiver.deliveries)})")
        await receiver.stop()


def main() -> None:
    p = argparse.ArgumentParser(description="Open-loop load generator")
    p.add_argument("--gateway", default="http://127.0.0.1:8000")
    p.add_argument("--rate", type=float, required=True, help="requests per second offered")
    p.add_argument("--duration", type=float, required=True, help="seconds")
    p.add_argument("--models", default="model-a:1", help="model:weight,model:weight")
    p.add_argument("--batch-size", type=int, default=1)
    p.add_argument("--tokens", type=int, default=1_000)
    p.add_argument("--token-jitter", type=float, default=0.0)
    p.add_argument("--seed", type=int, default=1)
    p.add_argument("--callback-port", type=int, default=0, help="start a callback receiver on this port")
    p.add_argument("--fail-first-callbacks", type=int, default=0)
    p.add_argument("--drain", type=float, default=0, help="after the load, wait up to N s for the gateway to finish")
    args = p.parse_args()
    logging.basicConfig(level=logging.INFO)
    asyncio.run(run(args))


if __name__ == "__main__":
    main()
