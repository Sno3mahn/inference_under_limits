"""Run the gateway:  python -m gateway --provider-url http://127.0.0.1:9000

Always one process. Limiter state lives in memory, so N worker processes would each think
they own the full limit and together send N times too much. Scale-out is a design topic
(docs/ARCHITECTURE.md §13), not a uvicorn flag.
"""

import argparse
import logging

import uvicorn

from gateway.api import create_app
from gateway.service import Service, ServiceConfig, load_model_limits


def main() -> None:
    parser = argparse.ArgumentParser(description="Inference gateway")
    parser.add_argument("--config", default="../config/models.yaml", help="model limits (rpm/tpm per model)")
    parser.add_argument("--provider-url", default="http://127.0.0.1:9000")
    parser.add_argument("--db", default="../runs/gateway.db")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--utilization", type=float, default=0.97)
    parser.add_argument("--deadline-s", type=float, default=120.0, help="default queue deadline")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    config = ServiceConfig(models=load_model_limits(args.config), provider_url=args.provider_url, db_path=args.db)
    config.gateway.scheduler.utilization = args.utilization
    config.gateway.default_deadline_s = args.deadline_s

    app = create_app(Service(config))
    # Passing the app object (not an import string) means uvicorn can't fork workers.
    uvicorn.run(app, host=args.host, port=args.port, access_log=False, log_level="warning")


if __name__ == "__main__":
    main()
