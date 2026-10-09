"""Charts for the Go benchmark JSON files (go/cmd/bench output).

    python -m verify.bench_charts ../runs/go ../docs/results/go

One panel per benchmark: completions per second for every run, with the sum of the 97% caps as a
reference line. Warm-up (excluded from the headline rate) is shaded.
"""

import json
import sys
from pathlib import Path

from matplotlib.ticker import FuncFormatter

from verify.report import GRID, LIMIT, SERIES, _human, _legend, plt

LIMITED = [("pinned-300000", "300k/s required: 64 models, each on one shard"),
           ("spread-300000", "300k/s required: 1 model split across 12 shards"),
           ("pinned-1000000", "1M/s stretch: 64 models, each on one shard")]


def throughput_chart(src: Path, out: Path) -> Path:
    present = [(n, t) for n, t in LIMITED if (src / f"{n}.json").exists()]
    fig, axes = plt.subplots(len(present), 1, figsize=(9, 2.6 * len(present)), squeeze=False)
    for ax, (name, title) in zip(axes[:, 0], present, strict=True):
        data = json.loads((src / f"{name}.json").read_text())
        median = data["median"]
        cfg = median["config"]
        warmup_s, duration_s = cfg["warmup_ns"] / 1e9, cfg["duration_ns"] / 1e9
        ax.axvspan(0, warmup_s, color=GRID, alpha=0.45, lw=0)
        ax.axhline(median["limits"]["total_cap_per_s"], color=LIMIT, lw=1.2, label="sum of 97% caps")
        for i, run in enumerate(data["runs"]):
            per_s = run["completed_per_second"][: int(duration_s)]
            ax.plot(range(len(per_s)), per_s, color=SERIES[i % len(SERIES)], lw=1.2,
                    label=f"run {i + 1}: {run['completed_per_s'] / 1e3:,.1f}k/s after warm-up")
        ax.set_title(title, loc="left", fontsize=10, pad=20)
        ax.set_ylabel("completed / s")
        ax.set_ylim(bottom=0)
        ax.yaxis.set_major_formatter(FuncFormatter(_human))
        _legend(ax)
    axes[-1, 0].set_xlabel("seconds since load started  (shaded: warm-up, excluded from the headline)")
    fig.tight_layout()
    out.mkdir(parents=True, exist_ok=True)
    path = out / "throughput.png"
    fig.savefig(path, dpi=130)
    plt.close(fig)
    return path


if __name__ == "__main__":
    src = Path(sys.argv[1] if len(sys.argv) > 1 else "../runs/go")
    dst = Path(sys.argv[2] if len(sys.argv) > 2 else "../docs/results/go")
    print(throughput_chart(src, dst))
