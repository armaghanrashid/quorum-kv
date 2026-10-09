"""Render the demo's event log (cmd/demo --csv) as an animated timeline.

    go run ./cmd/demo --csv events.csv
    python tools/render.py events.csv --out docs/media

Writes hero.gif (animated, <= 900 px wide) and timeline.png (static, 1600 px
wide). Frames are drawn with matplotlib and assembled with ffmpeg.
"""

from __future__ import annotations

import argparse
import csv
import shutil
import subprocess
import tempfile
from dataclasses import dataclass, field
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.patches import Rectangle  # noqa: E402
from PIL import Image  # noqa: E402

BG = "#0d1117"
PANEL = "#161b22"
GRID = "#30363d"
TEXT = "#e6edf3"
MUTED = "#8b949e"
STATE_COLOR = {
    "follower": "#21262d",
    "precandidate": "#4a3a12",
    "candidate": "#d29922",
    "leader": "#3fb950",
    "crashed": "#4a1d1d",
}
NODE_COLOR = ["#58a6ff", "#d2a85c", "#a78bfa", "#56d364", "#ff7b72"]
PART_COLORS = ["#bc8cff", "#39c5cf", "#f0883e"]
CRASH = "#f85149"
TICK = "#79c0ff"


@dataclass
class Segment:
    start: float
    end: float
    state: str
    term: int = 0


@dataclass
class Timeline:
    n: int
    end: float
    segments: list[list[Segment]]
    commits: list[list[tuple[float, int]]]
    crashes: list[tuple[float, int]]
    restarts: list[tuple[float, int]]
    partitions: list[tuple[float, float, list[list[int]]]]
    captions: list[tuple[float, str]] = field(default_factory=list)


def load(path: Path) -> Timeline:
    rows = list(csv.DictReader(path.open()))
    ev = [
        (int(r["t_ms"]) / 1000.0, int(r["node"]), r["kind"], int(r["term"]), int(r["index"]), r["detail"])
        for r in rows
    ]
    ev.sort(key=lambda e: e[0])
    n = max(e[1] for e in ev) + 1
    end = ev[-1][0] + 0.35

    state = ["follower"] * n
    term = [0] * n
    since = [0.0] * n
    segments: list[list[Segment]] = [[] for _ in range(n)]
    commits: list[list[tuple[float, int]]] = [[] for _ in range(n)]
    crashes: list[tuple[float, int]] = []
    restarts: list[tuple[float, int]] = []
    partitions: list[tuple[float, float, list[list[int]]]] = []
    captions: list[tuple[float, str]] = []

    def switch(node: int, t: float, new: str, tm: int | None = None) -> None:
        if t > since[node]:
            segments[node].append(Segment(since[node], t, state[node], term[node]))
        state[node], since[node] = new, t
        if tm is not None:
            term[node] = tm

    open_part: tuple[float, list[list[int]]] | None = None
    for t, node, kind, tm, idx, detail in ev:
        if kind == "role":
            switch(node, t, detail, tm)
            if detail == "leader":
                captions.append((t, f"n{node} wins the election for term {tm}"))
        elif kind == "commit":
            commits[node].append((t, idx))
        elif kind == "crash":
            switch(node, t, "crashed")
            crashes.append((t, node))
            captions.append((t, f"n{node} (the leader) crashes"))
        elif kind == "restart":
            switch(node, t, "follower")
            restarts.append((t, node))
            captions.append((t, f"n{node} restarts from disk and rejoins as a follower"))
        elif kind == "partition":
            groups = [[int(x) for x in g.split(",")] for g in detail.split("|")]
            open_part = (t, groups)
            captions.append((t, f"network partition {detail.replace('|', ' | ')}: the majority side keeps committing"))
        elif kind == "heal" and open_part:
            partitions.append((open_part[0], t, open_part[1]))
            open_part = None
            captions.append((t, "partition healed: the minority catches up, no term inflation (PreVote)"))
        elif kind == "client":
            captions.append((t, "client " + detail))
        elif kind == "start":
            captions.append((t, "five followers, no leader yet"))
    if open_part:
        partitions.append((open_part[0], end, open_part[1]))
    for node in range(n):
        if end > since[node]:
            segments[node].append(Segment(since[node], end, state[node], term[node]))
    return Timeline(n, end, segments, commits, crashes, restarts, partitions, captions)


def stats_at(tl: Timeline, t: float) -> tuple[int | None, int, list[int]]:
    """Leader id (or None), its term, and every node's commit index at time t."""
    leader, term = None, 0
    for node, segs in enumerate(tl.segments):
        for s in segs:
            if s.start <= t <= s.end and s.start < t + 1e-9 and s.state == "leader" and (leader is None or s.term > term):
                leader, term = node, s.term
    cidx = []
    for node in range(tl.n):
        c = 0
        for ct, ci in tl.commits[node]:
            if ct <= t:
                c = ci
        cidx.append(c)
    return leader, term, cidx


def draw(tl: Timeline, t: float, fig: plt.Figure, scale: float) -> None:
    fig.clear()
    fig.patch.set_facecolor(BG)
    ax = fig.add_axes((0.075, 0.215, 0.765, 0.585))
    ax.set_facecolor(PANEL)
    ax.set_xlim(0, tl.end)
    ax.set_ylim(tl.n - 0.45, -0.55)
    for s in ax.spines.values():
        s.set_color(GRID)
    ax.set_yticks([])
    ax.tick_params(axis="x", colors=MUTED, labelsize=8 * scale, length=3)
    ax.set_xticks([i for i in range(0, int(tl.end) + 1)])
    ax.set_xticklabels([f"{i}s" for i in range(0, int(tl.end) + 1)])
    ax.grid(axis="x", color=GRID, lw=0.5, alpha=0.6)
    ax.set_axisbelow(True)

    h = 0.62
    # Partition bands first so lanes draw on top of them.
    for p0, p1, groups in tl.partitions:
        if p0 > t:
            continue
        ax.axvspan(p0, min(p1, t), color="#bc8cff", alpha=0.10, lw=0)
        ax.axvline(p0, color="#bc8cff", lw=1.0 * scale, ls=(0, (4, 3)))
        if p1 <= t:
            ax.axvline(p1, color="#bc8cff", lw=1.0 * scale, ls=(0, (4, 3)))
    for node in range(tl.n):
        for s in tl.segments[node]:
            if s.start >= t:
                break
            x1 = min(s.end, t)
            kw = {}
            if s.state == "crashed":
                kw = dict(hatch="////", edgecolor="#7d2a2a")
            elif s.state == "leader":
                kw = dict(edgecolor="#7ee787", linewidth=0.8)
            ax.add_patch(Rectangle((s.start, node - h / 2), x1 - s.start, h, facecolor=STATE_COLOR[s.state], lw=kw.pop("linewidth", 0), **kw))
            if s.state == "leader" and x1 - s.start > 0.25:
                ax.text(s.start + 0.04, node, f"leader  term {s.term}", color="#04260f", va="center", ha="left",
                        fontsize=7.5 * scale, fontweight="bold", clip_on=True)
        # Commit ticks.
        for ct, _ in tl.commits[node]:
            if ct <= t:
                ax.plot([ct, ct], [node - h / 2 + 0.06, node + h / 2 - 0.06], color=TICK, lw=0.9 * scale, alpha=0.85, solid_capstyle="butt")
    for ct, node in tl.crashes:
        if ct <= t:
            ax.plot([ct], [node], marker="X", ms=9 * scale, color=CRASH, mec=BG, mew=0.8)
    for rt, node in tl.restarts:
        if rt <= t:
            ax.plot([rt], [node], marker="^", ms=8 * scale, color="#7ee787", mec=BG, mew=0.8)
    for p0, p1, groups in tl.partitions:
        if p0 <= t:
            ax.text(p0 + 0.04, -0.47, "split  " + "  |  ".join("{" + ",".join(f"n{x}" for x in g) + "}" for g in groups),
                    color="#d2a8ff", fontsize=7.5 * scale, va="top", ha="left", fontweight="bold")
    ax.axvline(t, color="#ffffff", lw=1.1 * scale, alpha=0.9)

    # Lane labels (left) and per-node commit index (right).
    leader, term, cidx = stats_at(tl, t)
    for node in range(tl.n):
        fig.text(0.062, ax.transData.transform((0, node))[1] / fig.bbox.height, f"n{node}", color=NODE_COLOR[node],
                 fontsize=10 * scale, fontweight="bold", ha="right", va="center", family="DejaVu Sans Mono")
        fig.text(0.855, ax.transData.transform((0, node))[1] / fig.bbox.height, f"commit {cidx[node]:>3}",
                 color=TEXT if cidx[node] else MUTED, fontsize=8.5 * scale, ha="left", va="center", family="DejaVu Sans Mono")

    # Header.
    fig.text(0.075, 0.925, "quorum-kv", color=TEXT, fontsize=14 * scale, fontweight="bold", va="center")
    fig.text(0.075 + 0.17 / scale * (1.0 if scale <= 1 else 1.05), 0.925, "Raft on a simulated network: 5 nodes", color=MUTED, fontsize=10 * scale, va="center")
    lead = f"leader n{leader}  term {term}" if leader is not None else "no leader"
    fig.text(0.84, 0.925, f"t = {t:4.2f}s", color=TEXT, fontsize=10 * scale, ha="right", va="center", family="DejaVu Sans Mono")
    fig.text(0.075, 0.855, lead, color="#7ee787" if leader is not None else CRASH, fontsize=10 * scale, va="center", family="DejaVu Sans Mono")

    # Legend.
    lx = 0.30
    for label, key in [("follower", "follower"), ("pre-vote", "precandidate"), ("candidate", "candidate"), ("leader", "leader"), ("down", "crashed")]:
        fig.patches.append(Rectangle((lx, 0.845), 0.018, 0.022, transform=fig.transFigure, facecolor=STATE_COLOR[key],
                                     edgecolor=GRID, lw=0.6, hatch="////" if key == "crashed" else None))
        fig.text(lx + 0.024, 0.856, label, color=MUTED, fontsize=7.5 * scale, va="center")
        lx += 0.105
    fig.text(0.855, 0.856, "| = commit", color=TICK, fontsize=7.5 * scale, va="center")

    # Captions: the two most recent events.
    recent = [c for c in tl.captions if c[0] <= t][-3:]
    ys = [0.135, 0.085, 0.040]
    for i, (ct, text) in enumerate(reversed(recent)):
        fig.text(0.075, ys[i], f"{ct:5.2f}s  {text}", color=TEXT if i == 0 else MUTED, fontsize=(9.5 if i == 0 else 8.5) * scale,
                 va="center", alpha=1.0 if i == 0 else 0.75, family="DejaVu Sans Mono")


def render(csv_path: Path, out: Path, slow: float, fps: int, hold: float) -> None:
    tl = load(csv_path)
    out.mkdir(parents=True, exist_ok=True)

    # Static, 1600 px wide.
    fig = plt.figure(figsize=(16, 7.2), dpi=100)
    draw(tl, tl.end, fig, 1.35)
    png = out / "timeline.png"
    fig.savefig(png, facecolor=BG)
    plt.close(fig)
    with Image.open(png) as im:  # re-save without metadata
        clean = Image.new("RGB", im.size)
        clean.paste(im.convert("RGB"))
        clean.save(png, optimize=True)

    # Animation, 896 x 504.
    fig = plt.figure(figsize=(8, 4.5), dpi=112)
    frames = int(tl.end * slow * fps) + 1
    tmp = Path(tempfile.mkdtemp(prefix="quorum-frames-"))
    try:
        total = frames + int(hold * fps)
        for i in range(total):
            t = min(tl.end, i / fps / slow)
            draw(tl, t, fig, 1.0)
            fig.savefig(tmp / f"f{i:04d}.png", facecolor=BG)
        gif = out / "hero.gif"
        vf = (
            "split[a][b];[a]palettegen=max_colors=48:stats_mode=diff[p];"
            "[b][p]paletteuse=dither=none:diff_mode=rectangle"
        )
        subprocess.run(
            ["ffmpeg", "-y", "-loglevel", "error", "-framerate", str(fps), "-i", str(tmp / "f%04d.png"),
             "-vf", vf, "-loop", "0", str(gif)],
            check=True,
        )
        print(f"{gif}: {gif.stat().st_size / 1e6:.2f} MB, {total} frames; {png}")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
        plt.close(fig)


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("csv", type=Path)
    ap.add_argument("--out", type=Path, default=Path("docs/media"))
    ap.add_argument("--slow", type=float, default=1.8, help="animation seconds per real second (default 1.8, i.e. slowed down)")
    ap.add_argument("--fps", type=int, default=20)
    ap.add_argument("--hold", type=float, default=2.5, help="seconds to hold the final frame")
    a = ap.parse_args()
    render(a.csv, a.out, a.slow, a.fps, a.hold)
