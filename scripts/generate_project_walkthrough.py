from pathlib import Path
import math
import textwrap

from PIL import Image, ImageDraw, ImageFont


ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / "media" / "project_walkthrough.gif"
COVER = ROOT / "media" / "project_walkthrough_cover.png"

WIDTH = 1280
HEIGHT = 720
FPS = 12
SCENE_SECONDS = 7
FRAME_MS = int(1000 / FPS)

BG = (248, 250, 252)
INK = (15, 23, 42)
MUTED = (71, 85, 105)
LINE = (203, 213, 225)

AZURE = (0, 120, 212)
GREEN = (13, 148, 136)
PURPLE = (124, 58, 237)
GOLD = (202, 138, 4)
RED = (220, 38, 38)


def load_font(size, bold=False):
    candidates = [
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf" if bold else "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "/usr/share/fonts/truetype/liberation2/LiberationSans-Bold.ttf" if bold else "/usr/share/fonts/truetype/liberation2/LiberationSans-Regular.ttf",
    ]
    for candidate in candidates:
        path = Path(candidate)
        if path.exists():
            return ImageFont.truetype(str(path), size)
    return ImageFont.load_default()


TITLE = load_font(40, True)
SUBTITLE = load_font(26, False)
SECTION = load_font(28, True)
LABEL = load_font(20, True)
BODY = load_font(20, False)
SMALL = load_font(16, False)


SCENES = [
    {
        "title": "The Problem",
        "subtitle": "Operational data is locked in on-premises SQL Server.",
        "caption": "Manual exports slow reporting and make analytics hard to scale.",
        "active": ["source"],
    },
    {
        "title": "Project Goal",
        "subtitle": "Move SalesLT data into a secure Azure lakehouse pipeline.",
        "caption": "Automated ingestion, repeatable transformations, and BI-ready output.",
        "active": ["source", "adf", "bronze", "silver", "gold", "synapse", "powerbi"],
    },
    {
        "title": "Ingest",
        "subtitle": "Data Factory copies SQL Server tables through a self-hosted runtime.",
        "caption": "Key Vault protects credentials while ADF lands raw Parquet files.",
        "active": ["source", "ir", "adf", "keyvault", "bronze"],
        "flow_to": "bronze",
    },
    {
        "title": "Bronze Layer",
        "subtitle": "Raw SalesLT tables land in ADLS Gen2 as Parquet.",
        "caption": "The raw zone preserves the source shape for replay and auditing.",
        "active": ["bronze"],
    },
    {
        "title": "Silver Layer",
        "subtitle": "Databricks standardizes date fields and writes Delta tables.",
        "caption": "The Silver layer improves consistency while preserving business columns.",
        "active": ["bronze", "databricks", "silver"],
        "flow_to": "silver",
    },
    {
        "title": "Gold Layer",
        "subtitle": "Columns become underscore-separated and analyst-friendly.",
        "caption": "Gold Delta tables are curated for SQL analytics and Power BI.",
        "active": ["silver", "databricks", "gold"],
        "flow_to": "gold",
    },
    {
        "title": "Serving",
        "subtitle": "Synapse serverless SQL exposes views over Gold Delta data.",
        "caption": "Reporting tools query a SQL interface without another warehouse copy.",
        "active": ["gold", "synapse"],
        "flow_to": "synapse",
    },
    {
        "title": "Final Output",
        "subtitle": "Power BI dashboards show customers, products, orders, and sales trends.",
        "caption": "Business users get cleaner, fresher analytics from the Azure pipeline.",
        "active": ["synapse", "powerbi"],
        "flow_to": "powerbi",
    },
    {
        "title": "Outcome",
        "subtitle": "A secure, automated path from on-premises data to business intelligence.",
        "caption": "Faster reporting, cleaner data, less manual work, and reusable cloud analytics.",
        "active": ["source", "adf", "bronze", "silver", "gold", "synapse", "powerbi"],
    },
]


NODES = {
    "source": {"label": "SQL Server\nAdventureWorksLT", "xy": (70, 285, 220, 110), "color": AZURE},
    "ir": {"label": "Self-hosted IR", "xy": (310, 245, 190, 85), "color": GREEN},
    "adf": {"label": "Data Factory", "xy": (310, 380, 190, 85), "color": AZURE},
    "keyvault": {"label": "Key Vault", "xy": (310, 515, 190, 85), "color": GOLD},
    "bronze": {"label": "Bronze\nRaw Parquet", "xy": (610, 205, 205, 95), "color": GOLD},
    "databricks": {"label": "Databricks\nPySpark", "xy": (610, 350, 205, 95), "color": RED},
    "silver": {"label": "Silver\nClean Delta", "xy": (610, 495, 205, 95), "color": MUTED},
    "gold": {"label": "Gold\nCurated Delta", "xy": (870, 350, 205, 95), "color": PURPLE},
    "synapse": {"label": "Synapse SQL\nServerless views", "xy": (1015, 235, 205, 95), "color": PURPLE},
    "powerbi": {"label": "Power BI\nDashboards", "xy": (1015, 485, 205, 95), "color": GOLD},
}

EDGES = [
    ("source", "ir"),
    ("ir", "adf"),
    ("keyvault", "adf"),
    ("adf", "bronze"),
    ("bronze", "databricks"),
    ("databricks", "silver"),
    ("silver", "databricks"),
    ("databricks", "gold"),
    ("gold", "synapse"),
    ("synapse", "powerbi"),
]


def ease(value):
    return 0.5 - 0.5 * math.cos(math.pi * value)


def center(node_key):
    x, y, w, h = NODES[node_key]["xy"]
    return x + w // 2, y + h // 2


def draw_wrapped(draw, text, xy, font, fill, width, spacing=6):
    x, y = xy
    for paragraph in text.split("\n"):
        for line in textwrap.wrap(paragraph, width=width):
            draw.text((x, y), line, font=font, fill=fill)
            bbox = draw.textbbox((0, 0), line, font=font)
            y += bbox[3] - bbox[1] + spacing
    return y


def draw_arrow(draw, start, end, color=LINE, width=3, progress=1.0):
    sx, sy = start
    ex, ey = end
    px = sx + (ex - sx) * progress
    py = sy + (ey - sy) * progress
    draw.line((sx, sy, px, py), fill=color, width=width)
    if progress >= 0.98:
        angle = math.atan2(ey - sy, ex - sx)
        size = 12
        p1 = (ex - size * math.cos(angle - 0.45), ey - size * math.sin(angle - 0.45))
        p2 = (ex - size * math.cos(angle + 0.45), ey - size * math.sin(angle + 0.45))
        draw.polygon([end, p1, p2], fill=color)


def draw_card(draw, key, active_keys):
    node = NODES[key]
    x, y, w, h = node["xy"]
    active = key in active_keys
    color = node["color"]
    fill = (255, 255, 255) if active else (241, 245, 249)
    outline = color if active else LINE
    shadow = (15, 23, 42, 22 if active else 10)

    draw.rounded_rectangle((x + 8, y + 8, x + w + 8, y + h + 8), radius=18, fill=shadow)
    draw.rounded_rectangle((x, y, x + w, y + h), radius=18, fill=fill, outline=outline, width=3 if active else 2)
    draw.rounded_rectangle((x + 18, y + 22, x + 58, y + 62), radius=10, fill=(*color, 35))
    draw.ellipse((x + 29, y + 33, x + 47, y + 51), fill=color)
    draw_wrapped(draw, node["label"], (x + 72, y + 23), LABEL, INK if active else MUTED, 18, spacing=4)


def draw_lake_summary(draw):
    tables = "10 SalesLT tables"
    layers = "bronze -> silver -> gold"
    draw.rounded_rectangle((70, 620, 1210, 675), radius=20, fill=(255, 255, 255), outline=LINE, width=2)
    draw.text((95, 636), tables, font=LABEL, fill=INK)
    draw.text((300, 636), "Customer, Product, Address, Orders, Categories, Descriptions", font=SMALL, fill=MUTED)
    draw.text((840, 636), layers, font=LABEL, fill=PURPLE)


def draw_frame(scene_index, scene, frame_index):
    frame = Image.new("RGB", (WIDTH, HEIGHT), BG)
    draw = ImageDraw.Draw(frame, "RGBA")
    progress = ease(frame_index / max(1, FPS * SCENE_SECONDS - 1))

    draw.text((54, 42), "On-Premises SQL Server to Azure Data Platform", font=TITLE, fill=INK)
    draw.text((58, 98), "Animated project walkthrough", font=SUBTITLE, fill=MUTED)

    # Header scene card.
    draw.rounded_rectangle((55, 140, 1225, 190), radius=18, fill=(255, 255, 255), outline=LINE, width=2)
    draw.text((80, 151), f"{scene_index + 1}. {scene['title']}", font=SECTION, fill=INK)
    draw.text((330, 156), scene["subtitle"], font=BODY, fill=MUTED)

    active = set(scene["active"])
    for src, dst in EDGES:
        line_color = (148, 163, 184)
        line_progress = 1.0
        if scene.get("flow_to") == dst or (src in active and dst in active):
            line_color = AZURE
            line_progress = progress
        draw_arrow(draw, center(src), center(dst), color=line_color, width=4 if line_color == AZURE else 2, progress=line_progress)

    for key in NODES:
        draw_card(draw, key, active)

    draw_lake_summary(draw)

    # Caption panel.
    draw.rounded_rectangle((55, 205, 1225, 250), radius=16, fill=(239, 246, 255), outline=(191, 219, 254), width=2)
    draw.text((82, 218), scene["caption"], font=BODY, fill=(30, 64, 175))

    # Progress dots.
    start_x = 430
    y = 694
    for idx in range(len(SCENES)):
        color = AZURE if idx <= scene_index else (203, 213, 225)
        draw.ellipse((start_x + idx * 48, y, start_x + idx * 48 + 14, y + 14), fill=color)

    return frame


def main():
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    frames = []
    for scene_index, scene in enumerate(SCENES):
        for frame_index in range(FPS * SCENE_SECONDS):
            frames.append(draw_frame(scene_index, scene, frame_index))

    frames[0].save(
        OUTPUT,
        save_all=True,
        append_images=frames[1:],
        duration=FRAME_MS,
        loop=0,
        optimize=True,
    )
    frames[0].save(COVER, "PNG", optimize=True)
    print(f"Wrote {OUTPUT}")
    print(f"Wrote {COVER}")


if __name__ == "__main__":
    main()
