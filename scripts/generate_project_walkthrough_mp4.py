from pathlib import Path
import io
import math
import subprocess
import textwrap
import zipfile

import cairosvg
import imageio_ffmpeg
from PIL import Image, ImageDraw, ImageFont


ROOT = Path(__file__).resolve().parents[1]
ICON_ZIP = Path("/tmp/Azure_Public_Service_Icons_V23.zip")
ICON_DIR = ROOT / "media" / "icons" / "azure"
OUTPUT = ROOT / "media" / "project_walkthrough_official_icons.mp4"
COVER = ROOT / "media" / "project_walkthrough_official_icons_cover.png"

WIDTH = 1280
HEIGHT = 720
FPS = 24
SCENE_SECONDS = 5

BG = (247, 250, 252)
INK = (15, 23, 42)
MUTED = (71, 85, 105)
LINE = (203, 213, 225)
AZURE = (0, 120, 212)
BLUE = (37, 99, 235)


ICON_SOURCES = {
    "sql": "Azure_Public_Service_Icons/Icons/databases/10132-icon-service-SQL-Server.svg",
    "ir": "Azure_Public_Service_Icons/Icons/databases/02392-icon-service-SSIS-Lift-And-Shift-IR.svg",
    "adf": "Azure_Public_Service_Icons/Icons/analytics/10126-icon-service-Data-Factories.svg",
    "keyvault": "Azure_Public_Service_Icons/Icons/security/10245-icon-service-Key-Vaults.svg",
    "storage": "Azure_Public_Service_Icons/Icons/storage/10086-icon-service-Storage-Accounts.svg",
    "databricks": "Azure_Public_Service_Icons/Icons/analytics/10787-icon-service-Azure-Databricks.svg",
    "synapse": "Azure_Public_Service_Icons/Icons/analytics/00606-icon-service-Azure-Synapse-Analytics.svg",
    "powerbi": "Azure_Public_Service_Icons/Icons/analytics/03332-icon-service-Power-BI-Embedded.svg",
}


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
    "source": {"label": "SQL Server\nSalesLT", "xy": (58, 304, 220, 104), "icon": "sql"},
    "ir": {"label": "Self-hosted\nIR", "xy": (315, 264, 188, 86), "icon": "ir"},
    "adf": {"label": "Data\nFactory", "xy": (315, 396, 188, 86), "icon": "adf"},
    "keyvault": {"label": "Key Vault", "xy": (315, 518, 188, 86), "icon": "keyvault"},
    "bronze": {"label": "Bronze\nRaw\nParquet", "xy": (610, 264, 205, 96), "icon": "storage"},
    "databricks": {"label": "Databricks\nPySpark", "xy": (610, 384, 205, 96), "icon": "databricks"},
    "silver": {"label": "Silver\nClean\nDelta", "xy": (610, 504, 205, 96), "icon": "storage"},
    "gold": {"label": "Gold\nCurated\nDelta", "xy": (870, 384, 205, 96), "icon": "storage"},
    "synapse": {"label": "Synapse SQL\nServerless\nviews", "xy": (1015, 264, 205, 96), "icon": "synapse"},
    "powerbi": {"label": "Power BI\nDashboards", "xy": (1015, 504, 205, 96), "icon": "powerbi"},
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


def load_font(size, bold=False):
    candidates = [
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf" if bold else "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "/usr/share/fonts/truetype/liberation2/LiberationSans-Bold.ttf" if bold else "/usr/share/fonts/truetype/liberation2/LiberationSans-Regular.ttf",
    ]
    for candidate in candidates:
        if Path(candidate).exists():
            return ImageFont.truetype(candidate, size)
    return ImageFont.load_default()


TITLE = load_font(39, True)
SUBTITLE = load_font(24)
SECTION = load_font(27, True)
LABEL = load_font(19, True)
BODY = load_font(20)
SMALL = load_font(16)


def ensure_icons():
    ICON_DIR.mkdir(parents=True, exist_ok=True)
    archive = None
    try:
        for key, source in ICON_SOURCES.items():
            svg_path = ICON_DIR / f"{key}.svg"
            png_path = ICON_DIR / f"{key}.png"
            if not svg_path.exists():
                if archive is None:
                    if not ICON_ZIP.exists():
                        raise FileNotFoundError(
                            f"{ICON_ZIP} not found. Download Azure_Public_Service_Icons_V23.zip from Microsoft Learn first."
                        )
                    archive = zipfile.ZipFile(ICON_ZIP)
                svg_path.write_bytes(archive.read(source))
            if not png_path.exists():
                cairosvg.svg2png(bytestring=svg_path.read_bytes(), write_to=str(png_path), output_width=96, output_height=96)
    finally:
        if archive is not None:
            archive.close()


def load_icons():
    return {key: Image.open(ICON_DIR / f"{key}.png").convert("RGBA") for key in ICON_SOURCES}


def ease(value):
    return 0.5 - 0.5 * math.cos(math.pi * value)


def center(node_key):
    x, y, w, h = NODES[node_key]["xy"]
    return x + w // 2, y + h // 2


def draw_wrapped(draw, text, xy, font, fill, width, spacing=5):
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


def draw_card(frame, draw, key, active_keys, icons):
    node = NODES[key]
    x, y, w, h = node["xy"]
    active = key in active_keys
    outline = AZURE if active else LINE
    fill = (255, 255, 255) if active else (241, 245, 249)
    shadow = (15, 23, 42, 24 if active else 10)

    draw.rounded_rectangle((x + 7, y + 8, x + w + 7, y + h + 8), radius=18, fill=shadow)
    draw.rounded_rectangle((x, y, x + w, y + h), radius=18, fill=fill, outline=outline, width=3 if active else 2)
    icon = icons[node["icon"]].resize((54, 54), Image.Resampling.LANCZOS)
    frame.paste(icon, (x + 16, y + 22), icon)
    draw_wrapped(draw, node["label"], (x + 82, y + 22), LABEL, INK if active else MUTED, 14, spacing=3)


def draw_frame(scene_index, scene, frame_index, icons):
    frame = Image.new("RGB", (WIDTH, HEIGHT), BG)
    draw = ImageDraw.Draw(frame, "RGBA")
    progress = ease(frame_index / max(1, FPS * SCENE_SECONDS - 1))

    draw.text((52, 42), "SQLServer-Azure-DataLakehouse", font=TITLE, fill=INK)
    draw.text((56, 95), "MP4 walkthrough with Microsoft Azure Architecture Icons", font=SUBTITLE, fill=MUTED)

    draw.rounded_rectangle((55, 136, 1225, 187), radius=18, fill=(255, 255, 255), outline=LINE, width=2)
    draw.text((80, 148), f"{scene_index + 1}. {scene['title']}", font=SECTION, fill=INK)
    draw.text((330, 153), scene["subtitle"], font=BODY, fill=MUTED)

    draw.rounded_rectangle((55, 203, 1225, 249), radius=16, fill=(239, 246, 255), outline=(191, 219, 254), width=2)
    draw_wrapped(draw, scene["caption"], (82, 212), BODY, (30, 64, 175), 68, spacing=2)

    active = set(scene["active"])
    for src, dst in EDGES:
        line_color = (148, 163, 184)
        line_progress = 1.0
        if scene.get("flow_to") == dst or (src in active and dst in active):
            line_color = BLUE
            line_progress = progress
        draw_arrow(draw, center(src), center(dst), color=line_color, width=4 if line_color == BLUE else 2, progress=line_progress)

    for key in NODES:
        draw_card(frame, draw, key, active, icons)

    draw.rounded_rectangle((70, 622, 1210, 675), radius=20, fill=(255, 255, 255), outline=LINE, width=2)
    draw.text((96, 638), "10 SalesLT tables", font=LABEL, fill=INK)
    draw.text((300, 640), "Customer, Product, Address, Orders, Categories, Descriptions", font=SMALL, fill=MUTED)
    draw.text((842, 638), "bronze -> silver -> gold", font=LABEL, fill=(124, 58, 237))

    start_x = 430
    y = 694
    for idx in range(len(SCENES)):
        color = AZURE if idx <= scene_index else (203, 213, 225)
        draw.ellipse((start_x + idx * 48, y, start_x + idx * 48 + 14, y + 14), fill=color)

    return frame


def write_mp4(frames):
    ffmpeg = imageio_ffmpeg.get_ffmpeg_exe()
    cmd = [
        ffmpeg,
        "-y",
        "-f",
        "rawvideo",
        "-vcodec",
        "rawvideo",
        "-s",
        f"{WIDTH}x{HEIGHT}",
        "-pix_fmt",
        "rgb24",
        "-r",
        str(FPS),
        "-i",
        "-",
        "-an",
        "-vcodec",
        "libx264",
        "-pix_fmt",
        "yuv420p",
        "-movflags",
        "+faststart",
        str(OUTPUT),
    ]
    proc = subprocess.Popen(cmd, stdin=subprocess.PIPE)
    try:
        for frame in frames:
            proc.stdin.write(frame.tobytes())
    finally:
        proc.stdin.close()
    return_code = proc.wait()
    if return_code:
        raise RuntimeError(f"ffmpeg exited with code {return_code}")


def main():
    ensure_icons()
    icons = load_icons()
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)

    frames = []
    for scene_index, scene in enumerate(SCENES):
        for frame_index in range(FPS * SCENE_SECONDS):
            frames.append(draw_frame(scene_index, scene, frame_index, icons))

    write_mp4(frames)
    frames[0].save(COVER, "PNG", optimize=True)
    print(f"Wrote {OUTPUT}")
    print(f"Wrote {COVER}")


if __name__ == "__main__":
    main()
