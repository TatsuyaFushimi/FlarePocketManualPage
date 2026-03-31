import asyncio
import base64
import re
import shutil
from pathlib import Path

import anthropic

from .database import update_job, save_issues

BASE_DIR = Path(__file__).parent.parent
FFMPEG = "/tmp/ffmpeg_bin/ffmpeg"

# In-memory progress (same process as FastAPI)
_progress: dict = {}


def get_progress(job_id: str) -> dict:
    return _progress.get(job_id, {})


def _set(job_id, status, progress, text):
    _progress[job_id] = {"status": status, "progress": progress, "text": text}
    update_job(job_id, status=status, progress=progress, progress_text=text)


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

async def process_job(job_id: str, source: tuple, slide_mode: str, engine: str = "claude"):
    tmp_dir = BASE_DIR / "tmp" / job_id
    frames_dir = tmp_dir / "frames"
    try:
        tmp_dir.mkdir(parents=True, exist_ok=True)

        _set(job_id, "processing", 5, "動画を取得中...")
        video_path = await _get_video(source, tmp_dir)

        if engine == "gemini":
            issues = await _process_gemini(job_id, video_path, slide_mode)
        else:
            frames_dir.mkdir(parents=True, exist_ok=True)
            _set(job_id, "processing", 15, "フレームを抽出中...")
            total_frames = await _extract_frames(video_path, frames_dir)

            _set(job_id, "processing", 22, "キーワードを確認中...")
            keywords = _grep_keywords()

            _set(job_id, "processing", 25, f"添削中... (全 {total_frames} フレーム、4並列)")
            issues = await _parallel_review(frames_dir, total_frames, slide_mode, keywords)

        _set(job_id, "processing", 95, "レポートを保存中...")
        save_issues(job_id, issues)

        shutil.rmtree(tmp_dir, ignore_errors=True)
        _set(job_id, "done", 100, f"完了（{len(issues)} 件）")

    except Exception as e:
        shutil.rmtree(tmp_dir, ignore_errors=True)
        _set(job_id, "error", 0, f"エラー: {e}")
        raise


# ---------------------------------------------------------------------------
# Video / frame helpers
# ---------------------------------------------------------------------------

async def _get_video(source: tuple, tmp_dir: Path) -> Path:
    source_type, source_value = source
    video_path = tmp_dir / "input.mp4"

    if source_type == "url":
        loop = asyncio.get_event_loop()

        def _dl():
            from pytubefix import YouTube
            yt = YouTube(source_value)
            stream = yt.streams.get_by_itag(18) or yt.streams.filter(progressive=True).first()
            stream.download(output_path=str(tmp_dir), filename="input.mp4")

        await loop.run_in_executor(None, _dl)
    else:
        shutil.copy(source_value, video_path)

    return video_path


async def _extract_frames(video_path: Path, frames_dir: Path) -> int:
    cmd = [
        FFMPEG, "-i", str(video_path),
        "-vf", "fps=1", "-q:v", "2",
        str(frames_dir / "frame_%05d.jpg"), "-y"
    ]
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
    )
    await proc.wait()
    return len(list(frames_dir.glob("frame_*.jpg")))


def _grep_keywords() -> list[str]:
    script_path = BASE_DIR / "input" / "script.txt"
    if not script_path.exists():
        return []
    try:
        text = script_path.read_text(encoding="utf-8", errors="ignore")
        targets = ["eBay", "Shopee", "SHOPEE", "SLS", "SNS", "Zoom", "ZOOM",
                   "Sekai", "ASIN", "リンギット", "ドル"]
        return [t for t in targets if t in text]
    except Exception:
        return []


# ---------------------------------------------------------------------------
# Parallel review
# ---------------------------------------------------------------------------

def _split(total: int, n: int = 4) -> list[dict]:
    size = total // n
    sections = []
    for i in range(n):
        start = i * size + 1
        end = (i + 1) * size if i < n - 1 else total
        sections.append({"start": start, "end": end})
    return sections


async def _parallel_review(frames_dir, total_frames, slide_mode, keywords) -> list[dict]:
    sections = _split(total_frames, n=4)
    tasks = [
        _review_section(i + 1, s["start"], s["end"], frames_dir, slide_mode, keywords)
        for i, s in enumerate(sections)
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    all_issues: list[dict] = []
    for r in results:
        if isinstance(r, list):
            all_issues.extend(r)

    all_issues.sort(key=_tc_sort)
    return all_issues


def _tc_sort(issue: dict) -> int:
    tc = issue.get("timecode", "")
    parts = re.findall(r"\d+", tc)
    try:
        if len(parts) >= 3:
            return int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
        if len(parts) == 2:
            return int(parts[0]) * 60 + int(parts[1])
    except Exception:
        pass
    return 999999


# ---------------------------------------------------------------------------
# Single section review (agentic tool-use loop)
# ---------------------------------------------------------------------------

SYSTEM = """\
あなたは動画編集の外部レビュアーです。read_frame ツールでフレームを読み込みながら、
テロップの誤字・固有名詞・改行・モザイク等を指摘してください。

チェックルール：
- HY1: 固有名詞誤表記（eBay/Shopee/Sekai Pocket/Zoom/SLS等）、誤字脱字
- HY2: 二重否定・二重敬語・「やつ」等の口語表現・語彙ミス
- HY4: 全角数字→半角
- HY6: 助詞直後改行・接続助詞の途中改行・短すぎる1行テロップ
- MO: ASIN/セラーID/仕入価格の露出（バーコード・商品価格・実践者顔写真は指摘不要）
- SE: 演出テロップ（迫力系・強調系）なし（音声SEの有無はフレームでは判定不可なので指摘しない）
- CA: バナー・ロゴ未表示・エンドカード誤字
- TX2: 相槌のみのカット候補

スライドについて：
- スライド上にテロップ・バナーが重なって表示されるのは正常（指摘不要）

必須ルール（幻覚防止・タイムコード精度）：
1. 指摘する前に必ず read_frame で該当フレームを目視確認する。フレームを見ずに指摘しない。
2. 疑問文「？」なしの指摘は特に誤検出が多い。指摘前に必ず該当フレームで「？」の有無を確認する。
3. タイムコードは read_frame が返したファイル名に表示された時刻（例: frame_01346.jpg → 22:26）をそのまま使うこと。自分で時刻を計算・推測しない。
4. テロップ出現の正確なタイミングを確認するため、疑わしい箇所は前後3フレームも読んで、テロップが最初に表示されているフレームの時刻を記載する。
5. 冒頭SEは指摘しない（フレームから判定不可）。

出力形式（必須・箇条書き）：
- MM:SS HY1 テロップ「xxx」→「yyy」に修正
疑わしければ指摘する。ただしフレームを確認した上で指摘すること。"""


async def _review_section(
    section_num: int,
    start: int,
    end: int,
    frames_dir: Path,
    slide_mode: str,
    keywords: list[str],
) -> list[dict]:
    client = anthropic.AsyncAnthropic()

    slide_note = "スライドは無視してください。" if slide_mode == "ignore" else "スライドも含めてチェックしてください。"
    kw_note = f"特に注意: {', '.join(keywords)}" if keywords else ""

    tools = [
        {
            "name": "read_frame",
            "description": f"フレームを読み込みます（このセクション: {start}〜{end}）",
            "input_schema": {
                "type": "object",
                "properties": {
                    "frame_number": {
                        "type": "integer",
                        "description": f"フレーム番号（{start}〜{end}）",
                    }
                },
                "required": ["frame_number"],
            },
        }
    ]

    start_mm = f"{start // 60:02d}:{start % 60:02d}"
    end_mm = f"{end // 60:02d}:{end % 60:02d}"

    messages = [
        {
            "role": "user",
            "content": (
                f"Section {section_num}: frame {start}〜{end}（{start_mm}〜{end_mm}）を担当します。\n"
                f"{slide_note}{kw_note}\n\n"
                "5フレーム間隔で read_frame を呼んで全範囲をスキャンし、"
                "疑わしい箇所は前後も確認してください。"
                "最後に全指摘を箇条書きでまとめてください。"
            ),
        }
    ]

    for _ in range(80):  # max iterations
        response = await client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=8096,
            system=SYSTEM,
            tools=tools,
            messages=messages,
        )

        if response.stop_reason == "end_turn":
            text = "".join(b.text for b in response.content if hasattr(b, "text"))
            return _parse(text)

        if response.stop_reason == "tool_use":
            messages.append({"role": "assistant", "content": response.content})
            tool_results = []
            for block in response.content:
                if block.type != "tool_use" or block.name != "read_frame":
                    continue
                frame_num = block.input.get("frame_number", start)
                frame_path = frames_dir / f"frame_{frame_num:05d}.jpg"
                if frame_path.exists():
                    img_b64 = base64.standard_b64encode(frame_path.read_bytes()).decode()
                    content = [
                        {"type": "image", "source": {"type": "base64", "media_type": "image/jpeg", "data": img_b64}},
                        {"type": "text", "text": f"frame_{frame_num:05d}.jpg ({frame_num // 60:02d}:{frame_num % 60:02d})"},
                    ]
                else:
                    content = f"frame {frame_num} が見つかりません"
                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": block.id,
                    "content": content,
                })
            messages.append({"role": "user", "content": tool_results})
        else:
            break

    return []


def _parse(text: str) -> list[dict]:
    issues = []
    pattern = re.compile(r"-\s*([\d:]+(?:〜[\d:]+)?|全編[^\s]*)\s+([A-Z]{2}\d?)\S*\s*[：:]?\s*(.+)")
    for line in text.splitlines():
        m = pattern.match(line.strip())
        if not m:
            continue
        tc, rule, desc = m.group(1).strip(), m.group(2).strip(), m.group(3).strip()
        if "問題ありません" in desc:
            continue
        issues.append({
            "timecode": tc,
            "rule": rule,
            "description": desc,
            "feedback": None,
        })
    return issues


# ---------------------------------------------------------------------------
# Gemini engine
# ---------------------------------------------------------------------------

GEMINI_PROMPT = """\
あなたは日本語動画編集の外部レビュアーです。
以下のルールに従ってテロップ・演出の問題点を指摘してください。

チェックルール：
- HY1: 固有名詞誤表記（eBay/Shopee/Sekai Pocket/Zoom/SLS等）、誤字脱字
- HY2: 二重否定・二重敬語・「やつ」等の口語表現・語彙ミス
- HY4: 全角数字→半角
- HY6: 助詞直後改行・接続助詞の途中改行・短すぎる1行テロップ
- MO: ASIN/セラーID/仕入価格の露出（バーコード・商品価格・実践者顔写真は指摘不要）
- SE: 演出テロップ（迫力系・強調系）なし
- CA: バナー・ロゴ未表示・エンドカード誤字
- TX2: 相槌のみのカット候補

スライドについて：スライド上にテロップ・バナーが重なって表示されるのは正常（指摘不要）

必須ルール：
1. タイムコードは動画内の実際の時刻を正確に記載する（推測しない）
2. 疑問文「？」の有無は映像を確認してから指摘する
3. バーコード・商品価格・実践者顔写真のMO指摘は不要
4. 冒頭SEは指摘しない
5. 全角→半角の指摘はテロップの文字が実際に全角の場合のみ指摘する

出力形式（箇条書きのみ・表は使わない）：
- MM:SS HY1 テロップ「xxx」→「yyy」に修正

疑わしければ指摘する。動画全体を通してチェックし、指摘をすべて箇条書きで出力してください。"""


def _tc_to_sec(tc: str) -> int:
    parts = tc.strip().split(":")
    try:
        if len(parts) == 3:
            return int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
        if len(parts) == 2:
            return int(parts[0]) * 60 + int(parts[1])
    except Exception:
        pass
    return 0


def _sec_to_tc(s: int) -> str:
    h = s // 3600
    m = (s % 3600) // 60
    sec = s % 60
    if h > 0:
        return f"{h}:{m:02d}:{sec:02d}"
    return f"{m:02d}:{sec:02d}"


def _parse_gemini(text: str, tc_offset: int = 0) -> list[dict]:
    issues = []
    pattern = re.compile(r"-\s*(\d+:\d+(?::\d+)?)\s+([A-Z]{2}\d?)\S*\s*[：:]?\s*(.+)")
    for line in text.splitlines():
        m = pattern.match(line.strip())
        if not m:
            continue
        tc, rule, desc = m.group(1).strip(), m.group(2).strip(), m.group(3).strip()
        if "問題ありません" in desc:
            continue
        if "より丁寧な表現に修正することをお勧めします" in desc and "→" not in desc:
            continue
        if tc_offset:
            tc = _sec_to_tc(_tc_to_sec(tc) + tc_offset)
        issues.append({"timecode": tc, "rule": rule, "description": desc, "feedback": None})
    return issues


async def _process_gemini(job_id: str, video_path: Path, slide_mode: str) -> list[dict]:
    import os
    import time as _time
    from google import genai

    api_key = os.environ.get("GEMINI_API_KEY", "")
    if not api_key:
        raise RuntimeError("GEMINI_API_KEY が設定されていません")

    client = genai.Client(api_key=api_key)
    slide_note = "スライドは無視してください。" if slide_mode == "ignore" else "スライドも含めてチェックしてください。"
    prompt = GEMINI_PROMPT + f"\n\n{slide_note}"

    # 動画の長さを取得して分割必要か判定（60分 = 3600秒が目安）
    proc = await asyncio.create_subprocess_exec(
        FFMPEG, "-i", str(video_path),
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.PIPE,
    )
    _, stderr = await proc.communicate()
    duration = 0
    m = re.search(r"Duration:\s*(\d+):(\d+):(\d+)", stderr.decode(errors="ignore"))
    if m:
        duration = int(m.group(1)) * 3600 + int(m.group(2)) * 60 + int(m.group(3))

    LIMIT = 55 * 60  # 55分以上なら分割

    loop = asyncio.get_event_loop()

    def _upload_wait(path: Path, label: str):
        f = client.files.upload(file=str(path))
        for _ in range(60):
            if f.state.name == "ACTIVE":
                return f
            if f.state.name == "FAILED":
                raise RuntimeError(f"Gemini upload failed: {label}")
            _time.sleep(5)
            f = client.files.get(name=f.name)
        raise RuntimeError(f"Gemini upload timeout: {label}")

    if duration <= LIMIT:
        # 分割不要
        _set(job_id, "processing", 30, "Gemini にアップロード中...")
        video_file = await loop.run_in_executor(None, _upload_wait, video_path, "full")
        _set(job_id, "processing", 50, "Gemini 添削中...")

        def _generate(vf):
            resp = client.models.generate_content(
                model="gemini-2.5-flash",
                contents=[vf, prompt],
            )
            client.files.delete(name=vf.name)
            return resp.text

        text = await loop.run_in_executor(None, _generate, video_file)
        issues = _parse_gemini(text)
    else:
        # 前半・後半に分割
        mid = duration // 2
        part1 = video_path.parent / "part1.mp4"
        part2 = video_path.parent / "part2.mp4"

        _set(job_id, "processing", 20, "動画を分割中...")
        mid_tc = _sec_to_tc(mid)
        for args in [
            [FFMPEG, "-i", str(video_path), "-t", str(mid), "-c", "copy", str(part1), "-y"],
            [FFMPEG, "-i", str(video_path), "-ss", str(mid), "-c", "copy", str(part2), "-y"],
        ]:
            p = await asyncio.create_subprocess_exec(*args,
                stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL)
            await p.wait()

        _set(job_id, "processing", 30, "Gemini にアップロード中（前半）...")
        f1 = await loop.run_in_executor(None, _upload_wait, part1, "part1")
        _set(job_id, "processing", 40, "Gemini にアップロード中（後半）...")
        f2 = await loop.run_in_executor(None, _upload_wait, part2, "part2")

        _set(job_id, "processing", 50, "Gemini 添削中（前半）...")

        def _gen1(vf):
            r = client.models.generate_content(model="gemini-2.5-flash", contents=[vf, prompt])
            client.files.delete(name=vf.name)
            return r.text

        text1 = await loop.run_in_executor(None, _gen1, f1)
        issues = _parse_gemini(text1, tc_offset=0)

        _set(job_id, "processing", 75, f"Gemini 添削中（後半, offset={mid_tc}）...")

        def _gen2(vf):
            r = client.models.generate_content(model="gemini-2.5-flash", contents=[vf, prompt])
            client.files.delete(name=vf.name)
            return r.text

        text2 = await loop.run_in_executor(None, _gen2, f2)
        issues += _parse_gemini(text2, tc_offset=mid)

    issues.sort(key=_tc_sort)
    return issues
