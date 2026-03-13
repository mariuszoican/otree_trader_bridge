import json
import socket
import traceback
from datetime import datetime, timezone
from pathlib import Path
from urllib import error, parse, request


def _log(message, prefix="trader_bridge", **context):
    ts = datetime.now(timezone.utc).isoformat()
    if context:
        details = ", ".join(f"{k}={repr(v)}" for k, v in sorted(context.items()))
        print(f"[{prefix}][{ts}] {message} | {details}", flush=True)
    else:
        print(f"[{prefix}][{ts}] {message}", flush=True)


_DAY_TIMING_LOG_PATH = Path(__file__).resolve().parent.parent / "logs" / "day_timing.log"


def _json_safe(value):
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(v) for v in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except Exception:
            return str(value)
    return str(value)


def _log_day_timing(event, **context):
    payload = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "event": str(event),
        **{str(k): _json_safe(v) for k, v in context.items()},
    }
    serialized = json.dumps(payload, sort_keys=True)
    print(f"[day_timing] {serialized}", flush=True)
    _DAY_TIMING_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with _DAY_TIMING_LOG_PATH.open("a", encoding="utf-8") as f:
        f.write(serialized + "\n")


def _as_int(value, fallback):
    try:
        return int(value)
    except (TypeError, ValueError):
        return fallback


def _as_bool(value, fallback=False):
    if isinstance(value, bool):
        return value
    if value is None:
        return fallback
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False
    return fallback


def _as_float(value, fallback=0.0):
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(fallback)


def _resolve_day_duration_minutes(cfg, default_duration_minutes):
    return max(
        1,
        _as_int(
            cfg.get("trading_day_duration", default_duration_minutes),
            default_duration_minutes,
        ),
    )


def _normalize_http_base(base_url, default_base_url):
    url = str(base_url or "").strip()
    if not url:
        return str(default_base_url)
    return url.rstrip("/")


def _ws_base_from_http(http_base):
    pieces = parse.urlsplit(http_base)
    scheme = "wss" if pieces.scheme == "https" else "ws"
    path = pieces.path.rstrip("/")
    return parse.urlunsplit((scheme, pieces.netloc, path, "", ""))


def _post_json(url, payload, timeout_seconds, log_fn=_log):
    log_fn("HTTP POST starting", url=url, timeout_seconds=timeout_seconds, payload=payload)
    body = json.dumps(payload).encode("utf-8")
    req = request.Request(
        url=url,
        data=body,
        method="POST",
        headers={"Content-Type": "application/json"},
    )
    try:
        with request.urlopen(req, timeout=timeout_seconds) as resp:
            charset = resp.headers.get_content_charset("utf-8")
            raw = resp.read().decode(charset)
            log_fn(
                "HTTP POST response received",
                status=getattr(resp, "status", None),
                reason=getattr(resp, "reason", None),
                headers=dict(resp.headers.items()),
                raw_body=raw,
            )
            parsed = json.loads(raw)
            log_fn("HTTP POST response parsed", parsed=parsed)
            return parsed
    except error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        log_fn("HTTP POST failed with HTTPError", status=exc.code, reason=exc.reason, body=detail)
        raise RuntimeError(f"Trading API HTTP {exc.code}: {detail}") from exc
    except error.URLError as exc:
        log_fn("HTTP POST failed with URLError", reason=str(exc.reason), type=str(type(exc.reason)))
        raise RuntimeError(f"Trading API unreachable at {url}: {exc.reason}") from exc
    except socket.timeout as exc:
        log_fn("HTTP POST failed with socket.timeout", timeout_seconds=timeout_seconds)
        raise RuntimeError(f"Trading API timed out after {timeout_seconds} seconds.") from exc
    except Exception as exc:
        log_fn("HTTP POST failed with unexpected error", error=str(exc), traceback=traceback.format_exc())
        raise


def _get_json(url, timeout_seconds, log_fn=_log):
    log_fn("HTTP GET starting", url=url, timeout_seconds=timeout_seconds)
    req = request.Request(url=url, method="GET")
    try:
        with request.urlopen(req, timeout=timeout_seconds) as resp:
            charset = resp.headers.get_content_charset("utf-8")
            raw = resp.read().decode(charset)
            log_fn(
                "HTTP GET response received",
                status=getattr(resp, "status", None),
                reason=getattr(resp, "reason", None),
                headers=dict(resp.headers.items()),
                raw_body=raw,
            )
            parsed = json.loads(raw)
            log_fn("HTTP GET response parsed", parsed=parsed)
            return parsed
    except error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        log_fn("HTTP GET failed with HTTPError", status=exc.code, reason=exc.reason, body=detail)
        raise RuntimeError(f"Trading API HTTP {exc.code}: {detail}") from exc
    except error.URLError as exc:
        log_fn("HTTP GET failed with URLError", reason=str(exc.reason), type=str(type(exc.reason)))
        raise RuntimeError(f"Trading API unreachable at {url}: {exc.reason}") from exc
    except socket.timeout as exc:
        log_fn("HTTP GET failed with socket.timeout", timeout_seconds=timeout_seconds)
        raise RuntimeError(f"Trading API timed out after {timeout_seconds} seconds.") from exc
    except Exception as exc:
        log_fn("HTTP GET failed with unexpected error", error=str(exc), traceback=traceback.format_exc())
        raise
