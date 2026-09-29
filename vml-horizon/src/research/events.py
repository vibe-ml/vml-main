"""Translate Deep Agent stream chunks into UI events."""

from typing import Any

from deepagents.backends.utils import file_data_to_string

REPORT_PATH = "/final_report.md"
_TEXT_LIMIT = 2000


def message_text(message: Any) -> str:
    """Read plain text from a LangChain message or a dict payload."""
    if isinstance(message, dict):
        content = message.get("content")
    else:
        content = getattr(message, "content", "")
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for block in content:
            if isinstance(block, str):
                parts.append(block)
            elif isinstance(block, dict):
                parts.append(str(block.get("text") or ""))
        return "\n".join(part for part in parts if part)
    return str(content or "")


def _tool_text(name: str, args: Any) -> str:
    """Describe a tool call without dumping raw argument objects."""
    if isinstance(args, dict):
        if name == "task":
            target = args.get("subagent_type") or "a scout"
            return f"Delegate to {target}"
        query = args.get("query")
        if isinstance(query, str) and query.strip():
            return f"Search: {query.strip()}"
    return _clip(str(args or name or "tool"))


def _clip(text: str) -> str:
    cleaned = " ".join(text.split())
    if len(cleaned) <= _TEXT_LIMIT:
        return cleaned
    return cleaned[:_TEXT_LIMIT] + "…"


def read_report(files: Any) -> str | None:
    """Return the finished report when this update wrote it."""
    if not isinstance(files, dict):
        return None
    raw = files.get(REPORT_PATH)
    if raw is None:
        return None
    if isinstance(raw, str):
        return raw
    if isinstance(raw, dict) and "content" in raw:
        return file_data_to_string(raw)
    return None


def _tool_calls(message: Any) -> list[Any]:
    if isinstance(message, dict):
        return list(message.get("tool_calls") or [])
    return list(getattr(message, "tool_calls", None) or [])


def _message_kind(message: Any) -> str:
    if isinstance(message, dict):
        return str(message.get("type") or message.get("role") or "")
    return str(getattr(message, "type", "") or "")


def events_from_message(scope: str, message: Any) -> list[dict[str, str]]:
    """Describe one model or tool message for the activity log."""
    kind = _message_kind(message)
    events: list[dict[str, str]] = []
    if kind in {"ai", "assistant"}:
        for call in _tool_calls(message):
            name = call.get("name") if isinstance(call, dict) else getattr(call, "name", "")
            args = call.get("args") if isinstance(call, dict) else getattr(call, "args", {})
            events.append(
                {
                    "kind": "tool",
                    "scope": scope,
                    "name": str(name or "tool"),
                    "text": _tool_text(str(name or ""), args),
                }
            )
        text = message_text(message).strip()
        if text:
            events.append({"kind": "note", "scope": scope, "name": "", "text": _clip(text)})
        return events
    if kind in {"tool", "tool_result"}:
        name = message.get("name") if isinstance(message, dict) else getattr(message, "name", "")
        events.append(
            {
                "kind": "result",
                "scope": scope,
                "name": str(name or "tool"),
                "text": _clip(message_text(message)),
            }
        )
    return events


def _scope_name(namespace: Any) -> str:
    if isinstance(namespace, (list, tuple)) and namespace:
        return str(namespace[0]).split(":")[0] or "coordinator"
    return "coordinator"


def _updates(chunk: Any) -> list[tuple[str, dict[str, Any]]]:
    if isinstance(chunk, dict) and chunk.get("type") == "updates":
        return [(_scope_name(chunk.get("ns")), chunk.get("data") or {})]
    if isinstance(chunk, tuple) and len(chunk) == 2 and isinstance(chunk[1], dict):
        return [(_scope_name(chunk[0]), chunk[1])]
    if isinstance(chunk, dict) and "type" not in chunk:
        return [("coordinator", chunk)]
    return []


def events_from_chunk(chunk: Any) -> list[dict[str, str]]:
    """Flatten one stream chunk into activity and report events."""
    events: list[dict[str, str]] = []
    for scope, data in _updates(chunk):
        if not isinstance(data, dict):
            continue
        for update in data.values():
            if not isinstance(update, dict):
                continue
            for message in update.get("messages") or []:
                events.extend(events_from_message(scope, message))
            report = read_report(update.get("files"))
            if report:
                events.append({"kind": "report", "scope": scope, "name": "", "text": report})
    return events
