"""AstrBot plugin that persists group chat context as an incremental flow."""

from __future__ import annotations

import asyncio
import time
from datetime import datetime
from pathlib import Path
from sys import maxsize
from typing import Any

from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.provider import LLMResponse, ProviderRequest
from astrbot.api.star import Context, Star
from astrbot.core.platform.message_type import MessageType
from astrbot.core.utils.astrbot_path import get_astrbot_data_path

from .store import GroupFlowStore


PLUGIN_NAME = "astrbot_plugin_group_context_flow"

CONFIG_PATHS = {
    "enabled": ("flow_settings", "enabled"),
    "max_log_records": ("flow_settings", "max_log_records"),
    "max_delta_messages": ("flow_settings", "max_delta_messages"),
    "record_self_messages": ("flow_settings", "record_self_messages"),
    "record_empty_messages": ("flow_settings", "record_empty_messages"),
    "warn_builtin_ltm": ("flow_settings", "warn_builtin_ltm"),
    "debug_log": ("debug_settings", "debug_log"),
}

CONFIG_DEFAULTS = {
    "enabled": True,
    "max_log_records": 5000,
    "max_delta_messages": 0,
    "record_self_messages": False,
    "record_empty_messages": True,
    "warn_builtin_ltm": True,
    "debug_log": False,
}


def _clean_one_line(value: Any) -> str:
    text = "" if value is None else str(value)
    return " ".join(text.replace("\r", " ").replace("\n", " ").split())


class GroupContextFlowPlugin(Star):
    def __init__(self, context: Context, config: dict | None = None):
        super().__init__(context)
        self.context = context
        self.config = config or {}
        self._locks: dict[str, asyncio.Lock] = {}
        self._llm_self_message_cache: dict[str, list[dict[str, Any]]] = {}
        self.store = self._build_store()

    def _cfg(self, key: str, default: Any = None) -> Any:
        path = CONFIG_PATHS.get(key)
        if path:
            section = self.config.get(path[0], {})
            if isinstance(section, dict) and path[1] in section:
                return section[path[1]]
        return self.config.get(key, CONFIG_DEFAULTS.get(key, default))

    def _build_store(self) -> GroupFlowStore:
        data_dir = Path(get_astrbot_data_path()) / "plugin_data" / PLUGIN_NAME
        return GroupFlowStore(
            data_dir,
            max_log_records=int(self._cfg("max_log_records", 5000) or 0),
        )

    async def initialize(self):
        self.store = self._build_store()
        logger.info(f"[{PLUGIN_NAME}] 群聊上下文 Flow 插件已加载，数据目录: {self.store.base_dir}")

    def _lock_for(self, flow_id: str) -> asyncio.Lock:
        lock = self._locks.get(flow_id)
        if lock is None:
            lock = asyncio.Lock()
            self._locks[flow_id] = lock
        return lock

    def _flow_id(self, event: AstrMessageEvent) -> str:
        return f"{event.get_platform_id()}:group:{event.get_group_id()}"

    def _message_id(self, event: AstrMessageEvent) -> str:
        message_id = getattr(event.message_obj, "message_id", "")
        if message_id:
            return str(message_id)
        return f"{event.get_sender_id()}:{getattr(event.message_obj, 'timestamp', int(time.time()))}:{event.get_message_outline()}"

    def _group_name(self, event: AstrMessageEvent) -> str:
        group = getattr(event.message_obj, "group", None)
        name = getattr(group, "group_name", "") if group else ""
        return str(name or "")

    def _message_text(self, event: AstrMessageEvent) -> str:
        outline = event.get_message_outline()
        if outline and outline.strip():
            return outline.strip()
        message = event.get_message_str()
        return str(message or "").strip()

    def _message_fingerprint(self, value: Any) -> str:
        return _clean_one_line(value)

    def _remember_llm_self_message(self, event: AstrMessageEvent, resp: LLMResponse) -> None:
        if event.get_message_type() != MessageType.GROUP_MESSAGE or not event.get_group_id():
            return
        if not resp or resp.role != "assistant":
            return
        text = self._message_fingerprint(resp.completion_text)
        if not text:
            return
        flow_id = self._flow_id(event)
        expires_at = time.time() + 180
        cached_texts = {text}
        chain = getattr(resp, "result_chain", None)
        if chain and getattr(chain, "chain", None):
            for comp in chain.chain:
                comp_text = self._message_fingerprint(getattr(comp, "text", ""))
                if comp_text:
                    cached_texts.add(comp_text)
        cache = self._llm_self_message_cache.setdefault(flow_id, [])
        cache[:] = [item for item in cache if float(item.get("expires_at", 0)) > time.time()]
        for cached_text in cached_texts:
            cache.append({"text": cached_text, "expires_at": expires_at})

    def _is_recent_llm_self_message(self, flow_id: str, text: str) -> bool:
        normalized = self._message_fingerprint(text)
        if not normalized:
            return False
        now = time.time()
        cache = self._llm_self_message_cache.get(flow_id, [])
        cache[:] = [item for item in cache if float(item.get("expires_at", 0)) > now]
        for item in cache:
            cached_text = self._message_fingerprint(item.get("text", ""))
            if not cached_text:
                continue
            if normalized == cached_text:
                return True
            if len(normalized) >= 10 and normalized in cached_text:
                return True
            if len(cached_text) >= 10 and cached_text in normalized:
                return True
        return False

    def _record_from_event(self, event: AstrMessageEvent) -> dict[str, Any]:
        timestamp = int(getattr(event.message_obj, "timestamp", 0) or time.time())
        return {
            "seq": 0,
            "message_id": self._message_id(event),
            "platform_id": event.get_platform_id(),
            "platform_name": event.get_platform_name(),
            "flow_id": self._flow_id(event),
            "group_id": event.get_group_id(),
            "group_name": self._group_name(event),
            "sender_id": event.get_sender_id(),
            "sender_name": event.get_sender_name(),
            "self_id": event.get_self_id(),
            "timestamp": timestamp,
            "text": self._message_text(event),
        }

    async def _ensure_current_record(self, event: AstrMessageEvent) -> int | None:
        if event.get_message_type() != MessageType.GROUP_MESSAGE or not event.get_group_id():
            return None
        if not bool(self._cfg("enabled", True)):
            return None

        record = self._record_from_event(event)
        flow_id = record["flow_id"]
        is_self_message = (
            bool(record["sender_id"])
            and bool(record["self_id"])
            and record["sender_id"] == record["self_id"]
        )
        if is_self_message and self._is_recent_llm_self_message(flow_id, record["text"]):
            if bool(self._cfg("debug_log", False)):
                logger.debug(
                    f"[{PLUGIN_NAME}] skipped llm self message flow_id={flow_id} message_id={record['message_id']}"
                )
            return None
        if (
            not bool(self._cfg("record_self_messages", False))
            and is_self_message
        ):
            return None
        if not bool(self._cfg("record_empty_messages", True)) and not record["text"]:
            return None

        async with self._lock_for(flow_id):
            seq = self.store.append_record(flow_id, record)
        event.set_extra("_group_context_flow_seq", seq)
        event.set_extra("_group_context_flow_message_id", record["message_id"])
        if bool(self._cfg("debug_log", False)):
            logger.debug(
                f"[{PLUGIN_NAME}] recorded flow_id={flow_id} seq={seq} message_id={record['message_id']}"
            )
        return seq

    def _format_time(self, timestamp: int) -> str:
        try:
            return datetime.fromtimestamp(timestamp).strftime("%H:%M:%S")
        except (OSError, OverflowError, ValueError):
            return ""

    def _format_delta(self, records: list[dict[str, Any]]) -> str:
        lines = []
        for record in records:
            sender_name = _clean_one_line(record.get("sender_name")) or _clean_one_line(
                record.get("sender_id")
            )
            timestamp = self._format_time(int(record.get("timestamp") or 0))
            text = _clean_one_line(record.get("text")) or "[消息]"
            lines.append(
                f"[{sender_name}/{timestamp}]: {text}"
                if timestamp
                else f"[{sender_name}]: {text}"
            )
        return "\n---\n".join(lines)

    def _context_texts(self, contexts: list[dict]) -> list[str]:
        texts: list[str] = []
        for item in contexts:
            if not isinstance(item, dict):
                continue
            content = item.get("content", "")
            if isinstance(content, str):
                texts.append(self._message_fingerprint(content))
            elif isinstance(content, list):
                parts = []
                for part in content:
                    if isinstance(part, dict) and part.get("type") == "text":
                        parts.append(str(part.get("text", "")))
                if parts:
                    texts.append(self._message_fingerprint(" ".join(parts)))
        return [text for text in texts if text]

    def _record_already_in_context(
        self, record: dict[str, Any], context_texts: list[str]
    ) -> bool:
        text = self._message_fingerprint(record.get("text", ""))
        if not text:
            return False
        for context_text in context_texts:
            if text == context_text:
                return True
            if len(text) >= 10 and text in context_text:
                return True
            if len(context_text) >= 10 and context_text in text:
                return True
        return False

    def _builtin_ltm_enabled(self, event: AstrMessageEvent) -> bool:
        try:
            cfg = self.context.get_config(umo=event.unified_msg_origin)
            settings = cfg.get("provider_ltm_settings", {})
            return bool(settings.get("group_icl_enable", False))
        except Exception:
            return False

    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE, priority=maxsize - 20)
    async def record_group_message(self, event: AstrMessageEvent):
        """记录群聊消息到插件持久化 flow。"""
        await self._ensure_current_record(event)

    @filter.on_llm_request(priority=maxsize - 20)
    async def inject_group_flow_delta(self, event: AstrMessageEvent, req: ProviderRequest):
        """在本轮触发消息之前追加持久化群聊增量。"""
        if not bool(self._cfg("enabled", True)):
            return
        if event.get_message_type() != MessageType.GROUP_MESSAGE or not event.get_group_id():
            return
        if not req.conversation:
            return
        if event.get_extra("_group_context_flow_injected", False):
            return

        if bool(self._cfg("warn_builtin_ltm", True)) and self._builtin_ltm_enabled(event):
            logger.warning(
                f"[{PLUGIN_NAME}] 检测到内置群聊上下文感知已启用，建议关闭以避免重复注入。"
            )

        current_seq = event.get_extra("_group_context_flow_seq")
        if not isinstance(current_seq, int):
            current_seq = await self._ensure_current_record(event)
        if not current_seq:
            return

        flow_id = self._flow_id(event)
        conversation_id = req.conversation.cid
        async with self._lock_for(flow_id):
            cursor = self.store.get_cursor(flow_id, conversation_id)
            inject_until_seq = max(0, current_seq - 1)
            cursor_target_seq = current_seq
            records = self.store.get_range(flow_id, cursor + 1, inject_until_seq)

        context_texts = self._context_texts(req.contexts)
        records = [
            record
            for record in records
            if not self._record_already_in_context(record, context_texts)
        ]

        max_delta = int(self._cfg("max_delta_messages", 0) or 0)
        skipped_count = 0
        if max_delta > 0 and len(records) > max_delta:
            skipped_count = len(records) - max_delta
            records = records[-max_delta:]

        event.set_extra(
            "_group_context_flow_pending_cursor",
            {
                "flow_id": flow_id,
                "conversation_id": conversation_id,
                "target_seq": cursor_target_seq,
                "injected_count": len(records),
            },
        )
        event.set_extra("_group_context_flow_injected", True)
        if records:
            req.contexts.append({"role": "user", "content": self._format_delta(records)})
        if bool(self._cfg("debug_log", False)):
            skipped_text = f" skipped={skipped_count}" if skipped_count else ""
            logger.debug(
                f"[{PLUGIN_NAME}] injected flow_id={flow_id} conversation={conversation_id} "
                f"cursor={cursor} inject_until={inject_until_seq} cursor_target={cursor_target_seq} "
                f"count={len(records)}{skipped_text}"
            )

    @filter.on_llm_response(priority=-maxsize + 20)
    async def update_flow_cursor(self, event: AstrMessageEvent, resp: LLMResponse):
        """LLM 有响应后推进 cursor，避免下一轮重复注入同一段群聊历史。"""
        self._remember_llm_self_message(event, resp)
        pending = event.get_extra("_group_context_flow_pending_cursor")
        if not isinstance(pending, dict) or not resp:
            return
        flow_id = str(pending.get("flow_id") or "")
        conversation_id = str(pending.get("conversation_id") or "")
        if not flow_id or not conversation_id:
            return

        target_seq = int(pending.get("target_seq") or 0)
        async with self._lock_for(flow_id):
            self.store.set_cursor(
                flow_id,
                conversation_id,
                target_seq,
                unified_msg_origin=event.unified_msg_origin,
            )
        if bool(self._cfg("debug_log", False)):
            logger.debug(
                f"[{PLUGIN_NAME}] cursor updated flow_id={flow_id} conversation={conversation_id} target={target_seq}"
            )

    @filter.command("gflow_status")
    async def group_flow_status(self, event: AstrMessageEvent):
        """查看当前群聊 flow 状态。"""
        if event.get_message_type() != MessageType.GROUP_MESSAGE or not event.get_group_id():
            yield event.plain_result("gflow_status 仅支持群聊。")
            return

        curr_cid = await self.context.conversation_manager.get_curr_conversation_id(
            event.unified_msg_origin
        )
        flow_id = self._flow_id(event)
        async with self._lock_for(flow_id):
            stats = self.store.stats(flow_id, curr_cid)

        yield event.plain_result(
            "群聊上下文 Flow 状态：\n"
            f"flow_id: {flow_id}\n"
            f"conversation_id: {curr_cid or 'N/A'}\n"
            f"records: {stats['records']}\n"
            f"latest_seq: {stats['latest_seq']}\n"
            f"cursor: {stats['cursor']}"
        )

    @filter.permission_type(filter.PermissionType.ADMIN)
    @filter.command("gflow_clear")
    async def group_flow_clear(self, event: AstrMessageEvent):
        """清空当前群聊的插件原始 flow 日志和 cursor。"""
        if event.get_message_type() != MessageType.GROUP_MESSAGE or not event.get_group_id():
            yield event.plain_result("gflow_clear 仅支持群聊。")
            return

        flow_id = self._flow_id(event)
        async with self._lock_for(flow_id):
            self.store.clear_flow(flow_id)
        yield event.plain_result("已清空当前群聊的 group context flow 插件数据。")
