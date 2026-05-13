# astrbot_plugin_group_context_flow

将群聊消息持久化为连续 flow，并在 AstrBot 调用 LLM 前按 conversation 增量注入。

## 行为

- 只处理群聊消息。
- 插件记录群内消息到 `data/plugin_data/astrbot_plugin_group_context_flow/`。
- 触发 LLM 时，插件只把当前触发消息之前、尚未注入当前 `conversation.cid` 的群聊增量追加到 `req.contexts`。
- 当前触发 AI 的消息不放进增量上下文，仍保留为 AstrBot 本轮 user prompt；响应成功后 cursor 会推进到当前触发消息，避免下一轮重复出现。
- LLM 响应后推进 cursor，避免下一轮重复注入。
- 如果平台把 LLM 回复回流成机器人自己的群消息，插件会无条件跳过；`record_self_messages` 只影响非 LLM 流程产生的机器人自身平台消息。
- 内置 `/reset` 或 `/new` 成功后，插件会把当前 conversation 的增量边界推进到指令消息本身；原始群聊 flow 不会被清空，但新/重置后的对话不会回灌旧消息。
- AstrBot 后续会把注入后的 messages 保存到 `conversation.history`，并继续使用自身的上下文截断或 LLM 压缩策略。

## Request Message 形态

```json
[
  {
    "role": "system",
    "content": "# Persona Instructions\n..."
  },
  {
    "role": "user",
    "content": "<group_messages_delta>\n[Bob/20:05:02]: 我不吃辣\n---\n[Carol/20:05:20]: 那寿司更稳\n</group_messages_delta>"
  },
  {
    "role": "user",
    "content": "@Bot 那就定寿司吗？"
  }
]
```

## 建议配置

建议关闭 AstrBot 内置的群聊上下文感知/聊天记忆增强，避免同一段群聊历史被重复注入。

如果担心首次启用时已有群聊日志过长，可以设置 `max_delta_messages`。默认 `0` 表示不限制，由 AstrBot 的上下文上限处理逻辑接管。

## 指令

- `/gflow_status`：查看当前群聊 flow 记录数、最新 seq 和当前 conversation cursor。
- `/gflow_clear`：管理员清空当前群聊的插件原始 flow 日志和 cursor。
