# OpenClaw Team

## 一句话介绍

OpenClaw Team 是一个开源的 Agent 运营平台，帮你把多个 AI Agent 组成一支团队，统一管理它们的任务分配、工作流程和运行状态。

## 解决什么问题

你可能已经有了一些 AI Agent，但遇到这些问题：

- Agent 各跑各的，没有统一管理的地方
- 任务分配靠手动，不知道该给谁
- 出了问题不知道，等发现已经晚了
- 做完的事情没记录，下次又从头来

OpenClaw Team 就是为了解决这些问题。

## 核心功能

**任务调度** — 创建任务后自动匹配合适的 Agent，不用手动分配。

**工作流编排** — 多个 Agent 需要配合时，画个流程图就行，支持审批节点和执行回放。

**运行监控** — 每个 Agent 的状态、负载、异常，一个页面全看到。异常自动提醒。

**即时沟通** — 随时和任何 Agent 对话，支持线程、@提及和文件共享。

**3D 办公室** — 用 3D 视图看整个团队的工作状态，谁在忙、谁空闲、谁需要帮助。

**技能市场** — Agent 的能力可以按技能包管理，装上就能用，随时回滚。

## 技术特点

- **本地部署** — 数据在你自己的服务器上，不经过第三方
- **50+ 模型** — 支持 OpenAI、Anthropic、GLM、DeepSeek 等主流模型
- **Docker 一键启动** — `docker compose up -d`，5 分钟跑起来
- **完整 API** — 37 个 REST 端点，方便与现有系统集成
- **多租户** — 支持多团队、多权限，适合企业使用

## 快速体验

```bash
git clone https://github.com/imgolye/openclaw-team.git
cd openclaw-team
cp .env.example .env
docker compose up -d
# 打开 http://localhost:18890
```

## 适合谁

- 正在用多个 AI Agent 的技术团队
- 需要给 Agent 团队建立运营规范的负责人
- 希望把 AI Agent 从实验工具变成日常生产力的团队

## 链接

- GitHub: https://github.com/imgolye/openclaw-team
- 许可证: AGPL-3.0（商业使用请联系授权）
