#!/usr/bin/env python3
"""
看板任务更新工具 - 供各团队 Agent 调用

用法:
  # 新建任务（任务分派时）
  python3 kanban_update.py create TASK-20260223-012 "任务标题" Planning 产品策略负责人 产品策略负责人

  # 更新状态
  python3 kanban_update.py state TASK-20260223-012 QualityReview "方案已提交质量与风控负责人"

  # 添加流转记录
  python3 kanban_update.py flow TASK-20260223-012 "产品策略负责人" "质量与风控负责人" "规划方案提交审核"

  # 完成任务
  python3 kanban_update.py done TASK-20260223-012 "/path/to/output" "任务完成摘要"

  # 添加/更新子任务 todo
  python3 kanban_update.py todo TASK-20260223-012 1 "实现API接口" in-progress
  python3 kanban_update.py todo TASK-20260223-012 1 "" completed

  # 🔥 实时进展汇报（Agent 主动调用，频率不限）
  python3 kanban_update.py progress TASK-20260223-012 "正在分析需求，拟定3个子方案" "1.调研技术选型|2.撰写设计文档|3.实现原型"
"""
import datetime
import json
import logging
import os
import pathlib
import re
import sys
from copy import deepcopy

if __package__ in {None, ""}:
    repo_root = pathlib.Path(__file__).resolve().parents[1]
    repo_root_str = str(repo_root)
    if repo_root_str not in sys.path:
        sys.path.insert(0, repo_root_str)


def _resolve_workspace_root():
    cwd = pathlib.Path.cwd().resolve()
    if cwd.name.startswith("workspace-"):
        return cwd
    script_path = pathlib.Path(__file__).resolve()
    if script_path.parent.name == "scripts" and script_path.parent.parent.name.startswith("workspace-"):
        return script_path.parent.parent
    return script_path.parent.parent


WORKSPACE_ROOT = _resolve_workspace_root()
OPENCLAW_DIR = WORKSPACE_ROOT.parent if WORKSPACE_ROOT.name.startswith("workspace-") else WORKSPACE_ROOT
WORKSPACE_ID = WORKSPACE_ROOT.name[len("workspace-"):] if WORKSPACE_ROOT.name.startswith("workspace-") else WORKSPACE_ROOT.name
DASHBOARD_DIR = OPENCLAW_DIR / 'dashboard'
DASHBOARD_DIRTY_STAMP = DASHBOARD_DIR / '.dashboard-dirty.stamp'

log = logging.getLogger('kanban')
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(name)s] %(message)s', datefmt='%H:%M:%S')

from backend.adapters.storage.dashboard import list_task_records, replace_task_records_for_workspace  # noqa: E402
from backend.domain.core.kanban import bundled_kanban_defaults  # noqa: E402

# ── 从 kanban_config.json 加载主题配置；缺失时回退到内置现代企业默认值 ──
_KANBAN_CONFIG_FILE = WORKSPACE_ROOT / 'data' / 'kanban_config.json'

def _load_kanban_config():
    """Load theme-specific kanban config. Falls back to bundled defaults."""
    if _KANBAN_CONFIG_FILE.exists():
        try:
            with open(_KANBAN_CONFIG_FILE) as f:
                return json.load(f)
        except Exception:
            pass
    return None

_kanban_cfg = _load_kanban_config() or {}
_DEFAULT_KANBAN_CFG = bundled_kanban_defaults()

_DEFAULT_STATE_ORG_MAP = _DEFAULT_KANBAN_CFG['state_org_map']
_DEFAULT_STATE_AGENT_MAP = _DEFAULT_KANBAN_CFG['state_agent_map']
_DEFAULT_ORG_AGENT_MAP = _DEFAULT_KANBAN_CFG['org_agent_map']
_DEFAULT_AGENT_LABELS = _DEFAULT_KANBAN_CFG['agent_labels']

STATE_ORG_MAP = _kanban_cfg.get('state_org_map') or _DEFAULT_STATE_ORG_MAP
_STATE_AGENT_MAP = _kanban_cfg.get('state_agent_map') or _DEFAULT_STATE_AGENT_MAP
_ORG_AGENT_MAP = _kanban_cfg.get('org_agent_map') or _DEFAULT_ORG_AGENT_MAP
_AGENT_LABELS = _kanban_cfg.get('agent_labels') or _DEFAULT_AGENT_LABELS
_OWNER_TITLE = _kanban_cfg.get('owner_title') or _DEFAULT_KANBAN_CFG['owner_title']
_TASK_PREFIX = _kanban_cfg.get('task_prefix') or _DEFAULT_KANBAN_CFG['task_prefix']

# Localized labels — heuristic: if owner_title is ASCII → English, else Chinese
if _kanban_cfg and all(ord(c) < 128 for c in _OWNER_TITLE):
    # English
    L_DONE = 'Task completed'
    L_ORDER_ISSUED = 'Assigned, awaiting {org}'
    L_REJECT_CREATE = 'Rejected creating {task_id}: {reason}'
    L_KANBAN_REJECT = '[Kanban] Rejected: {reason}'
    L_TASK_CLOSED = 'Task {task_id} already closed (state={state}), cannot overwrite'
    L_KANBAN_CLOSED = '[Kanban] Task {task_id} already closed (state={state}), cannot overwrite'
    L_TASK_EXISTS = 'Task {task_id} already exists (state={state}), will be overwritten'
    L_TASK_NOT_FOUND = 'Task {task_id} not found'
    L_ARG_ERROR = 'Error: "{cmd}" requires at least {min} arguments, got {actual}'
    L_TITLE_TOO_SHORT = 'Title too short ({length}<{min_len} chars)'
    L_TITLE_JUNK = 'Title "{t}" is not a valid task'
    L_TITLE_PUNCT_ONLY = 'Title contains only punctuation'
    L_TITLE_LOOKS_PATH = 'Title looks like a file path, please summarize the task'
    L_TITLE_EMPTY = 'Title is empty after sanitization'
else:
    # Chinese
    L_DONE = '任务已完成'
    L_ORDER_ISSUED = '已派发，等待{org}处理'
    L_REJECT_CREATE = '⚠️ 拒绝创建 {task_id}：{reason}'
    L_KANBAN_REJECT = '[看板] 拒绝创建：{reason}'
    L_TASK_CLOSED = '⚠️ 任务 {task_id} 已完结 (state={state})，不可覆盖'
    L_KANBAN_CLOSED = '[看板] 任务 {task_id} 已完结 (state={state})，不可覆盖'
    L_TASK_EXISTS = '任务 {task_id} 已存在 (state={state})，将被覆盖'
    L_TASK_NOT_FOUND = '任务 {task_id} 不存在'
    L_ARG_ERROR = '错误："{cmd}" 命令至少需要 {min} 个参数，实际 {actual} 个'
    L_TITLE_TOO_SHORT = '标题过短（{length}<{min_len}字），疑似不是有效任务'
    L_TITLE_JUNK = '标题 "{t}" 不是有效任务'
    L_TITLE_PUNCT_ONLY = '标题只有标点符号'
    L_TITLE_LOOKS_PATH = '标题看起来像文件路径，请用中文概括任务'
    L_TITLE_EMPTY = '标题清洗后为空'

MAX_PROGRESS_LOG = 100  # 单任务最大进展日志条数

def load():
    return _normalize_tasks(
        list_task_records(
            OPENCLAW_DIR,
            workspace_id=WORKSPACE_ID,
            workspace_path=str(WORKSPACE_ROOT),
        )
    )

def mark_dashboard_dirty():
    try:
        DASHBOARD_DIR.mkdir(parents=True, exist_ok=True)
        DASHBOARD_DIRTY_STAMP.write_text(now_iso(), encoding='utf-8')
    except OSError:
        pass

def save(tasks):
    replace_task_records_for_workspace(
        OPENCLAW_DIR,
        WORKSPACE_ID,
        str(WORKSPACE_ROOT),
        _normalize_tasks(tasks),
    )
    # 标记 dashboard 脏状态，由主服务在真正需要时按需重建。
    mark_dashboard_dirty()


def _update_tasks(modifier):
    current_tasks = load()
    updated = modifier(deepcopy(current_tasks))
    task_list = updated if isinstance(updated, list) else (updated.get('tasks', []) if isinstance(updated, dict) else [])
    save(task_list)
    return task_list

def now_iso():
    return datetime.datetime.now(datetime.timezone.utc).isoformat().replace('+00:00', 'Z')

def find_task(tasks, task_id):
    return next((t for t in tasks if t.get('id') == task_id), None)


def _normalize_task_fields(task):
    """兼容旧脚本：任务级同时维护 state/status，进展字段同时维护 now/currentUpdate、block/blockers。"""
    if not isinstance(task, dict):
        return task
    state = task.get('state')
    status = task.get('status')
    if state and not status:
        task['status'] = state
    elif status and not state:
        task['state'] = status

    now_text = task.get('now')
    current_update = task.get('currentUpdate')
    if now_text and not current_update:
        task['currentUpdate'] = now_text
    elif current_update and not now_text:
        task['now'] = current_update

    block = task.get('block')
    blockers = task.get('blockers')
    if block and not blockers:
        task['blockers'] = block
    elif blockers and not block:
        task['block'] = blockers
    return task


def _normalize_tasks(tasks):
    return [_normalize_task_fields(t) for t in tasks]


def next_task_id(prefix=None, date_str=None):
    if prefix is None:
        prefix = _TASK_PREFIX
    """返回当天第一个未被占用的任务号。"""
    if not date_str:
        date_str = datetime.datetime.now().strftime('%Y%m%d')
    tasks = load()
    used = {
        t.get('id') for t in tasks
        if isinstance(t, dict) and isinstance(t.get('id'), str) and t.get('id').startswith(f'{prefix}-{date_str}-')
    }
    for n in range(1, 10000):
        task_id = f'{prefix}-{date_str}-{n:03d}'
        if task_id not in used:
            return task_id
    raise RuntimeError(f'当天任务号已耗尽: {prefix}-{date_str}-NNNN')


# 任务标题最低要求
_MIN_TITLE_LEN = 6
_JUNK_TITLES = {
    '?', '？', '好', '好的', '是', '否', '不', '不是', '对', '了解', '收到',
    '嗯', '哦', '知道了', '开启了么', '可以', '不行', '行', 'ok', 'yes', 'no',
    '你去开启', '测试', '试试', '看看',
}

def _sanitize_text(raw, max_len=80):
    """清洗文本：剥离文件路径、URL、Conversation 元数据、常见分派前缀、截断过长内容。"""
    t = (raw or '').strip()
    # 1) 剥离 Conversation info / Conversation 后面的所有内容
    t = re.split(r'\n*Conversation\b', t, maxsplit=1)[0].strip()
    # 2) 剥离 ```json 代码块
    t = re.split(r'\n*```', t, maxsplit=1)[0].strip()
    # 3) 剥离 Unix/Mac 文件路径 (/Users/xxx, /home/xxx, /opt/xxx, ./xxx)
    t = re.sub(r'[/\\.~][A-Za-z0-9_\-./]+(?:\.(?:py|js|ts|json|md|sh|yaml|yml|txt|csv|html|css|log))?', '', t)
    # 4) 剥离 URL
    t = re.sub(r'https?://\S+', '', t)
    # 5) 清理常见前缀: "任务分派:" "指令:" "派单:" 等
    t = re.sub(r'^(任务分派|任务派发|派单|指令|指派)([（(][^)）]*[)）])?[：:\uff1a]\s*', '', t)
    # 6) 剥离系统元数据关键词
    t = re.sub(r'(message_id|session_id|chat_id|open_id|user_id|tenant_key)\s*[:=]\s*\S+', '', t)
    # 7) 合并多余空白
    t = re.sub(r'\s+', ' ', t).strip()
    # 8) 截断过长内容
    if len(t) > max_len:
        t = t[:max_len] + '…'
    return t


def _sanitize_title(raw):
    """清洗标题（最长 80 字符）。"""
    return _sanitize_text(raw, 80)


def _sanitize_remark(raw):
    """清洗流转备注（最长 120 字符）。"""
    return _sanitize_text(raw, 120)


def _infer_agent_id_from_runtime(task=None):
    """尽量推断当前执行该命令的 Agent。"""
    for k in ('OPENCLAW_AGENT_ID', 'OPENCLAW_AGENT', 'AGENT_ID'):
        v = (os.environ.get(k) or '').strip()
        if v:
            return v

    cwd = str(pathlib.Path.cwd())
    m = re.search(r'workspace-([a-zA-Z0-9_\-]+)', cwd)
    if m:
        return m.group(1)

    fpath = str(pathlib.Path(__file__).resolve())
    m2 = re.search(r'workspace-([a-zA-Z0-9_\-]+)', fpath)
    if m2:
        return m2.group(1)

    if task:
        state = task.get('state', '')
        org = task.get('org', '')
        aid = _STATE_AGENT_MAP.get(state)
        if aid is None and state in ('Doing', 'Next'):
            aid = _ORG_AGENT_MAP.get(org)
        if aid:
            return aid
    return ''


def _is_valid_task_title(title):
    """校验标题是否足够作为一个有效任务。"""
    t = (title or '').strip()
    if len(t) < _MIN_TITLE_LEN:
        return False, f'标题过短（{len(t)}<{_MIN_TITLE_LEN}字），疑似不是有效任务'
    if t.lower() in _JUNK_TITLES:
        return False, f'标题 "{t}" 不是有效任务'
    # 纯标点或问号
    if re.fullmatch(r'[\s?？!！.。,，…·\-—~]+', t):
        return False, '标题只有标点符号'
    # 看起来像文件路径
    if re.match(r'^[/\\~.]', t) or re.search(r'/[a-zA-Z0-9_-]+/[a-zA-Z0-9_-]+', t):
        return False, f'标题看起来像文件路径，请用中文概括任务'
    # 只剩标点和空白（清洗后可能变空）
    if re.fullmatch(r'[\s\W]*', t):
        return False, '标题清洗后为空'
    return True, ''


def cmd_create(task_id, title, state, org, official, remark=None):
    """新建任务（任务分派时立即调用）"""
    # 清洗标题（剥离元数据）
    title = _sanitize_title(title)
    # 任务标题校验
    valid, reason = _is_valid_task_title(title)
    if not valid:
        log.warning(f'⚠️ 拒绝创建 {task_id}：{reason}')
        print(f'[看板] 拒绝创建：{reason}', flush=True)
        return 1
    actual_org = STATE_ORG_MAP.get(state, org)
    clean_remark = _sanitize_remark(remark) if remark else f"{_OWNER_TITLE}: {title}"
    create_conflict = {'state': None}
    created = {'ok': False}

    def modifier(tasks):
        existing = next((t for t in tasks if t.get('id') == task_id), None)
        if existing:
            if existing.get('state') in ('Done', 'Cancelled'):
                log.warning(f'⚠️ 任务 {task_id} 已完结 (state={existing["state"]})，不可覆盖')
                create_conflict['state'] = existing['state']
                return tasks
            if existing.get('state') not in (None, '', 'Inbox', 'Pending'):
                log.warning(f'任务 {task_id} 已存在 (state={existing["state"]})，将被覆盖')
        tasks = [t for t in tasks if t.get('id') != task_id]
        tasks.insert(0, {
            "id": task_id, "title": title, "official": official,
            "org": actual_org, "state": state, "status": state,
            "now": clean_remark[:60] if remark else L_ORDER_ISSUED.format(org=actual_org),
            "currentUpdate": clean_remark[:60] if remark else L_ORDER_ISSUED.format(org=actual_org),
            "eta": "-", "block": "无", "blockers": "无", "output": "", "ac": "",
            "flow_log": [{"at": now_iso(), "from": _OWNER_TITLE, "to": actual_org, "remark": clean_remark}],
            "updatedAt": now_iso()
        })
        created['ok'] = True
        return tasks

    _update_tasks(modifier)
    if not created['ok']:
        state_name = create_conflict['state'] or 'unknown'
        print(f'[看板] 任务 {task_id} 已完结 (state={state_name})，不可覆盖', flush=True)
        return 2
    log.info(f'✅ 创建 {task_id} | {title[:30]} | state={state}')
    return 0


def cmd_next_id(prefix=None, date_str=None):
    if prefix is None:
        prefix = _TASK_PREFIX
    task_id = next_task_id(prefix=prefix, date_str=date_str)
    print(task_id, flush=True)
    return 0


def cmd_state(task_id, new_state, now_text=None):
    """更新任务状态（原子操作）"""
    old_state = [None]
    found = [False]
    def modifier(tasks):
        t = find_task(tasks, task_id)
        if not t:
            log.error(f'任务 {task_id} 不存在')
            return tasks
        found[0] = True
        old_state[0] = t['state']
        t['state'] = new_state
        t['status'] = new_state
        # 仅对明确的流转节点更新负责部门；Doing/Done/Blocked 需保留真实执行方
        if new_state in _STATE_AGENT_MAP and new_state in STATE_ORG_MAP:
            t['org'] = STATE_ORG_MAP[new_state]
        if now_text:
            t['now'] = now_text
            t['currentUpdate'] = now_text
        t['updatedAt'] = now_iso()
        return tasks
    _update_tasks(modifier)
    if not found[0]:
        print(L_TASK_NOT_FOUND.format(task_id=task_id), flush=True)
        return 1
    log.info(f'✅ {task_id} 状态更新: {old_state[0]} → {new_state}')
    return 0


def cmd_flow(task_id, from_dept, to_dept, remark):
    """添加流转记录（原子操作）"""
    clean_remark = _sanitize_remark(remark)
    found = [False]
    def modifier(tasks):
        t = find_task(tasks, task_id)
        if not t:
            log.error(f'任务 {task_id} 不存在')
            return tasks
        found[0] = True
        t.setdefault('flow_log', []).append({
            "at": now_iso(), "from": from_dept, "to": to_dept, "remark": clean_remark
        })
        t['updatedAt'] = now_iso()
        return tasks
    _update_tasks(modifier)
    if not found[0]:
        print(L_TASK_NOT_FOUND.format(task_id=task_id), flush=True)
        return 1
    log.info(f'✅ {task_id} 流转记录: {from_dept} → {to_dept}')
    return 0


def cmd_done(task_id, output_path='', summary=''):
    """标记任务完成（原子操作）"""
    found = [False]
    def modifier(tasks):
        t = find_task(tasks, task_id)
        if not t:
            log.error(f'任务 {task_id} 不存在')
            return tasks
        found[0] = True
        t['state'] = 'Done'
        t['status'] = 'Done'
        t['output'] = output_path
        t['now'] = summary or L_DONE
        t['currentUpdate'] = summary or L_DONE
        t.setdefault('flow_log', []).append({
            "at": now_iso(), "from": t.get('org', '执行部门'),
            "to": _OWNER_TITLE, "remark": f"✅ {L_DONE}: {summary or L_DONE}"
        })
        t['updatedAt'] = now_iso()
        return tasks
    _update_tasks(modifier)
    if not found[0]:
        print(L_TASK_NOT_FOUND.format(task_id=task_id), flush=True)
        return 1
    log.info(f'✅ {task_id} 已完成')
    return 0


def cmd_block(task_id, reason):
    """标记阻塞（原子操作）"""
    found = [False]
    def modifier(tasks):
        t = find_task(tasks, task_id)
        if not t:
            log.error(f'任务 {task_id} 不存在')
            return tasks
        found[0] = True
        t['state'] = 'Blocked'
        t['status'] = 'Blocked'
        t['block'] = reason
        t['blockers'] = reason
        t['updatedAt'] = now_iso()
        return tasks
    _update_tasks(modifier)
    if not found[0]:
        print(L_TASK_NOT_FOUND.format(task_id=task_id), flush=True)
        return 1
    log.warning(f'⚠️ {task_id} 已阻塞: {reason}')
    return 0


def cmd_progress(task_id, now_text, todos_pipe='', tokens=0, cost=0.0, elapsed=0):
    """🔥 实时进展汇报 — Agent 主动调用，不改变状态，只更新 now + todos

    now_text: 当前正在做什么的一句话描述（必填）
    todos_pipe: 可选，用 | 分隔的 todo 列表，格式：
        "已完成的事项✅|正在做的事项🔄|计划做的事项"
        - 以 ✅ 结尾 → completed
        - 以 🔄 结尾 → in-progress
        - 其他 → not-started
    tokens: 可选，本次消耗的 token 数
    cost: 可选，本次成本（美元）
    elapsed: 可选，本次耗时（秒）
    """
    clean = _sanitize_remark(now_text)
    # 解析 todos_pipe
    parsed_todos = None
    if todos_pipe:
        new_todos = []
        for i, item in enumerate(todos_pipe.split('|'), 1):
            item = item.strip()
            if not item:
                continue
            if item.endswith('✅'):
                status = 'completed'
                title = item[:-1].strip()
            elif item.endswith('🔄'):
                status = 'in-progress'
                title = item[:-1].strip()
            else:
                status = 'not-started'
                title = item
            new_todos.append({'id': str(i), 'title': title, 'status': status})
        if new_todos:
            parsed_todos = new_todos

    # 解析资源消耗参数
    try:
        tokens = int(tokens) if tokens else 0
    except (ValueError, TypeError):
        tokens = 0
    try:
        cost = float(cost) if cost else 0.0
    except (ValueError, TypeError):
        cost = 0.0
    try:
        elapsed = int(elapsed) if elapsed else 0
    except (ValueError, TypeError):
        elapsed = 0

    done_cnt = [0]
    total_cnt = [0]
    found = [False]
    def modifier(tasks):
        t = find_task(tasks, task_id)
        if not t:
            log.error(f'任务 {task_id} 不存在')
            return tasks
        found[0] = True
        t['now'] = clean
        t['currentUpdate'] = clean
        if parsed_todos is not None:
            t['todos'] = parsed_todos
        # 多 Agent 并行进展日志
        at = now_iso()
        agent_id = _infer_agent_id_from_runtime(t)
        agent_label = _AGENT_LABELS.get(agent_id, agent_id)
        log_todos = parsed_todos if parsed_todos is not None else t.get('todos', [])
        log_entry = {
            'at': at, 'agent': agent_id, 'agentLabel': agent_label,
            'text': clean, 'todos': log_todos,
            'state': t.get('state', ''), 'org': t.get('org', ''),
        }
        # 资源消耗（可选字段，有值才写入）
        if tokens > 0:
            log_entry['tokens'] = tokens
        if cost > 0:
            log_entry['cost'] = cost
        if elapsed > 0:
            log_entry['elapsed'] = elapsed
        t.setdefault('progress_log', []).append(log_entry)
        # 限制 progress_log 大小，防止无限增长
        if len(t['progress_log']) > MAX_PROGRESS_LOG:
            t['progress_log'] = t['progress_log'][-MAX_PROGRESS_LOG:]
        t['updatedAt'] = at
        done_cnt[0] = sum(1 for td in t.get('todos', []) if td.get('status') == 'completed')
        total_cnt[0] = len(t.get('todos', []))
        return tasks
    _update_tasks(modifier)
    if not found[0]:
        print(L_TASK_NOT_FOUND.format(task_id=task_id), flush=True)
        return 1
    res_info = ''
    if tokens or cost or elapsed:
        res_info = f' [res: {tokens}tok/${cost:.4f}/{elapsed}s]'
    log.info(f'📡 {task_id} 进展: {clean[:40]}... [{done_cnt[0]}/{total_cnt[0]}]{res_info}')
    return 0

def cmd_todo(task_id, todo_id, title, status='not-started', detail=''):
    """添加或更新子任务 todo（原子操作）

    status: not-started / in-progress / completed
    detail: 可选，该子任务的详细产出/说明（Markdown 格式）
    """
    # 校验 status 值
    if status not in ('not-started', 'in-progress', 'completed'):
        status = 'not-started'
    result_info = [0, 0]
    found = [False]
    def modifier(tasks):
        t = find_task(tasks, task_id)
        if not t:
            log.error(f'任务 {task_id} 不存在')
            return tasks
        found[0] = True
        if 'todos' not in t:
            t['todos'] = []
        existing = next((td for td in t['todos'] if str(td.get('id')) == str(todo_id)), None)
        if existing:
            existing['status'] = status
            if title:
                existing['title'] = title
            if detail:
                existing['detail'] = detail
        else:
            item = {'id': todo_id, 'title': title, 'status': status}
            if detail:
                item['detail'] = detail
            t['todos'].append(item)
        t['updatedAt'] = now_iso()
        result_info[0] = sum(1 for td in t['todos'] if td.get('status') == 'completed')
        result_info[1] = len(t['todos'])
        return tasks
    _update_tasks(modifier)
    if not found[0]:
        print(L_TASK_NOT_FOUND.format(task_id=task_id), flush=True)
        return 1
    log.info(f'✅ {task_id} todo [{result_info[0]}/{result_info[1]}]: {todo_id} → {status}')
    return 0

_CMD_MIN_ARGS = {
    'create': 6, 'state': 3, 'flow': 5, 'done': 2, 'block': 3, 'todo': 4, 'progress': 3, 'next-id': 1,
}

if __name__ == '__main__':
    args = sys.argv[1:]
    if not args:
        print(__doc__)
        sys.exit(0)
    cmd = args[0]
    if cmd in _CMD_MIN_ARGS and len(args) < _CMD_MIN_ARGS[cmd]:
        print(L_ARG_ERROR.format(cmd=cmd, min=_CMD_MIN_ARGS[cmd], actual=len(args)))
        print(__doc__)
        sys.exit(1)
    if cmd == 'create':
        sys.exit(cmd_create(args[1], args[2], args[3], args[4], args[5], args[6] if len(args)>6 else None))
    elif cmd == 'next-id':
        sys.exit(cmd_next_id(args[1] if len(args) > 1 else None, args[2] if len(args) > 2 else None))
    elif cmd == 'state':
        sys.exit(cmd_state(args[1], args[2], args[3] if len(args)>3 else None))
    elif cmd == 'flow':
        sys.exit(cmd_flow(args[1], args[2], args[3], args[4]))
    elif cmd == 'done':
        sys.exit(cmd_done(args[1], args[2] if len(args)>2 else '', args[3] if len(args)>3 else ''))
    elif cmd == 'block':
        sys.exit(cmd_block(args[1], args[2]))
    elif cmd == 'todo':
        # 解析可选 --detail 参数
        todo_pos = []
        todo_detail = ''
        ti = 1
        while ti < len(args):
            if args[ti] == '--detail' and ti + 1 < len(args):
                todo_detail = args[ti + 1]; ti += 2
            else:
                todo_pos.append(args[ti]); ti += 1
        sys.exit(cmd_todo(
            todo_pos[0] if len(todo_pos) > 0 else '',
            todo_pos[1] if len(todo_pos) > 1 else '',
            todo_pos[2] if len(todo_pos) > 2 else '',
            todo_pos[3] if len(todo_pos) > 3 else 'not-started',
            detail=todo_detail,
        ))
    elif cmd == 'progress':
        # 解析可选 --tokens/--cost/--elapsed 参数
        pos_args = []
        kw = {}
        i = 1
        while i < len(args):
            if args[i] == '--tokens' and i + 1 < len(args):
                kw['tokens'] = args[i + 1]; i += 2
            elif args[i] == '--cost' and i + 1 < len(args):
                kw['cost'] = args[i + 1]; i += 2
            elif args[i] == '--elapsed' and i + 1 < len(args):
                kw['elapsed'] = args[i + 1]; i += 2
            else:
                pos_args.append(args[i]); i += 1
        sys.exit(cmd_progress(
            pos_args[0] if len(pos_args) > 0 else '',
            pos_args[1] if len(pos_args) > 1 else '',
            pos_args[2] if len(pos_args) > 2 else '',
            tokens=kw.get('tokens', 0),
            cost=kw.get('cost', 0.0),
            elapsed=kw.get('elapsed', 0),
        ))
    else:
        print(__doc__)
        sys.exit(1)
