"""Runtime part: late-bound constants and caches."""

PAYLOAD_CACHE = {}

PAYLOAD_CACHE_MAX_SIZE = 200  # evict oldest entry when exceeded

BACKGROUND_CACHE_KEYS = set()

CACHE_BUILD_EVENTS = {}

BUNDLE_CACHE_LOCK = threading.RLock()

BUNDLE_WARM_WORKERS = defaultdict(set)

BUNDLE_WARM_WORKERS_LOCK = threading.Lock()

FORCE_SYNC_DASHBOARD_REFRESH = False

CHAT_THREAD_DISPATCH_MODES = {"direct", "mentions", "broadcast"}

TEAM_WAKE_SCOPES = {"lead", "all"}

CHAT_BROADCAST_INTENT_TOKENS = {
    "all hands",
    "team sync",
    "whole team",
    "entire team",
    "everyone",
    "everybody",
    "all reply",
    "broadcast",
    "全员",
    "大家",
    "各位",
    "所有人",
    "整个团队",
    "全组",
    "团队同步",
    "一起同步",
    "一起对齐",
    "一起处理",
    "一起跟进",
    "一起看看",
    "统一对齐",
    "广播",
    "其他同事",
    "其他员工",
    "其他人",
    "找同事",
    "拉同事",
    "拉人",
    "协作一下",
    "一起协作",
}

CHAT_SKILL_TRIGGER_HINTS = {
    "web-content-fetcher": (
        "http://",
        "https://",
        "url",
        "链接",
        "网页",
        "网页正文",
        "文章",
        "公众号",
        "微信",
        "wechat",
        "weixin",
        "mp.weixin.qq.com",
        "抓取",
        "读取网页",
        "读取文章",
    ),
    "browse": (
        "打开网页",
        "打开链接",
        "浏览器",
        "访问",
        "网页",
        "站点",
        "网站",
    ),
    "playwright": (
        "浏览器",
        "点击",
        "自动化",
        "网页测试",
        "表单",
        "页面操作",
    ),
    "openai-docs": (
        "openai",
        "responses api",
        "chat completions",
        "gpt-5",
        "codex",
        "openai docs",
    ),
}

TEAM_COLLABORATION_PROFILE_DEFAULTS = {
    "core": {
        "humanToneGuide": "像真实产品和项目负责人沟通一样说话，先给判断，再补背景、风险和下一步，不说空话。",
        "leadChecklist": [
            "先用一句话确认是否接单，以及你准备怎么推进。",
            "把任务拆成 1-3 个可执行子项，明确谁负责什么、何时回报。",
            "涉及跨团队或高风险时，先给判断和建议，不要只抛问题。",
        ],
        "memberChecklist": [
            "先说你负责哪一块，再说你马上做什么、预计多久回第一个结果。",
            "发现依赖别人时，直接说明需要谁、需要什么、为什么现在要。",
            "如果暂时不用介入，也说明你后续会在什么条件下接手。",
        ],
        "proactiveRules": [
            "范围、优先级或负责人不清楚时，先收口结论再继续推进。",
            "发现跨团队依赖时，主动点名需要谁支持和你需要什么。",
            "每次回复至少带上下一步、时间点或决策建议，不只汇报现象。",
        ],
        "updateContract": "像真人同事汇报：先结论，再动作，再时间点和风险或需要支持。",
        "escalationRule": "遇到阻塞超过一次往返，直接在 Team 线程说清卡点、影响、需要谁以及希望何时响应。",
    },
    "delivery": {
        "humanToneGuide": "像真实研发同事协作一样说话，短句、自然、直接，先说进展再说卡点。",
        "leadChecklist": [
            "先确认你牵头，再给出今天的执行拆分和第一步。",
            "把实施、验证、依赖分别分给合适成员，不让大家等指令。",
            "任何阻塞都要马上点名相关人，并给出替代方案或临时绕法。",
        ],
        "memberChecklist": [
            "先说你负责哪一块，再说你马上要做什么。",
            "发现依赖别人时，直接在 Team 线程说明需要谁、要什么、最晚何时要到。",
            "如果暂时不用介入，也说明触发你接手的条件，不要只回一句 standby。",
        ],
        "proactiveRules": [
            "看到风险先报结论，再说影响和你建议的处理。",
            "如果 30 分钟内没有推进，不要沉默，主动同步卡点和下一步。",
            "有把握能顺手解决的问题就直接补位，不要只转述。",
        ],
        "updateContract": "先说现在做到哪，再说下一步，再说依赖或风险。",
        "escalationRule": "被依赖人没有响应或方案卡住时，直接在 Team 线程点名并说明影响面。",
    },
    "release": {
        "humanToneGuide": "像真实 QA / 发布负责人沟通一样，先结论，再证据，再说明放行条件。",
        "leadChecklist": [
            "先说这条链路当前能不能放行，再给出验证计划。",
            "把验证、风险、上线动作拆清楚，别让实施和验收混在一起。",
            "不满足发布条件时，明确 block 原因和解锁条件。",
        ],
        "memberChecklist": [
            "先说你的验证结论，再列关键证据或缺口。",
            "发现风险就立即回线程，不要等全部验证完再一次性汇报。",
            "需要补环境、补日志或补产物时，直接说明谁来补、补什么。",
        ],
        "proactiveRules": [
            "看到 release 风险先阻断，再给修复建议。",
            "任何结论都尽量带验证证据、环境前提或回滚条件。",
            "如果问题已定位，就主动给出下一步验收方式。",
        ],
        "updateContract": "先结论，再证据，再说是否放行或还差什么。",
        "escalationRule": "遇到会影响放行的风险，立刻在 Team 线程同步并标明是否需要 block。",
    },
    "signals": {
        "humanToneGuide": "像真实市场、运营和客户沟通同事一样，先说对象和结论，再说对外口径与下一步。",
        "leadChecklist": [
            "先说这次沟通面对谁、目标是什么，再分配文档或口径产出。",
            "把 brief、release note、对外说明分清楚，不要混成一段空话。",
            "发现需要产品或交付补事实时，主动追问并说明用途。",
        ],
        "memberChecklist": [
            "先说你负责的文档或对外动作，再说何时给首版。",
            "如果信息不够，对着缺口直接问，不要先写空泛版本。",
            "需要别人补素材时，明确你要哪一段信息和使用场景。",
        ],
        "proactiveRules": [
            "对外口径有歧义时，先把候选版本列出来再求确认。",
            "文档缺事实就主动追问，不要用模板话凑满。",
            "每次同步都带上受众、用途和下一步。",
        ],
        "updateContract": "先说给谁看、核心结论是什么、接下来补什么。",
        "escalationRule": "对外口径存在风险或事实不清时，立即回线程确认，不带着猜测发布。",
    },
    "fallback": {
        "humanToneGuide": "像真实同事协作一样说话，先结论，再动作，再风险或需要支持。",
        "leadChecklist": [
            "先确认谁牵头，再明确今天先做什么。",
            "把任务拆成清楚的小步，不让成员等模糊指令。",
            "遇到卡点马上说清影响和需要谁支持。",
        ],
        "memberChecklist": [
            "先说你负责哪一块，再说你马上开始什么。",
            "如果需要别人配合，直接说明对象、依赖和时点。",
            "暂时不用介入时，也说清后续触发条件。",
        ],
        "proactiveRules": [
            "不要只汇报现象，尽量给出判断和下一步。",
            "看到依赖或风险先同步，不要等别人来问。",
            "能顺手补位解决的问题就主动接住。",
        ],
        "updateContract": "先结论，再动作，再风险或需要支持。",
        "escalationRule": "任务被卡住时，直接在 Team 线程说明卡点、影响和需要谁。",
    },
}

AUTO_OPERATION_COMPANY_TASK_TOKENS = (
    "经营公司",
    "经营这家公司",
    "运营公司",
    "公司经营",
    "公司运营",
    "经营团队",
    "经营节奏",
    "经营目标",
    "增长",
    "营收",
    "获客",
    "留存",
    "复购",
    "转化",
    "经营复盘",
    "business",
    "operate company",
    "run company",
    "company operation",
    "company ops",
    "growth",
    "revenue",
)

CONVERSATION_SOURCE_LABELS = {
    "main": "主会话",
    "telegram": "Telegram",
    "qqbot": "QQ Bot",
    "feishu": "飞书",
    "whatsapp": "WhatsApp",
    "discord": "Discord",
    "slack": "Slack",
    "cron": "定时任务",
    "subagent": "子代理",
}

READ_ONLY_CONVERSATION_SOURCES = {"cron", "subagent"}

MODEL_PROVIDER_CATALOG = [
    {"id": "openai", "label": "OpenAI", "prefixes": ("gpt", "o1", "o3", "o4", "omni", "text-embedding"), "env": ("OPENAI_API_KEY",)},
    {"id": "anthropic", "label": "Anthropic", "prefixes": ("claude",), "env": ("ANTHROPIC_API_KEY",)},
    {"id": "google", "label": "Google", "prefixes": ("gemini",), "env": ("GOOGLE_API_KEY", "GEMINI_API_KEY")},
    {"id": "gemma", "label": "Gemma 4", "prefixes": ("gemma", "google/gemma"), "env": ("GOOGLE_API_KEY", "GEMINI_API_KEY", "HF_TOKEN", "HUGGING_FACE_HUB_TOKEN", "HUGGINGFACE_TOKEN")},
    {"id": "deepseek", "label": "DeepSeek", "prefixes": ("deepseek",), "env": ("DEEPSEEK_API_KEY",)},
    {"id": "qwen", "label": "Qwen", "prefixes": ("qwen",), "env": ("QWEN_API_KEY", "DASHSCOPE_API_KEY")},
    {"id": "zhipu", "label": "Zhipu GLM", "prefixes": ("zai/", "glm", "zhipu", "bigmodel"), "env": ("BIGMODEL_API_KEY", "ZHIPUAI_API_KEY", "ZAI_API_KEY")},
    {"id": "openrouter", "label": "OpenRouter", "prefixes": ("openrouter/",), "env": ("OPENROUTER_API_KEY",)},
    {"id": "xai", "label": "xAI", "prefixes": ("grok",), "env": ("XAI_API_KEY",)},
]

MODEL_PROVIDER_CAPABILITY_CATALOG = {
    "gemma": {
        "summary": "Gemma 4 fits edge copilots and multi-agent local orchestration with long context and native tool contracts.",
        "bestFit": "Edge copilots, structured workflows, multimodal assistants",
        "contextWindow": "128K edge / 256K large",
        "multimodalInputs": ["image", "video", "audio"],
        "starterModels": ["gemma-4-e2b", "gemma-4-e4b", "google/gemma-4-e2b", "google/gemma-4-e4b"],
        "localRuntimeProfiles": [
            {
                "id": "gemma-4-e2b-edge",
                "label": "Gemma 4 E2B Edge",
                "description": "Lightweight edge preset for local agent workflows and function-calling routes.",
                "backend": "llama_cpp",
                "entrypoint": "",
                "modelPath": "gemma-4-e2b-it-q4_k_m.gguf",
                "projectorPath": "",
                "contextLength": 131072,
                "gpuLayers": 0,
                "cacheTypeK": "f16",
                "cacheTypeV": "f16",
                "kvCacheEnabled": False,
                "extraArgs": [],
                "notes": "Run a host-side OpenAI-compatible Gemma service and connect OpenClaw Team to it.",
            },
            {
                "id": "gemma-4-e4b-edge",
                "label": "Gemma 4 E4B Edge",
                "description": "Higher-capacity edge preset for longer structured workflows and richer multimodal routing.",
                "backend": "llama_cpp",
                "entrypoint": "",
                "modelPath": "gemma-4-e4b-it-q4_k_m.gguf",
                "projectorPath": "",
                "contextLength": 65536,
                "gpuLayers": 0,
                "cacheTypeK": "f16",
                "cacheTypeV": "f16",
                "kvCacheEnabled": False,
                "extraArgs": [],
                "notes": "Run a host-side OpenAI-compatible Gemma service and connect OpenClaw Team to it.",
            },
        ],
        "capabilityTags": [
            "Agentic",
            "Function calling",
            "Structured JSON",
            "128K / 256K",
            "Image / video",
            "Audio input",
            "Edge",
        ],
        "supports": {
            "agenticWorkflows": True,
            "functionCalling": True,
            "structuredJson": True,
            "longContext": True,
            "multimodalImage": True,
            "multimodalVideo": True,
            "audioInput": True,
            "edgeOptimized": True,
        },
    },
    "zhipu": {
        "summary": "Zhipu GLM fits Chinese-first hosted routing where you want the whole team to move onto one ready API model quickly.",
        "bestFit": "Chinese collaboration, hosted fallback, quick all-team rollout",
        "contextWindow": "",
        "multimodalInputs": [],
        "starterModels": ["glm-5-turbo"],
        "localRuntimeProfiles": [],
        "capabilityTags": [
            "Chinese-first",
            "Hosted API",
            "GLM-5",
        ],
        "supports": {
            "agenticWorkflows": True,
            "functionCalling": True,
            "structuredJson": True,
            "longContext": True,
        },
    },
}

STALE_TASK_BLOCK_SUMMARIES = {
    "已将任务标记为阻塞。",
    "任务已标记为阻塞。",
}

TASK_INTELLIGENCE_PROFILES = [
    {
        "id": "incident",
        "label": "故障响应",
        "keywords": ("incident", "outage", "sev", "故障", "告警", "异常", "宕机", "恢复", "热修"),
        "workflowTemplate": "incident",
        "laneHints": ("triage", "recovery", "ops", "intake"),
        "risk": "high",
    },
    {
        "id": "release",
        "label": "发布交付",
        "keywords": ("release", "deploy", "ship", "上线", "发布", "交付", "灰度", "staging"),
        "workflowTemplate": "delivery",
        "laneHints": ("ops", "quality", "build"),
        "risk": "watch",
    },
    {
        "id": "quality",
        "label": "验证验收",
        "keywords": ("qa", "test", "verify", "validation", "验收", "验证", "测试", "回归", "审议"),
        "workflowTemplate": "delivery",
        "laneHints": ("quality", "verify", "review"),
        "risk": "watch",
    },
    {
        "id": "engineering",
        "label": "研发实现",
        "keywords": ("build", "feature", "api", "code", "开发", "实现", "接口", "重构", "修复", "工程"),
        "workflowTemplate": "delivery",
        "laneHints": ("build", "engineering", "execute"),
        "risk": "watch",
    },
    {
        "id": "growth",
        "label": "增长实验",
        "keywords": ("growth", "experiment", "campaign", "转化", "增长", "实验", "活动", "投放"),
        "workflowTemplate": "growth",
        "laneHints": ("experiment", "analysis", "scale"),
        "risk": "watch",
    },
    {
        "id": "research",
        "label": "分析调研",
        "keywords": ("research", "analyze", "analysis", "调研", "分析", "研究", "方案", "评估"),
        "workflowTemplate": "growth",
        "laneHints": ("signal", "analysis", "plan", "intake"),
        "risk": "good",
    },
    {
        "id": "communication",
        "label": "沟通协调",
        "keywords": ("message", "reply", "route", "sync", "沟通", "协调", "回复", "分拣", "转派", "会话"),
        "workflowTemplate": "delivery",
        "laneHints": ("intake", "triage"),
        "risk": "good",
    },
]

HIGH_RISK_KEYWORDS = ("security", "payment", "billing", "legal", "compliance", "生产事故", "权限", "安全", "财务", "客户投诉")

MANUAL_REVIEW_KEYWORDS = ("approval", "审批", "合同", "退款", "赔付", "security", "legal", "finance", "compliance")

ROUTING_AGENT_TOKEN_GROUPS = {
    "qa": {"qa", "quality", "review", "verify", "validation", "验收", "审核", "测试"},
    "vp_compliance": {"compliance", "risk", "audit", "合规", "风控", "审计"},
    "engineering": {"engineering", "build", "开发", "工程", "交付"},
    "devops": {"devops", "ops", "deploy", "运维", "上线"},
    "data_team": {"data", "dataset", "analytics", "数据", "报表"},
    "marketing": {"growth", "campaign", "marketing", "增长", "投放", "营销"},
    "coo": {"coo", "operations", "执行", "运营"},
    "vp_strategy": {"strategy", "planner", "规划", "策略", "research", "analysis", "调研", "分析"},
    "assistant": {"assistant", "router", "协调", "路由", "intake", "triage"},
}

TASK_CATEGORY_AGENT_PRIORITY = {
    "incident": ("devops", "engineering", "qa", "coo"),
    "release": ("devops", "qa", "engineering", "coo"),
    "quality": ("qa", "engineering", "devops"),
    "engineering": ("engineering", "devops", "qa"),
    "growth": ("marketing", "data_team", "engineering"),
    "research": ("vp_strategy", "data_team", "marketing"),
    "communication": ("assistant", "coo", "vp_strategy"),
}

TASK_CATEGORY_TEAM_PRIORITY = {
    "incident": ("team-delivery", "team-release", "team-core"),
    "release": ("team-release", "team-core", "team-delivery"),
    "quality": ("team-release", "team-delivery", "team-core"),
    "engineering": ("team-delivery", "team-release", "team-core"),
    "growth": ("team-signals", "team-core"),
    "research": ("team-signals", "team-core"),
    "communication": ("team-core", "team-signals"),
}

TEAM_SPECIALISM_TOKEN_GROUPS = {
    "team-core": {
        "tokens": ("strategy", "priority", "decision", "review", "risk", "协调", "决策", "优先级", "评审", "风控"),
        "reason": "任务描述命中协调、评审或决策类关键词。",
    },
    "team-delivery": {
        "tokens": ("build", "implement", "integration", "repair", "fix", "engineering", "开发", "实现", "修复", "联调", "构建"),
        "reason": "任务描述命中 build、实现或修复类关键词。",
    },
    "team-release": {
        "tokens": ("release", "ship", "deploy", "rollback", "qa", "verify", "验收", "发布", "上线", "回滚", "验证", "测试"),
        "reason": "任务描述命中 QA、发布或验收类关键词。",
    },
    "team-signals": {
        "tokens": ("brief", "briefing", "docs", "readme", "announce", "signal", "campaign", "文档", "口径", "简报", "公告"),
        "reason": "任务描述命中文档、brief 或对外同步类关键词。",
    },
    "team-company": {
        "tokens": (
            "all hands",
            "all-hands",
            "company-wide",
            "company wide",
            "cross-team",
            "cross team",
            "全员",
            "公司级",
            "跨团队",
            "总动员",
            "全公司",
            "全体",
        ),
        "reason": "任务描述命中公司级全员协同或跨团队总动员类关键词。",
    },
}

ALL_HANDS_TASK_TOKENS = (
    "all hands",
    "all-hands",
    "allhands",
    "company-wide",
    "company wide",
    "companywide",
    "cross-team",
    "cross team",
    "cross-functional",
    "cross functional",
    "every agent",
    "all agents",
    "full team",
    "全员",
    "公司级",
    "跨团队",
    "跨组",
    "总动员",
    "全公司",
    "全体",
    "统一广播",
    "全组同步",
)

LANE_HINT_AGENT_PRIORITY = {
    "build": ("engineering",),
    "engineering": ("engineering",),
    "execute": ("engineering",),
    "quality": ("qa",),
    "verify": ("qa",),
    "review": ("qa",),
    "ops": ("devops",),
    "deploy": ("devops",),
    "release": ("devops",),
    "recovery": ("devops",),
    "analysis": ("data_team", "vp_strategy"),
    "data": ("data_team",),
    "signal": ("marketing",),
    "plan": ("vp_strategy",),
    "strategy": ("vp_strategy",),
    "intake": ("assistant", "coo"),
    "triage": ("assistant", "coo"),
}

TEAM_SELECTION_STOPWORDS = {
    "",
    "a",
    "an",
    "and",
    "for",
    "from",
    "into",
    "need",
    "with",
    "任务",
    "处理",
    "推进",
    "安排",
    "需要",
    "当前",
    "进行",
    "through",
    "validation",
}

AGENT_PROGRESS_FRESHNESS_WINDOW = timedelta(minutes=20)

AGENT_STALE_PROGRESS_ESCALATION_WINDOW = timedelta(minutes=40)

TEAM_OWNERSHIP_ROLE_ORDER = ("command", "execution", "gate", "signals")

CONVERSATION_PRESSURE_KEYWORDS = (
    "blocked",
    "blocking",
    "urgent",
    "stuck",
    "failed",
    "failure",
    "issue",
    "error",
    "retry",
    "timeout",
    "阻塞",
    "卡住",
    "失败",
    "异常",
    "超时",
    "紧急",
    "告警",
)

DELIVERY_FAILURE_REASON_LABELS = {
    "config": "配置错误",
    "auth": "鉴权失败",
    "rate_limit": "触达限流",
    "transport": "网络异常",
    "channel_disabled": "通道停用",
    "unknown": "未知原因",
}

WECHAT_OFFICIAL_REPLY_FALLBACK = "已收到，我先接住，稍后会继续在这里回复你。"

WECHAT_CALLBACK_ROUTE = re.compile(r"^/api/customer-access/wechat/([a-zA-Z0-9_-]+)/callback/?$")

WECHAT_ACCESS_TOKEN_REFRESH_BUFFER_SECONDS = 120

WECHAT_ACCESS_TOKEN_CACHE = {}

WECHAT_VOICE_REPLY_DEFAULT_VOICE = "serena"

WECHAT_VOICE_REPLY_SUPPORTED_VOICES = {
    "serena",
    "uncle_fu",
    "vivian",
    "dylan",
    "eric",
    "sohee",
    "ono_anna",
    "aiden",
    "ryan",
}

WECHAT_VOICE_REPLY_MAX_CHARS = 320

WECHAT_VOICE_REPLY_TIMEOUT_SECONDS = 45

WECHAT_VOICE_REPLY_MAX_TEXT_SEND_CHARS = 1200
