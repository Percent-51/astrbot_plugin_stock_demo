# -*- coding: utf-8 -*-
"""股市插件冒烟回归（不需要 AstrBot 环境）。

验的是「拷过去能不能跑」：
  S1 结构：AI / 庄家密语 / 大赦 已彻底消失，无 menu 桥残留
  S2 数据源适配器：SOURCES 覆盖全部联动源键、权重和 1.0、低频键入档
  S3 货币接口：get_balance / add_coins 自洽，接自己的账本只需换两个函数
  S4 定价：20 个 tick 不炸、不归零、不越涨跌停
  S5 交易：买入扣钱建仓 / 卖出加钱清仓 / 手续费进金库
  S6 分红：金库按 XBK 持股发钱；赤字反向共担
  S7 退市：持仓 5 折退款 + 停牌 + 重置 10
  S8 展示：持仓 / 排行 / 排行今日 / 规则文本
  S9 路由：密语与大赦在精简版里彻底失联
"""
import asyncio
import importlib.util
import pathlib
import sys
import tempfile
import types

for name in ("astrbot", "astrbot.api", "astrbot.api.event", "astrbot.api.star",
             "astrbot.core", "astrbot.core.message", "astrbot.core.message.components",
             "astrbot.core.message.message_event_result"):
    sys.modules.setdefault(name, types.ModuleType(name))
sys.modules["astrbot.api"].logger = types.SimpleNamespace(
    info=lambda *a, **k: None, warning=lambda *a, **k: None, debug=lambda *a, **k: None)
sys.modules["astrbot.api.event"].AstrMessageEvent = object
sys.modules["astrbot.api.event"].filter = types.SimpleNamespace(
    command=lambda *a, **k: (lambda f: f),
    command_group=lambda *a, **k: (lambda f: f),
    event_message_type=lambda *a, **k: (lambda f: f),
    EventMessageType=types.SimpleNamespace(GROUP_MESSAGE=1, PRIVATE_MESSAGE=2),
    EventType=types.SimpleNamespace(ALL_MESSAGE="ALL"))
sys.modules["astrbot.api.star"].Context = object


class _Star:
    def __init__(self, context):
        self.context = context


sys.modules["astrbot.api.star"].Star = _Star
_TMPDIR = pathlib.Path(tempfile.mkdtemp(prefix="stockdemo_"))
sys.modules["astrbot.api.star"].StarTools = types.SimpleNamespace(
    get_data_dir=lambda n: _TMPDIR)
sys.modules["astrbot.api.star"].register = lambda *a, **k: (lambda f: f)


class _FakePlain:
    def __init__(self, text=""):
        self.text = text

    def __repr__(self):
        return "Plain(%r)" % self.text


class _FakeImage:
    def __init__(self, file=""):
        self.file = file

    @classmethod
    def fromFileSystem(cls, path):
        return cls(path)


sys.modules["astrbot.core.message.components"].Plain = _FakePlain
sys.modules["astrbot.core.message.components"].Image = _FakeImage
sys.modules["astrbot.core.message.message_event_result"].MessageChain = lambda comps: comps

_HERE = pathlib.Path(__file__).resolve().parent
_CANDS = [
    _HERE.parent / "main.py",                          # 标准仓库结构：tests/ 的上一级
    _HERE / "astrbot_plugin_stock_demo" / "main.py",   # 分发包结构：脚本与插件目录同级
]
_DEFAULT = next((p for p in _CANDS if p.is_file()), _CANDS[0])
PATH = sys.argv[1] if len(sys.argv) > 1 else str(_DEFAULT)
spec = importlib.util.spec_from_file_location("sm", PATH)
sm = importlib.util.module_from_spec(spec)
spec.loader.exec_module(sm)

_fails = []
_n = 0


def check(tag, ok, detail=""):
    global _n
    _n += 1
    print(("PASS " if ok else "FAIL ") + tag + ("  " + detail if detail else ""))
    if not ok:
        _fails.append(tag)


def drain(agen):
    async def _run():
        return [x async for x in agen]
    return asyncio.run(_run())


def texts(msgs):
    return "\n".join(getattr(m, "text", "") for m in msgs)


class _Ev:
    """最小事件替身：能取群号/发送者/昵称/正文，能 stop_event。"""
    def __init__(self, text="", gid=None, uid="10086", name="测试"):
        self._t, self._g, self._u, self._n = text, gid, uid, name
        self.stopped = False
        self.unified_msg_origin = "umo"

    def get_message_str(self):
        return self._t

    def get_group_id(self):
        return self._g

    def get_sender_id(self):
        return self._u

    def get_sender_name(self):
        return self._n

    def plain_result(self, s):
        return _FakePlain(s)

    def image_result(self, s):
        return _FakeImage(s)

    def stop_event(self):
        self.stopped = True


def new_plugin():
    """每个用例一个干净存档（否则上一条用例的钱包/持仓会漏到下一条）。"""
    for _f in _TMPDIR.glob("stock.json*"):
        _f.unlink()
    p = sm.StockDemoPlugin(None)
    p.group_id = "123456"
    return p


# ============================================================
# S1 结构
# ============================================================
print("== S1 结构 ==")
src = open(PATH, encoding="utf-8").read()
for bad in ("AI_ENABLED", "AI_SYSTEM_PROMPT", "AI_DRIFT_MAX", "_ai_loop", "_ai_ask",
            "_ai_snapshot_locked", "_msg_buf", "SECRET_EVENT_WORDS", "OWNER_OPS",
            "_secret_swan", "_secret_regime", "_secret_forcemove", "_owner_buyback",
            "_buyback_execute_locked", "_menu()", "_group_state", "_user_state",
            "_data_lock", "menu._persist", "MENU_PLUGIN_NAME"):
    check("S1 无残留 %s" % bad, bad not in src)
check("S1 类名已中性化", "class StockDemoPlugin(Star)" in src and "XiaobaoStockPlugin" not in src)
check("S1 日志前缀已换", "[XiaobaoStock]" not in src and "[StockDemo]" in src)

# ============================================================
# S2 数据源适配器
# ============================================================
print("== S2 数据源适配器 ==")
allkeys = set()
for _c, _cfg in sm.STOCKS.items():
    for _k, _w in _cfg["sources"]:
        allkeys.add(_k)
check("S2 全部联动源键都有适配器", allkeys <= set(sm.SOURCES),
      "缺 " + str(sorted(allkeys - set(sm.SOURCES))))
check("S2 适配器无多余键", set(sm.SOURCES) <= set(sm.METRIC_LABEL),
      "多 " + str(sorted(set(sm.SOURCES) - set(sm.METRIC_LABEL))))
for _c, _cfg in sm.STOCKS.items():
    s = sum(w for _k, w in _cfg["sources"])
    # 含反向源（负权重）的股，权重和会小于 1.0（BBB 0.70），这是设计如此
    check("S2 %s 权重和 0.6~1.0" % _c, 0.6 <= s <= 1.0 + 1e-9, "%.4f" % s)
low = {k for _c in sm.STOCKS for k, _w in sm.STOCKS[_c]["sources"]
       if k.startswith("game_") or k in ("aff_confess", "aff_steal",
                                         "cc_hit", "cc_born", "cc_preg")}
check("S2 低频键全部入 LOWFREQ_KEYS", low <= sm.LOWFREQ_KEYS,
      "缺 " + str(sorted(low - sm.LOWFREQ_KEYS)))
check("S2 适配器函数可调用", all(callable(f) for f in sm.SOURCES.values()))

p = new_plugin()
m0 = p._sample_metrics_locked()
check("S2 未接源时全 0", all(v == 0.0 for v in m0.values()) and len(m0) == len(sm.METRIC_LABEL),
      "n=%d" % len(m0))

# 注入一个假数据源，验证适配器真的被调用
_old = sm.SOURCES["checkin"]
sm.SOURCES["checkin"] = lambda day, gid: 42.0
m1 = p._sample_metrics_locked()
check("S2 换源立即生效", m1.get("checkin") == 42.0, str(m1.get("checkin")))
sm.SOURCES["checkin"] = lambda day, gid: (_ for _ in ()).throw(RuntimeError("boom"))
m2 = p._sample_metrics_locked()
check("S2 数据源抛错降级为 0 不崩", m2.get("checkin") == 0.0)
sm.SOURCES["checkin"] = _old

# ============================================================
# S3 货币接口
# ============================================================
print("== S3 货币接口 ==")
p = new_plugin()
check("S3 初始余额 0", p.get_balance("u1") == 0)
p.add_coins("u1", "甲", 500)
check("S3 加币生效", p.get_balance("u1") == 500)
p.add_coins("u1", "甲", -200)
check("S3 扣币生效", p.get_balance("u1") == 300)
p.add_coins("u1", "甲", -99999)
check("S3 扣到 0 不为负", p.get_balance("u1") == 0)
check("S3 钱包记名字", (p._wallet().get("u1") or {}).get("name") == "甲")

# ============================================================
# S4 定价
# ============================================================
print("== S4 定价 ==")
p = new_plugin()
p._state["wallet"] = {"u%d" % i: {"name": "u%d" % i, "coins": 1000} for i in range(10)}
sm.SOURCES["checkin"] = lambda day, gid: 50.0
sm.SOURCES["pet"] = lambda day, gid: 30.0
sm.SOURCES["bank_dep"] = lambda day, gid: 5000.0
bad = []
for _ in range(20):
    p._tick_locked()
    for c, cfg in sm.STOCKS.items():
        px = float(p._st(c)["price"])
        if not (sm.PRICE_FLOOR <= px <= 200):
            bad.append((c, px))
check("S4 20 tick 价格全部在合理区间", not bad, str(bad[:3]))
check("S4 价格永不归零", all(float(p._st(c)["price"]) >= sm.PRICE_FLOOR for c in sm.STOCKS))
check("S4 走势历史在累积", len(p._st("XBK").get("hist") or []) >= 2)
check("S4 采样已落盘", bool(p._state.get("samples")))
sm.SOURCES["checkin"] = sm._zero
sm.SOURCES["pet"] = sm._zero
sm.SOURCES["bank_dep"] = sm._zero

# ============================================================
# S5 交易
# ============================================================
print("== S5 交易 ==")
p = new_plugin()
p.add_coins("u1", "甲", 10000)
px0 = float(p._st("XBK")["price"])
out = texts(drain(p._trade(_Ev(), "u1", "甲", "买", "XBK 100")))
check("S5 买入成功", "成交：买入" in out, out[:60])
check("S5 扣钱正确",
      p.get_balance("u1") == 10000 - int(100 * px0 * (1 + sm.FEE) + 0.999),
      str(p.get_balance("u1")))
check("S5 持仓 100 股", int(p._state["holdings"]["u1"]["pos"]["XBK"]["sh"]) == 100)
check("S5 均价=买入价", abs(float(p._state["holdings"]["u1"]["pos"]["XBK"]["avg"]) - px0) < 1e-6)
check("S5 手续费进金库", int(p._state["div_pool"]) == int(100 * px0 * sm.FEE + 0.999),
      str(p._state["div_pool"]))
check("S5 资金面已记账", bool((p._state.get("flow") or {})))

out = texts(drain(p._trade(_Ev(), "u1", "甲", "买", "XBK 2000")))
check("S5 余额不足有提示", "先搬砖" in out, out[:40])

bal_before = p.get_balance("u1")
out = texts(drain(p._trade(_Ev(), "u1", "甲", "卖", "XBK 40")))
check("S5 卖出成功", "成交：卖出" in out, out[:60])
check("S5 卖出加钱", p.get_balance("u1") > bal_before)
check("S5 持仓降到 60", int(p._state["holdings"]["u1"]["pos"]["XBK"]["sh"]) == 60,
      str(p._state["holdings"]["u1"]["pos"]["XBK"]["sh"]))

out = texts(drain(p._trade(_Ev(), "u2", "乙", "卖", "XBK 10")))
check("S5 无持仓有提示", "没有" in out, out[:40])

# ============================================================
# S6 分红
# ============================================================
print("== S6 分红 ==")
p = new_plugin()
p.add_coins("u1", "甲", 0)
p._state["holdings"] = {"u1": {"name": "甲", "pos": {"XBK": {"sh": 100, "avg": 10.0, "today": 0}}}}
p._state["div_pool"] = 1000
lines = p._distribute_dividend_locked("2026-10-20") or []
check("S6 分红有播报", any("分红日" in x for x in lines), str(lines[:1]))
check("S6 每股派 10", any("每股派 10.00" in x for x in lines), str(lines))
check("S6 到手 1000", p.get_balance("u1") == 1000, str(p.get_balance("u1")))
check("S6 金库清零", int(p._state["div_pool"]) == 0, str(p._state["div_pool"]))

p._state["div_pool"] = -600
lines = p._distribute_dividend_locked("2026-10-21") or []
check("S6 亏损日共担", any("亏损日" in x for x in lines), str(lines[:1]))
check("S6 扣到 0 为止", p.get_balance("u1") == 400, str(p.get_balance("u1")))
check("S6 没收满的部分滚负", int(p._state["div_pool"]) == 0, str(p._state["div_pool"]))

# ============================================================
# S7 退市
# ============================================================
print("== S7 退市 ==")
p = new_plugin()
p._state["holdings"] = {"u1": {"name": "甲", "pos": {"XBK": {"sh": 100, "avg": 10.0, "today": 0}}}}
p._st("XBK")["price"] = 4.0
notes = []
p._delist_liquidate_locked("XBK", p._st("XBK"), notes)
check("S7 有退市播报", any("正式退市" in x for x in notes), str(notes[:1]))
check("S7 5 折退款=200", p.get_balance("u1") == 200, str(p.get_balance("u1")))
check("S7 持仓清零", int(p._state["holdings"]["u1"]["pos"]["XBK"]["sh"]) == 0)
check("S7 停牌 2h", float(p._st("XBK")["delist_until"]) > 0)
check("S7 价格重置 10", abs(float(p._st("XBK")["price"]) - sm.INIT_PRICE) < 1e-9)

# ============================================================
# S8 展示
# ============================================================
print("== S8 展示 ==")
p = new_plugin()
p.add_coins("u1", "甲", 5000)
drain(p._trade(_Ev(), "u1", "甲", "买", "XBK 10"))
out = texts(drain(p._portfolio(_Ev(), "u1", "甲")))
check("S8 持仓明细", "持仓明细" in out and "XBK" in out, out[:60])
out = texts(drain(p._leaderboard(_Ev())))
check("S8 总资产排行", "总资产排行" in out and "甲" in out, out[:60])
p._snapshot_assets_locked()
out = texts(drain(p._leaderboard_today(_Ev())))
check("S8 今日收益榜", "收益率" in out or "暂无" in out or "今日" in out, out[:60])
check("S8 规则文本 6 只", all(c in sm.RULES_TEXT for c in sm.STOCKS))
check("S8 规则文本不泄露后门", "黑天鹅" in sm.RULES_TEXT and "挠痒痒" not in sm.RULES_TEXT
      and "大赦" not in sm.RULES_TEXT and "定盘" not in sm.RULES_TEXT)

# ============================================================
# S9 路由
# ============================================================
print("== S9 路由 ==")
p = new_plugin()
ev = _Ev("挠痒痒", gid="123456")
check("S9 密语在精简版失联", drain(p.on_message(ev)) == [])
ev = _Ev("股市大赦", gid="123456")
check("S9 大赦在精简版失联", drain(p.on_message(ev)) == [])
ev = _Ev("黑天鹅 XBK 涨", gid="123456")
check("S9 定向黑天鹅失联", drain(p.on_message(ev)) == [])
ev = _Ev("小堡股市：规则", gid="123456")
check("S9 规则命令可用", "规则" in texts(drain(p.on_message(ev))))
ev = _Ev("小堡股市：规则", gid="999999")
check("S9 别群不响应", drain(p.on_message(ev)) == [])
ev = _Ev("小堡股市：规则", gid=None)
check("S9 私聊不响应", drain(p.on_message(ev)) == [])
ev = _Ev("买 XBK 10", gid="123456")
p.add_coins("10086", "测试", 5000)
check("S9 裸代码买入可用", "成交：买入" in texts(drain(p.on_message(ev))))
ev = _Ev("小堡股市：持仓", gid="123456")
check("S9 持仓命令可用", "持仓明细" in texts(drain(p.on_message(ev))))
ev = _Ev("小堡股市：瞎写", gid="123456")
check("S9 未知子命令有兜底提示", "没认出" in texts(drain(p.on_message(ev))))

# ============================================================
# S10 事件 / 黑天鹅（强制触发，验清理后核心路径仍能跑）
# ============================================================
print("== S10 事件与黑天鹅 ==")
import inspect

p = new_plugin()
check("S10 事件方法已无 code/sign 参数",
      list(inspect.signature(p._fire_event_locked).parameters) == [],
      str(list(inspect.signature(p._fire_event_locked).parameters)))
check("S10 黑天鹅方法已无 code/sign/now 参数",
      list(inspect.signature(p._fire_black_swan_locked).parameters) == [],
      str(list(inspect.signature(p._fire_black_swan_locked).parameters)))

p = new_plugin()
hit = p._fire_event_locked()
check("S10 随机事件能触发", isinstance(hit, dict) and len(hit) == 1
      and "🎲" in list(hit.values())[0], str(hit)[:60])
check("S10 事件股代码合法", bool(hit) and next(iter(hit)) in sm.STOCKS)

p = new_plugin()
_before = {c: float(p._st(c)["price"]) for c in sm.STOCKS}
swan = p._fire_black_swan_locked()
check("S10 黑天鹅能触发", isinstance(swan, dict) and "🌪️" in list(swan.values())[0],
      str(swan)[:60])
_scode = next(iter(swan))
_chg = abs(float(p._st(_scode)["price"]) / _before[_scode] - 1)
check("S10 黑天鹅幅度在 ±40~60% 区间",
      sm.BLACK_SWAN_MIN - 0.01 <= _chg <= sm.BLACK_SWAN_MAX + 0.01, "%.2f%%" % (_chg * 100))

p = new_plugin()
_cnt = sum(1 for _ in range(12) if p._fire_black_swan_locked())
check("S10 冷却：6 只各触发一次后停手", _cnt == len(sm.STOCKS), "触发 %d 次" % _cnt)
check("S10 冷却期满池仍空则返回 None", p._fire_black_swan_locked() is None)

print("\n==== %d PASS / %d FAIL ====" % (_n - len(_fails), len(_fails)))
if _fails:
    print("FAIL 列表：" + " | ".join(_fails))
    sys.exit(1)
