# -*- coding: utf-8 -*-
"""群聊股市插件（可分发精简版）

一句话：股价不靠掷骰子，靠群友在 Bot 里的真实行为当基本面。
零 LLM、零 token、零外部依赖 —— 拷进 AstrBot 就能跑。

行情由五股力量合成（每 5 分钟一个 tick，24h 交易，每 2 小时一滚周期）：
    涨跌 = 行为基本面 55% + 资金面（群友买卖净额） + 动量 5% + 噪声 20%×股性 + 均值回归 10%
    另有：随机事件（±8%~28%，每股 1 小时冷却）、黑天鹅（±40%~60%，突破涨跌停）、
         剧情行情（随机开启的 2 小时单边行情，不播报）、事件余波。
    防操纵：同指标刷量边际递减、单 tick 封顶 ±12%、每股独立涨跌停 22%~35%、
            价格下限 1（永不归零）、连续 2h 低于 5 面值线退市清算 + 2h 停牌重组。

六只股各吃一组「联动源」（见 STOCKS 的 sources，权重和 = 1.0，负权重 = 反向源）：
    XBK 蟹堡王    —— 签到 / 摸摸 / 投喂 / 陪玩 / 互动 / 跑跑里程
    BBB 堡贝银行  —— 银行存款 / 贷款 / 高利贷(反向) / 红包流水 / 记账笔数
    PLB 娱乐城    —— 彩票流水 / 刮刮乐 / 抽卡 / 聊·炸弹 / 聊·轮盘
    SMW 传媒      —— 海龟汤 / 剧场 / 接龙 / 猜猜猜 / 猜人（记真实开局局数）
    HZB 电器      —— 聊·唱歌 / 说话 / 手机 / 相册 / 打 BOSS
    CCB 婚介      —— 相亲 / 表白 / 抢对象 / 配对成交 / 新生 / 在孕
    数据从哪来？全部由文件顶部的 SOURCES 适配器决定 —— 见那里。

指令（前缀可改，见 CMD_PREFIXES）：
    行情 / 资讯 / 联动 / 买 XBK 200 / 卖 XBK 100 / 持仓 / 排行 / 排行 今日 / 规则
    手续费 0.3% 双向，全部进金库，每晚 22:05 按 XBK 持股分红给股东。

移植只需动三处：
    ① 顶部 PLUGIN_NAME / GROUP_ID
    ② SOURCES 数据源适配器（把 6 只股的联动源接到你自己的玩法上）
    ③ get_balance / add_coins（接你自己的货币系统；不接就用内置本地钱包）

状态文件：data/plugin_data/<PLUGIN_NAME>/stock.json
"""
from __future__ import annotations

import asyncio
import json
import math
import random
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.star import Context, Star, StarTools

# ============================================================
# ★★★ 移植配置：整个插件只有这一块必须改 ★★★
# ============================================================
PLUGIN_NAME = "astrbot_plugin_stock_demo"   # 你的插件目录名（数据会存到 data/plugin_data/<这个名字>/）
GROUP_ID = "123456789"                      # ★ 改成你的群号（留着不改 = 插件不响应任何消息）
# 指令前缀：群友发「小堡股市：行情」触发。想换成「股市：」就改这里（含中英文冒号 + 空格三种写法）
CMD_PREFIXES = ("小堡股市：", "小堡股市:", "小堡股市 ")

CST = timezone(timedelta(hours=8))

# ---------- 参数 ----------
INIT_PRICE = 10.0        # 股价初值（中位玩家 25 堡贝买得起 2-3 股）
FEE = 0.003              # 手续费 0.3% 双向
LIMIT_PCT = 0.30         # 全局涨跌停兜底（v2.0 起每只股另有独立 limit）
TICK_CAP = 0.12          # v2.0 单 tick 封顶 ±12%（PLB 噪声 ±7.9% 不再被压平）
TICK_SEC = 300           # 5 分钟 tick（v2.2.0 提速，各系数按每小时经济当量折算）
ANOMALY_PCT = 0.45       # 3 tick 累计 ±45% → 异常告警+降温（v1.9.1 D档）
PRICE_FLOOR = 1.0        # 永不归零
CENTER_ALPHA = 0.01      # 价值中枢跟随长期基本盘的慢速 EMA
NOISE_SIGMA = 0.07       # v1.1.0 联动可见性：0.15→0.07。噪声日内随机游走 ±36% → ±17%，
                         # 原来联动趋势（照常 +3.5%/日）被噪声完全淹没，现在肉眼可辨（仿真信噪比 −0.14 → 0.81）
EVENT_CHANCE = 0.006     # 自发随机事件：每股每 tick 0.6%（v2.2.0 减半，每小时频率不变）
DELIST_REFUND = 0.5      # 清算退款 = 现价 × 50%（自己卖=全价，拖到清算=半价）
DELIST_PAUSE_SEC = 7200  # v2.9.2 退市停牌 2h 后重组上市（2h 倒计时 + 2h 停牌 = 两周期）
FLOOR_LINE = 5.0         # v2.4.0 面值线：收盘 < 5 堡贝计入面值退市计数
FLOOR_STREAK_MAX = 24    # v2.9.3 连续 24 个 5min tick（2h）低于面值线 → 面值退市（按 tick 计，非周期）
# v2.9.10：移除 v2.9.8 引入的「深低估豁免」。豁免线（中枢×0.5）与面值线（绝对 5.0）参照系不同，
# 中枢随行情漂移后，同一参数在中枢 ≥10 的股票上使退市机制整体失效（一跌破面值即豁免）、
# 在中枢 <10 的股票上又形同虚设（豁免线已低于面值线）。现回归 v2.9.7 语义：
# 价格 < 面值线即计数，连续 2h → 退市清算（现价 5 折）→ 停牌 2h → 回 10 堡贝重组上市。
LOW_PULL_TRIGGER = 0.30  # v2.9.8 联动 2.0：价格低于中枢 30% 以上 → 回归皮筋加强
LOW_PULL_BOOST = 3.0     # v2.9.8 联动 2.0：深度低估时回归系数 ×3（帮被错杀的低价回中枢）
NEUTRAL_RATIO = 0.78     # v1.1.0 联动可见性：0.9→0.78。ratio 是「今日累计 ÷ 近 3 日同时刻均值」，
                         # 旧值 0.9 意味着「要比平时还热闹才微涨」；改后「玩到平时的 78% 就止跌，
                         # 80% 起涨」，照常玩 = 稳定小涨而不是随噪声随机漂
SCORE_FLOOR = -0.45      # v2.9.8 联动 2.0：冷清时缓跌（原 −0.6 顶格砸盘）

REGIME_CHANCE = 0.12      # v2.3.0 每周期结算时，震荡股开新剧概率（场上平均 1~2 只在演）
REGIME_DRIFT = 0.005      # v2.3.0 每 tick 剧情推力基数（5min 口径），按股性 vol 放大
REGIME_BULL_TICKS = (24, 24)   # v2.7.0 主升固定一个周期（24 tick = 2h），整期换幕
REGIME_BEAR_TICKS = 24         # v2.7.0 阴跌同步一个周期
ECHO_CHANCE = 0.25        # v2.3.0 余波：事件/天鹅后每 tick 概率连环（v2.9.9 0.35→0.25 收敛）
ECHO_TICKS = (5, 10)      # v2.3.0 余波持续 0.5~1h（v2.9.9 6~12 → 5~10）
ECHO_PCT = (0.02, 0.06)   # v2.3.0 每次余波 ±2~6%（v2.9.9 3~8% → 2~6%；事件本身幅度不动）
EVENT_MIN_PCT = 0.08     # 事件幅度下限 ±8%
EVENT_MAX_PCT = 0.28     # 事件幅度上限 ±28%（v1.9.1 D档，顶格近触板）
EVENT_COOLDOWN_SEC = 3600  # 单股事件冷却 1 小时
SENSE = 0.22             # v1.1.0 联动可见性：0.16→0.22。配合上面两项，「多玩 60%」一周 ≈ +35%，
                         # 「照常玩」一周 ≈ +7%；中枢回归皮筋把长线压住（30 日照常 ≈ +21%，不会失控）
FLOW_CAP = 0.06          # v2.9.0 单周期资金面位移封顶 ±6%（联动/骰子/剧情为主、资金面为辅）
FLOW_CAP_X = 0.08        # v2.9.0 个人单股单期计入封顶（按市值换手 8%）——单人对市场的影响有上限
FLOW_A = 0.04            # v2.9.0 饱和曲线半饱和换手（群体净换手 4% 时影响力对折）——堆钱边际递减
FLOW_SELL_MULT = 1.3     # v2.0 卖出冲击系数（卖比买更砸盘，堵拉盘套现）
BLACK_SWAN_CHANCE = 0.0002   # v2.0 黑天鹅：每股每 tick 0.02%（v2.2.0 减半），全群约 3~4 天一次
BLACK_SWAN_MIN = 0.40    # v2.0 黑天鹅幅度下限 ±40%
BLACK_SWAN_MAX = 0.60    # v2.0 黑天鹅幅度上限 ±60%（突破涨跌停）
BLACK_SWAN_COOLDOWN = 21600  # v2.0 单股黑天鹅冷却 6 小时
HIST_MAX = 288           # v2.2.0 保留一整天逐 tick 价格（288 点@5min）；图表仍只取近 96 点（8h）
FLOW_MV_FLOOR = 500.0    # v1.8 流通市值分母下限（防小盘被一人推爆）
# v1.5 起全天候交易，无开盘/收盘窗口

# ---------- v2.11.0 活跃度闭环（唯一 KPI = 群活跃度，股价只是手段） ----------
# 诊断：闭环 = 群友发言/玩玩法 → 联动基本面 → 股价涨跌 → 群里播报 → 感知因果 → 更想聊。
# 前三环都通（联动 2.0，行为占定价 55%），第四环「播报」在 v1.9.1 因夜间刷屏被砍断，
# 群友默认感知「这股市是随机的，跟我没关系」。本版把播报接回来，且不刷屏。
LK_BCAST_SEC = 1800        # 联动热度播报节流：30 分钟一次（提醒要够多，靠节流防刷屏）
LK_QUIET_HOURS = (3, 9)    # 静默时段 [3:00, 9:00) 不播（别在半夜扰民）
LB_HOUR = 20               # 今日收益榜播报时刻：每晚 20:00 后首个 tick 触发，每天一次

# 代码 -> (名称, 概率题材, 联动源 [(metric, weight)], 基线备注)

# v2.7.0：字符走势条已随「走势」命令一并移除


# ---------- v2.0 黑天鹅事件池：(方向 +1涨/-1跌, 文案) ----------
BLACK_SWAN_EVENTS: dict[str, list[tuple[int, str]]] = {
    "XBK": [
        (+1, "蟹堡王秘方被米其林评审偷拍曝光，全球吃货涌入排队"),
        (-1, "蟹堡王后厨卫生丑闻登上热搜，门店被围堵"),
        (+1, "蟹老板宣布无限期免费加蟹黄，股价一飞冲天"),
        (-1, "蟹老板被曝挪用公款炒股，蟹堡王紧急停业整顿"),
    ],
    "BBB": [
        (+1, "堡贝银行宣布存款利率翻倍，全群挤兑式存款"),
        (-1, "堡贝银行金库被曝空账，挤兑潮全面爆发"),
        (+1, "堡贝银行拿到蟹堡王集团巨额注资，信用评级飙升"),
        (-1, "堡贝银行风控系统崩溃，坏账率一夜飙升"),
    ],
    "PLB": [
        (+1, "痞老板娱乐城开出史上最大奖，赌客蜂拥而至"),
        (-1, "痞老板跑路传闻引爆恐慌盘，娱乐城被搬空"),
        (+1, "娱乐城拿下独家版权，人气一夜暴涨"),
        (-1, "娱乐城被查出老千，全场封停整顿"),
    ],
    "SMW": [
        (+1, "水母湾传媒独家爆料引爆全网，广告商排队送钱"),
        (-1, "水母湾传媒被曝编造新闻，公信力崩塌"),
        (+1, "水母湾传媒新剧爆红，收视率破纪录"),
        (-1, "水母湾传媒主编集体离职，股价雪崩"),
    ],
    "HZB": [
        (+1, "海之霸电器发布革命性新品，预售秒罄"),
        (-1, "海之霸电器电池爆炸事故被曝光，全线召回"),
        (+1, "海之霸电器拿下国家级大单，产能翻倍"),
        (-1, "海之霸电器核心专利被判无效，竞争对手围剿"),
    ],
    # v2.12.0 新股 CCB（小堡婚介）：黑天鹅 4 条，正负 1:1
    "CCB": [
        (+1, "全群集体脱单日，婚介所门被挤破，成交额翻十倍"),
        (+1, "婚介所被评为年度最佳红娘机构，资本疯狂涌入"),
        (-1, "婚介所被曝买卖会员隐私，全平台下架"),
        (-1, "红娘集体罢工，门口贴出『暂停营业』"),
    ],
}


# ---------- 随机事件池（v1.6）：(方向 +1涨/-1跌, 文案) ----------
# v2.9.9 事件扩充：每股 12 条、正负严格 1:1（原 31 条配比不齐：SMW 4:2 / HZB 3:2 偏多）；
# 池子变厚 = 单条事件被抽中的概率下降 → 连续同向的概率同步下降，方向更公平、文案更不重样。
EVENTS: dict[str, list[tuple[int, str]]] = {
    "XBK": [
        (+1, "蟹堡王推出新品『钓鱼堡』，门口排队两小时"),
        (+1, "海底蟹堡秘方冲上热搜，全网来打卡"),
        (+1, "蟹老板宣布全员加蟹黄，士气大涨"),
        (+1, "小堡研发双层海苔堡，试卖一空"),
        (+1, "蟹堡王上线外卖专线，午市订单翻倍"),
        (+1, "蟹堡王拿下海底美食节冠名，客流暴涨"),
        (-1, "痞老板又来偷秘方，后厨一片混乱"),
        (-1, "外卖平台抽成上调，利润被压缩"),
        (-1, "章鱼哥摆臭脸劝退了三桌客人"),
        (-1, "供货海藻断货，成本猛涨"),
        (-1, "厨房油锅起火，紧急停业半天"),
        (-1, "员工集体请病假，出餐速度腰斩"),
    ],
    "BBB": [
        (+1, "存款利率上调，堡贝排队存钱"),
        (+1, "理财新品『蟹黄宝』遭疯抢"),
        (+1, "发薪日流水暴涨"),
        (+1, "定期存款送蟹黄堡券，储户排队开户"),
        (+1, "银行 App 上线零钱自动生息，好评刷屏"),
        (+1, "堡贝银行拿下全群工资代发，资金池暴涨"),
        (-1, "金库被痞老板顺走一车，警方追查中"),
        (-1, "几笔大额借款追不回来，坏账风波"),
        (-1, "系统宕机半天，储户排队取钱"),
        (-1, "异地转账手续费上调，储户转投他行"),
        (-1, "理财到期兑付延迟，门口围了一群人"),
        (-1, "网点装修停业一周，业务量腰斩"),
    ],
    "PLB": [
        (+1, "夜场爆满，老虎机吐币如流水"),
        (+1, "新赌桌『幸运海星』上线，玩家挤破头"),
        (+1, "豪礼大放送，人气飙升"),
        (+1, "周年庆全场返水翻倍，赌桌排队到门外"),
        (+1, "新玩法『海星连连看』上线，日流水破纪录"),
        (+1, "痞老板请来海妖驻唱，夜夜爆满"),
        (-1, "被举报出千，监管上门检查"),
        (-1, "头奖被天选玩家一次性端走，彩池大失血"),
        (-1, "停电检修三小时，玩家悻悻而归"),
        (-1, "抽水比例上调引发不满，玩家集体离场"),
        (-1, "老虎机主板烧毁，一半机台停摆"),
        (-1, "被曝未成年入场，监管连夜查封"),
    ],
    "SMW": [
        (+1, "独家爆料刷屏全网，播放量爆炸"),
        (+1, "新剧开播即登顶热度榜"),
        (+1, "海龟汤新档期收视长虹"),
        (+1, "爆款综艺接到大单赞助"),
        (+1, "独家专访痞老板，全网转载"),
        (+1, "新综艺招商超额三倍，赞助商排队"),
        (-1, "当家主播塌房，广告主连夜撤单"),
        (-1, "剧本库存告急，档期青黄不接"),
        (-1, "直播事故翻车，热搜挂了一整天"),
        (-1, "广告系统误投垃圾流量，广告主索赔"),
        (-1, "当家编剧被挖角，新剧停摆"),
        (-1, "版权官司败诉，赔了一大笔"),
    ],
    "HZB": [
        (+1, "新品手机『海霸 X』发布，预售秒空"),
        (+1, "以旧换新补贴出炉，门店爆单"),
        (+1, "小堡相册打印机卖断货"),
        (+1, "海之霸空调新品上市，经销商抢货"),
        (+1, "拿下海外大单，工厂三班倒"),
        (+1, "小堡联名款耳机预售破十万"),
        (-1, "大批量召回：某型号电池鼓包"),
        (-1, "隔壁蟹堡王用电超标被查，连带整改"),
        (-1, "原材料涨价，毛利率被压薄"),
        (-1, "售后网点大面积关停，投诉暴涨"),
        (-1, "新品发布会翻车，口碑雪崩"),
        (-1, "被曝虚标参数，监管约谈"),
    ],
    # v2.12.0 新股 CCB（小堡婚介）：12 条、正负 1:1，与既有池同规
    "CCB": [
        (+1, "婚介所门口挂起红灯笼，相亲队伍排到了街角"),
        (+1, "金牌红娘一场撮合三对，语录被全群转发"),
        (+1, "会员制改版上线，入会费收到手软"),
        (+1, "一对老情侣补办婚礼，红包雨下了一整晚"),
        (+1, "婚介所推出『盲盒相亲』，报名表一小时抢空"),
        (+1, "草草中心送来锦旗，感谢联营带来客源"),
        (-1, "三对情侣同日分手，红娘当场沉默"),
        (-1, "会员资料被曝造假，退费电话打爆前台"),
        (-1, "相亲角被投诉扰民，城管上门劝离"),
        (-1, "抢对象抢出火气，两拨人在门口对峙"),
        (-1, "金牌红娘跳槽，带走半个客户名单"),
        (-1, "婚介所被曝『拉郎配』，口碑一夜崩塌"),
    ],
}

STOCKS: dict[str, dict[str, Any]] = {
    # v2.8.0 联动重整：五岛各归其位（行为→XBK｜银行→BBB｜博彩→PLB｜话题→SMW｜海之霸话题+BOSS→HZB），
    # 权重均和为 1.0；负权重 = 反向联动（高利贷借得越多越压 BBB）。
    "XBK": {"name": "蟹堡王", "vol": 1.0, "limit": 0.22, "sources": [
        ("checkin", 0.25), ("pet", 0.20), ("fed_meals", 0.20), ("play", 0.15),
        ("bite", 0.10), ("run_km", 0.10)]},
    "BBB": {"name": "堡贝银行", "vol": 1.3, "limit": 0.28, "sources": [
        ("bank_dep", 0.30), ("loan_out", 0.15), ("shark_out", -0.15),
        ("rp_flow", 0.25), ("bank_events", 0.15)]},
    "PLB": {"name": "痞老板娱乐城", "vol": 1.8, "limit": 0.35, "sources": [
        ("lottery_flow", 0.30), ("scratch", 0.20), ("gacha_dr", 0.25),
        ("kw_bomb", 0.15), ("kw_russian", 0.10)]},
    # v2.9.13 SMW 换源（治本方案 A）：kw_* 话题关键词 → game_* 真实开局次数。
    # 关键词口径可被水聊刷高（实测 kw_soup 日 0-21 条 vs 真实开局 0-5 局），
    # 且与实际是否真在玩脱钩；改读引擎 day_counts 后联动只认真实玩法。权重不变。
    "SMW": {"name": "水母湾传媒", "vol": 1.4, "limit": 0.30, "sources": [
        ("game_soup", 0.30), ("game_theater", 0.25), ("game_story", 0.20),
        ("game_guess", 0.15), ("game_akinator", 0.10)]},
    "HZB": {"name": "海之霸电器", "vol": 1.5, "limit": 0.32, "sources": [
        ("kw_sing", 0.25), ("kw_speak", 0.20), ("kw_phone", 0.20),
        ("kw_album", 0.15), ("boss", 0.20)]},
    # v2.12.0 新股 CCB 小堡婚介：只吃「群友之间」的互动，与 XBK 的「摸小堡」严格区分。
    # aff_* 来自组队插件 couples.json 的按日计数；cc_* 来自草草系统的按日成交/出生/在孕。
    # 权重和 1.00；vol 1.7 属疯狂档（婚介生意情绪化），涨跌停 ±33%。
    "CCB": {"name": "小堡婚介", "vol": 1.7, "limit": 0.33, "sources": [
        ("aff_touch", 0.30), ("aff_confess", 0.22), ("cc_hit", 0.18),
        ("cc_born", 0.12), ("aff_steal", 0.10), ("cc_preg", 0.08)]},
}

# v2.8.0 金额型联动源：计分走对数归一（log1p 差 ×100 ≈ 百分比涨幅），
# 存量（存款/贷款）与流水（红包/彩票）统一量纲，不再按元直算炸估值。
MONEY_KEYS = {"bank_dep", "loan_out", "shark_out", "rp_flow", "lottery_flow"}

# v2.9.13 SMW 游戏热度源：真实开局次数。引擎在 5 个玩法开局点各 +1，落
# grp.global.games.day_counts[day][key]（见 games 引擎 _bump_game_day）。
GAME_KEYS = {"game_soup", "game_theater", "game_story", "game_guess", "game_akinator"}

# v2.9.13 低基数键：平滑地板 1.0（计数键默认 3.0）。游戏开局是低频事件
# （实测日均 0-5 局，kw 口径 0-21 条），沿用 3.0 会把 ratio 压在 1.0 附近 →
# 联动失去动态范围。线上真实数据仿真：SMW fund 极差 地板3.0=0.176（折算日漂移
# ±6.2%）／地板1.0=0.335（±11.8%）；同档对照 XBK ±8.6% / BBB ±3.2%。
LOWFREQ_KEYS = set(GAME_KEYS) | {
    # v2.12.0 CCB 婚介源：表白限 1 次/人/日、抢对象限 3 次/人/日、生娃 0~3 胎/日，
    # 计数键默认 3.0 地板会把 ratio 压死在 1.0 → 与游戏局数同档，地板取 1.0。
    "aff_confess", "aff_steal", "cc_hit", "cc_born", "cc_preg",
}

# v2.8.0 话题词典：群聊关键词 → 话题热度键（PLB/HZB 的 kw_* 源）。
# 黑话随时往元组里加词即可；命中 = 子串包含，每人每话题每日最多计入 KW_CLAMP 条。
# v2.9.13：kw_soup/kw_theater/kw_story/kw_guess/kw_guessman 五条退役——SMW 改读
# 真实开局次数（game_*），关键词口径只留给 PLB/HZB。
KW_LEXICON = {
    "kw_bomb": ("炸弹", "拆弹", "引爆", "踩雷"),
    "kw_russian": ("轮盘", "开枪", "扣扳机", "活到最后"),
    "kw_sing": ("唱歌", "点歌", "翻唱", "来一首", "唱一个"),
    "kw_speak": ("小堡说话", "说句话", "唠嗑", "陪聊"),
    "kw_phone": ("小堡手机", "玩手机", "掏手机"),
    "kw_album": ("相册", "拍立得", "照片墙"),
}
KW_CLAMP = 3   # 每人每话题每日计入上限（防刷，边际全砍）


def _stock_tag(code: str) -> str:
    """v2.0 股性标签：按波动率分档（v2.7.0 起行情总卡「股性」列展示）。"""
    vol = float(STOCKS.get(code, {}).get("vol", 1.0))
    return "稳健" if vol < 1.2 else ("活跃" if vol < 1.5 else "疯狂")


# metric 键 -> 覆盖说明（资讯里用真实数据说话）
METRIC_LABEL = {
    "checkin": "签到人次", "pet": "摸摸小堡", "bite": "咬小堡", "play": "陪小堡玩",
    "fed_meals": "投喂正餐", "run_km": "小堡跑跑(公里)",
    "bank_dep": "银行总存款", "bank_events": "银行记账笔数", "loan_out": "贷款本金",
    "shark_out": "高利贷本金", "rp_flow": "红包流水",
    "lottery_flow": "彩票投注流水", "scratch": "刮刮乐", "gacha_dr": "抽卡抽数",
    "boss": "打 BOSS 人次",
    "kw_bomb": "聊·炸弹", "kw_russian": "聊·轮盘",
    "kw_sing": "聊·唱歌", "kw_speak": "聊·说话",
    "kw_phone": "聊·手机", "kw_album": "聊·相册",
    # v2.9.13 SMW 换源：真实开局次数（引擎 global.games.day_counts）
    "game_soup": "海龟汤局数", "game_theater": "剧场局数", "game_story": "接龙局数",
    "game_guess": "猜猜猜局数", "game_akinator": "猜人局数",
    # v2.12.0 CCB 小堡婚介联动源（群友之间，不是摸小堡）
    "aff_touch": "相亲摸摸", "aff_confess": "正式表白", "aff_steal": "抢对象",
    "cc_hit": "草草成交", "cc_born": "新生胎数", "cc_preg": "在孕待产",
}


# ============================================================
# ★★★ 数据源适配器：第二块需要你动手的地方 ★★★
# ============================================================
# 每只股票吃哪些指标、权重多少，见上面 STOCKS 里各股的 sources 列表
# （权重和 = 1.0；负权重 = 反向源，数值越大越压股价）。
# 你只需要为「自己群里真的有的玩法」实现对应的取数函数，其余留 _zero 即可。
#
# 函数签名：fn(day: str, gid: str) -> float
#   day = "2026-10-20"（今天）
#   gid = 群号
#   返回 该指标「今日累计值」（次数 / 金额 / 局数，单位随指标而定）
#
# 安全约定：返回 0 表示今天没数据；抛异常会被捕获并按 0 处理，
#           联动只会显得冷清，绝不会崩插件。
# 经验值：日均只有 0~5 次的低频指标（开局次数、表白次数），
#         记得把它的键加进上面的 LOWFREQ_KEYS，否则热度会被压平。


def _zero(day: str, gid: str) -> float:
    """占位数据源：还没接的指标一律返回 0。"""
    return 0.0


def _read_json(plugin_dir: str, filename: str) -> dict:
    """从别的插件的数据目录读一个 JSON；读不到/格式坏都返回空 dict，不抛错。

    这是跨插件取数最常用的方式（本插件也支持直接读文件，见下例）。
    """
    try:
        p = StarTools.get_data_dir(plugin_dir) / filename
        if p.exists():
            data = json.loads(p.read_text(encoding="utf-8"))
            return data if isinstance(data, dict) else {}
    except Exception as exc:
        logger.debug("[StockDemo] 读 %s/%s 失败：%s", plugin_dir, filename, exc)
    return {}


def _src_checkin(day: str, gid: str) -> float:
    """【完整示例】签到人次：数「今日签到了几个人」。

    把 "your_plugin_dir" / "state.json" / 下面几层 json 路径换成你自己的即可。
    """
    data = _read_json("your_plugin_dir", "state.json")
    users = ((data.get("groups") or {}).get(gid) or {}).get("users") or {}
    n = 0
    for u in users.values():
        if not isinstance(u, dict):
            continue
        d = (u.get("daily") or {}).get(day)
        if isinstance(d, dict) and d.get("checkin"):
            n += 1
    return float(n)


# 指标键 -> 取数函数。键名必须与 STOCKS 里 sources 用的名字一致。
SOURCES = {
    # —— 行为类（次数）——
    "checkin": _src_checkin,   # 签到人次（上面是完整示例，照抄改路径）
    "pet": _zero,              # 摸摸/撸一撸 次数
    "bite": _zero,             # 互动小动作 次数
    "play": _zero,             # 陪玩 次数
    "fed_meals": _zero,        # 投喂 次数
    "run_km": _zero,           # 跑步/运动 公里数
    # —— 金额类（会走 log 归一，单位是「堡贝/金币」，不是次数）——
    "bank_dep": _zero,         # 银行总存款
    "bank_events": _zero,      # 记账笔数（这个其实是次数，不是金额）
    "loan_out": _zero,         # 贷款本金
    "shark_out": _zero,        # 高利贷本金（反向源）
    "rp_flow": _zero,          # 红包流水
    "lottery_flow": _zero,     # 彩票投注流水
    # —— 博彩类（次数）——
    "scratch": _zero,          # 刮刮乐次数
    "gacha_dr": _zero,         # 抽卡抽数
    # —— 游戏类（局数，低频，已在 LOWFREQ_KEYS）——
    "game_soup": _zero,        # 海龟汤局数
    "game_theater": _zero,     # 剧场局数
    "game_story": _zero,       # 接龙局数
    "game_guess": _zero,       # 猜猜猜局数
    "game_akinator": _zero,    # 猜人局数
    # —— 话题类（群聊关键词命中条数）——
    "kw_bomb": _zero,
    "kw_russian": _zero,
    "kw_sing": _zero,
    "kw_speak": _zero,
    "kw_phone": _zero,
    "kw_album": _zero,
    # —— BOSS ——
    "boss": _zero,             # 打 BOSS 人次
    # —— 婚介类（群友之间，低频）——
    "aff_touch": _zero,        # 相亲摸摸次数
    "aff_confess": _zero,      # 正式表白次数
    "aff_steal": _zero,        # 抢对象次数
    "cc_hit": _zero,           # 配对成交次数
    "cc_born": _zero,          # 新生胎数
    "cc_preg": _zero,          # 在孕待产数
}

RULES_TEXT = (
    "📈 小堡股市 · 规则\n"
    "━━━━━━━━━━━━━━\n"
    "・6 只代码：XBK 蟹堡王｜BBB 堡贝银行｜PLB 痞老板娱乐城｜SMW 水母湾传媒｜HZB 海之霸电器｜CCB 小堡婚介\n"
    "・行情四驱动：行为+话题/游戏热度为主（群里聊什么、开几局游戏都算） + 资金面 + 随机事件 + 黑天鹅\n"
    "・股性分化：野性系数越高，日常抖动与基本面弹性越大；每只股独立涨跌停（XBK±22%｜BBB±28%｜SMW±30%｜HZB±32%｜CCB±33%｜PLB±35%），单 tick 封顶 ±12%\n"
    "・资金面：买卖会影响行情，量越大越明显——但市场未必照单全收\n"
    "・随机事件：每股每 tick 有概率触发，幅度 ±8%~±28%\n"
    "・黑天鹅：极端行情 ±40%~±60%，直接突破涨跌停，全群约 3~5 天一次\n"
    "・节奏：每 5 分钟一动，每 2 小时一滚（每天 12 期，0 点起算）\n"
    "・指令：小堡股市：行情｜资讯｜联动｜买 XBK 200｜卖 XBK 100（1 股起）\n"
    "・财富：小堡股市：持仓｜排行（总资产）｜排行 今日（收益率）；手续费 0.3% 双向，随时买卖无时限\n"
    "・持仓 XBK = 蟹堡王股东：每晚 22:05 按持股分全平台抽水（刮卡盈余+彩票抽成+股市手续费）\n"
    "・退市：连续 2h（24 个 5 分钟）低于 5 堡贝面值线 → 退市清算，持仓按现价 5 折强制退款；\n"
    "　　倒计时期间自己卖出=全价，拖到清算=半价；停牌 2 小时后回 10 堡贝重组上市"
)

_STATE_DEFAULT = dict(
    stocks={},             # code -> {"price","open","prev_close","center","mom","moves":[],"alert_n":0}
    holdings={},           # uid  -> {"name","pos":{code:{"sh","avg","today"}}}
    samples={},            # metric -> {"days": {day: {hour: value}}, "last": value}
    last_settle_day="",
    last_settle_period="",   # 周期结算指针（day#p，方案 A）
    day_closes={},         # day -> {code: close} 近 30 日
    last_news={},          # v2.0.3 上一轮归因（供「资讯」按需查询，不主动播报）
    event_log=[],          # v2.0.5 最近随机事件环形日志（供「资讯」回放最近 5 次）
    kw_day={},             # v2.9.6 话题热度当日累计 {day:{kw:n}}（持久化，重启不再清零）
    kw_user={},            # v2.9.6 话题钳制账 {day:{uid:{kw:n}}}（持久化）
    wallet={},             # 本地钱包 {uid: {"name": str, "coins": int}}（接自己的货币系统就废掉这张表）
    div_pool=0,            # 金库：累计的手续费，每晚按 XBK 持股分红（负数 = 亏损日共担）
    asset_snap={},         # v2.11.0 活跃度闭环：每日首个 tick 的总资产快照 {day:{uid:total}}（收益率榜基线）
    asset_snap_ts={},      # v2.11.1 快照时刻 {day:ts}：收益榜须等快照「过一个 tick」才播（防同 tick 基线==当前）
    last_lk_bcast_ts=0.0,  # v2.11.0 上次联动热度播报时间戳（30 分钟节流，随存档持久化防重启刷屏）
    lb_last_day="",        # v2.11.0 上次播报今日收益榜的日期（每天一次）
)


class StockDemoPlugin(Star):
    def __init__(self, context: Context, config: "dict | None" = None):
        super().__init__(context)
        self.data_dir: Path = StarTools.get_data_dir(PLUGIN_NAME)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.state_file = self.data_dir / "stock.json"
        self.render_dir = self.data_dir / "render"   # v2.0.7 行情卡 PNG 输出目录
        self.render_dir.mkdir(parents=True, exist_ok=True)
        self._lock = asyncio.Lock()
        self._state: dict[str, Any] = self._load_state()
        self.umos: dict[str, str] = dict(self._state.get('umos', {}))  # v1.8.2 随存档恢复，重启后播报不再失联
        self.group_id = str((config or {}).get("group_id") or GROUP_ID).strip() or GROUP_ID
        if self.group_id == "123456789":
            logger.warning("[StockDemo] 还没填群号！请把 main.py 顶部的 GROUP_ID 改成你的群号"
                           "（或在插件配置里填 group_id），否则插件不会响应任何消息")
        logger.info("[StockDemo] 股市插件已加载：群 %s｜5min tick｜6 只股｜噪声+事件+黑天鹅+联动+资金面",
                    self.group_id)
        self._kw_day = self._state.setdefault("kw_day", {})    # v2.9.6 持久化：话题热度当日累计 {day:{kw:n}}
        self._kw_user = self._state.setdefault("kw_user", {})  # v2.9.6 持久化：话题钳制账 {day:{uid:{kw:n}}}
        self._kw_lock = asyncio.Lock()

    # ---------- 状态 ----------
    def _load_state(self) -> dict[str, Any]:
        try:
            if self.state_file.exists():
                loaded = json.loads(self.state_file.read_text(encoding="utf-8"))
                if isinstance(loaded, dict):
                    for k, v in _STATE_DEFAULT.items():
                        loaded.setdefault(k, json.loads(json.dumps(v)))
                    self._backfill_hist(loaded)   # v2.0.1 老存档补走势历史
                    self._backfill_delist(loaded)   # v2.1.1 老存档补退市字段
                    return loaded
        except Exception as exc:
            logger.warning("[StockDemo] 状态读取失败：%s", exc)
        return json.loads(json.dumps(_STATE_DEFAULT))

    @staticmethod
    def _backfill_hist(state: dict) -> None:
        """v2.0.1：老存档无 hist 字段 → 用本期开盘价 + moves 反推最近几个 tick 回填，
        让「走势」上线即有形，而非干等 4 小时。moves[i] = price[i+1]/price[i] - 1。"""
        for code, s in (state.get("stocks") or {}).items():
            if not isinstance(s, dict) or s.get("hist"):
                continue
            try:
                price = float(s.get("price", INIT_PRICE))
            except (TypeError, ValueError):
                price = float(INIT_PRICE)
            pts = []
            try:
                opn = float(s.get("open"))
            except (TypeError, ValueError):
                opn = None
            if opn and opn > 0:
                pts.append(opn)                      # 本周期开盘价（最早锚点）
            inv, cur = [], price
            for m in reversed(list(s.get("moves") or [])[-3:]):
                try:
                    m = float(m)
                except (TypeError, ValueError):
                    break
                if m <= -0.95:
                    break
                cur = cur / (1 + m)                  # 反推上一个 tick 的价
                inv.append(cur)
            pts.extend(reversed(inv))                # 还原成时间正序
            pts.append(price)
            out = []
            for v in pts:
                r = round(float(v), 2)
                if not out or abs(r - out[-1]) > 1e-9:
                    out.append(r)
            s["hist"] = out[-HIST_MAX:] or [round(price, 2)]

    @staticmethod
    def _backfill_delist(state: dict) -> None:
        """v2.1.1：老存档无退市字段 → 用 hist 最低价回填 prev_day_low/day_low，
        「面值线」列与退市进度上线即生效，而非干等次日 0 点（次日自动被真实锁定覆盖）。"""
        for code, s in (state.get("stocks") or {}).items():
            if not isinstance(s, dict):
                continue
            try:
                price = float(s.get("price", INIT_PRICE))
            except (TypeError, ValueError):
                price = float(INIT_PRICE)
            hist = [float(v) for v in (s.get("hist") or [])
                    if isinstance(v, (int, float)) and v > 0]
            low = min(hist) if hist else price
            try:
                dl = float(s.get("day_low", 0) or 0)
            except (TypeError, ValueError):
                dl = 0.0
            if dl <= 0:
                s["day_low"] = low
            try:
                pdl = float(s.get("prev_day_low", 0) or 0)
            except (TypeError, ValueError):
                pdl = 0.0
            if pdl <= 0:
                s["prev_day_low"] = float(s["day_low"])
            try:
                float(s.get("delist_until", 0) or 0)
            except (TypeError, ValueError):
                s["delist_until"] = 0

    def _persist_locked(self) -> None:
        self._state["umos"] = dict(self.umos)   # v1.8.2 umo 随存档持久化
        tmp = self.state_file.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(self._state, ensure_ascii=False, indent=1), encoding="utf-8")
        tmp.replace(self.state_file)

    def _st(self, code: str) -> dict[str, Any]:
        st = self._state.setdefault("stocks", {})
        if code not in st:
            st[code] = {"price": INIT_PRICE, "open": INIT_PRICE, "prev_close": INIT_PRICE,
                        "center": INIT_PRICE, "mom": 0.0, "moves": [], "alert_n": 0,
                        "hist": [INIT_PRICE]}
        return st[code]

    # ============================================================
    # ★★★ 货币接口：接你自己的经济系统，只改这两个函数 ★★★
    # ============================================================
    # 默认实现是插件内置的本地钱包（存在 stock.json 的 wallet 表里），
    # 好处是拷过去就能跑；如果你已经有堡贝/金币系统，把这两个函数体
    # 换成读你自己的账本即可，其余代码一行都不用动。
    #
    # 约定：
    #   get_balance(uid)            -> int   该用户当前余额
    #   add_coins(uid, name, amount) -> None  加/扣（amount 可为负，扣到 0 为止）
    # ============================================================
    def _wallet(self) -> dict[str, Any]:
        return self._state.setdefault("wallet", {})

    def get_balance(self, uid: str) -> int:
        """查余额。换成你自己的：return 你的账本[uid]["coins"]"""
        u = self._wallet().get(str(uid)) or {}
        return int(u.get("coins", 0) or 0)

    def add_coins(self, uid: str, name: str, amount: int) -> None:
        """加/扣币。换成你自己的：你的账本[uid]["coins"] += amount"""
        w = self._wallet()
        uid = str(uid)
        u = w.setdefault(uid, {"name": name or uid, "coins": 0})
        if name:
            u["name"] = name
        u["coins"] = max(0, int(u.get("coins", 0) or 0) + int(amount))

    # ---------- 联动源取样 ----------
    def _sample_metrics_locked(self) -> dict[str, float]:
        """按 SOURCES 适配器快照当日累计计数。

        这里只负责「遍历 SOURCES 里的每个指标、调它的取数函数、兜住异常」，
        具体数据从哪来完全由模块顶部的 SOURCES 决定 —— 换机器人只改那边。
        """
        day = datetime.now(CST).strftime("%Y-%m-%d")
        m: dict[str, float] = {}
        for _k in METRIC_LABEL:
            _fn = SOURCES.get(_k)
            if _fn is None:
                m[_k] = 0.0
                continue
            try:
                m[_k] = float(_fn(day, self.group_id) or 0.0)
            except Exception as exc:
                logger.debug("[StockDemo] 数据源 %s 取数失败，按 0 处理：%s", _k, exc)
                m[_k] = 0.0
        return m

    def _record_sample(self, metrics: dict[str, float]) -> None:
        day = datetime.now(CST).strftime("%Y-%m-%d")
        hour = f"{datetime.now(CST).hour:02d}"   # zero-pad str：JSON 往返后键恒 str（17:26 修 int/str 混键崩 tick）
        samples = self._state.setdefault("samples", {})
        for k, v in metrics.items():
            sd = samples.setdefault(k, {"days": {}})
            sd["days"].setdefault(day, {})[hour] = float(v)
            # 只留近 8 天
            days_sorted = sorted(sd["days"].keys())
            for old in days_sorted[:-8]:
                del sd["days"][old]

    @staticmethod
    def _heat_ratios(today: dict, samples: dict, day: str, days_back: int = 3) -> dict[str, float]:
        """v2.5.0 联动热度：今日 ÷ 近3日日峰值（无历史的键用 max(今日,8) 兜底）。"""
        out: dict[str, float] = {}
        for k in METRIC_LABEL:
            tv = float(today.get(k, 0) or 0)
            hist_days = []
            sd = (samples.get(k) or {}).get("days") or {}
            for d, hh in sd.items():
                if d == day or not isinstance(hh, dict) or not hh:
                    continue
                try:
                    hist_days.append((d, max(float(v) for v in hh.values())))
                except (TypeError, ValueError):
                    continue
            if hist_days:
                hist_days.sort(key=lambda x: x[0])
                peak = max([v for _d, v in hist_days[-days_back:]] + [tv, 1.0])
            else:
                # v2.12.0 冷启动兜底：低基数键（游戏局数 / 婚介源）日均 0~5，
                # 沿用 8.0 会让新键上线首日恒显「冷」→ 按低基数档取 2.0。
                peak = max(tv, 2.0 if k in LOWFREQ_KEYS else 8.0)
            out[k] = min(tv / peak, 1.0) if peak > 0 else 0.0
        return out

    @staticmethod
    def _fundamentals(sample: dict[str, float], prev_hour_val: dict[str, float],
                      baseline_delta: dict[str, float], d_u: float,
                      night: bool = False) -> dict[str, float]:
        """连续热度分（v2.9.8 联动 2.0）：今日累计 ÷ 近 3 日同时刻累计均值。

        score = clamp(ratio − 0.9, −0.45, +1.4) × 股性，ratio = (今日 + s)/(基线 + s)：
        - ratio > 1（今天比平时热闹）→ 正分，越猛越大（上限 +1.4×股性 ≈ +49%/天）
        - ratio ≈ 1（和平时差不多）→ 小正分 +0.1（"一般小涨"）
        - ratio < 1（冷清）→ 负分缓跌，下限 −0.45（原 −0.6 顶格砸盘）
        平滑 s = max(3, 0.15×基线)（金额键 max(1, 0.15×基线)）→ 稀疏指标不再因单小时抖动炸分；
        无历史参考（基线 0）→ ratio = 1.0 中性，不给虚假热度。
        v2.9.9：金额键封顶改到 ratio 层（基线 3 倍封顶）；无历史 + 负权重源 → 真中性 0 分。
        """
        out: dict[str, float] = {}
        for code, cfg in STOCKS.items():
            fund = 0.0
            wsum = 0.0
            for metric, w in cfg["sources"]:
                now_v = float(sample.get(metric, 0.0) or 0.0)
                _phv = prev_hour_val.get(metric)
                phv = float(_phv) if isinstance(_phv, (int, float)) else 0.0  # 恒 0（增量=今日累计）
                _is_money = metric in MONEY_KEYS
                _is_lowfreq = metric in LOWFREQ_KEYS   # v2.9.13 游戏局数：低基数键
                if _is_money:   # 金额键对数归一：log1p 差×100 ≈ 百分比涨幅
                    delta = max(0.0, 100.0 * (math.log1p(max(0.0, now_v))
                                              - math.log1p(max(0.0, phv))))
                else:
                    delta = max(0.0, now_v - phv)
                    # 刷量边际递减：人均 ×3 封顶，超出部分 0.3 折（v2.9.9 起只作用于计数键）
                    cap = 3.0 * d_u
                    if delta > cap > 0:
                        delta = cap + (delta - cap) * 0.3
                base = float(baseline_delta.get(metric, 0.0) or 0.0)
                if base > 1e-9:
                    # v2.9.13 低基数键（游戏局数）地板同金额键取 1.0：日均仅 0-5 局，
                    # 沿用 3.0 会让 ratio 恒贴 1.0 → SMW 联动失去动态范围（仿真 0.176→0.335）。
                    _s = max(1.0 if (_is_money or _is_lowfreq) else 3.0, 0.15 * base)
                    ratio = (delta + _s) / (base + _s)   # 平滑：稀疏指标不炸分
                else:
                    ratio = 1.0                          # 无历史参考 → 中性
                # v2.9.9 金额键封顶修正：金额键 delta 是绝对对数水平（量纲 ≠ 人次），
                # 套用「3×人数」封顶属口径错位（bank_dep 正分被误压成负分）；改在 ratio 层封顶——
                # 存量为基线 3 倍即封顶、超出 0.3 折，与计数键「人均 ×3」同一语义。
                if _is_money and ratio > 3.0:
                    ratio = 3.0 + (ratio - 3.0) * 0.3
                score = max(SCORE_FLOOR, min(1.4, ratio - NEUTRAL_RATIO)) * float(cfg["vol"])
                if base <= 1e-9 and (w < 0 or now_v <= 0):
                    # 无历史（ratio 恒 1.0）时死源真中性：
                    #   负权重源 → 一律 0（v2.9.9 原语义，无基线不该凭空压价）；
                    #   正权重源 → 今日也无量才 0（v2.9.12 补：不再给幻影正分，
                    #   如彩票流水一周没人下注反而 +0.1）；今日有量保留小正分（M4）。
                    score = 0.0
                if night and score < 0:
                    score = 0.0  # v1.9.1 夜间安静区（2-7点）：没人发言不压价
                fund += w * score
                wsum += abs(w)   # 负权重（高利贷）按绝对值进分母，反向源不稀释归一
            if wsum > 0:
                fund /= wsum
            out[code] = fund
        return out

    def _baseline_delta_7d(self, metric_samples: dict[str, dict]) -> tuple[dict[str, float], dict[str, float]]:
        """近 3 日同时刻累计均值基线（v2.9.8 联动 2.0 校准）。

        v2.9.7 的「昨日同一小时 1h 增量」在真实数据上太抖：指标稀疏（1h 增量大多为 0），
        昨天那个小时恰好有人玩就把今天判成最大冷清（PLB 的 −1.08 全来自刮刮乐单源冤案）。
        v2.9.8 改为：今日累计 ÷ 近 3 日同时刻累计均值（缺日自动跳过；全缺 → 0 走中性冷启动）。
        返回值第二项固定为空 dict（增量即今日累计，_fundamentals 里 phv 恒 0），保留签名兼容。
        """
        now = datetime.now(CST)
        hour2 = f"{now.hour:02d}"
        _hist_days = [(now - timedelta(days=i)).strftime("%Y-%m-%d") for i in (1, 2, 3)]
        base: dict[str, float] = {}
        samples = self._state.get("samples", {})
        for k, sd in metric_samples.items():
            days = sd.get("days", {})
            vals = []
            for d in _hist_days:
                v = (days.get(d) or {}).get(hour2)
                try:
                    vals.append(float(v))
                except (TypeError, ValueError):
                    continue
            if not vals:
                base[k] = 0.0        # 无历史参考 → 中性（_fundamentals 走冷启动）
                continue
            _m = sum(vals) / len(vals)
            base[k] = (100.0 * math.log1p(max(0.0, _m))) if k in MONEY_KEYS else _m
        return base, {}

    # ---------- tick 定价 ----------
    @staticmethod
    def _period_index(now: datetime) -> int:
        """周期索引（v1.5）：全天 2 小时一滚，0:00 起共 12 期（0-11）。"""
        return (now.hour * 60 + now.minute) // 120

    def _period_key(self, now: datetime | None = None) -> str:
        # v2.9.11 期索引补零：此键要参与 `k < cur_key` 的字符串比较做跨期清账。
        # 不补零时 '#9' > '#10'（'9' > '1'），期 9 的旧账在期 10/11 永远清不掉，
        # 要等跨日（日期前缀变了）才被清；届时清理循环变量 k 指向的旧期键又与
        # 实际残留键错位 → del 打空 → 抛错冻结行情卡。补零后同日内字符串序 == 数值序。
        now = now or datetime.now(CST)
        return f"{now.strftime('%Y-%m-%d')}#{self._period_index(now):02d}"

    def _tick_locked(self) -> tuple[bool, dict[str, str]]:
        """推进一步定价，返回 (有动作, 资讯要点)。"""
        now = datetime.now(CST)
        sample = self._sample_metrics_locked()
        self._record_sample(sample)
        base, prev = self._baseline_delta_7d(self._state.get("samples", {}))
        d_u = float(len(self._wallet()) or 1)   # 参与人数：信号按人头摊薄（人少时单次行为更有效）
        night = 2 <= now.hour < 7  # v1.9.1 夜间安静区：2:00-7:00
        fund = self._fundamentals(sample, prev, base, d_u, night)
        news: dict[str, str] = {}
        alerts: dict[str, str] = {}
        fund_snap: dict[str, float] = {}   # v2.11.0 活跃度闭环：逐股基本面分留档（热度播报挑最热/最冷）
        # v1.8 资金面：按 2 小时周期记账，期切换自动清账（老账不永久扭曲盘面）
        cur_key = self._period_key(now)
        flow_all = self._state.setdefault("flow", {})
        for k in [k for k in flow_all if k < cur_key]:
            del flow_all[k]
        flow_map = flow_all.setdefault(cur_key, {})
        flow_user_all = self._state.setdefault("flow_user", {})   # v2.9.0 个人换手账本（与 flow 同周期清账）
        for k in [k for k in flow_user_all if k < cur_key]:
            del flow_user_all[k]
        flow_user_map = flow_user_all.setdefault(cur_key, {})
        applied_all = self._state.setdefault("flow_applied", {})
        for k in [k for k in applied_all if k < cur_key]:
            del applied_all[k]
        applied = applied_all.setdefault(cur_key, {})
        for code, cfg in STOCKS.items():
            s = self._st(code)
            if float(s.get("delist_until", 0) or 0) > time.time():
                continue          # v2.1.0 停牌重组中：价格冻结
            price = float(s["price"])
            f = fund.get(code, 0.0)
            fund_snap[code] = float(f)   # v2.11.0 留档本 tick 基本面分
            mom = float(s.get("mom", 0.0))
            center = float(s.get("center", INIT_PRICE))
            vol = float(cfg.get("vol", 1.0))
            noise = random.gauss(0, NOISE_SIGMA * vol)   # v2.0 噪声按股性放大
            _rel = (price - center) / max(center, PRICE_FLOOR)
            rev = -_rel * 0.004  # v1.9.1 D档回归再减 1/3，偏离 20% 不急回
            if _rel < -LOW_PULL_TRIGGER:   # v2.9.8 联动 2.0：深度低估 → 皮筋加强，帮被错杀的低价回中枢
                rev *= LOW_PULL_BOOST
            # v1.8 三驱动：行为 0.55 + 资金面(见下) + 噪声 0.141 + 回归 0.10 + 动量 0.05（v2.2.0 按 5min tick 折算）
            move = 0.55 * f * SENSE / 72.0 + 0.05 * mom / 72.0 + 0.141 * noise + 0.10 * rev * 18
            move += float(s.get("regime", 0) or 0) * REGIME_DRIFT * vol   # v2.3.0 剧情推力（秘而不宣）
            if int(s.get("echo_left", 0) or 0) > 0 and random.random() < ECHO_CHANCE:
                move += int(s.get("echo_dir", 0) or 0) * random.uniform(*ECHO_PCT)   # v2.3.0 事件余波（静默，走同一封顶管线）
            mv = 0.0
            for _h in self._state.get("holdings", {}).values():
                _p = (_h.get("pos", {}) or {}).get(code)
                if _p and int(_p.get("sh", 0) or 0) > 0:
                    mv += int(_p["sh"]) * price
            mv = max(mv, FLOW_MV_FLOOR)
            # v2.9.0 边际递减：每人单股单期净额按市值换手封顶（FLOW_CAP_X），
            # 群体净换手走饱和曲线（FLOW_A 半饱和）——小单有手感、巨鲸钝化、堆钱不出活
            _fu = flow_user_map.get(code, {}) or {}
            _x = 0.0
            for _net in _fu.values():
                _x += max(-FLOW_CAP_X, min(FLOW_CAP_X, float(_net or 0) / mv))
            if _x < 0:
                _x *= FLOW_SELL_MULT   # v2.0 卖出冲击 ×1.3（基数已饱和化）
            raw_flow = _x
            # v2.0 负向封顶 ×1.3：让"卖出冲击"真正生效（原正负同为 ±10%，1.3 倍被吃平）
            # v2.7.0 冲击骰子：市场消化系数每期每股掷一次（0.3~1.1，12% 概率大单被吞 ×0.1），
            # 周期内稳定不重掷——投币不再"必涨满"，确定性套利变风险投资
            _dm = self._state.setdefault("flow_dice", {})
            # v2.9.11 修笔误：原 `del _dm[k]` 误用了外层清账循环变量 k（旧期键），
            # 而非本循环变量 _k。残留骰子键既清不掉，还会 KeyError / UnboundLocalError
            # 直接把整个调度循环打挂（每 tick 复现，行情卡「数据时点」就此冻结）。
            for _k in [k for k in _dm if k < cur_key]:
                del _dm[_k]
            _dice = float(_dm.setdefault(cur_key, {}).get(code, 0) or 0)
            if _dice <= 0:
                _dice = random.uniform(0.3, 1.1)
                if random.random() < 0.12:
                    _dice *= 0.1
                _dm[cur_key][code] = _dice
            _sat = abs(raw_flow) / (abs(raw_flow) + FLOW_A) if raw_flow else 0.0   # v2.9.0 饱和曲线
            _sgn = 1.0 if raw_flow > 0 else (-1.0 if raw_flow < 0 else 0.0)
            target_flow = _sgn * FLOW_CAP * _sat * _dice
            delta_flow = target_flow - float(applied.get(code, 0.0) or 0)
            move += delta_flow
            applied[code] = target_flow
            move = max(-TICK_CAP, min(TICK_CAP, move))  # v2.0 单 tick 封顶 ±12%
            _limit = float(cfg.get("limit", LIMIT_PCT))   # v2.0 每股独立涨跌停
            new_price = max(PRICE_FLOOR, price * (1 + move))
            day_open = float(s.get("open", new_price))
            drift_from_open = (new_price - day_open) / max(day_open, PRICE_FLOOR)
            if abs(drift_from_open) > _limit:
                new_price = day_open * (1 + _limit * (1 if drift_from_open > 0 else -1))
            moves = list(s.get("moves", []))[-2:]
            moves.append(new_price / max(price, PRICE_FLOOR) - 1)
            burst = sum(moves[-3:])
            if abs(burst) > ANOMALY_PCT:
                new_price = day_open * (1 + _limit * (1 if new_price > day_open else -1))
                moves = moves[-2:] + [(new_price / max(price, PRICE_FLOOR) - 1) * 0.2]
                alerts[code] = f"⚠️ {code} 三 tick 波动 {burst * 100:+.1f}%，已自动降温（管理员请留意刷盘）"
            s["price"] = round(new_price, 2)
            s["day_low"] = min(float(s.get("day_low", s["price"]) or s["price"]), float(s["price"]))   # v2.1.1 逐 tick 记当日最低
            if int(s.get("echo_left", 0) or 0) > 0:
                s["echo_left"] = int(s["echo_left"]) - 1
            if int(s.get("regime_left", 0) or 0) > 0:
                s["regime_left"] = int(s["regime_left"]) - 1
                if int(s["regime_left"]) == 0:
                    s["regime"] = 0   # v2.3.0 剧情自然杀青，无播报
            _hist = list(s.get("hist") or [])
            _hist.append(s["price"])
            s["hist"] = _hist[-HIST_MAX:]   # v2.0 走势条数据
            s["moves"] = moves[-3:]
            s["mom"] = 0.7 * mom + 0.3 * max(-TICK_CAP, min(TICK_CAP, sum(moves[-3:]) * 72))
            s["alert_n"] = int(s.get("alert_n", 0) or 0) + (1 if code in alerts else 0)
            # v2.9.3 面值退市倒计时：按 5min tick 连续计数（24 tick = 2h），跌破即起算、站回即清零
            # v2.9.10 移除深低估豁免：判定只看绝对面值线，不再与中枢挂钩（见常量区说明）
            if float(s["price"]) < FLOOR_LINE:
                s["floor_streak"] = int(s.get("floor_streak", 0) or 0) + 1
                _fs = int(s["floor_streak"])
                if _fs >= FLOOR_STREAK_MAX:
                    _dn: list[str] = []
                    self._delist_liquidate_locked(
                        code, s, _dn,
                        reason=f"连续 {FLOOR_STREAK_MAX} 个 5 分钟（2h）低于面值线 {FLOOR_LINE:.0f}")
                    if _dn:
                        alerts[code] = _dn[0]
                    continue          # 已退市：本 tick 不再出资讯
                if _fs in (12, 18):   # 半程/四分之三程各提醒一次，不刷屏
                    alerts[code] = (f"🪙 {code} {cfg_name(code)} 面值倒计时：已连压 {_fs}/{FLOOR_STREAK_MAX} 次"
                                    f"（5 分钟/次，共 2h；面值线 {FLOOR_LINE:.0f}）——现在卖出全价，拖到清算只拿半价喵")
            elif int(s.get("floor_streak", 0) or 0) > 0:
                s["floor_streak"] = 0
            if abs(f) > 0.15:
                main_src, main_w = cfg["sources"][0] if cfg["sources"] else ("-", 0)
                deltas_txt = "; ".join(
                    f"{METRIC_LABEL.get(mm, mm)} 今日 {sample.get(mm, 0):.0f}"
                    for mm, _ in cfg.get("sources", [])[:2]
                )
                news[code] = f"{code} {cfg['name']}：{deltas_txt} → 基本面 {f:+.2f}"
            if abs(target_flow) >= 0.005:
                # v2.7.0 资讯暗话：藏精确数字与净额，只给方向感（档位词）
                _ta = abs(target_flow)
                _wd = ("微温" if target_flow > 0 else "微凉") if _ta < 0.02 else (
                    ("偏暖" if target_flow > 0 else "偏冷") if _ta < 0.05
                    else ("汹涌" if target_flow > 0 else "恐慌"))
                ft = f"资金面{_wd}"
                news[code] = (news[code] + "；" + ft) if code in news else f"{code} {cfg['name']}：{ft}"
        alerts.update(self._maybe_events_locked(now))
        self._state["last_news"] = {          # v2.0.3 存档本轮归因，供「资讯」按需查询
            "period": cur_key,
            "ts": time.time(),
            "news": dict(news),
            "alerts": dict(alerts),
            "fund": dict(fund_snap),   # v2.11.0 活跃度闭环：基本面分留档
        }
        return True, {"news": news, "alerts": alerts}

    def _settle_day_locked(self) -> list[str]:
        """方案 A：每 2 小时周期结算（period-key 变化时触发）。

        - 结算上一周期：prev_close 落定、open（本周期开盘价）重置、center 收敛、动量跨期清零
        - 全天 12 期滚动；每天首期（0:00-2:00）结算时记日结收盘（v1.5 起 T+ 解冻与盘外冻结已删）
        """
        now = datetime.now(CST)
        hm = now.hour * 60 + now.minute
        pk = self._period_key(now)
        if self._state.get("last_settle_period") == pk:
            return []
        # 周期切换（含跨日开盘）：上一周期收盘 → 新周期开盘
        notes: list[str] = []
        now_ts = time.time()
        for code in STOCKS:
            st_ = self._st(code)
            # v2.1.0 停牌重组：到期重生
            if float(st_.get("delist_until", 0) or 0) > 0:
                if now_ts >= float(st_["delist_until"]):
                    st_["price"] = float(INIT_PRICE)
                    st_["open"] = float(INIT_PRICE)
                    st_["prev_close"] = float(INIT_PRICE)
                    st_["center"] = float(INIT_PRICE)
                    st_["hist"] = [float(INIT_PRICE)]
                    st_["moves"] = []
                    st_["mom"] = 0.0
                    st_["day_low"] = float(INIT_PRICE)
                    st_["prev_day_low"] = float(INIT_PRICE)
                    st_["delist_until"] = 0
                    st_["regime"] = 0
                    st_["regime_left"] = 0
                    st_["echo_dir"] = 0
                    st_["echo_left"] = 0
                    st_["floor_streak"] = 0
                    notes.append(f"🎊 {code} {cfg_name(code)} 重组上市！价格重置回 10 堡贝，往事清零喵")
                else:
                    continue          # 停牌中：价格冻结，不参与周期结算
            st_["prev_close"] = st_["price"]
            st_["open"] = st_["price"]   # 本周期开盘价 = 上期收盘；涨跌停基准随之重置
            st_["center"] = round((1 - CENTER_ALPHA) * st_["center"] + CENTER_ALPHA * st_["price"], 2)
            st_["mom"] = 0.0             # 动量跨期清零，防隔期惯性
            st_["moves"] = []
            # v2.9.2 退市机制：只留「面值线」一条（破位线/恐慌压制已下线）
            if self._period_index(now) == 0:
                st_["prev_day_low"] = float(st_.get("day_low", st_["price"]) or st_["price"])
                st_["day_low"] = float(st_["price"])
            else:
                st_["day_low"] = min(float(st_.get("day_low", st_["price"]) or st_["price"]), float(st_["price"]))
            # v2.9.3 面值退市倒计时已改到 _tick_locked 内按 5min tick 计数（结算每 2h 才跑一次，
            # 旧口径会让「24 期」实际变成 48 小时）
            # v2.3.0 剧情机制：震荡股每周期掷骰开新剧（秘而不宣，玩家看 K 线猜）
            if not int(st_.get("regime", 0) or 0) and random.random() < REGIME_CHANCE:
                if random.random() < 0.5:
                    st_["regime"] = 1
                    st_["regime_left"] = random.randint(*REGIME_BULL_TICKS)
                else:
                    st_["regime"] = -1
                    st_["regime_left"] = REGIME_BEAR_TICKS
        # v1.5：无 T+ 解冻（买入即卖）；每天首期结算时记日结收盘（= 上一日收盘价）
        if self._period_index(now) == 0:
            dc = self._state.setdefault("day_closes", {})
            day = now.strftime("%Y-%m-%d")
            if day not in dc:
                dc[day] = {c: self._st(c)["prev_close"] for c in STOCKS}
                while len(dc) > 30:
                    del dc[sorted(dc.keys())[0]]
        self._state["last_settle_period"] = pk
        self._schedule_events_locked(now)
        return notes
    # ---------- v2.1.0 退市清算 ----------
    def _delist_liquidate_locked(self, code: str, st_: dict[str, Any], notes: list[str], reason: str = "") -> None:
        """持仓按现价 5 折强制退款（menu 账本），股票停牌 2h 后 10 堡贝重组上市。"""
        refund_px = round(max(PRICE_FLOOR, float(st_["price"]) * DELIST_REFUND), 2)
        pay_total, pay_users = 0, 0
        for uid, h in (self._state.get("holdings", {}) or {}).items():
            pos = (h.get("pos") or {}).get(code)
            if not pos or int(pos.get("sh", 0) or 0) <= 0:
                continue
            sh_n = int(pos["sh"])
            pay = int(sh_n * refund_px + 0.999)
            self.add_coins(uid, h.get("name") or uid, pay)
            pay_total += pay
            pay_users += 1
            pos["sh"] = 0
            pos["avg"] = 0.0
            pos["today"] = 0
        notes.append(
            f"🌪️ {code} {cfg_name(code)} {reason or f'连续 {FLOOR_STREAK_MAX} 个 5 分钟低于面值线 {FLOOR_LINE:.0f}'}，正式退市！"
            f"清算价 {refund_px:.2f}（现价 5 折），已向 {pay_users} 位股东退款 {pay_total} 堡贝。"
            f"2 小时后重组上市喵")
        st_["price"] = float(INIT_PRICE)
        st_["open"] = float(INIT_PRICE)
        st_["prev_close"] = float(INIT_PRICE)
        st_["center"] = float(INIT_PRICE)
        st_["hist"] = [float(INIT_PRICE)]
        st_["moves"] = []
        st_["mom"] = 0.0
        st_["day_low"] = float(INIT_PRICE)
        st_["prev_day_low"] = float(INIT_PRICE)
        st_["delist_until"] = time.time() + DELIST_PAUSE_SEC
        st_["regime"] = 0
        st_["regime_left"] = 0
        st_["echo_dir"] = 0
        st_["echo_left"] = 0
        st_["floor_streak"] = 0

    # ---------- 股东分红（v1.7） ----------
    def _maybe_dividend_locked(self) -> list[str] | None:
        """每日 22:05（错过则 0:00-5:00 补）把金库按 XBK 持股发完。需持有全部锁。"""
        now = datetime.now(CST)
        hm = now.hour * 60 + now.minute
        if hm >= 22 * 60 + 5:
            target = now.strftime("%Y-%m-%d")
        elif hm <= 4 * 60 + 59:
            target = (now - timedelta(days=1)).strftime("%Y-%m-%d")
        else:
            return None
        paid = self._state.setdefault("div_paid_dates", [])
        if target in paid:
            return None
        return self._distribute_dividend_locked(target)

    def _distribute_dividend_locked(self, for_date: str) -> list[str] | None:
        """金库 = 累计手续费（见 _trade）；负数 = 亏损日，股东按持股共担。"""
        pool = int(self._state.get("div_pool", 0) or 0)
        holders: list[tuple[str, str, int]] = []
        for uid, h in (self._state.get("holdings", {}) or {}).items():
            pos = (h.get("pos") or {}).get("XBK") or {}
            sh = int(pos.get("sh", 0) or 0)
            if sh > 0:
                holders.append((uid, h.get("name", "?"), sh))
        self._state.setdefault("div_paid_dates", []).append(for_date)
        while len(self._state["div_paid_dates"]) > 7:
            del self._state["div_paid_dates"][0]
        self._state["div_last"] = {"for_date": for_date, "per_share": 0.0, "pool": pool}
        if pool < 0 and holders:
            # 亏损日（无上限）：金库赤字按持股比例向股东反向收取，扣到余额清零为止
            deficit = -pool
            total_sh = sum(sh for _u, _n, sh in holders)
            per_share = deficit / total_sh
            collected = 0
            by_uid: dict[str, int] = {}
            top: list[tuple[str, int, int]] = []
            for uid, name, sh in sorted(holders, key=lambda t: -t[2]):
                amt = int(deficit * sh / total_sh)
                if amt <= 0:
                    continue
                cut = min(amt, self.get_balance(uid))
                if cut > 0:
                    self.add_coins(uid, name, -cut)
                    collected += cut
                    by_uid[uid] = -cut
                    top.append((name, sh, cut))
            self._state["div_pool"] = pool + collected  # 收上来的抵赤字，没收满的部分继续滚负
            day = datetime.now(CST).strftime("%Y-%m-%d")
            self._state["div_today"] = {"day": day, "by": by_uid}
            self._state["div_last"] = {"for_date": for_date, "per_share": -round(per_share, 2), "pool": pool}
            lines = [
                "📉 蟹堡王亏损日 —— 金库赤字 %d 堡贝（%s 刮卡/抽水倒挂）" % (deficit, for_date),
                "· 总股本 %d 股，每股摊 %.2f（扣到余额清零为止，无上限）" % (total_sh, per_share),
            ]
            for name, sh, cut in top[:5]:
                lines.append(f"· 💀 {name} 持 {sh} 股 → 扣 {cut}")
            lines.append("（本店亏损，股东共担风险；刮卡回血靠你们喵）")
            return lines
        if pool <= 0 or not holders:
            return None  # 没金库或没股东：滚存
        total_sh = sum(sh for _u, _n, sh in holders)
        per_share = pool / total_sh
        given = 0
        by_uid: dict[str, int] = {}
        top: list[tuple[str, int, int]] = []
        for uid, name, sh in sorted(holders, key=lambda t: -t[2]):
            amt = int(pool * sh / total_sh)
            if amt > 0:
                self.add_coins(uid, name, amt)
                by_uid[uid] = by_uid.get(uid, 0) + amt
                given += amt
                top.append((name, sh, amt))
        self._state["div_pool"] = pool - given
        day = datetime.now(CST).strftime("%Y-%m-%d")
        self._state["div_today"] = {"day": day, "by": by_uid}
        self._state["div_last"] = {"for_date": for_date, "per_share": round(per_share, 2), "pool": pool}
        lines = [
            "💰 蟹堡王分红日 —— 今日金库 %d 堡贝（%s 抽水）" % (pool, for_date),
            "· 总股本 %d 股，每股派 %.2f" % (total_sh, per_share),
        ]
        for name, sh, amt in top[:5]:
            lines.append(f"· 🏆 {name} 持 {sh} 股 → 分得 {amt}")
        lines.append("（没持股的眼馋的，买 XBK 做蟹堡王股东喵）")
        return lines


    def _schedule_events_locked(self, now: datetime) -> None:
        """周期切换时排本周期的事件槽：白天（8-24 点）2 场、深夜 1 场，随机分钟触发。"""
        pk = self._period_key(now)
        n = 2 if 8 <= now.hour < 24 else 1
        slots = sorted({random.randint(0, 119) for _ in range(n)})
        sched = self._state.setdefault("events_sched", {})
        sched[pk] = {"slots": slots, "fired": []}
        for k in sorted(sched.keys())[:-3]:
            del sched[k]

    def _maybe_events_locked(self, now: datetime) -> dict[str, str]:
        """到点的定时事件 + 小概率自发性事件，返回插播文案。"""
        out: dict[str, str] = {}
        sched = self._state.setdefault("events_sched", {})
        slot = sched.get(self._period_key(now))
        minute = (now.hour % 2) * 60 + now.minute
        if slot:
            for i, m in enumerate(slot["slots"]):
                if minute >= m and i not in slot["fired"]:
                    slot["fired"].append(i)
                    hit = self._fire_event_locked()
                    if hit:
                        out.update(hit)
                        break
        if random.random() < EVENT_CHANCE:
            hit = self._fire_event_locked()
            if hit:
                out.update(hit)
        if random.random() < BLACK_SWAN_CHANCE:      # v2.0 黑天鹅
            swan = self._fire_black_swan_locked()
            if swan:
                out.update(swan)
        return out

    def _fire_event_locked(self) -> dict[str, str] | None:
        """随机挑一只不在冷却的股触发事件，幅度 ±8%~28%，受涨跌停约束。"""
        now_ts = time.time()
        cands = [c for c in STOCKS if now_ts >= float(self._st(c).get("event_until", 0) or 0)]
        if not cands:
            return None
        code = random.choice(cands)
        s = self._st(code)
        sign, text = random.choice(EVENTS[code])   # 池子是 (方向, 文案)
        pct = random.uniform(EVENT_MIN_PCT, EVENT_MAX_PCT) * (1 if sign > 0 else -1)
        price = float(s["price"])
        new_price = max(PRICE_FLOOR, price * (1 + pct))
        open_ = float(s.get("open", new_price))
        _lim = float(STOCKS[code].get("limit", LIMIT_PCT))
        if abs(new_price - open_) / max(open_, PRICE_FLOOR) > _lim:
            new_price = open_ * (1 + _lim * (1 if new_price > open_ else -1))
        s["price"] = round(new_price, 2)
        s["day_low"] = min(float(s.get("day_low", s["price"]) or s["price"]), float(s["price"]))   # v2.1.1 事件插针也记当日最低
        s.setdefault("moves", []).append(new_price / max(price, PRICE_FLOOR) - 1)
        s["moves"] = s["moves"][-3:]
        s["event_until"] = now_ts + EVENT_COOLDOWN_SEC
        s["echo_dir"] = 1 if pct > 0 else -1   # v2.3.0 余波方向随事件
        s["echo_left"] = random.randint(*ECHO_TICKS)
        self._push_event_log(now_ts, code, pct, text, "event")
        return {code: f"🎲 {text} —— {code} {cfg_name(code)} {pct * 100:+.1f}%"}

    def _fire_black_swan_locked(self) -> dict[str, str] | None:
        """黑天鹅：极端行情 ±40~60%，突破涨跌停并重设周期基准价。"""
        now_ts = time.time()
        cands = [c for c in STOCKS if now_ts >= float(self._st(c).get("swan_until", 0) or 0)]
        if not cands:
            return None
        code = random.choice(cands)
        s = self._st(code)
        sign, text = random.choice(BLACK_SWAN_EVENTS[code])
        pct = random.uniform(BLACK_SWAN_MIN, BLACK_SWAN_MAX) * (1 if sign > 0 else -1)
        price = float(s["price"])
        new_price = max(PRICE_FLOOR, price * (1 + pct))
        s["price"] = round(new_price, 2)
        s["open"] = round(new_price, 2)   # 重设周期基准，避免下个 tick 被涨跌停拉回
        s["moves"] = []                   # v2.0 清空动量：防下个 tick 触发"异常降温"把天鹅反向放大
        s["mom"] = 0.0
        s["regime"] = 0                    # v2.3.0 天鹅命中：剧情立即杀青
        s["regime_left"] = 0
        s["echo_dir"] = 1 if pct > 0 else -1   # v2.3.0 天鹅余波：同向连环
        s["echo_left"] = random.randint(*ECHO_TICKS)
        _h = list(s.get("hist") or [])
        _h.append(s["price"])
        s["hist"] = _h[-HIST_MAX:]
        s["swan_until"] = now_ts + BLACK_SWAN_COOLDOWN
        self._push_event_log(now_ts, code, pct, text, "swan")
        return {code: f"🌪️ 黑天鹅｜{text} —— {code} {cfg_name(code)} {pct * 100:+.1f}%"}

    # ---------- v2.0.5 随机事件回放 ----------
    EVENT_LOG_MAX = 20

    def _push_event_log(self, ts: float, code: str, pct: float, text: str, kind: str) -> None:
        """把刚触发的事件/黑天鹅写入环形日志（保留最近 EVENT_LOG_MAX 条）。"""
        log = self._state.setdefault("event_log", [])
        log.append({"ts": float(ts), "code": code, "pct": round(float(pct), 4),
                    "text": text, "kind": kind})
        del log[:-self.EVENT_LOG_MAX]

    def _recent_events_lines(self, n: int = 5) -> list[str]:
        """最近 n 次随机事件回放（倒序，最新在上）。"""
        log = [e for e in (self._state.get("event_log") or []) if isinstance(e, dict)][-n:]
        out = [f"── 最近 {len(log)} 次随机事件 ──"] if log else ["── 最近随机事件 ──"]
        if not log:
            out.append("· 暂无记录：事件每股冷却 1 小时，白天约每小时一场，攒几条后再看")
            return out
        up = dn = 0
        for e in reversed(log):
            ts = float(e.get("ts") or 0)
            hhmm = datetime.fromtimestamp(ts, CST).strftime("%H:%M") if ts > 0 else "--:--"
            pct = float(e.get("pct") or 0)
            up += 1 if pct > 0 else 0
            dn += 1 if pct < 0 else 0
            arrow = "▲" if pct > 0 else ("▼" if pct < 0 else "—")
            mark = " 🌪️黑天鹅" if e.get("kind") == "swan" else ""
            out.append(f"· {hhmm} {e.get('code', '')} {arrow}{abs(pct) * 100:.1f}%｜{e.get('text', '')}{mark}")
        out.append(f"· 近 {len(log)} 次：{up} 涨 / {dn} 跌")
        last_ts = float(log[-1].get("ts") or 0)
        if last_ts > 0:
            ago = max(0, int(time.time() - last_ts))
            out.append(f"· 最近一次 {ago // 60} 分钟前｜事件每股冷却 1 小时，白天约每小时一场")
        return out

    async def _news(self, event):
        """v2.0.6 资讯：只回放最近 5 次随机事件（v2.0.6 起去掉涨跌归因段）。

        v1.9.1 曾因「每 tick 主动播报」夜间刷屏而取消主动播报；本版仍按需拉取。
        事件日志由 _push_event_log 在 _fire_event_locked / _fire_black_swan_locked
        触发时写入 _state['event_log']；异动告警仍取自 _state['last_news']['alerts']。
        """
        now = datetime.now(CST)
        lines = [f"📈 小堡股市：资讯（第 {self._period_index(now) + 1}/12 期）"]
        lines.extend(self._recent_events_lines(5))
        ln = self._state.get("last_news") or {}
        for _a in (ln.get("alerts") or {}).values():
            lines.append(f"· {_a}")
        yield event.plain_result("\n".join(lines))

    # ---------- 调度 ----------
    async def _scheduler(self) -> None:
        while True:
            try:
                now_t = time.time()
                next_t = (int(now_t) // TICK_SEC + 1) * TICK_SEC   # v2.3.1 对齐整 5 分钟刻度（:00/:05/:10…，重启自纠偏）
                self._state["next_tick_ts"] = next_t               # 行情卡倒计时用
                await asyncio.sleep(max(next_t - now_t, 0.05))
                async with self._lock:
                    acted, extra = self._tick_locked()
                    self._state["last_tick_ts"] = time.time()   # v2.3.1 卡片「数据时点」= 最近一次价格刷新
                    settle_lines = self._settle_day_locked()
                    div_lines = self._maybe_dividend_locked()
                    self._snapshot_assets_locked()               # 每日首个 tick 记收益率榜基线
                    _lk_ok = self._linkage_bcast_ready_locked()        # 热度播报门槛（卡片，出锁后渲染）
                    _lb_seg = self._daily_leaderboard_lines_locked()   # 每晚 20:00 今日收益榜
                    self._persist_locked()
                # v1.9.1：取消每 tick 的 📰 实时联动播报（夜间刷屏源），只保留 🎲 事件与告警
                out_lines = list(extra.get("alerts", {}).values()) if acted else []
                for _ln in (settle_lines or []):
                    if _ln not in out_lines:
                        out_lines.append(_ln)
                if out_lines:
                    out_lines.insert(0, "📈 小堡股市 盘面插播")
                if _lb_seg:
                    out_lines.extend(_lb_seg)
                # v2.11.1 热度播报改卡片：渲染在**锁外**（html_render 会 await），
                # 与文字播报合成同一条消息（图片在前），仍不新增 _announce 调用点（5 → 5）。
                _lk_card = None
                if _lk_ok:
                    try:
                        _lk_card = await self._render_linkage_card()
                    except Exception as exc:
                        logger.warning("[StockDemo] 热度播报卡片渲染失败，本次跳过：%s", exc)
                if _lk_card:
                    out_lines.insert(0, "📈 小堡股市 · 今日热度（看卡）")
                if out_lines or _lk_card:
                    await self._announce(out_lines, image=_lk_card)
                if div_lines:
                    await self._announce(div_lines)
            except asyncio.CancelledError:
                return
            except Exception as exc:
                logger.warning("[StockDemo] 调度异常：%s", exc, exc_info=True)   # v2.9.11 带栈：下次一屏定位抛错行

    # ---------- v2.11.0 活跃度闭环：把「群友行为 → 股价」的因果显性化 ----------
    def _snapshot_assets_locked(self) -> None:
        """每日首个 tick 快照各持有人总资产，作「今日收益率」基线。"""
        day = datetime.now(CST).strftime("%Y-%m-%d")
        snaps = self._state.setdefault("asset_snap", {})
        if day in snaps:
            return
        cur: dict[str, int] = {}
        for uid, h in (self._state.get("holdings", {}) or {}).items():
            asset = 0
            for code, pos in (h.get("pos") or {}).items():
                asset += int(pos["sh"]) * float(self._st(code)["price"])
            cur[uid] = asset + self.get_balance(uid)
        snaps[day] = cur
        tss = self._state.setdefault("asset_snap_ts", {})   # v2.11.1 记快照时刻
        tss[day] = time.time()
        for k in sorted(snaps.keys())[:-3]:   # 只留最近 3 个日键
            snaps.pop(k, None)
            tss.pop(k, None)

    def _linkage_bcast_ready_locked(self) -> bool:
        """热度播报门槛：非静默 + 距上次 >= 30 分钟 + 有基本面留档。

        通过则立即写 last_lk_bcast_ts（防同 tick / 重启重复播）并返回 True；
        实际卡片渲染由调用方在**锁外**执行（html_render 不可在持锁时 await）。
        设计取向：不设内容门槛（提醒要多），靠节流防刷屏。
        v2.11.1 改卡片：不再拼文本行（原「热度+涨跌」并排自相矛盾），改发联动卡
        （含各股热度条 + 驱动源 + 现价，天然规避「最热却跌」的误导）。
        """
        now = datetime.now(CST)
        if LK_QUIET_HOURS[0] <= now.hour < LK_QUIET_HOURS[1]:
            return False
        _now = time.time()
        try:
            _last = float(self._state.get("last_lk_bcast_ts", 0) or 0)
        except (TypeError, ValueError):
            _last = 0.0
        if _now - _last < LK_BCAST_SEC:
            return False
        fund = (self._state.get("last_news") or {}).get("fund") or {}
        if not fund:
            return False
        self._state["last_lk_bcast_ts"] = _now
        return True

    def _daily_leaderboard_lines_locked(self) -> list[str]:
        """每晚 20:00 今日收益榜。

        口径 = 今日收益率（当前总资产 vs 今日零点快照）；Top 3 + 垫底 1 名，带名字。
        每天只播一次（lb_last_day 记日）。返回 [] 表示本次不播。
        """
        now = datetime.now(CST)
        if now.hour * 60 + now.minute < LB_HOUR * 60:
            return []
        day = now.strftime("%Y-%m-%d")
        if str(self._state.get("lb_last_day", "") or "") == day:
            return []
        # v2.11.1 修复「全员 +0.0%」：若快照与榜单落在同一 tick，基线==当前 ⇒ 收益率恒 0。
        # 要求快照时刻距当前 >= 一个 tick（部署/重启当天首播自动顺延到下一 tick）。
        # 此处 return [] 不写 lb_last_day，故下一 tick 会重试。
        try:
            _st_ts = float((self._state.get("asset_snap_ts") or {}).get(day, 0) or 0)
        except (TypeError, ValueError):
            _st_ts = 0.0
        if _st_ts <= 0 or (time.time() - _st_ts) < TICK_SEC:
            return []
        snap = (self._state.get("asset_snap") or {}).get(day) or {}
        rows: list[tuple[str, float, float]] = []
        for uid, h in (self._state.get("holdings", {}) or {}).items():
            asset = 0
            for code, pos in (h.get("pos") or {}).items():
                asset += int(pos["sh"]) * float(self._st(code)["price"])
            total = asset + self.get_balance(uid)
            base = snap.get(uid)
            try:
                base = float(base)
            except (TypeError, ValueError):
                continue
            if base <= 0:
                continue
            rows.append((h.get("name") or uid, (total - base) / base, total - base))
        self._state["lb_last_day"] = day
        if not rows:
            return []
        rows.sort(key=lambda r: -r[1])
        lines = [f"🏆 小堡股市 · 今日收益榜（{now.strftime('%m-%d')}）"]
        medals = ["🥇", "🥈", "🥉"]
        for i, (nm, ret, dlt) in enumerate(rows[:3]):
            lines.append(f"{medals[i]} {nm} {ret * 100:+.1f}%（{dlt:+.0f}）")
        if len(rows) >= 2:
            nm, ret, dlt = rows[-1]
            lines.append(f"· 今日垫底：{nm} {ret * 100:+.1f}%（{dlt:+.0f}）")
        lines.append("· 完整总榜：小堡股市：排行")
        return lines

    async def _announce(self, lines: list[str], image: str | None = None) -> None:
        """v2.11.1 支持图文混排：image 为卡片 PNG 绝对路径时，图片在前、文字在后同一条消息。"""
        umo = self.umos.get(self.group_id, "")
        if not umo:
            return
        try:
            from astrbot.core.message.components import Plain, Image
            from astrbot.core.message.message_event_result import MessageChain
            comps = []
            if image:
                try:
                    comps.append(Image.fromFileSystem(image))
                except Exception as exc:
                    logger.warning("[StockDemo] 播报卡片附加失败，降级纯文本：%s", exc)
                    image = None
            if lines:
                comps.append(Plain("\n".join(lines)))
            if not comps:
                return
            # v1.8.1 修复：裸 list 会被 send_message 当 MessageChain 用（'list' has no attribute 'chain'），
            # 必须包 MessageChain——历史上所有群播报（事件/讯息/分红卡）都因此静默失败
            await self.context.send_message(umo, MessageChain(comps))
            logger.info("[StockDemo] 播报已发送 %d 行%s到 %s：%s",
                        len(lines), ("+卡片 " if image else " "), self.group_id,
                        (lines[0] if lines else "仅卡片"))
        except Exception as exc:
            logger.warning("[StockDemo] 播报失败：%s", exc)

    async def initialize(self) -> None:
        # 启动即起调度循环（首轮 tick 在 TICK_SEC 后，启动瞬间不补价）
        self._sched_task = asyncio.create_task(self._scheduler())

    async def terminate(self) -> None:
        task = getattr(self, "_sched_task", None)
        if task:
            task.cancel()

    # ---------- 话题热度（v2.8.0：群聊关键词计入 PLB/SMW/HZB 联动）----------
    async def _kw_record(self, event: AstrMessageEvent) -> None:
        """非本插件命令的群聊按 KW_LEXICON 计话题热度；每人每话题每日钳制 KW_CLAMP 条。
        v2.9.6：当日累计持久化进 _state（重启不再清零）；独立 asyncio 锁，不与 menu 锁嵌套成环。"""
        try:
            if not str(event.get_group_id() or "").strip():
                return   # 只统计群聊，私聊不计话题
            uid = str(event.get_sender_id() or "").strip()
            text = event.get_message_str() or ""
            if not uid or not text:
                return
            day = datetime.now(CST).strftime("%Y-%m-%d")
            _hit = False
            async with self._kw_lock:
                if len(self._kw_day) > 1:   # 跨日清理旧账
                    for _d in [k for k in self._kw_day if k != day]:
                        self._kw_day.pop(_d, None)
                        self._kw_user.pop(_d, None)
                dd = self._kw_day.setdefault(day, {})
                uub = self._kw_user.setdefault(day, {}).setdefault(uid, {})
                for kw, words in KW_LEXICON.items():
                    if uub.get(kw, 0) >= KW_CLAMP:
                        continue
                    if any(w in text for w in words):
                        uub[kw] = uub.get(kw, 0) + 1
                        dd[kw] = dd.get(kw, 0) + 1
                        _hit = True
            if _hit:
                # v2.9.6 节流落盘（话题计数已并入 _state；命中后最多 30 秒写一次盘）
                _now_t = time.time()
                if _now_t - float(getattr(self, "_kw_save_ts", 0.0) or 0.0) >= 30.0:
                    self._kw_save_ts = _now_t
                    self._persist_locked()
        except Exception as exc:
            logger.debug("[StockDemo] 话题统计跳过：%s", exc)

    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE, priority=32000)
    async def on_kw_message(self, event: AstrMessageEvent):
        """v2.9.12 话题计数前置：全场最高优先级，只计数、绝不 stop_event。

        原先计数挂在 on_message(26000)，会被更高优处理器的 stop_event 截胡：
        「小堡相册」由 guard(priority=30000) 精确匹配发图后 stop_event，消息永远
        到不了 26000 的计数器 → kw_album 恒零，玩法越热门显得越冷清。
        提到 32000（guard 之上）先计数再放行，后续谁截胡都影响不到计数；
        同时消除与 bomb/lottery(26000) 同级 tie 的派发顺序不确定性。"""
        await self._kw_record(event)

    # ---------- 命令 ----------
    @filter.event_message_type(
        filter.EventMessageType.GROUP_MESSAGE | filter.EventMessageType.PRIVATE_MESSAGE,
        priority=26000,
    )
    async def on_message(self, event: AstrMessageEvent):
        raw = (event.get_message_str() or "").strip().lstrip("/／").strip()
        sub, arg = "", ""
        for prefix in CMD_PREFIXES:
            if raw.startswith(prefix):
                body = raw[len(prefix):].strip()
                sub, _, rest = body.partition(" ")
                sub, arg = sub.strip(), rest.strip()
                break
        if not sub:
            # 裸代码玩法：买 XBK 200 / 卖 SMW 100（省总名前缀）
            for kw in ("买 ", "卖 "):
                if raw.startswith(kw):
                    parts = raw.split()
                    if len(parts) == 3 and parts[1].upper() in STOCKS and parts[2].isdigit():
                        sub = kw.strip()
                        arg = parts[1].upper() + " " + parts[2]
                        break
        if not sub:
            return   # 话题计数已前置到 on_kw_message(32000)，此处不再重复计数
        try:
            gid = str(event.get_group_id() or "").strip()
        except Exception:
            gid = ""
        # 只在本群响应：私聊、别群一律静默（防别处误触发把行情搞乱）
        if gid != self.group_id:
            return
        self.umos[self.group_id] = event.unified_msg_origin
        event.stop_event()
        uid = str(event.get_sender_id() or "").strip()
        name = str(event.get_sender_name() or "")[:20]

        if sub == "规则":
            yield event.plain_result(RULES_TEXT)
        elif sub == "行情":
            async for line in self._quote(event, arg):
                yield line
        elif sub in ("买", "卖"):
            async for line in self._trade(event, uid, name, sub, arg):
                yield line
        elif sub == "持仓":
            async for line in self._portfolio(event, uid, name):
                yield line
        elif sub == "资讯":
            async for line in self._news(event):
                yield line
        elif sub == "联动":
            async for line in self._linkage(event):
                yield line
        elif sub == "排行":
            async for line in self._leaderboard(event, arg):
                yield line
        else:
            yield event.plain_result(
                "没认出这个说法喵。可用：行情｜资讯｜联动｜买 XBK 200｜卖 XBK 100｜持仓｜排行｜排行 今日｜规则"
            )

    # ---------- 行情 ----------
    def _data_point_txt(self) -> str:
        """v2.3.1：数据时点 = 最近一次价格刷新时刻（无记录则用当前时间）。"""
        ts = float(self._state.get("last_tick_ts", 0) or 0)
        if ts <= 0:
            ts = time.time()
        return datetime.fromtimestamp(ts, CST).strftime("%Y-%m-%d %H:%M:%S")

    def _tick_countdown_line(self) -> str:
        """v1.8.6：距下次价格刷新（tick）的倒计时。"""
        nxt = float(self._state.get("next_tick_ts", 0) or 0)
        if nxt <= 0:
            return "· ⏳ 下次价格刷新：马上（约几分钟内）"
        return "· ⏳ 下次价格刷新：" + datetime.fromtimestamp(nxt, CST).strftime("%Y-%m-%d %H:%M:%S")

    def _quote_line(self, code: str) -> str:
        cfg = STOCKS[code]
        s = self._st(code)
        px = float(s["price"])
        opn = float(s.get("open", px))
        pct = (px / opn - 1) * 100 if opn > 0 else 0.0
        moves = list(s.get("moves", []) or [])
        tick_pct = moves[-1] * 100 if moves else 0.0   # v1.8.7：上一班 tick（约5分钟前→现在）
        arrow = "🔴" if pct >= 0 else "🟢"
        return f"{arrow} {code} {cfg['name']}  {px:.2f} 堡贝  {tick_pct:+.2f}% / {pct:+.2f}%"

    # ---------- 行情卡（v2.0.7：HTML→PNG；失败由调用方回退纯文字） ----------
    _QC_COLORS = {"XBK": "#D93B3B", "BBB": "#E8A33D", "PLB": "#8E5BD9",
                  "SMW": "#2E9BD6", "HZB": "#3FA76A", "CCB": "#D96A9B"}
    _QC_UP = "#D93B3B"
    _QC_DOWN = "#16A34A"
    _QC_TMPL = """<!DOCTYPE html><html><head><meta charset='utf-8'><style>
*{margin:0;padding:0;box-sizing:border-box}
body{width:720px;background:#FBF3E4;padding:4px;font-family:"Noto Sans CJK SC","PingFang SC","Microsoft YaHei",sans-serif}
.card{background:#fff;border-radius:14px;overflow:hidden;border:2px solid #EFE0C6;box-shadow:0 6px 18px rgba(107,61,20,.10)}
.hd{background:linear-gradient(120deg,#6B3D14,#96622B);color:#fff;padding:10px 12px;display:flex;justify-content:space-between;align-items:center}
.hd .t{font-size:20px;font-weight:800}
.hd .s{font-size:12px;opacity:.92}
.lg{padding:8px 12px 0;display:flex;flex-wrap:wrap;gap:5px 14px}
.lg .li{font-size:11.5px;color:#5C4326;display:inline-flex;align-items:center}
.lg .li i{width:11px;height:11px;border-radius:3px;display:inline-block;margin-right:5px}
.chart{padding:2px 10px 0}
.list{padding:2px 12px 2px}
.thead{display:flex;font-size:11px;color:#A98A63;padding:6px 0;border-top:1px solid #EFE0C6}
.thead .c1{width:58px}.thead .c2{flex:1}.thead .c3{width:84px}.thead .c9{width:40px;text-align:center}
.thead .c4{width:60px;text-align:right}.thead .c5{width:70px;text-align:right}.thead .c6{width:70px;text-align:right}
.thead .c7{width:58px;text-align:right}.thead .c8{width:62px;text-align:right}
.row{display:flex;align-items:center;padding:7px 0;border-top:1px solid #F3EAD8}
.row .code{width:58px;font-weight:800;font-size:14px;color:#6B3D14;display:flex;align-items:center}
.row .code i{width:9px;height:9px;border-radius:2px;display:inline-block;margin-right:5px}
.row .nm{flex:1;font-size:13px;color:#4A3320}
.row .tg{width:40px;text-align:center;font-weight:800;font-size:12px}
.row .sp{width:84px}
.row .px{width:60px;text-align:right;font-weight:800;font-size:14.5px;color:#2E2013}
.row .pc{width:70px;text-align:right;font-weight:800;font-size:13px}
.row .yh{width:58px;text-align:right;font-size:13px;color:#6B4F2E}
.row .dl{width:62px;text-align:right;font-weight:800;font-size:13px}
.ft{padding:6px 12px 2px;font-size:11px;color:#A98A63;display:flex;justify-content:space-between;border-top:1px solid #EFE0C6}
.dnote{padding:0 12px 9px;font-size:11px;color:#A98A63;line-height:1.5}
</style></head><body><div class='card'>
<div class='hd'><div class='t'>📈 小堡股市 · 行情</div><div class='s'>24h 交易中 · 每 2 小时一滚</div></div>
<div class='lg'>__LEGEND__</div>
<div class='chart'><svg width='688' height='220'>__CHART__</svg></div>
<div class='list'>
  <div class='thead'><div class='c1'>代码</div><div class='c2'>名称</div><div class='c9'>股性</div><div class='c3'>近18期走势</div><div class='c4'>现价</div><div class='c5'>近5分钟</div><div class='c6'>本周期</div><div class='c7'>面值线</div><div class='c8'>退市进度</div></div>
  __ROWS__
</div>
<div class='ft'><div>__CD__</div><div>数据时点 __TS__</div></div>
<div class='dnote'>退市进度：现价连续低于面值线 5.00 的时长（每格 5 分钟，满 24 格＝2 小时即退市清算，按现价 5 折退款）</div>
</div></body></html>"""

    async def _render_quote_card(self) -> str:
        """渲染「行情」快照卡 PNG 并落盘，返回本地路径；渲染异常向上抛。"""
        codes = list(STOCKS)
        colors = self._QC_COLORS
        up, down = self._QC_UP, self._QC_DOWN
        sdata = self._state.get("stocks", {}) or {}

        def _srs(c: str) -> list:
            s = sdata.get(c) or {}
            h = [float(x) for x in (s.get("hist") or [])][-96:]   # v2.2.0 存 288 画 96：卡片仍显示近 8 小时
            try:
                px = float(s.get("price", INIT_PRICE))
            except (TypeError, ValueError):
                px = float(INIT_PRICE)
            return (h + [px]) or [px]

        ser = {}
        for c in codes:
            pts = _srs(c)
            base = pts[0] if pts else 1.0
            ser[c] = [((v / base) - 1.0) * 100.0 if base else 0.0 for v in pts]
        n = max(len(ser[c]) for c in codes)
        if n < 2:  # 单点补平点，让折线可画
            for c in codes:
                while len(ser[c]) < 2:
                    ser[c] = ser[c] + ser[c][-1:]
            n = max(len(ser[c]) for c in codes)
        # v2.12.0 新股（CCB）上市初期历史点少：前端按首值补平，避免折线只挤在最左侧一小段。
        # 补的是「相对首日基准的 0%」，语义=上市后一直持平在 IPO 价，不是伪造波动。
        for c in codes:
            if len(ser[c]) < n:
                ser[c] = [ser[c][0]] * (n - len(ser[c])) + ser[c]

        def _multiline_svg(W, H, pl, pr, pt, pb):
            """y 轴 % 刻度画在 pl 左侧边栏（绘图区外），曲线只在 pl..W-pr 内。"""
            allv = [v for c in codes for v in ser[c]]
            mn, mx = min(allv), max(allv)
            mn = min(mn, 0.0)
            mx = max(mx, 0.0)
            span = (mx - mn) or 1.0
            mn -= span * 0.10
            mx += span * 0.10
            rng = mx - mn

            def xpos(i):
                return pl + i * (W - pl - pr) / (n - 1)

            def ypos(v):
                return H - pb - (v - mn) / rng * (H - pt - pb)

            out = []
            g = int(mn // 10) * 10
            while g <= mx:
                if mn <= g <= mx:
                    y = ypos(g)
                    if g != 0:
                        out.append("<line x1='%.1f' y1='%.1f' x2='%.1f' y2='%.1f' stroke='#F0E6D2' stroke-width='1'/>"
                                   % (pl, y, W - pr, y))
                    lab = "0%" if g == 0 else "%+d%%" % g
                    col = "#A98A63" if g == 0 else "#BCA382"
                    out.append("<text x='%.1f' y='%.1f' font-size='10' text-anchor='end' fill='%s'>%s</text>"
                               % (pl - 8, y + 3.5, col, lab))
                g += 10
            y0 = ypos(0.0)
            out.append("<line x1='%.1f' y1='%.1f' x2='%.1f' y2='%.1f' stroke='#C9AE86' stroke-width='1.2' stroke-dasharray='5 4'/>"
                       % (pl, y0, W - pr, y0))
            for c in codes:
                pts = [(xpos(i), ypos(v)) for i, v in enumerate(ser[c])]
                poly = " ".join("%.1f,%.1f" % (x, y) for x, y in pts)
                col = colors[c]
                out.append("<polyline points='%s' fill='none' stroke='%s' stroke-width='2.3' stroke-linejoin='round' stroke-linecap='round'/>"
                           % (poly, col))
                out.append("<circle cx='%.1f' cy='%.1f' r='3.2' fill='%s'/>" % (pts[-1][0], pts[-1][1], col))
            return "".join(out)

        def _spark_svg(vals, W, H, pad, color):
            mn, mx = min(vals), max(vals)
            if mx - mn < 1e-9:
                mx = mn + 1.0
            P = []
            for i, v in enumerate(vals):
                x = pad + i * (W - 2 * pad) / max(len(vals) - 1, 1)
                y = H - pad - (v - mn) / (mx - mn) * (H - 2 * pad)
                P.append((x, y))
            poly = " ".join("%.1f,%.1f" % (x, y) for x, y in P)
            return ("<polyline points='%s' fill='none' stroke='%s' stroke-width='2' "
                    "stroke-linejoin='round' stroke-linecap='round'/>"
                    "<circle cx='%.1f' cy='%.1f' r='2.4' fill='%s'/>" % (poly, color, P[-1][0], P[-1][1], color))

        chart = _multiline_svg(688, 220, 52, 10, 12, 12)
        legend = "".join(
            "<span class='li'><i style='background:%s'></i>%s %s</span>" % (colors[c], c, STOCKS[c]["name"])
            for c in codes)
        rows = []
        for c in codes:
            s = sdata.get(c) or {}
            _tag = _stock_tag(c)
            _tagc = {"稳健": "#5BA843", "活跃": "#D98E2B", "疯狂": "#C25543"}.get(_tag, "#A98A63")
            px = float(s.get("price", INIT_PRICE))
            opn = float(s.get("open", px)) or px
            period = (px / opn - 1) * 100 if opn else 0.0
            mv = list(s.get("moves") or [])
            tick = mv[-1] * 100 if mv else 0.0
            c1 = up if tick >= 0 else down
            c2 = up if period >= 0 else down
            sp = _spark_svg(ser[c], 84, 26, 2, colors[c])
            stk_n = int(s.get("floor_streak", 0) or 0)   # v2.9.4 退市进度＝连续低于面值线的 5min tick 数（N/24）
            if float(s.get("delist_until", 0) or 0) > time.time():
                dtxt, dcol = "停牌", up
            else:
                dtxt, dcol = ("%d/%d" % (min(stk_n, FLOOR_STREAK_MAX), FLOOR_STREAK_MAX),
                              up if stk_n > 0 else "#A98A63")
            rows.append(
                "<div class='row'><div class='code'><i style='background:%s'></i>%s</div>"
                "<div class='nm'>%s</div>"
                "<div class='tg' style='color:%s'>%s</div>"
                "<div class='sp'><svg width='84' height='26'>%s</svg></div>"
                "<div class='px'>%.2f</div>"
                "<div class='pc' style='color:%s'>%+.2f%%</div>"
                "<div class='pc' style='color:%s'>%+.2f%%</div>"
                "<div class='yh'>%.2f</div>"
                "<div class='dl' style='color:%s'>%s</div></div>"
                % (colors[c], c, STOCKS[c]["name"], _tagc, _tag, sp, px, c1, tick, c2, period, FLOOR_LINE, dcol, dtxt))
        nxt = float(self._state.get("next_tick_ts", 0) or 0)
        if nxt <= 0:
            cd = "⏳ 下次价格刷新：马上"
        else:
            cd = "⏳ 下次价格刷新：" + datetime.fromtimestamp(nxt, CST).strftime("%Y-%m-%d %H:%M:%S")
        html = (self._QC_TMPL.replace("__LEGEND__", legend)
                .replace("__CHART__", chart)
                .replace("__ROWS__", "".join(rows))
                .replace("__CD__", cd)
                .replace("__TS__", self._data_point_txt()))
        data = await self.html_render(
            html, {}, False,
            {"type": "png", "full_page": True, "viewport_width": 720,
             "viewport_height": 560, "device_scale_factor_level": "ultra"})
        if isinstance(data, bytes):
            raw_png = data
        elif isinstance(data, str) and Path(data).is_file():
            raw_png = Path(data).read_bytes()
        else:
            raise RuntimeError("行情卡渲染服务没有返回图片数据")
        if raw_png[:8] != b"\x89PNG\r\n\x1a\n":
            raise RuntimeError("行情卡渲染结果不是有效 PNG")
        out = self.render_dir / ("quote-%d.png" % int(time.time() * 1000))
        out.write_bytes(_shrink_card_png(raw_png))
        try:  # 清 10 分钟前旧卡，最多留 30 张
            now_t = time.time()
            stale = [p for p in sorted(self.render_dir.glob("quote-*.png"))
                     if now_t - p.stat().st_mtime >= 600]
            for p in stale[:-30]:
                p.unlink()
        except OSError:
            pass
        return str(out)

    _LK_COLORS = {"XBK": "#D98E2B", "BBB": "#3D6FB4", "PLB": "#7B4FA6",
                  "SMW": "#3B9E8F", "HZB": "#C25543", "CCB": "#D96A9B"}
    _LK_EMOJI = {"XBK": "🍔", "BBB": "🏦", "PLB": "🃏", "SMW": "📺", "HZB": "🔌", "CCB": "💗"}
    _LK_MONEY = {"lottery_flow", "bank_dep", "loan_out", "shark_out", "rp_flow"}   # v2.9.6 修：scratch 是次数不是金额，去 ¥ 前缀

    _LK_TMPL = """<!DOCTYPE html><html><head><meta charset='utf-8'><style>
*{margin:0;padding:0;box-sizing:border-box}
body{width:720px;background:#FBF3E4;padding:2px;font-family:"Noto Sans CJK SC","PingFang SC","Microsoft YaHei",sans-serif}
.card{background:#fff;border-radius:14px;overflow:hidden;border:2px solid #EFE0C6;box-shadow:0 6px 18px rgba(107,61,20,.10)}
.hd{background:linear-gradient(120deg,#6B3D14,#96622B);color:#fff;padding:8px 12px;display:flex;justify-content:space-between;align-items:center}
.hd .t{font-size:20px;font-weight:800}
.hd .s{font-size:12px;opacity:.92}
.lead{padding:6px 12px 0;font-size:12px;color:#6B4F2E}
.blk{padding:5px 12px 0}
.r1{display:flex;align-items:center;gap:6px}
.r1 i{width:10px;height:10px;border-radius:3px;display:inline-block}
.r1 .cd{font-weight:800;font-size:15px;color:#6B3D14;width:44px}
.r1 .nm{flex:1;font-size:13.5px;color:#4A3320;font-weight:700}
.r1 .px{font-size:12px;color:#A98A63;margin-right:6px}
.bat i{width:11px;height:12px;border-radius:2px;display:inline-block;margin-left:2px}
.bat .on{background:#5BA843}
.bat .off{background:#EEE3CC}
.r1 .pct{font-size:12px;font-weight:800;color:#6B4F2E;width:36px;text-align:right}
.r2{padding:4px 0 6px;display:flex;flex-wrap:wrap;gap:3px 5px}
.chip{font-size:11px;color:#5C4326;background:#FAF3E3;border:1px solid #EFE0C6;border-radius:7px;padding:2px 6px;display:inline-flex;align-items:center}
.chip i{width:7px;height:7px;border-radius:50%;display:inline-block;margin-right:3px;flex:none}
.chip b{color:#2E2013;margin-left:4px}
.chip.hot{background:#EAF4E4;border-color:#BFDCAF}
.chip.cold{background:#F7ECEA;border-color:#E7CFC9}
.chip.neg{background:#F9ECEA;border-color:#E0B9B2}
.chip.neg b{color:#8F3B2E}
.nm .tg{font-size:9.5px;font-weight:800;border:1px solid currentColor;border-radius:6px;padding:0 4px;margin-left:4px;vertical-align:1px}
.lg{padding:4px 12px 0;font-size:10.5px;color:#8A7354;display:flex;align-items:center;gap:4px;flex-wrap:wrap}
.lg i{width:8px;height:8px;border-radius:50%;display:inline-block;flex:none}
.ft{padding:5px 12px 7px;font-size:11.5px;color:#5C4326;background:#FCF6E9;border-top:1px solid #EFE0C6}
.ft .note{font-size:10px;color:#B09A77;margin-top:2px}
</style></head><body><div class='card'>
<div class='hd'><div class='t'>📊 小堡股市 · 联动指南</div><div class='s'>__DAY__ · 热度每小时刷新</div></div>
<div class='lead'>玩什么、聊什么，抬哪只股——<b>格子里的数字都是今天的真实数据</b>，绿了就是有人在喂。</div>
<div class='lg'><i style='background:#D98E2B'></i>行为源　<i style='background:#C9A227'></i>金额源（¥）　<i style='background:#3B9E8F'></i>游戏源（真实局数）　<i style='background:#7B4FA6'></i>话题源（群里聊）　<i style='background:#C25543'></i>↓ 反向源（越高越压）</div>
__BLOCKS__
<div class='ft'>__TIP__
<div class='note'>热度条 = 今日 ÷ 近 3 日峰值｜v2.9.13 起 SMW 记真实开局｜N局 = 今日该玩法开局次数｜聊·N = 群聊提到该话题的条数</div></div>
</div></body></html>"""

    async def _render_linkage_card(self) -> str:
        """v2.5.0 联动指南卡：各股联动功能今日真实数据 + 热度条，HTML→PNG 落盘。"""
        now = datetime.now(CST)
        day = now.strftime("%Y-%m-%d")
        today = self._sample_metrics_locked()
        ratios = self._heat_ratios(today, self._state.get("samples") or {}, day)
        blocks, allr = [], []
        for code in STOCKS:
            cfg = STOCKS[code]
            rs = []
            for k, w in cfg["sources"]:
                tv = float(today.get(k, 0) or 0)
                rs.append((k, w, tv, float(ratios.get(k, 0.0))))
            hv = sum(w * r for _k, w, _tv, r in rs)
            n_on = round(hv * 8)
            cells = "".join("<i class='%s'></i>" % ("on" if i < n_on else "off") for i in range(8))
            chips = "".join(
                "<span class='chip%s'><i style='background:%s'></i>%s%s <b>%s</b></span>" % (
                    (" neg" if k == "shark_out" else
                     (" hot" if r >= .6 else (" cold" if r <= .1 else ""))),
                    ("#C25543" if k == "shark_out" else
                     ("#C9A227" if k in self._LK_MONEY else
                      ("#3B9E8F" if k.startswith("game_") else
                       ("#7B4FA6" if k.startswith("kw_") else "#D98E2B")))),
                    "↓ " if k == "shark_out" else "",
                    METRIC_LABEL.get(k, k),
                    (("¥" + format(tv, ",.0f")) if k in self._LK_MONEY else format(tv, ",.0f")))
                for k, _w, tv, r in rs)
            _tag = _stock_tag(code)   # v2.8.0 联动卡拉通 v2.7.0 股性标签
            _tagc = {"稳健": "#5BA843", "活跃": "#D98E2B", "疯狂": "#C25543"}.get(_tag, "#A98A63")
            blocks.append(
                "<div class='blk'><div class='r1'><i style='background:%s'></i>"
                "<span class='cd'>%s</span><span class='nm'>%s %s"
                "<span class='tg' style='color:%s'>%s</span></span>"
                "<span class='px'>%.2f 堡贝</span><span class='bat'>%s</span><span class='pct'>%d%%</span></div>"
                "<div class='r2'>%s</div></div>"
                % (self._LK_COLORS[code], code, self._LK_EMOJI[code], cfg["name"],
                   _tagc, _tag, float(self._st(code)["price"]), cells, int(round(hv * 100)), chips))
            allr.extend((METRIC_LABEL.get(k, k), code, r) for k, _w, _tv, r in rs)
        hottest = max(allr, key=lambda x: x[2])
        coldest = min(allr, key=lambda x: x[2])
        tip = ("🔥 今日最热：<b>%s</b> ｜ 😴 最冷：<b>%s</b> —— 多玩多聊哪样，就抬哪只股"
               % (hottest[0], coldest[0]))
        html = (self._LK_TMPL.replace("__DAY__", day)
                .replace("__BLOCKS__", "".join(blocks))
                .replace("__TIP__", tip))
        data = await self.html_render(
            html, {}, False,
            {"type": "png", "full_page": True, "viewport_width": 720,
             "viewport_height": 120, "device_scale_factor_level": "ultra"})
        if isinstance(data, bytes):
            raw = data
        elif isinstance(data, str) and Path(data).is_file():
            raw = Path(data).read_bytes()
        else:
            raise RuntimeError("联动卡渲染服务没有返回图片数据")
        if raw[:8] != b"\x89PNG\r\n\x1a\n":
            raise RuntimeError("联动卡渲染结果不是有效 PNG")
        out = self.render_dir / ("linkage-%d.png" % int(time.time() * 1000))
        out.write_bytes(_shrink_card_png(raw))
        return str(out)

    async def _linkage(self, event):
        """v2.5.0 联动指南：一张卡讲清哪些功能在喂哪只股。"""
        try:
            card = await self._render_linkage_card()
        except Exception as exc:
            logger.warning("[StockDemo] 联动卡渲染失败：%s", exc)
            yield event.plain_result("联动卡这会儿渲染不出来，稍后再试喵。")
            return
        yield event.image_result(card)

    async def _quote(self, event, arg: str):
        now = datetime.now(CST)
        open_note = f"🟢 24h 交易中 · 第 {self._period_index(now) + 1}/12 期（每 2 小时一滚）"
        # v2.7.0 单股详情已删：「行情 代码」与「行情」同权，统一出总卡（总卡唯一入口）
        try:  # v2.0.7 行情快照改出 PNG 卡
            _card = await self._render_quote_card()
            yield event.image_result(_card)
            return
        except Exception as _card_exc:
            logger.warning("[StockDemo] 行情卡渲染失败，回退纯文字：%s", _card_exc)
        lines = ["📈 小堡股市 行情快照", open_note] + [self._quote_line(c) for c in STOCKS]
        lines.append("· 涨跌幅 = 近5分钟 / 本周期累计")
        lines.append(self._tick_countdown_line())
        lines.append("· 指令（完整写法）：小堡股市：买 XBK 200｜小堡股市：卖 XBK 100")
        lines.append("· 更多：小堡股市：资讯｜小堡股市：持仓｜小堡股市：排行｜小堡股市：规则")
        yield event.plain_result("\n".join(lines))

    # ---------- 交易 ----------
    async def _trade(self, event, uid: str, name: str, side: str, arg: str):
        parts = arg.replace("：", " ").replace(":", " ").split()
        if len(parts) != 2 or parts[0].upper() not in STOCKS or not parts[1].isdigit():
            yield event.plain_result("格式：小堡股市：买 XBK 200（代码 + 股数，1 股起）")
            return
        code, shares = parts[0].upper(), int(parts[1])
        if shares < 1:
            yield event.plain_result("至少 1 股起喵。")
            return
        if float(self._st(code).get("delist_until", 0) or 0) > time.time():
            yield event.plain_result(f"⛔ {code} {cfg_name(code)} 停牌重组中（退市清算后 2 小时恢复），先看看别的喵。")
            return
        sh = shares
        async with self._lock:
            s = self._st(code)
            px = float(s["price"])
            holdings = self._state.setdefault("holdings", {})
            h = holdings.setdefault(uid, {"name": name, "pos": {}})
            pos = h["pos"].setdefault(code, {"sh": 0, "avg": 0.0, "today": 0})
            if side == "买":
                cost = int(sh * px * (1 + FEE) + 0.999)
                bal = self.get_balance(uid)
                if bal < cost:
                    yield event.plain_result(
                        f"买 {sh} 股 {code} 要 {cost} 堡贝（含 0.3% 手续费），你只有 {bal}——先搬砖喵"
                    )
                    return
                self.add_coins(uid, name, -cost)
                self._state["div_pool"] = int(self._state.get("div_pool", 0) or 0) + \
                    int(sh * px * FEE + 0.999)   # 手续费进金库，每晚分红给 XBK 股东
                total_sh = pos["sh"] + sh
                pos["avg"] = (pos["avg"] * pos["sh"] + px * sh) / max(total_sh, 1)
                pos["sh"] = total_sh
                # v1.8 资金面记账：买入推价（按成交额计）
                fa = self._state.setdefault("flow", {}).setdefault(self._period_key(), {})
                fa[code] = float(fa.get(code, 0) or 0) + sh * px
                # v2.9.0 个人换手账本：按人按股按期记净额（tick 侧钳制聚合）
                _fu = (self._state.setdefault("flow_user", {})
                       .setdefault(self._period_key(), {}).setdefault(code, {}))
                _fu[uid] = float(_fu.get(uid, 0) or 0) + sh * px
                self._persist_locked()
                yield event.plain_result(
                    f"📈 小堡股市 成交：买入 {code} {cfg_name(code)} {sh} 股 @ {px:.2f}，"
                    f"实付 {cost}（手续费 {int(sh * px * FEE + 0.999)}），随买随卖喵"
                )
            else:
                free = int(pos["sh"])
                if free <= 0:
                    yield event.plain_result(f"你没有 {code} {cfg_name(code)} 的持仓喵")
                    return
                sh_sell = min(sh, free)
                avg_before = float(pos.get("avg", 0) or 0)   # v2.0.4 修：清仓会把 avg 归零，必须在归零前读，否则盈亏恒 0
                proceeds = int(sh_sell * px * (1 - FEE))
                fee_amt = int(sh_sell * px * FEE + 0.999)
                self.add_coins(uid, name, proceeds)
                self._state["div_pool"] = int(self._state.get("div_pool", 0) or 0) + fee_amt
                pos["sh"] = pos["sh"] - sh_sell
                # v1.8 资金面记账：卖出压价
                fa = self._state.setdefault("flow", {}).setdefault(self._period_key(), {})
                fa[code] = float(fa.get(code, 0) or 0) - sh_sell * px
                _fu = (self._state.setdefault("flow_user", {})
                       .setdefault(self._period_key(), {}).setdefault(code, {}))
                _fu[uid] = float(_fu.get(uid, 0) or 0) - sh_sell * px
                if pos["sh"] <= 0:
                    pos["avg"] = 0.0
                self._persist_locked()
                gain = (px - avg_before) * sh_sell if avg_before > 0 else 0.0
                pnl = (f"，参考均价 {avg_before:.2f} → 盈亏 {gain:+.0f}"
                       if avg_before > 0 else "")
                yield event.plain_result(
                    f"📉 小堡股市 成交：卖出 {code} {cfg_name(code)} {sh_sell} 股 @ {px:.2f}，"
                    f"到账 {proceeds}（手续费 {fee_amt}）{pnl}"
                )

    # ---------- 持仓 ----------
    async def _portfolio(self, event, uid: str, name: str):
        async with self._lock:
            h = (self._state.get("holdings", {}) or {}).get(uid)
            cash = self.get_balance(uid)
            if not h or not h.get("pos"):
                yield event.plain_result(f"你还没有持仓喵（余额 {cash}）。买法：小堡股市：买 XBK 200")
                return
            asset = 0
            lines = [f"💼 小堡股市 持仓明细 —— {name}"]
            for code, pos in h["pos"].items():
                if int(pos["sh"]) <= 0:
                    continue
                mv = int(pos["sh"]) * float(self._st(code)["price"])
                asset += mv
                pct = (float(self._st(code)["price"]) / float(pos["avg"]) - 1) * 100 if pos["avg"] else 0
                flag = "🔴" if pct >= 0 else "🟢"
                bonus = 0
                dt_ = self._state.get("div_today") or {}
                if code == "XBK" and dt_.get("day") == datetime.now(CST).strftime("%Y-%m-%d"):
                    bonus = int((dt_.get("by") or {}).get(uid, 0) or 0)
                lines.append(
                    f"{flag} {code} {cfg_name(code)}｜{pos['sh']} 股 @均 {pos['avg']:.2f} → 市值 {mv}（{pct:+.1f}%）"
                    + (f"｜今日分红 {bonus:+d}" if bonus else "")

                )
            lines.append(f"· 证券 {asset} + 现金 {cash} = 总资产 {asset + cash} 堡贝")
        yield event.plain_result("\n".join(lines))

    # ---------- 排行 ----------
    async def _leaderboard(self, event, arg: str = ""):
        if str(arg or "").strip() in ("今日", "今天", "today"):
            async for line in self._leaderboard_today(event):
                yield line
            return
        async with self._lock:
            rows = []
            for uid, h in (self._state.get("holdings", {}) or {}).items():
                asset = 0
                for code, pos in (h.get("pos") or {}).items():
                    asset += int(pos["sh"]) * float(self._st(code)["price"])
                cash = self.get_balance(uid)
                rows.append(((h.get("name") or uid), asset, cash))
            rows.sort(key=lambda r: -(r[1] + r[2]))
            lines = ["🏆 小堡股市 总资产排行（证券+现金）"]
            for i, (nm, a, c) in enumerate(rows[:10], 1):
                medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else f"{i}."
                lines.append(f"{medal} {nm} —— 总资产 {a + c:.0f}（证券 {a:.0f}）")
            if len(rows) < 2:
                lines.append("· 排行还太冷清：快开仓让别人见识下操盘手喵")
        yield event.plain_result("\n".join(lines))

    async def _leaderboard_today(self, event):
        """「排行 今日」：按今日收益率排名（对总资产榜的话题化补充）。"""
        async with self._lock:
            day = datetime.now(CST).strftime("%Y-%m-%d")
            snap = (self._state.get("asset_snap") or {}).get(day) or {}
            rows = []
            for uid, h in (self._state.get("holdings", {}) or {}).items():
                asset = 0
                for code, pos in (h.get("pos") or {}).items():
                    asset += int(pos["sh"]) * float(self._st(code)["price"])
                total = asset + self.get_balance(uid)
                try:
                    base = float(snap.get(uid))
                except (TypeError, ValueError):
                    continue
                if base <= 0:
                    continue
                rows.append(((h.get("name") or uid), (total - base) / base, total - base))
            rows.sort(key=lambda r: -r[1])
            lines = [f"🏆 小堡股市 · 今日收益榜（{datetime.now(CST).strftime('%m-%d')}）"]
            if not rows:
                lines.append("· 今天还没有可比的持仓（收益榜按今日零点总资产起算）")
            else:
                for i, (nm, ret, dlt) in enumerate(rows[:10], 1):
                    medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else f"{i}."
                    lines.append(f"{medal} {nm} {ret * 100:+.1f}%（{dlt:+.0f}）")
                if len(rows) >= 2:
                    nm, ret, dlt = rows[-1]
                    lines.append(f"· 今日垫底：{nm} {ret * 100:+.1f}%（{dlt:+.0f}）")
            lines.append("· 想看总资产榜：小堡股市：排行")
        yield event.plain_result("\n".join(lines))


def cfg_name(code: str) -> str:
    return STOCKS.get(code, {}).get("name", code)


def _shrink_card_png(raw: bytes) -> bytes:
    """把渲染出来的卡片 PNG 量化到 256 色，省约 70% 体积。

    任何异常都原样返回（失败即发原图），绝不因压缩影响出图。
    做法与 checkin_shop 的 256 色量化同源；小堡真实卡片 1:1 实证肉眼无差别。
    """
    try:
        if not raw or raw[:8] != b"\x89PNG\r\n\x1a\n":
            return raw
        import io as _io
        from PIL import Image as _Image
        im = _Image.open(_io.BytesIO(raw)).convert("RGB")
        q = im.convert("P", palette=_Image.Palette.ADAPTIVE, colors=256)
        buf = _io.BytesIO()
        q.save(buf, "PNG", optimize=True, compress_level=9)
        out = buf.getvalue()
        if out[:8] != b"\x89PNG\r\n\x1a\n" or len(out) >= len(raw):
            return raw
        return out
    except Exception:
        return raw
