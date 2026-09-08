"""闲鱼商品擦亮。

擦亮会把商品重新推到搜索和推荐列表前面，是平台提供的免费曝光手段。
卖家手动擦亮几十个商品很费时，这里做成定时任务批量执行。

接口 ``mtop.taobao.idle.item.polish`` 走 H5 端签名。

注意 URL 路径版本必须是 **1.0**：路径 2.0 是个只返回成功不做事的空壳
（实测对已下架商品也返回 ``SUCCESS + exposure: true``，擦了等于没擦）；
路径 1.0 会做真实验证（已下架报 ``FAIL_BIZ_UNSUPPORTED_ITEM_STATUS``、
当日已擦亮报 ``POLISH_DUPLICATE``/``POLISH_AGAIN``）。``v`` 参数保持 2.0，
与上游 9100 星项目一致。
"""

import asyncio
import json
import time
from typing import Any, Dict, List, Optional

import aiohttp
from loguru import logger

from utils.xianyu_utils import generate_sign, trans_cookies


POLISH_API = "mtop.taobao.idle.item.polish"
POLISH_URL = f"https://h5api.m.goofish.com/h5/{POLISH_API}/1.0/"
APP_KEY = "34839810"

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/138.0.0.0 Safari/537.36"
)


async def polish_item(
    session: aiohttp.ClientSession,
    cookies_str: str,
    item_id: str,
    timeout: int = 20,
) -> Dict[str, Any]:
    """擦亮单个商品。

    Returns:
        ``{"success": bool, "message": str, "cookies_str": str}``。
        ``cookies_str`` 是合并响应后的最新 Cookie，调用方应回写。
    """
    if not item_id or not cookies_str:
        return {"success": False, "message": "缺少商品ID或Cookie", "cookies_str": cookies_str}

    data_val = json.dumps({"itemId": str(item_id)}, separators=(",", ":"))
    timestamp = str(int(time.time() * 1000))
    try:
        token_value = trans_cookies(cookies_str).get("_m_h5_tk", "")
    except ValueError:
        token_value = ""
    token = token_value.split("_")[0] if token_value else ""

    params = {
        "jsv": "2.7.2",
        "appKey": APP_KEY,
        "t": timestamp,
        "sign": generate_sign(timestamp, token, data_val),
        # v 参数用 2.0；URL 路径版本才是真正的路由开关（见文件头说明）
        "v": "2.0",
        "type": "originaljson",
        "accountSite": "xianyu",
        "dataType": "json",
        "timeout": "20000",
        "api": POLISH_API,
        "sessionOption": "AutoLoginOnly",
        "spm_cnt": "a21ybx.item.0.0",
        "spm_pre": "a21ybx.personal.feeds.1.42f86ac21eZ9zd",
        "log_id": "42f86ac21eZ9zd",
    }
    headers = {
        "accept": "application/json",
        "content-type": "application/x-www-form-urlencoded",
        "origin": "https://www.goofish.com",
        "referer": "https://www.goofish.com/",
        "user-agent": USER_AGENT,
        "sec-ch-ua": '"Google Chrome";v="141", "Not=A?Brand";v="8", "Not A(Brand)";v="141"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Win32"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-site",
        "cookie": cookies_str.replace("\n", "").replace("\r", ""),
    }

    try:
        async with session.post(
            POLISH_URL,
            params=params,
            data={"data": data_val},
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=timeout),
        ) as response:
            result = await response.json(content_type=None)
            cookies_str = _merge_cookies(response, cookies_str)
    except Exception as e:
        return {"success": False, "message": f"请求异常: {e}", "cookies_str": cookies_str}

    ret_list = result.get("ret", []) if isinstance(result, dict) else []
    message = "; ".join(str(v) for v in ret_list) or "未知响应"
    ret_text = message
    # 「一天只能擦亮一次」就是当日擦亮已达成，视为成功（与上游一致）。
    # 路径 1.0 对这种情况返回 FAIL_BIZ_* 业务错误而非 SUCCESS。
    duplicate = any(
        marker in ret_text
        for marker in ("POLISH_DUPLICATE", "POLISH_AGAIN", "一天只能擦亮一次", "已经擦亮过")
    )
    success = duplicate or any("SUCCESS" in str(v) for v in ret_list)
    if duplicate and "SUCCESS" not in ret_text:
        message = "今日已擦亮过（视为成功）"
    return {"success": success, "message": message, "cookies_str": cookies_str}


def _merge_cookies(response, cookies_str: str) -> str:
    """合并响应下发的新令牌，保持后续请求签名有效。"""
    if "set-cookie" not in response.headers:
        return cookies_str

    updates = {}
    for raw in response.headers.getall("set-cookie", []):
        pair = raw.split(";", 1)[0].strip()
        if "=" not in pair:
            continue
        key, value = pair.split("=", 1)
        if key in ("_m_h5_tk", "_m_h5_tk_enc"):
            updates[key] = value

    if not updates:
        return cookies_str
    try:
        current = trans_cookies(cookies_str) if cookies_str else {}
    except ValueError:
        current = {}
    current.update(updates)
    return "; ".join(f"{k}={v}" for k, v in current.items())


async def polish_account_items(
    cookie_id: str,
    cookies_str: str,
    item_ids: Optional[List[str]] = None,
    interval: float = 1.0,
) -> Dict[str, Any]:
    """批量擦亮账号下的商品。

    Args:
        item_ids: 指定商品；为空时擦亮该账号本地商品库中的全部商品。
        interval: 每次擦亮之间的间隔，避免触发频控。

    Returns:
        ``{"total", "success", "failed", "details", "cookies_str"}``
    """
    from app.db_manager import db_manager
    from utils import risk_control

    if not cookies_str:
        return {"total": 0, "success": 0, "failed": 0, "details": [], "cookies_str": cookies_str}

    if item_ids is None:
        items = db_manager.get_items_by_cookie(cookie_id) or []
        item_ids = [str(i.get("item_id")) for i in items if i.get("item_id")]

    if not item_ids:
        logger.info(f"【{cookie_id}】没有可擦亮的商品")
        return {"total": 0, "success": 0, "failed": 0, "details": [], "cookies_str": cookies_str}

    guard = risk_control.registry.get(cookie_id)
    success = 0
    failed = 0
    skipped = 0
    # 中断信息（结构化）：{"reason": "cooldown"|"risk", "skipped": n, "message": str}
    aborted: Dict[str, Any] = {}
    details: List[Dict[str, Any]] = []

    async with aiohttp.ClientSession() as session:
        for index, item_id in enumerate(item_ids):
            # 命中风控后立即中断整批：擦亮是纯增益操作，
            # 继续打剩余商品只会延长风控时间。
            if guard.is_blocked:
                skipped = len(item_ids) - index
                aborted = {
                    "reason": "cooldown",
                    "skipped": skipped,
                    "message": f"{risk_control.cooldown_message(cookie_id)}，已跳过 {skipped} 个商品",
                }
                logger.warning(f"【{cookie_id}】擦亮批量任务中断：{aborted['message']}")
                break

            result = await polish_item(session, cookies_str, item_id)
            cookies_str = result["cookies_str"]

            if result["success"]:
                success += 1
                # 请求被平台正常受理，说明账号未处于风控状态
                guard.reset()
            else:
                failed += 1

            details.append({
                "item_id": item_id,
                "success": result["success"],
                "message": result["message"],
            })

            # 命中平台风控：熔断并中断剩余商品（触发风控的那条已计入 details）
            if not result["success"] and risk_control.trip_if_risk_error(
                    cookie_id, result["message"], "商品擦亮"):
                skipped = len(item_ids) - index - 1
                aborted = {
                    "reason": "risk",
                    "skipped": skipped,
                    "message": "命中平台风控，已中断批量擦亮",
                }
                logger.warning(f"【{cookie_id}】擦亮命中平台风控，中断批量任务")
                break

            # 平台对擦亮有频控，逐个之间留间隔
            if interval and index < len(item_ids) - 1:
                await asyncio.sleep(interval)

    logger.info(
        f"【{cookie_id}】商品擦亮完成: 共 {len(item_ids)} 个，成功 {success}，"
        f"失败 {failed}"
        + (f"，跳过 {skipped} 个（{aborted['message']}）" if aborted else "")
    )
    return {
        "total": len(item_ids),
        "success": success,
        "failed": failed,
        "skipped": skipped,
        "aborted": aborted,
        "details": details,
        "cookies_str": cookies_str,
    }
