#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import json
import time
import re
import requests
import tushare as ts
from datetime import datetime

# ---------- 配置 ----------
TUSHARE_TOKEN = os.environ.get('TUSHARE_TOKEN')
if not TUSHARE_TOKEN:
    raise Exception("请设置环境变量 TUSHARE_TOKEN")
ts.set_token(TUSHARE_TOKEN)
pro = ts.pro_api()

# 指数名称 -> 新浪代码映射表（请根据实际基金基准逐步补充）
INDEX_NAME_TO_SINA = {
    # A股主要指数
    "沪深300指数": "sh000300",
    "中证500指数": "sh000905",
    "中证100指数": "sh000903",
    "上证50指数": "sh000016",
    "创业板指数": "sz399006",
    "深证100指数": "sz399330",
    "中证红利指数": "sh000922",
    "中证消费指数": "sh000932",
    "中证医药指数": "sh000933",
    "中证军工指数": "sh399967",
    "中证环保指数": "sh000827",
    "中证白酒指数": "sh399997",
    # 基本面指数
    "中证锐联基本面50指数": "sh000925",
    "中证锐联基本面200指数": "sh000921",
    "中证锐联基本面400指数": "sh000922",
    "中证锐联基本面600指数": "sh000923",
    # 海外指数（需其他数据源，暂留空）
    "标普500指数": None,
    "纳斯达克100指数": None,
    "恒生指数": None,
    # 继续补充...
}

# ---------- 工具函数 ----------
def get_lof_list():
    """获取 LOF 列表及业绩比较基准"""
    df = pro.fund_basic(market='E', fields='ts_code,name,fund_type,benchmark')
    if df is None or df.empty:
        raise Exception("获取 fund_basic 失败")
    # 筛选 LOF 类型（fund_type 包含 'LOF'）
    df = df[df['fund_type'].str.contains('LOF', na=False)]
    df['code'] = df['ts_code'].str[:6]
    # 过滤掉 benchmark 为空的
    df = df[df['benchmark'].notna()]
    return df.to_dict('records')

def extract_index_name(benchmark):
    """从业绩比较基准字符串中提取指数名称（支持常见模式）"""
    # 常见指数关键词（按需扩充）
    keywords = [
        "沪深300", "中证500", "中证100", "上证50", "创业板", "深证100",
        "中证红利", "中证消费", "中证医药", "中证军工", "中证环保", "中证白酒",
        "中证锐联基本面50", "中证锐联基本面200", "中证锐联基本面400", "中证锐联基本面600",
        "标普500", "纳斯达克100", "恒生指数"
    ]
    for kw in keywords:
        if kw in benchmark:
            # 补全为映射表中的标准名称
            if kw in ["沪深300", "中证500", "中证100", "上证50", "创业板", "深证100"]:
                return kw + "指数"
            elif kw in ["中证红利", "中证消费", "中证医药", "中证军工", "中证环保", "中证白酒"]:
                return kw + "指数"
            elif kw.startswith("中证锐联基本面"):
                return kw + "指数"  # 例如 "中证锐联基本面50指数"
            else:
                return kw + ("指数" if "指数" not in kw else "")
    # 如果没匹配到，返回整个 benchmark 前30个字符（用于手动补充）
    return benchmark[:30]

def get_index_realtime(index_name):
    """根据指数名称获取新浪实时涨跌幅（返回小数，如 0.01 表示 +1%）"""
    if index_name not in INDEX_NAME_TO_SINA:
        print(f"  指数 {index_name} 未在映射表中")
        return None
    sina_code = INDEX_NAME_TO_SINA[index_name]
    if sina_code is None:
        print(f"  海外指数 {index_name} 暂不支持实时")
        return None

    url = f'https://hq.sinajs.cn/list={sina_code}'
    headers = {
        'Referer': 'https://finance.sina.com.cn',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }
    try:
        resp = requests.get(url, headers=headers, timeout=5)
        resp.encoding = 'gbk'
        text = resp.text
        # 解析格式：var hq_str_sh000300="指数名称,昨收,今开,当前价,涨跌额,涨跌幅,..."
        match = re.search(r'"(.*?)"', text)
        if not match:
            return None
        parts = match.group(1).split(',')
        if len(parts) < 6:
            return None
        # 涨跌幅字段通常是第6个字段（索引5），百分比形式如 0.5 表示 0.5%
        change_pct = float(parts[5])
        return change_pct / 100.0
    except Exception as e:
        print(f"  获取指数实时数据失败: {index_name} - {e}")
        return None

def get_latest_nav(ts_code):
    """获取最新净值（unit_nav）"""
    try:
        df = pro.fund_nav(ts_code=ts_code, limit=1)
        if df is not None and not df.empty:
            return df.iloc[0]['unit_nav']
    except Exception as e:
        print(f"  获取净值失败: {e}")
    return None

def get_latest_price(ts_code):
    """获取最新价格（日线收盘价）"""
    try:
        df = ts.pro_bar(ts_code=ts_code, asset='FD', freq='D', limit=1)
        if df is not None and not df.empty:
            return df.iloc[0]['close']
    except Exception as e:
        print(f"  获取价格失败: {e}")
    return None

# ---------- 主函数 ----------
def main():
    print(f"开始更新 LOF 数据，时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lofs = get_lof_list()
    print(f"获取到 {len(lofs)} 只 LOF 基金（含基准）")

    results = []
    for idx, fund in enumerate(lofs):
        ts_code = fund['ts_code']
        code = fund['code']
        name = fund['name']
        benchmark = fund.get('benchmark', '')
        print(f"处理 {idx+1}/{len(lofs)}: {code} {name}")

        # 获取前一日净值
        nav = get_latest_nav(ts_code)
        if not nav:
            print(f"  跳过：无净值")
            continue

        # 获取实时价格
        price = get_latest_price(ts_code)
        if not price:
            print(f"  跳过：无价格")
            continue

        # 解析指数名称
        index_name = extract_index_name(benchmark) if benchmark else None
        index_change = None
        if index_name:
            index_change = get_index_realtime(index_name)
            if index_change is not None:
                print(f"  指数: {index_name}, 涨跌幅: {index_change*100:.2f}%")
            else:
                print(f"  无法获取指数 {index_name} 实时数据")

        # 计算实时估值和溢价
        if index_change is not None:
            estimated_nav = nav * (1 + index_change)
            premium_estimated = (price - estimated_nav) / estimated_nav * 100
        else:
            estimated_nav = nav
            premium_estimated = None

        # 传统溢价（基于前一日净值）
        premium_prev = (price - nav) / nav * 100

        results.append({
            'code': code,
            'name': name,
            'price': round(price, 4),
            'nav_prev': round(nav, 4),
            'nav_estimated': round(estimated_nav, 4) if index_change is not None else None,
            'index_name': index_name,
            'index_change': round(index_change * 100, 2) if index_change is not None else None,
            'premium_estimated': round(premium_estimated, 2) if premium_estimated is not None else None,
            'premium_prev': round(premium_prev, 2)
        })

        # 控制请求频率（Tushare 建议 0.5秒以上）
        time.sleep(0.5)

    # 按实时溢价排序（缺失的放在最后）
    results.sort(key=lambda x: x['premium_estimated'] if x['premium_estimated'] is not None else -9999, reverse=True)

    # 保存 JSON
    output_path = 'lof_data.json'
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    print(f"更新完成，共 {len(results)} 条有效数据，保存至 {output_path}")

if __name__ == '__main__':
    main()
