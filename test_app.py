import streamlit as st
import pandas as pd
import yfinance as yf
import os
import time
import random
from datetime import datetime, date, timedelta
from concurrent.futures import ThreadPoolExecutor

st.set_page_config(layout="wide", page_title="新高値監視Pro-超安定版")

# --- 設定 ---
DB_FILE = "stock_all_time_high_db.csv"

@st.cache_data(ttl=86400)
def get_stock_list():
    url = "https://www.jpx.co.jp/markets/statistics-equities/misc/tvdivq0000001vg2-att/data_j.xls"
    df = pd.read_excel(url)
    df = df[['コード', '銘柄名', '市場・商品区分']]
    return df[df['市場・商品区分'].str.contains('プライム|スタンダード|グロース', na=False)]

def fetch_stock_data(row, db_dict):
    code = str(row['コード'])
    ticker_sym = f"{code}.T"
    
    # 人間味を出すためのランダムな待ち時間
    time.sleep(random.uniform(0.2, 0.5))
    
    # 通信エラー対策のリトライ（3回まで）
    for attempt in range(3):
        try:
            ticker = yf.Ticker(ticker_sym)
            today = date.today()
            
            if code in db_dict:
                last_date = pd.to_datetime(db_dict[code]['date']).date()
                if last_date >= today: return None # すでに今日取得済み
                
                # 前回から今日までの全期間を取得
                hist = ticker.history(start=last_date)
                if hist.empty: return None
                
                high_ath = db_dict[code]['high_ath']
                high_1y = db_dict[code]['high_1y']
            else:
                # 初回は全期間(max)
                df_full = ticker.history(period="max")
                if df_full.empty: return None
                high_ath = df_full['High'][:-1].max()
                high_1y = df_full['High'].iloc[-251:-1].max() if len(df_full) > 251 else high_ath
                hist = df_full.tail(1)

            current_price = hist['Close'].iloc[-1]
            max_in_period = hist['High'].max()
            
            # 状態判定
            status = "待機"
            if current_price >= high_ath: status = "🌟上場来高値"
            elif current_price >= high_1y: status = "🔥1年高値"
            elif current_price >= high_ath * 0.96: status = "⏳🌟接近(上場来)"
            elif current_price >= high_1y * 0.96: status = "⏳🔥接近(1年)"

            return {
                "コード": code, "銘柄名": row['銘柄名'], "現在値": round(current_price, 1),
                "上場来": round(high_ath, 1), "1年高値": round(high_1y, 1),
                "状態": status, "high_ath": max(high_ath, max_in_period), 
                "high_1y": max(high_1y, max_in_period),
                "date": hist.index[-1].strftime('%Y-%m-%d')
            }
        except Exception:
            if attempt < 2:
                time.sleep(2) # 失敗したら少し長めに待つ
                continue
            return None
    return None

st.title("📈 新高値監視Pro - 超安定モデル")

# データベース読み込み
db_dict = {}
if os.path.exists(DB_FILE):
    try:
        db_df = pd.read_csv(DB_FILE)
        db_df['コード'] = db_df['コード'].astype(str)
        db_dict = db_df.set_index('コード').to_dict('index')
        st.info(f"保存済みデータ: {len(db_dict)} 銘柄")
    except: pass

if st.button("🚀 安定スキャン実行 (5スレッド)"):
    stock_list = get_stock_list()
    # 全件だと時間がかかるため進捗を表示
    progress_text = "スキャン中..."
    my_bar = st.progress(0, text=progress_text)
    
    results = []
    # 5スレッドで慎重に回す
    with ThreadPoolExecutor(max_workers=5) as executor:
        rows = stock_list.to_dict('records')
        for i, res in enumerate(executor.map(lambda r: fetch_stock_data(r, db_dict), rows)):
            if res: results.append(res)
            # 100銘柄ごとに進捗更新
            if i % 100 == 0:
                my_bar.progress(i / len(rows), text=f"{i}/{len(rows)} 銘柄完了...")

    my_bar.empty()
    
    if results:
        res_df = pd.DataFrame(results)
        # DB保存（これで次回から爆速）
        save_df = res_df[['コード', '銘柄名', 'high_ath', 'high_1y', 'date']]
        save_df.to_csv(DB_FILE, index=False)
        st.session_state.scan_result = results
        st.success(f"スキャン完了！ {len(results)} 銘柄取得")

# --- 結果表示 ---
if 'scan_result' in st.session_state:
    res_df = pd.DataFrame(st.session_state.scan_result)
    t1, t2 = st.tabs(["🔥 高値更新", "⏳ 接近中"])
    with t1:
        st.dataframe(res_df[res_df['状態'].str.contains("更新")].sort_values("状態", ascending=False), use_container_width=True)
    with t2:
        st.dataframe(res_df[res_df['状態'].str.contains("接近")].sort_values("状態", ascending=False), use_container_width=True)