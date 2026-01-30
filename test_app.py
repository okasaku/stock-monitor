import streamlit as st
import pandas as pd
import yfinance as yf
import os
import time
import random
from datetime import datetime, date, timedelta
from concurrent.futures import ThreadPoolExecutor

st.set_page_config(layout="wide", page_title="ますぷろ式・新高値監視Pro")

# --- 設定 ---
DB_FILE = "masupro_stock_master_db.csv"

def play_sound():
    # 高値更新通知音
    audio_html = """
        <audio autoplay>
            <source src="https://assets.mixkit.co.jp/active_storage/sfx/2869/2869-preview.mp3" type="audio/mpeg">
        </audio>
    """
    st.components.v1.html(audio_html, height=0)

@st.cache_data(ttl=86400)
def get_stock_list():
    # JPXから最新の銘柄リスト（市場区分付き）を取得
    url = "https://www.jpx.co.jp/markets/statistics-equities/misc/tvdivq0000001vg2-att/data_j.xls"
    df = pd.read_excel(url)
    df = df[['コード', '銘柄名', '市場・商品区分']]
    return df[df['市場・商品区分'].str.contains('プライム|スタンダード|グロース', na=False)]

def fetch_stock_data(row, db_dict):
    code = str(row['コード'])
    ticker_sym = f"{code}.T"
    time.sleep(random.uniform(0.1, 0.2)) # ブロック回避のための待機
    
    for attempt in range(3):
        try:
            ticker = yf.Ticker(ticker_sym)
            today = date.today()
            
            # --- 空白期間の差分取得ロジック ---
            if code in db_dict:
                last_update = pd.to_datetime(db_dict[code]['date']).date()
                if last_update >= today:
                    return {**db_dict[code], "コード": code, "銘柄名": row['銘柄名'], "市場": row['市場・商品区分']}
                
                # 前回保存日の翌日から今日までのデータのみ取得
                hist = ticker.history(start=last_update + timedelta(days=1))
                high_ath = db_dict[code]['high_ath']
                high_1y = db_dict[code]['high_1y']
                ath_date = pd.to_datetime(db_dict[code]['ath_date']).date()
                y1_date = pd.to_datetime(db_dict[code]['y1_date']).date()

                if hist.empty:
                    return {**db_dict[code], "コード": code, "銘柄名": row['銘柄名'], "市場": row['市場・商品区分'], "date": today.strftime('%Y-%m-%d')}
            else:
                # 初回取得時
                df_full = ticker.history(period="max")
                if df_full.empty: return None
                high_ath = df_full['High'][:-1].max()
                ath_date = df_full['High'][:-1].idxmax().date()
                high_1y = df_full['High'].iloc[-251:-1].max() if len(df_full) > 251 else high_ath
                y1_date = df_full['High'].iloc[-251:-1].idxmax().date() if len(df_full) > 251 else ath_date
                hist = df_full.tail(1)

            current_price = hist['Close'].iloc[-1]
            max_now = hist['High'].max()
            
            # 乖離率計算
            k_ath = round(((current_price / high_ath) - 1) * 100, 2)
            k_1y = round(((current_price / high_1y) - 1) * 100, 2)
            
            # 状態判定
            status = "待機"
            if current_price >= high_ath: status = "🌟上場来高値"
            elif current_price >= high_1y: status = "🔥1年高値"
            elif k_ath >= -5.0: status = "⏳🌟上場来接近"
            elif k_1y >= -5.0: status = "⏳🔥1年接近"

            return {
                "コード": code, "銘柄名": row['銘柄名'], "市場": row['市場・商品区分'],
                "現在値": round(current_price, 1), "過去高値": round(high_ath, 1) if "上場来" in status else round(high_1y, 1),
                "状態": status, 
                "経過日数": (today - ath_date).days if "上場来" in status else (today - y1_date).days,
                "乖離率": k_ath if "上場来" in status else k_1y,
                "high_ath": max(high_ath, max_now), "ath_date": (today if max_now >= high_ath else ath_date).strftime('%Y-%m-%d'),
                "high_1y": max(high_1y, max_now), "y1_date": (today if max_now >= high_1y else y1_date).strftime('%Y-%m-%d'),
                "date": today.strftime('%Y-%m-%d')
            }
        except:
            time.sleep(1)
            continue
    return None

# --- UIセクション ---
st.sidebar.title("🛠 ますぷろ式・設定")
auto_ref = st.sidebar.checkbox("場中自動更新(5-60分間隔)")
interval = st.sidebar.slider("更新間隔(分)", 1,5, 60, 15)

# --- 既存データの読み込み (更新しなくても表示する) ---
if 'df' not in st.session_state:
    if os.path.exists(DB_FILE):
        df_load = pd.read_csv(DB_FILE)
        df_load['コード'] = df_load['コード'].astype(str)
        st.session_state.df = df_load
    else:
        st.session_state.df = pd.DataFrame()

if 'sel_ticker' not in st.session_state:
    st.session_state.sel_ticker = None

def run_scan():
    s_list = get_stock_list()
    db_dict = st.session_state.df.set_index('コード').to_dict('index') if not st.session_state.df.empty else {}
    results = []
    with ThreadPoolExecutor(max_workers=5) as ex:
        rows = s_list.to_dict('records')
        p = st.progress(0)
        for i, res in enumerate(ex.map(lambda r: fetch_stock_data(r, db_dict), rows)):
            if res: results.append(res)
            if i % 100 == 0: p.progress(i / len(rows), text=f"スキャン中: {i}/{len(rows)}")
    if results:
        st.session_state.df = pd.DataFrame(results)
        st.session_state.df.to_csv(DB_FILE, index=False)
        if not st.session_state.df[st.session_state.df['状態'].str.contains("高値")].empty:
            play_sound()
            st.toast("新高値ブレイク検知！", icon="🚨")

st.title("📈 ますぷろ式・新高値監視アプリ")
col_btn1, col_btn2 = st.columns([1, 4])
with col_btn1:
    if st.button("🚀 差分スキャン実行"):
        run_scan()
with col_btn2:
    if not st.session_state.df.empty:
        last_date = st.session_state.df['date'].iloc[0]
        st.info(f"現在のデータ基準日: {last_date} (スキャンなしで表示中)")

# --- 4つのタブ表示 ---
if not st.session_state.df.empty:
    df = st.session_state.df
    tabs = st.tabs(["🌟 上場来高値", "🔥 1年高値", "⏳🌟 上場来接近", "⏳🔥 1年接近"])
    c_base = ['コード', '銘柄名', '市場', '現在値', '過去高値', '経過日数']
    
    def show_table(target, cols):
        target = target.reset_index(drop=True)
        ev = st.dataframe(target[cols], use_container_width=True, hide_index=True, on_select="rerun", selection_mode="single-row")
        if ev and len(ev.get("selection", {}).get("rows", [])) > 0:
            st.session_state.sel_ticker = target.iloc[ev["selection"]["rows"][0]]['コード']

    with tabs[0]:
        show_table(df[df['状態'] == "🌟上場来高値"].sort_values("経過日数"), c_base)
    with tabs[1]:
        show_table(df[df['状態'] == "🔥1年高値"].sort_values("経過日数"), c_base)
    with tabs[2]:
        show_table(df[df['状態'] == "⏳🌟上場来接近"].sort_values("乖離率", ascending=False), c_base + ['乖離率'])
    with tabs[3]:
        show_table(df[df['状態'] == "⏳🔥1年接近"].sort_values("乖離率", ascending=False), c_base + ['乖離率'])

    # --- 日足チャート表示セクション ---
    st.divider()
    if st.session_state.sel_ticker:
        ticker_code = st.session_state.sel_ticker
        name = df[df['コード'] == ticker_code]['銘柄名'].values[0]
        st.subheader(f"📊 {ticker_code} {name} の日足チャート")
        
        # 6ヶ月分の日足を取得
        chart_df = yf.Ticker(f"{ticker_code}.T").history(period="6mo")
        if not chart_df.empty:
            st.line_chart(chart_df['Close'])
            # 出来高の急増を確認するための棒グラフ
            st.bar_chart(chart_df['Volume'])
            
            m1, m2, m3 = st.columns(3)
            with m1: st.metric("現在値", f"¥{chart_df['Close'].iloc[-1]:,.1f}")
            with m2: st.metric("期間内最高値", f"¥{chart_df['High'].max():,.1f}")
            with m3: st.metric("直近出来高", f"{int(chart_df['Volume'].iloc[-1]):,}")
    else:
        st.write("👆 リストの行をクリックすると、ここに日足チャートが表示されます。")

if auto_ref:
    time.sleep(interval * 60)
    st.rerun()