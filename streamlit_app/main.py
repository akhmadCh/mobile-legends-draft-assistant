import streamlit as st
import sys
import os
import time
import pandas as pd
import subprocess

# --- 1. SETUP PATH SYSTEM ---
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from source.ml.recommender import DraftRecommender
from source.utils.minio_helper import read_df_from_minio, upload_df_to_minio

try:
    from source.ml.predictor import DraftPredictor
except ImportError:
    DraftPredictor = None

# --- 2. KONFIGURASI HALAMAN ---
st.set_page_config(
    page_title="MLBB Tactical Center",
    page_icon="🛡️",
    layout="wide",
    initial_sidebar_state="expanded" # Sidebar disembunyikan agar fokus ke Draft
)

# --- 3. THE "WOW" CSS INJECTION ---
def load_css():
    st.markdown("""
    <style>
        /* IMPORT FONT KEREN */
        @import url('https://fonts.googleapis.com/css2?family=Rajdhani:wght@400;600;700&family=Roboto+Mono:wght@400;700&display=swap');

        /* RESET & BACKGROUND */
        .stApp {
            background-color: #050510;
            background-image: radial-gradient(circle at 50% 0%, #1a1a40 0%, #050510 70%);
            font-family: 'Rajdhani', sans-serif;
            color: #e0e6ed;
        }

        /* HIDE DEFAULT STREAMLIT ELEMENTS */
        #MainMenu {visibility: hidden;}
        footer {visibility: hidden;}

        /* --- COMPONENTS: GLASSMORPHISM CARDS --- */
        .glass-card {
            background: rgba(255, 255, 255, 0.03);
            backdrop-filter: blur(16px);
            -webkit-backdrop-filter: blur(16px);
            border: 1px solid rgba(255, 255, 255, 0.08);
            border-radius: 12px;
            padding: 20px;
            margin-bottom: 15px;
            box-shadow: 0 4px 30px rgba(0, 0, 0, 0.3);
            transition: all 0.3s ease;
        }
        
        .glow-text-blue { 
            color: #00f2ea; 
            text-shadow: 0 0 10px rgba(0, 242, 234, 0.6); 
        }
        .glow-text-red { 
            color: #ff0055; 
            text-shadow: 0 0 10px rgba(255, 0, 85, 0.6); 
        }

        /* --- HEADER TITLE --- */
        .main-header {
            text-align: center;
            padding: 20px 0;
            margin-bottom: 30px;
            border-bottom: 1px solid rgba(255,255,255,0.1);
        }
        .main-header h1 {
            font-size: 3.5rem;
            font-weight: 700;
            letter-spacing: 4px;
            text-transform: uppercase;
            margin: 0;
            background: linear-gradient(90deg, #00f2ea, #fff, #ff0055);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
        }
        .sub-header {
            font-family: 'Roboto Mono', monospace;
            font-size: 0.9rem;
            color: #64748b;
            letter-spacing: 2px;
        }

        /* --- HERO SLOTS (Custom Selectbox Wrapper) --- */
        /* Kita manipulasi tampilan selectbox agar terlihat seperti slot game */
        div[data-baseweb="select"] > div {
            background-color: rgba(0,0,0,0.4) !important;
            border-color: rgba(255,255,255,0.1) !important;
            color: white !important;
            border-radius: 6px !important;
        }
        
        /* --- RECOMMENDATION CARDS --- */
        .rec-item {
            display: flex;
            justify-content: space-between;
            align-items: center;
            background: linear-gradient(90deg, rgba(0,0,0,0.5) 0%, rgba(20,20,40,0.5) 100%);
            border: 1px solid rgba(255,255,255,0.05);
            padding: 10px 15px;
            margin-bottom: 8px;
            border-radius: 6px;
            transition: transform 0.2s;
        }
        .rec-item:hover {
            transform: translateX(5px);
            border-color: #ffd700;
            background: rgba(255, 215, 0, 0.05);
        }
        .hero-name {
            font-weight: 700;
            font-size: 1.1rem;
            color: #f1f5f9;
        }
        .hero-score {
            font-family: 'Roboto Mono', monospace;
            font-size: 0.8rem;
            color: #ffd700;
        }
        .hero-desc {
            font-size: 0.8rem;
            color: #94a3b8;
            margin-top: 2px;
        }

        /* --- TURN INDICATOR --- */
        .turn-indicator {
            text-align: center;
            padding: 8px;
            border-radius: 4px;
            font-weight: bold;
            font-family: 'Roboto Mono', monospace;
            animation: pulse 1.5s infinite;
            margin-bottom: 10px;
        }
        .turn-blue { background: rgba(0, 242, 234, 0.15); color: #00f2ea; border: 1px solid #00f2ea; }
        .turn-red { background: rgba(255, 0, 85, 0.15); color: #ff0055; border: 1px solid #ff0055; }

        @keyframes pulse {
            0% { opacity: 0.7; }
            50% { opacity: 1; box-shadow: 0 0 15px rgba(255,255,255,0.1); }
            100% { opacity: 0.7; }
        }

        /* SCROLLBAR CUSTOM */
        ::-webkit-scrollbar { width: 6px; }
        ::-webkit-scrollbar-track { background: #0f172a; }
        ::-webkit-scrollbar-thumb { background: #334155; border-radius: 3px; }
        ::-webkit-scrollbar-thumb:hover { background: #475569; }

        /* WIN RATE BAR */
        .win-rate-container {
            width: 100%;
            height: 10px;
            background: #1e293b;
            border-radius: 5px;
            margin-top: 10px;
            overflow: hidden;
        }
        .win-rate-fill {
            height: 100%;
            background: linear-gradient(90deg, #ef4444 0%, #eab308 50%, #22c55e 100%);
        }
    </style>
    """, unsafe_allow_html=True)

load_css()

# --- 4. LOGIC LOAD RESOURCES ---
@st.cache_resource
def load_system():
    try:
        rec = DraftRecommender()
        hero_list = sorted(rec.df_stats['hero_name'].unique().tolist()) if rec.df_stats is not None else []
        pred = DraftPredictor() if DraftPredictor else None
        return rec, pred, hero_list
    except Exception as e:
        return None, None, []

recommender, predictor, all_heroes = load_system()

if not recommender:
    st.error("System Failure: Check Data Paths.")
    st.stop()

# --- 5. SESSION STATE MANAGER ---
defaults = {
    'draft_stage': 'ban',
    'blue_bans': [None]*5, 'red_bans': [None]*5,
    'blue_picks': [None]*5, 'red_picks': [None]*5,
    'user_profile': {'main_roles': ["Mage", "Marksman"], 'comfort_heroes': [], 'avoid_roles': []}
}
for k, v in defaults.items():
    if k not in st.session_state: st.session_state[k] = v

def reset_draft():
    for k in ['draft_stage', 'blue_bans', 'red_bans', 'blue_picks', 'red_picks']:
        st.session_state[k] = defaults[k] if k != 'draft_stage' else 'ban'
    st.rerun()

def get_available_heroes(current_val=None):
    used = st.session_state.blue_bans + st.session_state.red_bans + \
           st.session_state.blue_picks + st.session_state.red_picks
    used_set = set([x for x in used if x is not None])
    # Jika current_val ada, izinkan dia muncul agar tidak hilang dari list saat sudah dipilih
    return [h for h in all_heroes if h not in used_set or h == current_val]

# --- 6. HEADER VISUAL ---
st.markdown("""
<div class="main-header">
    <div class="sub-header">SYSTEM READY // ONLINE // v2.4.0</div>
    <h1>TACTICAL DRAFT HUD</h1>
</div>
""", unsafe_allow_html=True)

# --- 7. SIDEBAR (SETTINGS) ---
with st.sidebar:
    st.markdown("### 🛠️ CONFIGURATION")
    with st.expander("👤 User Profile", expanded=True):
      # 1. Role Utama (Apa yang kamu jago?)
      # User bisa pilih lebih dari satu, misal: Mage dan Marksman
      preferred_roles = st.multiselect(
         "Role Andalan (Main Role):",
         ["Mage", "Marksman", "Assassin", "Fighter", "Tank", "Support"],
         default=["Mage", "Marksman"], # Default biar ga kosong
         help="Sistem akan memprioritaskan hero dengan role ini untuk Anda."
      )
      
      # 2. Hero Nyaman (Comfort Picks)
      # Input manual hero apa yang user SUKA banget (opsional)
      # Pastikan variabel 'all_heroes' sudah di-load sebelumnya dari load_system()
      comfort_heroes = st.multiselect(
         "Hero Nyaman (Comfort Pick):",
         options=all_heroes if 'all_heroes' in locals() else [],
         help="Hero yang pasti Anda kuasai, apapun metanya."
      )
      
      # 3. Role Anti (User gabisa main ini)
      avoid_roles = st.multiselect(
         "Role yang Dihindari (Anti-Role):",
         ["Mage", "Marksman", "Assassin", "Fighter", "Tank", "Support"],
         default=["Assassin"], # Contoh umum: susah main assassin
         help="Sistem tidak akan menyarankan role ini UNTUK ANDA, tapi mungkin untuk teman."
      )

      # Simpan ke Session State agar bisa dibaca di seluruh aplikasi
      st.session_state['user_profile'] = {
         'main_roles': preferred_roles,
         'comfort_heroes': comfort_heroes,
         'avoid_roles': avoid_roles
      }
    
    with st.expander("⚙️ Match Settings"):
        first_pick = st.radio("First Pick", ["Blue Team (You)", "Red Team (Enemy)"])
    
    st.divider()
    if st.button("☣️ RESET PROTOCOL", use_container_width=True, type="secondary"):
        reset_draft()
        
    # Upload Logic (Simplified visual)
    st.divider()
    uploaded_file = st.file_uploader("📥 Update Dataset (CSV)", type=["csv"])
    if uploaded_file and st.button("Upload & Process"):
        if st.button("🚀 Kirim Data", use_container_width=True):
            try:
                with st.spinner("Mengupload & Memproses Data..."):
                    # 1. Baca CSV Baru
                    df_new = pd.read_csv(uploaded_file)
                    
                    # Validasi Kolom
                    required_cols = ['timestamp', 'my_team', 'enemy_team', 'result']
                    if not all(col in df_new.columns for col in required_cols):
                        st.error(f"Format CSV Salah! Kolom wajib: {required_cols}")
                    else:
                        # 2. Load Existing Raw Data
                        BUCKET_NAME = "mlbb-lake"
                        RAW_PATH = "raw/user_history/match_history_user.csv"
                        df_old = read_df_from_minio(BUCKET_NAME, RAW_PATH, file_format='csv')
                        
                        # 3. Gabungkan Data
                        if df_old is not None and not df_old.empty:
                            df_combined = pd.concat([df_old, df_new], ignore_index=True)
                        else:
                            df_combined = df_new
                            
                        # 4. Simpan ke MinIO (Raw)
                        upload_df_to_minio(df_combined, BUCKET_NAME, RAW_PATH, file_format='csv')
                        
                        # 5. Trigger ETL (Transform)
                        subprocess.Popen(["python", "-m", "source.transform.process_user_data"])
                        
                        st.success(f"Berhasil menambahkan {len(df_new)} match! Data sedang diproses...")
                        time.sleep(2)
                        st.rerun()
                        
            except Exception as e:
                st.error(f"Gagal Upload: {e}")

    # === [FEATURE UPDATE]: DISPLAY REAL DATA HISTORY ===
    st.divider()
    st.subheader("📚 Data History User")
    
    # Load Real Raw Data untuk Display
    try:
        df_hist_display = read_df_from_minio("mlbb-lake", "raw/user_history/match_history_user.csv", file_format='csv')
        
        if df_hist_display is not None and not df_hist_display.empty:
            st.write(f"Total: **{len(df_hist_display)} Matches**")
            
            # Tampilkan Data Frame (Bukan cuma angka)
            with st.expander("Lihat Riwayat Detail", expanded=False):
                # Tampilkan kolom penting saja
                cols_show = ['timestamp', 'result', 'my_team']
                st.dataframe(
                    df_hist_display[cols_show].sort_values('timestamp', ascending=False),
                    use_container_width=True,
                    hide_index=True
                )
        else:
            st.caption("Belum ada data history.")
            
    except Exception as e:
        st.caption("Gagal memuat history.")

# ==============================================================================
# PHASE 1: BANNING (The Exclusion Zone)
# ==============================================================================
if st.session_state.draft_stage == 'ban':
    st.markdown("""
    <div style="display:flex; justify-content:center; align-items:center; margin-bottom:20px;">
        <span style="font-size:1.5rem; font-weight:bold; color:#ef4444; border: 1px solid #ef4444; padding: 5px 20px; border-radius: 20px;">
            PHASE 1: BANNING SEQUENCE
        </span>
    </div>
    """, unsafe_allow_html=True)

    c1, c2, c3 = st.columns([1, 1.5, 1])

    # --- BLUE BANS ---
    with c1:
        st.markdown('<div class="glass-card team-blue"><h3 class="glow-text-blue">BLUE BANS</h3>', unsafe_allow_html=True)
        for i in range(5):
            curr = st.session_state.blue_bans[i]
            opts = ["-"] + get_available_heroes(curr)
            idx = opts.index(curr) if curr in opts else 0
            val = st.selectbox(f"Ban B{i+1}", opts, index=idx, key=f"ban_b_{i}", label_visibility="collapsed")
            if val != "-" and val != curr:
                st.session_state.blue_bans[i] = val
                st.rerun()
            elif val == "-" and curr is not None:
                st.session_state.blue_bans[i] = None
                st.rerun()
        st.markdown('</div>', unsafe_allow_html=True)

    # --- RECOMMENDATION CENTER ---
    with c2:
        st.markdown('<div class="glass-card" style="text-align:center; height: 100%;">', unsafe_allow_html=True)
        st.markdown("#### ⚠️ THREAT ANALYSIS")
        
        current_bans = [x for x in st.session_state.blue_bans + st.session_state.red_bans if x]
        recs = recommender.recommend_dynamic_ban([], [], current_bans)
        
        with st.container(height=350, border=False):
            if recs:
                for r in recs:
                    st.markdown(f"""
                    <div class="rec-item">
                        <div>
                            <div class="hero-name" style="color:#fca5a5;">{r['hero']}</div>
                            <div class="hero-desc">{r['reason']}</div>
                        </div>
                        <div class="hero-score">PRIORITY</div>
                    </div>
                    """, unsafe_allow_html=True)
            else:
                st.caption("Insufficient Data for Analysis")
        
        if st.button("CONFIRM BANS & INITIATE PICK", type="primary", use_container_width=True):
            st.session_state.draft_stage = 'pick'
            st.rerun()
            
        st.markdown('</div>', unsafe_allow_html=True)

    # --- RED BANS ---
    with c3:
        st.markdown('<div class="glass-card team-red"><h3 class="glow-text-red">RED BANS</h3>', unsafe_allow_html=True)
        for i in range(5):
            curr = st.session_state.red_bans[i]
            opts = ["-"] + get_available_heroes(curr)
            idx = opts.index(curr) if curr in opts else 0
            val = st.selectbox(f"Ban R{i+1}", opts, index=idx, key=f"ban_r_{i}", label_visibility="collapsed")
            if val != "-" and val != curr:
                st.session_state.red_bans[i] = val
                st.rerun()
        st.markdown('</div>', unsafe_allow_html=True)


# ==============================================================================
# PHASE 2: BATTLEFIELD (PICK PHASE)
# ==============================================================================
else:
    # --- INFO BAR ---
    bans = [b for b in st.session_state.blue_bans + st.session_state.red_bans if b]
    st.markdown(f"""
    <div style="text-align:center; margin-bottom: 20px; font-family:'Roboto Mono'; font-size:0.8rem; color:#64748b;">
        🚫 BANNED HEROES: <span style="color:#94a3b8;">{', '.join(bans) if bans else 'NONE'}</span>
    </div>
    """, unsafe_allow_html=True)

    # --- TURN LOGIC ---
    if "Blue" in first_pick:
        pick_order = [('B',0), ('R',0), ('R',1), ('B',1), ('B',2), ('R',2), ('R',3), ('B',3), ('B',4), ('R',4)]
    else:
        pick_order = [('R',0), ('B',0), ('B',1), ('R',1), ('R',2), ('B',2), ('B',3), ('R',3), ('R',4), ('B',4)]
    
    curr_team, curr_idx = None, None
    for team, idx in pick_order:
        if team == 'B' and st.session_state.blue_picks[idx] is None:
            curr_team, curr_idx = 'Blue', idx; break
        if team == 'R' and st.session_state.red_picks[idx] is None:
            curr_team, curr_idx = 'Red', idx; break

    # --- MAIN GRID ---
    col_l, col_m, col_r = st.columns([1, 1.8, 1])

    # --- LEFT: BLUE TEAM ---
    with col_l:
        st.markdown('<div class="glass-card team-blue">', unsafe_allow_html=True)
        st.markdown('<h3 class="glow-text-blue" style="text-align:center;">ALLY FORCE</h3>', unsafe_allow_html=True)
        
        for i in range(5):
            is_active = (curr_team == 'Blue' and curr_idx == i)
            
            # Active indicator
            if is_active:
                st.markdown('<div class="turn-indicator turn-blue">YOUR TURN</div>', unsafe_allow_html=True)
            
            # Slot
            curr = st.session_state.blue_picks[i]
            opts = ["-"] + get_available_heroes(curr)
            idx = opts.index(curr) if curr in opts else 0
            val = st.selectbox(f"S{i+1}", opts, index=idx, key=f"p_b_{i}", label_visibility="collapsed")
            
            # Spacer visual
            st.markdown('<div style="margin-bottom:12px;"></div>', unsafe_allow_html=True)

            if val != "-" and val != curr:
                st.session_state.blue_picks[i] = val
                st.rerun()
        
        st.markdown('</div>', unsafe_allow_html=True)

    # --- CENTER: TACTICAL VISOR (Analytics & Recs) ---
    with col_m:
        my_team = [x for x in st.session_state.blue_picks if x]
        en_team = [x for x in st.session_state.red_picks if x]
        banned = [x for x in st.session_state.blue_bans + st.session_state.red_bans if x]

        # 1. WIN PROBABILITY GAUGE
        if predictor and my_team and en_team:
            try:
                win_prob = predictor.predict_win_rate(my_team, en_team)
                st.markdown(f"""
                <div class="glass-card" style="padding: 15px; text-align:center;">
                    <div style="font-size:0.9rem; color:#94a3b8; letter-spacing:1px;">PREDICTED WIN RATE</div>
                    <div style="font-size:2.5rem; font-weight:bold; color:{'#22c55e' if win_prob > 0.5 else '#ef4444'};">
                        {win_prob:.1%}
                    </div>
                    <div class="win-rate-container">
                        <div class="win-rate-fill" style="width: {win_prob*100}%;"></div>
                    </div>
                </div>
                """, unsafe_allow_html=True)
            except: pass

        # 2. RECOMMENDATION ENGINE
        if curr_team == 'Blue':
            recs_user, recs_team = recommender.recommend_personalized(my_team, en_team, banned, st.session_state['user_profile'])
            
            st.markdown('<div class="glass-card">', unsafe_allow_html=True)
            
            # Tabs manual using columns for cleaner look
            t1, t2 = st.tabs(["PERSONALIZED", "TEAM SYNERGY"])
            
            with t1:
               st.caption("Recommended based on your Role & Comfort:")
               if recs_user:
                  for r in recs_user:
                        st.markdown(f"""
                        <div class="rec-item">
                           <div>
                              <div class="hero-name">{r['hero']}</div>
                              <div class="hero-desc">{r['reason']}</div>
                           </div>
                           
                           <div class="hero-score" style="color:{'red' if r['wr'] < 0.4 else '#22c55e'};">
                              {"NOT RECOMMEND!" if r['wr'] < 0.4 else 'BEST PICK'}
                           </div>
                        </div>
                        """, unsafe_allow_html=True)
               else:
                  st.info("No specific personal data match. Check Team Synergy.")


            with t2:
                st.caption("Recommended for Team Balance & Counters:")
                with st.container(height=400):
                    if recs_team:
                        for r in recs_team:
                            st.markdown(f"""
                            <div class="rec-item">
                                <div>
                                    <div class="hero-name">{r['hero']}</div>
                                    <div class="hero-desc">{r['reason']}</div>
                                </div>
                                <div class="hero-score">{int(r['score'])} PTS</div>
                            </div>
                            """, unsafe_allow_html=True)
            st.markdown('</div>', unsafe_allow_html=True)

        elif curr_team == 'Red':
             st.markdown("""
             <div class="glass-card" style="display:flex; justify-content:center; align-items:center; height:200px; flex-direction:column;">
                <h2 class="glow-text-red" style="animation: pulse 2s infinite;">ENEMY IS PICKING...</h2>
                <p style="color:#94a3b8;">Analyze their potential strategy.</p>
             </div>
             """, unsafe_allow_html=True)
        
        else:
            # END GAME SAVE
            st.markdown('<div class="glass-card" style="text-align:center;">', unsafe_allow_html=True)
            st.markdown("### 💾 MATCH REPORT")
            st.caption("Simpan data untuk meningkatkan akurasi personalisasi Anda.")
            
            with st.form("save_match"):
                c1, c2 = st.columns(2)
                
                with c1:
                    st.markdown("**Hasil Pertandingan:**")
                    res = st.radio("Outcome", ["Victory (Menang)", "Defeat (Kalah)"], label_visibility="collapsed")
                
                with c2:
                    st.markdown("**Hero yang ANDA Mainkan:**")
                    # Ambil daftar hero tim kita yang sudah dipick (filter yang None)
                    my_final_team = [x for x in st.session_state.blue_picks if x]
                    
                    if my_final_team:
                        user_hero = st.selectbox("Pilih Hero Anda:", my_final_team)
                    else:
                        st.warning("Draft belum lengkap.")
                        user_hero = None

                st.markdown("---")
                
                if st.form_submit_button("CONFIRM & SAVE DATA", type="primary", use_container_width=True):
                    if user_hero and my_final_team and en_team:
                        # Siapkan data untuk dikirim
                        status_str = "Win" if "Victory" in res else "Loss"
                        
                        # Panggil fungsi save (akan kita update di backend setelah ini)
                        # Kita kirim 'user_hero' secara spesifik
                        success = recommender.save_match_result(
                            my_team=my_final_team, 
                            enemy_team=en_team, 
                            result_status=status_str,
                            user_hero_played=user_hero # <--- INI KUNCINYA
                        )
                        
                        if success:
                            st.success("✅ Match Saved! AI Learning in progress...")
                            # Trigger ETL Otomatis (biarkan seperti ini)
                            subprocess.Popen(["python", "-m", "source.transform.process_user_data"])
                            time.sleep(2)
                            reset_draft()
                        else:
                            st.error("Gagal menyimpan data.")
                    else:
                        st.error("Lengkapi Draft Pick terlebih dahulu!")
            
            st.markdown('</div>', unsafe_allow_html=True)


    # --- RIGHT: RED TEAM ---
    with col_r:
        st.markdown('<div class="glass-card team-red">', unsafe_allow_html=True)
        st.markdown('<h3 class="glow-text-red" style="text-align:center;">ENEMIES</h3>', unsafe_allow_html=True)
        
        for i in range(5):
            is_active = (curr_team == 'Red' and curr_idx == i)
            
            if is_active:
                st.markdown('<div class="turn-indicator turn-red">ENEMY TURN</div>', unsafe_allow_html=True)
            
            curr = st.session_state.red_picks[i]
            opts = ["-"] + get_available_heroes(curr)
            idx = opts.index(curr) if curr in opts else 0
            val = st.selectbox(f"M{i+1}", opts, index=idx, key=f"p_r_{i}", label_visibility="collapsed")
            
            st.markdown('<div style="margin-bottom:12px;"></div>', unsafe_allow_html=True)

            if val != "-" and val != curr:
                st.session_state.red_picks[i] = val
                st.rerun()
        
        st.markdown('</div>', unsafe_allow_html=True)