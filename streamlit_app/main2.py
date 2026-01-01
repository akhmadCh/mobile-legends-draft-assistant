import streamlit as st
import sys
import os
import time
import pandas as pd
import subprocess # Untuk menjalankan script ETL otomatis

# --- 1. SETUP PATH SYSTEM ---
# Agar bisa mengimpor modul dari folder 'source'
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from source.ml.recommender import DraftRecommender
from source.utils.minio_helper import read_df_from_minio, upload_df_to_minio # Import helper MinIO

# Opsional: Import Predictor jika sudah siap
try:
    from source.ml.predictor import DraftPredictor
except ImportError:
    DraftPredictor = None

# --- 2. KONFIGURASI HALAMAN & CSS KEREN ---
st.set_page_config(
    page_title="MLBB Draft Assistant",
    page_icon="🛡️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# CSS Custom untuk Tampilan "Gaming" + SCROLLBAR CUSTOM
st.markdown("""
<style>
    /* Background Gelap Elegan */
    .stApp {
        background-color: #0e1117;
        color: #e0e0e0;
    }
    
    /* Header Judul */
    .main-title {
        font-size: 3rem;
        font-weight: 800;
        background: -webkit-linear-gradient(#Fcd34d, #F59e0b);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        text-align: center;
        margin-bottom: 10px;
        text-shadow: 0px 0px 10px rgba(245, 158, 11, 0.3);
    }
    
    /* Card Rekomendasi */
    .rec-card {
        background-color: #1f2937;
        border-radius: 8px;
        padding: 12px;
        margin-bottom: 8px;
        box-shadow: 0 4px 6px rgba(0,0,0,0.3);
        transition: transform 0.2s;
    }
    .rec-card:hover {
        transform: scale(1.01);
        background-color: #2d3748;
    }
    .rec-hero {
        font-size: 1.1rem;
        font-weight: bold;
        color: #f3f4f6;
    }
    .rec-reason {
        font-size: 0.85rem;
        color: #9ca3af;
        margin-top: 4px;
        white-space: pre-line;
        line-height: 1.4;
    }
    
    /* Indikator Giliran */
    .turn-active {
        border: 2px solid #3b82f6;
        background-color: rgba(59, 130, 246, 0.1);
        border-radius: 10px;
        padding: 10px;
        animation: pulse 2s infinite;
        text-align: center;
        font-weight: bold;
        color: #60a5fa;
    }
    
    @keyframes pulse {
        0% { box-shadow: 0 0 0 0 rgba(59, 130, 246, 0.4); }
        70% { box-shadow: 0 0 0 10px rgba(59, 130, 246, 0); }
        100% { box-shadow: 0 0 0 0 rgba(59, 130, 246, 0); }
    }

    /* Custom Scrollbar */
    ::-webkit-scrollbar { width: 8px; height: 8px; }
    ::-webkit-scrollbar-track { background: #111827; border-radius: 4px; }
    ::-webkit-scrollbar-thumb { background: #4b5563; border-radius: 4px; }
    ::-webkit-scrollbar-thumb:hover { background: #6b7280; }
    
    /* Box Simpan Hasil */
    .save-box {
        background-color: #1f2937;
        padding: 15px;
        border-radius: 10px;
        border: 1px solid #374151;
        margin-top: 20px;
    }
</style>
""", unsafe_allow_html=True)

# --- 3. LOAD RESOURCES (CACHED) ---
@st.cache_resource
def load_system():
    print("--- Loading System Resources ---")
    
    try:
        # 1. Load Recommender
        rec = DraftRecommender()
        
        # DEBUG: Cek apakah data berhasil dimuat
        if rec.df_stats is None or rec.df_stats.empty:
            st.error("Data Stats Kosong! Cek file parquet di folder data/.")
            hero_list = []
        else:
            hero_list = sorted(rec.df_stats['hero_name'].unique().tolist())

        # 2. Load Predictor
        pred = DraftPredictor() if DraftPredictor else None
        
        return rec, pred, hero_list
        
    except Exception as e:
        st.error(f"Terjadi Error saat Load System: {e}")
        return None, None, []

try:
    recommender, predictor, all_heroes = load_system()
except Exception as e:
    st.error(f"Gagal memuat sistem: {e}")
    st.stop()

# --- 4. SESSION STATE ---
if 'draft_stage' not in st.session_state: st.session_state.draft_stage = 'ban' 
if 'blue_bans' not in st.session_state: st.session_state.blue_bans = [None]*5
if 'red_bans' not in st.session_state: st.session_state.red_bans = [None]*5
if 'blue_picks' not in st.session_state: st.session_state.blue_picks = [None]*5
if 'red_picks' not in st.session_state: st.session_state.red_picks = [None]*5

def reset_draft():
    st.session_state.draft_stage = 'ban'
    st.session_state.blue_bans = [None]*5
    st.session_state.red_bans = [None]*5
    st.session_state.blue_picks = [None]*5
    st.session_state.red_picks = [None]*5
    st.rerun()

# --- 5. SIDEBAR ---
with st.sidebar:
    # --- [BAGIAN BARU] 1. SETUP PROFIL PENGGUNA (Define Your Pool) ---
    st.header("👤 Profil Pengguna")
    
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
    
    st.divider()

    
    st.header("⚙️ Pengaturan")
    first_pick = st.radio("First Pick (Giliran Pertama):", ["Tim Saya (Blue)", "Musuh (Red)"])
    
    st.divider()
    
    if st.button("🔄 Reset Draft", use_container_width=True):
        reset_draft()
        
    st.info("💡 **Info:** Posisi Anda selalu di **Tim Biru (Kiri)**.")
    
    st.divider()
    
    # === [FEATURE UPDATE]: INPUT CSV HISTORY ===
    st.subheader("📂 Upload Dataset Match History")
    uploaded_file = st.file_uploader("Input CSV Match History", type=["csv"])
    
    if uploaded_file is not None:
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


# --- 6. JUDUL UTAMA ---
st.markdown("<div class='main-title'>🛡️ MLBB Draft Assistant</div>", unsafe_allow_html=True)

def get_available_heroes(current_val=None):
    used =  st.session_state.blue_bans + st.session_state.red_bans + \
            st.session_state.blue_picks + st.session_state.red_picks
    used_set = set([x for x in used if x is not None])
    return [h for h in all_heroes if h not in used_set or h == current_val]

# ==============================================================================
# PHASE 1: BANNING
# ==============================================================================
if st.session_state.draft_stage == 'ban':
    st.markdown("### 🚫 PHASE 1: BANNED HEROES")
    
    col1, col2, col3 = st.columns([1, 1.2, 1])
    
    # --- KOLOM KIRI: BAN SAYA (BLUE) ---
    with col1:
        st.info("🟦 Ban Tim Saya")
        for i in range(5):
            curr_val = st.session_state.blue_bans[i]
            opts = ["-"] + get_available_heroes(curr_val)
            idx = opts.index(curr_val) if curr_val in opts else 0
            sel = st.selectbox(f"Ban Blue {i+1}", opts, index=idx, key=f"ban_b_{i}")
            
            if sel != "-" and sel != curr_val:
                st.session_state.blue_bans[i] = sel
                st.rerun()
            elif sel == "-" and curr_val is not None:
                st.session_state.blue_bans[i] = None
                st.rerun()

    # --- KOLOM TENGAH: REKOMENDASI (SCROLLABLE) ---
    with col2:
        st.write("#### 💡 Saran Ban (Top 25)")
        
        # Ambil daftar yang sudah di-ban
        current_bans = [x for x in st.session_state.blue_bans + st.session_state.red_bans if x]
        recs = recommender.recommend_dynamic_ban([], [], current_bans)
        
        if recs:
            with st.container(height=400, border=True):
                for r in recs:
                    st.markdown(f"""
                    <div class="rec-card"">
                        <div class="rec-hero" style="color: #fca5a5;">{r['hero']}</div>
                        <div class="rec-reason">{r['reason']}</div>
                    </div>
                    """, unsafe_allow_html=True)
        else:
            st.warning("Data belum cukup untuk memberikan rekomendasi.")
            
        st.divider()
        if st.button("✅ Selesai Ban & Lanjut Pick", type="primary", use_container_width=True):
            st.session_state.draft_stage = 'pick'
            st.rerun()

    # --- KOLOM KANAN: BAN MUSUH (RED) ---
    with col3:
        st.error("🟥 Ban Musuh")
        for i in range(5):
            curr_val = st.session_state.red_bans[i]
            opts = ["-"] + get_available_heroes(curr_val)
            idx = opts.index(curr_val) if curr_val in opts else 0
            sel = st.selectbox(f"Ban Red {i+1}", opts, index=idx, key=f"ban_r_{i}")
            
            if sel != "-" and sel != curr_val:
                st.session_state.red_bans[i] = sel
                st.rerun()

# ==============================================================================
# PHASE 2: PICKING
# ==============================================================================
else:
    bans_str = ", ".join([b for b in st.session_state.blue_bans + st.session_state.red_bans if b])
    st.caption(f"⛔ **Banned:** {bans_str}")
    
    st.markdown("### ⚔️ PHASE 2: DRAFT PICK")
    
    if "Tim Saya" in first_pick:
        pick_order = [('B',0), ('R',0), ('R',1), ('B',1), ('B',2), ('R',2), ('R',3), ('B',3), ('B',4), ('R',4)]
    else:
        pick_order = [('R',0), ('B',0), ('B',1), ('R',1), ('R',2), ('B',2), ('B',3), ('R',3), ('R',4), ('B',4)]
        
    current_turn_team = None
    current_turn_idx = None
    
    for team, idx in pick_order:
        if team == 'B':
            if st.session_state.blue_picks[idx] is None:
                current_turn_team, current_turn_idx = 'Blue', idx
                break
        else:
            if st.session_state.red_picks[idx] is None:
                current_turn_team, current_turn_idx = 'Red', idx
                break

    col_blue, col_center, col_red = st.columns([1, 1.5, 1])

    # --- TIM SAYA ---
    with col_blue:
        st.markdown("<h4 style='color:#3b82f6; text-align:center;'>🟦 TIM SAYA</h4>", unsafe_allow_html=True)
        for i in range(5):
            active = (current_turn_team == 'Blue' and current_turn_idx == i)
            if active:
                st.markdown("<div class='turn-active'>GILIRAN ANDA</div>", unsafe_allow_html=True)
            
            curr_val = st.session_state.blue_picks[i]
            opts = ["-"] + get_available_heroes(curr_val)
            idx = opts.index(curr_val) if curr_val in opts else 0
            sel = st.selectbox(f"S{i+1}", opts, index=idx, key=f"pick_b_{i}", label_visibility="collapsed")
            st.write("") 
            
            if sel != "-" and sel != curr_val:
                st.session_state.blue_picks[i] = sel
                st.rerun()

    # --- TIM MUSUH ---
    with col_red:
        st.markdown("<h4 style='color:#ef4444; text-align:center;'>🟥 TIM MUSUH</h4>", unsafe_allow_html=True)
        for i in range(5):
            active = (current_turn_team == 'Red' and current_turn_idx == i)
            if active:
                st.markdown("<div class='turn-active' style='border-color:#ef4444; color:#ef4444; background-color:rgba(239,68,68,0.1);'>GILIRAN MUSUH</div>", unsafe_allow_html=True)
                
            curr_val = st.session_state.red_picks[i]
            opts = ["-"] + get_available_heroes(curr_val)
            idx = opts.index(curr_val) if curr_val in opts else 0
            sel = st.selectbox(f"M{i+1}", opts, index=idx, key=f"pick_r_{i}", label_visibility="collapsed")
            st.write("")
            
            if sel != "-" and sel != curr_val:
                st.session_state.red_picks[i] = sel
                st.rerun()

    # --- TENGAH ---
    with col_center:
        my_team = [x for x in st.session_state.blue_picks if x]
        en_team = [x for x in st.session_state.red_picks if x]
        
        # if predictor and len(my_team) == 5 and len(en_team) == 5:
        if predictor and my_team and en_team:
            try:
                win_prob = predictor.predict_win_rate(my_team, en_team)
                st.metric("PELUANG MENANG", f"{win_prob:.1%}")
                st.progress(win_prob)
                
                if win_prob > 0.6: st.success("Draft Superior! Pertahankan.")
                elif win_prob < 0.4: st.error("Draft Tertinggal! Cari Counter.")
                else: st.info("Draft Seimbang.")
            except Exception:
                pass 
                
        st.divider()
        
        if current_turn_team == 'Blue':
            st.subheader("💡 Analisis & Rekomendasi")
            
            # Ambil profil user dari session state
            user_profile = st.session_state.get('user_profile', {})
            banned = [x for x in st.session_state.blue_bans + st.session_state.red_bans if x]
            
            # Panggil fungsi baru yang sudah dipisah
            recs_user, recs_team = recommender.recommend_personalized(my_team, en_team, banned, user_profile)
            
            # Layout 2 Kolom (Kiri: User [Fixed], Kanan: Team [Scrollable])
            col_u, col_t = st.columns([1, 1]) 
            
            # --- KOLOM KIRI: REKOMENDASI UNTUK ANDA (Top 5) ---
            with col_u:
                st.info("🎯 Best For YOU (Top 5)")
                if recs_user:
                    for r in recs_user:
                        st.markdown(f"""
                        <div style="
                            background-color: #1e3a8a; 
                            padding: 12px; 
                            border-radius: 8px; 
                            margin-bottom: 8px; 
                            box-shadow: 2px 2px 5px rgba(0,0,0,0.2);">
                            <div style="font-weight:bold; font-size:1.1rem; color:white;">{r['hero']}</div>
                            <div style="font-size:0.85rem; color:#bfdbfe; margin-top:4px;">{r['reason']}</div>
                        </div>
                        """, unsafe_allow_html=True)
                else:
                    st.write("Belum ada saran spesifik.")

            # --- KOLOM KANAN: REKOMENDASI UNTUK TIM (Top 25 - Scrollable) ---
            with col_t:
                st.warning("🤝 Suggest to TEAM (Top 25)")
                st.caption(f"Daftar hero potensial untuk strategi tim ({len(recs_team)} opsi).")
                
                # FITUR BARU: SCROLLABLE CONTAINER
                # height=500 artinya tinggi fix 500px, kalau isinya lebih -> muncul scrollbar
                with st.container(height=500, border=True):
                    if recs_team:
                        for r in recs_team:
                            st.markdown(f"""
                            <div style="
                                background-color: #451a03; 
                                padding: 10px; 
                                border-radius: 6px; 
                                margin-bottom: 6px;
                                border: 1px solid #78350f;">
                                <div style="font-weight:bold; color:#fed7aa;">{r['hero']} <span style="font-size:0.8rem; color:#fdba74;">(Score: {r['score']:.0f})</span></div>
                                <div style="font-size:0.8rem; color:#ffedd5;">{r['reason']}</div>
                            </div>
                            """, unsafe_allow_html=True)
                    else:
                        st.write("Tim aman, fokus ke pick Anda saja.")
                
        elif current_turn_team == 'Red':
            st.info("Menunggu musuh memilih hero...")
        
        else:
            # === SAVE RESULT (WITH AUTO ETL) ===
            st.success("🎉 DRAFT SELESAI!")
            st.balloons()
            
            st.markdown("""
            <div class='save-box'>
                <h4 style="text-align: center; color: #4da6ff; margin-bottom: 5px;">💾 Simpan Hasil Pertandingan</h4>
                <p style="text-align: center; color: #9ca3af; font-size: 0.85rem; margin-bottom: 15px;">
                    Simpan hasil untuk melatih AI agar lebih memahami gaya bermain Anda.
                </p>
            </div>
            """, unsafe_allow_html=True)
            
            with st.form("save_match_form"):
                c1, c2 = st.columns([2, 1])
                with c1:
                    st.write("**Hasil Akhir Pertandingan:**")
                    match_result = st.radio("Status:", ["🏆 Win (Menang)", "❌ Loss (Kalah)"], horizontal=True, label_visibility="collapsed")
                
                with c2:
                    st.write("") 
                    submit_btn = st.form_submit_button("Simpan & Reset", type="primary", use_container_width=True)
                
                if submit_btn:
                    final_my_team = [x for x in st.session_state.blue_picks if x]
                    final_enemy_team = [x for x in st.session_state.red_picks if x]
                    status_str = "Win" if "Win" in match_result else "Loss"
                    
                    with st.spinner("Menyimpan ke History & Memproses Data..."):
                        # 1. Simpan ke Raw
                        success = recommender.save_match_result(final_my_team, final_enemy_team, status_str)
                    
                        if success:
                            # 2. Trigger ETL Otomatis
                            subprocess.Popen(["python", "-m", "source.transform.process_user_data"])
                            
                            st.success("✅ Data tersimpan! Mereset draft...")
                            time.sleep(2)
                            reset_draft()
                        else:
                            st.error("❌ Gagal menyimpan. Cek koneksi.")