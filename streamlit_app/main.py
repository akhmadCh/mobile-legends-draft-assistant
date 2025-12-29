import streamlit as st
import sys
import os
import time

# --- 1. SETUP PATH SYSTEM ---
# Agar bisa mengimpor modul dari folder 'source'
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from source.ml.recommender import DraftRecommender
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
        padding: 12px; /* Sedikit dikecilkan paddingnya biar muat banyak */
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
        white-space: pre-line; /* Agar enter terbaca */
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

    /* --- CUSTOM SCROLLBAR (Biar Gak Putih Polos) --- */
    /* Width */
    ::-webkit-scrollbar {
        width: 8px;
        height: 8px;
    }
    /* Track */
    ::-webkit-scrollbar-track {
        background: #111827; 
        border-radius: 4px;
    }
    /* Handle */
    ::-webkit-scrollbar-thumb {
        background: #4b5563; 
        border-radius: 4px;
    }
    /* Handle on hover */
    ::-webkit-scrollbar-thumb:hover {
        background: #6b7280; 
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
            print("ERROR: rec.df_stats is Empty")
            hero_list = []
        else:
            print(f"SUCCESS: Loaded {len(rec.df_stats)} rows of stats.")
            hero_list = sorted(rec.df_stats['hero_name'].unique().tolist())

        # 2. Load Predictor (Opsional)
        pred = DraftPredictor() if DraftPredictor else None
        
        return rec, pred, hero_list
        
    except Exception as e:
        st.error(f"Terjadi Error saat Load System: {e}")
        print(f"CRITICAL ERROR: {e}")
        return None, None, []

# Inisialisasi
try:
    recommender, predictor, all_heroes = load_system()
except Exception as e:
    st.error(f"Gagal memuat sistem: {e}")
    st.stop()

# --- 4. SESSION STATE MANAGEMENT ---
# Menyimpan status draft agar tidak hilang saat refresh
if 'draft_stage' not in st.session_state: st.session_state.draft_stage = 'ban' 
if 'blue_bans' not in st.session_state: st.session_state.blue_bans = [None]*5
if 'red_bans' not in st.session_state: st.session_state.red_bans = [None]*5
if 'blue_picks' not in st.session_state: st.session_state.blue_picks = [None]*5
if 'red_picks' not in st.session_state: st.session_state.red_picks = [None]*5

# Fungsi Reset
def reset_draft():
    st.session_state.draft_stage = 'ban'
    st.session_state.blue_bans = [None]*5
    st.session_state.red_bans = [None]*5
    st.session_state.blue_picks = [None]*5
    st.session_state.red_picks = [None]*5
    st.rerun()

# --- 5. SIDEBAR ---
with st.sidebar:
    st.header("⚙️ Pengaturan")
    first_pick = st.radio("First Pick:", ["Tim Saya (Blue)", "Musuh (Red)"])
    
    st.divider()
    
    # Tombol Reset
    if st.button("🔄 Reset Draft", use_container_width=True):
        reset_draft()
        
    st.info("💡 **Tips:** Sistem menggunakan data historis dari Gold Layer untuk memberikan rekomendasi yang preskriptif.")

# --- 6. JUDUL UTAMA ---
st.markdown("<div class='main-title'>🛡️ MLBB Draft Assistant</div>", unsafe_allow_html=True)

# Helper: Filter hero yang sudah dipilih agar tidak muncul lagi di dropdown
def get_available_heroes(current_val=None):
    used =  st.session_state.blue_bans + st.session_state.red_bans + \
            st.session_state.blue_picks + st.session_state.red_picks
    
    # Filter 'None' dan buat set agar pencarian cepat
    used_set = set([x for x in used if x is not None])
    
    # Kembalikan list hero yang belum dipakai, ATAU hero yang sedang dipilih di slot ini
    return [h for h in all_heroes if h not in used_set or h == current_val]

# ==============================================================================
# PHASE 1: BANNING
# ==============================================================================
if st.session_state.draft_stage == 'ban':
    st.markdown("### 🚫 PHASE 1: BANNED HEROES")
    
    col1, col2, col3 = st.columns([1, 1.2, 1])
    
    # --- KOLOM KIRI: BAN SAYA ---
    with col1:
        st.info("🟦 Ban Tim Saya")
        for i in range(5):
            curr_val = st.session_state.blue_bans[i]
            opts = ["-"] + get_available_heroes(curr_val)
            
            # Logic index agar tidak error jika value berubah
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
        st.write("#### 💡 Saran Ban (Top 10)")
        
        # Ambil daftar yang sudah di-ban
        current_bans = [x for x in st.session_state.blue_bans + st.session_state.red_bans if x]
        
        # Minta Rekomendasi
        recs = recommender.recommend_dynamic_ban([], [], current_bans)
        
        if recs:
            # [BARU] Container Scrollable untuk Ban (Tinggi 400px)
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

    # --- KOLOM KANAN: BAN MUSUH ---
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
    # Tampilkan Ringkasan Ban Kecil di Atas
    bans_str = ", ".join([b for b in st.session_state.blue_bans + st.session_state.red_bans if b])
    st.caption(f"⛔ **Banned:** {bans_str}")
    
    st.markdown("### ⚔️ PHASE 2: DRAFT PICK")
    
    # Tentukan Giliran (Snake Draft Logic)
    if "Saya" in first_pick:
        pick_order = [('B',0), ('R',0), ('R',1), ('B',1), ('B',2), ('R',2), ('R',3), ('B',3), ('B',4), ('R',4)]
    else:
        pick_order = [('R',0), ('B',0), ('B',1), ('R',1), ('R',2), ('B',2), ('B',3), ('R',3), ('R',4), ('B',4)]
        
    current_turn_team = None
    current_turn_idx = None
    
    # Cari slot kosong pertama sesuai urutan
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

    # --- TIM BIRU (KIRI) ---
    with col_blue:
        st.markdown("<h4 style='color:#3b82f6; text-align:center;'>🟦 TIM SAYA</h4>", unsafe_allow_html=True)
        for i in range(5):
            active = (current_turn_team == 'Blue' and current_turn_idx == i)
            
            # Styling container jika aktif
            if active:
                st.markdown("<div class='turn-active'>GILIRAN ANDA</div>", unsafe_allow_html=True)
            
            curr_val = st.session_state.blue_picks[i]
            opts = ["-"] + get_available_heroes(curr_val)
            idx = opts.index(curr_val) if curr_val in opts else 0
            
            # Selectbox
            sel = st.selectbox(f"S{i+1}", opts, index=idx, key=f"pick_b_{i}", label_visibility="collapsed")
            
            # Jarak antar slot
            st.write("") 
            
            if sel != "-" and sel != curr_val:
                st.session_state.blue_picks[i] = sel
                st.rerun()

    # --- TIM MERAH (KANAN) ---
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

    # --- TENGAH: ANALISIS & REKOMENDASI (SCROLLABLE) ---
    with col_center:
        # A. PREDIKSI WIN RATE (Jika model ada)
        my_team = [x for x in st.session_state.blue_picks if x]
        en_team = [x for x in st.session_state.red_picks if x]
        
        if predictor and my_team and en_team:
            try:
                # Asumsi predictor menerima list nama hero
                win_prob = predictor.predict_win_rate(my_team, en_team)
                
                st.metric("PELUANG MENANG", f"{win_prob:.1%}")
                st.progress(win_prob)
                
                if win_prob > 0.6: st.success("Draft Superior! Pertahankan.")
                elif win_prob < 0.4: st.error("Draft Tertinggal! Cari Counter.")
                else: st.info("Draft Seimbang.")
                
            except Exception as e:
                pass 
                
        st.divider()
        
        # B. REKOMENDASI PICK
        if current_turn_team == 'Blue':
            st.subheader("💡 Rekomendasi Pick (Top 25)")
            
            banned = [x for x in st.session_state.blue_bans + st.session_state.red_bans if x]
            
            # PANGGIL LOGIKA CERDAS
            recs = recommender.recommend_dynamic_pick(my_team, en_team, banned)
            
            if recs:
                # [BARU] Container Scrollable untuk Pick (Tinggi 500px biar muat banyak)
                with st.container(height=500, border=True):
                    for r in recs:
                        st.markdown(f"""
                        <div class="rec-card">
                            <div class="rec-hero">{r['hero']}</div>
                            <div class="rec-reason">{r['reason']}</div>
                        </div>
                        """, unsafe_allow_html=True)
            else:
                st.info("Belum ada rekomendasi spesifik.")
                
        elif current_turn_team == 'Red':
            st.info("Menunggu musuh memilih hero...")
        else:
            st.success("🎉 DRAFT SELESAI!")
            st.balloons()