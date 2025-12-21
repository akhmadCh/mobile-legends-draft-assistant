import streamlit as st
import sys
import os
import re 
import pandas as pd

# --- SETUP PATH & IMPORT ---
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from source.ml.predictor import DraftPredictor
from source.ml.recommender import DraftRecommender

# --- CONFIG ---
st.set_page_config(page_title="MLBB Draft Strategist", layout="wide", page_icon="🛡️")

# --- STYLING (CSS) ---
st.markdown("""
<style>
    /* Global Fonts */
    .big-title { font-size: 40px; font-weight: 800; text-align: center; margin-bottom: 20px; letter-spacing: 1px; }
    
    /* Headers */
    .section-header { text-align: center; font-size: 24px; font-weight: bold; margin: 20px 0 10px 0; text-transform: uppercase; letter-spacing: 1px; }
    
    .blue-header { color: #42A5F5; border-bottom: 4px solid #42A5F5; padding-bottom: 5px; margin-bottom: 15px; font-weight: 900; text-align: center; font-size: 22px; }
    .red-header { color: #EF5350; border-bottom: 4px solid #EF5350; padding-bottom: 5px; margin-bottom: 15px; font-weight: 900; text-align: center; font-size: 22px; }

    /* Input Box Styling (Agar Lebar & Jelas) */
    .stSelectbox label { font-size: 16px; font-weight: bold; }
    div[data-baseweb="select"] > div {
        min-height: 50px !important;
        background-color: #f8f9fa !important;
        border: 1px solid #ccc !important;
    }
    div[data-baseweb="select"] span {
        font-size: 18px !important; 
        font-weight: 600 !important;
        line-height: 50px !important;
    }

    /* Turn Indicator */
    .turn-box {
        padding: 15px;
        border-radius: 8px;
        margin-bottom: 15px;
        text-align: center;
        font-weight: bold;
        font-size: 20px;
        border: 3px solid #FFD700;
        background-color: rgba(255, 215, 0, 0.15);
        animation: pulse 1.5s infinite;
    }
    @keyframes pulse {
        0% { box-shadow: 0 0 0 0 rgba(255, 215, 0, 0.4); }
        70% { box-shadow: 0 0 0 10px rgba(255, 215, 0, 0); }
        100% { box-shadow: 0 0 0 0 rgba(255, 215, 0, 0); }
    }

    /* Ban Summary (Read Only) */
    .ban-tag {
        display: inline-block;
        padding: 5px 10px;
        margin: 2px;
        background-color: #444;
        color: #fff;
        border-radius: 4px;
        font-size: 14px;
    }
    
    /* Recommendations */
    .rec-card {
        background-color: #fff;
        color: #333;
        padding: 15px;
        border-radius: 8px;
        border-left: 6px solid #00C851;
        margin-bottom: 10px;
        box-shadow: 0 2px 5px rgba(0,0,0,0.1);
    }
    .rec-hero-name { font-size: 20px; font-weight: 800; color: #000; margin-bottom: 4px;}
    .rec-reason { font-size: 14px; color: #555; line-height: 1.4; }
</style>
""", unsafe_allow_html=True)

# --- LOAD RESOURCES & BUILD HERO REGISTRY ---
@st.cache_resource
def load_resources():
    pred = DraftPredictor()
    rec = DraftRecommender()
    
    # --- HERO REGISTRY SYSTEM ---
    registry = {}

    # 1. PERBAIKAN: Normalisasi Agresif (Membersihkan Sampah Data)
    # Mengubah "Angela 2024 V2" -> "angela", supaya cocok dengan "01_Angela" -> "angela"
    def normalize_key(name):
        s = str(name).lower()
        s = s.replace('&', 'and')
        # Hapus tahun (2020-2029) dan teks versi (v1, v2, rev)
        s = re.sub(r'202\d', '', s)
        s = re.sub(r'\bv\d+\b', '', s)
        # Hapus semua karakter SELAIN huruf (hapus angka, titik, spasi, dll)
        s = re.sub(r'[^a-z]', '', s) 
        return s

    # 2. PERBAIKAN: Prettify Lebih Bersih
    # Supaya jika yang terambil "Angela 2024 V2", tampilannya tetap "Angela"
    def prettify_name(raw_name):
        s = str(raw_name).strip()
        # Hapus angka awalan (01_, 1.)
        s = re.sub(r'^[\d\s._-]+', '', s)
        # Hapus tahun dan versi dari Tampilan Layar
        s = re.sub(r'202\d', '', s)
        s = re.sub(r'v\d+', '', s, flags=re.IGNORECASE)
        # Rapikan spasi
        s = s.replace('_', ' ').replace('.', ' ').replace('-', ' ')
        s = re.sub(r'\s+', ' ', s)
        return s.title().strip()

    # A. LOAD DARI MODEL AI
    for col in pred.feature_names:
        if "Hero_" in col:
            parts = col.split("_")
            try:
                if "Hero" in parts:
                    idx = parts.index("Hero")
                    raw_model_name = "_".join(parts[idx+1:])
                    
                    key = normalize_key(raw_model_name)
                    display = prettify_name(raw_model_name)
                    
                    if key not in registry:
                        registry[key] = {
                            'display': display,
                            'model': raw_model_name,
                            'stats': None
                        }
            except: continue

    # B. LOAD DARI STATISTIK
    if not rec.df_stats.empty and 'Nama Hero' in rec.df_stats.columns:
        for raw_stats_name in rec.df_stats['Nama Hero'].unique():
            # Dengan normalisasi baru, "Angela 2024 V2" akan menjadi "angela"
            # Sehingga dia akan MATCH dengan data yang sudah ada dari Model AI
            key = normalize_key(raw_stats_name)
            
            if key in registry:
                # Update data stats-nya saja, jangan buat entry baru
                registry[key]['stats'] = raw_stats_name
            else:
                # Jika hero benar-benar baru (belum ada di model)
                registry[key] = {
                    'display': prettify_name(raw_stats_name),
                    'model': None,
                    'stats': raw_stats_name
                }

    # 3. PERBAIKAN FINAL: Gunakan set() untuk Hapus Duplikat Mutlak
    # Ini memastikan tidak ada string nama yang muncul 2x di dropdown
    display_names = sorted(list(set([val['display'] for val in registry.values()])))
    
    display_map = {val['display']: val for val in registry.values()}
    
    return pred, rec, display_names, display_map

try:
    predictor, recommender, all_heroes, hero_map = load_resources()     
except Exception as e:
    st.error(f"Gagal memuat sistem: {e}")
    st.stop()

# --- SESSION STATE ---
if 'draft_stage' not in st.session_state: st.session_state.draft_stage = 'ban' # ban / pick
if 'blue_bans' not in st.session_state: st.session_state.blue_bans = [None]*5
if 'red_bans' not in st.session_state: st.session_state.red_bans = [None]*5
if 'blue_picks' not in st.session_state: st.session_state.blue_picks = [None]*5
if 'red_picks' not in st.session_state: st.session_state.red_picks = [None]*5

# --- HELPER FUNCTIONS ---
def get_unavailable_display_names():
    """Mengambil semua hero (nama display) yang sudah dipakai."""
    return [x for x in st.session_state.blue_bans + st.session_state.red_bans + st.session_state.blue_picks + st.session_state.red_picks if x is not None]

def reset_draft():
    st.session_state.draft_stage = 'ban'
    st.session_state.blue_bans = [None]*5
    st.session_state.red_bans = [None]*5
    st.session_state.blue_picks = [None]*5
    st.session_state.red_picks = [None]*5
    st.rerun()

def finish_ban_phase():
    st.session_state.draft_stage = 'pick'
    st.rerun()

# --- SIDEBAR PENGATURAN ---
with st.sidebar:
    st.header("⚙️ Konfigurasi")
    first_pick_choice = st.radio("Siapa First Pick?", ["Tim Biru (Saya)", "Tim Merah (Musuh)"])
    st.divider()
    if st.button("🔄 Reset Draft (Mulai Ulang)", type="primary", use_container_width=True):
        reset_draft()
    st.info("Tombol Reset akan mengembalikan ke Fase Ban.")

# --- LOGIKA URUTAN PICK ---
if "Biru" in first_pick_choice:
    PICK_ORDER = [('Blue', 0), ('Red', 0), ('Red', 1), ('Blue', 1), ('Blue', 2), ('Red', 2), ('Red', 3), ('Blue', 3), ('Blue', 4), ('Red', 4)]
else:
    PICK_ORDER = [('Red', 0), ('Blue', 0), ('Blue', 1), ('Red', 1), ('Red', 2), ('Blue', 2), ('Blue', 3), ('Red', 3), ('Red', 4), ('Blue', 4)]

current_turn = None
for team, idx in PICK_ORDER:
    if team == 'Blue':
        if st.session_state.blue_picks[idx] is None:
            current_turn = (team, idx); break
    else:
        if st.session_state.red_picks[idx] is None:
            current_turn = (team, idx); break

# --- MAIN LAYOUT ---
st.markdown("<div class='big-title'>🛡️ MLBB Draft Assistant</div>", unsafe_allow_html=True)

# Helper untuk filter dropdown
def get_options(current_val):
    unavailable = get_unavailable_display_names()
    return [h for h in all_heroes if h not in unavailable or h == current_val]

# ==============================================================================
# TAHAP 1: PHASE BANNING
# ==============================================================================
if st.session_state.draft_stage == 'ban':
    st.markdown("<div class='section-header'>🚫 TAHAP 1: BANNING HERO</div>", unsafe_allow_html=True)
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("<div class='blue-header'>BAN TIM BIRU (SAYA)</div>", unsafe_allow_html=True)
        for i in range(5):
            val = st.session_state.blue_bans[i]
            opts = get_options(val)
            sel = st.selectbox(f"Ban Hero {i+1}", ["-"] + opts, index=opts.index(val)+1 if val in opts else 0, key=f"ban_blue_{i}")
            if sel != "-" and sel != val:
                st.session_state.blue_bans[i] = sel; st.rerun()
            elif sel == "-" and val is not None:
                st.session_state.blue_bans[i] = None; st.rerun()

    with col2:
        st.markdown("<div class='red-header'>BAN TIM MERAH (MUSUH)</div>", unsafe_allow_html=True)
        for i in range(5):
            val = st.session_state.red_bans[i]
            opts = get_options(val)
            sel = st.selectbox(f"Ban Hero {i+1}", ["-"] + opts, index=opts.index(val)+1 if val in opts else 0, key=f"ban_red_{i}")
            if sel != "-" and sel != val:
                st.session_state.red_bans[i] = sel; st.rerun()
            elif sel == "-" and val is not None:
                st.session_state.red_bans[i] = None; st.rerun()

    st.markdown("---")
    c1, c2, c3 = st.columns([1, 2, 1])
    with c2:
        if st.button("✅ SELESAI BAN & LANJUT KE PICK", type="primary", use_container_width=True):
            finish_ban_phase()

# ==============================================================================
# TAHAP 2: PHASE PICKING
# ==============================================================================
elif st.session_state.draft_stage == 'pick':
    # Ringkasan Ban
    with st.expander("👁️ Lihat Daftar Banned Hero", expanded=False):
        c1, c2 = st.columns(2)
        with c1:
            st.markdown("**Banned by Blue:** " + ", ".join([f"`{b}`" for b in st.session_state.blue_bans if b]))
        with c2:
            st.markdown("**Banned by Red:** " + ", ".join([f"`{b}`" for b in st.session_state.red_bans if b]))
    
    st.markdown("<div class='section-header'>⚔️ TAHAP 2: DRAFT PICK</div>", unsafe_allow_html=True)
    col_blue, col_mid, col_red = st.columns([1.2, 1.5, 1.2])

    # --- TIM BIRU ---
    with col_blue:
        st.markdown("<div class='blue-header'>TIM BIRU (SAYA)</div>", unsafe_allow_html=True)
        for i in range(5):
            val = st.session_state.blue_picks[i]
            is_active = (current_turn == ('Blue', i))
            
            container = st.container()
            if is_active: container.markdown(f"<div class='turn-box' style='border-color: #42A5F5; background-color: rgba(66, 165, 245, 0.1);'>GILIRAN SAYA</div>", unsafe_allow_html=True)
            
            # Logic Lock
            disabled = False
            if current_turn:
                if val is None and not is_active: disabled = True # Future slot locked
            
            sel = container.selectbox(f"Pick Blue {i+1}", ["-"] + get_options(val), index=get_options(val).index(val)+1 if val in get_options(val) else 0, disabled=disabled, key=f"pick_blue_{i}", label_visibility="collapsed")
            if sel != "-" and sel != val:
                st.session_state.blue_picks[i] = sel; st.rerun()
            elif sel == "-" and val is not None:
                st.session_state.blue_picks[i] = None; st.rerun()
            st.write("")

    # --- TIM MERAH ---
    with col_red:
        st.markdown("<div class='red-header'>TIM MERAH (MUSUH)</div>", unsafe_allow_html=True)
        for i in range(5):
            val = st.session_state.red_picks[i]
            is_active = (current_turn == ('Red', i))
            
            container = st.container()
            if is_active: container.markdown(f"<div class='turn-box' style='border-color: #EF5350; background-color: rgba(239, 83, 80, 0.1);'>GILIRAN MUSUH</div>", unsafe_allow_html=True)
            
            disabled = False
            if current_turn:
                if val is None and not is_active: disabled = True
            
            sel = container.selectbox(f"Pick Red {i+1}", ["-"] + get_options(val), index=get_options(val).index(val)+1 if val in get_options(val) else 0, disabled=disabled, key=f"pick_red_{i}", label_visibility="collapsed")
            if sel != "-" and sel != val:
                st.session_state.red_picks[i] = sel; st.rerun()
            elif sel == "-" and val is not None:
                st.session_state.red_picks[i] = None; st.rerun()
            st.write("")

    # --- AI ANALYSIS ---
    with col_mid:
        st.write("")
        # 1. PREDIKSI (Pakai Model Name: "01_Miya")
        clean_blue = [x for x in st.session_state.blue_picks if x]
        clean_red = [x for x in st.session_state.red_picks if x]
        
        if clean_blue and clean_red:
            # Terjemahkan: Tampilan "Miya" -> Model "01_Miya"
            # Jika tidak ada di model (None), gunakan nama displaynya (fallback)
            model_blue = [hero_map[h]['model'] if hero_map[h]['model'] else h for h in clean_blue]
            model_red = [hero_map[h]['model'] if hero_map[h]['model'] else h for h in clean_red]
            
            win_prob = float(predictor.predict_win_rate(model_blue, model_red))
            
            st.metric("PELUANG MENANG SAYA", f"{win_prob:.1%}")
            st.progress(win_prob)
            if win_prob > 0.55: st.success("Posisi Unggul! ✅")
            elif win_prob < 0.45: st.error("Posisi Tertinggal! ⚠️")
            else: st.info("Posisi Seimbang ⚖️")
        
        st.divider()

        # 2. REKOMENDASI (Pakai Stats Name jika ada, atau Model Name)
        if current_turn and current_turn[0] == 'Blue':
            st.subheader("💡 Rekomendasi AI")
            
            # Persiapkan Data untuk Recommender
            # Kita gunakan nama statistik ("Miya") jika ada, agar cocok dengan CSV counter
            # Jika tidak, pakai model name
            
            def get_rec_name(display_name):
                entry = hero_map.get(display_name)
                if not entry: return display_name
                return entry['stats'] if entry['stats'] else entry['model']
            
            my_picks_rec = [get_rec_name(h) for h in clean_blue]
            en_picks_rec = [get_rec_name(h) for h in clean_red]
            bans_rec = [get_rec_name(h) for h in st.session_state.blue_bans + st.session_state.red_bans if h]
            
            recs = recommender.recommend_dynamic_pick(my_picks_rec, en_picks_rec, bans_rec)
            
            if recs:
                for r in recs[:4]:
                    # r['hero'] adalah nama mentah dari CSV. Kita harus cari nama Display-nya.
                    # Cari di registry: entri mana yang punya 'stats' == r['hero']
                    found_display = r['hero'] # Default fallback
                    
                    # Scanning registry (karena kita butuh reverse lookup dari 'stats')
                    for d_name, d_data in hero_map.items():
                        # Cek kecocokan stats name ATAU model name (jika stats kosong)
                        if d_data['stats'] == r['hero'] or d_data['model'] == r['hero']:
                            found_display = d_name
                            break
                    
                    st.markdown(f"""
                    <div class="rec-card">
                        <div class="rec-hero-name">{found_display}</div>
                        <div class="rec-reason">{r['reason']}</div>
                    </div>
                    """, unsafe_allow_html=True)
            else:
                st.caption("Data history belum cukup.")
        elif current_turn:
             st.info("Menunggu Musuh Pick...")
        else:
            st.success("Draft Selesai!")
            st.balloons()