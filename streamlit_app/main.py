import streamlit as st
import sys
import os

# Setup Path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from source.ml.predictor import DraftPredictor
from source.ml.recommender import DraftRecommender

st.set_page_config(page_title="Draft Assistant", layout="wide")

# --- Styles ---
st.markdown("""
<style>
    .team-box { padding: 10px; border-radius: 8px; text-align: center; color: white; font-weight: bold; margin-bottom: 5px; }
    .blue-bg { background: linear-gradient(90deg, #1CB5E0 0%, #000851 100%); }
    .red-bg { background: linear-gradient(90deg, #e52d27 0%, #b31217 100%); }
    .turn-indicator { border: 2px solid #F1C40F; padding: 10px; border-radius: 5px; text-align: center; font-weight: bold; color: #F1C40F; background-color: #262730; animation: pulse 2s infinite; }
    .rec-box { background-color: #1E1E1E; padding: 15px; border-radius: 10px; border-left: 5px solid #00C851; margin-top: 10px; }
    @keyframes pulse { 0% { box-shadow: 0 0 0 0 rgba(241, 196, 15, 0.4); } 70% { box-shadow: 0 0 0 10px rgba(241, 196, 15, 0); } 100% { box-shadow: 0 0 0 0 rgba(241, 196, 15, 0); } }
</style>
""", unsafe_allow_html=True)

st.title("🛡️ MLBB Draft Strategist (Ranked Mode)")

# --- Init Resources & Fix Hero List ---
@st.cache_resource
def load_resources():
    return DraftPredictor(), DraftRecommender()

try:
    predictor, recommender = load_resources()
    
    # --- PERBAIKAN LOGIC LIST HERO ---
    # 1. Ambil dari Model (Hero yang ada di training data)
    model_heroes = {h.replace("T1_", "") for h in predictor.feature_names if h.startswith("T1_")}
    
    # 2. Ambil dari Statistik (Semua hero yang ada di database statistik)
    stats_heroes = set()
    if not recommender.df_stats.empty:
        stats_heroes = set(recommender.df_stats['Nama Hero'].dropna().unique())
    
    # 3. Gabungkan keduanya agar LENGKAP
    # Jika statistik kosong (file error), fallback ke model_heroes
    if stats_heroes:
        all_heroes = sorted(list(model_heroes.union(stats_heroes)))
    else:
        all_heroes = sorted(list(model_heroes))
        
except Exception as e:
    st.error(f"Gagal memuat sistem: {e}")
    st.stop()

# --- Session State ---
if 'draft_history' not in st.session_state: 
    st.session_state.blue_bans = [None]*5
    st.session_state.red_bans = [None]*5
    st.session_state.blue_picks = [None]*5
    st.session_state.red_picks = [None]*5

# --- Sidebar Configuration ---
with st.sidebar:
    st.header("⚙️ Konfigurasi Awal")
    
    first_pick_side = st.radio(
        "Siapa First Pick?", 
        ["Tim Kita (Blue)", "Musuh (Red)"],
        index=0
    )
    
    if st.button("🔄 Reset Draft", type="primary"):
        st.session_state.blue_bans = [None]*5
        st.session_state.red_bans = [None]*5
        st.session_state.blue_picks = [None]*5
        st.session_state.red_picks = [None]*5
        st.rerun()

    st.divider()
    st.info("Input akan terbuka secara berurutan sesuai aturan Draft Pick Mobile Legends (Snake Draft).")

# --- LOGIC URUTAN DRAFT (SNAKE DRAFT) ---
def get_draft_sequence(is_blue_first_pick):
    seq = []
    start_team = 'Blue' if is_blue_first_pick else 'Red'
    second_team = 'Red' if is_blue_first_pick else 'Blue'
    
    # Phase 1: Bans (10 Slot, selang-seling)
    for i in range(5):
        seq.append(('ban', start_team, i))
        seq.append(('ban', second_team, i))
        
    # Phase 2: Picks (Snake Draft)
    seq.append(('pick', start_team, 0)) 
    seq.append(('pick', second_team, 0))
    seq.append(('pick', second_team, 1))
    seq.append(('pick', start_team, 1))
    seq.append(('pick', start_team, 2))
    seq.append(('pick', second_team, 2))
    seq.append(('pick', second_team, 3))
    seq.append(('pick', start_team, 3))
    seq.append(('pick', start_team, 4))
    seq.append(('pick', second_team, 4))
    
    return seq

is_blue_fp = (first_pick_side == "Tim Kita (Blue)")
draft_sequence = get_draft_sequence(is_blue_fp)

# --- TENTUKAN GILIRAN ---
current_turn_info = None 
active_step_index = 0

for i, step in enumerate(draft_sequence):
    phase, team, idx = step
    is_filled = False
    if team == 'Blue':
        if phase == 'ban': is_filled = st.session_state.blue_bans[idx] is not None
        else: is_filled = st.session_state.blue_picks[idx] is not None
    else: 
        if phase == 'ban': is_filled = st.session_state.red_bans[idx] is not None
        else: is_filled = st.session_state.red_picks[idx] is not None
        
    if not is_filled:
        current_turn_info = step
        active_step_index = i
        break

if current_turn_info is None:
    current_turn_info = ('done', 'none', -1)

# --- Helper ---
def get_unavailable():
    used = []
    for x in st.session_state.blue_bans + st.session_state.red_bans + st.session_state.blue_picks + st.session_state.red_picks:
        if x is not None: used.append(x)
    return used

# --- UI LAYOUT ---
col_blue, col_mid, col_red = st.columns([1, 1.2, 1])

def render_slot(col, team_name, phase, idx, key_prefix):
    if team_name == 'Blue':
        current_val = st.session_state.blue_bans[idx] if phase == 'ban' else st.session_state.blue_picks[idx]
    else:
        current_val = st.session_state.red_bans[idx] if phase == 'ban' else st.session_state.red_picks[idx]

    is_my_turn = (current_turn_info == (phase, team_name, idx))
    disabled = not is_my_turn and (current_val is None)
    if current_val is not None: disabled = True

    label = f"{'🚫' if phase == 'ban' else '⚔️'} {phase.title()} {idx+1}"
    
    options = get_unavailable()
    valid_options = [h for h in all_heroes if h not in options]
    if current_val: valid_options = [current_val]
    
    placeholder = "Menunggu Giliran..." if disabled and current_val is None else f"Pilih {phase.title()}..."
    
    with col:
        selection = st.selectbox(
            label, 
            options=valid_options,
            index=0 if current_val else None,
            placeholder=placeholder,
            disabled=disabled,
            key=f"{key_prefix}_{phase}_{idx}",
            label_visibility="collapsed"
        )
        
        if selection and selection != current_val and is_my_turn:
            if team_name == 'Blue':
                if phase == 'ban': st.session_state.blue_bans[idx] = selection
                else: st.session_state.blue_picks[idx] = selection
            else:
                if phase == 'ban': st.session_state.red_bans[idx] = selection
                else: st.session_state.red_picks[idx] = selection
            st.rerun()

# === RENDER TEAM COLUMNS ===
with col_blue:
    st.markdown('<div class="team-box blue-bg">TIM ANDA (BLUE)</div>', unsafe_allow_html=True)
    st.caption("🚫 Bans")
    for i in range(5): render_slot(col_blue, 'Blue', 'ban', i, 'b')
    st.caption("⚔️ Picks")
    for i in range(5): render_slot(col_blue, 'Blue', 'pick', i, 'b')

with col_red:
    st.markdown('<div class="team-box red-bg">MUSUH (RED)</div>', unsafe_allow_html=True)
    st.caption("🚫 Bans")
    for i in range(5): render_slot(col_red, 'Red', 'ban', i, 'r')
    st.caption("⚔️ Picks")
    for i in range(5): render_slot(col_red, 'Red', 'pick', i, 'r')

# === ANALISIS AI (MIDDLE) ===
with col_mid:
    st.markdown("<h3 style='text-align: center'>🧠 Analisis Strategi</h3>", unsafe_allow_html=True)
    
    cur_phase, cur_team, cur_idx = current_turn_info
    
    if cur_phase != 'done':
        if cur_team == 'Blue':
            st.markdown(f'<div class="turn-indicator">GILIRAN ANDA MEMILIH ({cur_phase.upper()})</div>', unsafe_allow_html=True)
            
            st.markdown('<div class="rec-box">', unsafe_allow_html=True)
            my_picks = [x for x in st.session_state.blue_picks if x]
            en_picks = [x for x in st.session_state.red_picks if x]
            all_banned = [x for x in st.session_state.blue_bans + st.session_state.red_bans if x]
            
            if cur_phase == 'ban':
                st.markdown("#### 💡 Saran Ban")
                recs = recommender.recommend_dynamic_ban(my_picks, en_picks, all_banned)
                if recs:
                    for r in recs[:3]:
                        st.write(f"🚫 **{r['hero']}**: {r['reason']}")
                else:
                    st.caption("Belum ada data cukup untuk rekomendasi.")
            
            elif cur_phase == 'pick':
                st.markdown("#### 💡 Saran Pick")
                recs = recommender.recommend_dynamic_pick(my_picks, en_picks, all_banned)
                if recs:
                    for i, r in enumerate(recs[:3]):
                        st.markdown(f"**{i+1}. {r['hero']}**")
                        st.caption(f"Reason: {r['reason']}")
                else:
                    st.caption("Pilih hero power atau meta.")
            
            st.markdown('</div>', unsafe_allow_html=True)
            
        else:
            st.warning(f"⏳ Menunggu Musuh memilih {cur_phase}...")
            st.caption("Analisis akan muncul setelah musuh memilih.")
    else:
        st.success("Draft Selesai!")

    # Prediksi Win Rate
    st.divider()
    b_picks_clean = [x for x in st.session_state.blue_picks if x]
    r_picks_clean = [x for x in st.session_state.red_picks if x]
    
    if b_picks_clean and r_picks_clean:
        win_prob = predictor.predict_win_rate(b_picks_clean, r_picks_clean)
        st.metric("Peluang Menang Anda", f"{win_prob:.1%}")
        st.progress(win_prob)
        
        if win_prob > 0.55: st.success("Tim Anda Unggul!")
        elif win_prob < 0.45: st.error("Musuh Unggul!")
        else: st.info("Seimbang")