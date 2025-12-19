import streamlit as st
import sys
import os

# Tambahkan path root agar bisa import module dari folder source
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from source.ml.predictor import DraftPredictor

st.set_page_config(page_title="MLBB Draft Assistant", layout="wide")
st.title("🛡️ MLBB Draft Pick Predictor ⚔️")

# Load Model (Hanya sekali saat app start)
@st.cache_resource
def load_predictor():
    return DraftPredictor()

try:
    predictor = load_predictor()
    st.success("Model ML berhasil dimuat!")
except Exception as e:
    st.error(f"Gagal memuat model: {e}")
    st.stop()

# --- UI Input ---
col1, col2 = st.columns(2)

with col1:
    st.subheader("Team Blue (First Pick)")
    # Ambil list nama hero dari feature names model (hilangkan prefix T1_)
    hero_list = [h.replace("T1_", "") for h in predictor.feature_names if h.startswith("T1_")]
    
    blue_team = st.multiselect("Pilih 5 Hero Tim Biru", options=hero_list, max_selections=5)

with col2:
    st.subheader("Team Red (Second Pick)")
    red_team = st.multiselect("Pilih 5 Hero Tim Merah", options=hero_list, max_selections=5)

# --- Prediksi ---
if st.button("Analisis Draft"):
    if len(blue_team) < 5 or len(red_team) < 5:
        st.warning("Harap pilih 5 hero untuk kedua tim agar prediksi akurat.")
    else:
        win_prob = predictor.predict_win_rate(blue_team, red_team)
        st.metric(label="Probabilitas Kemenangan Tim Biru", value=f"{win_prob:.2%}")
        
        if win_prob > 0.5:
            st.success("Tim Biru diprediksi MENANG! 🏆")
        else:
            st.error("Tim Biru diprediksi KALAH (Tim Merah Unggul).")