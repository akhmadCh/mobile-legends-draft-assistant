import requests
import pandas as pd
import time
import random
import json

# --- KONFIGURASI API ---
url = "https://mlbb.io/api/hero/counter-pick-suggestions"

# Headers sesuai data cURL kamu
# PENTING: Jika error 401/403, update bagian 'cookie' dengan yang baru dari browser
headers = {
    'accept': 'application/json, text/plain, */*',
    'accept-language': 'id,en-US;q=0.9,en;q=0.8',
    'content-type': 'application/json',
    'origin': 'https://mlbb.io',
    'referer': 'https://mlbb.io/counter-pick',
    'user-agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Mobile Safari/537.36',
    'x-client-secret': '259009191be734535393edc59e865dce',
    'cookie': 'locale=id; _pk_id.1.cb63=7430911a8e4116dc.1764948894.; __Host-next-auth.csrf-token=b82a0e26175a80f7a59b9b106c55509bfa30cb10ebe33c2360e6b02b842a6c0e%7C0d767532a2e000026f99b5c99e4e3f60247b57bdffa0d79b7c7805de3beaa5da; __Secure-next-auth.callback-url=https%3A%2F%2Fmlbb.io%2Fhero-tier; sub_data=U2FsdGVkX19dNxTgrx+Uwmx05Axpa+l72U4wYvs6GJLrldY03Aa8ObIIV0x6g4SUpgjFVzIr+2yV8Tg8TyqQFqOaqsxZqJksueX7M4SaKFpcqVRQp45Qcq017BMf2+EhEfYT5GT/gGeUet+DYgzuYxiL1OROck7mQSrTH4ZXgYM=; _pk_ref.1.cb63=%5B%22%22%2C%22%22%2C1765607430%2C%22https%3A%2F%2Faccounts.google.com%2F%22%5D; _pk_ses.1.cb63=1; __Secure-next-auth.session-token=eyJhbGciOiJkaXIiLCJlbmMiOiJBMjU2R0NNIn0..nDJBcet0gl-Hjpyi.GT-mz_ZORUOwWxHVWnjFYx5q2tLKHey2qeUdd4BXBBGf6eC12j9BsW1rNQyr_Nnpf2DlbbeToyxrsniH5tmLZRB9DCuJk9w60kM2riP6V92Nk9GJduZI8Ri038sTsNmf3d2HHQRpTz9rmJvPVF9oS6yK8V854vkdpXqD6d0Po_RLPhMuiLB5kKSIhrnZqu8xo9ooaOOw8f1ZynjO2XkercvCTgWidIGTIAlmzwJPA3POJtaW_1aN0lPJangSXIoiM5FZ4rnn_prMgvSwYlut6V9ARlXGByqxsTo99wXY_82UcAAWulDMWF3oQVUelzfYq3STcwMcX7oAk2RxCPGv8IP3_rp-AzFaQ-q468UoWyRNx_39cYGYWlxq6_TA9ys-UC4s9T-gzsDTLg2VJ51wKq2GMqys_phlhRBSNlMIfISFt-nyrzw6tzeWFGIi47sm6Xu9MkaDc66BtWdpMjEb.hfE2F4e2cQLZUK_X9Ku9kQ'
}

all_counter_data = []

print("Memulai proses pengambilan data counter...")

# Range ID Hero (Estimasi 1 - 125)
for target_id in range(1, 130): 
    payload = {"enemyHeroes": [target_id]}
    
    try:
        response = requests.post(url, headers=headers, json=payload)
        
        if response.status_code == 200:
            json_data = response.json()
            
            # Cek apakah ada data di dalam key 'data'
            if 'data' in json_data and len(json_data['data']) > 0:
                
                # Kita ambil nama Target Hero (Musuh) dari data pertama agar CSV rapi
                # Ambil dari dalam list 'counteredHeroes'
                target_hero_name = "Unknown"
                first_item = json_data['data'][0]
                if 'counteredHeroes' in first_item and len(first_item['counteredHeroes']) > 0:
                    target_hero_name = first_item['counteredHeroes'][0].get('name', 'Unknown')

                print(f"[OK] ID {target_id} ({target_hero_name}) - Ditemukan {len(json_data['data'])} counter.")
                
                # Loop semua hero counter yang direkomendasikan
                for item in json_data['data']:
                    
                    # --- PERBAIKAN KUNCI DI SINI ---
                    # Menggabungkan list role/lane/speciality menjadi string pakai koma
                    roles = ", ".join(item.get('role', []))
                    lanes = ", ".join(item.get('lane', []))
                    specs = ", ".join(item.get('speciality', []))
                    
                    record = {
                        'Target_ID': target_id,
                        'Target_Name': target_hero_name,       # Nama Musuh (Misal: Aamon)
                        'Counter_Name': item.get('heroName'),  # Nama Counter (Misal: Gloo) -> SEBELUMNYA SALAH DISINI
                        'Score': item.get('score'),
                        'Tier': item.get('tier'),
                        'Role': roles,
                        'Lane': lanes,
                        'Speciality': specs
                    }
                    all_counter_data.append(record)
            else:
                # Hero ID tidak valid atau tidak ada data
                pass
        else:
            print(f"[FAIL] ID {target_id} Status Code: {response.status_code}")
            
    except Exception as e:
        print(f"[ERROR] ID {target_id}: {e}")
        
    # Jeda waktu agar aman
    time.sleep(random.uniform(0.5, 1.2))

# --- SIMPAN DATA ---
if all_counter_data:
    df = pd.DataFrame(all_counter_data)
    
    output_filename = 'data_counter_mlbb_final.csv'
    df.to_csv(output_filename, index=False)
    
    print("\n" + "="*50)
    print(f"SELESAI! Data berhasil disimpan ke: {output_filename}")
    print(f"Total Baris Data: {len(df)}")
    print("="*50)
    print(df.head())
else:
    print("\nTidak ada data yang tersimpan. Cek cookie atau koneksi.")