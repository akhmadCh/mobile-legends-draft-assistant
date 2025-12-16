import requests
import json

# --- CONFIG ---
url = "https://mlbb.io/api/hero/counter-pick-suggestions"

# Gunakan headers yang sama (pastikan cookie masih fresh/tidak expired)
headers = {
    'accept': 'application/json, text/plain, */*',
    'content-type': 'application/json',
    'origin': 'https://mlbb.io',
    'referer': 'https://mlbb.io/counter-pick',
    'user-agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Mobile Safari/537.36',
    'x-client-secret': '259009191be734535393edc59e865dce', 
    # COPY PASTE COOKIE TERBARU DARI CURL JIKA YANG LAMA SUDAH ERROR
    'cookie': 'locale=id; _pk_id.1.cb63=7430911a8e4116dc.1764948894.; __Host-next-auth.csrf-token=b82a0e26175a80f7a59b9b106c55509bfa30cb10ebe33c2360e6b02b842a6c0e%7C0d767532a2e000026f99b5c99e4e3f60247b57bdffa0d79b7c7805de3beaa5da; __Secure-next-auth.callback-url=https%3A%2F%2Fmlbb.io%2Fhero-tier; sub_data=U2FsdGVkX19dNxTgrx+Uwmx05Axpa+l72U4wYvs6GJLrldY03Aa8ObIIV0x6g4SUpgjFVzIr+2yV8Tg8TyqQFqOaqsxZqJksueX7M4SaKFpcqVRQp45Qcq017BMf2+EhEfYT5GT/gGeUet+DYgzuYxiL1OROck7mQSrTH4ZXgYM=; _pk_ref.1.cb63=%5B%22%22%2C%22%22%2C1765607430%2C%22https%3A%2F%2Faccounts.google.com%2F%22%5D; _pk_ses.1.cb63=1; __Secure-next-auth.session-token=eyJhbGciOiJkaXIiLCJlbmMiOiJBMjU2R0NNIn0..nDJBcet0gl-Hjpyi.GT-mz_ZORUOwWxHVWnjFYx5q2tLKHey2qeUdd4BXBBGf6eC12j9BsW1rNQyr_Nnpf2DlbbeToyxrsniH5tmLZRB9DCuJk9w60kM2riP6V92Nk9GJduZI8Ri038sTsNmf3d2HHQRpTz9rmJvPVF9oS6yK8V854vkdpXqD6d0Po_RLPhMuiLB5kKSIhrnZqu8xo9ooaOOw8f1ZynjO2XkercvCTgWidIGTIAlmzwJPA3POJtaW_1aN0lPJangSXIoiM5FZ4rnn_prMgvSwYlut6V9ARlXGByqxsTo99wXY_82UcAAWulDMWF3oQVUelzfYq3STcwMcX7oAk2RxCPGv8IP3_rp-AzFaQ-q468UoWyRNx_39cYGYWlxq6_TA9ys-UC4s9T-gzsDTLg2VJ51wKq2GMqys_phlhRBSNlMIfISFt-nyrzw6tzeWFGIi47sm6Xu9MkaDc66BtWdpMjEb.hfE2F4e2cQLZUK_X9Ku9kQ'
}

# Payload untuk hero Aamon (ID 109)
payload = {"enemyHeroes": [109]}

try:
    response = requests.post(url, headers=headers, json=payload)
    data = response.json()
    
    # Ambil 1 data counter pertama saja untuk dicek
    if 'data' in data and len(data['data']) > 0:
        first_item = data['data'][0]
        print("=== ISI DATA MENTAH DARI API (COPY INI) ===")
        print(json.dumps(first_item, indent=4))
    else:
        print("Data kosong atau error auth.")
        
except Exception as e:
    print(f"Error: {e}")