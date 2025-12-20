from datetime import datetime

def get_timestamp():
   """return waktu saat ini untuk metadata"""
   return datetime.now().strftime("%Y-%m-%d %H:%M:%S")