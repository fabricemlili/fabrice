import time

def log(msg, level="INFO"):
    print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} | {level.upper():<8} | {msg}")