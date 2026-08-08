from python_files.polymarket_perps_stream import main as polymarket_perps_main
from python_files.polymarket_twap_stream import main as polymarket_twap_main

def main():
    # Start the Polymarket Perps WebSocket stream
    polymarket_perps_main()

    # Start the Polymarket TWAP WebSocket stream
    polymarket_twap_main()