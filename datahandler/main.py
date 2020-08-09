import time

from lib.data_netatmo import Netatmo
from lib.data_luftdaten import Luftdaten
from lib.data_uba import UBA

def main():

    netatmo = Netatmo()
    netatmo.create_history()

    luftdaten = Luftdaten()
    luftdaten.create_history()

    uba       = UBA()
    uba.create_history()

    # # Manually write all data from files to DB
    # netatmo.write_complete_history_to_db()
    # luftdaten.write_complete_history_to_db()
    # uba.write_complete_history_to_db()

    while True:
        uba.update_latest_data()
        netatmo.update_latest_data()
        luftdaten.update_latest_data()

        time.sleep(60)
        print('sleep 60s...')

main()
