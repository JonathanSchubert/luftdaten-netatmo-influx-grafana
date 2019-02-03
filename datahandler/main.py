import time

from lib.data_luftdaten import Luftdaten
from lib.data_uba import UBA

def main():

    luftdaten = Luftdaten()
    luftdaten.update_data_complete()

    uba       = UBA()
    uba.update_data_complete()

    while True:
        luftdaten.update_data_today()
        uba.update_data_today()

        time.sleep(120)

main()
