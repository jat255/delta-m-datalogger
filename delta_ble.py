import os
import time
from argparse import ArgumentParser
from statistics import mean
from collections import defaultdict
from tqdm import tqdm
from binascii import hexlify, unhexlify
from datetime import datetime, timezone, timedelta
import logging
import json
from influxdb import InfluxDBClient
from dotenv import load_dotenv
import pygatt
from ble import *

FILE_DIR = os.path.dirname(os.path.abspath(__file__))
DOTENV_PATH = os.path.join(FILE_DIR, '.env')
load_dotenv(dotenv_path=DOTENV_PATH)


def get_args():
    arg_parser = ArgumentParser(description="Log Delta Inverter data")
    # arg_parser.add_argument('--mac_address',
    #                         help="MAC address of device to connect")
    arg_parser.add_argument('-v', '--verbose', action='count',
                            default=0)
    args = arg_parser.parse_args()
    return args


def setup_logging(args):
    if args.verbose > 2:
        args.verbose = 2

    verbosity_map = {
        0: logging.WARN,
        1: logging.INFO,
        2: logging.DEBUG
    }
    logging.basicConfig()
    logging.getLogger('pygatt').setLevel(verbosity_map[args.verbose])
    logging.getLogger(__name__).setLevel(verbosity_map[args.verbose])


class DeltaSolarBLE:
    ignored_messages = [
        "SerialNumber", "Unknown", "SystemVersion", "Timezone",
        "PowerVersion", "SafetyVersion", "HasRGM", "CurrentPowerError",
        "CurrentSafetyError", "SyncDetail", "SynchronizationProgress",
        "SynchronizationLength", "CheckData2"]

    def __init__(self, write_out=False):
        self.data = defaultdict(list)
        self.last_message_title = ""
        self.mac = os.getenv('MAC_ADDRESS')
        self.adapter = pygatt.GATTToolBackend(search_window_size=2048)
        self.dt = datetime.now(timezone.utc)
        self.write_out = write_out

    def handle_data(self, handle, value):
        """
        handle -- integer, characteristic read handle the data was received on
        value -- bytearray, the data returned in the notification
        """
        if self.last_message_title in self.ignored_messages:
            return

        try:
            message = getMessageContent_bytes(value)
        except:
            message = ""
        if message:
            logging.info(
                f"Received data: {value} -- {hexlify(value)} -- {message}")
            self.data[self.last_message_title].append(message)
            logging.info(f"Appending to {self.last_message_title} data!")

    def get_data(self):
        try:
            self.adapter.start()
            device = self.adapter.connect(self.mac)
            uuid_write = "ee6d7171-88c6-11e8-b444-060400ef5315"
            uuid_notify = "ee6d7172-88c6-11e8-b444-060400ef5315"

            logging.info(f"Subscribing to: {uuid_notify}")
            device.subscribe(uuid_notify,
                             callback=self.handle_data,
                             indication=False)

            for h in tqdm(sent_values):
                if h in message_string_by_hex:
                    name = message_string_by_hex[h]
                else:
                    name = 'Unknown'

                time.sleep(.20)
                logging.info(f"Writing {name} : {h}")
                self.last_message_title = name
                device.char_write(uuid_write, unhexlify(h), False)

        except Exception as e:
            logging.error(f"Received exception: {e}")
        finally:
            self.adapter.stop()

    def process_data(self):
        # the inverter sends two copies of each value, so I assume
        # that is so they can be averaged over a short time
        self.data = {k: mean(v) for k, v in self.data.items()}

        # add time:
        self.data['timestamp'] = self.dt.isoformat()

        # make some unit corrections:
        for k, v in self.data.items():
            if v == -1:
                self.data[k] = None
            elif k == 'timestamp':
                pass
            elif 'Current' in k:
                # current values need to be divided by 10 to get to Amps
                self.data[k] /= 10.0
            elif 'Frequency' in k:
                self.data[k] /= 100.0
            elif 'Energy' in k or 'Power' in k:
                # energy is given in Wh and power in W, so convert to kWh/kW
                self.data[k] /= 1000.0
            else:
                self.data[k] = float(v)

    def write_data(self):
        with open(self.dt.strftime("%Y%m%d_%H%M.json"), 'w') as f:
            json.dump(self.data, f, indent=2)

    def post_data(self):
        if not all(key in os.environ for key in \
                   ('INFLUX_HOST', 'INFLUX_PORT', 'INFLUX_USER',
                    'INFLUX_PASS', 'INFLUX_DB')):
            raise EnvironmentError("One or more of the environment variables "
                                   "required for the InfluxDB connection was "
                                   "not found in the environment. Check the "
                                   ".env file for the values INFLUX_HOST, "
                                   "INFLUX_PORT, INFLUX_USER, INFLUX_PASS, "
                                   "and INFLUX_DB and try again")
        client = InfluxDBClient(host=os.getenv('INFLUX_HOST'),
                                port=int(os.getenv('INFLUX_PORT')),
                                username=os.getenv('INFLUX_USER'),
                                password=os.getenv('INFLUX_PASS'),
                                database=os.getenv('INFLUX_DB'))

        # with open(os.path.join(os.path.dirname(__file__),
        #                        '20210209_1039.json'), 'r') as f:
        #     data = json.load(f)

        # get last value of "DailyEnergy" for yesterday
        try:
            y_dt = datetime.now() - timedelta(days=1)
            y_beg = y_dt.replace(hour=0, minute=0, second=0, microsecond=0)
            y_beg_utc = datetime.utcfromtimestamp(y_beg.timestamp())
            y_end = y_dt.replace(hour=23, minute=59, second=59, microsecond=999999)
            y_end_utc = datetime.utcfromtimestamp(y_end.timestamp())
            data = list(client.query(f'select median("DailyEnergy") FROM inverter_data WHERE time ' + \
                                     f'> \'{y_beg_utc.strftime("%Y-%m-%d %H:%M:%S")}\' AND ' + \
                                     f'time < \'{y_end_utc.strftime("%Y-%m-%d %H:%M:%S")}\' ' + \
                                     f'GROUP BY time(15m)')[('inverter_data', None)])
            last_value = [x['median'] for x in data if x['median']][-1]
            self.data["TodaysEnergy"] = self.data["DailyEnergy"] - last_value
        except Exception as e:
            pass

        json_body = [
            {
                "measurement": "inverter_data",
                "tags": {
                    "model": "M4-TL-US"
                },
                "time": self.data.pop('timestamp'),
                "fields": self.data
            }
        ]
        client.create_database(os.getenv('INFLUX_DB'))
        print(f"uploading to influx: {json_body}")
        r = client.write_points(json_body)


if __name__ == "__main__":
    setup_logging(get_args())
    d = DeltaSolarBLE()
    d.get_data()
    d.process_data()
    
    if d.write_out:
        d.write_data()

    print(json.dumps(d.data, indent=2))

    d.post_data()
