import os
import time
import sys
from argparse import ArgumentParser
from statistics import mean
from collections import defaultdict
from tqdm import tqdm
from binascii import hexlify, unhexlify
from datetime import datetime, timezone, timedelta
import logging
import json

from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS
from dotenv import load_dotenv

import pygatt
from ble import *

FILE_DIR = os.path.dirname(os.path.abspath(__file__))
DOTENV_PATH = os.path.join(FILE_DIR, '.env')
load_dotenv(dotenv_path=DOTENV_PATH)
ble_logger = logging.getLogger()

def get_args():
    arg_parser = ArgumentParser(description="Log Delta Inverter data")
    # arg_parser.add_argument('--mac_address',
    #                         help="MAC address of device to connect")
    arg_parser.add_argument('-v', '--verbose', action='count',
                            default=0)
    args = arg_parser.parse_args()
    return args


def setup_logging(args):
    if args.verbose > 3:
        args.verbose = 3

    verbosity_map = {
        0: logging.WARN,
        1: logging.INFO,
        2: logging.DEBUG,
        3: logging.DEBUG
    }
    logging.basicConfig(level=logging.DEBUG)
    logging.getLogger('pygatt').setLevel(verbosity_map[args.verbose - 1])
    ble_logger.setLevel(verbosity_map[args.verbose])


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
            ble_logger.debug(
                f"Received data: {value} -- {hexlify(value)} -- {message}")
            self.data[self.last_message_title].append(message)
            ble_logger.debug(f"Appending to {self.last_message_title} data!")

    def get_data(self):
        try:
            self.adapter.start()
            device = self.adapter.connect(self.mac)
            uuid_write = "ee6d7171-88c6-11e8-b444-060400ef5315"
            uuid_notify = "ee6d7172-88c6-11e8-b444-060400ef5315"

            ble_logger.debug(f"Subscribing to: {uuid_notify}")
            device.subscribe(uuid_notify,
                             callback=self.handle_data,
                             indication=False)
            for h in sent_values:
                if h in message_string_by_hex:
                    name = message_string_by_hex[h]
                else:
                    name = 'Unknown'

                time.sleep(.20)
                ble_logger.debug(f"Writing {name} : {h}")
                self.last_message_title = name
                device.char_write(uuid_write, unhexlify(h), False)

        except Exception as e:
            ble_logger.error(f"Received exception getting data via BT-LE: {e}")
            self.adapter.stop()
            raise e
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
                   ('INFLUX_HOST', 'INFLUX_PORT', 'INFLUX_TOKEN',
                    'INFLUX_BUCKET', 'INFLUX_ORG')):
            raise EnvironmentError("One or more of the environment variables "
                                   "required for the InfluxDB connection was "
                                   "not found in the environment. Check the "
                                   ".env file for the values INFLUX_HOST, "
                                   "INFLUX_PORT, INFLUX_TOKEN, INFLUX_ORG and INFLUX_BUCKET "
                                   "and try again")
        with InfluxDBClient(
            url=f"http://{os.getenv('INFLUX_HOST')}:{int(os.getenv('INFLUX_PORT'))}",
            token=os.getenv('INFLUX_TOKEN'), org=os.getenv('INFLUX_ORG')) as client:

            write_api = client.write_api(write_options=SYNCHRONOUS)
            query_api = client.query_api()

            # with open(os.path.join(os.path.dirname(__file__),
            #                        '20210209_1039.json'), 'r') as f:
            #     data = json.load(f)

            # get last value of "DailyEnergy" for yesterday
            try:
                query = f"""
                from(bucket: "{os.getenv('INFLUX_BUCKET')}")
                    |> range(start: -2d, stop: -1d)
                    |> filter(fn: (r) => r["_measurement"] == "inverter_data")
                    |> filter(fn: (r) => r["_field"] == "DailyEnergy")
                    |> last()
                """
                data = query_api.query(query)
                last_value = data[0].records[0].get_value()
                print(f"last_value was {last_value}")
                self.data["TodaysEnergy"] = self.data["DailyEnergy"] - last_value
                if self.data["TodaysEnergy"] < 0:
                    self.data["TodaysEnergy"] = self.data["DailyEnergy"]
            except Exception as e:
                ble_logger.warning(f'Exception calculating "TodaysEnergy": {e}')
                pass

            # filter out bad values that sometimes get in
            if "TodaysEnergy" in self.data and "DailyEnergy" in self.data and \
              self.data["DailyEnergy"] and self.data["TodaysEnergy"] > 50:
                ble_logger.warning(f'Deleting bad Energy value: {self.data["DailyEnergy"]}')
                del self.data["TodaysEnergy"]
                del self.data["DailyEnergy"]
            if "Power" in self.data and self.data["Power"] and self.data["Power"] > 5:
                ble_logger.warning(f"Deleting bad Power value: {self.data['Power']}")
                del self.data["Power"]
            if "PV1Voltage" in self.data and self.data["PV1Voltage"] and self.data["PV1Voltage"] > 400:
                ble_logger.warning(f"Deleting bad PV1Voltage value: {self.data['PV1Voltage']}")
                del self.data["PV1Voltage"]
            if "PV2Voltage" in self.data and self.data["PV2Voltage"] and self.data["PV2Voltage"] > 400:
                ble_logger.warning(f"Deleting bad PV2Voltage value: {self.data['PV2Voltage']}")
                del self.data["PV2Voltage"]

            json_body = [
                {
                    "measurement": "inverter_data",
                    "tags": {
                        "model": "M6-TL-US"
                    },
                    "time": self.data.pop('timestamp'),
                    "fields": self.data
                }
            ]
            ble_logger.info(f"uploading to influx: {json_body}")
            write_api.write(os.getenv('INFLUX_BUCKET'), os.getenv('INFLUX_ORG'), json_body)


if __name__ == "__main__":
    setup_logging(get_args())
    ble_logger.info(f"Set up logging @ {datetime.now().astimezone().isoformat()}")
    # ble_logger.info("Creating DeltaSolarBLE")
    d = DeltaSolarBLE()
    ble_logger.info("Getting data from inverter")
    try:
        d.get_data()
    except Exception as e:
        ble_logger.error("Exiting early due to error getting data from inverter!")
        sys.exit(1)
    ble_logger.info("Processing data")
    d.process_data()

    if d.write_out:
        ble_logger.info("Writing data")
        d.write_data()

    ble_logger.info("Data read from inverter:")
    ble_logger.info(json.dumps(d.data, indent=2))

    ble_logger.info(f"Posting data to InfluxDB")
    d.post_data()
    ble_logger.info(f"Exiting after successful run\n")
