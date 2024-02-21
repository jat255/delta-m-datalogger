import os
import time
import sys
from argparse import ArgumentParser
from statistics import mean
from collections import defaultdict
from tqdm import tqdm
from binascii import hexlify, unhexlify
from datetime import datetime, timezone, timedelta
import smtplib
import socket
import logging
import json
import asyncio
from rich.console import Console
from rich.logging import RichHandler

from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS
from dotenv import load_dotenv

import pygatt
from bleak import BleakClient, BleakGATTCharacteristic
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
    logging.basicConfig(level=logging.DEBUG, handlers=[RichHandler(rich_tracebacks=True)])
    sub_level = verbosity_map[args.verbose - 1] if args.verbose > 0 else 0
    logging.getLogger('urllib3.connectionpool').setLevel(sub_level)
    logging.getLogger('pygatt').setLevel(sub_level)
    logging.getLogger('bleak.backends').setLevel(sub_level)
    ble_logger.setLevel(verbosity_map[args.verbose])

    # set up loki logging if configured
    if "LOKI_ENDPOINT" in os.environ:
        import logging_loki
        loki_handler = logging_loki.LokiHandler(
            url=os.environ.get("LOKI_ENDPOINT"),
            tags={"application": "delta-solar-tracker"},
            version="1"
        )
        ble_logger.addHandler(loki_handler)

class DeltaSolarBLE:
    ignored_messages = [
        "SerialNumber", "Unknown", "SystemVersion", "Timezone",
        "PowerVersion", "SafetyVersion", "HasRGM", "CurrentPowerError",
        "CurrentSafetyError", "SyncDetail", "SynchronizationProgress",
        "SynchronizationLength", "CheckData2"]
    UUID_WRITE = "ee6d7171-88c6-11e8-b444-060400ef5315"
    UUID_NOTIFY = "ee6d7172-88c6-11e8-b444-060400ef5315"

    def __init__(self, write_out=False):
        self.data = defaultdict(list)
        self.last_message_title = ""
        self.mac = os.getenv('MAC_ADDRESS')
        self.adapter = pygatt.GATTToolBackend(search_window_size=2048)
        self.dt = datetime.now(timezone.utc)
        self.write_out = write_out
        self.logger = logging.getLogger('DeltaSolarBLE')

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

    def handle_data_bleak(self, sender: BleakGATTCharacteristic, data: bytearray):
        """
        handle -- integer, characteristic read handle the data was received on
        value -- bytearray, the data returned in the notification
        """
        if self.last_message_title in self.ignored_messages:
            return

        try:
            message = getMessageContent_bytes(data)
        except:
            message = ""
        if message:
            ble_logger.debug(
                f"Received data: {data} -- {hexlify(data)} -- {message}")
            self.data[self.last_message_title].append(message)
            ble_logger.debug(f"Appending to {self.last_message_title} data!")


    def send_alert_email(
        self,
        subject="Delta Solar Tracker data log",
        msg=""
    ):
        for var in ['EMAIL_SMTP_HOST', 'EMAIL_SMTP_PORT', 'EMAIL_TO_ADDRESS']:
            if os.getenv(var) is None:
                self.logger.warning(
                    f"{var} environment variable was not defined, so cannot send email"
                )
            return
        try:
            smtpObj = smtplib.SMTP(
                os.getenv('EMAIL_SMTP_HOST'),
                os.getenv('EMAIL_SMTP_PORT')
            )
            from_address = f"delta_solar_tracker@{socket.gethostname()}"
            message = f"""From: Delta Solar Tracker <{from_address}>
Subject: {subject}

{msg}

{json.dumps(self.data, indent=2)}
"""

            smtpObj.sendmail(
                from_address,
                os.getenv('EMAIL_TO_ADDRESS').split(','),
                message
            )
            self.logger.info("Successfully sent email")
        except Exception as e:
            self.logger.error("Unable to send email:")
            self.logger.error(e)

    async def get_data_bleak(self):
        async with BleakClient(self.mac) as client:
            ble_logger.debug(f"starting Bleak get_data")
            await client.start_notify(self.UUID_NOTIFY, self.handle_data_bleak)
            for h in sent_values:
                if h in message_string_by_hex:
                    name = message_string_by_hex[h]
                else:
                    name = 'Unknown'

                time.sleep(.50)
                ble_logger.debug(f"Writing {name} : {h}")
                self.last_message_title = name
                await client.write_gatt_char(self.UUID_WRITE, unhexlify(h), response=True)
            await client.stop_notify(self.UUID_NOTIFY)

    def get_data_pygatt(self):
        """
        This is a deprecated function that stopped working sometimne around Feb. 2024 on my
        Raspberry Pi, probably due to gatttool being deprecated on Linux. Use get_data_bleak() instead
        """
        try:
            ble_logger.debug(f"starting pygatt adapter")
            self.adapter.start()
            ble_logger.debug(f"connecting to {self.mac}")
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
            ble_logger.error(f"Received exception getting data via BT-LE: {e.__repr__()}")
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
                today = datetime.now().date()
                today_midnight = datetime(today.year, today.month, today.day, 
                                          0, 0, 0)
                stop = today_midnight.astimezone().isoformat()
                yesterday = today + timedelta(days=-1)
                yesterday_midnight = datetime(yesterday.year, yesterday.month, yesterday.day, 
                                              0, 0, 0)
                start = yesterday_midnight.astimezone().isoformat()
                query = f"""
                from(bucket: "{os.getenv('INFLUX_BUCKET')}")
                    |> range(start: {start}, stop: {stop})
                    |> filter(fn: (r) => r["_measurement"] == "inverter_data")
                    |> filter(fn: (r) => r["_field"] == "DailyEnergy")
                    |> last()
                """
                data = query_api.query(query)
                yesterday_last_value = data[0].records[0].get_value()
                yesterday_last_time = data[0].records[0].get_time()
                ble_logger.info(f"Yesterday's last energy value was {yesterday_last_value}")
                ble_logger.info(f"Caculating TodaysEnergy")
                self.data["TodaysEnergy"] = self.data["DailyEnergy"] - yesterday_last_value
                if self.data["TodaysEnergy"] < 0:
                    ble_logger.warning(f"TodaysEnergy was negative: {self.data['TodaysEnergy']}, so setting to DailyEnergy: {self.data['DailyEnergy']}")
                    self.data["TodaysEnergy"] = self.data["DailyEnergy"]

                # get most recent "DailyEnergy" value by looking at last 3 hours
                ble_logger.debug("Creating timedelta")
                diff = timedelta(hours=3)
                query = f"""
                from(bucket: "{os.getenv('INFLUX_BUCKET')}")
                    |> range(
                        start: {(datetime.now() - diff).astimezone().isoformat()}, 
                        stop: {datetime.now().astimezone().isoformat()})
                    |> filter(fn: (r) => r["_measurement"] == "inverter_data")
                    |> filter(fn: (r) => r["_field"] == "DailyEnergy")
                    |> last()
                """
                # ble_logger.debug(f"Query is: \"{query}\"")
                data = query_api.query(query)
                ble_logger.debug("Executed query")
                if not data:
                    # maybe first run of the day, so there's no data yet today
                    most_recent_DailyEnergy = yesterday_last_value
                    most_recent_time = yesterday_last_time
                    ble_logger.info(f"Using yesterday's last value as most recent energy/time: {yesterday_last_time}/{yesterday_last_value} kWh")
                else:
                    data_0 = data[0]
                    ble_logger.info(f"data_0 value was {data_0}")
                    most_recent_DailyEnergy = data_0.records[0].get_value()
                    most_recent_time = data_0.records[0].get_time()
                current_time = datetime.fromisoformat(self.data['timestamp'])
                ble_logger.info(f"Most recent DailyEnergy value was {most_recent_DailyEnergy}")
                ble_logger.info(f"Most recent time value was {most_recent_time}")

            except Exception as e:
                import web_pdb; web_pdb.set_trace()
                ble_logger.warning(f'Exception calculating "TodaysEnergy": {e}')
                pass

            # filter out bad values that sometimes get in

            # calculate rate of generation between this measurement and last
            if most_recent_time == yesterday_last_time:
                # if this is the first measurement of the day, set generation rate to 0
                generation_rate = 0
            else:
                time_diff_hours = (current_time - most_recent_time).seconds / 3600
                energy_diff_kWh = self.data["DailyEnergy"] - most_recent_DailyEnergy
                generation_rate = energy_diff_kWh / time_diff_hours
    
            ble_logger.info(f"Cacluated generation_rate was {generation_rate} kW")
            if "TodaysEnergy" in self.data and "DailyEnergy" in self.data and generation_rate > 5:
                ble_logger.warning(f'Deleting bad Energy values because rate was too high: DailyEnergy: {self.data["DailyEnergy"]}; TodaysEnergy: {self.data["TodaysEnergy"]}; generation_rate: {generation_rate}')
                del self.data["TodaysEnergy"]
                del self.data["DailyEnergy"]
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
    
    # tries three times before giving up
    do_loop = True
    tries = 0
    while do_loop:
        tries += 1
        try:
            asyncio.run(d.get_data_bleak())
            do_loop = False
        except Exception as e:
            if tries >= 3:
                ble_logger.error("Exiting early due to error getting data from inverter three times!")
                ble_logger.exception(e)
                sys.exit(1)
            else:
                ble_logger.error(f"Error getting data from inverter on attempt {tries}, will try again")
                ble_logger.exception(e)
            

    ble_logger.info("Processing data")
    d.process_data()

    if d.write_out:
        ble_logger.info("Writing data")
        d.write_data()

    ble_logger.info("Data read from inverter:")
    ble_logger.info(json.dumps(d.data, indent=2))

    ble_logger.info(f"Posting data to InfluxDB")
    try:
        d.post_data()
    except Exception as e:
        ble_logger.error(f"Error posting data to InfluxDB; writing to disk instead")
        d.write_data()
        ble_logger.error(f"Sending alert email")
        d.send_alert_email(
            subject="Could not upload data to InfluxDB!",
            msg="Delta solar tracker could not upload data to influxdb; including below so it is not lost"
        )
    ble_logger.info(f"Exiting after successful run\n")
