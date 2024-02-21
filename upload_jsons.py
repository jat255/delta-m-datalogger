"""
This script will glob a number of json files (written by the 
`delta_ble.DeltaSolarBLE.dump_data` method) and upload them to the
InfluxDB instance using the same mechanism as the original script.

This is useful if for some reason the InfluxDB server was down and
the data was written to disk, rather than to the DB.

Written: 2023-06-21 13:39
"""

import delta_ble
import json
from pathlib import Path
import datetime
import logging
from rich.console import Console
from rich.prompt import Prompt
from rich.logging import RichHandler
import sys

logging.basicConfig(level=logging.DEBUG, handlers=[RichHandler(rich_tracebacks=True)])
logger = logging.getLogger()

c = Console()
b = delta_ble.DeltaSolarBLE()

# edit this glob as needed
for p in sorted(list(Path('.').glob('20240221*.json'))):
    try:
        with p.open() as f:
            data = json.load(f)
            b.data = data
            b.dt = datetime.datetime.strptime(p.name, "%Y%m%d_%H%M.json")
            b.dt = b.dt.replace(tzinfo=datetime.timezone.utc)
        time_str = b.dt.isoformat()
        b.data['timestamp'] = time_str
        c.print(b.data)
        answer = Prompt.ask("Okay?", choices=["Yes", "Skip", "Stop"], default="Yes")
        if answer.lower() == 'yes':
            b.post_data()
        elif answer.lower() == 'stop':
            sys.exit(1)
        else:
            continue
    except Exception as e:
        c.print_exception(show_locals=True)
        raise e
