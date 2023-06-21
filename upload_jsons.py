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

b = delta_ble.DeltaSolarBLE()

# edit this glob as needed
for p in list(Path('.').glob('20230621*.json')):
    with p.open() as f:
        data = json.load(f)
        b.data = data
        b.dt = datetime.datetime.strptime(p.name, "%Y%m%d_%H%M.json")
    b.data['timestamp'] = b.dt.isoformat()
    b.post_data()
