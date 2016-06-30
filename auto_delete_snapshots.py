from ovirtsdk.api import API
from ovirtsdk.xml import params
import ovirtsdk
from datetime import datetime, timedelta
import json
import argparse
import syslog
import sys


syslog.openlog("Snapshot Deletion Cron Job")
parser = argparse.ArgumentParser(description="-j path to json file")
parser.add_argument('-j', action='store', dest='jsonfile', required=True, help="path to json file")
args = parser.parse_args()
my_json_file = args.jsonfile


def main():
    syslog.syslog(syslog.LOG_INFO, "INFO: Starting Snapshot Deletion cron job")
    rhev_connection = connect_to_rhev()
    virtual_machines = get_all_vms(rhev_connection)
    snapshots = get_all_snapshots(virtual_machines)
    remove_snapshots(snapshots)
    disconnect_from_rhev(rhev_connection)
    syslog.syslog(syslog.LOG_INFO, "INFO: Completed Snapshot Deletion cron job")
    syslog.closelog()


def connect_to_rhev():
    try:
        with open(my_json_file) as data_file:
            data = json.load(data_file)
        my_url=data["rhevapiconnection"]["url"]
        my_username=data["rhevapiconnection"]["username"]
        my_password=data["rhevapiconnection"]["password"]
        my_ca_cert=data["rhevapiconnection"]["ca_cert"]
    except IOError as ex:
        syslog.syslog(syslog.LOG_CRIT, 'Could not load json file, error was: %s . Script will now end' % (ex))
        sys.exit()
    try:
        api = API (url=my_url,
                   username=my_username,
                   password=my_password,
                   ca_file=my_ca_cert)
        return api
    except ovirtsdk.infrastructure.errors.ConnectionError as ex:
        syslog.syslog(syslog.LOG_CRIT, 'Failed to connect to rhev, error was: %s . Script will now end' % (ex))
        sys.exit()


def get_all_vms(rhev_connection):
    virtual_machines = rhev_connection.vms.list()
    return virtual_machines


def get_all_snapshots(virtual_machines):
    snapshots={}
    for virtual_machine in virtual_machines:
        snapshots[virtual_machine] = virtual_machine.snapshots.list()
    return snapshots


def remove_snapshots(snapshots):
    three_days_ago = (datetime.now() - timedelta(days=3)).date()
    fourteen_days_ago = (datetime.now() - timedelta(days=14)).date()
    for virtual_machine, snapshots in snapshots.iteritems():
        for snapshot in snapshots:
            snapshot_id = snapshot.get_id()
            snapshot_date = snapshot.get_date().date()
            snapshot_description = (snapshot.get_description()).lower()
            try:
                if "active" not in snapshot_description and "keep" not in snapshot_description and snapshot_date < three_days_ago:
                    syslog.syslog(syslog.LOG_WARNING, "WARN: Would Be Deleting: snapshot_id=%s virtual_machine=%s snapshot_date=%s snapshot_description=%s" % (snapshot_id, virtual_machine.get_name(), snapshot_date, snapshot_description))
                    snapshot.delete(async=False, correlation_id=snapshot_id)
                elif "active" not in snapshot_description and "keep" in snapshot_description and snapshot_date < fourteen_days_ago:
                    syslog.syslog(syslog.LOG_WARNING, "WARN: Would Be Deleting: snapshot_id=%s virtual_machine=%s snapshot_date=%s snapshot_description=%s" % (snapshot_id, virtual_machine.get_name(), snapshot_date, snapshot_description))
                    snapshot.delete(async=False, correlation_id=snapshot_id)
            except ovirtsdk.infrastructure.errors.RequestError as ex:
                 syslog.syslog(syslog.LOG_CRIT, 'Failed to remove snapshot: %s' % (ex))

def disconnect_from_rhev(rhev_connection):
    rhev_connection.disconnect()


if __name__ == "__main__":
    main()
