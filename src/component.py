import csv
import logging
import datetime
import sys
import psutil

from keboola.component.base import ComponentBase
from keboola.component.exceptions import UserException

import time

import PureCloudPlatformClientV2 as pc2
from PureCloudPlatformClientV2.rest import ApiException

KEY_CLIENT_ID = 'client_id'
KEY_PASSWORD = '#password'
KEY_CLOUD_URL = 'cloud_url'
KEY_DAYS = 'last_days_interval'
KEY_START = 'start_date'

REQUIRED_PARAMETERS = [KEY_CLIENT_ID, KEY_PASSWORD, KEY_CLOUD_URL]
REQUIRED_IMAGE_PARS = []


class Component(ComponentBase):
    """
        Extends base class for general Python components. Initializes the CommonInterface
        and performs configuration validation.

        For easier debugging the data folder is picked up by default from `../data` path,
        relative to working directory.

        If `debug` parameter is present in the `config.json`, the default logger is set to verbose DEBUG mode.
    """

    def __init__(self):
        super().__init__()

    def run(self):
        logging.info("Memory usage BRK1> %s" % str(psutil.Process().memory_info().rss))

        self.validate_configuration_parameters(REQUIRED_PARAMETERS)
        self.validate_image_parameters(REQUIRED_IMAGE_PARS)
        params = self.configuration.parameters

        if params.get(KEY_DAYS):
            DAYS_COUNT = int(params.get(KEY_DAYS))
        else:
            DAYS_COUNT = 3

        if params.get(KEY_START):
            from_datetime = datetime.datetime.strptime(params.get(KEY_START), '%Y-%m-%d')
        else:
            # get data from previous calendar day only
            from_datetime = datetime.datetime.combine(
                datetime.datetime.utcnow(),
                datetime.time(00, 00, 00)
            ) - datetime.timedelta(days=DAYS_COUNT)

        end_date = from_datetime + datetime.timedelta(days=DAYS_COUNT)
        filter = {
            "interval": "%s/%s" % (
                from_datetime.isoformat(timespec="seconds"),
                end_date.isoformat(timespec="seconds")
            )
        }

        # obtain data
        logging.info("Memory usage BRK2> %s" % str(psutil.Process().memory_info().rss))
        api_client = pc2.api_client.ApiClient(
            host=params.get(KEY_CLOUD_URL)
        ).get_client_credentials_token(params.get(KEY_CLIENT_ID), params.get(KEY_PASSWORD))

        usersApi = pc2.UsersApi(api_client=api_client)

        # @note: login_details[user_id][date] = first mention in day
        login_details = dict()

        # @note: mapping between emails and names
        names = dict()

        try:
            api_response = usersApi.post_analytics_users_details_jobs(filter)
            async_job_id = api_response.job_id

            counter = 30
            while counter > 0:
                job_status = usersApi.get_analytics_users_details_job(async_job_id)
                if job_status.state == "FULFILLED":
                    break
                time.sleep(1)

            counter -= 1

            if counter == 0:
                # job is not finished in X seconds
                sys.exit(1)

            # job is ready
            cursor = None

            while True:
                logging.info("Memory usage BRK3> %s" % str(psutil.Process().memory_info().rss))
                if cursor is None:
                    response = usersApi.get_analytics_users_details_job_results(async_job_id)
                else:
                    response = usersApi.get_analytics_users_details_job_results(async_job_id, cursor=cursor)

                cursor = response.cursor

                for user_detail in response.user_details:
                    detail = usersApi.get_user(user_detail.user_id)
                    user_name = detail.username

                    names[user_name] = detail.name

                    if user_name not in login_details:
                        login_details[user_name] = dict()

                    if user_detail.primary_presence is not None:
                        presence_from = user_detail.primary_presence
                    else:
                        presence_from = user_detail.routing_status

                    for presence in presence_from:
                        if hasattr(presence, "presence.system_presence") and presence.system_presence in ["OFFLINE"]:
                            continue

                        if hasattr(presence, "presence.routing_status") and presence.routing_status in ["OFF_QUEUE"]:
                            continue

                        # datetime is in UTC, so we want to move it to local timezone
                        start_time_utc = presence.start_time
                        start_time_local = start_time_utc.replace(tzinfo=datetime.timezone.utc).astimezone(tz=None)

                        date_string = str(start_time_local.date())

                        if date_string < str(from_datetime.date()):
                            # @workaroud: Why genesis returns date outside the date-scope?
                            # skip these records
                            continue

                        if date_string not in login_details[user_name]:
                            login_details[user_name][date_string] = start_time_local

                        login_details[user_name][date_string] = min(
                            login_details[user_name][date_string],
                            start_time_local
                        )

                if cursor is None:
                    break
        except ApiException as e:
            print("Exception when calling ConversationsApi->post_analytics_users_detail_query: %s\n" % e)
            sys.exit(1)

        # datetime object to proper datetime string
        for detail in login_details:
            for record in login_details[detail]:
                login_details[detail][record] = str(login_details[detail][record])

        # export to CSV (email; name; date; datetime) with PK (email; date)
        with open('data_file.csv', 'w') as data_file:
            csv_writer = csv.writer(data_file)

            csv_writer.writerow(["email", "name", "date", "first_login"])
            for user in login_details:
                for date in login_details[user]:
                    csv_writer.writerow([user, names[user], date, login_details[user][date]])

        logging.info("Save table")
        logging.info("Memory usage BRK-SAVE> %s" % str(psutil.Process().memory_info().rss))
        conversation_table = self.create_out_table_definition(
             'worktime.csv', incremental=True, primary_key=['email', 'date'])
        with open(conversation_table.full_path, mode='wt', encoding='utf-8', newline='') as out_file:
            writer = csv.DictWriter(
                out_file,
                fieldnames=['email', 'name', 'date', 'first_login']
            )
            writer.writeheader()

            for user in login_details:
                for date in login_details[user]:
                    writer.writerow(
                        {"email": user, "name": names[user], "date": date, "first_login": login_details[user][date]}
                    )

        self.write_manifest(conversation_table)


if __name__ == "__main__":
    try:
        comp = Component()
        # this triggers the run method by default and is controlled by the configuration.action parameter
        comp.execute_action()
    except UserException as exc:
        logging.exception(exc)
        sys.exit(1)
    except Exception as exc:
        logging.exception(exc)
        sys.exit(2)
