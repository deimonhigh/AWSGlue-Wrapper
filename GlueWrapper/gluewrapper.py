import json
import boto3
import datetime

BRAZIL_TZ = datetime.timedelta(hours=2)
DATE = datetime.datetime.utcnow()
DATE = DATE - BRAZIL_TZ

EVENT = {
    "NameSync": None,
    "DateBegin": str(DATE)[0:-3],
    "DateEnd": None,
    "Status": 1,
    "MsgErr": None
}

SEMAPHORE_NAME = ""


def __trigger_log(fname, body, status=1):
    try:
        timestamp = datetime.datetime.now().strftime('%Y%m%d%H%M%S')

        boto3.resource('s3') \
            .Object('abb-integracao', "logs/{0}/{0}-{1}-{2}".format(fname, timestamp, status)) \
            .put(Body=body)

    except Exception:
        pass


def __end(file_name):
    try:
        boto3.resource('s3') \
            .Object('abb-integracao', "jobsemaphore/{0}.txt".format(file_name)) \
            .delete()
    except Exception:
        pass


def run_etl(procedure, file_name):
    def handler(fn):
        def wrapper(*args, **kwargs):
            EVENT['NameSync'] = "job_" + procedure

            try:
                fn(*args, **kwargs)

            except Exception as e:
                __trigger_log(procedure, json.dumps(EVENT))

                date_end = datetime.datetime.utcnow() - BRAZIL_TZ
                EVENT["Status"] = 3
                EVENT["DateEnd"] = str(date_end)[0:-3]
                EVENT["MsgErr"] = str(e)

                __trigger_log(procedure, json.dumps(EVENT), EVENT['Status'])

                raise Exception(e)

            finally:
                try:
                    __end(file_name)
                except:
                    pass

        return wrapper

    return handler
