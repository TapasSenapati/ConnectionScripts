import requests
import json
import time
from requests_kerberos import HTTPKerberosAuth

url = "https://dbt-spark-gateway.ciadev.cna2-sx9y.cloudera.site:8998"
auth = HTTPKerberosAuth()

data = {"kind": "spark"}

headers = {"Content-Type": "application/json"}

# Create sessions
id = requests.post(
    url + "/sessions", data=json.dumps(data), headers=headers, auth=auth
).json()["id"]

print(id)

# Wait for started state
while True:
    r = requests.get(
        url + "/sessions/" + str(id) + "/state", headers=headers, auth=auth
    ).json()
    print(r)
    if r["state"] == "idle":
        break
    time.sleep(2)


def submitCode(code):
    # Submit code
    data = {"code": code}

    print(data)

    r = requests.post(
        url + "/sessions/" + str(id) + "/statements",
        data=json.dumps(data),
        headers=headers,
        auth=auth,
    )

    print(r)
    print(r.json())
    return r


def getResult(r):
    jr = r.json()

    while True:
        u = requests.get(
            url + "/sessions/" + str(id) + "/statements/" + repr(jr["id"]),
            headers=headers,
            auth=auth,
        ).json()
        print(u)
        if u["state"] == "available":
            break
        time.sleep(2)


def getLivySQL(sql):
    code = 'val d = spark.sql("' + sql + '")\nval e=d.collect\n%json e'

    return code


getResult(submitCode("sc.parallelize(1 to 5).count()"))

getResult(submitCode(getLivySQL("select 1")))