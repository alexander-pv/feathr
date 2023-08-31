from feathr_rbac.rbac.db_rbac import DbRBAC
import json

rbac = DbRBAC()


def check(r):
    return r.status_code, json.loads(r.content.decode("utf-8"))
