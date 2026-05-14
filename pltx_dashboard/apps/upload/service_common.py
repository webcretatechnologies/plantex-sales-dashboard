from django.db import connection


DB_BATCH_SIZE = 10_000


def get_upsert_kwargs(unique_fields, update_fields):
    kwargs = {"update_conflicts": True, "update_fields": update_fields}
    if connection.vendor != "mysql":
        kwargs["unique_fields"] = unique_fields
    return kwargs
