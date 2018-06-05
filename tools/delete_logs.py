#!/usr/bin/env python3
"""Deletes all log groups for lambda handlers created by Kappa."""
import logging

import boto3

LAMBDA_PREFIX = "/aws/lambda/__ir"


def delete_logs():
    logging.basicConfig(level=logging.INFO)
    client = boto3.client("logs")

    params = dict(logGroupNamePrefix=LAMBDA_PREFIX)
    while True:
        response = client.describe_log_groups(**params)
        for g in response["logGroups"]:
            name = g["logGroupName"]
            try:
                client.delete_log_group(logGroupName=name)
            except Exception as e:
                logging.info("Log group deletion failed: %s", e)
            else:
                logging.info("Log group deleted: %s", name)

        next_token = response.get("nextToken")
        if not next_token:
            break
        params["nextToken"] = next_token


if __name__ == '__main__':
    delete_logs()

