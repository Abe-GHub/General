# Salesforce API to Databricks (Delta / Unity Catalog)

A Databricks notebook (`salesforce_to_databricks.py`, notebook source format) that pulls Salesforce objects through the Salesforce API and lands them as Delta tables in Unity Catalog.

## What it does

- Authenticates with `simple-salesforce` using the OAuth 2.0 Client Credentials flow (username/password/token as a fallback), with credentials stored in a Databricks secret scope
- Uses `describe()` to pull every standard and custom field per object, without hardcoding
- Extracts with Bulk API 2.0 into a Unity Catalog Volume, then reads the CSVs with Spark
- Loads incrementally using a `SystemModstamp` watermark and a Delta `MERGE` on `Id`
- Optional cells for hard-delete handling (`IsDeleted` via queryAll) and API limit checks

## Libraries

| Library | Purpose |
|---|---|
| `simple-salesforce` | Auth, SOQL via REST, `describe()`, Bulk API 2.0 |
| `pandas` | Small REST result sets (in the Databricks runtime) |
| `delta.tables` | `MERGE` into Delta (in the Databricks runtime) |
| `dbutils.secrets` | Credential management |

## Setup

1. Create a Salesforce Connected App (or External Client App) with OAuth and the Client Credentials flow enabled, and assign a "Run As" integration user.
2. Create the secret scope:
   ```
   databricks secrets create-scope salesforce
   databricks secrets put-secret salesforce sf_consumer_key
   databricks secrets put-secret salesforce sf_consumer_secret
   databricks secrets put-secret salesforce sf_domain   # e.g. mycompany.my
   ```
3. Import `salesforce_to_databricks.py` into your workspace, set `CATALOG`, `SCHEMA`, and `OBJECTS` in the config cell, and run.

## Notes

- Columns land as strings from the Bulk CSV output; cast types in the silver layer (or dbt).
- Bulk API 2.0 does not return compound fields (address, location) or base64 fields; the notebook skips them.
- For managed, no-code ingestion, Databricks Lakeflow Connect has a native Salesforce connector. This notebook is useful when you need custom SOQL, other Salesforce APIs, or full control over the load logic.
