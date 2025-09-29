import io
import requests
import xarray as xr
import pandas as pd
import snowflake.connector
from dagster import job, op

# ==========================
# NASA + Snowflake Config
# ==========================
NASA_URL = "https://data.gesdisc.earthdata.nasa.gov/data/GLDAS/GLDAS_NOAH025_M.2.1/2000/GLDAS_NOAH025_M.A200001.021.nc4"

# 🔑 Token من Earthdata (خليه في متغير)
EARTHDATA_TOKEN = "eyJ0eXAiOiJKV1QiLCJvcmlnaW4iOiJFYXJ0aGRhdGEgTG9naW4iLCJzaWciOiJlZGxqd3RwdWJrZXlfb3BzIiwiYWxnIjoiUlMyNTYifQ.eyJ0eXBlIjoiVXNlciIsInVpZCI6Im1heWFyMjAwNSIsImV4cCI6MTc2MzU5Njc5OSwiaWF0IjoxNzU4NDA4MjYwLCJpc3MiOiJodHRwczovL3Vycy5lYXJ0aGRhdGEubmFzYS5nb3YiLCJpZGVudGl0eV9wcm92aWRlciI6ImVkbF9vcHMiLCJhY3IiOiJlZGwiLCJhc3N1cmFuY2VfbGV2ZWwiOjN9.IzcEHuwMgdaH_VsYDkNDSRn5vKZ0WGXadtc8cOKlhskX9rcglRgNpmFQkHJeXpix_W5FHy86knW3Cpd7EFcVHXfUTHyjpE8nMjObiGUPEpX_UTHkuRZWMuLXsLpVUlBPXNc9wWWMa7DqTbaRovAnyv7GdTQw3juUpwnxv2-qZPxfU0hNlgeaj4TqroCFAo6eIzMqp1xCFii22ie9gu-bEe6NmprFA0PkKOwyMe-_pe19uCPZ07Bmp6u9BRrc5qO0G-lgICg16Cx7RO2Vx60pjpwa_FLhmO-04qR93C15flALxE40LyfjzW68tJVjIzQMk-2iq-bmnNlzrfcLpm6vzg"

# Snowflake Config
SNOWFLAKE_ACCOUNT = "KBZQPZO-WX06551"
SNOWFLAKE_USER = "MAYARHANY1999"
SNOWFLAKE_AUTHENTICATOR = "externalbrowser"
SNOWFLAKE_ROLE = "ACCOUNTADMIN"
SNOWFLAKE_WAREHOUSE = "NASA_WH"
SNOWFLAKE_DATABASE = "NASA_DB"
SNOWFLAKE_SCHEMA = "PUBLIC"


# ==========================
# DAGSTER OPS
# ==========================
@op
def extract_temperature():
    """يسحب ملف NetCDF من NASA Earthdata باستخدام Bearer Token"""
    headers = {"Authorization": f"Bearer {EARTHDATA_TOKEN}"}
    response = requests.get(NASA_URL, headers=headers)
    response.raise_for_status()

    data = io.BytesIO(response.content)
    ds = xr.open_dataset(data, engine="h5netcdf")

    # ناخد متغير الحرارة لأول يوم
    temp = ds["Tair_f_inst"].isel(time=0)
    df = temp.to_dataframe().reset_index()
    return df


@op
def transform_temperature(df: pd.DataFrame):
    """تحويل بيانات الحرارة"""
    avg_temp = df["Tair_f_inst"].mean()
    transformed = pd.DataFrame({"avg_temperature": [avg_temp]})
    return transformed


@op
def load_temperature_to_snowflake(df: pd.DataFrame):
    """تحميل البيانات لـ Snowflake"""
    conn = snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        authenticator=SNOWFLAKE_AUTHENTICATOR,
        role=SNOWFLAKE_ROLE,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
    )
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS TEMPERATURE (
            avg_temperature FLOAT
        )
    """)

    for _, row in df.iterrows():
        cur.execute(
            "INSERT INTO TEMPERATURE (avg_temperature) VALUES (%s)",
            (row["avg_temperature"],),
        )

    conn.commit()
    cur.close()
    conn.close()


# ==========================
# DAGSTER JOB
# ==========================
@job
def nasa_temperature_pipeline():
    data = extract_temperature()
    transformed = transform_temperature(data)
    load_temperature_to_snowflake(transformed)
