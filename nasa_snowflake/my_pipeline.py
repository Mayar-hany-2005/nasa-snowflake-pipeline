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
EARTHDATA_TOKEN = "eyJ0eXAiOiJKV1QiLCJvcmlnaW4iOiJFYXJ0aGRhdGEgTG9naW4iLCJzaWciOiJlZGxqd3RwdWJrZXlfb3BzIiwiYWxnIjoiUlMyNTYifQ.eyJ0eXBlIjoiVXNlciIsInVpZCI6ImE3bWVkX2Vzc28iLCJleHAiOjE3NjQzNzQzOTksImlhdCI6MTc1OTE1NjcwNSwiaXNzIjoiaHR0cHM6Ly91cnMuZWFydGhkYXRhLm5hc2EuZ292IiwiaWRlbnRpdHlfcHJvdmlkZXIiOiJlZGxfb3BzIiwiYWNyIjoiZWRsIiwiYXNzdXJhbmNlX2xldmVsIjozfQ.37ornZlS0nY1ri4VPKlCpKs763OHwQi0iCFmZ_wp80i_jm_g4OoBMBO8PuzEn6bth9MiUDDO0N3VTClWwJyzr9-ohRCAhnwllaCM0PLJVr7OKQ8nZF7MjjvFXJu4CUh5IPs9ojxGrroY27o-pWRQK7LCv7gstr6xF3szQt3wL0YBrki4EABFxNzm2KetIlkyplYBpGp2HIpfofAZcTFECNIC11qE6L8KwhlTDSi4-OTRGXSOTe3Wd6Ol6QsO6RmyU9iUIbuhb-mBqSVXRxd8s8HFlKqcLHBtT4j1f4qG5P7lpB1wEYTYyAZjI3bppLkYEP6ybYj4Kaoe6moCYqMwAg"

# Snowflake Config
SNOWFLAKE_ACCOUNT = "KBZQPZO-WX06551"
SNOWFLAKE_USER = "A7MEDESSO"
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
    account="KBZQPZO-WX06551",
    user="A7MEDESSO",
    password="Ahmedesso@2005",   # ✨ تحط الباسورد هنا
    authenticator="snowflake",
    warehouse="NASA_WH",
    database="NASA_DB",
    schema="PUBLIC",
    role="ACCOUNTADMIN"
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
