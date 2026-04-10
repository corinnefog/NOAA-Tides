import pandas as pd
import logging
import boto3
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import requests
import seaborn as sns
from datetime import datetime, timedelta, timezone
from boto3.dynamodb.conditions import Key
import io
import os
from decimal import Decimal

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

TABLE_NAME   = os.environ["DYNAMODB_TABLE"]
S3_BUCKET    = os.environ["S3_BUCKET"]
AWS_REGION   = os.environ.get("AWS_REGION", "us-east-1")
STATION_ID = "8638610"

def round_to_6min(dt):
    # Total seconds in a 6-minute interval
    seconds = 6 * 60
    # Get total seconds from epoch and round to the nearest interval
    rounded_timestamp = round(dt.timestamp() / seconds) * seconds
    return datetime.fromtimestamp(rounded_timestamp, tz=timezone.utc)

def fetch_noaa(product, target_dt):
    date_str = target_dt.strftime("%Y%m%d")
    url = (
        f"https://api.tidesandcurrents.noaa.gov/api/prod/datagetter?"
        f"begin_date={date_str}&end_date={date_str}"
        f"&station={STATION_ID}&product={product}"
        f"&datum=MLLW&time_zone=gmt&units=english&format=json"
    )
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    return resp.json()

def get_reading(target_dt, resp):
    records = resp.get("data") or resp.get("predictions")
    target_str = target_dt.strftime("%Y-%m-%d %H:%M")
    for record in records:
        if record["t"] == target_str:
            return float(record["v"])
    raise ValueError(f"No reading found for {target_str}")

def classify_surge(diff):
    if diff >= -0.1 and diff <= 0.1:
        label = "stable"
    elif diff > 0.1:
        label = "falling"
    elif diff >= -0.5 and diff < -0.1:
        label = "rising"
    else:
        label = "surge"
    return label

def main():
    # Step 1: get current UTC time rounded to 6 min
    now = datetime.now(timezone.utc)
    target_dt = round_to_6min(now - timedelta(hours=6))

    # Step 2: fetch NOAA data
    water_resp = fetch_noaa("water_level", target_dt)
    pred_resp = fetch_noaa("predictions", target_dt)

    # Step 3: extract readings
    observed = get_reading(target_dt, water_resp)
    predicted = get_reading(target_dt, pred_resp)

    # Step 4: compute difference
    diff = predicted - observed

    # Step 5: classify surge
    surge = classify_surge(diff)

    # Step 6: log results
    log.info(f"Time: {target_dt}")
    log.info(f"Observed: {observed}")
    log.info(f"Predicted: {predicted}")
    log.info(f"Diff: {diff}")
    log.info(f"Surge classification: {surge}")

    dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
    table = dynamodb.Table(TABLE_NAME)

    table.put_item(Item={"station_id": STATION_ID, "timestamp": target_dt.isoformat(), "observed":Decimal(str(observed)), "predicted":Decimal(str(predicted)), "difference":Decimal(str(diff)),  "classification_level":surge})

    history = table.query(KeyConditionExpression=Key("station_id").eq(STATION_ID),
        ScanIndexForward=True,   #ascending order
	)

    df = pd.DataFrame(history['Items']) 
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["observed"] = df["observed"].astype(float)
    df["predicted"] = df["predicted"].astype(float)
    df["difference"] = df["difference"].astype(float)

    sns.lineplot(data=df, x="timestamp", y="observed", label="Observed")
    sns.lineplot(data=df, x="timestamp", y="predicted", label="Predicted")

    plt.title("Observed v. Predicted Tide Levels in VB/Norfolk")
    plt.xlabel("Timestamp")
    plt.ylabel("Tide Level")
    plt.legend()
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    buf = io.BytesIO()
    plt.savefig(buf, format="png", dpi=150, bbox_inches="tight")
    buf.seek(0)
    plt.close()
    log.info("Plot generated (%d bytes, %d points)", len(buf.getvalue()), len(df))

    s3 = boto3.client("s3", region_name=AWS_REGION)
    s3.put_object(
        Bucket=S3_BUCKET,
        Key="plot.png",
        Body=buf.getvalue(),
        ContentType="image/png",
    )
    log.info("Uploaded plot.png to s3://%s", S3_BUCKET)

    csv_buff = io.BytesIO()
    df.to_csv(csv_buff, index=False)
    csv_buff.seek(0)
    s3.put_object(Bucket=S3_BUCKET, Key="data.csv", Body=csv_buff.getvalue(), ContentType="text/csv") 
    log.info("Uploaded data.csv to s3://%s", S3_BUCKET)

if __name__ == "__main__":
    main()
