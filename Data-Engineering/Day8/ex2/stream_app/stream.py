import faust

app = faust.App('sensor-processor', broker='kafka://kafka:9092', topic_partitions=1)


class SensorEvent(faust.Record, serializer='json'):
    sensor_id: int
    value: float
    timestamp: int

sensor_topic = app.topic('sensor-data', value_type=SensorEvent)

windowed_counts = app.Table(
    'counts',
    default=int
).tumbling(60.0, expires=120.0)

@app.agent(sensor_topic)
async def process(events):
    async for event in events:
        windowed_counts[event.sensor_id] += 1
        print(f"Sensor {event.sensor_id} -> Count this minute: {windowed_counts[event.sensor_id]}")

if __name__ == '__main__':
    app.main()