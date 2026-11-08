import json
import os
from jsonschema import Draft202012Validator, FormatChecker
from kafka  import KafkaConsumer

schema = json.load(open("schema/schema.json"))
validator = Draft202012Validator(schema, format_checker = FormatChecker())
consumer = KafkaConsumer (
	"edge-frames",
	bootstrap_servers =f"{os.getenv('KAFKA_HOST', 'localhost')}:{os.getenv('KAFKA_PORT','9092')}", 
	auto_offset_reset ="earliest",
	consumer_timeout_ms=5000,
	value_deserializer= lambda b: json.loads(b.decode("utf-8")),
)
ok, bad =0,0
for record in consumer:
	errors = list(validator.iter_errors(record.value))
	if errors:
		bad+=1
		if bad<=5:
			print("Sai ở frame", record.value.get("frame_seq"), "->", errors[0].message)
	else:
		ok +=1
print(f"Hợp lệ: {ok}, sai {bad}")
