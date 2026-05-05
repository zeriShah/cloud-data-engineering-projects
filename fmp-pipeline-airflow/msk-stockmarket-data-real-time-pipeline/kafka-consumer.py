import json
from kafka import KafkaConsumer
from kafka.sasl.oauth import AbstractTokenProvider      # ← Critical import
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

# ⚠️ MUST inherit AbstractTokenProvider
class MSKTokenProvider(AbstractTokenProvider):
    def token(self):
        token, _ = MSKAuthTokenProvider.generate_auth_token('us-east-1')
        return token

BOOTSTRAP = "YOUR-BOOTSTRAP-STRING:9098"   # ← Replace this

consumer = KafkaConsumer(
    'stock-market-data',
    bootstrap_servers=BOOTSTRAP,
    security_protocol='SASL_SSL',
    sasl_mechanism='OAUTHBEARER',
    sasl_oauth_token_provider=MSKTokenProvider(),
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    key_deserializer=lambda k: k.decode('utf-8') if k else None,
    auto_offset_reset='earliest',
    group_id='stock-consumer-group'
)

print("👂 Listening... Ctrl+C to stop")
for msg in consumer:
    print(f"📨 Partition:{msg.partition} Offset:{msg.offset} | {msg.value}")