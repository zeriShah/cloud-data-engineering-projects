import json, random, time
from datetime import datetime
from kafka import KafkaProducer
from kafka.sasl.oauth import AbstractTokenProvider      # ← Critical import
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

# ⚠️ MUST inherit AbstractTokenProvider
class MSKTokenProvider(AbstractTokenProvider):
    def token(self):
        token, _ = MSKAuthTokenProvider.generate_auth_token('us-east-1')
        return token

BOOTSTRAP = "YOUR-BOOTSTRAP-STRING:9098"   # ← Replace this

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP,
    security_protocol='SASL_SSL',
    sasl_mechanism='OAUTHBEARER',
    sasl_oauth_token_provider=MSKTokenProvider(),
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8')
)

STOCKS = ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA', 'META', 'NVDA']
count = 0
print("🚀 Starting Producer... Ctrl+C to stop")
while True:
    stock = random.choice(STOCKS)
    msg = {
        "symbol": stock,
        "price": round(random.uniform(100, 1500), 2),
        "volume": random.randint(1000, 50000),
        "change": round(random.uniform(-5.0, 5.0), 2),
        "timestamp": datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
        "exchange": "NASDAQ"
    }
    producer.send('stock-market-data', key=stock, value=msg)
    count += 1
    print(f"✅ Sent [{count}]: {msg}")
    time.sleep(1)