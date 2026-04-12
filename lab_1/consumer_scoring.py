from kafka import KafkaConsumer, KafkaProducer
import json, time


consumer = KafkaConsumer('transactions', bootstrap_servers='broker:9092',
    auto_offset_reset='earliest', group_id='scoring-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')))

alert_producer = KafkaProducer(bootstrap_servers='broker:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'))

def score_transaction(tx):
    score, rules = 0, []
    if tx['amount'] > 3000:
        rules.append('R1')
        score = score + 3
        
    if tx['category'] == 'elektronika' and tx['amount'] > 1500:
        rules.append('R2')
        score = score + 2
        
    if tx['hour'] < 6:
        rules.append('R3')
        score = score + 1
         
    return score, rules



for message in consumer:
    tx = message.value
    tx_score, tx_rules = score_transaction(tx)
    if tx_score >=3:
        tx['alert_score'], tx['alert_rules'] = tx_score, tx_rules
        alert_producer.send('alerts', tx)
        print(f"Podejrzana transakcja {tx['tx_id']} = {tx_score} | {tx_rules}")
        time.sleep(0.5)

alert_producer.flush()
alert_producer.close()
