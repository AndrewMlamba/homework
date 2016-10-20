def parse_header(data):
    magic_string = data.read(4)
    version = data.read(1)
    num_records = int.from_bytes(data.read(4), byteorder='big')
    return magic_string, version, num_records 

def parse_record(data):
    record_type_byte = data.read(1)
    record_type_mappings = {b'\x00': 'debit', 
        b'\x01': 'credit', b'\x02': 'start_autopay', b'\x03': 'end_autopay'}
    record_type = record_type_mappings.get(record_type_byte, 'unkown')
    timestamp = int.from_bytes(data.read(4), byteorder='big')
    user_id = int.from_bytes(data.read(8), byteorder='big')
    if record_type == 'debit' or record_type == 'credit':
        amount = int.from_bytes(data.read(8), byteorder='big')
    else:
        amount = None
    
    return record_type, timestamp, user_id, amount

def create_data():
    records = []
    with open("txnlog.dat", "rb") as data:
        magic_string, version, num_records = parse_header(data)
        for x in range(num_records):
            record_type, timestamp, user_id, amount = parse_record(data)
            records.append({
                'record_type': record_type,
                'timestamp': timestamp,
                'user_id': user_id,
                'amount': amount
            })
        return records 

def sum_subset(data, record_type):
    return sum([record['amount'] for record in data if record['record_type'] == record_type])

def count_subset(data, record_type):
    return len([record for record in data if record['record_type'] == record_type])

def get_user_balance(data, user_id):
    user_subset = [record for record in data if record['user_id'] == user_id]
    debits = sum_subset(user_subset, 'debit')
    credits = sum_subset(user_subset, 'credit')
    return credits - debits

if __name__ == "__main__":
    data = create_data()
    print("The total amount of debits in dollars is:", + sum_subset(data, 'debit'))
    print("The total amount of credits in dollars is:", + sum_subset(data, 'credit'))
    print("The total number of autopays started is:", count_subset(data, 'start_autopay'))
    print("The total number of autopays ended is:", count_subset(data, 'end_autopay'))
    print("The balance for user 2456938384156277127 is:", get_user_balance(data, 2456938384156277127))
