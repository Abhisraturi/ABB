import snap7
from snap7.util import get_int

def read_tag(ip, rack, slot, db_number, byte_offset):
    client = snap7.client.Client()
    try:
        client.connect(ip, rack, slot)
        if not client.get_connected():
            print("Connection failed")
            return

        data = client.db_read(db_number, byte_offset, 2)
        value = get_int(data, 0)
        print(f"DB{db_number}.DBW{byte_offset} = {value}")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        client.disconnect()

if __name__ == "__main__":
    read_tag(
        ip="172.16.12.40",
        rack=5,
        slot=0,
        db_number=1261,
        byte_offset=104
    )
