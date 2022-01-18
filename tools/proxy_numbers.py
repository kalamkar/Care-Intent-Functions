from twilio.rest import Client

TWILIO_ACCOUNT_SID = 'ACd3f03d1554da132e550d541480419d42'
TWILIO_AUTH_TOKEN = 'c05ceb45e0fc570aa45643e3ddbb0308'
VOICE = 'https://us-central1-careintent.cloudfunctions.net/receive/voice'
SMS = 'https://us-central1-careintent.cloudfunctions.net/receive/text'
client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)


def buy_numbers(numbers):
    for number in numbers:
        client.incoming_phone_numbers.create(phone_number=number, sms_url=SMS, voice_url=VOICE)


def update_webhooks(number_pattern):
    for number in client.incoming_phone_numbers.list(phone_number=number_pattern):
        if number.sms_url != SMS:
            number.update(sms_url=SMS, voice_url=VOICE)


def in_use_phones():
    phones = set()
    from google.cloud import firestore
    db = firestore.Client()
    for member_doc in db.collection_group('members').stream():
        member = member_doc.to_dict()
        if 'proxy' in member:
            phones.add(member['proxy']['value'])
    return phones


def main():
    numbers = client.available_phone_numbers('US').local.list('316', '613002*')
    for number in numbers:
        print(number.phone_number)


if __name__ == '__main__':
    main()
