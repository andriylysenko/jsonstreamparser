from tokenizer.jsontokenizer import JsonTokenizer
from decoder.streamdecoder import JsonDecoder
from six.moves.http_client import HTTPSConnection
import ssl
import datetime

counter = [0]


def handle_event(e, p, counter):
    # print(e)
    if counter[0] % 1000 == 0:
        print('processed ' + str(counter[0]) + ' objects')
    counter[0] += 1


if __name__ == '__main__':
    url = '/prust/wikipedia-movie-data/master/movies.json'
    endpoint = HTTPSConnection('raw.githubusercontent.com', '443', context= ssl._create_unverified_context())
    try:
        ts1 = datetime.datetime.now().timestamp()
        print('start time=' + str(ts1))
        endpoint.request('GET', url)
        response = endpoint.getresponse()

        tokenizer = JsonTokenizer(response, 'ISO-8859-1', 65536)

        JsonDecoder()\
            .tokenizer(tokenizer)\
            .root_class_name('Data')\
            .event_handler(lambda e, p: handle_event(e, p, counter))\
            .predicate('genres') \
            .with_snake_cased_props()\
            .decode()

        ts2 = datetime.datetime.now().timestamp()
        print('end time=' + str(ts2))
        print('delta=' + str(ts2-ts1))
    finally:
        endpoint.close()
    print('total events count=' + str(counter[0]))


