## JsonStreamParser
Library to parse json stream in a stream fashion

#### Table of Content
1. [ General Info ](#generalinfo)
2. [ Technologies ](#tech)
3. [ Setup ](#setup)
4. [ Examples of usage ](#example)

<a name="generalinfo"></a>
## 1. General Info
Json parser that parses large json text as a stream and notifies when the object you are interested in is created. For example if you subscribe for deeply nested object the event_handler function that you provide will be called every time once deeply nested object is parsed and transformed into python classes. Custom transformation can also be specified.

<a name="tech"></a>
## 2. Technologies
Python3 + C++ extension

<a name="setup"></a>
## 3. Setup
```pip install streamjsonparser```

<a name="example"></a>
## 4. Examples of usage
```
        from tokenizer.jsontokenizer import JsonTokenizer
        from decoder.streamdecoder import JsonDecoder

        json = """
        [
           {
               "id": "0001",
               "type": "donut",
               "name": "Cake",
               "ppu": 0.55,
               "batters":
                   {
                       "batter":
                           [
                               { "id": "1001", "type": "Regular" },
                               { "id": "1002", "type": "Chocolate" },
                               { "id": "1003", "type": "Blueberry" },
                               { "id": "1004", "type": "Devil's Food" }
                           ]
                   },
               "topping":
                   [
                       { "id": "5001", "type": "None" },
                       { "id": "5002", "type": "Glazed" },
                       { "id": "5005", "type": "Sugar" },
                       { "id": "5007", "type": "Powdered Sugar" },
                       { "id": "5006", "type": "Chocolate with Sprinkles" },
                       { "id": "5003", "type": "Chocolate" },
                       { "id": "5004", "type": "Maple" }
                   ]
           },
           {
               "id": "0002",
               "type": "donut",
               "name": "Raised",
               "ppu": 0.55,
               "batters":
                   {
                       "batter":
                           [
                               { "id": "1001", "type": "Regular" }
                           ]
                   },
               "topping":
                   [
                       { "id": "5001", "type": "None" },
                       { "id": "5002", "type": "Glazed" },
                       { "id": "5005", "type": "Sugar" },
                       { "id": "5003", "type": "Chocolate" },
                       { "id": "5004", "type": "Maple" }
                   ]
           },
           {
               "id": "0003",
               "type": "donut",
               "name": "Old Fashioned",
               "ppu": 0.55,
               "batters":
                   {
                       "batter":
                           [
                               { "id": "1001", "type": "Regular" },
                               { "id": "1002", "type": "Chocolate" }
                           ]
                   },
               "topping":
                   [
                       { "id": "5001", "type": "None" },
                       { "id": "5002", "type": "Glazed" },
                       { "id": "5003", "type": "Chocolate" },
                       { "id": "5004", "type": "Maple" }
                   ]
           }
        ]"""

        # call event_handler function when every root object from root array is created
        # the event_handler is called with e - object and p - path to object from root
        # the root object class name will have name Example
        result = list()
        stream = io.StringIO(json)
        tokenizer = JsonTokenizer(stream)
        JsonDecoder() \
            .tokenizer(tokenizer) \
            .root_class_name('Example') \
            .event_handler(lambda e, p: result.append((e, p))) \
            .decode()

        # call event_handler function when every nested butter object is created
        result = list()
        stream = io.StringIO(json)
        tokenizer = JsonTokenizer(stream)
        JsonDecoder() \
            .tokenizer(tokenizer) \
            .predicate('batter') \
            .event_handler(lambda e, p: result.append((e, p))) \
            .decode()
```

Another example:
```
from tokenizer.jsontokenizer import JsonTokenizer
from decoder.streamdecoder import JsonDecoder
from six.moves.http_client import HTTPSConnection
import ssl

counter = [0]

def handle_event(e, p, counter):
    print(e)
    if counter[0] % 1000 == 0:
        print('processed ' + str(counter[0]) + ' objects')
    counter[0] += 1


if __name__ == '__main__':
    url = '/prust/wikipedia-movie-data/master/movies.json'
    endpoint = HTTPSConnection('raw.githubusercontent.com', '443', context= ssl._create_unverified_context())
    try:
        endpoint.request('GET', url)
        response = endpoint.getresponse()

        tokenizer = JsonTokenizer(response, 'ISO-8859-1', 65536)

        JsonDecoder()\
            .tokenizer(tokenizer) \
            .root_class_name('Data') \
            .event_handler(lambda e, p: handle_event(e, p, counter)) \
            .predicate('genres') \
            .with_snake_cased_props() \
            .decode()
    finally:
        endpoint.close()
    print('total events count=' + str(counter[0]))
```