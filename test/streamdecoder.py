import unittest
import io
from tokenizer.jsontokenizer import JsonTokenizer
from decoder.streamdecoder import JsonDecoder


class TestJsonDecoderMethods(unittest.TestCase):

    def test_decode(self):
        json = '{"name1": "value1", "name2": "value2"}'

        result = list()
        stream = io.StringIO(json)
        tokenizer = JsonTokenizer(stream)
        JsonDecoder() \
            .tokenizer(tokenizer) \
            .root_class_name('Example') \
            .event_handler(lambda e, p: result.append((e, p))) \
            .decode()

        self.assertEqual(len(result), 1)

        (obj, path) = result[0]
        self.assertEqual(path, '')
        self.assertEqual(type(obj).__name__, 'Example')
        self.assertEqual(obj.name1, 'value1')
        self.assertEqual(obj.name2, 'value2')

    def test_decode_subscribe_on_nested_object(self):
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

        result = list()
        stream = io.StringIO(json)
        tokenizer = JsonTokenizer(stream)
        JsonDecoder() \
            .tokenizer(tokenizer) \
            .root_class_name('Example') \
            .predicate('batters') \
            .event_handler(lambda e, p: result.append((e, p))) \
            .decode()

        self.assertEqual(len(result), 3)
        self.assertEqual(len(result[0][0].batter), 4)
        self.assertEqual(len(result[1][0].batter), 1)
        self.assertEqual(len(result[2][0].batter), 2)

        (obj, path) = result[0]
        self.assertEqual(path, 'batters')

        self.assertEqual(type(obj).__name__, 'Batters')
        self.assertEqual(type(obj.batter).__name__, 'list')
        self.assertEqual(len(obj.batter), 4)
        self.assertEqual(type(obj.batter[0]).__name__, 'Batter')

        self.assertEqual(obj.batter[0].id, '1001')
        self.assertEqual(obj.batter[0].type, 'Regular')

        self.assertEqual(obj.batter[1].id, '1002')
        self.assertEqual(obj.batter[1].type, 'Chocolate')

        self.assertEqual(obj.batter[2].id, '1003')
        self.assertEqual(obj.batter[2].type, 'Blueberry')

        self.assertEqual(obj.batter[3].id, '1004')
        self.assertEqual(obj.batter[3].type, "Devil's Food")

    def test_decode_subscribe_on_nested_array(self):
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

        result = list()
        stream = io.StringIO(json)
        tokenizer = JsonTokenizer(stream)
        JsonDecoder() \
            .tokenizer(tokenizer) \
            .root_class_name('Example') \
            .predicate('batters.batter') \
            .event_handler(lambda e, p: result.append((e, p))) \
            .decode()

        self.assertEqual(len(result), 7)

        self.assertEqual(result[1][1], 'batters.batter')
        self.assertEqual(result[6][1], 'batters.batter')

        self.assertEqual(type(result).__name__, 'list')
        (obj1, path1) = result[0]
        self.assertEqual(path1, 'batters.batter')
        self.assertEqual(type(obj1).__name__, 'Batter')
        self.assertEqual(obj1.id, '1001')
        self.assertEqual(obj1.type, 'Regular')

        (obj6, path6) = result[6]
        self.assertEqual(path6, 'batters.batter')
        self.assertEqual(type(obj6).__name__, 'Batter')
        self.assertEqual(obj6.id, '1002')
        self.assertEqual(obj6.type, 'Chocolate')

    def test_decode_subscribe_on_simple_value(self):
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
               "ppu": 0.56,
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
               "ppu": 0.57,
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

        result = list()
        stream = io.StringIO(json)
        tokenizer = JsonTokenizer(stream)
        JsonDecoder() \
            .tokenizer(tokenizer) \
            .root_class_name('Example') \
            .predicate('ppu') \
            .event_handler(lambda e, p: result.append((e, p))) \
            .decode()

        self.assertEqual(len(result), 3)
        self.assertEqual(result[0][0], '0.55')
        self.assertEqual(result[1][0], '0.56')
        self.assertEqual(result[2][0], '0.57')

    def test_decode_with_translators(self):
        class MyBatter:
            __slots__ = ('my_id', 'my_type')

            def __init__(self, id, type):
                self.my_id = id
                self.my_type = type

        class MyBatters:
            __slots__ = ('my_batter')

            def __init__(self, batter):
                self.my_batter = batter

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
               "ppu": 0.56,
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
               "ppu": 0.57,
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

        result = list()
        stream = io.StringIO(json)
        tokenizer = JsonTokenizer(stream)
        JsonDecoder() \
            .tokenizer(tokenizer) \
            .root_class_name('Example') \
            .translator('batters', lambda o: MyBatters(o.batter)) \
            .translator('batters.batter', lambda o: MyBatter(o.id, o.type)) \
            .event_handler(lambda e, p: result.append(e)) \
            .decode()

        self.assertEqual(len(result), 3)
        self.assertEqual(type(result[0].batters).__name__, 'MyBatters')
        self.assertEqual(type(result[0].batters.my_batter[0]).__name__, 'MyBatter')
        self.assertEqual(result[0].batters.my_batter[0].my_id, '1001')
        self.assertEqual(result[0].batters.my_batter[0].my_type, 'Regular')

    def test_decode_array_of_arrays(self):
        json = """
               {
                   "id": "123",
                   "type": "car",
                   "data":
                   [
                       ["red", "blue", "green"],
                       [1,2,3],
                       [true, false, true]   
                   ]
               }"""

        result = list()
        stream = io.StringIO(json)
        tokenizer = JsonTokenizer(stream)
        JsonDecoder() \
            .tokenizer(tokenizer) \
            .root_class_name('Example') \
            .event_handler(lambda e, p: result.append(e)) \
            .decode()

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].id, '123')
        self.assertEqual(result[0].type, 'car')
        self.assertEqual(len(result[0].data), 3)
        self.assertEqual(len(result[0].data[0]), 3)
        self.assertEqual(result[0].data[0][0], 'red')
        self.assertEqual(result[0].data[0][1], 'blue')
        self.assertEqual(result[0].data[0][2], 'green')
        self.assertEqual(len(result[0].data[1]), 3)
        self.assertEqual(result[0].data[1][0], '1')
        self.assertEqual(result[0].data[1][1], '2')
        self.assertEqual(result[0].data[1][2], '3')
        self.assertEqual(len(result[0].data[2]), 3)
        self.assertEqual(result[0].data[2][0], True)
        self.assertEqual(result[0].data[2][1], False)
        self.assertEqual(result[0].data[2][2], True)


suite = unittest.TestLoader().loadTestsFromTestCase(TestJsonDecoderMethods)
unittest.TextTestRunner(verbosity=2).run(suite)
