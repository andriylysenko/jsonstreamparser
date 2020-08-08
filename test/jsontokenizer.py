import unittest
import io
from tokenizer.jsontokenizer import JsonTokenizer


class TestJsonTokenizerMethods(unittest.TestCase):

    def test_next(self):
        json = '{"name1": "value1", "name2": "value2"}'

        stream = io.StringIO(json)
        tokenizer = JsonTokenizer(stream)

        self.assertEqual(tokenizer.next(), ('{', 0))
        self.assertEqual(tokenizer.next(), ('name1', 1))
        self.assertEqual(tokenizer.next(), ('value1', 1))
        self.assertEqual(tokenizer.next(), ('name2', 1))
        self.assertEqual(tokenizer.next(), ('value2', 1))
        self.assertEqual(tokenizer.next(), ('}', 0))
        self.assertEqual(tokenizer.has_next(), False)

    def test_next_with_different_value_types(self):
        json = '{"name1": "value1", "name2": null, "name3": true, "name4": false, "name5": 10.67}'

        stream = io.StringIO(json)
        tokenizer = JsonTokenizer(stream)

        self.assertEqual(tokenizer.next(), ('{', 0))
        self.assertEqual(tokenizer.next(), ('name1', 1))
        self.assertEqual(tokenizer.next(), ('value1', 1))
        self.assertEqual(tokenizer.next(), ('name2', 1))
        self.assertEqual(tokenizer.next(), ('null', 2))
        self.assertEqual(tokenizer.next(), ('name3', 1))
        self.assertEqual(tokenizer.next(), ('true', 2))
        self.assertEqual(tokenizer.next(), ('name4', 1))
        self.assertEqual(tokenizer.next(), ('false', 2))
        self.assertEqual(tokenizer.next(), ('name5', 1))
        self.assertEqual(tokenizer.next(), ('10.67', 2))
        self.assertEqual(tokenizer.next(), ('}', 0))
        self.assertEqual(tokenizer.has_next(), False)

    def test_next_simple_array(self):
        json = '["value1", "value2", "value3"]'

        stream = io.StringIO(json)
        tokenizer = JsonTokenizer(stream)

        self.assertEqual(tokenizer.next(), ('[', 0))
        self.assertEqual(tokenizer.next(), ('value1', 1))
        self.assertEqual(tokenizer.next(), ('value2', 1))
        self.assertEqual(tokenizer.next(), ('value3', 1))
        self.assertEqual(tokenizer.next(), (']', 0))
        self.assertEqual(tokenizer.has_next(), False)

    def test_next_nested(self):
        json = '{"name1": "value1", "name2": {"name22": "value22", "name23": "value23"}}'

        stream = io.StringIO(json)
        tokenizer = JsonTokenizer(stream)

        self.assertEqual(tokenizer.next(), ('{', 0))
        self.assertEqual(tokenizer.next(), ('name1', 1))
        self.assertEqual(tokenizer.next(), ('value1', 1))
        self.assertEqual(tokenizer.next(), ('name2', 1))
        self.assertEqual(tokenizer.next(), ('{', 0))
        self.assertEqual(tokenizer.next(), ('name22', 1))
        self.assertEqual(tokenizer.next(), ('value22', 1))
        self.assertEqual(tokenizer.next(), ('name23', 1))
        self.assertEqual(tokenizer.next(), ('value23', 1))
        self.assertEqual(tokenizer.next(), ('}', 0))
        self.assertEqual(tokenizer.next(), ('}', 0))
        self.assertEqual(tokenizer.has_next(), False)

    def test_next_nested_array(self):
        json = '{"name1": "value1", "name2": [10, 20, true, "some value"]}'

        stream = io.StringIO(json)
        tokenizer = JsonTokenizer(stream)

        self.assertEqual(tokenizer.next(), ('{', 0))
        self.assertEqual(tokenizer.next(), ('name1', 1))
        self.assertEqual(tokenizer.next(), ('value1', 1))
        self.assertEqual(tokenizer.next(), ('name2', 1))
        self.assertEqual(tokenizer.next(), ('[', 0))
        self.assertEqual(tokenizer.next(), ('10', 2))
        self.assertEqual(tokenizer.next(), ('20', 2))
        self.assertEqual(tokenizer.next(), ('true', 2))
        self.assertEqual(tokenizer.next(), ('some value', 1))
        self.assertEqual(tokenizer.next(), (']', 0))
        self.assertEqual(tokenizer.next(), ('}', 0))
        self.assertEqual(tokenizer.has_next(), False)

    def test_next_nested_mixedarray(self):
        json = """
           {
               "name1": "value1",
              "name2": [
                   10,
                   20,
                   {
                       "name3": "value3",
                       "name4": "value4"
                   },
                   30]
           }"""

        stream = io.StringIO(json)
        tokenizer = JsonTokenizer(stream)

        self.assertEqual(tokenizer.next(), ('{', 0))
        self.assertEqual(tokenizer.next(), ('name1', 1))
        self.assertEqual(tokenizer.next(), ('value1', 1))
        self.assertEqual(tokenizer.next(), ('name2', 1))
        self.assertEqual(tokenizer.next(), ('[', 0))
        self.assertEqual(tokenizer.next(), ('10', 2))
        self.assertEqual(tokenizer.next(), ('20', 2))
        self.assertEqual(tokenizer.next(), ('{', 0))
        self.assertEqual(tokenizer.next(), ('name3', 1))
        self.assertEqual(tokenizer.next(), ('value3', 1))
        self.assertEqual(tokenizer.next(), ('name4', 1))
        self.assertEqual(tokenizer.next(), ('value4', 1))
        self.assertEqual(tokenizer.next(), ('}', 0))
        self.assertEqual(tokenizer.next(), ('30', 2))
        self.assertEqual(tokenizer.next(), (']', 0))
        self.assertEqual(tokenizer.next(), ('}', 0))
        self.assertEqual(tokenizer.has_next(), False)

    def test_next_deeply_nested(self):
        json = """
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
                       { "id": "1004", "type": "Devils Food" }
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
       }"""

        stream = io.StringIO(json)
        tokenizer = JsonTokenizer(stream)

        self.assertEqual(tokenizer.next(), ('{', 0))
        self.assertEqual(tokenizer.next(), ('id', 1))
        self.assertEqual(tokenizer.next(), ('0001', 1))
        self.assertEqual(tokenizer.next(), ('type', 1))
        self.assertEqual(tokenizer.next(), ('donut', 1))
        self.assertEqual(tokenizer.next(), ('name', 1))
        self.assertEqual(tokenizer.next(), ('Cake', 1))
        self.assertEqual(tokenizer.next(), ('ppu', 1))
        self.assertEqual(tokenizer.next(), ('0.55', 2))
        self.assertEqual(tokenizer.next(), ('batters', 1))
        self.assertEqual(tokenizer.next(), ('{', 0))
        self.assertEqual(tokenizer.next(), ('batter', 1))
        self.assertEqual(tokenizer.next(), ('[', 0))
        self.assertEqual(tokenizer.next(), ('{', 0))
        self.assertEqual(tokenizer.next(), ('id', 1))
        self.assertEqual(tokenizer.next(), ('1001', 1))
        self.assertEqual(tokenizer.next(), ('type', 1))
        self.assertEqual(tokenizer.next(), ('Regular', 1))
        self.assertEqual(tokenizer.next(), ('}', 0))
        self.assertEqual(tokenizer.next(), ('{', 0))
        self.assertEqual(tokenizer.next(), ('id', 1))
        self.assertEqual(tokenizer.next(), ('1002', 1))
        self.assertEqual(tokenizer.next(), ('type', 1))
        self.assertEqual(tokenizer.next(), ('Chocolate', 1))
        self.assertEqual(tokenizer.next(), ('}', 0))
        self.assertEqual(tokenizer.next(), ('{', 0))
        self.assertEqual(tokenizer.next(), ('id', 1))
        self.assertEqual(tokenizer.next(), ('1003', 1))
        self.assertEqual(tokenizer.next(), ('type', 1))
        self.assertEqual(tokenizer.next(), ('Blueberry', 1))
        self.assertEqual(tokenizer.next(), ('}', 0))
        self.assertEqual(tokenizer.next(), ('{', 0))
        self.assertEqual(tokenizer.next(), ('id', 1))
        self.assertEqual(tokenizer.next(), ('1004', 1))
        self.assertEqual(tokenizer.next(), ('type', 1))
        self.assertEqual(tokenizer.next(), ('Devils Food', 1))
        self.assertEqual(tokenizer.next(), ('}', 0))
        self.assertEqual(tokenizer.next(), (']', 0))
        self.assertEqual(tokenizer.next(), ('}', 0))
        self.assertEqual(tokenizer.next(), ('topping', 1))
        self.assertEqual(tokenizer.next(), ('[', 0))
        self.assertEqual(tokenizer.next(), ('{', 0))
        self.assertEqual(tokenizer.next(), ('id', 1))
        self.assertEqual(tokenizer.next(), ('5001', 1))
        self.assertEqual(tokenizer.next(), ('type', 1))
        self.assertEqual(tokenizer.next(), ('None', 1))
        self.assertEqual(tokenizer.next(), ('}', 0))
        self.assertEqual(tokenizer.next(), ('{', 0))
        self.assertEqual(tokenizer.next(), ('id', 1))
        self.assertEqual(tokenizer.next(), ('5002', 1))
        self.assertEqual(tokenizer.next(), ('type', 1))
        self.assertEqual(tokenizer.next(), ('Glazed', 1))
        self.assertEqual(tokenizer.next(), ('}', 0))
        self.assertEqual(tokenizer.next(), ('{', 0))
        self.assertEqual(tokenizer.next(), ('id', 1))
        self.assertEqual(tokenizer.next(), ('5005', 1))
        self.assertEqual(tokenizer.next(), ('type', 1))
        self.assertEqual(tokenizer.next(), ('Sugar', 1))
        self.assertEqual(tokenizer.next(), ('}', 0))
        self.assertEqual(tokenizer.next(), ('{', 0))
        self.assertEqual(tokenizer.next(), ('id', 1))
        self.assertEqual(tokenizer.next(), ('5007', 1))
        self.assertEqual(tokenizer.next(), ('type', 1))
        self.assertEqual(tokenizer.next(), ('Powdered Sugar', 1))
        self.assertEqual(tokenizer.next(), ('}', 0))
        self.assertEqual(tokenizer.next(), ('{', 0))
        self.assertEqual(tokenizer.next(), ('id', 1))
        self.assertEqual(tokenizer.next(), ('5006', 1))
        self.assertEqual(tokenizer.next(), ('type', 1))
        self.assertEqual(tokenizer.next(), ('Chocolate with Sprinkles', 1))
        self.assertEqual(tokenizer.next(), ('}', 0))
        self.assertEqual(tokenizer.next(), ('{', 0))
        self.assertEqual(tokenizer.next(), ('id', 1))
        self.assertEqual(tokenizer.next(), ('5003', 1))
        self.assertEqual(tokenizer.next(), ('type', 1))
        self.assertEqual(tokenizer.next(), ('Chocolate', 1))
        self.assertEqual(tokenizer.next(), ('}', 0))
        self.assertEqual(tokenizer.next(), ('{', 0))
        self.assertEqual(tokenizer.next(), ('id', 1))
        self.assertEqual(tokenizer.next(), ('5004', 1))
        self.assertEqual(tokenizer.next(), ('type', 1))
        self.assertEqual(tokenizer.next(), ('Maple', 1))
        self.assertEqual(tokenizer.next(), ('}', 0))
        self.assertEqual(tokenizer.next(), (']', 0))
        self.assertEqual(tokenizer.next(), ('}', 0))
        self.assertEqual(tokenizer.has_next(), False)

    def test_next_array_of_arrays(self):
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

        stream = io.StringIO(json)
        tokenizer = JsonTokenizer(stream)

        self.assertEqual(tokenizer.next(), ('{', 0))
        self.assertEqual(tokenizer.next(), ('id', 1))
        self.assertEqual(tokenizer.next(), ('123', 1))
        self.assertEqual(tokenizer.next(), ('type', 1))
        self.assertEqual(tokenizer.next(), ('car', 1))
        self.assertEqual(tokenizer.next(), ('data', 1))
        self.assertEqual(tokenizer.next(), ('[', 0))
        self.assertEqual(tokenizer.next(), ('[', 0))
        self.assertEqual(tokenizer.next(), ('red', 1))
        self.assertEqual(tokenizer.next(), ('blue', 1))
        self.assertEqual(tokenizer.next(), ('green', 1))
        self.assertEqual(tokenizer.next(), (']', 0))
        self.assertEqual(tokenizer.next(), ('[', 0))
        self.assertEqual(tokenizer.next(), ('1', 2))
        self.assertEqual(tokenizer.next(), ('2', 2))
        self.assertEqual(tokenizer.next(), ('3', 2))
        self.assertEqual(tokenizer.next(), (']', 0))
        self.assertEqual(tokenizer.next(), ('[', 0))
        self.assertEqual(tokenizer.next(), ('true', 2))
        self.assertEqual(tokenizer.next(), ('false', 2))
        self.assertEqual(tokenizer.next(), ('true', 2))
        self.assertEqual(tokenizer.next(), (']', 0))
        self.assertEqual(tokenizer.next(), (']', 0))
        self.assertEqual(tokenizer.next(), ('}', 0))
        self.assertEqual(tokenizer.has_next(), False)


suite = unittest.TestLoader().loadTestsFromTestCase(TestJsonTokenizerMethods)
unittest.TextTestRunner(verbosity=2).run(suite)
