from tokenizer.jsontokenizer import JsonTokenizer
from typing import Dict
import stream_json_parser_utils


class JsonDecoder:
    _tokenizer: JsonTokenizer = None
    _class_name: str = 'Root'
    _handler = lambda e, p: e
    _predicate: str = None
    _translators: Dict = None
    _snake_case: bool = False

    def __init__(self):
        self._translators = dict()
        self._snake_case = False

    def tokenizer(self, tokenizer):
        self._tokenizer = tokenizer
        return self

    def root_class_name(self, class_name):
        self._class_name = class_name
        return self

    def event_handler(self, handler):
        self._handler = handler
        return self

    def predicate(self, predicate):
        self._predicate = predicate
        return self

    def translator(self, path, translator):
        self._translators[path] = translator
        return self

    def with_snake_cased_props(self):
        self._snake_case = True
        return self

    def decode(self):
        path = []
        token, token_type = self._tokenizer.next()
        if token == '{':
            self._decode(self._class_name, path,
                         self._predicate.split('.') if self._predicate and len(self._predicate) > 0 else list(),
                         self._handler)
        elif token == '[':
            self._decode_list(self._class_name, path,
                              self._predicate.split('.') if self._predicate and len(self._predicate) > 0 else list(),
                              self._handler)

    def _decode(self, class_name, path, predicate, handler):
        props = dict()
        while self._tokenizer.has_next():
            (prop_name, prop_type) = self._tokenizer.next()
            if prop_name == '}':
                if len(path) >= len(predicate):
                    obj = type(class_name, (), props)()
                    p = '.'.join(path)
                    if p in self._translators:
                        # obj = self._translators[p].translate(obj)
                        obj = self._translators[p](obj)
                    handler(obj, p)
                return
            path.append(prop_name)
            # print(path)
            (prop_value, value_type) = self._tokenizer.next()
            if value_type == 2:
                if prop_value == 'null':
                    prop_value = None
                elif prop_value == 'true':
                    prop_value = True
                elif prop_value == 'false':
                    prop_value = False
            if prop_value == '{':
                if self._path_matches(path, predicate):
                    self._decode(self.to_pascal_case(prop_name), path, predicate, handler)
                elif len(path) > len(predicate):
                    elements = list()
                    self._decode(self.to_pascal_case(prop_name), path, predicate,
                                 lambda e, p: self._record_result(elements, e))
                    prop_value = elements.pop()
                else:
                    self._skip_value()
            elif prop_value == '[':
                if self._path_matches(path, predicate):
                    self._decode_list(self.to_pascal_case(prop_name), path, predicate, handler)
                elif len(path) > len(predicate):
                    elements = list()
                    self._decode_list(self.to_pascal_case(prop_name), path, predicate,
                                      lambda e, p: self._record_result(elements, e))
                    prop_value = elements
                else:
                    self._skip_list()
            else:
                if self._path_matches(path, predicate) and len(path) == len(predicate):
                    p = '.'.join(path)
                    if p in self._translators:
                        # prop_value = self._translators[p].translate(prop_value)
                        prop_value = self._translators[p](prop_value)
                    handler(prop_value, p)

            if len(path) > len(predicate):
                props[self.to_snake_case(prop_name) if self._snake_case else prop_name] = prop_value
            path.pop()

    def _decode_list(self, class_name, path, predicate, handler):
        p = None
        while self._tokenizer.has_next():
            elem, elem_type = self._tokenizer.next()
            if elem == ']':
                break
            if elem == '{':
                self._decode(class_name, path, predicate, handler)
            elif elem == '[':
                if self._path_matches(path, predicate):
                    self._decode_list(class_name, path, predicate, handler)
                elif len(path) > len(predicate):
                    elements = list()
                    self._decode_list(class_name, path, predicate, lambda e, p: self._record_result(elements, e))
                    handler(elements, p)
                else:
                    self._skip_list()
            else:
                if elem_type == 2:
                    if elem == 'null':
                        elem = None
                    elif elem == 'true':
                        elem = True
                    elif elem == 'false':
                        elem = False
                if not p:
                    p = '.'.join(path)
                if p in self._translators:
                    # elem = self._translators[p].translate(elem)
                    elem = self._translators[p](elem)
                handler(elem, p)

    def _skip_value(self):
        while self._tokenizer.has_next():
            prop_name, prop_type = self._tokenizer.next()
            if prop_name == '}':
                return
            prop_value, prop_type = self._tokenizer.next()
            if prop_value == '{':
                self._skip_value()
            elif prop_value == '[':
                self._skip_list()

    def _skip_list(self):
        while self._tokenizer.has_next():
            elem, elem_type = self._tokenizer.next()
            if elem == ']':
                break
            if elem == '{':
                self._skip_value()

    def _record_result(self, elements, e):
        elements.append(e)

    def to_snake_case(self, s):
        return stream_json_parser_utils.to_snake_case(s)

    def to_pascal_case(self, s):
        return stream_json_parser_utils.to_pascal_case(s)

    def _path_matches(self, path, predicate):
        return True if len(predicate) > 0 and len(predicate) >= len(path) and path[-1] == predicate[
            len(path) - 1] else False
