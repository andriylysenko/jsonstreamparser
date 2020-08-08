import io
import ctypes
import stream_json_parser_utils


class JsonTokenizer:
    _stream: io.RawIOBase = None
    _encoding = None
    _buffer_size: int = None
    _buffer = None
    _buffer_pos = ctypes.c_int(-1)
    _simple_value_started = ctypes.c_bool(False)
    _need_to_decode = None

    def __init__(self, stream: io.RawIOBase, encoding: str = 'utf-8', buffer_size: int = 20):
        self._stream = stream
        if isinstance(self._stream, io.TextIOBase):
            self._need_to_decode = False
        else:
            self._need_to_decode = True
        self._encoding = encoding
        self._buffer_size = buffer_size
        self._buffer = ''
        self._buffer_pos = ctypes.c_int(-1)
        self._simple_value_started = ctypes.c_bool(False)

    def next(self) -> str:
        (buffer, self._buffer_size, self._buffer_pos.value, token, token_type,
         self._simple_value_started.value) = stream_json_parser_utils.next(self._stream, self._buffer_size,
                                                                           self._need_to_decode, self._encoding,
                                                                           self._buffer, self._buffer_pos.value,
                                                                           self._simple_value_started.value)
        if buffer:
            self._buffer = buffer
        return token, token_type

    def has_next(self) -> bool:
        (buffer, self._buffer_size, self._buffer_pos.value, hasnext) = stream_json_parser_utils.has_next(self._stream,
                                                                                                         self._buffer_size,
                                                                                                         self._need_to_decode,
                                                                                                         self._encoding,
                                                                                                         self._buffer,
                                                                                                         self._buffer_pos.value)
        if buffer:
            self._buffer = buffer
        return hasnext
