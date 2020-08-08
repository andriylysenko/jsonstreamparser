#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include <iostream>
#include <stdint.h>

static PyObject* readRawStream(PyObject* stream, long bufferSize, bool needToDecode, PyObject* encoding) {
    PyObject *bufferObject = PyObject_CallMethodObjArgs(stream, PyUnicode_FromString("read"), PyLong_FromLong(bufferSize), NULL);
    if (!needToDecode) {
        return bufferObject;
    }
    PyObject *bufferObjectEncoded = PyObject_CallMethodObjArgs(bufferObject, PyUnicode_FromString("decode"), encoding, NULL);
    return bufferObjectEncoded;
}

static bool hasNextInt(PyObject* stream, long bufferSize, bool needToDecode, PyObject* encoding, PyObject **bufferObject, long** bufferPos, long** readSize,  bool* bufferUpdated) {
    *bufferUpdated = false;
    if (**bufferPos == -1 || **bufferPos == **readSize) {
        *bufferObject = readRawStream(stream, bufferSize, needToDecode, encoding);
        **bufferPos = 0;
        **readSize = PyUnicode_GET_LENGTH(*bufferObject);
        *bufferUpdated = true;
    }
    return **bufferPos < **readSize;
}

static PyObject* hasNext(PyObject* self, PyObject* args) {
    PyObject* stream = nullptr;
    long bufferSize;
    PyObject* needToDecodeObject = nullptr;
    PyObject* encoding = nullptr;
    PyObject* bufferObject = nullptr;
    long bufferPos;

    if (!PyArg_ParseTuple(args, "OlOOOl", &stream, &bufferSize, &needToDecodeObject, &encoding, &bufferObject, &bufferPos)) {
        return nullptr;
    }
    bool needToDecode = (bool)PyObject_IsTrue(needToDecodeObject);

    long* bPos = &bufferPos;
    long* readSize = &bufferSize;
    bool bufferUpdated;
    bool hasnext = hasNextInt(stream, bufferSize, needToDecode, encoding, &bufferObject, &bPos, &readSize, &bufferUpdated);
    PyObject* const result = PyTuple_New(4);
    if (bufferUpdated) {
        PyTuple_SET_ITEM(result, 0, bufferObject);
    } else {
        PyObject *noneValue = Py_None;
        Py_INCREF(noneValue);
        PyTuple_SET_ITEM(result, 0, noneValue);
    }
    PyTuple_SET_ITEM(result, 1, PyLong_FromLong(*readSize));
    PyTuple_SET_ITEM(result, 2, PyLong_FromLong(*bPos));
    PyObject *hasnextPyObject = hasnext ? Py_True : Py_False;
    Py_INCREF(hasnextPyObject);
    PyTuple_SET_ITEM(result, 3, hasnextPyObject);
    return result;
}

static PyObject* append(PyObject* tokenObj, long tokenSize, Py_UCS4 maxChar,  long kind, void* buffer, long start, long end) {
    if (tokenSize == 0) {
        PyObject *result = PyUnicode_New(end - start, maxChar);
        for (int i = start, j = 0; i < end; i++, j++) {
            Py_UCS4 ch = PyUnicode_READ(kind, buffer, i);
            PyUnicode_WriteChar(result, j, ch);
        }
        return result;
    } else {
        PyObject *result = PyUnicode_New(tokenSize + (end - start), maxChar);
        void* tokenObjData = PyUnicode_DATA(tokenObj);
        for (int i = 0; i < tokenSize; i++) {
            Py_UCS4 ch = PyUnicode_READ(kind, tokenObjData, i);
            PyUnicode_WriteChar(result, i, ch);
        }
        Py_DECREF(tokenObj);
        for (int i = start, j = tokenSize; i < end; i++, j++) {
            Py_UCS4 ch = PyUnicode_READ(kind, buffer, i);
            PyUnicode_WriteChar(result, j, ch);
        }
        return result;
    }
}

static PyObject* convertToResult(PyObject* buffer, long readSize, long currentPos, PyObject* token, long token_type, bool simpleValueStarted) {
    PyObject* const result = PyTuple_New(6);
    if (buffer != nullptr) {
        PyTuple_SET_ITEM(result, 0, buffer);
    } else {
        PyObject *noneValue = Py_None;
        Py_INCREF(noneValue);
        PyTuple_SET_ITEM(result, 0, noneValue);
    }
    PyTuple_SET_ITEM(result, 1, PyLong_FromLong(readSize));
    PyTuple_SET_ITEM(result, 2, PyLong_FromLong(currentPos));
    PyTuple_SET_ITEM(result, 3, token);
    PyTuple_SET_ITEM(result, 4, PyLong_FromLong(token_type));
    PyObject *simpleValueStartedPyObject = simpleValueStarted ? Py_True : Py_False;
    Py_INCREF(simpleValueStartedPyObject);
    PyTuple_SET_ITEM(result, 5, simpleValueStartedPyObject);
    return result;
}

static PyObject* nextInt(PyObject* stream, long bufferSize, bool needToDecode, PyObject* encoding, PyObject* bufferObject, long bufferPos, bool simpleValueStarted) {
    PyObject** b = &bufferObject;
    long* bPos = &bufferPos;
    long* readSize = &bufferSize;

    PyUnicode_READY(*b);
    void *buffer = PyUnicode_DATA(*b);
    long kind = PyUnicode_KIND(*b);
    bool bufferUpdated = false;
    bool localBufferUpdated;
    while (hasNextInt(stream, bufferSize, needToDecode, encoding, b, &bPos, &readSize, &localBufferUpdated)) {
        if (localBufferUpdated) {
            bufferUpdated = true;
            PyUnicode_READY(*b);
            buffer = PyUnicode_DATA(*b);
            kind = PyUnicode_KIND(*b);
        }
        Py_UCS4 ch = PyUnicode_READ(kind, buffer, *bPos);

        if (ch == '{' || ch == '}' || ch == '[' || ch == ']') {
            (*bPos)++;
            if (ch == '[') {
                simpleValueStarted = true;
            } else {
                simpleValueStarted = false;
            }
            PyObject *tokenObj = PyUnicode_New(1, ch);
            PyUnicode_WriteChar(tokenObj, 0, ch);
            return convertToResult(bufferUpdated ? *b : nullptr, *readSize, *bPos, tokenObj, 0, simpleValueStarted);
        } else if (ch == ':' || ch == ',') {
            (*bPos)++;
            simpleValueStarted = true;
        } else if (ch == '"') {
            (*bPos)++;
            bool endOfToken = false;
            bool encodingStarted = false;
            long tokenSize = 0;
            Py_UCS4 maxChar = 0;
            PyObject *tokenObj = nullptr;
            while (!endOfToken && hasNextInt(stream, bufferSize, needToDecode, encoding, b, &bPos, &readSize, &localBufferUpdated)) {
                if (localBufferUpdated) {
                    bufferUpdated = true;
                    PyUnicode_READY(*b);
                    buffer = PyUnicode_DATA(*b);
                    kind = PyUnicode_KIND(*b);
                }
                int i = *bPos;
                int end = *readSize;
                while (i < end) {
                    Py_UCS4 v = PyUnicode_READ(kind, buffer, i);
                    if (v > maxChar) {
                        maxChar = v;
                    }
                    if (!encodingStarted && v == '"') {
                        endOfToken = true;
                        tokenObj = append(tokenObj, tokenSize, maxChar, kind, buffer, *bPos, i);
                        tokenSize += i - (*bPos);
                        *bPos = i+1;
                        simpleValueStarted = false;
                        return convertToResult(bufferUpdated ? *b : nullptr, *readSize, *bPos, tokenObj, 1, simpleValueStarted);
                    }
                    if (!encodingStarted && v == '\\') {
                        encodingStarted = true;
                    } else if (encodingStarted) {
                        encodingStarted = false;
                    }
                    i++;
                }
                tokenObj = append(tokenObj, tokenSize, maxChar, kind, buffer, *bPos, i);
                tokenSize += i - (*bPos);
                *bPos = i;
            }
            simpleValueStarted = false;
            return convertToResult(bufferUpdated ? *b : nullptr, *readSize, *bPos, tokenObj, 1, simpleValueStarted);
        } else if (simpleValueStarted && ch != ' ' && ch != '\t' && ch != '\n') {
            bool endOfToken = false;
            long tokenSize = 0;
            Py_UCS4 maxChar = 0;
            PyObject *tokenObj = nullptr;
            while (!endOfToken && hasNextInt(stream, bufferSize, needToDecode, encoding, b, &bPos, &readSize, &localBufferUpdated)) {
                if (localBufferUpdated) {
                    bufferUpdated = true;
                    PyUnicode_READY(*b);
                    buffer = PyUnicode_DATA(*b);
                    kind = PyUnicode_KIND(*b);
                }
                int i = *bPos;
                int end = *readSize;
                while (i < end) {
                    Py_UCS4 v = PyUnicode_READ(kind, buffer, i);
                    if (v > maxChar) {
                        maxChar = v;
                    }
                    if (!((v >= 'a' && v <= 'z') || (v >= '0' && v <= '9')
                        || v == '.' || v == 'e' || v == 'E' || v == '-')) {
                        endOfToken = true;
                        break;
                    }
                    i++;
                }
                tokenObj = append(tokenObj, tokenSize, maxChar, kind, buffer, *bPos, i);
                tokenSize += i - (*bPos);
                *bPos = i;
            }
            simpleValueStarted = false;
            return convertToResult(bufferUpdated ? *b : nullptr, *readSize, *bPos, tokenObj, 2, simpleValueStarted);
        } else {
            (*bPos)++;
        }
    }
    PyObject *noneTokenObj = Py_None;
    Py_INCREF(noneTokenObj);
    return convertToResult(nullptr, *readSize, *bPos, noneTokenObj, -1, false);
}

static PyObject* next(PyObject* self, PyObject* args) {
    PyObject* stream = nullptr;
    long bufferSize;
    PyObject* needToDecodeObject = nullptr;
    PyObject* encoding = nullptr;
    PyObject* bufferObject = nullptr;
    long bufferPos;
    PyObject* simpleValueStarted = nullptr;

    if (!PyArg_ParseTuple(args, "OlOOOlO", &stream, &bufferSize, &needToDecodeObject, &encoding, &bufferObject, &bufferPos, &simpleValueStarted)) {
        return nullptr;
    }
    bool needToDecode = (bool)PyObject_IsTrue(needToDecodeObject);

    return nextInt(stream, bufferSize, needToDecode, encoding, bufferObject, bufferPos, simpleValueStarted);
}

static PyObject* toSnakeCase(PyObject* self, PyObject* args) {
    PyObject* bufferObject = nullptr;

    if (!PyArg_ParseTuple(args, "O", &bufferObject)) {
        return nullptr;
    }

    long size = PyUnicode_GET_SIZE(bufferObject);
    wchar_t *str = (wchar_t*) PyUnicode_AS_UNICODE(bufferObject);
    wchar_t *p = str+1;
    const wchar_t* const end = str + size;
    long cnt = 0;
    while (p < end) {
        if (*p >= 'A' && *p <= 'Z') {
            cnt++;
        }
        p++;
    }
    wchar_t cpy[size+cnt];
    p = str;
    long i = 0;
    while (i < size+cnt) {
        if (*p >= 'A' && *p <= 'Z') {
            if (i == 0) {
                cpy[i] = tolower(*p);
            } else {
                cpy[i] = '_';
                i++;
                cpy[i] = tolower(*p);
            }
        } else {
            cpy[i] = *p;
        }
        i++;
        p++;
    }
    return PyUnicode_FromWideChar(cpy, size+cnt);
}

static PyObject* toPascalCase(PyObject* self, PyObject* args) {
    PyObject* bufferObject = nullptr;

    if (!PyArg_ParseTuple(args, "O", &bufferObject)) {
        return nullptr;
    }

    long size = PyUnicode_GET_SIZE(bufferObject);
    wchar_t *str = (wchar_t*) PyUnicode_AS_UNICODE(bufferObject);
    wchar_t cpy[size];
    wchar_t *p = str;
    long i = 0;
    while (i < size) {
        if (i == 0) {
            cpy[i] = toupper(*p);
        } else {
            cpy[i] = *p;
        }
        i++;
        p++;
    }
    return PyUnicode_FromWideChar(cpy, size);
}

static PyMethodDef UTILS_METHODS[] = {
 { "next", next, METH_VARARGS, ""},
 { "has_next", hasNext, METH_VARARGS, ""},
 { "to_snake_case", toSnakeCase, METH_VARARGS, ""},
 { "to_pascal_case", toPascalCase, METH_VARARGS, ""},
 { (const char*)0, (PyCFunction)0, 0, (const char*)0 }
};

static struct PyModuleDef moduleDefinition = {
 PyModuleDef_HEAD_INIT,
 "stream_json_parser_utils",    /* m_name */
 /* m_doc */
 "Some C++ functions for json tokenizer",
 -1, /* m_size */
 UTILS_METHODS, /* m_methods */
 NULL,  /* m_reload */
 NULL,  /* m_traverse */
 NULL,  /* m_clear */
 NULL   /* m_free */
};

PyMODINIT_FUNC PyInit_stream_json_parser_utils() {
    return PyModule_Create(&moduleDefinition);
}
