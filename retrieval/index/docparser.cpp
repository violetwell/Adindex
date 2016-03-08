#include "docparser.h"

DocParser::DocParser(int _groupno, char *_path, IndexSchema *_schema) {
	groupno = _groupno;
	path = _path;
	schema = _schema;
	fieldArray = schema->toArray();
	number = schema->size();
	rawfp = NULL;
	br = NULL;
	fileno = 0;
	dataBuffer = (char*)malloc(MAX_DOC_LENGTH);
	dataArray = (char**)malloc(sizeof(char*) * number);
	lenArray = (int*)malloc(sizeof(int) * number);
}

DocParser::DocParser(IndexSchema *_schema) {
	schema = _schema;
	fieldArray = schema->toArray();
	number = schema->size();
	rawfp = NULL;
	br = NULL;
	dataBuffer = (char*)malloc(MAX_DOC_LENGTH);
	dataArray = (char**)malloc(sizeof(char*) * number);
	lenArray = (int*)malloc(sizeof(int) * number);
}

DocParser::~DocParser() {
	if(dataBuffer != NULL) free(dataBuffer);
	if(lenArray != NULL) free(lenArray);
	if(dataArray != NULL) free(dataArray);
}

bool DocParser::initData(char* bindata, int datalen) {
	cursor = 0;
	if (bindata == NULL || datalen <= 2) return false;
	bzero(dataArray, sizeof(char**) * number);
	bzero(lenArray, sizeof(int) * number);
	int begin = 0;
	int flag = (int)(bindata[begin++] & 0xff);
	if (flag != DOC_BEGIN) return false;
	flag = (int)(bindata[begin++] & 0xff);                                    /* field id */
	while (flag >= 0 && flag < number) {
		IndexField *field = fieldArray[flag];
		int length = field->getLength();
		int res = 0;
		if (length == 0) {                                                    /* variable length */
			res = readBinary(bindata, begin, datalen, flag);
			begin += 4 + lenArray[flag];
		} else if (length > 0) {
			res = readFix(bindata, begin, datalen, flag, length);
			begin += length;
		} else {
			return false;
		}
		if (res <= 0) return false;
		flag = (int)(bindata[begin++] & 0xff);                                 /* next field */
	}
	if (flag != DOC_END) return false;
	return true;
}

int DocParser::readFix(char *bindata, int begin, int datalen, int index, int length) {
	lenArray[index] = length;
	dataArray[index] = dataBuffer + cursor;
	if ((datalen - begin) <= length) return 0;
	memcpy(dataBuffer + cursor, bindata + begin, length);
	cursor += length;
	return 1;
}

int DocParser::readBinary(char *bindata, int begin, int datalen, int index) {
	dataArray[index] = dataBuffer + cursor;
	if ((datalen - begin) <= 4) return 0;
	int ch1 = (int)(bindata[begin] & 0xff);
	int ch2 = (int)(bindata[begin + 1] & 0xff);
	int ch3 = (int)(bindata[begin + 2] & 0xff);
	int ch4 = (int)(bindata[begin + 3] & 0xff);
	if ((ch1 | ch2 | ch3 | ch4) < 0) return -1;
	int length = ((ch4 << 24) + (ch3 << 16) + (ch2 << 8) + (ch1 << 0));
	if ((datalen - begin - 4) <= length) return 0;
	lenArray[index] = length;
	memcpy(dataBuffer + cursor, bindata + begin + 4, length);
	cursor += length;
	return 1;
}

bool DocParser::hasNext() {                                      /* read a doc into dataBuffer */
	if (rawfp == NULL) {
		rawfp = Utils::openinfile(groupno, fileno, path, RAWFILENAME);
		br = new BufferedReader(rawfp, 0);
		if (rawfp == NULL) return false;
	}
	bzero(dataArray, sizeof(char**) * number);
	bzero(lenArray, sizeof(int) * number);
	cursor = 0;
	int begin = br->read();
	if (begin == -1) {                                           /* end of file */
		br->close();
		delete br;
		fileno++;                                                /* finish reading current file, move to next file */
		rawfp = NULL;
		return hasNext();
	} else if (begin == DOC_BEGIN) {
		int flag = br->read();
		while (flag >= 0 && flag < number) {
			IndexField *field = fieldArray[flag];
			int length = field->getLength();
			int res = 0;
			if (length == 0) {
				res = readBinary(flag);
			} else if (length > 0) {
				res = readFix(flag, length);
			} else {
				return false;
			}
			if (res <= 0) return false;
			flag = br->read();
		}
		if (flag == DOC_END) return true;
		else return false;
	} else {
		return false;
	}
}

char* DocParser::data(int index) {
	if (index >= number || index < 0) return NULL;
	return dataArray[index];
}

int DocParser::length(int index) {
	if (index >= number || index < 0) return -1;
	return lenArray[index];
}

int DocParser::readFix(int index, int length) {
	lenArray[index] = length;
	dataArray[index] = dataBuffer + cursor;
	int res = br->read(dataBuffer + cursor, 0, length);
	cursor += length;
	if (res != length) return -1;
	return 1;
}

int DocParser::readBinary(int index) {                           /* read variable length field, ie field.length = 0 */
	dataArray[index] = dataBuffer + cursor;
	int ch1 = br->read();
	int ch2 = br->read();
	int ch3 = br->read();
	int ch4 = br->read();
	if ((ch1 | ch2 | ch3 | ch4) < 0) return -1;
	int length = ((ch4 << 24) + (ch3 << 16) + (ch2 << 8) + (ch1 << 0));
	lenArray[index] = length;
	int res = br->read(dataBuffer + cursor, 0, length);
	cursor += length;
	if (res != length) return -1;
	return 1;
}

BufferedReader::BufferedReader(FILE *_file, int _size) {
	file = _file;
	if (_size <= 0) size = READ_BUFFER_LENGTH;
	else size = _size;
    count = 0;
	pos = 0;
	buf = (char*)malloc(size);
}

BufferedReader::~BufferedReader() {
	if (file != NULL) fclose(file);
	free(buf);
}

int BufferedReader::read() {                                            /* read a byte */
	if (pos >= count) {
		fill();
		if (pos >= count) return -1;
	}
	return buf[pos++] & 0xff;
}

float BufferedReader::readUb() {
	char temp[2];
	read(temp, 0, 2);
	return Utils::encode(temp);
}

long BufferedReader::readVLong() {
	int shift = 7, b = read();
    long i = b & 0x7F;
    for (; (b & 0x80) != 0; shift += 7) {
		b = read();
		i |= (b & 0x7FL) << shift;
    }
    return i;
}

long BufferedReader::readLong() {
	char rbuffer[8];
	read(rbuffer, 0, 8);
	return (rbuffer[0] & 0xff) + ((rbuffer[1] & 0xff) << 8) + 
	((rbuffer[2] & 0xff) << 16) + ((long)(rbuffer[3] & 0xff) << 24)
	+ ((long)(rbuffer[4] & 0xff) << 32) + ((long)(rbuffer[5] & 0xff) << 40) 
	+ ((long)(rbuffer[6] & 0xff) << 48) + ((long)(rbuffer[7] & 0xff) << 56);
}

int BufferedReader::readInt() {
	char rbuffer[4];
	read(rbuffer, 0, 4);
	return (rbuffer[0] & 0xff) + ((rbuffer[1] & 0xff) << 8) + ((rbuffer[2] & 0xff) << 16) + ((rbuffer[3] & 0xff) << 24);
}

int BufferedReader::readVInt() {
	int shift = 7, b = read();
    int i = b & 0x7F;
    for (; (b & 0x80) != 0; shift += 7) {
		b = read();
		i |= (b & 0x7F) << shift;
    }
    return i;
}

int BufferedReader::read1(char *b, int off, int len) {
	int avail = count - pos;
	if (avail <= 0) {
		fill();
		avail = count - pos;
		if (avail <= 0) return -1;
	}
	int cnt = (avail < len) ? avail : len;
	memcpy(b + off, buf + pos, cnt);
	pos += cnt;
	return cnt;
}

int BufferedReader::read(char *b, int off, int len) {                    /* read len bytes to b+off */
	if ((off | len | (off + len)) < 0) return -1;
	else if (len == 0) return 0;
	int n = 0;
	for (;;) {
		int nread = read1(b, off + n, len - n);
		if (nread <= 0) return (n == 0) ? nread : n;
		n += nread;
		if (n >= len) return n;
		if (feof(file)) return n;
	}
}

void BufferedReader::fill() {                                            /* fill the buffer */
	pos = 0;
	count = pos;
	int n = fread(buf + pos, sizeof(char), READ_BUFFER_LENGTH - pos, file);
	if (n > 0) count = n + pos;
}

void BufferedReader::close() {
	if (file != NULL) {
		fclose(file);
	    file = NULL;
	}
}
