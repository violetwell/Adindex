#include "store.h"

Store::Store(void *_conf) {
	Conf_t *conf = (Conf_t*)_conf;
	schema = conf->schema;
	int num = schema->size(), i = 0;
	storeFieldNumber = 0;
	storeField = (IndexField**)malloc(sizeof(IndexField*) * num);
	IndexField **fieldArray = schema->toArray();
	for (i = 0; i < num; i++) {
		IndexField *get = fieldArray[i];
		if (get->isStore()) {
			storeField[storeFieldNumber++] = get;
		}
	}
	path = conf->index;
	fileNumber = 0;
	maxdoc = conf->indexmaxdoc;
	posArr = (Store_Pos*)malloc(sizeof(Store_Pos) * maxdoc);
	lenArr = (Store_Length*)malloc(sizeof(Store_Length) * maxdoc);
	storeBuffer = (char*)malloc(sizeof(char) * MAX_STORE_LENGTH);
	storeLen = 0;
	storeFile = NULL;
	init();
}

Store::~Store() {
	if (storeField != NULL) {
		free(storeField);
		storeField = NULL;
	}
	if (posArr != NULL) {
		free(posArr);
		posArr = NULL;
	}
	if (lenArr != NULL) {
		free(lenArr);
		lenArr = NULL;
	}
	if (storeBuffer != NULL) {
		free(storeBuffer);
		storeBuffer = NULL;
	}
}

int Store::close() {
	if (docNumber > 0 && savePosLen() <= 0) return 0;
    if (storeFile != NULL) {
		fclose(storeFile);
		storeFile = NULL;
	}
	return 1;
}

int Store::saveDoc(DocParser *parser) {
	docNumber++;
	if (parser == NULL) return 0;
	storeLen = 0;
	int i = 0, number = 0, begin = storeLen++;
	for (i = 0; i < storeFieldNumber; i++) {
		IndexField *field = storeField[i];
		int fieldId = field->getNumber();
		char *data = parser->data(fieldId);
		int length = parser->length(fieldId);
		if (data == NULL || length <= 0) continue;
		number++;
		storeBuffer[storeLen++] = (char)fieldId;                                       /* fieldId */
		if (field->getLength() == 0) {                                                 /* variable length */
			int vint = length;
			while ((vint & ~0x7F) != 0) {
				storeBuffer[storeLen++] = (char)((vint & 0x7F) | 0x80);
				vint >>= 7;
			}
			storeBuffer[storeLen++] = (char)((vint & 0x7f));
		}
		memcpy(storeBuffer + storeLen, data, length);
		storeLen += length;
	}
	storeBuffer[begin] = (char)number;                                                  /* store field count */
	if (writeData() <= 0) return 0;
	return 1;
}

int Store::writeData() {
	if (NULL == storeFile) {
		char tpfile[MAX_DIR_LEN];
		snprintf(tpfile, MAX_DIR_LEN, "%s%s.%d.0", path, STORE_FILE_NAME, fileNumber);
		storeFile = fopen(tpfile, "w");
		if (NULL == storeFile) return 0;
	}
	fseek(storeFile, 0, SEEK_END);                                                       /* find end of file */
	posArr[docNumber - 1].pos = ftell(storeFile);                                        /* find shift from head of file */
	lenArr[docNumber - 1].length = storeLen;
	int len = fwrite(storeBuffer, sizeof(char), storeLen, storeFile);                    /* write store.dat.fileNumber.0 */
	if (len != storeLen) return 0;
	if (docNumber == maxdoc) {
		if (savePosLen() <= 0) return 0;
		fileNumber++;
		if (init() <= 0) return 0;                                                        /* close store.dat.fileNumber.0, next time will open a new store.dat.fileNumber+1.0 */
	}
	return 1;
}

int Store::init() {
	bzero(posArr, sizeof(Store_Pos) * maxdoc);
	bzero(lenArr, sizeof(Store_Length) * maxdoc);
	docNumber = 0;
	if (storeFile != NULL) {
		fclose(storeFile);
		storeFile = NULL;
	}
	return 1;
}

int Store::savePosLen() {
	FILE *posfp, *lengthfp;
	char tpfile[MAX_DIR_LEN];
	int ret = 0;
	snprintf(tpfile, MAX_DIR_LEN, "%s%s.%d", path, STORE_POS_FILE_NAME, fileNumber);
	posfp = fopen(tpfile,"w");
	if (NULL == posfp) return 0;
	ret = fwrite(posArr, sizeof(Store_Pos), docNumber, posfp);                                  /* write store.pos.fileNumber */
	if(docNumber != ret) return 0;
	fclose(posfp);
	snprintf(tpfile, MAX_DIR_LEN, "%s%s.%d", path, STORE_LEN_FILE_NAME, fileNumber);
	lengthfp = fopen(tpfile, "w");
	if (NULL == lengthfp) return 0;
	ret = fwrite(lenArr, sizeof(Store_Length), docNumber, lengthfp);                            /* write store.len.fileNumber */
	if (docNumber != ret) return 0;
	fclose(lengthfp);
	return 1;
}

Brife::Brife(void *_conf) {
	Conf_t *conf = (Conf_t*)_conf;
	schema = conf->schema;
	int num = schema->size(), i = 0;
	brifeFieldNumber = 0;
    brifeField = (IndexField**)malloc(sizeof(IndexField*) * num);
	IndexField **fieldArray = schema->toArray();
	for (i = 0; i < num; i++) {
		IndexField *get = fieldArray[i];
		if (get->isFilter() && get->getLength() > 0) {
			brifeField[brifeFieldNumber++] = get;
		}
	}
	path = conf->index;
	fileNumber = 0;
	maxdoc = conf->indexmaxdoc;
	dataBuffer = (char**)malloc(sizeof(char*) * brifeFieldNumber);
	dataName = (char**)malloc(sizeof(char*) * brifeFieldNumber);
	brifeFiles = (FILE**)malloc(sizeof(FILE*) * brifeFieldNumber);
	bzero(brifeFiles, sizeof(FILE*) * brifeFieldNumber);
	bzero(dataName, sizeof(char*) * brifeFieldNumber);
	bzero(dataBuffer, sizeof(char*) * brifeFieldNumber);
	for (i = 0; i < brifeFieldNumber; i++) {
		IndexField *field = brifeField[i];
		int length = field->getLength();
		dataBuffer[i] = (char*)malloc(sizeof(char) * length * maxdoc);
		dataName[i] = field->getName();
	}
	init();
}

Brife::~Brife() {
	int i = 0;
	if (brifeField != NULL) free(brifeField);
	if (dataBuffer != NULL) {
		for (i = 0; i < brifeFieldNumber; i++) {
			char *buffer = dataBuffer[i];
			if (buffer != NULL) free(buffer);
		}
		free(dataBuffer);
	}
	if (dataName != NULL) free(dataName);
	if (brifeFiles != NULL) free(brifeFiles);
	
}

int Brife::close() {
	if (docNumber > 0 && writeData() <= 0) return 0;
	int i = 0;
	for (i = 0; i < brifeFieldNumber; i++) {
		if (NULL != brifeFiles[i]) fclose(brifeFiles[i]);
	}
	return 1;
}

int Brife::saveDoc(DocParser *parser) {
	if (parser == NULL) return 0;
	int i = 0;
	for (i = 0; i < brifeFieldNumber; i++) {
		IndexField *field = brifeField[i];
		int fieldLength = field->getLength();
		int fieldId = field->getNumber();
		char *data = parser->data(fieldId);
		int length = parser->length(fieldId);
		if (data == NULL || length <= 0 || length != fieldLength) {
			bzero(dataBuffer[i] + docNumber * field->getLength(), sizeof(char) * field->getLength());              /* brief field of doc is illegal */
		} else {
			memcpy(dataBuffer[i] + docNumber * field->getLength(), data, length);                                  /* copy brief field into dataBuffer */
		}
	}
	if (++docNumber == maxdoc) {
		if (writeData() <= 0) return 0;
		init();
	}
	return 1;
}

int Brife::init() {
	int i = 0;
	for (i = 0; i < brifeFieldNumber; i++) {
		if (NULL != brifeFiles[i]) fclose(brifeFiles[i]);
		brifeFiles[i] = NULL;
	}
	for (i = 0; i < brifeFieldNumber; i++) {
		char *buffer = dataBuffer[i];
		bzero(buffer, sizeof(char) * brifeField[i]->getLength() * maxdoc);
	}
	docNumber = 0;
	return 1;
}

int Brife::writeData() {
	char tpfile[MAX_DIR_LEN];
	int i = 0, res = 0;
	for (i = 0; i < brifeFieldNumber; i++) {
		snprintf(tpfile, MAX_DIR_LEN, "%s%s.dat.%d", path, dataName[i], fileNumber);
		brifeFiles[i] = fopen(tpfile, "w");
		if (NULL == brifeFiles[i]) return 0;
		res = fwrite(dataBuffer[i], brifeField[i]->getLength(), docNumber, brifeFiles[i]);                         /* write brief file: fieldname.dat.fileNumber */
		if (res != docNumber) return 0;
	}
	fileNumber++;
	return 1;
}
