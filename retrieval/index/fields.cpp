#include "fields.h"

IndexField::IndexField(char* _name, int _number, char* _type, int _length, 
bool _index, bool _store, bool _filter) {
    char *tmp = (char*)malloc(strlen(_name) + 1);
	strcpy(tmp, _name);
	tmp[strlen(_name)] = 0;
	name = tmp;
    number = _number;
    if (strcmp(INDEX_FIELD_INT_STR, _type) == 0) {
	    type = INDEX_FIELD_INT;
    } else if (strcmp(INDEX_FIELD_STR_STR, _type) == 0) {
	    type = INDEX_FIELD_STR;
    } else if (strcmp(INDEX_FIELD_DNF_STR, _type) == 0) {
	    type = INDEX_FIELD_DNF;
    } else if (strcmp(INDEX_FIELD_TAG_STR, _type) == 0) {
		type = INDEX_FIELD_TAG;
	} else {
	    type = INDEX_FIELD_BIN;
    }
	length = _length;
    index = _index;
    store = _store;
    filter = _filter;
}

IndexField::~IndexField() {
    free(name);
}

char* IndexField::getName() {
    return name;
}

int IndexField::getNumber() {
    return number;
}

int IndexField::getType() {
    return type;
}

int IndexField::getLength() {
	return length;
}

bool IndexField::isIndex() {
    return index;
}

bool IndexField::isStore() {
    return store;
}

bool IndexField::isFilter() {
    return filter;
}

IndexSchema::IndexSchema() {
	num = 0;
	counter = 0;
	array = NULL;
	primary = NULL;
}

IndexSchema::~IndexSchema() {
	for(map<string, IndexField*>::const_iterator iter = schema.begin(); iter != schema.end(); ++iter) {
		IndexField* value = iter->second;
		delete value;
	}
	free(array);
}

IndexField* IndexSchema::getField(string name) {
    return schema[name];
}

void IndexSchema::setPrimary(IndexField *n) {
	primary = n;
}

IndexField* IndexSchema::getPrimary() {
	return primary;
}

int IndexSchema::size() {
	return num;
}

IndexField** IndexSchema::toArray() {
	return array;
}

void IndexSchema::addField(string name, IndexField *field) {
    schema[name] = field;
	num++;
	if (array == NULL) {
		counter = 4;
		array = (IndexField**)malloc(sizeof(IndexField*) * counter);
	}
	if (num == counter) {
		counter = counter * 2;
		IndexField** newArray = (IndexField**)malloc(sizeof(IndexField*) * counter);
		int i = 0;
		for (i = 0; i < num - 1; i++) {
			newArray[i] = array[i];
		}
		free(array);
		array = newArray;
	}
	array[num - 1] = field;
}

