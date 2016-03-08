#ifndef  __FIELDS_H_
#define  __FIELDS_H_

#include "def.h"
#include <map>
#include <string>
using namespace std;

class IndexField {
	
public:
    IndexField(char* _name, int _number, char* _type, int _length, 
	bool _index, bool _store, bool _filter);
	
    ~IndexField();
    char* getName();
    int getNumber();
    int getType();
	int getLength();
    bool isIndex();
    bool isStore();
    bool isFilter();
	
private:
    char *name;
    int number;                //the number of order in schema.xml
    int type;
	int length;
    bool index;
    bool store;
    bool filter;
	
};

class IndexSchema {
	
public:
    IndexSchema();
    ~IndexSchema();
    void addField(string name, IndexField *field);
    IndexField* getField(string name);
	void setPrimary(IndexField *n);
	IndexField* getPrimary();
	int size();
	IndexField** toArray();
	
private:
    map<string, IndexField*> schema;     //schema[name] = field
	IndexField *primary;
	IndexField **array;              //store IndexField* in the order they added in to schema
	int num, counter;                //num:number of IndexField
	
};

#endif
