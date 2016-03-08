#ifndef SCHEMA_H
#define SCHEMA_H
#include <iostream>
#include <stdlib.h>
#include <map>
#include <vector>
#include <rapidxml.hpp>
#include <rapidxml_print.hpp>
#include <rapidxml_utils.hpp>
#include <stdint.h>

typedef struct _FIELD {
	char id;                                             /* index of field in schema.xml, start from 0 */
	char* name;
	char* fieldtype;
	size_t length;
	bool indexed;
	bool stored;
	bool filter;
} Field;

class Schema {

private:
	std::string s_path;
	std::map<std::string, Field> schema;                  /* map of field name and field data */
	char* uniquekey;                                      /* name of unique key, "id" in this case */
	std::vector<unsigned char> allFid;                    /* all field id, increase from 0 */
	std::vector<std::string> allFname;                    /* all field name */

public:
	Schema(std::string s_path);
	~Schema();
	bool init();
	Field getField(std::string name);
	char getFid(std::string name);
	std::vector<unsigned char> getAllFid();
	std::vector<std::string> getAllFname();
	size_t getLength(std::string name);
	bool getStored(std::string name);
	bool getIndexed(std::string name);
	bool getFilter(std::string name);
	char* getUniquekey();
	char* getFieldtype(std::string name);
	char* Tobin(const char* qkey, const char* qval, int fieldType, size_t vlen);
	std::string Tostr(std::string name, std::vector<unsigned char> value);
	
};


#endif
