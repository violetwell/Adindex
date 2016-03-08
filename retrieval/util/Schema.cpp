#include "Schema.h"
#include <string>
#include <string.h>
#include <vector>
#include <rapidxml.hpp>
#include <rapidxml_print.hpp>
#include <rapidxml_utils.hpp>

using namespace rapidxml;

Schema::Schema(std::string schema_path) {
	s_path = schema_path;
}

Schema::~Schema() {
    std::map<std::string, Field>::iterator it;
    for (it = schema.begin(); it != schema.end(); it++ ) {
        if (it->second.fieldtype != NULL) {
            free(it->second.fieldtype);
        }
        if (it->second.name != NULL) {
            free(it->second.name);
        }
    }
    schema.clear();
    if (NULL != uniquekey) {
        free(uniquekey);
    }
}

bool Schema::init() {                                                                                  /* load schema.xml */
	try {
		file<> fdoc(s_path.c_str());

		xml_document<> doc;
		doc.parse<0>(fdoc.data());

		xml_node<>* root = doc.first_node();
		//printf("Load schema: %s\n", root->name());

		xml_node<char>* fields;
		xml_node<char>* uniquenode;
		for (xml_node<char>* node = root->first_node(); node != NULL; node=node->next_sibling()) {
			//std::cout << node->name() << std::endl;
			if (0 == strcmp("fields", node->name())) {
				fields = node;
			} else if (0 == strcmp("uniqueKey", node->name())) {
				uniquenode = node;
			}
		}
		//std::cout << fields->name() << std::endl;


		char index = 0x00;
		std::string fname;
		size_t tmp_length;
		bool tmp_flag;
		for (xml_node<char>* node = fields->first_node(); node != NULL; node=node->next_sibling()) {
			//char* name = node->name();
			Field field;

			//init field obj
			field.id = index;
			allFid.push_back(index);
			xml_attribute<char>* attr = node->first_attribute("name");
			if (attr != NULL) {
				//printf("name: %s\n", attr->value());
				fname = attr->value();
				field.name = (char*) malloc (sizeof(char) * strlen(attr->value()) + 1);
				if (NULL != field.name) {
					strcpy(field.name, attr->value());
				}
				allFname.push_back(fname);
			}
			attr = node->first_attribute("type");
			if (attr != NULL) {
				//printf("type: %s\n", attr->value());
				field.fieldtype = (char*) malloc (sizeof(char) * strlen(attr->value()) + 1);
				if (NULL != field.fieldtype) {
					strcpy(field.fieldtype, attr->value());
				}
			}
			attr = node->first_attribute("length");
			if (attr != NULL) {
				tmp_length = (size_t)atoi(attr->value());
				field.length = tmp_length;
			}
			attr = node->first_attribute("indexed");
			if (attr != NULL) {
				if(0 == strcmp("true", attr->value())) {
					tmp_flag = true;
				} else {
					tmp_flag = false;
				}
				field.indexed = tmp_flag;
			}
			attr = node->first_attribute("stored");
			if (attr != NULL) {
				if (0 == strcmp("true", attr->value())) {
					tmp_flag = true;
				} else {
					tmp_flag = false;
				}	
				field.stored = tmp_flag;
			}
			attr = node->first_attribute("filter");
			if (attr != NULL) {
				if (0 == strcmp("true", attr->value())) {
					tmp_flag = true;
				} else {
					tmp_flag = false;
				}
				field.filter = tmp_flag;
			}

			schema[fname] = field;
			index++;
		}

		xml_attribute<char>* uniqueattr = uniquenode->first_attribute("key");
		size_t uklen = uniqueattr->value_size();
		uniquekey = (char*)malloc(sizeof(char) * (uklen + 1));
        if (NULL != uniquekey) {
            memset(uniquekey, 0, (uklen+1));
        }
		if (uniqueattr != NULL && uniquekey != NULL) {
			strcpy(uniquekey,uniqueattr->value());
		}

		return true;
	} catch(...) {
		//printf("parse file error\n");
		return false;
	}
}


Field Schema::getField(std::string name) {
	//printf("getField\n");
	return this->schema[name];
}

char Schema::getFid(std::string name) {
	//printf("getFid %x\n", this->schema[name].id);
	return this->schema[name].id;
}

std::vector<unsigned char> Schema::getAllFid() {
	//printf("get AllFid len: %d\n", allFid.size());
	return this->allFid;
}

std::vector<std::string> Schema::getAllFname() {
	return this->allFname;
}

size_t Schema::getLength(std::string name) {
	//printf("getLength %d\n", this->schema[name].length);
	return this->schema[name].length;
}

bool Schema::getStored(std::string name) {
	return this->schema[name].stored;
}

bool Schema::getIndexed(std::string name) {
	return this->schema[name].indexed;
}

bool Schema::getFilter(std::string name) {
	return this->schema[name].filter;
}

char* Schema::getUniquekey() {
	//printf("getUniqueKey %s\n", this->uniquekey);
	return this->uniquekey;
}

char* Schema::getFieldtype(std::string name) {
	//printf("getFieldtype %s\n", this->schema[name].fieldtype);
	return this->schema[name].fieldtype;
}

char* Schema::Tobin(const char* qkey, const char* qval, int fieldType, size_t vlen) {             /* turn number-in-str to binary */
	if (-1 == fieldType) {
		return NULL;
	} else if (0 == fieldType) {                                                  /* bin */
		uint64_t val64;
		val64 = atoll(qval);                                                      /* turn to int */
		char* bin = (char*)malloc(sizeof(char) * vlen);
		if (!bin) {
			//printf("malloc memory failed for %s Tobin in period\n", qkey);
			;
		}
		memset(bin, 0, vlen);
		memcpy(bin, &val64, vlen);                                                 /* copy binary expression */
		//bin = (char*)&val64;
		return bin;
	} else if (1 == fieldType) {                                                   /* int, same as bin */
		int val32;
		val32 = atoi(qval);
		char* bin = (char*)malloc(sizeof(char) * vlen);
		if (!bin) {
			//printf("malloc memory failed for %s Tobin in int\n", qkey);
			;
		}
		memset(bin, 0, vlen);
		memcpy(bin, &val32, vlen);
		//bin = (char*)&val32;
		return bin;
	} else {                                                                      /* other, directly copy */
		char* bin = (char*)malloc(sizeof(char) * (vlen));
		if (!bin) {
			//printf("malloc memory failed for %s Tobin in string\n", qkey);
			;
		}
		memset(bin, 0, vlen);
		memcpy(bin, qval, vlen);
		return bin;
	}
}

std::string Schema::Tostr(std::string name, std::vector<unsigned char>value) {
	std::string str = "";
	size_t vlen = value.size();

	if (vlen == 0) {
		return str;
	}
	if (NULL == schema[name].fieldtype) {
		return str;
	}

	unsigned char* buf = (unsigned char*)malloc(sizeof(unsigned char) * (vlen));
	memset(buf, 0, (vlen));

	for (int i(0); i < value.size(); i++) {
		buf[i] = value[i];
	}
	if (0 == strcmp("int", schema[name].fieldtype)) {
		char st[4];
		int val = *(int*)buf;
		sprintf(st, "%d", val);
		str = st;
	} else if (0 == strcmp("str", schema[name].fieldtype)) {
		//std::string rr((char*)buf, (vlen+1));
		//str = rr;
		str.assign((char*)buf, (vlen));
	}
	if (NULL != buf) {
		free(buf);
	}
	return str;
}
