#include <iostream>
#include <cstdlib>
#include <string>
#include <stdlib.h>
#include <stdio.h>

#include "loadconf.h"

using namespace std;

LoadConf::LoadConf() {
}

LoadConf::~LoadConf() {
}

void LoadConf::trim(string& str) {
	str.erase(str.find_last_not_of(' ') + 1, string::npos);
	str.erase(0, str.find_first_not_of(' '));
}

int LoadConf::load_conf(string filename) {
	int pos;
	string key;
	string value;
	if (filename.length() == 0) {
		return -1;
	}
	i_file.open(filename.c_str());
	if (i_file.is_open()) {
		while (!i_file.eof()) {
			getline(i_file, text);
			pos = text.find_first_of(':');
			if (pos == (int)string::npos) {
				continue;
			}
			key = text.substr(0, pos);
			value = text.substr(pos + 1);
			trim(key);
			if (key[0] == '#') {
				continue;
			}
			trim(value);
			idx_conf.insert(map<string, string>::value_type(key, value));
		}
	} else {
		return -1;
	}
	i_file.close();
	return 1;
}	

char* LoadConf::get_str(string key) {
	iter = idx_conf.find(key);
	if (iter == idx_conf.end()) {
		return NULL;
	}
	return (char*)(iter->second).c_str();
}

int LoadConf::get_int(string key) {
	iter = idx_conf.find(key);
	if (iter == idx_conf.end()) {
		return -1;
	}
	return std::atoi((iter->second).c_str());
}
