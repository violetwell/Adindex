#ifndef __LOAD_CONF_H_
#define __LOAD_CONF_H_

#include <string>
#include <iostream>
#include <fstream>
#include <map>
#include <vector>

class LoadConf {
	
public:
	LoadConf();
	~LoadConf();
	int load_conf(std::string fn);
	char* get_str(std::string str);
	int get_int(std::string str);
	
private:
	void trim(std::string& str);
	
private:
	std::map<std::string, std::string> idx_conf;
	std::map<std::string, std::string>::iterator iter;
	std::ifstream i_file;
	std::string text;
	
};

#endif
