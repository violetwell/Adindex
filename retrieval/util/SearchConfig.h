#ifndef SEARCH_CONFIG_H
#define SEARCH_CONFIG_H

#include <map>
#include <rapidxml.hpp>
#include <string>
#include <vector>
#include <cstring>


typedef struct _SEARCHRULE {
	bool DNF;                                                 /* init FALSE */
	bool TEXT;                                                /* init TRUE */
	std::vector<std::string> matchobjs;                       /* array of match in search_conf.xml which is separated by "," */
} SearchRule;

class SearchConfig {

private:
	std::string c_path;	
	std::map<int, SearchRule> search_config;                  /* key: field id in search_conf.xml */
	bool extend;
	size_t adpu;
	size_t adlimit;

public:
	SearchConfig(std::string c_path);
	~SearchConfig();
	bool init();
	bool getExtend();
	size_t getAdpu();
	size_t getAdlimit();
	SearchRule getSearchRule(int tagfield);

};

#endif
