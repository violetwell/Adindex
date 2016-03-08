#include "SearchConfig.h"
#include <string>
#include <iostream>

int main()
{
	std::string c_path;
	c_path = "/opt/wk/retrieval/util/search_conf.xml";
	SearchConfig c(c_path);
	
	c.init();
	SearchRule s;
	std::string tag = "tags";
	s = c.getSearchRule(tag.c_str());
	bool f;
	f = c.getExtend();
	std::cout << f << std::endl;
	std::cout << s.DNF << s.TEXT << std::endl;
	for(int i(0); i < s.matchobjs.size(); i++)
	{
		std::cout << s.matchobjs[i] << std::endl;
	}

}
