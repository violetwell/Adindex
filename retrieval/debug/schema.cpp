#include "Schema.h"
#include <string>
#include <iostream>
#include <iomanip>

int main()
{
	std::string s_path;
	s_path = "/opt/wk/retrieval/util/schema.xml";
	Schema s(s_path);

	s.init();
	char* ukey;
	ukey =  s.getUniquekey();
	std::cout << ukey << std::endl;
	Field field;
	std::string name("id");
	field = s.getField(name);
	std::cout << std::hex(field.id) << " " << field.name  << " " << field.fieldtype << std::endl;
	return 0;

}
