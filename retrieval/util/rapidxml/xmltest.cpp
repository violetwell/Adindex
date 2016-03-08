#include <iostream>
#include <rapidxml.hpp>
#include <rapidxml_print.hpp>
#include <rapidxml_utils.hpp>

using namespace rapidxml;

int main(int argc, char** argv)
{

	file<> fdoc("schema.xml");
	std::cout << fdoc.data() << std::endl;
	xml_document<> doc;
	doc.parse<0>(fdoc.data());

	std::cout << doc.name() << std::endl;

	xml_node<>* root = doc.first_node();
	std::cout<< root->name() << std::endl;
	
	xml_node<>* node1 = root->first_node();
	std::cout<< node1->name() << std::endl;

	//xml_node<>* node11 = node1->first_node();
	//std::cout<< node11->name() << std::endl;
	//std::cout<< node11->value() << std::endl;
	
	for(xml_node<char>* node = node1->first_node(); node != NULL; node = node->next_sibling())
	{
		std::cout << node->name() << std::endl;
		std::cout << node->value() << std::endl;
		for(xml_attribute<char>* attr = node->first_attribute("name"); attr != NULL; attr = attr->next_attribute())
		{
			std::cout << attr->value() << std::endl;
		}
	}
	
	//xml_node<>* size = root->first_node("size");
	//size->append_node(doc.allocate_node(node_element,"w","0"));
	//size->append_node(doc.allocate_node(node_element,"h","0"));

	//std::string text;
	//rapidxml::print(std::back_inserter(text),doc,0);
	
	//std::cout<<text<<std::endl;
	//std::ofstream out("schema.xml");
	
	//out << doc;
	//system("PAUSE");
	return 0;

}
