#include "SearchConfig.h"
#include <string>
#include <rapidxml.hpp>
#include <rapidxml_print.hpp>
#include <rapidxml_utils.hpp>

using namespace rapidxml;

SearchConfig::SearchConfig(std::string config_path) {
	c_path = config_path;
	extend = false;
	adpu = 0;
	adlimit = 0;
}

SearchConfig::~SearchConfig() {

}

bool SearchConfig::init() {                                        /* load search_conf.xml */
	try {
		file<> fdoc(c_path.c_str());
	
		xml_document<> doc;
		doc.parse<0>(fdoc.data());
	
		xml_node<char>* root = doc.first_node();
		//printf("Load SearchConfig: %s\n", root->name());

		xml_node<char>* DNFroot;
		xml_node<char>* Similarity;
		xml_node<char>* AD;
		for (xml_node<char>* node = root->first_node(); node != NULL; node=node->next_sibling()) {
			if (0 == strcmp("DNF", node->name())) {
				DNFroot = node;
				xml_attribute<char>* exattr = node->first_attribute("extend");
				if (exattr) {
					if (0 == strcmp("true",exattr->value())) {
						extend = true;
					} else {
						extend = false;
					}
				}
			} else if (0 == strcmp("Similarity", node->name())) {
				Similarity = node;
			} else if (0 == strcmp("AD", node->name())) {
				AD = node;
				xml_attribute<char>* ad_attr = node->first_attribute("adpu");
				if (ad_attr) {
					adpu = atoi(ad_attr->value());
				}

				ad_attr = node->first_attribute("adlimit");
				if (ad_attr) {
					adlimit = atoi(ad_attr->value());
				}
			}
		}
	
		std::string sp_char = ",";
		for (xml_node<char>* node = Similarity->first_node(); node != NULL; node = node->next_sibling()) {
			std::string Mids;
			std::string Tid;
			int index(0);
			int lastposition(0);
			std::vector<std::string> mid;
			xml_attribute<char>* attr = node->first_attribute("id");
			Tid = attr->value();
			int Tid_i(-1);
			Tid_i = atoi(Tid.c_str());
			if (Tid_i >= 0) {
		
				attr = attr->next_attribute();
				if (attr != NULL &&  (0 == strcmp("match", attr->name()))) {
					Mids = attr->value();
					while (-1 != (index = Mids.find(sp_char, lastposition))) {
						mid.push_back(Mids.substr(lastposition, index-lastposition));
						lastposition = index + sp_char.size();
					}
				}
		
				if ( search_config.find(Tid_i) != search_config.end()) {
					search_config[Tid_i].TEXT = true;
					search_config[Tid_i].matchobjs = mid;
				} else {
					SearchRule su;
					su.TEXT = true;
					su.DNF = false;
					su.matchobjs = mid;
					search_config[Tid_i] = su;
				}
			} else {
				;
			}
		
		
		}

	
		return true;
	} catch(...) {
		return false;
	}

}

SearchRule SearchConfig::getSearchRule(int tagfield) {
	if (search_config.find(tagfield) != search_config.end()) {
		return this->search_config[tagfield];
	} else {
		std::vector<std::string> mos;
		SearchRule conf = {false, false, mos};
		return conf; 
	}
	return this->search_config[tagfield];
}

bool SearchConfig::getExtend() {
	return this->extend;
}

size_t SearchConfig::getAdpu() {
	return this->adpu;
}

size_t SearchConfig::getAdlimit() {
	return this->adlimit;
}
