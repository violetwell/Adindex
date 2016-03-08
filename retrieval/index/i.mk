ILIBL64PATH = /usr/local/lib64
ITHRIFTPATH = /usr/local/include/thrift
IBOOSTPATH = /usr/include/boost
ILOG4CXX = /usr/local/log4cxx/include
 
ISAMPLE_ROOT = $(PWD)/..

ITINYXMLPATH = $(ISAMPLE_ROOT)/index-lib/tinyxml
ILEVELDBPATH = $(ISAMPLE_ROOT)/index-lib/leveldb/include
IILTPATH = $(ISAMPLE_ROOT)/thrift/indexlogThrift
IRAPIDXML = $(ISAMPLE_ROOT)/util/rapidxml

IUTILPATH = $(ISAMPLE_ROOT)/util
IINDEXPATH = $(ISAMPLE_ROOT)/index
ISEARCHPATH = $(ISAMPLE_ROOT)/search


IINCLUDET = -I$(ILIBL64PATH) \
	-I$(ITINYXMLPATH) \
	-I$(ILEVELDBPATH) \
	-I$(IUTILPATH) \
	-I$(IINDEXPATH) \
	-I$(ISEARCHPATH) \
	-I$(IILTPATH) \
	-I$(ITHRIFTPATH) \
	-I$(IRAPIDXML) \
	-I$(ILOG4CXX)

ICC=g++
ICC_FLAG=-O1 -DHAVE_NETINET_IN_H
ICC_DEBUG=-g

ILIB_DIR = -lthrift -lpthread -lm -lcrypto -lstdc++

ISRC= ${wildcard *.cpp}
IOBJ= ${patsubst %.cpp, %.o, $(ISRC)}

Indexall:$(IOBJ)

$(IOBJ):%.o : %.cpp
	$(ICC) $(ICC_FLAG) -c $^ -o $@ $(IINCLUDET)

