SLIBL64PATH = /usr/local/lib64
STHRIFTPATH = /usr/local/include/thrift
SBOOSTPATH = /usr/include/boost
ILOG4CXX = /usr/local/log4cxx/include
 
SSAMPLE_ROOT = $(PWD)/..

STINYXMLPATH = $(SSAMPLE_ROOT)/index-lib/tinyxml
SLEVELDBPATH = $(SSAMPLE_ROOT)/index-lib/leveldb/include
SILTPATH = $(SSAMPLE_ROOT)/thrift/indexlogThrift
SITPATH = $(SSAMPLE_ROOT)/thrift/indexThrift

SUTILPATH = $(SSAMPLE_ROOT)/util
SINDEXPATH = $(SSAMPLE_ROOT)/index
SSEARCHPATH = $(SSAMPLE_ROOT)/search
SRAPIDXML = $(SSAMPLE_ROOT)/util/rapidxml

SINCLUDET = -I$(SLIBL64PATH) \
	-I$(STINYXMLPATH) \
	-I$(SLEVELDBPATH) \
	-I$(SUTILPATH) \
	-I$(SINDEXPATH) \
	-I$(SSEARCHPATH) \
	-I$(SRAPIDXML) \
	-I$(SILTPATH) \
	-I$(SITPATH) \
	-I$(STHRIFTPATH) \
    -I$(ILOG4CXX)


									     

SCC=g++
SCC_FLAG=-O1 -DHAVE_NETINET_IN_H
SCC_DEBUG=-g

SLIB_DIR = -lthrift -lpthread -lm -lcrypto -lstdc++

SSRC= ${wildcard *.cpp}
SOBJ= ${patsubst %.cpp, %.o, $(SSRC)}

Searchall:$(SOBJ)

$(SOBJ):%.o : %.cpp
	$(SCC) $(SCC_FLAG) -c $^ -o $@ $(SINCLUDET)

