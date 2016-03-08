#include "SearchQuery.h"


SearchQuery::SearchQuery(const QTerm& q) {
	_qterm = q;

	_dnfquery = NULL;
	_dnflen = 0;
	_dnfex = false;

	_simiquery = NULL;
	_similen = 0;
	_score2 = false;
	_score1 = false;
	_adsource = -1;

	_fsize = _qterm.filter.size();
	_ssize = _qterm.single_query.size();

	if ( _fsize > 0) {
		_fquery = (FilterInfo*)malloc(sizeof(FilterInfo));
		if (!_fquery) {
			;
		}
		memset(_fquery, 0, sizeof(FilterInfo));
	} else {
		_fquery = NULL;
	}

	if (_ssize > 0) {
		_singlequery = (QueryInfo*)malloc(sizeof(QueryInfo));
		if (!_singlequery) {
			;
		}
		memset(_singlequery, 0, sizeof(QueryInfo));
	} else {
		_singlequery = NULL;
	}

	scoreO = NULL;
	scoreT = NULL;
}

SearchQuery::~SearchQuery() {
	if (NULL != _dnfquery) {
		delete [] _dnfquery;
	}
	if (NULL != _simiquery) {
		delete [] _simiquery;
	}

	//free fquery(FilterInfo struct)
	if (_fsize > 0) {
		for (int i(0); i < _fsize; i++) {
			if (NULL != _fquery->contents[i])
				free(_fquery->contents[i]);
		}
		if (NULL != _fquery->contents)
			free(_fquery->contents);
		if (NULL != _fquery->filedIds)
			free(_fquery->filedIds);
		if (NULL != _fquery->contLen)
			free(_fquery->contLen);
		if (NULL != _fquery)
			free(_fquery);
	}

	//free _singlequery(QueryInfo struct)
	if (_ssize > 0) {
		for (int i(0); i < _ssize; i++) {
			if (NULL != _singlequery->contents[i]) {
				free(_singlequery->contents[i]);
			}
		}
		if (NULL != _singlequery->contents) 
			free(_singlequery->contents);
		if (NULL != _singlequery->fields)
			free(_singlequery->fields);
		if (NULL != _singlequery->fieldlen)
			free(_singlequery->fieldlen);
		if (NULL != _singlequery)
			free(_singlequery);
	}

	if (NULL != scoreO) {
		delete scoreO;
	}
	if (NULL != scoreT) {
		delete scoreT;
	}
}

bool SearchQuery::init(Schema* schema, SearchConfig* config)
{

	int _dnfsize = _qterm.dnf.size();
	if (_dnfsize > MAX_TAG_SIZE) {
		_dnfsize = MAX_TAG_SIZE;
	}
	int _simisize = _qterm.tags.size();
	if (_simisize > MAX_TAG_SIZE) {
		_simisize = MAX_TAG_SIZE;
	}
	std::vector<std::string> fields;

	char* fi = (char*)calloc(sizeof(int), sizeof(char));                          /* 4 char */
	if (NULL == fi) {
		return false;
	}

	bool _neednodnf = false;
	bool _has_id = false;
	int sqlen(0);
	if (_ssize > 0 && (NULL != _singlequery)) {                                   /* read single query into _singlequery */
		_singlequery->fields = (char*)malloc(sizeof(char) * _ssize);
		if (!_singlequery->fields) {
			return false;
		}
		memset(_singlequery->fields, 0, sizeof(char) * _ssize);

		_singlequery->contents = (char**)malloc(sizeof(char*) * _ssize);
		if (!_singlequery->contents) {
			return false;
		}
		memset(_singlequery->contents, 0, (sizeof(char*) * _ssize));

		_singlequery->fieldlen = (int*)malloc(sizeof(int) * _ssize);
		if (!_singlequery->fieldlen) {
			return false;
		}
		memset(_singlequery->fieldlen, 0, (sizeof(int) * _ssize) );

		std::string adsource = "adsource";
		std::string single_id = "id";
		std::string adsource_v = "0";
		std::string adsource_v2 = "2";
		for (int m(0); m < _ssize; m++) {
			std::string _sqkey = _qterm.single_query[m].qkey;
			std::string _sqval = _qterm.single_query[m].qval;

			if (adsource == _sqkey) {                                            /* read adsource to set _adsource, _neednodnf */
				_adsource = atoi(_sqval.c_str());
				if (adsource_v == _sqval) {
					_neednodnf = true;
				} else if (adsource_v2 == _sqval) {
					continue;
				}
            }
	
			if (single_id == _sqkey) {                                           /* set _has_id */
				_has_id = true;
			}

			char* fieldtype = schema->getFieldtype(_sqkey);
			bool indexed = schema->getIndexed(_sqkey);
			if ((NULL != fieldtype) && indexed) {                                /* add "indexed" queries */
				char sfield;
				sfield = schema->getFid(_sqkey);
				_singlequery->fields[sqlen] = sfield;                            /* fieldId */
				int fieldType(-1);
				size_t vlen(0);
				if (0 == strcmp("bin", fieldtype)) {
					fieldType = 0;
					vlen = sizeof(uint64_t);
				} else if( 0 == strcmp("int", fieldtype)) {
					fieldType = 1;
					vlen = sizeof(int);
				} else {
					fieldType = 2;
					vlen = _sqval.size();
				}
				_singlequery->contents[sqlen] = (char*)malloc(sizeof(char) * vlen);
				if (!_singlequery->contents[sqlen]) {
					return false;
				}
				memset(_singlequery->contents[sqlen], 0, (sizeof(char) * vlen) );

				char* tq = schema->Tobin(_sqkey.c_str(), _sqval.c_str(), fieldType, vlen);//, schema[_sqkey].fieldtype);     /* turn number-in-str to binary */
				if (tq != NULL) {
					memcpy(_singlequery->contents[sqlen], tq, vlen);              /* query value */
					_singlequery->fieldlen[sqlen] = vlen;                         /* length of query value */
					free(tq);
					sqlen++;
				}
			} else {
				_singlequery->contents[sqlen] = NULL;
				_singlequery->fieldlen[sqlen] = 0;
			}
		}
		_singlequery->fieldNum = sqlen;                                            /* count of single queries */

	}

	if (_dnfsize > 0) {                                                            /* read dnf query into _dnfquery */
		std::vector<DNF_Q> dnf_tags;
		std::set<int> dnf_fields;

		for (int i(0); i < _dnfsize; i++) {
			DNF_Q dnf_qe;
			int s;
			memset(dnf_qe.dnf_tag, 0, TAG_LEN);
			memcpy(dnf_qe.dnf_tag, &(_qterm.dnf[i]), TAG_LEN);                      /* dnf data(r_value+r_key) */

			memset(fi, 0, FIELD_LEN);                                               /* reverse last two bytes to get dnf key, ie:ap(adplaceid) */
			memcpy((fi + 1), (dnf_qe.dnf_tag + (TAG_LEN - 2)), 1);
			memcpy(fi, (dnf_qe.dnf_tag + (TAG_LEN - 1)), 1);

			s = *(int*)fi;
			dnf_qe.field = s;                                                       /* 4bytes, binary: dnfkey[0] dnfkey[1] 0 0 */

			dnf_tags.push_back(dnf_qe); 
			dnf_fields.insert(s);
		}

		int dnfc = 0;
		std::set<int>::iterator it_f;
		std::vector<DNF_Q>::iterator it_q;
		_dnfquery = new char[_dnfsize * DNF_TAG_LEN];
		memset(_dnfquery, 0, _dnfsize * DNF_TAG_LEN);
		char dnftail;
		for (it_f = dnf_fields.begin(); it_f != dnf_fields.end(); it_f++) {
			bool index = false;
			for (it_q = dnf_tags.begin(); it_q != dnf_tags.end(); it_q++) {
				if (it_q->field == (*it_f)) {
					if (index) {
						dnftail = SAME_FIELD;
					} else {
						dnftail = DIFF_FIELD;
					}
					size_t dlen = DNF_TAG_LEN - 1;
					for (int n(0); n < dlen; n++) {
						_dnfquery[n + (DNF_TAG_LEN*dnfc)] = it_q->dnf_tag[TAG_LEN - n - 1];            /* reverse binary data of dnf_tag, now: key + value */
					}
					_dnfquery[dlen + (DNF_TAG_LEN*dnfc)] = dnftail;
					//char tmp = _dnfquery[(DNF_TAG_LEN*dnfc)];
					//_dnfquery[(DNF_TAG_LEN*dnfc)] = _dnfquery[(DNF_TAG_LEN*dnfc) + 1];
					//_dnfquery[(DNF_TAG_LEN*dnfc) + 1] = tmp;
					dnfc++;
					index = true;
				}

			}
		}
	
		
		if ( !(dnfc > 0) && _neednodnf && config->getExtend() && !_has_id ) {           /* set _dnfex */
			_dnfex = true;
		}
		_dnflen = dnfc * DNF_TAG_LEN;
	} else if ( _neednodnf && config->getExtend() && !_has_id) {
		_dnfex = true;
	}

	if (NULL != fi) {
		free(fi);
	}
                                                                                         /* read tag query into _simiquery */
	int tagfield(0);                                                                     /* type of tag */
	if (_simisize > 0) {
		_simiquery = new char[_simisize * SIMI_TAG_LEN];
		memset(_simiquery, 0, _simisize * SIMI_TAG_LEN);
		int simic = 0;
		for (int i(0); i < _simisize; i++) {
			SearchRule confdnf;
			char bufdnf[TAG_LEN];
			memset(bufdnf, 0, TAG_LEN);
			memcpy(bufdnf, &(_qterm.tags[i]), TAG_LEN);

			memcpy((char*)&tagfield, (bufdnf + (TAG_LEN - 2)), 1);                       /* tag is i64, last 2 bytes are higher digits */
			memcpy(((char*)&tagfield + 1), (bufdnf + (TAG_LEN - 1)), 1);
			if (tagfield >= 0) {
				confdnf = config->getSearchRule(tagfield);                               /* get search rule of tag */

				if (confdnf.TEXT) {                                                      /* only add tag type found in search_conf */
					for (int y(0); y < confdnf.matchobjs.size(); y++) {
						_simiquery[(SIMI_TAG_LEN*simic)] = schema->getFid(confdnf.matchobjs[y].c_str());
						for (int x(1); x <= TAG_LEN; x++) {
							_simiquery[x + (simic*SIMI_TAG_LEN)] = bufdnf[TAG_LEN - x];   /* reverse tag */
						}
						
						char tmp;                                                         /* reverse last 2 bytes of r_tag */
						tmp = _simiquery[TAG_LEN + (simic*SIMI_TAG_LEN)];
						_simiquery[TAG_LEN + (simic*SIMI_TAG_LEN)] = _simiquery[TAG_LEN - 1 + (simic*SIMI_TAG_LEN)];
						_simiquery[TAG_LEN - 1 + (simic*SIMI_TAG_LEN)] = tmp;
					}
					simic++;
				}
			}
		}
		_similen = simic * SIMI_TAG_LEN;
	}

	int fqlen(0);                                                                      /* read filter query into _fquery */
	if (_fsize > 0 && (NULL != _fquery)) {
		_fquery->filedIds = (char*)malloc(sizeof(char) * _fsize);
		if (!_fquery->filedIds) {
			return false;
		}
		memset(_fquery->filedIds, 0, sizeof(char) * _fsize);

		_fquery->contents = (char**)malloc(sizeof(char*) * _fsize);
		if (!_fquery->filedIds || !_fquery->contents) {
			return false;
		}
		memset(_fquery->contents, 0, sizeof(char*) * _fsize);

        _fquery->contLen = (int*)malloc(sizeof(int) * _fsize);
        if (!_fquery->contLen) {
			return false;
        }
        memset(_fquery->contLen, 0, (sizeof(int) * _fsize) );

		for (int j(0); j < _fsize; j++) {
			std::string _fqkey = _qterm.filter[j].qkey;
			std::string _fqval = _qterm.filter[j].qval;

			char* fieldtype = schema->getFieldtype(_fqkey);
			if (NULL != fieldtype) {
				char ffield;
				ffield = schema->getFid(_fqkey);
				_fquery->filedIds[j] = ffield;                                          /* field Id */

				int fieldType(-1);
				size_t vlen(0);
				if (0 == strcmp("bin", fieldtype)) {
					fieldType = 0;
					vlen = sizeof(uint64_t);
				} else if( 0 == strcmp("int", fieldtype)) {
					fieldType = 1;
					vlen = sizeof(int);
				} else {
					fieldType = 2;
					vlen = _fqval.size();
				}
				_fquery->contents[j] = (char*)malloc(sizeof(char) * (vlen));
				if (!_fquery->contents[j]) {
					return false;
				}
				memset(_fquery->contents[j], 0, sizeof(char) * (vlen));

				char* fvalue = schema->Tobin(_fqkey.c_str(), _fqval.c_str(), fieldType, vlen);//, schema[_fqkey].fieldtype);     /* turn number-in-str to binary */
				if (fvalue != NULL) {
					memcpy(_fquery->contents[j], fvalue, vlen);                         /* data of filter query */
					_fquery->contLen[j] = vlen;                                         /* length of data */
					free(fvalue);
					fqlen++;
				}
			} else {
				_fquery->contents[j] = NULL;
				_fquery->contLen[j] = 0;
			}
		}
		_fquery->filedNum = fqlen;
	
	}

	

	if(_similen > 0) {
		_score2 = true;
	} else {
		_score2 = false;
	}

	if( _dnflen > 0 || sqlen > 0 ) {
		_score1 = true;
	} else {
		_score1 = false;
	}

	return true;
}

bool SearchQuery::getScoreType1() {
	return this->_score1;
}

bool SearchQuery::getScoreType2() {
	return this->_score2;
}

int SearchQuery::getAdSource() {
	return this->_adsource;
}

int SearchQuery::getDnfLen() {
	return this->_dnflen;
}

DocIterator* SearchQuery::score(IndexHandler* handler) {
	scoreO = new QueryScore1(_dnfquery, _dnflen, _dnfex, _fquery, _singlequery, handler);
	return (DocIterator*) scoreO;
}

DocIterator* SearchQuery::score(MinHeap* hq, IndexHandler* handler) {
	scoreT = new QueryScore2(hq, NULL, _simiquery, _similen, handler, _fquery);
	return (DocIterator*) scoreT;
}

