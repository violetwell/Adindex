#include "DnfScore.h"

DnfScore::DnfScore(DnfDocIterator* dscore, NoDnfDocIterator* ndscore, IndexHandler* _handler) : DocIterator(_handler) {
	_dScore = dscore;
	_nDScore = ndscore;
	_dnf_nextstat = 0;
	_dnf_skipstat = 0;
}

DnfScore::~DnfScore() {

}

float DnfScore::score() {
	return 0.0;
}

int DnfScore::docId() {
	return docid;
}

bool DnfScore::next() {
	if (0 == _dnf_nextstat) {
		bool dstep = _dScore->next();
		bool ndstep = _nDScore->next();

		if (dstep && ndstep) {
			if (_dScore->docId() > _nDScore->docId()) {
				docid = _nDScore->docId();
				_dnf_nextstat = 1;
			} else if (_dScore->docId() == _nDScore->docId()) {
				docid = _nDScore->docId();
				_dnf_nextstat = 0;
			} else if (_dScore->docId() < _nDScore->docId()) {
				docid = _dScore->docId();
				_dnf_nextstat = 2;
			}
			return true;
		} else if (!dstep && ndstep) {
			docid = _nDScore->docId();
			_dnf_nextstat = 3;
			return true;
		} else if (!ndstep && dstep) {
			docid = _dScore->docId();
			_dnf_nextstat = 4;
			return true;
		} else {
			return false;
		}
	} else if (1 == _dnf_nextstat) {
		bool ndstep = _nDScore->next();

		if (ndstep) {
			if (_dScore->docId() > _nDScore->docId()) {
				docid = _nDScore->docId();
				_dnf_nextstat = 1;
			} else if (_dScore->docId() == _nDScore->docId()) {
				docid = _nDScore->docId();
				_dnf_nextstat = 0;
			} else if (_dScore->docId() < _nDScore->docId()) {
				docid = _dScore->docId();
				_dnf_nextstat = 2;
			}
			return true;
		} else {
			docid = _dScore->docId();
			_dnf_nextstat = 4;
			return true;
		}
	} else if (2 == _dnf_nextstat) {
		bool dstep = _dScore->next();
		if (dstep) {
			if (_dScore->docId() > _nDScore->docId()) {
				docid = _nDScore->docId();
				_dnf_nextstat = 1;
			} else if (_dScore->docId() == _nDScore->docId()) {
				docid = _nDScore->docId();
				_dnf_nextstat = 0;
			} else if (_dScore->docId() < _nDScore->docId()) {
				docid = _dScore->docId();
				_dnf_nextstat = 2;
			}
			return true;

		} else {
			docid = _nDScore->docId();
			_dnf_nextstat = 3;
			return true;
		}
	} else if (3 == _dnf_nextstat) {
		if (_nDScore->next()) {
			docid = _nDScore->docId();
			return true;
		} else {
			return false;
		}
	} else {
		if (_dScore->next()) {
			docid = _dScore->docId();
			return true;
		} else {
			return false;
		}
	}
}

bool DnfScore::skipTo(int target) {
	if (0 == _dnf_skipstat) {
		bool dstep = _dScore->skipTo(target);
		bool ndstep = _nDScore->skipTo(target);

		if (dstep && ndstep) {
			if (_dScore->docId() > _nDScore->docId()) {
				docid = _nDScore->docId();
				_dnf_skipstat = 1;
			} else if (_dScore->docId() == _nDScore->docId()) {
				docid = _nDScore->docId();
				_dnf_skipstat = 0;
			} else {
				docid = _dScore->docId();
				_dnf_skipstat = 2;	
			}
		} else if (!dstep && ndstep) {
			docid = _nDScore->docId();
			_dnf_skipstat = 3;
			return true;
		} else if (!ndstep && dstep) {
			docid = _dScore->docId();
			_dnf_skipstat = 4;
			return true;
		} else {
			return false;
		}
		return true;
	} else if (1 == _dnf_skipstat) {
		bool ndstep = _nDScore->skipTo(target);

		if (ndstep) {
			if (_dScore->docId() > _nDScore->docId()) {
				docid = _nDScore->docId();
				_dnf_skipstat = 1; 
			} else if (_dScore->docId() == _nDScore->docId()) {
				docid = _nDScore->docId();
				_dnf_skipstat = 0;
			} else {
				docid = _dScore->docId();
				_dnf_skipstat = 2;
			}
			return true;

		} else {
			docid = _dScore->docId();
			_dnf_skipstat = 4;
			return true;
		}
	} else if (2 == _dnf_skipstat) {
		bool dstep = _dScore->skipTo(target);

		if (dstep) {	
			if (_dScore->docId() > _nDScore->docId()) {
				docid = _nDScore->docId();
				_dnf_nextstat = 1;
			} else if (_dScore->docId() == _nDScore->docId()) {
				docid = _nDScore->docId();
				_dnf_nextstat = 0;
			} else if (_dScore->docId() < _nDScore->docId()) {
				docid = _dScore->docId();
				_dnf_nextstat = 2;
			}
			return true;
		} else {
			docid = _nDScore->docId();
			_dnf_skipstat = 3;
			return true;
		}
	} else if (3 == _dnf_skipstat) {
		if (_nDScore->skipTo(target)) {
			docid = _nDScore->docId();
			return true;
		} else {
			return false;
		}
	} else {
		if (_dScore->skipTo(target)) {
			docid = _dScore->docId();
			return true;
		} else {
			return false;
		}
	}
}
