#ifndef DOCINFO_H
#define DOCINFO_H
#include <iostream>
#define DOCIDSENTEL  (-1)   //A number ensured to be not conflict with all the doc IDs
using namespace std;

class DocInfo{
public:
  int DocID;
  float Weight;
 //DocInfo():DocID(0), Weight(0){};
  DocInfo():DocID(-1), Weight(0){};
  //  friend ostream &operator<<(ostream &stream, const DocInfo di);
  friend ostream &operator<<(ostream &out, const DocInfo& di){
    out << "DocID=" << di.DocID;
    out << "  Weight=" << di.Weight << endl;
  }

  bool operator<(const DocInfo &di) const{
    return (this->Weight < di.Weight);
  }

  bool operator<=(const DocInfo &di) const{
    return (this->Weight <= di.Weight);

  }

  bool operator>(const DocInfo &di) const{
    return (this->Weight > di.Weight);
  }

  bool operator>=(const DocInfo &di) const{
    return (this->Weight >= di.Weight);
  }

  bool operator==(const DocInfo &di) const{
    return (this->Weight == di.Weight);
  }

  DocInfo &operator=(const DocInfo &di){
    this->Weight = di.Weight;
    this->DocID = di.DocID;
    return *this;
  }
};


#endif
