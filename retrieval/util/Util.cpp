#include "Util.h"
#include <stdint.h>

 int Util::GetVint(unsigned char *Data){
  int b = (int)(*Data);
  int Value = b & 0x007f;
  for(int shift = 7; (b & 0x0080) != 0; shift += 7) {
    b = (int)*(++Data);
    Value |= (b & 0x007f) << shift;
  }
  return Value;
 }

/*int Util::GetVint(unsigned char *Data){
  int64_t *LDataPtr = (int64_t *)Data;
  int64_t b = *LDataPtr;
  int Value = b & 0x7f;
  for(int shift = 7; (b & 0x80) != 0; shift += 7) {
    b = (int)*(++Data);
    Value |= (b & 0x7f) << shift;
  }
  return Value;
  }*/

 int Util::GetVintAndSize(unsigned char *Data, int *size){
  *size = 0;
  int b = (int)(*Data);
  int Value = b & 0x7f;
  for(int shift = 7; (b & 0x80) != 0; shift += 7) {
    (*size)++;
    b = (int)*(++Data);
    Value |= (b & 0x7f) << shift;
  }
  (*size)++;
  return Value;
}


 int Util::GetVintSize(unsigned char *Data){
  int size = 0;
  while((*Data & 0x80) != 0){ //There are more bytes in this Vlong value
    size++;
    Data++;
  }
  size++;//Count for the final byte
  return size;
}

 unsigned long Util::FileSize(const char *FileName) {
  struct stat FileInfo;
  if(stat(FileName, &FileInfo)){
    return 0;
  }else{
    return (unsigned long) FileInfo.st_size;
  }
}  


 int Util::GetVlongSize(unsigned char *Data){
  int size = 0;
  while((*Data & 0x80) != 0){ //There are more bytes in this Vlong value
    size++;
    Data++;
  }
  size++;//Count for the final byte
  return size;
}

 long Util::GetVlong(unsigned char *Data){
  long b = (long)(*Data);
  long Value = b & 0x7f;
  for(int shift = 7; (b & 0x80) != 0; shift += 7) {
    b = (long)*(++Data);
    Value |= (b & 0x7f) << shift;
  }
  return Value;
}




