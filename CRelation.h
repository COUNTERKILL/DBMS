/*
	File: CRelation.h
	Author: Stepan Prikazchikov
	About: Realization of relation of column indeces
*/
#pragma once

#include <map>
#include "CIndex.h"

using namespace std;

class CRelation
{
public:
									CRelation			();
	virtual							~CRelation			();
public:
	void							Initialize			();
public:
	CIndex&							GetIndex			(string name) { return m_data[name] };
private:
	map<string, CIndex*> 			m_data; // array of rows
		
	size_t							m_rowSize;
	size_t							m_rowsCount;
	
	
};