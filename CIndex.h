/*
	File: CIndex.h
	Author: Stepan Prikazchikov
	About: Realization of column index
*/
#pragma once

#include <string.h>
#include <string>
#include <assert.h>
#include "CSegment.h"

#define DIR "./db"
#define EXT ".csv"
#define NUM_SEGMENTS 2000


using namespace std

class CIndex
{
public:
									CIndex					(const string name, const bool, const CIndex*);
	virtual							~CIndex					();
public:
	void							Load					();
	
	bool							IsTransitive			() 					{ return m_bTransitive };
	string 							GetName					()					{ return m_name; };
	
public:
	CSegment& 						operator[] 				(size_t i) 			{ return *(m_data[i]); };
	size_t							GetSegmentsCount		()					{ return m_data.size(); };
	void							AddSegment				(CSegment& segment)	{ m_data.push_back(segment); };
private:
	vector<CSegment*>				m_data(NUM_SEGMENTS);
	
	string 							m_name;
	size_t							m_rowsCount;
	
	bool							m_bTransitive;
	CIndex*							m_indexTransitiveOn;
};