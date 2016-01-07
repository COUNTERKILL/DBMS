/*
	File: CIndex.h
	Author: Stepan Prikazchikov
	About: Realization of column index
*/
#pragma once

#include <string.h>
#include <string>

using namespace std

class CIndex
{
public:
									CIndex					();
									~CIndex					();
public:
	void							Load					(string name);
	
	bool							IsTransitive			() 					{ return m_bTransitive };
	
public:
	CSegment& 						operator[] 				(size_t i) 			{ return m_data[i]; };
	size_t							GetSegmentsCount		()					{ return m_data.size(); };
	void							AddSegment				(CSegment& segment)	{ m_data.push_back(segment); };
private:
	vector<CSegment>				m_data;
	
	string 							m_name;
	size_t							m_rowsCount;
	
	bool							m_bTransitive;
	CIndex*							m_indexTransitiveOn;
};