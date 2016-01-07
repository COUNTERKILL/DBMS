/*
	File: CSegment.h
	Author: Stepan Prikazchikov
	About: Realization of segment of column index
*/
#pragma once

#include <vector>
#include <assert.h>

#define ELEMENT_TYPE (int)

using namespace std;

class CSegment
{
public:
									CSegment			();
									CSegment			(const CSegment& segment);
	virtual							~CSegment			();

public:
	size_t 							GetKey				(const size_t index) const 			{ return m_keys[index];   };
	ELEMENT_TYPE					GetValue			(const size_t index) const 			{ return m_values[index]; };
	size_t							GetSize				() const							{ return m_keys.size(); };
	
public:
	void							AddElement			(const size_t key, const ELEMENT_TYPE value);

public:
	void							SetMinMaxValue		(const ELEMENT_TYPE min, const ELEMENT_TYPE max);
public:
	CSegment& 						operator= 			(const CSegment &);
	
private:
	vector<size_t> 					m_keys;
	vector<ELEMENT_TYPE> 			m_values;
	ELEMENT_TYPE					m_min;
	ELEMENT_TYPE					m_max;
};