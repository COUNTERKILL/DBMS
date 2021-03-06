#include "CSegment.h"

CSegment::CSegment()
{
	
};

CSegment::CSegment(const CSegment& segment)
{
	m_keys 		= segment.m_keys;
	m_values 	= segment.m_values;
	
	m_min		= segment.m_min;
	m_max		= segment.m_max;
};

CSegment::~CSegment()
{

};

void CSegment::AddElement(const size_t key, const ELEMENT_TYPE value) 	
{
	m_keys.push_back(key);
	m_values.push_back(value);  
};

void CSegment::SetMinMaxValue(const ELEMENT_TYPE min, const ELEMENT_TYPE max)
{
	assert(min <= max); 
	m_min = min;
	m_max = max; 
};

CSegment& CSegment::operator= 			(const CSegment& segment)
{
	m_keys 		= segment.m_keys;
	m_values 	= segment.m_values;
	
	m_min		= segment.m_min;
	m_max		= segment.m_max;
	return *this;
};
