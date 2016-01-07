#include <vector>

#define ELEMENT_TYPE (int)

class CSegment
{
public:
									CSegment			();
									CSegment			(CSegment& segment);
									~CSegment			();

public:
	size_t 							GetKey				(size_t index) 						{ return m_keys[index];   };
	ELEMENT_TYPE					GetValue			(size_t index) 						{ return m_values[index]; };
	size_t							GetSize				()									{ return m_keys.size(); };
	
public:
	void							AddElement			(size_t key, ELEMENT_TYPE value) 	{ m_keys.push_back(key); m_values.push_back(value);  };

public:
	void							SetMinMaxValue		(ELEMENT_TYPE min, ELEMENT_TYPE max) { assert(min <= max); this.m_min = min; this.m_max = max; };
public:
	CSegment& 						operator= 			(const CSegment & );
	
private:
	vector<size_t> 					m_keys;
	vector<ELEMENT_TYPE> 			m_values;
	ELEMENT_TYPE					m_min;
	ELEMENT_TYPE					m_max;
};