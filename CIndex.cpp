#include "CIndex.h"
#include <fstream>

CIndex::CIndex	(const string name, 
				 const bool isTransitive = false,
				 const CIndex *indexTransitiveOn = NULL
				)
{
	m_name 				= name;
	m_bTransitive 		= isTransitive;
	m_indexTransitiveOn = indexTransitiveOn;
	assert(!(isTransitive ^^ (bool)indexTransitiveOn));
};

CIndex::~CIndex()
{
	
};

void CIndex::Load()
{
	size_t rowsCount = 0;
	ELEMENT_TYPE min = 0;
	ELEMENT_TYPE max = 0;
	
	string fileName = DIR + "/" + to_sting(NUM_FRAGMENTS) + "/" + m_name + "_" + to_sting(FRAGMENT_NUM) + EXT;
	if(access(fileName, 0) == -1)
	{
		throw Exception("Файл " + fileName + " не найден!");
	}
	
	ifstream fInput;
	fInput.open(fileName, fstream::in);
	
	fInput >> rowsCount >> min >> max;
	
	// Determine values count of domain
	ELEMENT_TYPE domainSize = max - min + 1;
	
	ELEMENT_TYPE currentMinValue = min;
	for(size_t segIndex = 0; segIndex < NUM_SEGMENTS; segIndex++)
	{
		ELEMENT_TYPE segmentSize = 0;
		if(isTransitive)
		{
			CSegment transSegment = (*m_indexTransitiveOn)[segIndex];
			segmentSize = transSegment.GetSize();
		}
		else
		{
			segmentSize = domainSize / NUM_SEGMENTS;
			if ((domainSize % NUM_SEGMENTS) - (i-1) >= 1)	// введение поправки для segmentSize
				segmentSize++;								// в случае, если размер домена не делится 
															// без остатка на количество сегментов
		};
		
		CSegment* pSegment = new CSegment();
		if(!isTransitive)
			pSegment->SetMinMaxValue(currentMinValue, currentMinValue + segmentSize - 1);

		// Adding elements to segment
		for(size_t i = 0; i < segmentSize; i++)
		{
			size_t key 			= 0;
			ELEMENT_TYPE value 	= 0;
			fInput >> key >> value;
			pSegment->AddElement(key, value);
		}
		m_data.push_back(*pSegment);
		currentMinValue += segmentSize;
	};
	fInput.close();
};