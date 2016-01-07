#include <map>
#include "CIndex.h"

using namespace std;

class CRelation
{
public:
									CRelation			();
									~CRelation			();
public:
	void 							AddColumn			();
	void 							AddRow				();
	
private:
	map<string, CIndex> 			m_data; // array of rows
		
	size_t							m_rowSize;
	size_t							m_rowsCount;
	
	
};