#include "CRelation.h"

class CDBMS
{
public:
						CDBMS			();
						~CDBMS			();
public:
	void 				Initialize		();
	void 				Finalize		();
public:
	CQueryResult 		Query			(int query);
}