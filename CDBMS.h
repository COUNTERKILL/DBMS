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
	void 				Query			(int query);
private:
	CRelation			m_relation;
};