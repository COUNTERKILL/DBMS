#include "CDBMS.h"
#include <iostream>
#include <cstdlib>

using namespace std;

int main(int argc, char** argv)
{
	if(argc != 2)
	{
		cout << "Query type doesn't matchig" << endl;
		return -1;
	};
	try
	{
		QueryType q = (QueryType)atoi(argv[1]);
		CDBMS *pDBMS = new CDBMS(&argc, &argv);
		pDBMS->Initialize();
		pDBMS->Query(q);
		pDBMS->Finalize();
	}
	catch(string err)
	{
		cout << err << endl;
	}
	catch(exception* err)
	{
		cout << err->what() << endl;
	}
	catch(...)
	{
		cout << "Other error" << endl;
	};
	
	return 0;
};