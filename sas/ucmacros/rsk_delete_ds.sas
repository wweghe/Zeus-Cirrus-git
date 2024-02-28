/*
 Copyright (C) 2022-2024 SAS Institute Inc. Cary, NC, USA
*/

/**
   \file
\anchor rsk_delete_ds

   \brief   Delete dataset

   \param [in] DS:	The name of a dataset (table, view) to be deleted together with optional library in standard dot notation.

   \details
   If the given dataset exist performs drop table/drop view.


   <b>Example:</b>

   \code
   		%rsk_delete_ds(ds)
		%rsk_delete_ds(library.ds2)
   \endcode

   \ingroup mrm

   \author  SAS Institute Inc.
   \date    2022
*/

%macro rsk_delete_ds(DS);

	%if (%sysfunc(exist(&DS))) %then %do;
		proc sql;
			drop table &DS;
		quit;
	%end;
	%if (%sysfunc(exist(&DS, VIEW))) %then %do;
	    proc sql;
			drop view &DS;
		quit;
	%end;

%mend rsk_delete_ds;
