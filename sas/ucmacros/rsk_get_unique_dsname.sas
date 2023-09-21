/*
 Copyright (C) 2022-2023 SAS Institute Inc. Cary, NC, USA
*/

/**
   \file
\anchor rsk_get_unique_dsname

   \brief   Provides a unique SAS dataset name.

   \param [in] libName:	The library name where the output dataset name is unique.
   \param [in] exclude [optional]:	List of names excluded from potential output (space delimited).

   \details
   Given a library name returns a unique valid dataset name. Excludes any name occuring as exclude parameter.


   <b>Example:</b>

   \code
   		%let nm1 = %rsk_get_unique_dsname(WORK);
		%let nm2 = %rsk_get_unique_dsname(WORK, exclude=&nm1);
   \endcode

   \ingroup mrm

   \author  SAS Institute Inc.
   \date    2022
*/

%macro rsk_get_unique_dsname(libName, exclude=) / minoperator;
	%local __ds;
	%let __ds = _%sysfunc(putn(%sysfunc(rand(integer, 0, 2147483647)), hex8.));
	%if %sysevalf(%superq(exclude) ne, boolean) %then %do;
		%do %while((%rsk_dsexist(&libname..&__ds.)) or (&__ds. in %superq(exclude)));
			%let __ds = _%sysfunc(putn(%sysfunc(rand(integer, 0, 2147483647)), hex8.));
		%end;
	%end;
	%else %do;
		%do %while(%rsk_dsexist(&libname..&__ds.));
			%let __ds = _%sysfunc(putn(%sysfunc(rand(integer, 0, 2147483647)), hex8.));
		%end;
	%end;
	&__ds.
%mend;
