/*
 Copyright (C) 2022-2023 SAS Institute Inc. Cary, NC, USA
*/

/**
   \file
\anchor rsk_get_unique_varname

   \brief   Provides a unique variable name.

   \param [in] dsName:	The dataset name including optional library name where the output variable name is unique.
   \param [in] exclude [optional]:	List of names excluded from potential output (space delimited).

   \details
   Given a dataset name returns a unique valid variable name. Excludes any name occuring as exclude parameter.


   <b>Example:</b>

   \code
   		%let nm1 = %rsk_get_unique_varname(ds);
		%let nm2 = %rsk_get_unique_varname(ds, exclude=&nm1);
   \endcode

   \ingroup mrm

   \author  SAS Institute Inc.
   \date    2022
*/

%macro rsk_get_unique_varname(dsName, exclude=) / minoperator;
	%local __var;
	%let __var = _%sysfunc(putn(%sysfunc(rand(integer, 0, 2147483647)), hex8.));
	%do %while(&__var. in %rsk_getvarlist(&dsName.) %superq(exclude));
		%let __var = _%sysfunc(putn(%sysfunc(rand(integer, 0, 2147483647)), hex8.));
	%end;
	&__var.
%mend;
