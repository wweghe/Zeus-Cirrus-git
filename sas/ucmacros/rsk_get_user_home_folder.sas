/*
 Copyright (C) 2022-2024 SAS Institute Inc. Cary, NC, USA
*/

/*!
\file 
\anchor rsk_get_user_home_folder
\brief   Return the URI of the user private folder

\param [out] OUT Name of the output macro variable that will hold the metadata URI of the user's private folder (/User Folders/<userid>/My Folder)


\details
This macro will return the URI of the user's private folder (/User Folders/<userid>/My Folder).

\ingroup metadata
\author  SAS Institute Inc.
\date    2016
*/
%macro rsk_get_user_home_folder(out = out);
   filename _out temp;
   proc metadata
      out = _out
      in = "<GetUserFolders>
               <Tree  PersonName = '' FolderName='My Folder'/>
            </GetUserFolders>";
   run;
   data _null_;
      length line $200;
      infile _out delimiter='>' ;
      input line $ @@;
      if (index(line,"Id") > 0) then do;
         call symput("&out.", substr(line, index(line, "Id")+4, 17));
         stop;
      end;
   run;
   filename _out clear;
%mend;