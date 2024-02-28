/*
 Copyright (C) 2022-2024 SAS Institute Inc. Cary, NC, USA
*/

/**
\file 
\anchor rsk_get_files_recursively
\brief

Lists all Files in a folder and its subfolders.

\details

\param [in]  root_path : absolute pathname.
\param [in]  out_ds_dirs : output result data set with full paths.
\param [in]  out_ds_dirs : output result data set with files.
\param [in]  file_type : file ending.

\n

\ingroup CommonAnalytics utilities
\author  SAS Institute Inc.
\date    2016
*/
%macro rsk_get_files_recursively(root_path = ,
                                 out_ds_dirs = file_paths,
                                 out_ds_files = file_list,
                                 file_type = sas);

    data &out_ds_dirs.;
        length root $256.;
        root ="&root_path.";
        output;
    run;


    data &out_ds_dirs. &out_ds_files.;
        keep Path FileName FileType Subpath;
        length fref $8 Filename $256 FileType$16 Subpath $32;

        /*read the name of the directory to search */
        modify &out_ds_dirs.;

        /*make a copy of the name because we might reset root; return 0 if the operation was successful*/
        Path=root;

        rc=filename(fref,path);

        if rc=0 then do;
            did=dopen(fref);
            rc=filename(fref);
        end;
        else do;
            length msg $200.;
            msg=sysmsg(); /*Returns error or warning message text from processing the last data set or external file function.*/
            putlog msg=;
            did = .;
        end;

        if did < 0 then do;
            putlog 'ERR' 'OR: Unable to open ' Path=;
            return;
        end;

        dnum=dnum(did); /* Returns the number of members in a directory. */

        do i = 1 to dnum;
            filename = dread(did, i); /* returns the name of a directory member */
            fid = mopen(did, filename); /* Opens a file by directory ID and member name, and returns either the file identifier or a 0. */

            if fid > 0 then do;
                FileType = prxchange('s/.*\.{1,1}(.*)/$1/',1, filename); /*performs a pattern-matching replacement*/
                if filename = filetype then filetype = ' ';
                filename=scan(filename,1);
                Subpath=cats(scan(Path,-2,"/"),"/",scan(Path,-1,"/")); /*for different analysis tasks, same subfolders can exist and we need to compare the ones belonging to the same task*/
                output &out_ds_files.;
            end;
            else do;
                root = catt(path,"/", filename);
                output &out_ds_dirs.;
            end;
        end;
        rc = dclose(did);

    run;

    %if &file_type ne %then %do;
        data &out_ds_files.;
            set &out_ds_files.;
            where Filetype eq "&file_type";
        run;
    %end;

%mend;
