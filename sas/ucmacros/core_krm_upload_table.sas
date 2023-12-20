/*************************************************************************
 * Copyright 2023, SAS Institute Inc., Cary, NC, USA. All Rights Reserved.
 *
 * NAME:        core_krm_upload_table
 *
 * PURPOSE:     Load data into the KRM database
 *
 *
 * PARAMETERS: 
 *              ds_in 
 *                  <required> - Input data set in the form LIBREF.MEMBER_NAME
 *              krm_library_name 
 *                  <required> - libref for KRM data base
 *              target_table_nm 
 *                  <required> - Target table name (without libref) in KRM data base whose data will be changed
 *              mode 
 *                  <optional> - Determines how the table is loaded into KRM. Values: APPEND/REPLACE (Default: REPLACE).
 *              delete_where_clause 
 *                  <optional> - Filter condition used to delete records from target table prior to appending new data.
 *                               Only relevant when mode = APPEND.
 *              append_where_clause 
 *                  <optional> - Filter condition used to specify which records from the input table should be loaded/appended to the target table
 *
 * Details:
 *              The macro will update the designated target table in the KRM database with data from the input table. The macro will write an error message and 
 *              return to the caller if input data set does not exist. The macro will create the target table if the target table does not exist. 
 *              Two primary data modes, APPEND and REPLACE, are supported along with some variations as described below: 
 *
 *                 1. APPEND without DELETE_WHERE_CLAUSE or APPEND_WHERE_CLAUSE: This will result in adding all input observations to the target table
 *                 2. APPEND with DELETE_WHERE_CLAUSE: This will result in first deleting observations from the target table that match the where clause followed by
 *                    adding all input observations to the target table. This serves the functionality of an UPSERT (UPDATE+INSERT). 
 *                 3. APPEND with APPEND_WHERE_CLAUSE: This will result in selecting input observations that match the where clause followed by adding the selected 
 *                    observations to the target table
 *                 4. APPEND with both DELETE_WHERE_CLAUSE and APPEND_WHERE_CLAUSE: This will result in selecting input observations that match the APPEND_WHERE_CLAUSE
 *                    followed by adding these observations to the target table after deleting observations from the target table that match the DELETE_WHERE_CLAUSE
 *                 5. REPLACE without APPEND_WHERE_CLAUSE: This will result in removing all observations from the target table followed by adding all observations
 *                    from the input table
 *                 6. REPLACE with APPEND_WHERE_CLAUSE: This will result in removing all observations from the target table followed by adding only those observations
 *                     from the input table that match the APPEND_WHERE_CLAUSE 
 *
 *
 * EXAMPLE:     %core_krm_upload_table(  ds_in = ACCR_INT_DS, 
 *                                       krm_library_name = SAS KRM Database, 
 *                                       target_table_nm = ACCR_INT, 
 *                                       mode = append, 
 *                                   delete_where_clause = txn_id='354876',
 *                                       append_where_clause = txn_id<>'543276'
 *                                    );
 **************************************************************************/
%macro core_krm_upload_table(   ds_in                   =
                              , krm_library_name        = 
                              , target_table_nm         =
                              , mode                    = replace
                              , delete_where_clause     =
                              , append_where_clause     =
                            );

    %local
        keep_columns
        num_var
        auth
        uid_pos
        pwd_pos
        u_str
        e_u_str
        p_str
        e_p_str
        temp_table_to_delete;
    ;
    
    /* Check if there is anything to load, and exit if not */
    %if(not %rsk_dsexist(&ds_in.)) %then %do;
        /* Input data set does not exist. Throw error and exit. */
        %put ERROR: Input table &ds_in. does not exist. Skipping data load to KRM database.;
        %return;
    %end;

    /* Check if input data set has any observations, and exit if not */
    %if %rsk_attrn(&ds_in., NOBS) eq 0 %then %do;
        /* Input data set does not have any observations. Write to the log and exit. */
        %put NOTE: Input table &ds_in. does not have any observations. Skipping data load to KRM database.;
        %return;
    %end;

    /* Check that target table name has been provided */    
    %if %sysevalf(%superq(target_table_nm) =, boolean) %then %do;
        %put ERROR: Target table name not provided.;
        %return;
    %end;

    /* Make sure the mode parameter is set */
    %if %sysevalf(%superq(mode) =, boolean) %then
        %let mode = REPLACE;
    %else
    %let mode = %upcase(&mode.);     
   
    /* Validate the MODE parameter */
    %if not %sysfunc(prxmatch(/^(APPEND|REPLACE)$/i, %superq(mode))) %then %do;
        %put ERROR: input parameter mode = &mode. is invalid. Valid values are APPEND|REPLACE;
        %return;
    %end;
    
    /* Check if target KRM table exists */
    %if %rsk_dsexist(&krm_library_name..&target_table_nm.) %then %do;   
        %let keep_columns=;    

        /* If target table exists then restrict keep columns */    
        proc contents data = &krm_library_name..&target_table_nm. 
            out = tmp_content_base (keep=NAME type length)
            noprint nodetails short;
        run;
        %let temp_table_to_delete=&temp_table_to_delete tmp_content_base;

        /* Get column names for input data set */
        proc contents data = &ds_in. 
            out = tmp_content_data (keep=NAME)
            noprint nodetails short;
        run;
        %let temp_table_to_delete=&temp_table_to_delete tmp_content_data;
        
        /* Only keep columns that exist in both input and target tables */
        proc sql noprint;
            select b.NAME into :keep_columns separated by ' '
            from tmp_content_base b, tmp_content_data d
            where upcase(b.NAME)=upcase(d.NAME)
            ;
        quit;

        /* In REPLACE mode, remove all observations from the KRM table */
        %if(&mode. = REPLACE) %then %do;
            proc sql noprint;
                delete from &krm_library_name..&target_table_nm.;
            quit;
            %put # OF ROWS DELETED = &SQLOBS.;
        %end;
        %else %do;
            /* In APPEND mode, check if we need to delete any records */
            %if %sysevalf(%superq(delete_where_clause) ne, boolean) %then %do;
                /* Remove matching records from the target KRM table */
                proc sql noprint;
                    delete from &krm_library_name..&target_table_nm.
                    where %unquote(&delete_where_clause.);
                quit;
                %put # OF ROWS DELETED = &SQLOBS.;                
            %end;            
        %end;            
        
        /* Now append observations from input data set */
        proc append base = &krm_library_name..&target_table_nm. nowarn force data = &ds_in.
            %if %sysevalf(%superq(keep_columns) ne, boolean) %then %do;
                (keep = &keep_columns.);
            %end;
            ;

            /* Apply a where clause on the input data (if specified) */
            %if %sysevalf(%superq(append_where_clause) ne, boolean) %then %do;
                where %unquote(&append_where_clause.);
            %end;
        quit;  
    %end;        
    %else %do;
        /* KRM table does not exist. Create the KRM table. */
        data &krm_library_name..&target_table_nm.;
            set &ds_in.;

            /* Apply a where clause on the input data (if specified) */
            %if %sysevalf(%superq(append_where_clause) ne, boolean) %then %do;
                where %unquote(&append_where_clause.);
            %end;
        run;
        
        %if %rsk_dsexist(&krm_library_name..&target_table_nm.) %then          
            %put TABLE [&target_table_nm.] WAS CREATED IN KRM DATABASE;
        %else
            %put ERROR CREATING TABLE [&target_table_nm.] IN KRM DATABASE;
    %end;   

    %if not %core_is_blank(temp_table_to_delete) %then %do;
       proc delete data=&temp_table_to_delete;
       run;
    %end;   
    
%mend core_krm_upload_table;
