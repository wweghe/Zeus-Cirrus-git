/*************************************************************************
 * Copyright 2023, SAS Institute Inc., Cary, NC, USA. All Rights Reserved.
 *
 * NAME:        core_krm_upload_market_data
 *
 * PURPOSE:     Upload market data tables to KRM data base
 *
 *
 * PARAMETERS: 
 *              ds_in 
 *                  <required> - input data set
 *              krm_library_name 
 *                  <required> - libref for KRM data base
 *              target_table_nm 
 *                  <required> - Target table name (without libref) in KRM data base whose data will be changed
 *              id_vars
 *                  <required> - Unique identifier columns in target table  
 *              mode 
 *                  <optional> - Determines how the table is loaded into KRM. Values: APPEND/REPLACE/UPDATE (Default: APPEND)
 *              replace_update_where 
 *                  <optional> - Additional where condition for REPLACE and UPDATE
 *
 * EXAMPLE:     %core_krm_upload_market_data(ds_in = input_code_user,
 *                                           krm_library_name = krmdb,
 *                                           target_table_nm = code_user ,
 *                                           mode = update,
 *                                           id_vars = description,
 *                                           replace_update_where = %str(TYPE_ID='YCL') 
 *                                           );
 **************************************************************************/
 %macro core_krm_upload_market_data(ds_in                = ,
                                    krm_library_name     = ,
                                    target_table_nm      = ,
                                    id_vars              = ,
                                    mode                 = append,
                                    replace_update_where = 
                                    );
    %local temp_table_to_delete i id num_ids where_ids;

    /* Check if input data set has any observations, and exit if not */
    %if %rsk_attrn(&ds_in., NOBS) eq 0 %then %do;
        /* Input data set does not have any observations. Write to the log and exit. */
        %put NOTE: Input table &ds_in. does not have any observations. Skipping data load to KRM database.;
        %return;
    %end;
 
    /* Make sure the mode parameter is set */
    %if %sysevalf(%superq(mode) =, boolean) %then
        %let mode = APPEND;
    %else
        %let mode = %upcase(&mode.);     
   
    /* Validate the MODE parameter */
    %if not %sysfunc(prxmatch(/^(APPEND|REPLACE|UPDATE)$/i, %superq(mode))) %then %do;
        %put ERROR: input parameter mode = &mode. is invalid. Valid values are APPEND|REPLACE|UPDATE;
        %return;
    %end;

    %if %upcase(&mode) eq UPDATE and %core_is_blank(id_vars) %then %do;
        %put ERROR: The parameter id_vars cannot be blank when mode is "UPDATE".;
        %return;
    %end;

    %let num_ids=%sysfunc(countw(&id_vars));
    %if &num_ids gt 1 %then %do;
       %let where_ids=;
       %do i=1 %to &num_ids;
          %let id=%scan(&id_vars, &i, %str( ));
          %if &i gt 1 %then %let where_ids=&where_ids and;
          %let where_ids=&where_ids upcase(krm_target_table.&id)=upcase(upload_data.&id);
       %end;
    %end;

    %if &mode eq APPEND %then %do;
        %core_krm_upload_table(ds_in = &ds_in,
                           krm_library_name = &krm_library_name, 
                           target_table_nm = &target_table_nm,
                           mode = &mode
                           );
    %end;
    %else %if &mode eq REPLACE %then %do;
        proc sql noprint;
           delete from &krm_library_name..&target_table_nm
           %if not %core_is_blank(replace_update_where) %then %do;
              where %unquote(&replace_update_where)
           %end;
           ;
        quit;

        %core_krm_upload_table(ds_in = &ds_in,
                           krm_library_name = &krm_library_name, 
                           target_table_nm = &target_table_nm,
                           mode = APPEND
                           );
    %end;
    %else %do;
        %if %rsk_varexist(DS=&ds_in, VAR=DATA_DT) %then %do; /*Market history data*/
           %local data_dts dt i;
           proc sql noprint;
              select distinct(data_dt) format=DATE9. into :data_dts separated by " "
              from &ds_in;
           quit;
           %do i= 1 %to &sqlobs;
              %let dt = %scan(&data_dts, &i, %str( ));
              data _history_data;
                 set &ds_in(keep=&id_vars data_dt);
                 where data_dt = "&dt"d;
                 keep &id_vars;
              run;
              proc sql ;
                 delete from &krm_library_name..&target_table_nm %if &num_ids gt 1 %then  as krm_target_table;
                 where %if &num_ids gt 1 %then %do;
                          exists ( select 1 from _history_data as upload_data
                                   where &where_ids )
                       %end;
                       %else %do;
                          upcase(&id_vars) in (select upcase(&id_vars) from _history_data)
                       %end;
                       and
                       data_dt = "&dt"d
                       %if not %core_is_blank(replace_update_where) %then %do;
                          and %unquote(&replace_update_where)
                       %end;
                       ;
              quit;
           %end;   
           %let temp_table_to_delete=&temp_table_to_delete _history_data;         
        %end;
        %else %do; /*Marekt definition data*/
           /*Delete records from ds_in*/
           proc sql ;
              delete from &krm_library_name..&target_table_nm %if &num_ids gt 1 %then  as krm_target_table;
              where %if &num_ids gt 1 %then %do;
                       exists in ( select 1 from &ds_in as upload_data
                                   where &where_ids )
                    %end;
                    %else %do;
                       upcase(&id_vars) in (select upcase(&id_vars) from &ds_in)
                    %end;
                    %if not %core_is_blank(replace_update_where) %then %do;
                       and %unquote(&replace_update_where)
                    %end;
                    ;
           quit;
        %end;

        %core_krm_upload_table(ds_in = &ds_in,
                           krm_library_name = &krm_library_name, 
                           target_table_nm = &target_table_nm,
                           mode = APPEND
                           );
    %end;

    %if not %core_is_blank(temp_table_to_delete) %then %do;
       proc delete data=&temp_table_to_delete;
       run;
    %end;
%mend core_krm_upload_market_data;