/*
 Copyright (C) 2017 SAS Institute Inc. Cary, NC, USA
*/

/**
   \file 
\anchor core_apply_allocation_rules

   \brief   Apply allocation rules to detail level

   \param [in] ds_in Input dataset containing detail level data.
   \param [in] rule_def_ds Input dataset containing allocation rules.
   \param [in] exclude_filter_vars (Optional) List of space separated columns of the configuration dataset (RULE_DEF_DS) which should not be considered as filters.
   \param [in] drop (Optional) List of columns to be dropped from the output table.
   \param [out] ds_out Name of the output dataset containing allocated results.
   \param [out] ds_out_rule_summary Name of the output dataset containing a summary of the rules being applied and the number of records that were affected by each rule.
   \param [out] ds_out_audit (Optional) Name of the output dataset containing the details of each record that was modified.

   \details
   This macro applies a set of allocation rules to the input table &DS_IN. based on the rules defined inside the configuration table &RULE_DEF_DS.
   The structure of the RULE_DEF_DS configuration table is as follows:

   | Variable                              | Type            | Required?  | Label                     | Description                                                                                                                                               |
   |---------------------------------------|-----------------|------------|---------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------|
   | RULE_ID                               | CHARACTER(32)   | N          | Rule Identifier           | Rule Identifier.  Optional column, may not exist for all rule set types.                                                                                  |
   | RULE_DESC                             | CHARACTER(4096) | N          | Rule Description          | Rule Description.  Optional column, may not exist for all rule set types.                                                                                 |
   | RULE_METHOD                           | CHARACTER(10)   | Y          | Rule Method               | The type of Adjustment Value. Either value or formula                                                                                                     |
   | ADJUSTMENT_VALUE                      | CHARACTER(32000)| Y          | Adjustment Value          | The value of the adjustment. It can be absolute, relative percentage (i.e. as a percentage increase/decrease) or relative increment (additive adjustment). For Classification rules, this field will be character(150). For Allocation rules, this field can be numeric or character. |
   | MEASURE_VAR_NM                        | CHARACTER(32)   | Y          | Measure Variable Name     | Name of the column of the input &DS_IN table to be adjusted.                                                                                              |
   | ADJUSTMENT_TYPE                       | CHARACTER(10)   | Y          | Adjustment Type           | Type of adjustment: ABSOLUTE, RELATIVE, INCREMENT                                                                                                         |
   | ALLOCATION_METHOD                     | CHARACTER(20)   | Y          | Allocation Method         | Determines how the adjustment value is allocated down to detail level: EVEN, PROPORTIONAL, INDIVIDUAL. See below for details.                             |
   | AGGREGATION_METHOD                    | CHARACTER(10)   | Y          | Aggregation Method        | Controls how detail records are aggregated: SUM, MEAN, MAX, MIN.                                                                                          |
   | WEIGHT_VAR_NM                         | CHARACTER(32)   | N          | Weight Variable Name      | Specify a weight column for the allocation and/or aggregation. See below for details.                                                                     |
   | WEIGHTED_AGGREGATION_FLG              | CHARACTER(1)    | Y          | Weighted Aggregation Flag | Flag Y/N. Controls whether the system should perform weighted (Y) or simple (N) aggregation.                                                              |
   | <i> &lt;FILTER VARIABLE NAME&gt; </i> | TEXT            | N          | Filter variable           | Specifies the rule filter condition to be applied on the &lt;FILTER VARIABLE NAME&gt; column of the input &DS_IN table. See below for details.            |


   Each record of the configuration table specifies an allocation rule, all rules are applied in a single step to the input dataset, according to the order in which they appear.

   <br>
   <b>Filter variables</b> <br>
   Filtering conditions can contain any operator that is supported by both SAS Data Step and Proc SQL, i.e.:
      - Basic comparison: =, >=, <=, <>, gt, ge, le, lt, ne, etc.
      - Range/List operators: in, between
      - Boolean operators: (and, or, not)
      - Special operators: like, contains, =: (starts with).

   Special operators are automatically translated into regular expressions for pattern matching.<br>
   \code
      // The =: and LIKE operators are equivalent and always match from the start of the string.
      [<variable>] [NOT] =:         "<expression>"[/ix]      -->  [NOT] prxmatch('/^<expression>/[ix]', <variable>)
      [<variable>] [NOT] LIKE       '<expression>'[/ix]      -->  [NOT] prxmatch('/^<expression>/[ix]', <variable>)
      // The CONTAINS operator matches any subsequence of the string. Note the use of another variable to perform a dynamic match (the pattern is dynamically evaluated for each record).
      [<variable>] [NOT] CONTAINS   <other-variable>[/ix]    -->  [NOT] prxmatch(cat('/', strip(<other-variable>), '/[ix]'), <variable>)
   \endcode
   The expression can contain underscore (_) or wildcards (% or *) to match single or multiple characters. To escape these special characters use the backslash.<br>
   The optional modifier /i after the expression indicates a case insensitive match.<br>
   The optional modifier /x after the expression can be used to ignore spaces in the match pattern.

   Depending on allocation method being selected, the adjustment is applied as follows:
   - EVEN: The amount equally allocated across all records that match the rule's filtering condition
   - PROPORTIONAL: Allocation is proportional to the value of the <i>&lt;Measure&gt;</i> column or to the weight
   - INDIVIDUAL: the amount is applied individually to each record that matches the rule's filtering condition

   The sections below describe how each rule is applied based on the rule definition inside the configuration table. <br>
   The following notation is used:
   - \f$Adj\f$ is the adjustment value that must be allocated to details
   - \f$N\f$ is the number of records matching the rule's filtering condition
   - \f$v_n\f$ is each of \f$N\f$ detail values of the <i>&lt;Measure&gt;</i> column in the input table matching the rule's filtering condition (\f$n = 1 to N\f$)
   - \f$v'_n\f$ is the allocated value of the <i>&lt;Measure&gt;</i> column in the output table
   - \f$T\f$ is the total aggregated value across the records that match the rule's filtering condition.

   The value of \f$T\f$ depends on the aggregation method and whether the aggregation is simple or weighted.

   | Aggregation Method | Simple Aggregation              | Weighted Aggregation                              |
   |--------------------|---------------------------------|---------------------------------------------------|
   | SUM                | \f[T = \sum_n v_n\f]            | \f[T = \sum_n w_n \cdot v_n\f]                    |
   | MEAN               | \f[T = \frac{1}{N}\sum_n v_n\f] | \f[T = \frac{\sum_n w_n \cdot v_n}{\sum_n w_n}\f] |
   | MAX*               | \f[T = \max_n v_n\f]            | \f[T = \max_n v_n\f]                              |
   | MIN*               | \f[T = \min_n v_n\f]            | \f[T = \min_n v_n\f]                              |

   (*) Simple aggregation is always applied in case of Max and Min function.


   The table below shows how the allocation rules are applied based on the configuration selection

   | Allocation Method | Aggregation Method | Weighted Aggregation Flag | Weight provided? | Absolute Adjustment                                   | Relative Adjustment                                                 | Increment Adjustment                                        |
   |-------------------|--------------------|---------------------------|------------------|-------------------------------------------------------|---------------------------------------------------------------------|-------------------------------------------------------------|
   | EVEN              | SUM                | Y                         | *                | \f[v'_n = \frac{Adj}{\sum_n w_n}\f]                   | \f[v'_n = \frac{T \cdot (1 + Adj)}{\sum_n w_n}\f]                   | \f[v'_n = \frac{T + Adj}{\sum_n w_n}\f]                     |
   | EVEN              | SUM                | N                         | *                | \f[v'_n = \frac{Adj}{N}\f]                            | \f[v'_n = \frac{T \cdot (1 + Adj)}{N}\f]                            | \f[v'_n = \frac{T + Adj}{N}\f]                              |
   | EVEN              | MEAN               | *                         | *                | \f[v'_n = Adj\f]                                      | \f[v'_n = T \cdot (1 + Adj)\f]                                      | \f[v'_n = T + Adj\f]                                        |
   | PROPORTIONAL      | SUM                | N                         | Y                | \f[v'_n = Adj \cdot \frac{w_n}{\sum_n w_n}\f]         | \f[v'_n = \frac{T \cdot (1 + Adj) \cdot w_n}{\sum_n w_n}\f]         | \f[v'_n = \frac{(T + Adj) \cdot w_n}{\sum_n w_n}\f]         |
   | PROPORTIONAL      | SUM                | *                         | N                | \f[v'_n = \frac{Adj \cdot v_n}{T}\f]                  | \f[v'_n = v_n \cdot (1 + Adj)\f]                                    | \f[v'_n = \frac{(T + Adj) \cdot v_n}{T}\f]                  |
   | PROPORTIONAL      | SUM                | Y                         | *                | \f[v'_n = \frac{Adj \cdot v_n}{T}\f]                  | \f[v'_n = v_n \cdot (1 + Adj)\f]                                    | \f[v'_n = \frac{(T + Adj) \cdot v_n}{T}\f]                  |
   | PROPORTIONAL      | MEAN               | N                         | Y                | \f[v'_n = \frac{Adj \cdot N \cdot w_n}{\sum_n w_n}\f] | \f[v'_n = \frac{T \cdot (1 + Adj) \cdot N \cdot w_n}{\sum_n w_n}\f] | \f[v'_n = \frac{(T + Adj) \cdot N \cdot w_n}{\sum_n w_n}\f] |
   | PROPORTIONAL      | MEAN               | *                         | N                | \f[v'_n = \frac{Adj \cdot v_n}{T}\f]                  | \f[v'_n = v_n \cdot (1 + Adj)\f]                                    | \f[v'_n = \frac{(T + Adj) \cdot v_n}{T}\f]                  |
   | PROPORTIONAL      | MEAN               | Y                         | *                | \f[v'_n = \frac{Adj \cdot v_n}{T}\f]                  | \f[v'_n = v_n \cdot (1 + Adj)\f]                                    | \f[v'_n = \frac{(T + Adj) \cdot v_n}{T}\f]                  |
   | INDIVIDUAL        | SUM/MEAN           | *                         | *                | \f[v'_n = Adj\f]                                      | \f[v'_n =  v_n \cdot (1 + Adj)\f]                                   | \f[v'_n = (v_n + Adj)\f]                                            |
   | INDIVIDUAL        | MAX                | *                         | *                | \f[v'_n = \min(Adj, v_n)\f]                           | \f[v'_n =  v_n \cdot (1 + Adj)\f]                                   | \f[v'_n = \min(Adj, v_n)\f]                                 |
   | INDIVIDUAL        | MIN                | *                         | *                | \f[v'_n = \max(Adj, v_n)\f]                           | \f[v'_n =  v_n \cdot (1 + Adj)\f]                                   | \f[v'_n = \max(Adj, v_n)\f]                                 |

   (*) The * symbol is used as wildcard to indicate that any value is allowed.

   Regardless of the allocation method (EVEN/PROPORTIONAL/INDIVIDUAL), the allocation based on Maximum or Minimum is always performed as follows (except for the INDIVIDUAL cases described in the table above):

   | Aggregation Method | Absolute Adjustment         | Relative Adjustment                     | Increment Adjustment              |
   |--------------------|-----------------------------|-----------------------------------------|-----------------------------------|
   | MAX                | \f[v'_n = \min(Adj, v_n)\f] | \f[v'_n = \min(T \cdot (1+Adj), v_n)\f] | \f[v'_n = \min((T + Adj), v_n)\f] |
   | MIN                | \f[v'_n = \max(Adj, v_n)\f] | \f[v'_n = \max(T \cdot (1+Adj), v_n)\f] | \f[v'_n = \max((T + Adj), v_n)\f] |


   <b>Example rules:</b>

   | ADJUSTMENT_VALUE   | MEASURE_VAR_NM | ADJUSTMENT_TYPE | ALLOCATION_METHOD | AGGREGATION_METHOD | WEIGHT_VAR_NM | WEIGHTED_AGGREGATION_FLG | CREDIT_SCORE_BUCKET | COUNTRY_CD      |
   |-------------------:|----------------|-----------------|-------------------|--------------------|---------------|--------------------------|---------------------|-----------------|
   |            10,000  | ECL            | ABSOLUTE        | PROPORTIONAL      | SUM                | EXPOSURE_AMT  | Y                        | = '450-650'         | in ('IT', 'FR') |
   |            10,000  | ECL            | INCREMENT       | PROPORTIONAL      | SUM                | EXPOSURE_AMT  | N                        | =: '450'            | in ('IT', 'FR') |
   |              0.20  | ECL            | RELATIVE        | EVEN              | SUM                |               |                          | = '450-650'         | in ('IT', 'FR') |
   |              0.05  | PD             | ABSOLUTE        | PROPORTIONAL      | MEAN               | EXPOSURE_AMT  | Y                        | = '450-650'         | like 'F%'/i     |
   |              0.03  | PD             | ABSOLUTE        | INDIVIDUAL        | MIN                |               |                          |                     |                 |
   |        10,000,000  | ECL            | ABSOLUTE        | PROPORTIONAL      | MAX                |               |                          |                     | = 'SE'          |

   - <b>Rule 1</b><br>
      Allocate the amount 10,000 across the loans where CREDIT_SCORE_BUCKET = '450-650' and COUNTRY_CD in ('IT', 'FR'). <br>
      The amount should be allocated proportionally to the Exposure_Amt column. <br>
      The allocation must be such that the weighted aggregation sum(ECL*EXPOSURE_AMT) = 10,000 for the sub-portfolio that matches the filter condition

   - <b>Rule 2</b><br>
      Increase the total ECL amount by 10,000 and allocate the new value across the loans where CREDIT_SCORE_BUCKET starts with '450' and COUNTRY_CD in ('IT', 'FR'). <br>
      The amount should be allocated proportionally to the Exposure_Amt column. <br>
      The allocation must be such that sum(ECL') = sum(ECL) + 10,000 for the sub-portfolio that matches the filter condition, where ECL' is the result of the allocation and ECL is the value before the allocation.

   - <b>Rule 3</b><br>
      Increase by 20% the current total ECL for the loans where CREDIT_SCORE_BUCKET = '450-650' and COUNTRY_CD in ('IT', 'FR'). <br>
      The increased ECL should be equally allocated across all loans of the sub-portfolio that matched the filter condition.

   - <b>Rule 4</b><br>
      The aggregated exposure weighted average PD should be equal to 0.05 for the loans with CREDIT_SCORE_BUCKET = '450-650' where the country code starts with an 'F' (the /i modifier makes the search case-insensitive) followed by any character. <br>The aggregated PD value should be allocated proportionally.

   - <b>Rule 5</b><br>
      The minimum PD across all loans must not be lower than 0.03.

   - <b>Rule 6</b><br>
      The maximum ECL for the Swedish portfolio must not be higher than 10 million.

   \ingroup macroUtils

   \author  SAS Institute Inc.
   \date    2017
*/
%macro core_apply_allocation_rules(ds_in =
                                  , rule_def_ds =
                                  , ds_in_var_dependency =
                                  , exclude_filter_vars =
                                  , custom_filter_var =
                                  , ds_out =
                                  , ds_out_rule_summary = rules_summary
                                  , ds_out_audit = audit_log
                                  , drop =
                                  );
   %local
      current_dttm
      rule_id_col
      rule_desc_col
      adj_value_col
      measure_col
      adj_type_col
      alloc_method_col
      weight_col
      weighted_aggr_flg_col
      colnames
      any_aggr_rule_flg
      tot_rules
      total_row_cnt
      main_output_flg
      audit_flg
      var_dependency_flg
      audit_drop_stmt
      audit_var_list
      curr_var
      i
   ;

   /* Set the name of the columns that are expected to be found in the RULE_DEF_DS table */
   %let adj_value_col = adjustment_value;
   %let measure_col = measure_var_nm;
   %let adj_type_col = adjustment_type;
   %let alloc_method_col = allocation_method;
   %let aggr_method_col = aggregation_method;
   %let weight_col = weight_var_nm;
   %let weighted_aggr_flg_col = weighted_aggregation_flg;
   %let rule_id_col = rule_id;
   %let rule_desc_col = rule_desc;

   %let current_dttm = "%sysfunc(datetime(), datetime21.)"dt;
   %let total_row_cnt = 0;


   /* Check if we need to create the main output table */
   %if(%sysevalf(%superq(ds_out) ne, boolean)) %then
      %let main_output_flg = Y;
   %else
      %let main_output_flg = N;

   /* Check if we need to create the audit log table */
   %if(%sysevalf(%superq(ds_out_audit) ne, boolean)) %then
      %let audit_flg = Y;
   %else
      %let audit_flg = N;

   /* Exit if both the ds_out and the ds_out_audit parameters are missing */
   %if(&main_output_flg. = N and &audit_flg. = N) %then %do;
      %put ERROR: Both input parameters DS_OUT and DS_OUT_AUDIT are missing. You must specify at least one.;
      %return;
   %end;

   /* Find out all column names */
   proc contents data = &rule_def_ds.
                  out = tmp_content
                  noprint short;
   run;

   /* Process filter variables unless a custom filter column has been specified */
   %if(%sysevalf(%superq(custom_filter_var) =, boolean)) %then %do;
      /* Get the names of the character columns into a macro variable */
      proc sql noprint;
         select
            name
               into :colnames separated by ' '
         from
            tmp_content
         where
            type = 2
            and upcase(name) not in
               ("%upcase(&rule_id_col.)"
                , "%upcase(&rule_desc_col.)"
                , "%upcase(&adj_value_col.)"
                , "%upcase(&adj_type_col.)"
                , "%upcase(&alloc_method_col.)"
                , "%upcase(&measure_col.)"
                , "%upcase(&aggr_method_col.)"
                , "%upcase(&weight_col.)"
                , "%upcase(&weighted_aggr_flg_col.)"
                %if(%sysevalf(%superq(exclude_filter_vars) ne, boolean)) %then %do;
                   %do i = 1 %to %sysfunc(countw(&exclude_filter_vars., %str( )));
                     , "%upcase(%scan(&exclude_filter_vars., &i., %str( )))"
                   %end;
                %end;
                )
         ;
      quit;
   %end;

   /* Process the var dependency table if it was provided */
   %let var_dependency_flg = N;
   %if(%sysevalf(%superq(ds_in_var_dependency) ne, boolean)) %then %do;
      %let var_dependency_flg = Y;
      proc sql;
         create table __tmp_var_dependency__ as
         select
            upcase(trigger_var_name) as __trigger_var_name__
            , order_no as __order_no__
            , dependent_var_name as __dependent_var_name__
            , expression_txt as __expression_txt__
         from
            &ds_in_var_dependency.
         order by
            trigger_var_name
            , order_no
         ;
      quit;
   %end;

   /* Build filter conditions */
   %let tot_rules = 0;
   %let any_aggr_rule_flg = N;
   data _null_;
      set &rule_def_ds. end = last;
      length
         __tmp_where_clause $10000.
         current_filter $10000.
         start_with $1.
      ;

      /* Setup lookup for retrieving the dependency rules */
      %if(&var_dependency_flg. = Y) %then %do;
         if _N_ = 0 then
            set __tmp_var_dependency__;

         if _N_ = 1 then do;
            declare hash hDep(dataset: "__tmp_var_dependency__", multidata: "yes");
            hDep.defineKey("__trigger_var_name__");
            hDep.defineData("__dependent_var_name__", "__expression_txt__");
            hDep.defineDone();
         end;
      %end;

      /* Process filter variables unless a custom filter column has been specified */
      %if(%sysevalf(%superq(custom_filter_var) =, boolean)) %then %do;

         /* Build the where clause based on the specified filter columns */
         %if(%sysfunc(countw(&colnames., %str( ))) > 0) %then %do;
            array filters[*] &colnames.;
            array varnames[%sysfunc(countw(&colnames., %str( )))] $32 _temporary_ (%sysfunc(prxchange(s/(\w+)/"$1"/i, -1, &colnames.)));

            do n = 1 to dim(filters);
               if not missing(filters[n]) then do;
                  /* Copy the value of the current filter for further manipulation below */
                  current_filter = filters[n];
                  /* Pre-process the filter condition. Convert special operators (=:, like, contains) to regular expression */
                  if(prxmatch("/=:|LIKE|CONTAINS/i", filters[n])) then do;
                     /* Check if we need to add the "start with" modifier to the regular expression */
                     call missing(start_with);
                     if(prxmatch("/=:|LIKE/", filters[n])) then
                        start_with = "^";

                     /* Convert special dot character . -> \. (only if the dot is not preceded by an escape backslash)  */
                     current_filter = prxchange("s/(?<!\\)[.]/\./i", -1, current_filter);
                     /* Convert underscore to regex single-character wildcard -> (.) (only if the underscore is not preceded by an escape backslash)  */
                     current_filter = prxchange("s/(?<!\\)[_]/(.)/i", -1, current_filter);
                     /* Convert \_ to _ (remove escape backslash character) */
                     current_filter = prxchange("s/\\_/_/i", -1, current_filter);
                     /* Convert % or * to regex multi-character wildcard -> (.*) (only if the % or * is not preceded by an escape backslash) */
                     current_filter = prxchange("s/(?<!\\)[%*]/(.*)/i", -1, current_filter);
                     /* Convert \% to % (remove escape backslash character) */
                     current_filter = prxchange("s/\\%/%/i", -1, current_filter);
                     /* Convert the Starts-With (=:), LIKE and CONTAINS operators to a static perl regex match expression:
                           [<variable>] [NOT] =:         "<expression>"[/ix]  -->  [NOT] prxmatch('/^<expression>/[ix]', <variable>)
                           [<variable>] [NOT] LIKE       "<expression>"[/ix]  -->  [NOT] prxmatch('/^<expression>/[ix]', <variable>)
                           [<variable>] [NOT] CONTAINS   "<expression>"[/ix]  -->  [NOT] prxmatch('/<expression>/[ix]', <variable>)
                     */
                     current_filter = prxchange(cat("s/(", strip(varnames[n]), "\s+)?(NOT\s+)?(=:|LIKE|CONTAINS)\s+(([']([^']+)['])|([""]([^""]+)[""]))(\/([ix]{1,2})\w*)?/"
                                                    , "$2prxmatch('\/", strip(start_with), "$6$8\/$10', ", strip(varnames[n]), ")/i")
                                                , -1
                                                , current_filter
                                                );
                     /* Convert the Starts-With (=:), LIKE and CONTAINS operators to a dynamic perl regex match expression:
                           [<variable>] [NOT] =:         <other-variable>[/ix]  -->  [NOT] prxmatch(cat('/^', <other-variable>, '/[ix]'), <variable>)
                           [<variable>] [NOT] LIKE       <other-variable>[/ix]  -->  [NOT] prxmatch(cat('/^', <other-variable>, '/[ix]'), <variable>)
                           [<variable>] [NOT] CONTAINS   <other-variable>[/ix]  -->  [NOT] prxmatch(cat('/', <other-variable>, '/[ix]'), <variable>)
                     */
                     current_filter = prxchange(cat("s/(", strip(varnames[n]), "\s+)?(NOT\s+)?(=:|LIKE|CONTAINS)\s+(\w+)(\/([ix]{1,2})\w*)?/"
                                                    , "$2prxmatch(cat('\/", strip(start_with), "', strip($4), '\/$6'), ", strip(varnames[n]), ")/i")
                                                , -1
                                                , current_filter
                                                );

                  end;

                  /* Convert the BETWEEN operator to <value1> <= <variable> <= <value2> */
                  current_filter = prxchange(cat("s/(", strip(varnames[n]), "\s+)?(NOT\s+)?BETWEEN\s+((['][^']+['])|([""][^""]+[""])|(\w+))\s+AND\s+((['][^']+['])|([""][^""]+[""])|(\w+))/"
                                                 , "$2($3 <= ", strip(varnames[n]), " <= $7)/i")
                                             , -1
                                             , current_filter
                                             );

                  if(prxmatch(cats("/\b", varnames[n], "\b/i"), current_filter)) then
                     /* The expression contains already the variable name (i.e.: 0 < varname < 100). Add it AS-IS */
                     __tmp_where_clause = catx(" and ", __tmp_where_clause, current_filter);
                  else
                     __tmp_where_clause = catx(" and ", __tmp_where_clause, catx(" ", varnames[n], current_filter));
               end;
            end;
         %end;

      %end; /* %if(%sysevalf(%superq(custom_filter_var) =, boolean)) */
      %else %do;
         /* Use custom filter variable */
         __tmp_where_clause = &custom_filter_var.;
      %end;

      /* Make sure the where clause is always specified */
      if(missing(__tmp_where_clause)) then
         __tmp_where_clause = "1";

      call symputx(cats("where_clause_", put(_N_, 8.)), __tmp_where_clause, "L");
      call symputx(cats("rule_id_", put(_N_, 8.)), &rule_id_col., "L");
      call symputx(cats("rule_desc_", put(_N_, 8.)), &rule_desc_col., "L");
      call symputx(cats("measure_col_", put(_N_, 8.)), &measure_col., "L");
      call symputx(cats("allocation_method_", put(_N_, 8.)), upcase(&alloc_method_col.), "L");
      call symputx(cats("adjustment_type_", put(_N_, 8.)), upcase(&adj_type_col.), "L");
      call symputx(cats("aggr_method_", put(_N_, 8.)), upcase(&aggr_method_col.), "L");
      call symputx(cats("weight_col_", put(_N_, 8.)), &weight_col., "L");
      call symputx(cats("weighted_aggr_flg_", put(_N_, 8.)), coalescec(upcase(&weighted_aggr_flg_col.), "N"), "L");
      call symputx(cats("adj_value_", put(_N_, 8.)), &adj_value_col., "L");
      if upcase(&alloc_method_col.) ne "INDIVIDUAL" then
         call symputx("any_aggr_rule_flg", "Y", "L");

      %if(&var_dependency_flg. = Y) %then %do;
         __trigger_var_name__ = upcase(&measure_col.);
         __i__ = 0;
         call missing(__dependent_var_name__, __expression_txt__);
         do while(hDep.do_over() = 0);
            __i__ = __i__ + 1;
            call symputx(cats("dependent_var_", _N_, "_", __i__), __dependent_var_name__, "L");
            call symputx(cats("expression_txt_", _N_, "_", __i__), __expression_txt__, "L");
            call missing(__dependent_var_name__, __expression_txt__);
         end;
         call symputx(cats("dependent_var_cnt_", _N_), __i__, "L");
      %end;

      if last then
         call symputx("tot_rules", _N_, "L");
   run;


   %if (&any_aggr_rule_flg. = Y) %then %do;
      proc sql noprint;
         select
            count(*)
            %do i = 1 %to &tot_rules.;
               %if(&&allocation_method_&i. ne INDIVIDUAL) %then %do;
                  /* Aggregate the value for the records that match the where condition */
                  %if(&&weighted_aggr_flg_&i.. = Y) %then %do;
                     %if(&&aggr_method_&i.. = SUM or &&aggr_method_&i.. = ) %then
                        /* Perform weighted sum aggregation */
                        , sum(&&measure_col_&i.. * &&weight_col_&i.. * (&&where_clause_&i..)) format = best32.;
                     %else %if(&&aggr_method_&i.. = MEAN) %then
                        /* Perform weighted average aggregation */
                        , sum(&&measure_col_&i.. * &&weight_col_&i.. * (&&where_clause_&i..)) / sum(&&weight_col_&i.. * (&&where_clause_&i..)) format = best32.;
                     %else /* &&aggr_method_&i.. in (MAX, MIN) */
                        /* Perform weighted aggregation using the specified aggregation method*/
                        , &&aggr_method_&i..(&&measure_col_&i.. * ifn(&&where_clause_&i.., 1, .)) format = best32.;
                  %end;
                  %else %do;
                     /* Perform simple aggregation using the provided aggregation method (SUM/MEAN/MAX/MIN) */
                     %if(&&aggr_method_&i.. = SUM or &&aggr_method_&i.. = ) %then
                     , SUM(&&measure_col_&i.. * ifn(&&where_clause_&i.., 1, .)) format = best32.;
                     %else
                     , &&aggr_method_&i..(&&measure_col_&i.. * ifn(&&where_clause_&i.., 1, .)) format = best32.;
                  %end;

                  /* Count the records that match the where condition */
                  , sum((&&where_clause_&i..)) format = best32.

                  /* Compute sum of weights */
                  %if(%sysevalf(%superq(weight_col_&i.) ne, boolean)) %then
                     , sum(&&weight_col_&i.. * (&&where_clause_&i..)) format = best32.;

               %end; /* %if(&&allocation_method_&i. ne INDIVIDUAL) */
            %end; /* %do i = 1 %to &tot_rules.; */
               into
                  :Tot_records
                  %do i = 1 %to &tot_rules.;
                     %if(&&allocation_method_&i. ne INDIVIDUAL) %then %do;
                        , :current_value_aggr_&i.
                        , :current_value_cnt_&i.
                        %if(%sysevalf(%superq(weight_col_&i.) ne, boolean)) %then
                           , :current_weight_sum_&i.;
                     %end;
                  %end;
         from
            &ds_in.
         ;
      quit;
   %end;

   %let audit_var_list =
         MEASURE_NAME
         MEASURE_VAR_TYPE
         SEQUENCE_NO
         CURRENT_VALUE
         PREVIOUS_VALUE
         DELTA_VALUE
         WEIGHT_VALUE
         CURRENT_TXT_VALUE
         PREVIOUS_TXT_VALUE
         PROCESSED_DTTM
         RULE_ID
         RULE_DESC
   ;
   %do i = 1 %to %sysfunc(countw(&audit_var_list., %str( )));
      %let curr_var = %scan(&audit_var_list., &i., %str( ));
      %if(%rsk_varexist(&ds_in., &curr_var.)) %then
         %let audit_drop_stmt = &audit_drop_stmt. &curr_var.;
   %end;
   %if(%sysevalf(%superq(audit_drop_stmt) ne, boolean)) %then
      %let audit_drop_stmt = drop = &audit_drop_stmt.;

   data
      %if(&main_output_flg. = Y) %then %do;
         &ds_out.
            %if(&audit_flg. = Y) %then %do;
               (drop = __measure_name__
                       __measure_var_type__
                       __sequence_no__
                       __current_value__
                       __previous_value__
                       __delta_value__
                       __weight_value__
                       __current_txt_value__
                       __previous_txt_value__
                       __processed_dttm__
                       __rule_id__
                       __rule_desc__
               )
            %end;
      %end;

      %if(&audit_flg. = Y) %then %do;
         &ds_out_audit.(&audit_drop_stmt.
                        rename = (__measure_name__ = MEASURE_NAME
                                  __measure_var_type__ = MEASURE_VAR_TYPE
                                  __sequence_no__ = SEQUENCE_NO
                                  __current_value__ = CURRENT_VALUE
                                  __previous_value__ = PREVIOUS_VALUE
                                  __delta_value__ = DELTA_VALUE
                                  __weight_value__ = WEIGHT_VALUE
                                  __current_txt_value__ = CURRENT_TXT_VALUE
                                  __previous_txt_value__ = PREVIOUS_TXT_VALUE
                                  __processed_dttm__ = PROCESSED_DTTM
                                  __rule_id__ = RULE_ID
                                  __rule_desc__ = RULE_DESC
                                 )
                        )
      %end;

      ;

      set &ds_in. end = last;

      %if(&audit_flg. = Y) %then %do;
         length
            __measure_name__ $32.
            __measure_var_type__ $1.
            __sequence_no__ 8.
            __current_value__ 8.
            __previous_value__ 8.
            __delta_value__ 8.
            __weight_value__ 8.
            __current_txt_value__ $4096.
            __previous_txt_value__ $4096.
            __rule_id__ $32.
            __rule_desc__ $4096.
         ;

         format __processed_dttm__ datetime21.;
         __processed_dttm__ = &current_dttm.;

         /* Initialize the sequence number */
         __sequence_no__ = 0;
      %end;

      %if(%sysevalf(%superq(drop) ne, boolean)) %then %do;
         drop &drop.;
      %end;

      array affected_rows{&tot_rules.} 8. _temporary_;
      %do i = 1 %to &tot_rules.;

         if(&&where_clause_&i..) then do;

            %if(&audit_flg. = Y) %then %do;
               /* Update the sequence number for the current record every time it is hit by a rule */
               __sequence_no__ = __sequence_no__ + 1;

               /* Reset audit variables */
               call missing(__measure_var_type__
                            , __current_value__
                            , __previous_value__
                            , __delta_value__
                            , __weight_value__
                            , __current_txt_value__
                            , __previous_txt_value__
                            , __rule_id__
                            , __rule_desc__
                            );

               /* Save the value of the measure before the adjustment */
               __measure_name__ = "%upcase(&&measure_col_&i..)";
               __measure_var_type__ = "%core_get_vartype(&ds_in., &&measure_col_&i..)";
               %if(%core_get_vartype(&ds_in., &&measure_col_&i..) = N) %then
                  __previous_value__ = &&measure_col_&i..;
               %else
                  __previous_txt_value__ = &&measure_col_&i..;
               ;

               /* Save rule_id and rule_desc to ds_out_audit (lengths set in ddl file)*/
               __rule_id__ = "&&rule_id_&i..";
               __rule_desc__ = "&&rule_desc_&i..";
            %end;

            /* Even allocation */
            %if(&&allocation_method_&i.. = EVEN) %then %do;
               /* Relative adjustment */
               %if(&&adjustment_type_&i.. = RELATIVE) %then %do;
                  /* SUM aggregation */
                  %if(&&aggr_method_&i.. = SUM or &&aggr_method_&i.. = ) %then %do;
                     %if(&&weighted_aggr_flg_&i.. = Y) %then
                        /* Even allocation, Relative adjustment, Weighted SUM aggregation */
                        &&measure_col_&i.. = &&current_value_aggr_&i. * (1 + &&adj_value_&i..) / &&current_weight_sum_&i.;
                     %else
                        /* Even allocation, Relative adjustment, Simple SUM aggregation */
                        &&measure_col_&i.. = &&current_value_aggr_&i. * (1 + &&adj_value_&i..) / &&current_value_cnt_&i.;
                     ;
                  %end;
                  /* MEAN aggregation */
                  %else %if(&&aggr_method_&i.. = MEAN) %then %do;
                     /* Even allocation, Relative adjustment, MEAN aggregation (covers both simple and weighted average) */
                     &&measure_col_&i.. = &&current_value_aggr_&i. * (1 + &&adj_value_&i..);
                  %end;
                  /* MAX aggregation */
                  %else %if(&&aggr_method_&i.. = MAX) %then %do;
                     /* Even allocation, Relative adjustment, MAX aggregation */
                     &&measure_col_&i.. = min(&&current_value_aggr_&i. * (1 + &&adj_value_&i..), &&measure_col_&i..);
                  %end;
                  /* MIN aggregation */
                  %else %if(&&aggr_method_&i.. = MIN) %then %do;
                     /* Even allocation, Relative adjustment, MIN aggregation */
                     &&measure_col_&i.. = max(&&current_value_aggr_&i. * (1 + &&adj_value_&i..), &&measure_col_&i..);
                  %end;
               %end; /* %if(&&adjustment_type_&i.. = RELATIVE) */
               %else %if(&&adjustment_type_&i.. = INCREMENT) %then %do;
                  /* SUM aggregation */
                  %if(&&aggr_method_&i.. = SUM or &&aggr_method_&i.. = ) %then %do;
                     %if(&&weighted_aggr_flg_&i.. = Y) %then
                        /* Even allocation, Incremental adjustment, Weighted SUM aggregation */
                        &&measure_col_&i.. = %sysfunc(sum(&&current_value_aggr_&i., &&adj_value_&i.)) / &&current_weight_sum_&i.;
                     %else
                        /* Even allocation, Absolute adjustment, Simple SUM aggregation */
                        &&measure_col_&i.. = %sysfunc(sum(&&current_value_aggr_&i., &&adj_value_&i.)) / &&current_value_cnt_&i.;
                     ;
                  %end;
                  /* MEAN aggregation */
                  %else %if(&&aggr_method_&i.. = MEAN) %then %do;
                     /* Even allocation, Incremental adjustment, MEAN aggregation (covers both simple and weighted average) */
                     &&measure_col_&i.. = %sysfunc(sum(&&current_value_aggr_&i., &&adj_value_&i.));
                  %end;
                  /* MAX aggregation */
                  %else %if(&&aggr_method_&i.. = MAX) %then %do;
                     /* Even allocation, Incremental adjustment, MAX aggregation */
                     &&measure_col_&i.. = min(%sysfunc(sum(&&current_value_aggr_&i., &&adj_value_&i.)), &&measure_col_&i..);
                  %end;
                  /* MIN aggregation */
                  %else %if(&&aggr_method_&i.. = MIN) %then %do;
                     /* Even allocation, Incremental adjustment, MIN aggregation */
                     &&measure_col_&i.. = max(%sysfunc(sum(&&current_value_aggr_&i., &&adj_value_&i.)), &&measure_col_&i..);
                  %end;
               %end; /* %if(&&adjustment_type_&i.. = INCREMENT) */
               %else %do;
                  /* SUM aggregation */
                  %if(&&aggr_method_&i.. = SUM or &&aggr_method_&i.. = ) %then %do;
                     %if(&&weighted_aggr_flg_&i.. = Y) %then
                        /* Even allocation, Absolute adjustment, Weighted SUM aggregation */
                        &&measure_col_&i.. = &&adj_value_&i.. / &&current_weight_sum_&i.;
                     %else
                        /* Even allocation, Absolute adjustment, Simple SUM aggregation */
                        &&measure_col_&i.. = &&adj_value_&i.. / &&current_value_cnt_&i.;
                     ;
                  %end;
                  /* MEAN aggregation */
                  %else %if(&&aggr_method_&i.. = MEAN) %then %do;
                     /* Even allocation, Absolute adjustment, MEAN aggregation (covers both simple and weighted average) */
                     &&measure_col_&i.. = &&adj_value_&i..;
                  %end;
                  /* MAX aggregation */
                  %else %if(&&aggr_method_&i.. = MAX) %then %do;
                     /* Even allocation, Absolute adjustment, MAX aggregation */
                     &&measure_col_&i.. = min(&&adj_value_&i.., &&measure_col_&i..);
                  %end;
                  /* MIN aggregation */
                  %else %if(&&aggr_method_&i.. = MIN) %then %do;
                     /* Even allocation, Absolute adjustment, MIN aggregation */
                     &&measure_col_&i.. = max(&&adj_value_&i.., &&measure_col_&i..);
                  %end;
               %end; /* %else %if(&&adjustment_type_&i.. = ABSOLUTE) */
            %end; /* %if(&&allocation_method_&i.. = EVEN) */
            /* Proportional allocation */
            %else %if (&&allocation_method_&i.. = PROPORTIONAL) %then %do;
               /* Relative adjustment */
               %if(&&adjustment_type_&i.. = RELATIVE) %then %do;
                  /* SUM aggregation */
                  %if(&&aggr_method_&i.. = SUM or &&aggr_method_&i.. = ) %then %do;
                     %if(%sysevalf(%superq(weight_col_&i.) ne, boolean) and &&weighted_aggr_flg_&i.. = N) %then
                        /* Proportional allocation, Relative Adjustment, Simple SUM aggregation. Allocation is proportional to the weight column */
                        &&measure_col_&i.. = &&current_value_aggr_&i. * (1 + &&adj_value_&i.) * &&weight_col_&i.. / &&current_weight_sum_&i.;
                     %else
                        /* Proportional allocation, Relative Adjustment, Simple/Weighted SUM aggregation. Allocation is proportional to the measure column */
                        &&measure_col_&i.. = &&measure_col_&i.. * (1 + &&adj_value_&i.);
                     ;
                  %end;
                  /* MEAN aggregation */
                  %else %if(&&aggr_method_&i.. = MEAN) %then %do;
                     %if(%sysevalf(%superq(weight_col_&i.) ne, boolean) and &&weighted_aggr_flg_&i.. = N) %then
                        /* Proportional allocation, Relative Adjustment, Simple MEAN aggregation. Allocation is proportional to the weight column */
                        &&measure_col_&i.. = &&current_value_aggr_&i. * (1 + &&adj_value_&i.) * &&current_value_cnt_&i.. * &&weight_col_&i.. / &&current_weight_sum_&i.;
                     %else
                        /* Proportional allocation, Relative Adjustment, Simple/Weighted MEAN aggregation. Allocation is proportional to the measure column */
                        &&measure_col_&i.. = &&measure_col_&i.. * (1 + &&adj_value_&i.);
                     ;
                  %end;
                  /* MAX aggregation */
                  %else %if(&&aggr_method_&i.. = MAX) %then %do;
                     /* Proportional allocation, Relative adjustment, MAX aggregation */
                     &&measure_col_&i.. = min(&&current_value_aggr_&i. * (1 + &&adj_value_&i..), &&measure_col_&i..);
                  %end;
                  /* MIN aggregation */
                  %else %if(&&aggr_method_&i.. = MIN) %then %do;
                     /* Proportional allocation, Relative adjustment, MIN aggregation */
                     &&measure_col_&i.. = max(&&current_value_aggr_&i. * (1 + &&adj_value_&i..), &&measure_col_&i..);
                  %end;
               %end; /* %if(&&adjustment_type_&i.. = RELATIVE) */
               %else %if(&&adjustment_type_&i.. = INCREMENT) %then %do;
                  /* SUM aggregation */
                  %if(&&aggr_method_&i.. = SUM or &&aggr_method_&i.. = ) %then %do;
                     %if(%sysevalf(%superq(weight_col_&i.) ne, boolean) and &&weighted_aggr_flg_&i.. = N) %then
                        /* Proportional allocation, Incremental adjustment, Simple SUM aggregation. Allocation is proportional to the weight column. */
                        &&measure_col_&i.. = %sysfunc(sum(&&current_value_aggr_&i., &&adj_value_&i.)) * &&weight_col_&i.. / &&current_weight_sum_&i.;
                     %else
                        /* Proportional allocation, Incremental adjustment, Simple/Weighted SUM aggregation. Allocation is proportional to the measure column. */
                        &&measure_col_&i.. = %sysfunc(sum(&&current_value_aggr_&i., &&adj_value_&i.)) * &&measure_col_&i.. / &&current_value_aggr_&i.;
                     ;
                  %end;
                  /* MEAN aggregation */
                  %else %if(&&aggr_method_&i.. = MEAN) %then %do;
                     %if(%sysevalf(%superq(weight_col_&i.) ne, boolean) and &&weighted_aggr_flg_&i.. = N) %then
                        /* Proportional allocation, Incremental adjustment, Simple MEAN aggregation. Allocation is proportional to the weight column. */
                        &&measure_col_&i.. = %sysfunc(sum(&&current_value_aggr_&i., &&adj_value_&i.)) * &&current_value_cnt_&i.. * &&weight_col_&i.. / &&current_weight_sum_&i.;
                     %else
                        /* Proportional allocation, Incremental adjustment, Simple/Weighted MEAN aggregation. Allocation is proportional to the measure column. */
                        &&measure_col_&i.. = %sysfunc(sum(&&current_value_aggr_&i., &&adj_value_&i.)) * &&measure_col_&i.. / &&current_value_aggr_&i.;
                     ;
                  %end;
                  /* MAX aggregation */
                  %else %if(&&aggr_method_&i.. = MAX) %then %do;
                     /* Proportional allocation, Incremental adjustment, MAX aggregation */
                     &&measure_col_&i.. = min(%sysfunc(sum(&&current_value_aggr_&i., &&adj_value_&i.)), &&measure_col_&i..);
                  %end;
                  /* MIN aggregation */
                  %else %if(&&aggr_method_&i.. = MIN) %then %do;
                     /* Proportional allocation, Incremental adjustment, MIN aggregation */
                     &&measure_col_&i.. = max(%sysfunc(sum(&&current_value_aggr_&i., &&adj_value_&i.)), &&measure_col_&i..);
                  %end;
               %end; /* %if(&&adjustment_type_&i.. = INCREMENT) */
               %else %do;
                  /* SUM aggregation */
                  %if(&&aggr_method_&i.. = SUM or &&aggr_method_&i.. = ) %then %do;
                     %if(%sysevalf(%superq(weight_col_&i.) ne, boolean) and &&weighted_aggr_flg_&i.. = N) %then
                        /* Proportional allocation, Absolute adjustment, Simple SUM aggregation. Allocation is proportional to the weight column. */
                        &&measure_col_&i.. = &&adj_value_&i. * &&weight_col_&i.. / &&current_weight_sum_&i.;
                     %else
                        /* Proportional allocation, Absolute adjustment, Simple/Weighted SUM aggregation. Allocation is proportional to the measure column. */
                        &&measure_col_&i.. = &&adj_value_&i. * &&measure_col_&i.. / &&current_value_aggr_&i.;
                     ;
                  %end;
                  /* MEAN aggregation */
                  %else %if(&&aggr_method_&i.. = MEAN) %then %do;
                     %if(%sysevalf(%superq(weight_col_&i.) ne, boolean) and &&weighted_aggr_flg_&i.. = N) %then
                        /* Proportional allocation, Absolute adjustment, Simple MEAN aggregation. Allocation is proportional to the weight column. */
                        &&measure_col_&i.. = &&adj_value_&i. * &&current_value_cnt_&i.. * &&weight_col_&i.. / &&current_weight_sum_&i.;
                     %else
                        /* Proportional allocation, Absolute adjustment, Simple/Weighted MEAN aggregation. Allocation is proportional to the measure column. */
                        &&measure_col_&i.. = &&adj_value_&i. * &&measure_col_&i.. / &&current_value_aggr_&i.;
                     ;
                  %end;
                  /* MAX aggregation */
                  %else %if(&&aggr_method_&i.. = MAX) %then %do;
                     /* Proportional allocation, Absolute adjustment, MAX aggregation */
                     &&measure_col_&i.. = min(&&adj_value_&i.., &&measure_col_&i..);
                  %end;
                  /* MIN aggregation */
                  %else %if(&&aggr_method_&i.. = MIN) %then %do;
                     /* Proportional allocation, Absolute adjustment, MIN aggregation */
                     &&measure_col_&i.. = max(&&adj_value_&i.., &&measure_col_&i..);
                  %end;
               %end; /* %else %if(&&adjustment_type_&i.. = ABSOLUTE) */
            %end; /* %else %if (&&allocation_method_&i.. = PROPORTIONAL) */
            /* Individual adjustment */
            %else %do;
               %if(&&adjustment_type_&i.. = RELATIVE) %then %do;
                  /* Relative adjustment */
                  &&measure_col_&i.. = &&measure_col_&i.. * (1 + &&adj_value_&i..);
               %end;
               %else %do;
                  /* MAX */
                  %if(&&aggr_method_&i.. = MAX) %then %do;
                     /* Individual adjustment, Absolute, MAX */
                     &&measure_col_&i.. = min(&&adj_value_&i.., &&measure_col_&i..);
                  %end;
                  /* MIN */
                  %else %if(&&aggr_method_&i.. = MIN) %then %do;
                     /* Individual adjustment, Absolute, MIN */
                     &&measure_col_&i.. = max(&&adj_value_&i.., &&measure_col_&i..);
                  %end;
                  %else %do;
                     %if(&&adjustment_type_&i.. = INCREMENT) %then %do;
                        /* Individual adjustment, Incremental */
                        &&measure_col_&i.. = sum(&&measure_col_&i.., &&adj_value_&i.);
                     %end;
                     %else %do;
                        /* Individual adjustment, Absolute */
                        %if(%core_get_vartype(&ds_in., &&measure_col_&i..) = N) %then
                           &&measure_col_&i.. = &&adj_value_&i.;
                        %else
                           &&measure_col_&i.. = "&&adj_value_&i.";
                        ;
                     %end;
                  %end;
               %end; /* %if(&&adjustment_type_&i.. = ABSOLUTE or INCREMENT) */
            %end;

            /* Count the number of rows affected by this rule */
            affected_rows[&i.] = sum(affected_rows[&i.], 1);

            %if(&audit_flg. = Y) %then %do;
               /* Save the new value */
               %if(%core_get_vartype(&ds_in., &&measure_col_&i..) = N) %then %do;
                  __current_value__ = &&measure_col_&i..;

                  /*  Compute delta */
                  __delta_value__ = sum(__current_value__, - __previous_value__);

                  %if(&&weighted_aggr_flg_&i.. = Y) %then %do;
                     /* Store the value of the weight the was used */
                     __weight_value__ = &&weight_col_&i..;
                  %end;
               %end;
               %else
                  __current_txt_value__ = &&measure_col_&i..;
               ;

               /* Output record to the audit table */
               output &ds_out_audit.;

               %if(&var_dependency_flg. = Y) %then %do;
                  /* Process dependencies */
                  %do j = 1 %to &&dependent_var_cnt_&i..;
                     call missing(__measure_name__
                                  , __measure_var_type__
                                  , __current_txt_value__
                                  , __current_value__
                                  , __previous_value__
                                  , __previous_txt_value__
                                  , __delta_value__
                                  , __weight_value__
                                  );
                     __measure_name__ = "%upcase(&&dependent_var_&i._&j..)";
                     __measure_var_type__ = "%core_get_vartype(&ds_in., &&dependent_var_&i._&j..)";
                     %if(%core_get_vartype(&ds_in., &&dependent_var_&i._&j..) = N) %then %do;
                        /* Copy value of the dependent variable */
                        __previous_value__ = &&dependent_var_&i._&j..;
                        /* Modify the dependent variable */
                        &&dependent_var_&i._&j.. = &&expression_txt_&i._&j..;
                        /* Copy new value of the dependent variable */
                        __current_value__ = &&dependent_var_&i._&j..;
                        /*  Compute delta */
                        __delta_value__ = sum(__current_value__, - __previous_value__);
                     %end;
                     %else %do;
                        /* Copy value of the dependent variable */
                        __previous_txt_value__ = &&dependent_var_&i._&j..;
                        /* Modify the dependent variable */
                        &&dependent_var_&i._&j.. = &&expression_txt_&i._&j..;
                        /* Copy new value of the dependent variable */
                        __current_txt_value__ = &&dependent_var_&i._&j..;
                     %end;

                     /* Output record to the audit table */
                     output &ds_out_audit.;

                  %end; /* %do j = 1 %to &&dependent_var_cnt_&i.. */

               %end; /* %if(&var_dependency_flg. = Y) */

            %end; /* %if(&audit_flg. = Y) */

         end; /* if(&&where_clause_&i..) */

      %end; /* %do i = 1 %to &tot_rules. */

      %if(&main_output_flg. = Y) %then %do;
         output &ds_out.;
      %end;

      if last then do;
         drop _i_;
         do _i_ = 1 to dim(affected_rows);
            call symputx(cats("affected_row_cnt_", put(_i_, 8.)), coalesce(affected_rows[_i_],0), "L");
         end;
         /* Total number of records */
         call symputx("total_row_cnt", _N_, "L");
      end;
   run;

   data &ds_out_rule_summary.;
      length
         RULE_CONDITION $10000.
         ADJUSTMENT_VALUE $32000.
         AFFECTED_ROW_CNT 8.
         TOTAL_ROW_CNT 8.
      ;
      set &rule_def_ds.
         %if(%core_get_vartype(&rule_def_ds., ADJUSTMENT_VALUE) = N) %then
            (rename=(ADJUSTMENT_VALUE=adj_value_num));
         ;
      TOTAL_ROW_CNT = &total_row_cnt.;
      %if(&tot_rules. > 0) %then %do;
         RULE_CONDITION = symget(cats("where_clause_", put(_N_, 8.)));
         AFFECTED_ROW_CNT = symget(cats("affected_row_cnt_", put(_N_, 8.)));
      %end;
      /* Convert ADJUSTMENT_VALUE to character */
      %if(%core_get_vartype(&rule_def_ds., ADJUSTMENT_VALUE) = N) %then
         ADJUSTMENT_VALUE=strip(put(adj_value_num, comma25.4));
      ;
   run;
%mend;