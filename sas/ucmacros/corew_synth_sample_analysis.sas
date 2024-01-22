/*
 Copyright (C) 2022-2024 SAS Institute Inc. Cary, NC, USA
*/

/**
   \file
\anchor corew_synth_sample_generation
   \brief   Use random sampling from a sampling frame and/or portfolio to meet projections for a target variable

   \param [in] inSamplingFrame input SAS (2-level name) or CAS (1-level name) table that we will draw samples from.
   \param [in] inPortfolio input SAS (2-level name) or CAS (1-level name) portfolio table.  This table is used as the sampling frame in the call to corew_synth_sample_analysis
      to eliminate instruments.  If not provided, it is set to inSamplingFrame.
   \param [in] inCasLib Name of the input CAS library, if inSamplingFrame/inPortfolio are CAS tables
   \param [in] ds_in_projection input SAS dataset that contains projections by segments in the sampling frame for target variables
      over 1 or more horizons (and optionally for different scenarios).
   \param [in] ds_in_allocation (optional) input SAS dataset that contains allocation percentages for segments WITHIN the ds_in_projection segments.
   \param [in] target_var Target variable name.  Sampling is done only for these rows in ds_in_projection.  Samples are drawn until the projection
      amounts for this variable are hit.
   \param [in] relativeValue_var: name of the relative value variable in ds_in_projection.  This is the projection amount of the target variable
      that needs reached for each horizion.  (Default: relativeValue)
   \param [in] alloc_by_vars Space-separated list of allocation variables in the ds_in_allocation that contain allocation percentages within each segment.  (Default: instid)
      Note: Required if ds_in_allocation is provided.
   \param [in] projection_by_vars Space-separated list of segmentation variables in ds_in_projection.  (Optional - if not provided, we assume projections are top-level)
      Note: Sampling will by done by these segments, so these segment variables must exist in ds_in_projection and ds_in_allocation (if provided)
   \param [in] percentage_var Name of the allocation percentage variable in ds_in_allocation (Default: allocationWeight)
   \param [in] horizon_var Name of the horizon variable in ds_in_projection (Default: activationHorizon)
   \param [in] scenario_var Name of the scenario variable in ds_in_projection and ds_in_allocation (Default: scenario)
   \param [in] segmentFilterVar Name of the variable in ds_in_projection and ds_in_allocation that contains a SAS where-clause filter string to identify
      a specific segment.  The filter values for this variable should use single quotes (not double quotes). (Default: segmentFilterSas)
         Ex: segmentFilterSas="INSTTYPE='HELOCs'"
   \param [in] id_var The name of the key variable identifying each row (Default: instid)
   \param [in] id_var_prefix The prefix for the identifier for each new sample draw in the generation_type=GEN case (Default: synth_)
   \param [in] id_var_fmt The format of the numeric identifier added to each new sample in the generation_type=GEN case (Default: z8.)
   \param [in] id_reporting_date The formatted as-of date to add to the identifier for each new sample in the generation_type=GEN case.
   \param [in] custom_seed Seed used in PROC SURVEYSELECT.  You will get the same samples draw for a given custom seed every time.
   \param [in] reproducible_flag If Y, if inSamplingFrame is a CAS table, move each segment into SAS before sampling.  This is needed since
      PROC SURVEYSELECT can only guarantee reproducibility (for a given custom_seed) if the sampling frame is a SAS table (not CAS).  (Y/N) (Default: N)
      Note that this can add runtime, since inSamplingFrame must be moved into SAS in this case.
   \param [in] debug Whether or not to output extra debug info to the log (true/false) (Default: false)
   \param [in] unity_analysis_flg If true, a single isntrument is drawn from the sampling frame and its target variable
      value is set to the projection for that segment, scenario, and horizon (true/false) (Default: false)
   \param [in] simulation_factor A multiplier to the estimated number of samples drawn for each projection.  The estimated number of samples needed
      to match a projection for a given segment, scenario, horizon, and allocation variable within that segment is:
            _NSIZE_ = simulation_factor.*abs(relativeValue_var)/<targetVarMean_segment>)*percentage_var;
      If simulation_factor is higher, _NSIZE_ is higher.  This means more samples will be drawn so we'll be more likely to draw enough
      rows to meet the projection.  However, it could also mean additional runtime. (Default 1.5)
   \param [in] max_sampling_reps The maximum number of times to try and meet remaining projections that have not been reached
      for a given segment, scenario, horizon, and allocation variable.  (We sample, select observations from the sample
      until the target projection is reached, and if it isn't repeat a max of max_sampling_reps times.) (Default: 5)
   \param [in] projection_epsilon A percentage under a projection that is considered sufficient (Default: .01)
      Ex: projection_epsilon=.01 --> drawing (1 - .01)*projection is considered meeting the projection.
   \param [in] gen_flg Flag to say if sampling should be done to meet positive projections (Y/N) (Default: Y)
   \param [in] elim_flg Flag to say if sampling should be done to meet negative projections (Y/N) (Default: Y)
   \param [in] casSessionName Name of an existing CAS session where CAS operations should run
   \param [out] outCasLib Name of the output CAS library, if outGeneration/outElimination are CAS tables.
   \param [out] outGeneration Name of the output SAS (2-level name) or CAS (1-level name) table containing the selected sample instruments to meet positive projections
   \param [out] outElimination Name of the output SAS (2-level name) or CAS (1-level name) table containing the selected sample instruments to meet negative projections

   \details
   This macro calls corew_synth_sample_generations to perform random sampling to meet both positive projections (generation) and negative
   projections (elimination) defined in an input SAS dataset (ds_in_projection).  The projections are given by segment (segmentFiterSas),
   scenario, and horizon.

   Generation is done on an input sampling frame (inSamplingFrame).  This could be a portfolio, a sample of a portfolio, or any
   table that you wish to draw samples from.  Elimination is generally done on the actual portfolio (inPortfolio) so that real
   observations are actually eliminated.  However, it can be done on the sampling frame as well (inSamplingFrame is the fallback
   if inPortfolio is not provided).

   The projections within each segment and scenario can be allocated by percentages for additional variables given in an input allocation
   dataset (ds_in_allocation).

   <b>Example:</b>

   1) Set up the environment (set SASAUTOS and required LUA libraries).  Assumes the spre folder is under /riskcirruscore/core/code_libraries/release-core-{cadence-version}
   \code
      %let core_root_path=/riskcirruscore/core/code_libraries/release-core-%SYSGET(SAS_RISK_CIRRUS_CADENCE);
      option insert = (
         SASAUTOS = (
            "&core_root_path./spre/sas/ucmacros"
            )
         );
      filename LUAPATH ("&core_root_path./spre/lua");
   \endcode

   2) Run the macro to generate enough samples to meet projections
   \code
      %corew_synth_sample_analysis(inSamplingFrame = credit_portfolio_sample
                                 , inPortfolio = credit_portfolio
                                 , ds_in_projection = _tmp_analysis_proj_plus_
                                 , ds_in_allocation = _tmp_alloc_details_
                                 , outGeneration = synthetic_positions
                                 , outElimination = eliminated_positions
                                 , target_var = unpaid_balance_amt
                                 , alloc_by_vars = CURRENCY
                                 , custom_seed = 12345
                                 , debug = true
                                 );

    \endcode

   \author  SAS Institute Inc.
   \date    2023
*/


%macro corew_synth_sample_analysis(inSamplingFrame =
                                 , inPortfolio =
                                 , inCasLib =
                                 , ds_in_projection =
                                 , ds_in_allocation =
                                 , outGeneration = synthetic_positions
                                 , outElimination = eliminated_positions
                                 , outCasLib =
                                 , target_var =
                                 , alloc_by_vars = instid
                                 , projection_by_vars =
                                 , relativeValue_var = relativeValue
                                 , percentage_var = allocationWeight
                                 , horizon_var = activationHorizon
                                 , scenario_var = scenario
                                 , segmentFilterVar = segmentFilterSas
                                 , id_var = instid
                                 , id_var_prefix = synth_
                                 , id_var_fmt = z8.
                                 , id_reporting_date =
                                 , custom_seed =
                                 , reproducible_flag =
                                 , debug = false
                                 , unity_analysis_flg = false
                                 , simulation_factor = 1.5
                                 , max_sampling_reps = 5
                                 , projection_epsilon = .01
                                 , gen_flg = Y
                                 , elim_flg = Y
                                 , casSessionName = casauto
                                 );

   %local   inCasLibref outCasLibref
            csv_proj_by_vars csv_alloc_by_vars
            id_var_length;

   %if (%sysevalf(%superq(inSamplingFrame) eq, boolean)) %then %do;
      %put ERROR: inSamplingFrame is required.;
      %return;
   %end;

   %if (%sysevalf(%superq(inPortfolio) eq, boolean)) %then
      %let inPortfolio=&inSamplingFrame.;

   %if (%sysevalf(%superq(ds_in_projection) eq, boolean)) %then %do;
      %put ERROR: ds_in_projection is required.;
      %return;
   %end;

   %if (%sysevalf(%superq(target_var) eq, boolean)) %then %do;
      %put ERROR: target_var is required.;
      %return;
   %end;

   %if (%sysevalf(%superq(inCasLib) ne, boolean)) %then %do;
      %let inCasLibref = %rsk_get_unique_ref(type = lib, engine = cas, args = caslib="&inCasLib." sessref=&casSessionName.);
      %rsk_varattr_cas(caslib=&inCasLib., castable=&inPortfolio., cas_session_name=&casSessionName.
                       , var=&id_var., attr=length, out_var=id_var_length);
   %end;

   %if (%sysevalf(%superq(outCasLib) ne, boolean)) %then
      %let outCasLibref = %rsk_get_unique_ref(type = lib, engine = cas, args = caslib="&outCasLib." sessref=&casSessionName.);

   /* If an allocation variable (in alloc_by_vars) is also a projection variable (in projection_by_vars), remove the variable
   from the allocation variable list (alloc_by_vars). */
   %let alloc_by_vars=%rsk_filter_mvar_list(mvar_list=&alloc_by_vars., filter_mvar_list=&projection_by_vars., filter_method=drop);

   /* Projections dataset preparation */
   data _tmp_analysis_proj_plus_ _tmp_analysis_proj_minus_;
      set &ds_in_projection. (where=(upcase(targetVar)=upcase("&target_var.")));

      /* Add the scenario column if it doesn't exist */
      %if (not %rsk_varexist(&ds_in_projection., &scenario_var.)) %then %do;
         length &scenario_var. $200;
      %end;

      /* Add a dummy projection by-variable if no projection variables were provided (top-level projection) */
      %if (%sysevalf(%superq(projection_by_vars) eq, boolean)) %then  %do;
         __tmp_proj_var__="__temp_proj_value__";
         %let projection_by_vars=__tmp_proj_var__;
      %end;

      /* Split the input projections into 2 datasets - 1 with all positive projections, 1 with all negative */
      if &relativeValue_var. NE . and &relativeValue_var.>0 then do;
         output _tmp_analysis_proj_plus_;
      end;
      else if &relativeValue_var. NE . and &relativeValue_var.<0 then do;
         output _tmp_analysis_proj_minus_;
      end;
   run;

   /* Allocation dataset preparation (if provided) */
   %if (%sysevalf(%superq(ds_in_allocation) ne, boolean)) %then  %do;

      %if (%sysevalf(%superq(alloc_by_vars) eq, boolean)) %then %do;
         %put ERROR: alloc_by_vars is required if ds_in_allocation is provided.;
         %return;
      %end;

      /* Add the scenario column if it doesn't exist */
      data _tmp_alloc_details_;
         set &ds_in_allocation.;
         %if (not %rsk_varexist(&ds_in_allocation., &scenario_var.)) %then %do;
            length &scenario_var. $200;
         %end;
         
      /* Add the projection_by_vars column if it doesn't exist */   
         %if (not %rsk_varexist(&ds_in_allocation., &projection_by_vars.)) %then %do;
            length &projection_by_vars. $200;
         %end;
      run;

      /* Reweight (normalize) the allocation percentages by the BEP segment variables, in case it's needed */
      %let csv_proj_by_vars = %sysfunc(prxchange(s/\s+/%str(, )/i, -1, &projection_by_vars.));
      %let csv_alloc_by_vars = %sysfunc(prxchange(s/\s+/%str(, )/i, -1, &alloc_by_vars.));
      proc sql noprint;
         create table _tmp_alloc_details_normalized_ as
         select &csv_proj_by_vars., &csv_alloc_by_vars., &scenario_var., &percentage_var./sum(&percentage_var) as &percentage_var
         from _tmp_alloc_details_
         group by &csv_proj_by_vars., &scenario_var.
         ;
      quit;

      %let ds_in_allocation = _tmp_alloc_details_normalized_;

   %end;

   /* Perform instrument generation, if we have any positive projections and if requested */
   %if %rsk_attrn(_tmp_analysis_proj_plus_, nobs) > 0 and %upcase("&gen_flg.") = "Y" %then %do;

      %corew_synth_sample_generation( inSamplingFrame = %sysfunc(ifc("&inCasLib." ne "", &inCasLibref..&inSamplingFrame., &inSamplingFrame.))
                                    , ds_in_projection = _tmp_analysis_proj_plus_
                                    , ds_in_allocation = &ds_in_allocation.
                                    , outSample = work.synthetic_instruments
                                    , target_var = &target_var.
                                    , alloc_by_vars = &alloc_by_vars.
                                    , relativeValue_var = &relativeValue_var.
                                    , percentage_var = &percentage_var.
                                    , horizon_var = &horizon_var.
                                    , scenario_var = &scenario_var.
                                    , segmentFilterVar = &segmentFilterVar.
                                    , id_var = &id_var.
                                    , id_var_prefix = &id_var_prefix.
                                    , id_var_fmt = &id_var_fmt.
                                    , id_reporting_date = &id_reporting_date.
                                    , id_var_length = &id_var_length.
                                    , custom_seed = &custom_seed.
                                    , reproducible_flag = &reproducible_flag.
                                    , debug = &debug.
                                    , unity_analysis_flg = &unity_analysis_flg.
                                    , simulation_factor = &simulation_factor.
                                    , max_sampling_reps = &max_sampling_reps.
                                    , projection_epsilon = &projection_epsilon.
                                    );

      /* Create the &outCasLib..&outGeneration. output CAS table */
      %if "&outCasLib." ne "" %then %do;
         %core_cas_upload_and_convert(inLib = work
                                    , inTable = synthetic_instruments
                                    , inBasisCasLib = &inCasLib.
                                    , inBasisCasTable = &inSamplingFrame.
                                    , encoding_adjustment = Y
                                    , outLib = &outCasLib.
                                    , outTable = &outGeneration.
                                    , casSessionName = &casSessionName.
                                    );
      %end;

   %end;

   /* Perform instrument elimination, if we have any negative projections and if requested */
   %if %rsk_attrn(_tmp_analysis_proj_minus_, nobs) > 0 and %upcase("&elim_flg.") = "Y" %then %do;

      %corew_synth_sample_elimination(inSamplingFrame = %sysfunc(ifc("&inCasLib." ne "", &inCasLibref..&inPortfolio., &inPortfolio.))
                                    , ds_in_projection = _tmp_analysis_proj_minus_
                                    , outSample = work.eliminated_instruments
                                    , target_var = &target_var.
                                    , relativeValue_var = &relativeValue_var.
                                    , percentage_var = &percentage_var.
                                    , horizon_var = &horizon_var.
                                    , scenario_var = &scenario_var.
                                    , segmentFilterVar = &segmentFilterVar.
                                    , id_var = &id_var.
                                    , id_var_length = &id_var_length.
                                    , custom_seed = &custom_seed.
                                    , reproducible_flag = &reproducible_flag.
                                    , debug = &debug.
                                    , simulation_factor = &simulation_factor.
                                    , max_sampling_reps = &max_sampling_reps.
                                    , projection_epsilon = &projection_epsilon.
                                    );

      /* Create the &outCasLib..&outElimination. output CAS table */
      %if "&outCasLib." ne "" %then %do;
         %core_cas_upload_and_convert(inLib = work
                                    , inTable = eliminated_instruments
                                    , inBasisCasLib = &inCasLib.
                                    , inBasisCasTable = &inPortfolio.
                                    , encoding_adjustment = Y
                                    , outLib = &outCasLib.
                                    , outTable=  &outElimination.
                                    , casSessionName = &casSessionName.
                                    );
      %end;

   %end;

   /* Clean-up */
   %if %upcase(&debug.) ne TRUE %then %do;
      proc datasets library = work nolist nodetails nowarn;
         delete
            _tmp_analysis_proj_plus_
            _tmp_analysis_proj_minus_
            _tmp_alloc_details_
            _tmp_alloc_details_normalized_
            ;
      quit;

      %if (%sysevalf(%superq(inCasLib) ne, boolean)) %then %do;
         libname &inCasLibref. clear;
      %end;

      %if (%sysevalf(%superq(outCasLib) ne, boolean)) %then %do;
         libname &outCasLibref. clear;
      %end;

   %end;

%mend;