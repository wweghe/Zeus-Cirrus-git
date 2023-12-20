%macro rsk_filter_mvar_list(mvar_list=, filter_mvar_list=, filter_method=keep);

   %local filtered_list current_var current_filter_var i j;

   %let filter_method=%lowcase(&filter_method.);      /* keep or drop */

   %let filtered_list = ;
   %do i = 1 %to %sysfunc(countw(&mvar_list., " "));
      %let current_var = %scan(&mvar_list., &i., " ");

      %do j = 1 %to %sysfunc(countw(&filter_mvar_list., " "));
         %let current_filter_var = %scan(&filter_mvar_list., &j., " ");

         %if %upcase("&current_var.") = %upcase("&current_filter_var.") %then %do;
            %if "&filter_method."="keep" %then
               %let filtered_list = &filtered_list. &current_var.;
            %goto leave;
         %end;

      %end;

      %if "&filter_method."="drop" %then
         %let filtered_list = &filtered_list. &current_var.;

      %leave:

   %end;

   %bquote(&filtered_list.)

%mend;