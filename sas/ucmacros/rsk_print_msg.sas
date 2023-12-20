/*
 Copyright (C) 2022-2023 SAS Institute Inc. Cary, NC, USA
*/

/**
   \file 
\anchor rsk_print_msg
   \brief The macro looks up a message by key in the message data set
          and writes the localized message text to the log.

  <b> Identified Inputs </b>

   \param[in]  key                Massage key value

   \param[in]  s1                 Message optional value

   \param[in]  s2                 Message optional value

   \param[in]  s3                 Message optional value

   \param[in]  s4                 Message optional value

   \param[in]  s5                 Message optional value

   \param[in]  s6                 Message optional value

   \param[in]  s7                 Message optional value

   \ingroup CommonAnalytics

   \author SAS Institute INC.
   \date 2015

 */
 %macro rsk_print_msg(KEY, s1, s2, s3, s4, s5, s6, s7);

   %local TEXT;
   %let TEXT = %rsk_get_msg(&KEY, &s1, &s2, &s3, &s4, &s5, &s6, &s7);
   %put &TEXT;

%mend rsk_print_msg;