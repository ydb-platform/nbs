TExecFunc arrow_RI_FKey_cascade_del() { return MakeIndirectExec<true, true>(&RI_FKey_cascade_del); }
TExecFunc arrow_RI_FKey_cascade_upd() { return MakeIndirectExec<true, true>(&RI_FKey_cascade_upd); }
TExecFunc arrow_RI_FKey_check_ins() { return MakeIndirectExec<true, true>(&RI_FKey_check_ins); }
TExecFunc arrow_RI_FKey_check_upd() { return MakeIndirectExec<true, true>(&RI_FKey_check_upd); }
TExecFunc arrow_RI_FKey_noaction_del() { return MakeIndirectExec<true, true>(&RI_FKey_noaction_del); }
TExecFunc arrow_RI_FKey_noaction_upd() { return MakeIndirectExec<true, true>(&RI_FKey_noaction_upd); }
TExecFunc arrow_RI_FKey_restrict_del() { return MakeIndirectExec<true, true>(&RI_FKey_restrict_del); }
TExecFunc arrow_RI_FKey_restrict_upd() { return MakeIndirectExec<true, true>(&RI_FKey_restrict_upd); }
TExecFunc arrow_RI_FKey_setdefault_del() { return MakeIndirectExec<true, true>(&RI_FKey_setdefault_del); }
TExecFunc arrow_RI_FKey_setdefault_upd() { return MakeIndirectExec<true, true>(&RI_FKey_setdefault_upd); }
TExecFunc arrow_RI_FKey_setnull_del() { return MakeIndirectExec<true, true>(&RI_FKey_setnull_del); }
TExecFunc arrow_RI_FKey_setnull_upd() { return MakeIndirectExec<true, true>(&RI_FKey_setnull_upd); }
TExecFunc arrow_arraycontjoinsel() { return MakeIndirectExec<true, true>(&arraycontjoinsel); }
TExecFunc arrow_arraycontsel() { return MakeIndirectExec<true, true>(&arraycontsel); }
TExecFunc arrow_dsynonym_init() { return MakeIndirectExec<true, true>(&dsynonym_init); }
TExecFunc arrow_dsynonym_lexize() { return MakeIndirectExec<true, true>(&dsynonym_lexize); }
TExecFunc arrow_elem_contained_by_multirange() { return MakeIndirectExec<true, true>(&elem_contained_by_multirange); }
TExecFunc arrow_gist_box_consistent() { return MakeIndirectExec<true, true>(&gist_box_consistent); }
TExecFunc arrow_gist_box_distance() { return MakeIndirectExec<true, true>(&gist_box_distance); }
TExecFunc arrow_gist_box_penalty() { return MakeIndirectExec<true, true>(&gist_box_penalty); }
TExecFunc arrow_gist_box_picksplit() { return MakeIndirectExec<true, true>(&gist_box_picksplit); }
TExecFunc arrow_gist_box_same() { return MakeIndirectExec<true, true>(&gist_box_same); }
TExecFunc arrow_gist_box_union() { return MakeIndirectExec<true, false>(&gist_box_union); }
TExecFunc arrow_gist_circle_compress() { return MakeIndirectExec<true, true>(&gist_circle_compress); }
TExecFunc arrow_gist_circle_consistent() { return MakeIndirectExec<true, true>(&gist_circle_consistent); }
TExecFunc arrow_gist_circle_distance() { return MakeIndirectExec<true, true>(&gist_circle_distance); }
TExecFunc arrow_gist_point_compress() { return MakeIndirectExec<true, true>(&gist_point_compress); }
TExecFunc arrow_gist_point_consistent() { return MakeIndirectExec<true, true>(&gist_point_consistent); }
TExecFunc arrow_gist_point_distance() { return MakeIndirectExec<true, true>(&gist_point_distance); }
TExecFunc arrow_gist_point_fetch() { return MakeIndirectExec<true, true>(&gist_point_fetch); }
TExecFunc arrow_gist_point_sortsupport() { return MakeIndirectExec<true, true>(&gist_point_sortsupport); }
TExecFunc arrow_gist_poly_compress() { return MakeIndirectExec<true, true>(&gist_poly_compress); }
TExecFunc arrow_gist_poly_consistent() { return MakeIndirectExec<true, true>(&gist_poly_consistent); }
TExecFunc arrow_gist_poly_distance() { return MakeIndirectExec<true, true>(&gist_poly_distance); }
TExecFunc arrow_hash_multirange() { return MakeIndirectExec<true, true>(&hash_multirange); }
TExecFunc arrow_hash_multirange_extended() { return MakeIndirectExec<true, true>(&hash_multirange_extended); }
TExecFunc arrow_multirange_adjacent_multirange() { return MakeIndirectExec<true, true>(&multirange_adjacent_multirange); }
TExecFunc arrow_multirange_adjacent_range() { return MakeIndirectExec<true, true>(&multirange_adjacent_range); }
TExecFunc arrow_multirange_after_multirange() { return MakeIndirectExec<true, true>(&multirange_after_multirange); }
TExecFunc arrow_multirange_after_range() { return MakeIndirectExec<true, true>(&multirange_after_range); }
TExecFunc arrow_multirange_before_multirange() { return MakeIndirectExec<true, true>(&multirange_before_multirange); }
TExecFunc arrow_multirange_before_range() { return MakeIndirectExec<true, true>(&multirange_before_range); }
TExecFunc arrow_multirange_cmp() { return MakeIndirectExec<true, true>(&multirange_cmp); }
TExecFunc arrow_multirange_constructor0() { return MakeIndirectExec<true, false>(&multirange_constructor0); }
TExecFunc arrow_multirange_constructor1() { return MakeIndirectExec<true, false>(&multirange_constructor1); }
TExecFunc arrow_multirange_contained_by_multirange() { return MakeIndirectExec<true, true>(&multirange_contained_by_multirange); }
TExecFunc arrow_multirange_contained_by_range() { return MakeIndirectExec<true, true>(&multirange_contained_by_range); }
TExecFunc arrow_multirange_contains_elem() { return MakeIndirectExec<true, true>(&multirange_contains_elem); }
TExecFunc arrow_multirange_contains_multirange() { return MakeIndirectExec<true, true>(&multirange_contains_multirange); }
TExecFunc arrow_multirange_contains_range() { return MakeIndirectExec<true, true>(&multirange_contains_range); }
TExecFunc arrow_multirange_empty() { return MakeIndirectExec<true, true>(&multirange_empty); }
TExecFunc arrow_multirange_eq() { return MakeIndirectExec<true, true>(&multirange_eq); }
TExecFunc arrow_multirange_ge() { return MakeIndirectExec<true, true>(&multirange_ge); }
TExecFunc arrow_multirange_gt() { return MakeIndirectExec<true, true>(&multirange_gt); }
TExecFunc arrow_multirange_in() { return MakeIndirectExec<true, false>(&multirange_in); }
TExecFunc arrow_multirange_intersect() { return MakeIndirectExec<true, false>(&multirange_intersect); }
TExecFunc arrow_multirange_intersect_agg_transfn() { return MakeIndirectExec<true, false>(&multirange_intersect_agg_transfn); }
TExecFunc arrow_multirange_le() { return MakeIndirectExec<true, true>(&multirange_le); }
TExecFunc arrow_multirange_lower() { return MakeIndirectExec<true, true>(&multirange_lower); }
TExecFunc arrow_multirange_lower_inc() { return MakeIndirectExec<true, true>(&multirange_lower_inc); }
TExecFunc arrow_multirange_lower_inf() { return MakeIndirectExec<true, true>(&multirange_lower_inf); }
TExecFunc arrow_multirange_lt() { return MakeIndirectExec<true, true>(&multirange_lt); }
TExecFunc arrow_multirange_minus() { return MakeIndirectExec<true, false>(&multirange_minus); }
TExecFunc arrow_multirange_ne() { return MakeIndirectExec<true, true>(&multirange_ne); }
TExecFunc arrow_multirange_out() { return MakeIndirectExec<true, false>(&multirange_out); }
TExecFunc arrow_multirange_overlaps_multirange() { return MakeIndirectExec<true, true>(&multirange_overlaps_multirange); }
TExecFunc arrow_multirange_overlaps_range() { return MakeIndirectExec<true, true>(&multirange_overlaps_range); }
TExecFunc arrow_multirange_overleft_multirange() { return MakeIndirectExec<true, true>(&multirange_overleft_multirange); }
TExecFunc arrow_multirange_overleft_range() { return MakeIndirectExec<true, true>(&multirange_overleft_range); }
TExecFunc arrow_multirange_overright_multirange() { return MakeIndirectExec<true, true>(&multirange_overright_multirange); }
TExecFunc arrow_multirange_overright_range() { return MakeIndirectExec<true, true>(&multirange_overright_range); }
TExecFunc arrow_multirange_recv() { return MakeIndirectExec<true, false>(&multirange_recv); }
TExecFunc arrow_multirange_send() { return MakeIndirectExec<true, false>(&multirange_send); }
TExecFunc arrow_multirange_union() { return MakeIndirectExec<true, false>(&multirange_union); }
TExecFunc arrow_multirange_upper() { return MakeIndirectExec<true, true>(&multirange_upper); }
TExecFunc arrow_multirange_upper_inc() { return MakeIndirectExec<true, true>(&multirange_upper_inc); }
TExecFunc arrow_multirange_upper_inf() { return MakeIndirectExec<true, true>(&multirange_upper_inf); }
TExecFunc arrow_networkjoinsel() { return MakeIndirectExec<true, true>(&networkjoinsel); }
TExecFunc arrow_networksel() { return MakeIndirectExec<true, true>(&networksel); }
TExecFunc arrow_pg_get_constraintdef() { return MakeIndirectExec<true, false>(&pg_get_constraintdef); }
TExecFunc arrow_pg_get_constraintdef_ext() { return MakeIndirectExec<true, false>(&pg_get_constraintdef_ext); }
TExecFunc arrow_pg_get_expr() { return MakeIndirectExec<true, false>(&pg_get_expr); }
TExecFunc arrow_pg_get_expr_ext() { return MakeIndirectExec<true, false>(&pg_get_expr_ext); }
TExecFunc arrow_pg_get_function_arg_default() { return MakeIndirectExec<true, false>(&pg_get_function_arg_default); }
TExecFunc arrow_pg_get_function_arguments() { return MakeIndirectExec<true, false>(&pg_get_function_arguments); }
TExecFunc arrow_pg_get_function_identity_arguments() { return MakeIndirectExec<true, false>(&pg_get_function_identity_arguments); }
TExecFunc arrow_pg_get_function_result() { return MakeIndirectExec<true, false>(&pg_get_function_result); }
TExecFunc arrow_pg_get_function_sqlbody() { return MakeIndirectExec<true, false>(&pg_get_function_sqlbody); }
TExecFunc arrow_pg_get_functiondef() { return MakeIndirectExec<true, false>(&pg_get_functiondef); }
TExecFunc arrow_pg_get_indexdef() { return MakeIndirectExec<true, false>(&pg_get_indexdef); }
TExecFunc arrow_pg_get_indexdef_ext() { return MakeIndirectExec<true, false>(&pg_get_indexdef_ext); }
TExecFunc arrow_pg_get_partition_constraintdef() { return MakeIndirectExec<true, false>(&pg_get_partition_constraintdef); }
TExecFunc arrow_pg_get_partkeydef() { return MakeIndirectExec<true, false>(&pg_get_partkeydef); }
TExecFunc arrow_pg_get_ruledef() { return MakeIndirectExec<true, false>(&pg_get_ruledef); }
TExecFunc arrow_pg_get_ruledef_ext() { return MakeIndirectExec<true, false>(&pg_get_ruledef_ext); }
TExecFunc arrow_pg_get_serial_sequence() { return MakeIndirectExec<true, false>(&pg_get_serial_sequence); }
TExecFunc arrow_pg_get_statisticsobjdef() { return MakeIndirectExec<true, false>(&pg_get_statisticsobjdef); }
TExecFunc arrow_pg_get_statisticsobjdef_columns() { return MakeIndirectExec<true, false>(&pg_get_statisticsobjdef_columns); }
TExecFunc arrow_pg_get_statisticsobjdef_expressions() { return MakeIndirectExec<true, false>(&pg_get_statisticsobjdef_expressions); }
TExecFunc arrow_pg_get_triggerdef() { return MakeIndirectExec<true, false>(&pg_get_triggerdef); }
TExecFunc arrow_pg_get_triggerdef_ext() { return MakeIndirectExec<true, false>(&pg_get_triggerdef_ext); }
TExecFunc arrow_pg_get_userbyid() { return MakeIndirectExec<true, false>(&pg_get_userbyid); }
TExecFunc arrow_pg_get_viewdef() { return MakeIndirectExec<true, false>(&pg_get_viewdef); }
TExecFunc arrow_pg_get_viewdef_ext() { return MakeIndirectExec<true, false>(&pg_get_viewdef_ext); }
TExecFunc arrow_pg_get_viewdef_name() { return MakeIndirectExec<true, false>(&pg_get_viewdef_name); }
TExecFunc arrow_pg_get_viewdef_name_ext() { return MakeIndirectExec<true, false>(&pg_get_viewdef_name_ext); }
TExecFunc arrow_pg_get_viewdef_wrap() { return MakeIndirectExec<true, false>(&pg_get_viewdef_wrap); }
TExecFunc arrow_range_adjacent_multirange() { return MakeIndirectExec<true, true>(&range_adjacent_multirange); }
TExecFunc arrow_range_after_multirange() { return MakeIndirectExec<true, true>(&range_after_multirange); }
TExecFunc arrow_range_agg_finalfn() { return MakeIndirectExec<false, false>(&range_agg_finalfn); }
TExecFunc arrow_range_agg_transfn() { return MakeIndirectExec<false, true>(&range_agg_transfn); }
TExecFunc arrow_range_before_multirange() { return MakeIndirectExec<true, true>(&range_before_multirange); }
TExecFunc arrow_range_contained_by_multirange() { return MakeIndirectExec<true, true>(&range_contained_by_multirange); }
TExecFunc arrow_range_contains_multirange() { return MakeIndirectExec<true, true>(&range_contains_multirange); }
TExecFunc arrow_range_merge_from_multirange() { return MakeIndirectExec<true, false>(&range_merge_from_multirange); }
TExecFunc arrow_range_overlaps_multirange() { return MakeIndirectExec<true, true>(&range_overlaps_multirange); }
TExecFunc arrow_range_overleft_multirange() { return MakeIndirectExec<true, true>(&range_overleft_multirange); }
TExecFunc arrow_range_overright_multirange() { return MakeIndirectExec<true, true>(&range_overright_multirange); }
TExecFunc arrow_spg_kd_choose() { return MakeIndirectExec<true, true>(&spg_kd_choose); }
TExecFunc arrow_spg_kd_config() { return MakeIndirectExec<true, true>(&spg_kd_config); }
TExecFunc arrow_spg_kd_inner_consistent() { return MakeIndirectExec<true, true>(&spg_kd_inner_consistent); }
TExecFunc arrow_spg_kd_picksplit() { return MakeIndirectExec<true, true>(&spg_kd_picksplit); }
TExecFunc arrow_spghandler() { return MakeIndirectExec<true, true>(&spghandler); }
TExecFunc arrow_tsq_mcontained() { return MakeIndirectExec<true, true>(&tsq_mcontained); }
TExecFunc arrow_tsq_mcontains() { return MakeIndirectExec<true, true>(&tsq_mcontains); }
TExecFunc arrow_tsquery_and() { return MakeIndirectExec<true, false>(&tsquery_and); }
TExecFunc arrow_tsquery_cmp() { return MakeIndirectExec<true, true>(&tsquery_cmp); }
TExecFunc arrow_tsquery_eq() { return MakeIndirectExec<true, true>(&tsquery_eq); }
TExecFunc arrow_tsquery_ge() { return MakeIndirectExec<true, true>(&tsquery_ge); }
TExecFunc arrow_tsquery_gt() { return MakeIndirectExec<true, true>(&tsquery_gt); }
TExecFunc arrow_tsquery_le() { return MakeIndirectExec<true, true>(&tsquery_le); }
TExecFunc arrow_tsquery_lt() { return MakeIndirectExec<true, true>(&tsquery_lt); }
TExecFunc arrow_tsquery_ne() { return MakeIndirectExec<true, true>(&tsquery_ne); }
TExecFunc arrow_tsquery_not() { return MakeIndirectExec<true, false>(&tsquery_not); }
TExecFunc arrow_tsquery_numnode() { return MakeIndirectExec<true, true>(&tsquery_numnode); }
TExecFunc arrow_tsquery_or() { return MakeIndirectExec<true, false>(&tsquery_or); }
TExecFunc arrow_tsquery_phrase() { return MakeIndirectExec<true, false>(&tsquery_phrase); }
TExecFunc arrow_tsquery_phrase_distance() { return MakeIndirectExec<true, false>(&tsquery_phrase_distance); }
