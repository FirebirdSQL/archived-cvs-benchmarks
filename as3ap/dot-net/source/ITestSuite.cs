//
// AS3AP -	An ANSI SQL Standard Scalable and Portable Benchmark
//			for Relational Database Systems.
// Copyright (C) 2003-2006  Carlos Guzman Alvarez
//
// Distributable under LGPL license.
// You may obtain a copy of the License at http://www.gnu.org/copyleft/lesser.html
//
// This library is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public
// License as published by the Free Software Foundation; either
// version 2.1 of the License, or (at your option) any later version.
// 
// This library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
// Lesser General Public License for more details.
//

using System;
using System.Data;

namespace DatabaseBenchmark
{
	public interface ITestSuite : IDisposable
	{
		#region · Properties ·

		BenchMarkConfiguration Configuration
		{
			get;
			set;
		}

		int TupleCount
		{
			get;
			set;
		}

		Logger Log
		{
			get;
			set;
		}

		String TestSuiteName
		{
			get;
		}

		#endregion

		#region · Events ·
		
		event ResultEventHandler	Result;
		event ProgressEventHandler	Progress;
		
		#endregion

		#region · Methods ·

		void SetIsolationLevel(string methodName);
				
		void CreateDatabase();

		void ConnectDatabase();
				
		void DisconnectDatabase();

		void SingleUserTests();

		void MultiUserTests(int nInstances);
		
		int CountRows(string table);

		void agg_create_view();

		void agg_func();

		void agg_info_retrieval();

		void agg_scal();

		void agg_simple_report();

		void agg_subtotal_report();

		void agg_total_report();

		void bulk_append();

		void bulk_delete();

		void bulk_modify();

		void bulk_save();

		void create_idx_hundred_code_h();

		void create_idx_hundred_foreign();

		void create_idx_hundred_key_bt();

		void create_idx_tenpct_code_h();

		void create_idx_tenpct_decim_bt();

		void create_idx_tenpct_double_bt();

		void create_idx_tenpct_float_bt();

		void create_idx_tenpct_int_bt();

		void create_idx_tenpct_key_bt();

		void create_idx_tenpct_key_code_bt();

		void create_idx_tenpct_name_h();

		void create_idx_tenpct_signed_bt();

		void create_idx_tiny_key_bt();

		void create_idx_uniques_code_h();

		void create_idx_uniques_key_bt();

		void create_idx_updates_code_h();

		void create_idx_updates_decim_bt();

		void create_idx_updates_double_bt();

		void create_idx_updates_int_bt();

		void create_idx_updates_key_bt();

		void create_tables();

		void drop_updates_keys();

		void join_2();

		void join_2_cl();

		void integrity_test();

		void join_2_ncl();

		void join_3_cl();

		void join_3_ncl();

		void join_4_cl();

		void join_4_ncl();

		void o_mode_tiny();

		void o_mode_100k();

		void mu_checkmod_100_rand();

		void mu_drop_sel100_rand();

		void mu_checkmod_100_seq();

		void mu_drop_sel100_seq();

		void mu_ir_select();

		void mu_mod_100_rand();

		void mu_mod_100_seq();

		void mu_oltp_update();
		
		void mu_sel_100_rand();

		void mu_sel_100_seq();

		void mu_unmod_100_rand();

		void mu_unmod_100_seq();

		void proj_100();

		void proj_10pct();

		void sel_1_cl();

		void sel_1_ncl();

		void sel_100_cl();

		void sel_100_ncl(); 

		void sel_10pct_ncl();

		void sel_variable_select(long foo);

		void sel_variable_select_high();

		void sel_variable_select_low();

		void table_scan();

		void upd_app_t_end();

		void upd_app_t_mid();

		void upd_append_duplicate();

		void upd_del_t_end();

		void upd_del_t_mid();

		void upd_mod_t_cod(); 

		void upd_mod_t_end();

		void upd_mod_t_int();

		void upd_mod_t_mid();

		void upd_remove_duplicate();		

		#endregion
	}
}
