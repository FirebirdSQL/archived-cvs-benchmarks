//
// AS3AP -	An ANSI SQL Standard Scalable and Portable Benchmark
//			for Relational Database Systems.
//
// Ported from OSDB project at http://osdb.sourceforge.net
//
// Author: Carlos Guzmán Álvarez <carlosga@telefonica.net>
//
// Distributable under LGPL license.
// You may obtain a copy of the License at http://www.gnu.org/copyleft/lgpl.html
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// LGPL License for more details.
//
// This file was created by members of the Firebird development team.
// All individual contributions remain the Copyright (C) of those
// individuals.  Contributors to this file are either listed here or
// can be obtained from a CVS history command.
//
// (c) 2003. All rights reserved.
//
// For more information please see http://www.firebirdsql.org
//

using System;
using System.Data;

namespace AS3AP.BenchMark
{
	public interface ITestSuite
	{
		#region PROPERTIES

		BenchMarkConfiguration Configuration
		{
			get;
			set;
		}

		object TestResult
		{
			get;
			set;
		}

		bool TestFailed
		{
			get;
			set;
		}

		Backend Backend
		{
			get;
		}

		int TupleCount
		{
			get;
			set;
		}

		#endregion

		#region METHODS

		void CloseBackendLogger();

		void SetIsolationLevel(string methodName);

		void setup_database();
		
		void LoadData();
		
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