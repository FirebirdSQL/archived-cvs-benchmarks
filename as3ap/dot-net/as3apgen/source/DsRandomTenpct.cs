//
// AS3AP Data file Generator
//
// Author: Carlos Guzmán Álvarez <carlosga@telefonica.net>
//
// Distributable under GPL license.
// You may obtain a copy of the License at http://www.gnu.org/copyleft/gpl.html
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

namespace AS3AP.BenchMark.Generator 
{
    using System;
    using System.Data;
    using System.Xml;
    using System.Runtime.Serialization;
    
    
    [Serializable()]
    [System.ComponentModel.DesignerCategoryAttribute("code")]
    [System.Diagnostics.DebuggerStepThrough()]
    [System.ComponentModel.ToolboxItem(true)]
    public class DsRandomTenpct : DataSet {
        
        private _TableDataTable table_Table;
        
        public DsRandomTenpct() {
            this.InitClass();
            System.ComponentModel.CollectionChangeEventHandler schemaChangedHandler = new System.ComponentModel.CollectionChangeEventHandler(this.SchemaChanged);
            this.Tables.CollectionChanged += schemaChangedHandler;
            this.Relations.CollectionChanged += schemaChangedHandler;
        }
        
        protected DsRandomTenpct(SerializationInfo info, StreamingContext context) {
            string strSchema = ((string)(info.GetValue("XmlSchema", typeof(string))));
            if ((strSchema != null)) {
                DataSet ds = new DataSet();
                ds.ReadXmlSchema(new XmlTextReader(new System.IO.StringReader(strSchema)));
                if ((ds.Tables["RANDOM_TENPCT"] != null)) {
                    this.Tables.Add(new _TableDataTable(ds.Tables["RANDOM_TENPCT"]));
                }
                this.DataSetName = ds.DataSetName;
                this.Prefix = ds.Prefix;
                this.Namespace = ds.Namespace;
                this.Locale = ds.Locale;
                this.CaseSensitive = ds.CaseSensitive;
                this.EnforceConstraints = ds.EnforceConstraints;
                this.Merge(ds, false, System.Data.MissingSchemaAction.Add);
                this.InitVars();
            }
            else {
                this.InitClass();
            }
            this.GetSerializationData(info, context);
            System.ComponentModel.CollectionChangeEventHandler schemaChangedHandler = new System.ComponentModel.CollectionChangeEventHandler(this.SchemaChanged);
            this.Tables.CollectionChanged += schemaChangedHandler;
            this.Relations.CollectionChanged += schemaChangedHandler;
        }
        
        [System.ComponentModel.Browsable(false)]
        [System.ComponentModel.DesignerSerializationVisibilityAttribute(System.ComponentModel.DesignerSerializationVisibility.Content)]
        public _TableDataTable _Table {
            get {
                return this.table_Table;
            }
        }
        
        public override DataSet Clone() {
            DsRandomTenpct cln = ((DsRandomTenpct)(base.Clone()));
            cln.InitVars();
            return cln;
        }
        
        protected override bool ShouldSerializeTables() {
            return false;
        }
        
        protected override bool ShouldSerializeRelations() {
            return false;
        }
        
        protected override void ReadXmlSerializable(XmlReader reader) {
            this.Reset();
            DataSet ds = new DataSet();
            ds.ReadXml(reader);
            if ((ds.Tables["RANDOM_TENPCT"] != null)) {
                this.Tables.Add(new _TableDataTable(ds.Tables["RANDOM_TENPCT"]));
            }
            this.DataSetName = ds.DataSetName;
            this.Prefix = ds.Prefix;
            this.Namespace = ds.Namespace;
            this.Locale = ds.Locale;
            this.CaseSensitive = ds.CaseSensitive;
            this.EnforceConstraints = ds.EnforceConstraints;
            this.Merge(ds, false, System.Data.MissingSchemaAction.Add);
            this.InitVars();
        }
        
        protected override System.Xml.Schema.XmlSchema GetSchemaSerializable() {
            System.IO.MemoryStream stream = new System.IO.MemoryStream();
            this.WriteXmlSchema(new XmlTextWriter(stream, null));
            stream.Position = 0;
            return System.Xml.Schema.XmlSchema.Read(new XmlTextReader(stream), null);
        }
        
        internal void InitVars() {
            this.table_Table = ((_TableDataTable)(this.Tables["RANDOM_TENPCT"]));
            if ((this.table_Table != null)) {
                this.table_Table.InitVars();
            }
        }
        
        private void InitClass() {
            this.DataSetName = "RANDOM_TENPCT";
            this.Prefix = "";
            this.Namespace = "";
            this.Locale = new System.Globalization.CultureInfo("es-ES");
            this.CaseSensitive = false;
            this.EnforceConstraints = true;
            this.table_Table = new _TableDataTable();
            this.Tables.Add(this.table_Table);
        }
        
        private bool ShouldSerialize_Table() {
            return false;
        }
        
        private void SchemaChanged(object sender, System.ComponentModel.CollectionChangeEventArgs e) {
            if ((e.Action == System.ComponentModel.CollectionChangeAction.Remove)) {
                this.InitVars();
            }
        }
        
        public delegate void _TableRowChangeEventHandler(object sender, _TableRowChangeEvent e);
        
        [System.Diagnostics.DebuggerStepThrough()]
        public class _TableDataTable : DataTable, System.Collections.IEnumerable {
            
            private DataColumn columnCOL_KEY;
            
            private DataColumn columnCOL_FLOAT;
            
            private DataColumn columnCOL_SIGNED;
            
            private DataColumn columnCOL_DOUBLE;
            
            private DataColumn columnCOL_ADDRESS;
            
            internal _TableDataTable() : 
                    base("RANDOM_TENPCT") {
                this.InitClass();
            }
            
            internal _TableDataTable(DataTable table) : 
                    base(table.TableName) {
                if ((table.CaseSensitive != table.DataSet.CaseSensitive)) {
                    this.CaseSensitive = table.CaseSensitive;
                }
                if ((table.Locale.ToString() != table.DataSet.Locale.ToString())) {
                    this.Locale = table.Locale;
                }
                if ((table.Namespace != table.DataSet.Namespace)) {
                    this.Namespace = table.Namespace;
                }
                this.Prefix = table.Prefix;
                this.MinimumCapacity = table.MinimumCapacity;
                this.DisplayExpression = table.DisplayExpression;
            }
            
            [System.ComponentModel.Browsable(false)]
            public int Count {
                get {
                    return this.Rows.Count;
                }
            }
            
            internal DataColumn COL_KEYColumn {
                get {
                    return this.columnCOL_KEY;
                }
            }
            
            internal DataColumn COL_FLOATColumn {
                get {
                    return this.columnCOL_FLOAT;
                }
            }
            
            internal DataColumn COL_SIGNEDColumn {
                get {
                    return this.columnCOL_SIGNED;
                }
            }
            
            internal DataColumn COL_DOUBLEColumn {
                get {
                    return this.columnCOL_DOUBLE;
                }
            }
            
            internal DataColumn COL_ADDRESSColumn {
                get {
                    return this.columnCOL_ADDRESS;
                }
            }
            
            public _TableRow this[int index] {
                get {
                    return ((_TableRow)(this.Rows[index]));
                }
            }
            
            public event _TableRowChangeEventHandler _TableRowChanged;
            
            public event _TableRowChangeEventHandler _TableRowChanging;
            
            public event _TableRowChangeEventHandler _TableRowDeleted;
            
            public event _TableRowChangeEventHandler _TableRowDeleting;
            
            public void Add_TableRow(_TableRow row) {
                this.Rows.Add(row);
            }
            
            public _TableRow Add_TableRow(int COL_KEY, System.Single COL_FLOAT, int COL_SIGNED, System.Double COL_DOUBLE, string COL_ADDRESS) {
                _TableRow row_TableRow = ((_TableRow)(this.NewRow()));
                row_TableRow.ItemArray = new object[] {
                        COL_KEY,
                        COL_FLOAT,
                        COL_SIGNED,
                        COL_DOUBLE,
                        COL_ADDRESS};
                this.Rows.Add(row_TableRow);
                return row_TableRow;
            }
            
            public System.Collections.IEnumerator GetEnumerator() {
                return this.Rows.GetEnumerator();
            }
            
            public override DataTable Clone() {
                _TableDataTable cln = ((_TableDataTable)(base.Clone()));
                cln.InitVars();
                return cln;
            }
            
            protected override DataTable CreateInstance() {
                return new _TableDataTable();
            }
            
            internal void InitVars() {
                this.columnCOL_KEY = this.Columns["COL_KEY"];
                this.columnCOL_FLOAT = this.Columns["COL_FLOAT"];
                this.columnCOL_SIGNED = this.Columns["COL_SIGNED"];
                this.columnCOL_DOUBLE = this.Columns["COL_DOUBLE"];
                this.columnCOL_ADDRESS = this.Columns["COL_ADDRESS"];
            }
            
            private void InitClass() {
                this.columnCOL_KEY = new DataColumn("COL_KEY", typeof(int), null, System.Data.MappingType.Element);
                this.Columns.Add(this.columnCOL_KEY);
                this.columnCOL_FLOAT = new DataColumn("COL_FLOAT", typeof(System.Single), null, System.Data.MappingType.Element);
                this.Columns.Add(this.columnCOL_FLOAT);
                this.columnCOL_SIGNED = new DataColumn("COL_SIGNED", typeof(int), null, System.Data.MappingType.Element);
                this.Columns.Add(this.columnCOL_SIGNED);
                this.columnCOL_DOUBLE = new DataColumn("COL_DOUBLE", typeof(System.Double), null, System.Data.MappingType.Element);
                this.Columns.Add(this.columnCOL_DOUBLE);
                this.columnCOL_ADDRESS = new DataColumn("COL_ADDRESS", typeof(string), null, System.Data.MappingType.Element);
                this.Columns.Add(this.columnCOL_ADDRESS);
                this.columnCOL_KEY.AllowDBNull = false;
                this.columnCOL_FLOAT.AllowDBNull = false;
                this.columnCOL_SIGNED.AllowDBNull = false;
                this.columnCOL_DOUBLE.AllowDBNull = false;
                this.columnCOL_ADDRESS.AllowDBNull = false;
                this.columnCOL_ADDRESS.MaxLength = 800;
            }
            
            public _TableRow New_TableRow() {
                return ((_TableRow)(this.NewRow()));
            }
            
            protected override DataRow NewRowFromBuilder(DataRowBuilder builder) {
                return new _TableRow(builder);
            }
            
            protected override System.Type GetRowType() {
                return typeof(_TableRow);
            }
            
            protected override void OnRowChanged(DataRowChangeEventArgs e) {
                base.OnRowChanged(e);
                if ((this._TableRowChanged != null)) {
                    this._TableRowChanged(this, new _TableRowChangeEvent(((_TableRow)(e.Row)), e.Action));
                }
            }
            
            protected override void OnRowChanging(DataRowChangeEventArgs e) {
                base.OnRowChanging(e);
                if ((this._TableRowChanging != null)) {
                    this._TableRowChanging(this, new _TableRowChangeEvent(((_TableRow)(e.Row)), e.Action));
                }
            }
            
            protected override void OnRowDeleted(DataRowChangeEventArgs e) {
                base.OnRowDeleted(e);
                if ((this._TableRowDeleted != null)) {
                    this._TableRowDeleted(this, new _TableRowChangeEvent(((_TableRow)(e.Row)), e.Action));
                }
            }
            
            protected override void OnRowDeleting(DataRowChangeEventArgs e) {
                base.OnRowDeleting(e);
                if ((this._TableRowDeleting != null)) {
                    this._TableRowDeleting(this, new _TableRowChangeEvent(((_TableRow)(e.Row)), e.Action));
                }
            }
            
            public void Remove_TableRow(_TableRow row) {
                this.Rows.Remove(row);
            }
        }
        
        [System.Diagnostics.DebuggerStepThrough()]
        public class _TableRow : DataRow {
            
            private _TableDataTable table_Table;
            
            internal _TableRow(DataRowBuilder rb) : 
                    base(rb) {
                this.table_Table = ((_TableDataTable)(this.Table));
            }
            
            public int COL_KEY {
                get {
                    return ((int)(this[this.table_Table.COL_KEYColumn]));
                }
                set {
                    this[this.table_Table.COL_KEYColumn] = value;
                }
            }
            
            public System.Double COL_FLOAT {
                get {
                    return ((System.Single)(this[this.table_Table.COL_FLOATColumn]));
                }
                set {
                    this[this.table_Table.COL_FLOATColumn] = value;
                }
            }
            
            public int COL_SIGNED {
                get {
                    return ((int)(this[this.table_Table.COL_SIGNEDColumn]));
                }
                set {
                    this[this.table_Table.COL_SIGNEDColumn] = value;
                }
            }
            
            public System.Double COL_DOUBLE {
                get {
                    return ((System.Double)(this[this.table_Table.COL_DOUBLEColumn]));
                }
                set {
                    this[this.table_Table.COL_DOUBLEColumn] = value;
                }
            }
            
            public string COL_ADDRESS {
                get {
                    return ((string)(this[this.table_Table.COL_ADDRESSColumn]));
                }
                set {
                    this[this.table_Table.COL_ADDRESSColumn] = value;
                }
            }
        }
        
        [System.Diagnostics.DebuggerStepThrough()]
        public class _TableRowChangeEvent : EventArgs {
            
            private _TableRow eventRow;
            
            private DataRowAction eventAction;
            
            public _TableRowChangeEvent(_TableRow row, DataRowAction action) {
                this.eventRow = row;
                this.eventAction = action;
            }
            
            public _TableRow Row {
                get {
                    return this.eventRow;
                }
            }
            
            public DataRowAction Action {
                get {
                    return this.eventAction;
                }
            }
        }
    }
}
