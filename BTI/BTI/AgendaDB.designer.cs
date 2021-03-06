﻿#pragma warning disable 1591
//------------------------------------------------------------------------------
// <auto-generated>
//     This code was generated by a tool.
//     Runtime Version:4.0.30319.18408
//
//     Changes to this file may cause incorrect behavior and will be lost if
//     the code is regenerated.
// </auto-generated>
//------------------------------------------------------------------------------

namespace BTI
{
	using System.Data.Linq;
	using System.Data.Linq.Mapping;
	using System.Data;
	using System.Collections.Generic;
	using System.Reflection;
	using System.Linq;
	using System.Linq.Expressions;
	using System.ComponentModel;
	using System;
	
	
	[global::System.Data.Linq.Mapping.DatabaseAttribute(Name="AGENDA")]
	public partial class AgendaDBDataContext : System.Data.Linq.DataContext
	{
		
		private static System.Data.Linq.Mapping.MappingSource mappingSource = new AttributeMappingSource();
		
    #region Extensibility Method Definitions
    partial void OnCreated();
    partial void InsertCONTATO2(CONTATO2 instance);
    partial void UpdateCONTATO2(CONTATO2 instance);
    partial void DeleteCONTATO2(CONTATO2 instance);
    partial void InsertSETOR(SETOR instance);
    partial void UpdateSETOR(SETOR instance);
    partial void DeleteSETOR(SETOR instance);
    #endregion
		
		public AgendaDBDataContext() : 
				base(global::System.Configuration.ConfigurationManager.ConnectionStrings["AGENDAConnectionString"].ConnectionString, mappingSource)
		{
			OnCreated();
		}
		
		public AgendaDBDataContext(string connection) : 
				base(connection, mappingSource)
		{
			OnCreated();
		}
		
		public AgendaDBDataContext(System.Data.IDbConnection connection) : 
				base(connection, mappingSource)
		{
			OnCreated();
		}
		
		public AgendaDBDataContext(string connection, System.Data.Linq.Mapping.MappingSource mappingSource) : 
				base(connection, mappingSource)
		{
			OnCreated();
		}
		
		public AgendaDBDataContext(System.Data.IDbConnection connection, System.Data.Linq.Mapping.MappingSource mappingSource) : 
				base(connection, mappingSource)
		{
			OnCreated();
		}
		
		public System.Data.Linq.Table<CONTATO2> CONTATO2s
		{
			get
			{
				return this.GetTable<CONTATO2>();
			}
		}
		
		public System.Data.Linq.Table<SETOR> SETORs
		{
			get
			{
				return this.GetTable<SETOR>();
			}
		}
	}
	
	[global::System.Data.Linq.Mapping.TableAttribute(Name="dbo.CONTATO2")]
	public partial class CONTATO2 : INotifyPropertyChanging, INotifyPropertyChanged
	{
		
		private static PropertyChangingEventArgs emptyChangingEventArgs = new PropertyChangingEventArgs(String.Empty);
		
		private int _ID;
		
		private string _NOME;
		
		private string _RAMAL;
		
		private string _EMAIL;
		
		private System.Nullable<int> _SETOR_ID;
		
    #region Extensibility Method Definitions
    partial void OnLoaded();
    partial void OnValidate(System.Data.Linq.ChangeAction action);
    partial void OnCreated();
    partial void OnIDChanging(int value);
    partial void OnIDChanged();
    partial void OnNOMEChanging(string value);
    partial void OnNOMEChanged();
    partial void OnRAMALChanging(string value);
    partial void OnRAMALChanged();
    partial void OnEMAILChanging(string value);
    partial void OnEMAILChanged();
    partial void OnSETOR_IDChanging(System.Nullable<int> value);
    partial void OnSETOR_IDChanged();
    #endregion
		
		public CONTATO2()
		{
			OnCreated();
		}
		
		[global::System.Data.Linq.Mapping.ColumnAttribute(Storage="_ID", AutoSync=AutoSync.OnInsert, DbType="Int NOT NULL IDENTITY", IsPrimaryKey=true, IsDbGenerated=true)]
		public int ID
		{
			get
			{
				return this._ID;
			}
			set
			{
				if ((this._ID != value))
				{
					this.OnIDChanging(value);
					this.SendPropertyChanging();
					this._ID = value;
					this.SendPropertyChanged("ID");
					this.OnIDChanged();
				}
			}
		}
		
		[global::System.Data.Linq.Mapping.ColumnAttribute(Storage="_NOME", DbType="VarChar(50)")]
		public string NOME
		{
			get
			{
				return this._NOME;
			}
			set
			{
				if ((this._NOME != value))
				{
					this.OnNOMEChanging(value);
					this.SendPropertyChanging();
					this._NOME = value;
					this.SendPropertyChanged("NOME");
					this.OnNOMEChanged();
				}
			}
		}
		
		[global::System.Data.Linq.Mapping.ColumnAttribute(Storage="_RAMAL", DbType="VarChar(20)")]
		public string RAMAL
		{
			get
			{
				return this._RAMAL;
			}
			set
			{
				if ((this._RAMAL != value))
				{
					this.OnRAMALChanging(value);
					this.SendPropertyChanging();
					this._RAMAL = value;
					this.SendPropertyChanged("RAMAL");
					this.OnRAMALChanged();
				}
			}
		}
		
		[global::System.Data.Linq.Mapping.ColumnAttribute(Storage="_EMAIL", DbType="VarChar(50)")]
		public string EMAIL
		{
			get
			{
				return this._EMAIL;
			}
			set
			{
				if ((this._EMAIL != value))
				{
					this.OnEMAILChanging(value);
					this.SendPropertyChanging();
					this._EMAIL = value;
					this.SendPropertyChanged("EMAIL");
					this.OnEMAILChanged();
				}
			}
		}
		
		[global::System.Data.Linq.Mapping.ColumnAttribute(Storage="_SETOR_ID", DbType="Int")]
		public System.Nullable<int> SETOR_ID
		{
			get
			{
				return this._SETOR_ID;
			}
			set
			{
				if ((this._SETOR_ID != value))
				{
					this.OnSETOR_IDChanging(value);
					this.SendPropertyChanging();
					this._SETOR_ID = value;
					this.SendPropertyChanged("SETOR_ID");
					this.OnSETOR_IDChanged();
				}
			}
		}
		
		public event PropertyChangingEventHandler PropertyChanging;
		
		public event PropertyChangedEventHandler PropertyChanged;
		
		protected virtual void SendPropertyChanging()
		{
			if ((this.PropertyChanging != null))
			{
				this.PropertyChanging(this, emptyChangingEventArgs);
			}
		}
		
		protected virtual void SendPropertyChanged(String propertyName)
		{
			if ((this.PropertyChanged != null))
			{
				this.PropertyChanged(this, new PropertyChangedEventArgs(propertyName));
			}
		}
	}
	
	[global::System.Data.Linq.Mapping.TableAttribute(Name="dbo.SETOR")]
	public partial class SETOR : INotifyPropertyChanging, INotifyPropertyChanged
	{
		
		private static PropertyChangingEventArgs emptyChangingEventArgs = new PropertyChangingEventArgs(String.Empty);
		
		private int _ID;
		
		private string _DESCRICAO;
		
    #region Extensibility Method Definitions
    partial void OnLoaded();
    partial void OnValidate(System.Data.Linq.ChangeAction action);
    partial void OnCreated();
    partial void OnIDChanging(int value);
    partial void OnIDChanged();
    partial void OnDESCRICAOChanging(string value);
    partial void OnDESCRICAOChanged();
    #endregion
		
		public SETOR()
		{
			OnCreated();
		}
		
		[global::System.Data.Linq.Mapping.ColumnAttribute(Storage="_ID", DbType="Int NOT NULL", IsPrimaryKey=true)]
		public int ID
		{
			get
			{
				return this._ID;
			}
			set
			{
				if ((this._ID != value))
				{
					this.OnIDChanging(value);
					this.SendPropertyChanging();
					this._ID = value;
					this.SendPropertyChanged("ID");
					this.OnIDChanged();
				}
			}
		}
		
		[global::System.Data.Linq.Mapping.ColumnAttribute(Storage="_DESCRICAO", DbType="VarChar(50) NOT NULL", CanBeNull=false)]
		public string DESCRICAO
		{
			get
			{
				return this._DESCRICAO;
			}
			set
			{
				if ((this._DESCRICAO != value))
				{
					this.OnDESCRICAOChanging(value);
					this.SendPropertyChanging();
					this._DESCRICAO = value;
					this.SendPropertyChanged("DESCRICAO");
					this.OnDESCRICAOChanged();
				}
			}
		}
		
		public event PropertyChangingEventHandler PropertyChanging;
		
		public event PropertyChangedEventHandler PropertyChanged;
		
		protected virtual void SendPropertyChanging()
		{
			if ((this.PropertyChanging != null))
			{
				this.PropertyChanging(this, emptyChangingEventArgs);
			}
		}
		
		protected virtual void SendPropertyChanged(String propertyName)
		{
			if ((this.PropertyChanged != null))
			{
				this.PropertyChanged(this, new PropertyChangedEventArgs(propertyName));
			}
		}
	}
}
#pragma warning restore 1591
